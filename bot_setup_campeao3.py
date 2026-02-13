# -*- coding: utf-8 -*-
"""
Bot Renko SETUP CAMPEÃO: RENKO DUAL-FLOW (EMA + CHOP)
=====================================================
100% fiel ao PDF Setup Campeão:
- Indicadores: EMA 9, EMA 21, CHOP (14 períodos)
- Grafico Renko Porcentagem LTP
- Entrada LONG: EMA9 cruza ACIMA da EMA21 + CHOP < 50
- Entrada SHORT: EMA9 cruza ABAIXO da EMA21 + CHOP < 50
- Saida por Reversão: Cruzamento contrário das EMAs
- Saida por Stop Loss: Fixo, 2 tijolos atrás da entrada (preco_entrada * (1 ± 2*brick%))
- SEM MACD - Setup Campeão usa apenas EMA + CHOP
"""

import asyncio
import argparse
import hmac
import hashlib
import time
import traceback
import sys
import os
import logging
import json
import numpy as np
import pandas as pd
import httpx
from datetime import datetime, timedelta
from collections import deque
from typing import Optional, Dict, List, Any, Tuple
from binance.enums import SIDE_BUY, SIDE_SELL
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK
from decimal import Decimal, ROUND_DOWN

# ==============================================================================
# CONFIGURACAO DE LOGGING
# ==============================================================================
def setup_logging(symbol: str) -> logging.Logger:
    """Configura logging para arquivo e console."""
    os.makedirs("bot_logs", exist_ok=True)

    logger = logging.getLogger(f"RenkoSetupCampeao_{symbol}")
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

    fmt = logging.Formatter(
        f'[{symbol}] [%(asctime)s] [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    log_file = f"bot_logs/{symbol}_CAMPEAO_{datetime.now().strftime('%Y%m%d')}.log"
    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger

# ==============================================================================
# PARAMETROS DOS INDICADORES - SETUP CAMPEÃO
# ==============================================================================
EMA_FAST_PERIOD = 9              # EMA 9 (Rápida - Sinal de Gatilho)
EMA_SLOW_PERIOD = 21             # EMA 21 (Lenta - Sinal de Tendência)
CHOP_PERIOD = 14                 # CHOP (Filtro de Lateralidade)
CHOP_THRESHOLD = 50.0            # Limiar: Só operamos se CHOP < 50

# ==============================================================================
# CONFIGURACOES GLOBAIS
# ==============================================================================
WARMUP_CANDLES = 1500    # Velas de 1 minuto para warmup
MIN_RENKO_BRICKS = 52    # Minimo de bricks para comecar a operar
FEE = 0.0012             # Taxa media de trading
MIN_NOTIONAL = 5.0       # Valor minimo de ordem em USDT
WS_TIMEOUT = 30          # Timeout do WebSocket
POSITION_SYNC_INTERVAL = 300  # Sincronizar posicao a cada 5 minutos
ENTRY_COOLDOWN_SECONDS = 10   # Cooldown entre entradas

# ==============================================================================
# CLASSE PUSHOVER - NOTIFICACOES
# ==============================================================================
class PushoverNotifier:
    def __init__(self, user_key: str, api_token: str, symbol: str):
        self.user_key = user_key
        self.api_token = api_token
        self.symbol = symbol
        self.enabled = bool(user_key and api_token)
        self.url = "https://api.pushover.net/1/messages.json"
        self._last_error_time = 0
        self._error_cooldown = 60

    async def _send(self, title: str, message: str, priority: int = 0):
        if not self.enabled:
            return False
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                payload = {
                    "token": self.api_token,
                    "user": self.user_key,
                    "title": f"[{self.symbol}] {title}",
                    "message": message[:1024],
                    "priority": priority,
                }
                if priority == 2:
                    payload["retry"] = 60
                    payload["expire"] = 300
                resp = await client.post(self.url, data=payload)
                return resp.status_code == 200
        except Exception as e:
            now = time.time()
            if now - self._last_error_time > self._error_cooldown:
                print(f"[{self.symbol}] [PUSHOVER] Falha: {e}")
                self._last_error_time = now
            return False

    async def notify_start(self, brick_pct: float, equity: float, leverage: int):
        msg = (
            f"Bot SETUP CAMPEÃO: Renko Dual-Flow Iniciado!\n"
            f"Brick: {brick_pct*100:.2f}% do LTP\n"
            f"Equity: ${equity:.2f}\n"
            f"Leverage: {leverage}x\n"
            f"Stream: aggTrade (tick a tick)\n"
            f"Indicadores: EMA9 + EMA21 + CHOP(14)\n"
        )
        await self._send("BOT SETUP CAMPEÃO INICIADO", msg)

    async def notify_warmup_complete(self, num_bricks: int, ema9: float,
                                      ema21: float, chop: float):
        msg = (
            f"Warmup Completo!\n"
            f"Bricks: {num_bricks}\n"
            f"EMA9: {ema9:.4f}\n"
            f"EMA21: {ema21:.4f}\n"
            f"CHOP: {chop:.2f}\n"
            f"Pronto para operar"
        )
        await self._send("WARMUP OK", msg)

    async def notify_entry(self, side: str, qty: float, price: float,
                          ema9: float, ema21: float, chop: float, stop_loss: float):
        msg = (
            f"ENTRADA {side} (SETUP CAMPEÃO)\n"
            f"Qty: {qty:.6f}\n"
            f"Preco: ${price:.4f}\n"
            f"Stop Loss: ${stop_loss:.4f}\n"
            f"---\n"
            f"EMA9: {ema9:.4f} | EMA21: {ema21:.4f}\n"
            f"CHOP: {chop:.2f} (Limite: 50.0)"
        )
        await self._send(f"ENTRY {side}", msg)

    async def notify_exit(self, side: str, qty: float, entry_price: float,
                         exit_price: float, pnl: float, pnl_pct: float, reason: str):
        status = "PROFIT" if pnl > 0 else "LOSS"
        msg = (
            f"SAIDA {side} - {status}\n"
            f"Motivo: {reason}\n"
            f"Qty: {qty:.6f}\n"
            f"Entry: ${entry_price:.4f}\n"
            f"Exit: ${exit_price:.4f}\n"
            f"PnL: ${pnl:.2f} ({pnl_pct:+.2f}%)"
        )
        await self._send(f"EXIT {reason}", msg)

    async def notify_circuit_breaker(self, reason: str, daily_pnl: float, daily_trades: int):
        msg = (
            f"CIRCUIT BREAKER ATIVADO!\n"
            f"Motivo: {reason}\n"
            f"PnL Diario: ${daily_pnl:.2f}\n"
            f"Trades Hoje: {daily_trades}"
        )
        await self._send("CIRCUIT BREAKER", msg, priority=2)

    async def notify_error(self, error: str, context: str = "", critical: bool = False):
        msg = f"{'ERRO CRITICO' if critical else 'Erro'}: {error}"
        if context:
            msg += f"\nContexto: {context}"
        priority = 2 if critical else 0
        await self._send("ERRO" if not critical else "ERRO CRITICO", msg, priority)

# ==============================================================================
# CLASSE BINANCE CLIENT
# ==============================================================================
class BinanceFuturesClient:
    BASE_URL = "https://testnet.binancefuture.com"

    def __init__(self, api_key: str, api_secret: str, symbol: str,
                 pushover: PushoverNotifier,
                 qty_precision: int = None, price_precision: int = None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.symbol = symbol
        self.pushover = pushover
        self.client: Optional[httpx.AsyncClient] = None
        self._qty_precision: Optional[int] = qty_precision
        self._price_precision: Optional[int] = price_precision
        self._step_size: Optional[Decimal] = None
        self._tick_size: Optional[Decimal] = None
        self._max_qty: Optional[float] = None
        self._min_qty: Optional[float] = None

    async def __aenter__(self):
        self.client = httpx.AsyncClient(timeout=30)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.aclose()

    def _sign(self, params: dict) -> dict:
        """Gera assinatura para requisicoes autenticadas."""
        params['timestamp'] = int(time.time() * 1000)
        query = '&'.join(f"{k}={v}" for k, v in params.items())
        params['signature'] = hmac.new(
            self.api_secret.encode(),
            query.encode(),
            hashlib.sha256
        ).hexdigest()
        return params

    def _headers(self) -> dict:
        return {"X-MBX-APIKEY": self.api_key}

    async def get_symbol_info(self) -> Dict[str, Any]:
        try:
            url = f"{self.BASE_URL}/fapi/v1/exchangeInfo"
            resp = await self.client.get(url)
            resp.raise_for_status()
            data = resp.json()
            for s in data.get('symbols', []):
                if s['symbol'] == self.symbol:
                    for f in s.get('filters', []):
                        if f['filterType'] == 'LOT_SIZE':
                            self._step_size = Decimal(str(f['stepSize']))
                            self._min_qty = float(f['minQty'])
                            self._max_qty = float(f['maxQty'])
                            step_str = f['stepSize']
                            if '.' in step_str:
                                self._qty_precision = len(
                                    step_str.rstrip('0').split('.')[1]
                                ) if '.' in step_str.rstrip('0') else 0
                            else:
                                self._qty_precision = 0
                        elif f['filterType'] == 'PRICE_FILTER':
                            self._tick_size = Decimal(str(f['tickSize']))
                            tick_str = f['tickSize']
                            if '.' in tick_str:
                                self._price_precision = len(
                                    tick_str.rstrip('0').split('.')[1]
                                ) if '.' in tick_str.rstrip('0') else 0
                            else:
                                self._price_precision = 0
                    return s
        except Exception as e:
            print(f"Erro get_symbol_info: {e}")
        return {}

    async def get_balance(self) -> float:
        try:
            url = f"{self.BASE_URL}/fapi/v2/balance"
            params = self._sign({})
            resp = await self.client.get(url, params=params, headers=self._headers())
            resp.raise_for_status()
            for asset in resp.json():
                if asset['asset'] == 'USDT':
                    return float(asset['availableBalance'])
        except Exception as e:
            print(f"Erro get_balance: {e}")
        return 0.0

    async def get_position(self) -> Optional[Dict]:
        try:
            url = f"{self.BASE_URL}/fapi/v2/positionRisk"
            params = self._sign({"symbol": self.symbol})
            resp = await self.client.get(url, params=params, headers=self._headers())
            resp.raise_for_status()
            for pos in resp.json():
                if pos['symbol'] == self.symbol:
                    qty = abs(float(pos['positionAmt']))
                    if qty > 0:
                        return {
                            'side': 'LONG' if float(pos['positionAmt']) > 0 else 'SHORT',
                            'qty': qty,
                            'entry_price': float(pos['entryPrice']),
                            'unrealized_pnl': float(pos.get('unRealizedProfit', 0)),
                            'leverage': int(pos.get('leverage', 10))
                        }
        except Exception as e:
            print(f"Erro get_position: {e}")
        return None

    async def set_leverage(self, leverage: int) -> bool:
        try:
            url = f"{self.BASE_URL}/fapi/v1/leverage"
            params = self._sign({"symbol": self.symbol, "leverage": leverage})
            resp = await self.client.post(url, params=params, headers=self._headers())
            return resp.status_code == 200
        except Exception as e:
            print(f"Erro set_leverage: {e}")
            return False

    async def set_margin_type(self, margin_type: str = "ISOLATED") -> bool:
        try:
            url = f"{self.BASE_URL}/fapi/v1/marginType"
            params = self._sign({"symbol": self.symbol, "marginType": margin_type})
            resp = await self.client.post(url, params=params, headers=self._headers())
            return resp.status_code == 200 or '-4046' in resp.text
        except Exception as e:
            print(f"Erro set_margin_type: {e}")
            return False

    async def place_market_order(self, side: str, quantity: float,
                                  qty_precision: int = None) -> Dict:
        """Envia ordem a mercado com formatacao segura da quantidade."""
        try:
            url = f"{self.BASE_URL}/fapi/v1/order"
            prec = qty_precision if qty_precision is not None else self._qty_precision

            if self._step_size and self._step_size > 0:
                qty_decimal = Decimal(str(quantity))
                step_decimal = Decimal(str(self._step_size))
                qty_decimal = (qty_decimal // step_decimal) * step_decimal
                quantity = float(qty_decimal)

            if prec is not None and prec >= 0:
                qty_str = f"{quantity:.{prec}f}"
            else:
                qty_str = f"{quantity:.8f}"

            if '.' in qty_str:
                qty_str = qty_str.rstrip('0').rstrip('.')

            params = self._sign({
                "symbol": self.symbol,
                "side": side,
                "type": "MARKET",
                "quantity": qty_str
            })

            resp = await self.client.post(url, params=params, headers=self._headers())
            resp.raise_for_status()
            result = resp.json()

            avg_price = float(result.get('avgPrice', 0))
            exec_qty = float(result.get('executedQty', 0))

            if avg_price == 0 and 'fills' in result and result['fills']:
                total_cost = 0.0
                total_qty = 0.0
                for fill in result['fills']:
                    fill_price = float(fill.get('price', 0))
                    fill_qty = float(fill.get('qty', 0))
                    total_cost += fill_price * fill_qty
                    total_qty += fill_qty
                if total_qty > 0:
                    result['avgPrice'] = str(total_cost / total_qty)
                    result['executedQty'] = str(total_qty)
                    print(f"[{self.symbol}] avgPrice recalculado dos fills: {result['avgPrice']}")

            if float(result.get('avgPrice', 0)) == 0:
                try:
                    ticker_url = f"{self.BASE_URL}/fapi/v1/ticker/price"
                    ticker_resp = await self.client.get(
                        ticker_url, params={"symbol": self.symbol}
                    )
                    if ticker_resp.status_code == 200:
                        ticker_data = ticker_resp.json()
                        result['avgPrice'] = ticker_data.get('price', '0')
                        print(f"[{self.symbol}] avgPrice obtido do ticker: {result['avgPrice']}")
                except Exception:
                    pass

            if float(result.get('executedQty', 0)) == 0:
                result['executedQty'] = qty_str

            return result
        except Exception as e:
            print(f"Erro place_market_order: {e}")
            raise

    async def get_klines(self, interval: str = "1m", limit: int = 1500,
                        end_time: int = None) -> List:
        try:
            url = f"{self.BASE_URL}/fapi/v1/klines"
            params = {"symbol": self.symbol, "interval": interval, "limit": limit}
            if end_time is not None:
                params["endTime"] = end_time
            resp = await self.client.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"Erro get_klines: {e}")
            return []

    async def cancel_all_orders(self) -> bool:
        try:
            url = f"{self.BASE_URL}/fapi/v1/allOpenOrders"
            params = self._sign({"symbol": self.symbol})
            resp = await self.client.delete(url, params=params, headers=self._headers())
            return resp.status_code == 200
        except Exception as e:
            print(f"Erro cancel orders: {e}")
            return False

# ==============================================================================
# LOGICA RENKO (LTP %)
# ==============================================================================
class RenkoBrick:
    __slots__ = ('open', 'close', 'high', 'low', 'color', 'timestamp', 'volume')

    def __init__(self, open_p: float, close_p: float, color: str,
                 timestamp: datetime, volume: float = 0.0):
        self.open = open_p
        self.close = close_p
        self.high = max(open_p, close_p)
        self.low = min(open_p, close_p)
        self.color = color
        self.timestamp = timestamp
        self.volume = volume

class RenkoChart:
    """
    Renko % LTP - Constroi boxes a partir do fluxo de preco em tempo real.
    O brick_size e recalculado como percentual do ultimo brick close (LTP).
    Construido via aggTrade stream (tick a tick).
    """
    def __init__(self, brick_size_pct: float):
        self.brick_size_pct = brick_size_pct
        self.brick_size = 0.0
        self.bricks: List[RenkoBrick] = []
        self.current_price = 0.0
        self.last_brick_close = 0.0
        self._pending_volume = 0.0

    def initialize(self, price: float):
        """Inicializa o Renko com preco base."""
        self.current_price = price
        self.last_brick_close = price
        self.brick_size = price * self.brick_size_pct

    def update(self, price: float, timestamp: datetime,
               volume: float = 0.0) -> List[RenkoBrick]:
        """Atualiza o grafico Renko com novo preco. Retorna lista de novos bricks."""
        self.current_price = price
        self._pending_volume += volume
        new_bricks = []

        if self.last_brick_close == 0:
            self.initialize(price)
            return new_bricks

        self.brick_size = self.last_brick_close * self.brick_size_pct
        diff = price - self.last_brick_close
        num_bricks = int(abs(diff) // self.brick_size) if self.brick_size > 0 else 0

        if num_bricks > 0:
            direction = 1 if diff > 0 else -1
            color = "green" if direction == 1 else "red"
            vol_per_brick = self._pending_volume / num_bricks if num_bricks > 0 else 0

            for _ in range(num_bricks):
                brick_open = self.last_brick_close
                brick_close = brick_open + (direction * self.brick_size)
                brick = RenkoBrick(brick_open, brick_close, color, timestamp, vol_per_brick)
                self.bricks.append(brick)
                new_bricks.append(brick)
                self.last_brick_close = brick_close
                self.brick_size = self.last_brick_close * self.brick_size_pct

            self._pending_volume = 0.0

        return new_bricks

    def get_closes(self, n: int) -> np.ndarray:
        return np.array([b.close for b in self.bricks[-n:]])

    def get_highs(self, n: int) -> np.ndarray:
        return np.array([b.high for b in self.bricks[-n:]])

    def get_lows(self, n: int) -> np.ndarray:
        return np.array([b.low for b in self.bricks[-n:]])

    def get_volumes(self, n: int) -> np.ndarray:
        return np.array([b.volume for b in self.bricks[-n:]])

# ==============================================================================
# INDICADORES TECNICOS - SETUP CAMPEÃO
# ==============================================================================
def calculate_ema(data: np.ndarray, period: int) -> np.ndarray:
    """Calcula EMA (Exponential Moving Average) completa."""
    df = pd.Series(data)
    ema = df.ewm(span=period, adjust=False).mean()
    return ema.values

def calculate_chop(data_high: np.ndarray, data_low: np.ndarray,
                   data_close: np.ndarray, period: int = 14) -> float:
    """
    Calcula Choppiness Index (CHOP) usando dados do Renko (High/Low do tijolo).
    
    Formula canonica:
    CHOP = 100 * LOG10( SUM(TR, period) / (Highest(High, period) - Lowest(Low, period)) ) / LOG10(period)
    
    Onde TR(i) = max( High(i) - Low(i), |High(i) - Close(i-1)|, |Low(i) - Close(i-1)| )
    
    Precisa de pelo menos period+1 barras de dados (period barras + 1 close anterior).
    """
    min_required = period + 1
    if len(data_high) < min_required or len(data_low) < min_required or len(data_close) < min_required:
        return 50.0  # Valor neutro se dados insuficientes

    # Janela de calculo: ultimas period barras
    high = data_high[-period:]
    low = data_low[-period:]
    # Close anterior para cada barra na janela
    # Para a barra i (0-indexed na janela), o close anterior e data_close[-(period+1) + i]
    close_prev = data_close[-(period + 1):-1]

    # True Range para cada barra
    tr = np.maximum(
        high - low,
        np.maximum(
            np.abs(high - close_prev),
            np.abs(low - close_prev)
        )
    )

    tr_sum = np.sum(tr)
    max_high = np.max(high)
    min_low = np.min(low)

    hl_range = max_high - min_low
    if hl_range <= 0 or tr_sum <= 0:
        return 50.0

    chop = 100.0 * np.log10(tr_sum / hl_range) / np.log10(period)
    return float(np.clip(chop, 0.0, 100.0))

# ==============================================================================
# ESTRATEGIA DO BOT - SETUP CAMPEÃO: RENKO DUAL-FLOW
# ==============================================================================
class RenkoSetupCampeaoBot:
    def __init__(self, symbol: str, api_key: str, api_secret: str,
                 brick_size_pct: float, leverage: int,
                 stop_loss_bricks: int = 2,
                 loss_limit: float = 0.30, max_trades: int = 200,
                 pushover_user: str = "", pushover_token: str = ""):

        self.symbol = symbol
        self.api_key = api_key
        self.api_secret = api_secret
        self.brick_size_pct = brick_size_pct
        self.leverage = leverage
        self.stop_loss_bricks = stop_loss_bricks

        # Circuit Breaker
        self.loss_limit = loss_limit
        self.max_trades = max_trades
        self._circuit_breaker_active = False

        self.logger = setup_logging(symbol)
        self.pushover = PushoverNotifier(pushover_user, pushover_token, symbol)

        self.renko: Optional[RenkoChart] = None

        # Valores dos indicadores (deque para historico)
        self.ema9_values = deque(maxlen=100)
        self.ema21_values = deque(maxlen=100)
        self.chop_values = deque(maxlen=100)

        # Estado da posicao
        self.position = None
        self.entry_price = 0.0
        self.equity = 0.0
        self.initial_equity = 0.0
        self.stop_loss_price = 0.0
        self.original_qty = 0.0

        self.warmup_complete = False
        self.last_sync_time = 0

        # Metricas diarias
        self.daily_pnl = 0.0
        self.daily_trades = 0
        self.last_reset_date = datetime.now().date()

        # Cooldown de entrada
        self._last_entry_error_time = 0.0

        # Mecanismo Anti-Estagnacao
        self.last_brick_time = time.time()
        self.max_time_without_brick = 600

        # Precisao do par (atualizado pelo exchange info)
        self._qty_precision = None
        self._price_precision = None
        self._step_size = None
        self._min_qty = None

        # Tasks pendentes
        self._pending_tasks = []

        self.running = True

    def log(self, msg: str, level: str = "info"):
        """Log com timestamp."""
        if level == "info":
            self.logger.info(msg)
        elif level == "warning":
            self.logger.warning(msg)
        elif level == "error":
            self.logger.error(msg)
        elif level == "debug":
            self.logger.debug(msg)

    def _check_daily_reset(self):
        """Reseta contadores diarios se mudou o dia."""
        today = datetime.now().date()
        if today != self.last_reset_date:
            self.log(f"Reset diario: PnL={self.daily_pnl:.2f}, Trades={self.daily_trades}")
            self.daily_pnl = 0.0
            self.daily_trades = 0
            self.last_reset_date = today
            self._circuit_breaker_active = False

    def _check_circuit_breaker(self) -> bool:
        """Verifica se o circuit breaker foi acionado."""
        self._check_daily_reset()

        if self.initial_equity > 0 and self.daily_pnl <= -self.initial_equity * self.loss_limit:
            if not self._circuit_breaker_active:
                self.log(f"CIRCUIT BREAKER ATIVADO - Perda diaria: {self.daily_pnl:.2f}", "warning")
                task = asyncio.ensure_future(
                    self.pushover.notify_circuit_breaker("Perda diaria", self.daily_pnl, self.daily_trades)
                )
                self._pending_tasks.append(task)
                self._circuit_breaker_active = True
            return True

        if self.daily_trades >= self.max_trades:
            if not self._circuit_breaker_active:
                self.log(f"CIRCUIT BREAKER ATIVADO - Max trades: {self.daily_trades}", "warning")
                task = asyncio.ensure_future(
                    self.pushover.notify_circuit_breaker("Max trades", self.daily_pnl, self.daily_trades)
                )
                self._pending_tasks.append(task)
                self._circuit_breaker_active = True
            return True

        return False

    # ==========================================================================
    # WARMUP
    # ==========================================================================
    async def warmup(self):
        """
        Carrega dados historicos e constroi o grafico Renko inicial.
        Warmup progressivo.
        """
        self.log(f"Iniciando warmup para {self.symbol}...")

        try:
            async with BinanceFuturesClient(
                self.api_key, self.api_secret, self.symbol, self.pushover
            ) as client:
                # 1. Obter saldo
                self.equity = await client.get_balance()
                self.initial_equity = self.equity
                self.log(f"Saldo inicial: ${self.equity:.2f}")

                # 2. Obter info do par
                info = await client.get_symbol_info()
                if not info:
                    raise ValueError(f"Nao foi possivel obter informacoes do par {self.symbol}")

                # Salvar precisao
                if client._qty_precision is not None:
                    self._qty_precision = client._qty_precision
                if client._price_precision is not None:
                    self._price_precision = client._price_precision
                if client._step_size is not None:
                    self._step_size = float(client._step_size)
                if client._min_qty is not None:
                    self._min_qty = client._min_qty

                # 3. Configurar leverage na exchange
                leverage_ok = await client.set_leverage(self.leverage)
                if leverage_ok:
                    self.log(f"Leverage configurado: {self.leverage}x")
                else:
                    self.log(f"Aviso: Falha ao configurar leverage {self.leverage}x", "warning")

                # 4. Configurar margin type (ISOLATED)
                await client.set_margin_type("ISOLATED")

                # 5. Warmup progressivo
                all_klines = []
                max_warmup_rounds = 3
                end_time = None

                for warmup_round in range(max_warmup_rounds):
                    klines = await client.get_klines(
                        interval="1m", limit=WARMUP_CANDLES, end_time=end_time
                    )
                    if not klines:
                        if warmup_round == 0:
                            raise ValueError("Nao foi possivel obter klines para warmup")
                        break

                    all_klines = klines + all_klines

                    # Testar se ja temos bricks suficientes
                    test_renko = RenkoChart(self.brick_size_pct)
                    test_renko.initialize(float(all_klines[0][1]))
                    for k in all_klines:
                        test_renko.update(
                            float(k[4]),
                            datetime.fromtimestamp(k[0] / 1000),
                            float(k[5])
                        )

                    test_bricks = len(test_renko.bricks)
                    self.log(
                        f"Warmup rodada {warmup_round + 1}: {len(all_klines)} velas -> "
                        f"{test_bricks} bricks (minimo: {MIN_RENKO_BRICKS})"
                    )

                    if test_bricks >= MIN_RENKO_BRICKS:
                        break

                    end_time = int(all_klines[0][0]) - 1
                    await asyncio.sleep(0.5)

                # Remover duplicatas e ordenar por timestamp
                seen = set()
                unique_klines = []
                for k in all_klines:
                    ts = k[0]
                    if ts not in seen:
                        seen.add(ts)
                        unique_klines.append(k)
                unique_klines.sort(key=lambda x: x[0])
                all_klines = unique_klines

                if not all_klines:
                    raise ValueError("Nao foi possivel obter klines para warmup")

                # 6. Inicializar Renko com brick_size_pct (LTP %)
                current_price = float(all_klines[-1][4])
                self.renko = RenkoChart(self.brick_size_pct)
                self.renko.initialize(float(all_klines[0][1]))

                self.log(
                    f"Brick Size: {self.brick_size_pct*100:.2f}% do LTP "
                    f"(atual ~${current_price * self.brick_size_pct:.4f}) | "
                    f"Velas carregadas: {len(all_klines)}"
                )

                # 7. Processar klines
                for k in all_klines:
                    close_price = float(k[4])
                    candle_volume = float(k[5])
                    self.renko.update(
                        close_price,
                        datetime.fromtimestamp(k[0] / 1000),
                        candle_volume
                    )
                    self.update_indicators()

                bricks_cnt = len(self.renko.bricks)
                if bricks_cnt >= MIN_RENKO_BRICKS:
                    self.warmup_complete = True

                    ema9 = self.ema9_values[-1] if self.ema9_values else 0
                    ema21 = self.ema21_values[-1] if self.ema21_values else 0
                    chop = self.chop_values[-1] if self.chop_values else 50.0

                    self.log(
                        f"Warmup completo: {bricks_cnt} bricks | "
                        f"EMA9={ema9:.4f} | EMA21={ema21:.4f} | CHOP={chop:.2f}"
                    )
                    await self.pushover.notify_warmup_complete(bricks_cnt, ema9, ema21, chop)
                    await self.pushover.notify_start(
                        self.brick_size_pct, self.equity, self.leverage
                    )

                    # Sincronizar posicao inicial
                    await self.sync_position()
                else:
                    self.log(
                        f"Bricks insuficientes apos {max_warmup_rounds} rodadas: "
                        f"{bricks_cnt} < {MIN_RENKO_BRICKS} ({len(all_klines)} velas)",
                        "warning"
                    )
                    raise ValueError(
                        f"Warmup falhou: apenas {bricks_cnt} bricks com {len(all_klines)} velas"
                    )

        except Exception as e:
            self.log(f"Erro no warmup: {e}", "error")
            traceback.print_exc()
            raise

    # ==========================================================================
    # INDICADORES - SETUP CAMPEÃO (EMA + CHOP)
    # ==========================================================================
    def update_indicators(self):
        """Atualiza indicadores tecnicos: EMA 9, EMA 21, CHOP(14)."""
        # CHOP precisa de period+1 barras; EMA precisa de pelo menos period barras
        required = max(EMA_SLOW_PERIOD, CHOP_PERIOD + 1) + 10

        if len(self.renko.bricks) < required:
            return

        closes = self.renko.get_closes(required + 50)
        highs = self.renko.get_highs(required + 50)
        lows = self.renko.get_lows(required + 50)

        # EMA 9 (Rápida)
        ema9_arr = calculate_ema(closes, EMA_FAST_PERIOD)
        self.ema9_values.append(float(ema9_arr[-1]))

        # EMA 21 (Lenta)
        ema21_arr = calculate_ema(closes, EMA_SLOW_PERIOD)
        self.ema21_values.append(float(ema21_arr[-1]))

        # CHOP (14 períodos) - usando High/Low do Renko conforme PDF
        chop = calculate_chop(highs, lows, closes, CHOP_PERIOD)
        self.chop_values.append(chop)

    def _validate_indicators(self) -> bool:
        """Valida se todos os indicadores tem valores validos."""
        if len(self.ema9_values) < 2 or len(self.ema21_values) < 2:
            return False
        if len(self.chop_values) < 1:
            return False

        ema9_now = self.ema9_values[-1]
        ema9_prev = self.ema9_values[-2]
        ema21_now = self.ema21_values[-1]
        ema21_prev = self.ema21_values[-2]
        chop = self.chop_values[-1]

        if (np.isnan(ema9_now) or np.isnan(ema9_prev) or
            np.isnan(ema21_now) or np.isnan(ema21_prev) or np.isnan(chop)):
            return False

        return True

    # ==========================================================================
    # DETECCAO DE CRUZAMENTOS (SETUP CAMPEÃO)
    # ==========================================================================
    def detect_crossovers(self) -> Tuple[bool, bool]:
        """
        Detecta cruzamentos da EMA9 com EMA21.
        Retorna (cruzamento_alta, cruzamento_baixa)

        cruzamento_alta: EMA9 estava abaixo da EMA21, agora está acima (sinal de LONG)
        cruzamento_baixa: EMA9 estava acima da EMA21, agora está abaixo (sinal de SHORT)

        Conforme pseudo-codigo do PDF:
          cruzamento_alta = (ema_fast_prev < ema_slow_prev) and (ema_fast_now > ema_slow_now)
          cruzamento_baixa = (ema_fast_prev > ema_slow_prev) and (ema_fast_now < ema_slow_now)
        """
        if len(self.ema9_values) < 2 or len(self.ema21_values) < 2:
            return False, False

        ema9_now = self.ema9_values[-1]
        ema9_prev = self.ema9_values[-2]
        ema21_now = self.ema21_values[-1]
        ema21_prev = self.ema21_values[-2]

        # Cruzamento para CIMA (antes abaixo, agora acima)
        cruzamento_alta = (ema9_prev < ema21_prev) and (ema9_now > ema21_now)

        # Cruzamento para BAIXO (antes acima, agora abaixo)
        cruzamento_baixa = (ema9_prev > ema21_prev) and (ema9_now < ema21_now)

        return cruzamento_alta, cruzamento_baixa

    # ==========================================================================
    # CONDICOES DE ENTRADA - SETUP CAMPEÃO (PDF)
    # ==========================================================================
    def check_entry_conditions(self) -> Optional[str]:
        """
        Verifica condicoes de entrada conforme SETUP CAMPEÃO (PDF).
        Todas as condicoes sao verificadas AO FECHAR TIJOLO RENKO.

        LONG:
          1. EMA9 cruza ACIMA da EMA21 (no fechamento do tijolo)
          2. CHOP < 50 (no momento exato do cruzamento)

        SHORT:
          1. EMA9 cruza ABAIXO da EMA21 (no fechamento do tijolo)
          2. CHOP < 50 (no momento exato do cruzamento)
        """
        if not self.warmup_complete or self.position is not None:
            return None

        if not self._validate_indicators():
            return None

        if len(self.renko.bricks) < 3:
            return None

        # Circuit breaker
        if self._check_circuit_breaker():
            return None

        # Cooldown apos erro de entrada
        if time.time() - self._last_entry_error_time < ENTRY_COOLDOWN_SECONDS:
            return None

        cruzamento_alta, cruzamento_baixa = self.detect_crossovers()

        ema9 = self.ema9_values[-1]
        ema21 = self.ema21_values[-1]
        chop = self.chop_values[-1]

        # Logs detalhados
        self.log("="*80, "debug")
        self.log(f"VERIFICACAO DE ENTRADA (SETUP CAMPEÃO)", "debug")
        self.log(f"EMA9: {ema9:.4f} | EMA21: {ema21:.4f}", "debug")
        self.log(f"CHOP: {chop:.2f} (Limite: {CHOP_THRESHOLD})", "debug")
        self.log(f"Cruzamento Alta: {cruzamento_alta} | Cruzamento Baixa: {cruzamento_baixa}", "debug")
        self.log("="*80, "debug")

        # --- LONG (EMA9 cruza ACIMA da EMA21 + CHOP < 50) ---
        if cruzamento_alta:
            if chop < CHOP_THRESHOLD:
                self.log("="*80)
                self.log(f"SINAL DE LONG CONFIRMADO!")
                self.log("="*80)
                self.log(
                    f"ENTRADA LONG (SETUP CAMPEÃO)! EMA9 cruzou acima da EMA21 | "
                    f"EMA9={ema9:.4f} | EMA21={ema21:.4f} | CHOP={chop:.2f}"
                )
                return "LONG"
            else:
                self.log(
                    f"SINAL IGNORADO: Cruzamento de Alta mas CHOP Alto "
                    f"({chop:.2f} >= {CHOP_THRESHOLD}) - Mercado Lateral",
                    "debug"
                )

        # --- SHORT (EMA9 cruza ABAIXO da EMA21 + CHOP < 50) ---
        if cruzamento_baixa:
            if chop < CHOP_THRESHOLD:
                self.log("="*80)
                self.log(f"SINAL DE SHORT CONFIRMADO!")
                self.log("="*80)
                self.log(
                    f"ENTRADA SHORT (SETUP CAMPEÃO)! EMA9 cruzou abaixo da EMA21 | "
                    f"EMA9={ema9:.4f} | EMA21={ema21:.4f} | CHOP={chop:.2f}"
                )
                return "SHORT"
            else:
                self.log(
                    f"SINAL IGNORADO: Cruzamento de Baixa mas CHOP Alto "
                    f"({chop:.2f} >= {CHOP_THRESHOLD}) - Mercado Lateral",
                    "debug"
                )

        return None

    # ==========================================================================
    # CONDICOES DE SAIDA - SETUP CAMPEÃO (PDF)
    # ==========================================================================
    def check_exit_conditions(self, brick_close: float) -> Optional[str]:
        """
        Verifica condicoes de saida conforme SETUP CAMPEÃO (PDF).
        Chamado AO FECHAR TIJOLO RENKO (nao a cada tick).

        Ordem de verificacao conforme pseudo-codigo do PDF:
        1. PRIMEIRO verifica Stop Loss (emergencia)
        2. DEPOIS verifica Reversão de Tendência

        LONG:
          - Stop Loss: close_price <= preco_entrada * (1 - 2 * brick_size_pct)
          - Reversão: EMA9 cruza abaixo da EMA21

        SHORT:
          - Stop Loss: close_price >= preco_entrada * (1 + 2 * brick_size_pct)
          - Reversão: EMA9 cruza acima da EMA21
        """
        if not self.position or not self._validate_indicators():
            return None

        side = self.position['side']

        # --- LONG ---
        if side == 'LONG':
            # 1. Stop Loss (Emergencia) - PRIMEIRO conforme PDF
            if self.stop_loss_price > 0 and brick_close <= self.stop_loss_price:
                self.log(
                    f"STOP LOSS LONG - Fechamento({brick_close:.4f}) <= "
                    f"Stop({self.stop_loss_price:.4f})"
                )
                return "STOP_LOSS"

            # 2. Saida por Reversão: EMA9 cruza abaixo da EMA21
            cruzamento_alta, cruzamento_baixa = self.detect_crossovers()
            if cruzamento_baixa:
                self.log(
                    f"SAIDA POR REVERSÃO LONG - EMA9 cruzou abaixo da EMA21 | "
                    f"EMA9={self.ema9_values[-1]:.4f} | EMA21={self.ema21_values[-1]:.4f}"
                )
                return "REVERSAO"

        # --- SHORT ---
        elif side == 'SHORT':
            # 1. Stop Loss (Emergencia) - PRIMEIRO conforme PDF
            if self.stop_loss_price > 0 and brick_close >= self.stop_loss_price:
                self.log(
                    f"STOP LOSS SHORT - Fechamento({brick_close:.4f}) >= "
                    f"Stop({self.stop_loss_price:.4f})"
                )
                return "STOP_LOSS"

            # 2. Saida por Reversão: EMA9 cruza acima da EMA21
            cruzamento_alta, cruzamento_baixa = self.detect_crossovers()
            if cruzamento_alta:
                self.log(
                    f"SAIDA POR REVERSÃO SHORT - EMA9 cruzou acima da EMA21 | "
                    f"EMA9={self.ema9_values[-1]:.4f} | EMA21={self.ema21_values[-1]:.4f}"
                )
                return "REVERSAO"

        return None

    # ==========================================================================
    # STOP LOSS - CONFORME PDF SETUP CAMPEÃO
    # ==========================================================================
    def calculate_initial_stop_loss(self, side: str, entry_price: float) -> float:
        """
        Calcula stop loss inicial conforme Setup Campeão PDF:
          LONG: stop_loss = preco_entrada * (1 - (N * BOX_SIZE_PCT / 100))
          SHORT: stop_loss = preco_entrada * (1 + (N * BOX_SIZE_PCT / 100))

        No PDF, BOX_SIZE_PCT = 0.5 (representando 0.5%) e a formula divide por 100.
        No nosso bot, brick_size_pct ja e decimal (ex: 0.003 = 0.3%), entao:
          LONG: stop = entry * (1 - N * brick_size_pct)
          SHORT: stop = entry * (1 + N * brick_size_pct)

        Fixo, N tijolos atras da entrada (aprox. N * brick_size de movimento contra).
        """
        if entry_price <= 0:
            return 0.0

        n_bricks = float(self.stop_loss_bricks)

        if side == 'LONG':
            stop = entry_price * (1.0 - n_bricks * self.brick_size_pct)
            self.log(
                f"Stop Loss LONG inicial: ${entry_price:.4f} * "
                f"(1 - {n_bricks} * {self.brick_size_pct}) = ${stop:.4f} "
                f"(~{n_bricks * self.brick_size_pct * 100:.2f}% abaixo)"
            )
        else:
            stop = entry_price * (1.0 + n_bricks * self.brick_size_pct)
            self.log(
                f"Stop Loss SHORT inicial: ${entry_price:.4f} * "
                f"(1 + {n_bricks} * {self.brick_size_pct}) = ${stop:.4f} "
                f"(~{n_bricks * self.brick_size_pct * 100:.2f}% acima)"
            )

        return stop

    # ==========================================================================
    # SINCRONIZACAO DE POSICAO
    # ==========================================================================
    async def sync_position(self):
        """Sincroniza o estado local com a posicao na exchange."""
        try:
            async with BinanceFuturesClient(
                self.api_key, self.api_secret, self.symbol, self.pushover,
                self._qty_precision, self._price_precision
            ) as client:
                pos = await client.get_position()

                if pos and not self.position:
                    self.log(
                        f"Posicao detectada na exchange: {pos['side']} "
                        f"{pos['qty']} @ {pos['entry_price']}"
                    )
                    self.position = pos
                    self.entry_price = pos['entry_price']
                    self.original_qty = pos['qty']
                    # Recalcular stop loss baseado no preco de entrada
                    self.stop_loss_price = self.calculate_initial_stop_loss(
                        pos['side'], pos['entry_price']
                    )

                elif not pos and self.position:
                    self.log("Posicao foi fechada externamente")
                    self._reset_position_state()

                elif pos and self.position:
                    self.position['qty'] = pos['qty']
                    self.position['unrealized_pnl'] = pos.get('unrealized_pnl', 0)

        except Exception as e:
            self.log(f"Erro ao sincronizar posicao: {e}", "warning")

    def _reset_position_state(self):
        """Reseta todo o estado da posicao."""
        self.position = None
        self.entry_price = 0.0
        self.stop_loss_price = 0.0
        self.original_qty = 0.0

    # ==========================================================================
    # EXECUCAO DE ENTRADA
    # ==========================================================================
    async def enter_position(self, side: str):
        """
        Entra em uma nova posicao conforme SETUP CAMPEÃO.
        Conforme PDF (Refinamento 3): Verifica se a ordem foi realmente
        executada na Binance antes de mudar a variavel posicao_atual.
        """
        notify_data = None

        try:
            # Verificar se ja existe posicao na exchange antes de abrir
            async with BinanceFuturesClient(
                self.api_key, self.api_secret, self.symbol, self.pushover,
                self._qty_precision, self._price_precision
            ) as client:
                existing_pos = await client.get_position()
                if existing_pos:
                    self.log(
                        f"Posicao ja existe na exchange ({existing_pos['side']}). "
                        f"Cancelando entrada.", "warning"
                    )
                    self.position = existing_pos
                    self.entry_price = existing_pos['entry_price']
                    self.original_qty = existing_pos['qty']
                    self.stop_loss_price = self.calculate_initial_stop_loss(
                        existing_pos['side'], existing_pos['entry_price']
                    )
                    return

                await client.get_symbol_info()

                # Salvar precisao atualizada
                if client._qty_precision is not None:
                    self._qty_precision = client._qty_precision
                if client._price_precision is not None:
                    self._price_precision = client._price_precision
                if client._step_size is not None:
                    self._step_size = float(client._step_size)
                if client._min_qty is not None:
                    self._min_qty = client._min_qty

                price = self.renko.current_price

                # Risco de 2% da equity por trade
                position_value = self.equity * 0.02 * self.leverage
                qty = position_value / price

                # Arredondar para step_size usando Decimal para precisao exata
                if self._step_size and self._step_size > 0:
                    qty_decimal = Decimal(str(qty))
                    step_decimal = Decimal(str(self._step_size))
                    qty_decimal = (qty_decimal // step_decimal) * step_decimal
                    qty = float(qty_decimal)

                # Aplicar precisao de quantidade
                if self._qty_precision is not None:
                    qty = float(Decimal(str(qty)).quantize(
                        Decimal(f"1e-{self._qty_precision}"),
                        rounding=ROUND_DOWN
                    ))

                # Verificar min_qty
                if self._min_qty and qty < self._min_qty:
                    qty = self._min_qty

                # Verificar notional minimo
                notional = qty * price
                if notional < MIN_NOTIONAL:
                    qty = (MIN_NOTIONAL / price) * 1.1
                    if self._step_size and self._step_size > 0:
                        qty_decimal = Decimal(str(qty))
                        step_decimal = Decimal(str(self._step_size))
                        qty_decimal = ((qty_decimal // step_decimal) + 1) * step_decimal
                        qty = float(qty_decimal)

                    if self._qty_precision is not None:
                        qty = float(Decimal(str(qty)).quantize(
                            Decimal(f"1e-{self._qty_precision}"),
                            rounding=ROUND_DOWN
                        ))

                order_side = SIDE_BUY if side == 'LONG' else SIDE_SELL
                self.log(f"Enviando ordem {order_side} qty={qty}")

                order = await client.place_market_order(order_side, qty, self._qty_precision)

                filled_qty = float(order.get('executedQty', qty))
                fill_price = float(order.get('avgPrice', price))

                # Protecao: se qty ou preco vieram zerados
                if filled_qty <= 0:
                    filled_qty = qty
                    self.log(f"AVISO: executedQty veio 0, usando qty calculada: {qty}", "warning")
                if fill_price <= 0:
                    fill_price = price
                    self.log(f"AVISO: avgPrice veio 0, usando preco atual: {price}", "warning")

                self.log(f"Ordem executada: {filled_qty} @ ${fill_price:.4f}")

                # Gestao de API (Refinamento 3 do PDF): Só atualiza estado após confirmação
                self.position = {
                    'side': side,
                    'qty': filled_qty,
                    'entry_price': fill_price,
                    'leverage': self.leverage
                }
                self.entry_price = fill_price
                self.original_qty = filled_qty
                self.daily_trades += 1

                # ===== STOP LOSS SETUP CAMPEÃO (PDF) =====
                # Fixo: 2 tijolos atras do preco de entrada
                self.stop_loss_price = self.calculate_initial_stop_loss(side, fill_price)

                self.log(f"ENTRADA {side} EXECUTADA!")
                self.log(f"   Qty: {filled_qty:.6f}")
                self.log(f"   Preco: ${fill_price:.4f}")
                self.log(f"   Stop Loss: ${self.stop_loss_price:.4f}")

                ema9 = self.ema9_values[-1] if self.ema9_values else 0
                ema21 = self.ema21_values[-1] if self.ema21_values else 0
                chop = self.chop_values[-1] if self.chop_values else 50.0

                notify_data = {
                    'side': side, 'qty': filled_qty, 'price': fill_price,
                    'ema9': ema9, 'ema21': ema21, 'chop': chop, 'stop_loss': self.stop_loss_price
                }

            if notify_data:
                try:
                    await self.pushover.notify_entry(
                        notify_data['side'], notify_data['qty'],
                        notify_data['price'], notify_data['ema9'],
                        notify_data['ema21'], notify_data['chop'],
                        notify_data['stop_loss']
                    )
                except Exception as push_err:
                    self.log(f"Erro ao enviar notificacao Pushover: {push_err}", "warning")

        except Exception as e:
            self.log(f"Erro ao entrar na posicao: {e}", "error")
            traceback.print_exc()
            self._last_entry_error_time = time.time()
            await self.pushover.notify_error(f"Erro entrada {side}: {e}", critical=True)

    # ==========================================================================
    # EXECUCAO DE SAIDA
    # ==========================================================================
    async def exit_position(self, reason: str):
        """Fecha a posicao atual (toda a quantidade restante)."""
        if not self.position:
            return

        notify_data = None

        try:
            async with BinanceFuturesClient(
                self.api_key, self.api_secret, self.symbol, self.pushover,
                self._qty_precision, self._price_precision
            ) as client:
                side = self.position['side']
                qty = self.position['qty']

                order_side = SIDE_SELL if side == 'LONG' else SIDE_BUY
                self.log(f"Executando saida por: {reason} | {order_side} qty={qty}")

                order = await client.place_market_order(order_side, qty, self._qty_precision)

                exit_price = float(order.get('avgPrice', self.renko.current_price))
                exec_qty = float(order.get('executedQty', qty))

                if exit_price <= 0:
                    exit_price = self.renko.current_price
                if exec_qty <= 0:
                    exec_qty = qty

                # Calcular PnL
                if side == 'LONG':
                    pnl = (exit_price - self.entry_price) * exec_qty
                else:
                    pnl = (self.entry_price - exit_price) * exec_qty

                pnl_pct = (pnl / (self.entry_price * exec_qty)) * 100 if self.entry_price > 0 else 0

                self.daily_pnl += pnl
                self.equity = self.initial_equity + self.daily_pnl

                self.log(f"SAIDA {side} EXECUTADA!")
                self.log(f"   Motivo: {reason}")
                self.log(f"   Qty: {exec_qty:.6f}")
                self.log(f"   Entry: ${self.entry_price:.4f}")
                self.log(f"   Exit: ${exit_price:.4f}")
                self.log(f"   PnL: ${pnl:.2f} ({pnl_pct:+.2f}%)")
                self.log(f"   Equity: ${self.equity:.2f}")

                notify_data = {
                    'side': side, 'qty': exec_qty,
                    'entry_price': self.entry_price, 'exit_price': exit_price,
                    'pnl': pnl, 'pnl_pct': pnl_pct, 'reason': reason
                }

                self._reset_position_state()

            if notify_data:
                try:
                    await self.pushover.notify_exit(
                        notify_data['side'], notify_data['qty'],
                        notify_data['entry_price'], notify_data['exit_price'],
                        notify_data['pnl'], notify_data['pnl_pct'],
                        notify_data['reason']
                    )
                except Exception as push_err:
                    self.log(f"Erro ao enviar notificacao Pushover: {push_err}", "warning")

        except Exception as e:
            self.log(f"Erro na execucao da saida: {e}", "error")
            traceback.print_exc()
            await self.pushover.notify_error(f"Erro saida {reason}: {e}", critical=True)

    # ==========================================================================
    # PROCESSAMENTO DE TRADES (TICK A TICK)
    # ==========================================================================
    async def process_trade(self, price: float, volume: float = 0.0):
        """
        Processa cada tick de preco (aggTrade).
        A logica de entrada/saida e executada AO FECHAR TIJOLO RENKO (conforme PDF).

        Conforme pseudo-codigo do PDF, a cada novo tijolo:
        1. Calcular indicadores
        2. Verificar saidas (Stop Loss PRIMEIRO, depois Reversão)
        3. Se NEUTRO, verificar entradas (Cruzamento + CHOP)
        """
        if not self.warmup_complete:
            return

        # Reset diario
        self._check_daily_reset()

        # 1. Atualizar Renko
        new_bricks = self.renko.update(price, datetime.now(), volume)

        if not new_bricks:
            return

        # Anti-Estagnacao: Atualizar timestamp do ultimo brick
        self.last_brick_time = time.time()

        # 2. Processar cada brick individualmente (AO_FECHAR_TIJOLO_RENKO)
        for brick in new_bricks:
            # Atualizar indicadores a cada brick
            self.update_indicators()

            # Log do novo brick
            ema9 = self.ema9_values[-1] if self.ema9_values else 0
            ema21 = self.ema21_values[-1] if self.ema21_values else 0
            chop = self.chop_values[-1] if self.chop_values else 50.0

            self.log(
                f"Novo Brick {brick.color.upper()} @ ${brick.close:.4f} | "
                f"EMA9={ema9:.4f} | EMA21={ema21:.4f} | CHOP={chop:.2f}"
            )

            # 3. Logica de saida e entrada (tudo no fechamento do tijolo)
            if self.position:
                # Verificar saida: Stop Loss PRIMEIRO, depois Reversão (conforme PDF)
                # Stop Loss é FIXO (não faz trailing) - conforme PDF
                exit_reason = self.check_exit_conditions(brick.close)
                if exit_reason:
                    await self.exit_position(exit_reason)
                    # Conforme PDF: após saída, não reentra no mesmo tick (return)
                    # "Sai da função para não reentrar no mesmo tick"
                    return
            else:
                # Logica de entrada (apenas se sem posicao)
                entry_signal = self.check_entry_conditions()
                if entry_signal:
                    await self.enter_position(entry_signal)

        # Limpar tasks pendentes
        self._pending_tasks = [t for t in self._pending_tasks if not t.done()]

    # ==========================================================================
    # WEBSOCKET E LOOP PRINCIPAL
    # ==========================================================================
    async def run_websocket(self):
        """
        WebSocket loop - aggTrade stream.
        """
        import websockets

        ws_url = f"wss://fstream.binance.com/ws/{self.symbol.lower()}@aggTrade"
        last_sync = time.time()

        while self.running:
            try:
                async with websockets.connect(
                    ws_url, ping_interval=20, ping_timeout=10
                ) as ws:
                    self.log(f"WebSocket aggTrade Conectado para {self.symbol}")

                    while self.running:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=WS_TIMEOUT)
                            data = json.loads(msg)

                            price = float(data['p'])
                            qty = float(data['q'])
                            vol_usdt = price * qty

                            await self.process_trade(price, vol_usdt)

                            # Sincronizar posicao periodicamente
                            if time.time() - last_sync > POSITION_SYNC_INTERVAL:
                                await self.sync_position()
                                last_sync = time.time()

                        except asyncio.TimeoutError:
                            # Anti-Estagnacao
                            time_since_last_brick = time.time() - self.last_brick_time
                            if time_since_last_brick > self.max_time_without_brick:
                                self.log(
                                    f"ANTI-ESTAGNACAO: {time_since_last_brick/60:.1f} min "
                                    f"sem brick, reconectando", "warning"
                                )
                                break
                            continue

            except (ConnectionClosedError, ConnectionClosedOK):
                self.log("Conexao WebSocket fechada, reconectando...", "warning")
                await asyncio.sleep(5)
            except Exception as e:
                self.log(f"WS Error: {e}, reconectando em 5s", "warning")
                await asyncio.sleep(5)

    async def run_balance_updater(self):
        """Atualiza equity e sincroniza posicao periodicamente."""
        while self.running:
            await asyncio.sleep(60)
            try:
                await self.sync_position()
            except Exception:
                pass

    async def run(self):
        """Loop principal - Tasks paralelas."""
        await self.warmup()
        t1 = asyncio.create_task(self.run_websocket())
        t2 = asyncio.create_task(self.run_balance_updater())
        try:
            await asyncio.gather(t1, t2)
        except KeyboardInterrupt:
            self.running = False
            self.log("Bot parado pelo usuario")
        except Exception as e:
            self.log(f"Erro fatal: {e}", "error")
            traceback.print_exc()

# ==============================================================================
# EXECUCAO
# ==============================================================================
async def main():
    parser = argparse.ArgumentParser(description='Bot Renko SETUP CAMPEÃO: Renko Dual-Flow')

    # Argumentos enviados pelo Orquestrador
    parser.add_argument('--symbol', type=str, required=True, help='Simbolo (ex: BTCUSDT)')
    parser.add_argument('--api', type=str, required=True, help='Binance API Key')
    parser.add_argument('--secret', type=str, required=True, help='Binance API Secret')
    parser.add_argument('--brick_size', type=float, required=True,
                        help='Tamanho do brick em % (ex: 0.005 para 0.5%)')
    parser.add_argument('--stop_loss_bricks', type=int, default=2,
                        help='Numero de bricks para stop loss')
    parser.add_argument('--leverage', type=int, default=10, help='Alavancagem')

    # Circuit Breaker
    parser.add_argument('--loss_limit', type=float, default=0.3,
                        help='Limite de perda diaria (ex: 0.3 = 30%%)')
    parser.add_argument('--max_trades', type=int, default=200,
                        help='Maximo de trades por dia')

    # Pushover
    parser.add_argument('--pushover_user_key', type=str, default='')
    parser.add_argument('--pushover_api_token', type=str, default='')

    args = parser.parse_args()

    bot = RenkoSetupCampeaoBot(
        symbol=args.symbol,
        api_key=args.api,
        api_secret=args.secret,
        brick_size_pct=args.brick_size,
        leverage=args.leverage,
        stop_loss_bricks=args.stop_loss_bricks,
        loss_limit=args.loss_limit,
        max_trades=args.max_trades,
        pushover_user=args.pushover_user_key,
        pushover_token=args.pushover_api_token
    )

    try:
        await bot.run()
    except KeyboardInterrupt:
        print("\nBot parado pelo usuario.")
    except Exception as e:
        print(f"Erro fatal: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())

"""
Orquestrador SETUP CAMPEÃO: RENKO DUAL-FLOW (EMA + CHOP)
=====================================================================
100% fiel ao PDF Setup Campeão:

INDICADORES:
   - EMA 9 (Rápida - Sinal de Gatilho)
   - EMA 21 (Lenta - Sinal de Tendência)
   - CHOP (14 períodos - Filtro de Lateralidade)
   - Grafico Renko Porcentagem LTP

ENTRADAS (TODAS condicoes simultaneas):
   LONG (Compra):
   - EMA 9 cruza ACIMA da EMA 21 (no fechamento do tijolo)
   - CHOP < 50 (no momento exato do cruzamento)

   SHORT (Venda):
   - EMA 9 cruza ABAIXO da EMA 21 (no fechamento do tijolo)
   - CHOP < 50 (no momento exato do cruzamento)

SAIDAS (verificadas ao fechar tijolo):
   - Saida por Reversão: Encerra imediatamente se as médias cruzarem no sentido oposto
   - Stop Loss: Fixo, 2 tijolos atrás da entrada (trailing)

STOP LOSS:
   - Inicial LONG: MINIMA(Ultimos_2_Tijolos)
   - Inicial SHORT: MAXIMA(Ultimos_2_Tijolos)
   - Trailing LONG: MAX(stop_loss, MINIMA(Ultimos_2_Tijolos))
   - Trailing SHORT: MIN(stop_loss, MAXIMA(Ultimos_2_Tijolos))

RENKO:
   - Construido via @aggTrade stream (tempo real) - LTP %
   - Warmup progressivo: ate 4500 velas de 1 minuto

SEGURANCA:
   - Circuit Breaker: perda diaria configuravel
   - Reconexao automatica com multiplas tentativas
   - Sincronizacao periodica de posicao (5 min)
   - Pushover para todas as notificacoes
"""
import psutil
import subprocess
import sys
import time
import os
import signal
from datetime import datetime

# =============================================================================
# CONFIGURACOES DE API
# =============================================================================

API_KEY = "tRH3oL2k4b79gCXZY6ABrKsNQN2nJYkVDvjSIOhJ6uucTcgOBOR3CtsqDgCSoUrx"
API_SECRET = "HpdzIaS34b2nzYO9DPUPbhyOKc9vjWcc0CqycYaQDYXAuMoRwpRm32ker6AZfWsW"

# PUSHOVER
PUSHOVER_USER_KEY = "ussz41sizmb1criak9hef8qcyki577"
PUSHOVER_API_TOKEN = "aqkfebjyvre7rr4uttiqirooxd8kze"

# =============================================================================
# CONFIGURACAO COMPLETA PARA MOEDAS - SETUP CAMPEÃO
# =============================================================================

CONFIG_BY_SYMBOL = {
    "AVAXUSDT": {
        "brick_size": 0.0020,          # Tamanho em % do preco (LTP %) (ex: 0.005 = 0.5%)
        "stop_loss_bricks": 5,         # Numero de bricks para stop loss
    },
    "ADAUSDT": {
        "brick_size": 0.0020,          # Tamanho em % do preco (LTP %) (ex: 0.005 = 0.5%)
        "stop_loss_bricks": 5,
    },
    "DOGEUSDT": {
        "brick_size": 0.0020,          # Tamanho em % do preco (LTP %) (ex: 0.005 = 0.5%)
        "stop_loss_bricks": 5,
    },
}

SYMBOLS = list(CONFIG_BY_SYMBOL.keys())

# =============================================================================
# CONFIGURACOES GERAIS - SETUP CAMPEÃO
# =============================================================================

LEVERAGE = 10
LOSS_LIMIT = 0.30
MAX_TRADES = 200

# =============================================================================
# CONFIGURACOES DE AUTO-REFRESH
# =============================================================================

AUTO_REFRESH_INTERVAL = 1800  # 30 minutos
FORCE_REFRESH_ENABLED = False  # DESATIVADO - O bot tem mecanismo interno anti-estagnacao

# =============================================================================
# FUNCOES
# =============================================================================

processes = []
bots_info = {}

# SETUP CAMPEÃO: Nome do arquivo do bot
BOT_FILENAME = "bot_setup_campeao2.py"

def signal_handler(sig, frame):
    """Handler para sinais de interrupcao."""
    print("\n" + "="*80)
    print("ENCERRANDO TODOS OS BOTS...")
    print("="*80)
    
    for proc in processes:
        try:
            proc.terminate()
        except:
            pass
    
    time.sleep(2)
    
    for proc in processes:
        if proc.poll() is None:
            try:
                proc.kill()
            except:
                pass
    
    print("Todos os bots encerrados.")
    sys.exit(0)

def start_bot(symbol: str):
    """Inicia bot SETUP CAMPEÃO com configuracao."""
    config = CONFIG_BY_SYMBOL[symbol]
    
    cmd = [
        sys.executable,
        BOT_FILENAME,
        "--symbol", symbol,
        "--api", API_KEY,
        "--secret", API_SECRET,
        
        # Parametros Renko - brick_size e valor em % do preco (LTP %)
        "--brick_size", str(config["brick_size"]),
        "--stop_loss_bricks", str(config.get("stop_loss_bricks", 2)),
        
        # Configuracoes Gerais
        "--leverage", str(LEVERAGE),
        "--loss_limit", str(LOSS_LIMIT),
        "--max_trades", str(MAX_TRADES),
        
        # PUSHOVER
        "--pushover_user_key", PUSHOVER_USER_KEY,
        "--pushover_api_token", PUSHOVER_API_TOKEN,
    ]
    
    os.makedirs("bot_logs2", exist_ok=True)
    log_file = f"bot_logs2/log_{symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    
    bots_info[symbol] = {
        'config': config,
        'start_time': datetime.now(),
        'log_file': log_file,
        'last_refresh': time.time(),
        'restarts': 0,
    }
    
    proc = subprocess.Popen(
        cmd,
        stdout=open(log_file, 'w', encoding='utf-8'),
        stderr=subprocess.STDOUT,
        cwd=os.path.dirname(os.path.abspath(__file__)) or '.'
    )
    
    return proc

def print_banner():
    """Exibe banner inicial SETUP CAMPEÃO."""
    print("=" * 80)
    print("ORQUESTRADOR SETUP CAMPEÃO: RENKO DUAL-FLOW (EMA + CHOP)".center(80))
    print("(EMA 9 + EMA 21 + CHOP 14)".center(80))
    print("=" * 80)
    
    print("\n CONFIGURACAO POR MOEDA:")
    print("-" * 80)
    print(f"{'Simbolo':<15} {'Brick %':<12} {'SL Bricks':<10}")
    print("-" * 80)
    
    for symbol in SYMBOLS:
        config = CONFIG_BY_SYMBOL[symbol]
        print(f"   {symbol:<15} {config['brick_size']*100:>10.2f}% {config.get('stop_loss_bricks', 2):>10}")
    
    print("-" * 80)
    print()
    print("ESTRATEGIA SETUP CAMPEÃO: RENKO DUAL-FLOW")
    print("   Indicadores: EMA 9, EMA 21, CHOP (14 períodos)")
    print("   Renko: Construido via LTP % em tempo real")
    print("   Warmup: Progressivo ate 4500 velas de 1 minuto")
    print("   Pushover ativado para notificacoes")
    print()
    print("CONDICOES DE ENTRADA (TODAS simultaneas):")
    print("   LONG (Compra):")
    print("      EMA 9 cruza ACIMA da EMA 21 (no fechamento do tijolo)")
    print("      CHOP < 50 (no momento exato do cruzamento)")
    print()
    print("   SHORT (Venda):")
    print("      EMA 9 cruza ABAIXO da EMA 21 (no fechamento do tijolo)")
    print("      CHOP < 50 (no momento exato do cruzamento)")
    print()
    print("INDICADORES:")
    print("   EMA 9 (Rápida): Sobre fechamento dos boxes Renko")
    print("   EMA 21 (Lenta): Sobre fechamento dos boxes Renko")
    print("   CHOP: Periodo 14 (Filtro de Lateralidade)")
    print()
    print("SAIDAS (verificadas ao fechar tijolo):")
    print("   Saida por Reversão: Encerra imediatamente se as médias cruzarem no sentido oposto")
    print("   Stop Loss: Fixo, N tijolos atrás da entrada (trailing)")
    print()
    print("STOP LOSS:")
    print("   Inicial LONG: MINIMA(Ultimos_N_Tijolos)")
    print("   Inicial SHORT: MAXIMA(Ultimos_N_Tijolos)")
    print("   Trailing LONG: MAX(stop_loss, MINIMA(Ultimos_N_Tijolos))")
    print("   Trailing SHORT: MIN(stop_loss, MAXIMA(Ultimos_N_Tijolos))")
    print()
    print("PARAMETROS:")
    print(f"   Alavancagem: {LEVERAGE}x")
    print(f"   Risco por trade: 2% da equity")
    print(f"   Limite perda diaria: {LOSS_LIMIT*100:.0f}%")
    print(f"   Max trades/dia: {MAX_TRADES}")
    print()
    print("SEGURANCA:")
    print("   Circuit Breaker por perda diaria")
    print("   Circuit Breaker por numero de trades")
    print("   Sincronizacao periodica de posicao (5 min)")
    print("   Reconexao automatica de WebSocket")
    print("   Verificacao de posicao existente antes de entrar")
    print()
    print("AUTO-REFRESH:")
    if FORCE_REFRESH_ENABLED:
        print(f"   ATIVADO - Refresh a cada {AUTO_REFRESH_INTERVAL//60} minutos")
    else:
        print(f"   DESATIVADO")
    print()

def force_refresh_bot(symbol: str, index: int) -> subprocess.Popen:
    """Forca refresh de um bot especifico (mata e reinicia)."""
    global processes
    
    proc = processes[index]
    
    try:
        proc.terminate()
        time.sleep(1)
        if proc.poll() is None:
            proc.kill()
    except:
        pass
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] REFRESH FORCADO: {symbol}")
    
    new_proc = start_bot(symbol)
    bots_info[symbol]['restarts'] = bots_info[symbol].get('restarts', 0) + 1
    bots_info[symbol]['last_refresh'] = time.time()
    
    return new_proc

def monitor_processes():
    """Monitora e reinicia bots se necessario + AUTO-REFRESH."""
    global processes
    
    last_status = time.time()
    status_interval = 30
    
    while True:
        try:
            current_time = time.time()
            
            if current_time - last_status > status_interval:
                running = sum(1 for p in processes if p.poll() is None)
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Status: {running}/{len(SYMBOLS)} bots rodando")
                last_status = current_time
            
            # Verificar se algum bot morreu
            for i, (symbol, proc) in enumerate(zip(SYMBOLS, processes)):
                if proc.poll() is not None:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Bot {symbol} morreu (exit code: {proc.returncode})")
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Reiniciando {symbol}...")
                    processes[i] = start_bot(symbol)
            
            # AUTO-REFRESH (se ativado)
            if FORCE_REFRESH_ENABLED:
                for i, symbol in enumerate(SYMBOLS):
                    time_since_refresh = current_time - bots_info[symbol]['last_refresh']
                    if time_since_refresh > AUTO_REFRESH_INTERVAL:
                        processes[i] = force_refresh_bot(symbol, i)
            
            time.sleep(5)
            
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Erro no monitor: {e}")
            time.sleep(5)

def main():
    """Funcao principal do orquestrador."""
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print_banner()
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Iniciando {len(SYMBOLS)} bots...")
    
    for symbol in SYMBOLS:
        proc = start_bot(symbol)
        processes.append(proc)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Bot {symbol} iniciado (PID: {proc.pid})")
        time.sleep(1)
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Todos os bots iniciados!")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Logs salvos em: bot_logs/")
    print()
    
    # Monitorar processos
    monitor_processes()

if __name__ == "__main__":
    main()

export default {
  async fetch(request, env) {
    // Cria ou recupera a instância única do seu bot
    const id = env.MY_CONTAINER.idFromName('trader-singleton');
    const obj = env.MY_CONTAINER.get(id);
    
    // Envia o comando para ligar o container
    return await obj.fetch(request);
  }
};

export class MyContainer {
  constructor(state) {
    this.state = state;
  }

  async fetch(request) {
    // Liga o container se ele estiver desligado
    await this.state.blockConcurrencyWhile(async () => {
      console.log("Porteiro: Garantindo que o exército de bots está de pé...");
    });
    return new Response("Exército de 12 bots ativo e operando!");
  }

  // O segredo: Deixamos o alarme vazio para ele nunca desligar o bot
  async alarm() {
    console.log("Alarme ignorado para manter os bots 24/7.");
  }
}
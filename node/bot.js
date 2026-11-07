const zmq = require("zeromq");
const { criar, empacotar, desempacotar, formatar } = require("./mensagem");

const NOME = process.env.NOME || "bot_js";
const BROKER = process.env.BROKER || "tcp://broker:5555";
const INTERVALO = Number(process.env.INTERVALO || 3000);

const socket = new zmq.Request();

function dormir(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// Envia um pedido e devolve a resposta do servidor
async function pedir(tipo, dados) {
  const pedido = criar(tipo, NOME, dados);
  console.log(formatar(`${NOME} ENVIOU`, pedido));
  await socket.send(empacotar(pedido));

  const [quadro] = await socket.receive();
  const resposta = desempacotar(quadro);
  console.log(formatar(`${NOME} RECEBEU`, resposta));
  return resposta;
}

// Em caso de erro, o login e repetido
async function login() {
  while (true) {
    const resposta = await pedir("login", { usuario: NOME });
    if (resposta.status === "ok") return;
    console.log(`${NOME} login falhou, tentando de novo`);
    await dormir(2000);
  }
}

function nomeAleatorio() {
  return `canal-${Math.floor(Math.random() * 1000)}`;
}

async function main() {
  await socket.connect(BROKER);
  console.log(`${NOME} conectado ao broker em ${BROKER}`);
  await dormir(1000);

  await login();

  while (true) {
    const resposta = await pedir("listar_canais", {});
    const canais = resposta.dados.canais || [];

    if (canais.length < 5) {
      await pedir("criar_canal", { canal: nomeAleatorio() });
    }

    await dormir(INTERVALO);
  }
}

main();
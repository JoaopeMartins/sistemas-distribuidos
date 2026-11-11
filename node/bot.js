const zmq = require("zeromq");
const { criar, empacotar, desempacotar, formatar } = require("./mensagem");

const NOME = process.env.NOME || "bot_js";
const BROKER = process.env.BROKER || "tcp://broker:5555";
const PROXY = process.env.PROXY || "tcp://proxy:5558";

const socket = new zmq.Request();
const inscritor = new zmq.Subscriber();

const inscritos = [];

const PALAVRAS = ["oi", "tudo bem", "bom dia", "alguem ai", "testando", "ate mais"];

function dormir(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function mensagemAleatoria() {
  const palavra = PALAVRAS[Math.floor(Math.random() * PALAVRAS.length)];
  return `${palavra} #${Math.floor(Math.random() * 1000)}`;
}

async function pedir(tipo, dados) {
  const pedido = criar(tipo, NOME, dados);
  console.log(formatar(`${NOME} ENVIOU`, pedido));
  await socket.send(empacotar(pedido));

  const [quadro] = await socket.receive();
  const resposta = desempacotar(quadro);
  console.log(formatar(`${NOME} RECEBEU`, resposta));
  return resposta;
}

async function login() {
  while (true) {
    const resposta = await pedir("login", { usuario: NOME });
    if (resposta.status === "ok") return;
    console.log(`${NOME} login falhou, tentando de novo`);
    await dormir(2000);
  }
}

// Escuta as publicacoes dos canais em que o bot esta inscrito
async function escutar() {
  for await (const [canal, bruto] of inscritor) {
    const publicacao = desempacotar(bruto);
    const enviado = new Date(publicacao.timestamp * 1000).toISOString();
    const recebido = new Date().toISOString();
    console.log(
      `[${NOME} RECEBEU PUBLICACAO] canal=${canal.toString()} ` +
        `autor=${publicacao.dados.autor} mensagem="${publicacao.dados.mensagem}" ` +
        `enviado=${enviado} recebido=${recebido}`
    );
  }
}

function inscrever(canal) {
  inscritor.subscribe(canal);
  inscritos.push(canal);
  console.log(`${NOME} inscrito no canal ${canal} (total: ${inscritos.length})`);
}

function nomeAleatorio() {
  return `canal-${Math.floor(Math.random() * 1000)}`;
}

async function main() {
  await inscritor.connect(PROXY);
  console.log(`${NOME} inscritor conectado ao proxy em ${PROXY}`);
  escutar();

  await socket.connect(BROKER);
  console.log(`${NOME} conectado ao broker em ${BROKER}`);
  await dormir(1000);

  await login();

  while (true) {
    const resposta = await pedir("listar_canais", {});
    const canais = resposta.dados.canais || [];

    if (canais.length < 5) {
      const novo = nomeAleatorio();
      const r = await pedir("criar_canal", { canal: novo });
      if (r.status === "ok") canais.push(novo);
    }

    // Inscricao aleatoria, ate 3 canais
    const livres = canais.filter((c) => !inscritos.includes(c));
    if (inscritos.length < 3 && livres.length > 0) {
      inscrever(livres[Math.floor(Math.random() * livres.length)]);
      await dormir(500);
    }

    if (canais.length > 0) {
      const canal = canais[Math.floor(Math.random() * canais.length)];
      for (let i = 0; i < 10; i++) {
        await pedir("publicar", { canal, mensagem: mensagemAleatoria() });
        await dormir(1000);
      }
    } else {
      await dormir(1000);
    }
  }
}

main();
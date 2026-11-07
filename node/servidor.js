const zmq = require("zeromq");
const { criar, empacotar, desempacotar, formatar } = require("./mensagem");
const { Armazenamento } = require("./armazenamento");

const NOME = process.env.NOME || "servidor_js";
const BROKER = process.env.BROKER || "tcp://broker:5556";

const REGRA_CANAL = /^[a-z0-9_-]{3,20}$/;

const disco = new Armazenamento(NOME);
const canais = disco.carregarCanais();
const usuarios = new Set();

function tratar(pedido) {
  const { tipo, dados } = pedido;

  if (tipo === "login") {
    const usuario = (dados.usuario || "").trim();
    if (usuario === "") {
      return criar("login_resp", NOME, { mensagem: "nome de usuario vazio" }, "erro");
    }
    usuarios.add(usuario);
    disco.gravar("login", { usuario });
    return criar("login_resp", NOME, { mensagem: `bem-vindo, ${usuario}` }, "ok");
  }

  if (tipo === "criar_canal") {
    const canal = (dados.canal || "").trim();
    if (!REGRA_CANAL.test(canal)) {
      return criar("criar_canal_resp", NOME, { mensagem: "nome de canal invalido" }, "erro");
    }
    if (canais.includes(canal)) {
      return criar("criar_canal_resp", NOME, { mensagem: "canal ja existe" }, "erro");
    }
    canais.push(canal);
    disco.gravar("canal_criado", { canal, criado_por: pedido.origem });
    return criar("criar_canal_resp", NOME, { mensagem: `canal ${canal} criado` }, "ok");
  }

  if (tipo === "listar_canais") {
    return criar("listar_canais_resp", NOME, { canais }, "ok");
  }

  return criar("erro", NOME, { mensagem: `tipo desconhecido: ${tipo}` }, "erro");
}

async function main() {
  const socket = new zmq.Reply();
  await socket.connect(BROKER);
  console.log(`${NOME} conectado ao broker em ${BROKER}`);
  console.log(`${NOME} canais recuperados do disco: ${JSON.stringify(canais)}`);

  for await (const [quadro] of socket) {
    const pedido = desempacotar(quadro);
    console.log(formatar(`${NOME} RECEBEU`, pedido));

    const resposta = tratar(pedido);
    console.log(formatar(`${NOME} ENVIOU`, resposta));

    await socket.send(empacotar(resposta));
  }
}

main();
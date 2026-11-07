const zmq = require("zeromq");
const { criar, empacotar, desempacotar, formatar } = require("./mensagem");
const { Armazenamento } = require("./armazenamento");

const NOME = process.env.NOME || "servidor_js";
const BROKER = process.env.BROKER || "tcp://broker:5556";
const PROXY = process.env.PROXY || "tcp://proxy:5557";

const REGRA_CANAL = /^[a-z0-9_-]{3,20}$/;

const disco = new Armazenamento(NOME);
const canais = disco.carregarCanais();
const usuarios = new Set();

const publicador = new zmq.Publisher();

// Publica no canal: frame 1 e o topico em texto, frame 2 o envelope binario
async function publicar(canal, envelope) {
  await publicador.send([canal, empacotar(envelope)]);
}

async function tratar(pedido) {
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

  if (tipo === "publicar") {
    const canal = (dados.canal || "").trim();
    const texto = dados.mensagem || "";

    if (!REGRA_CANAL.test(canal)) {
      return criar("publicar_resp", NOME, { mensagem: "nome de canal invalido" }, "erro");
    }
    if (texto === "") {
      return criar("publicar_resp", NOME, { mensagem: "mensagem vazia" }, "erro");
    }

    const publicacao = criar("publicacao", NOME, {
      canal,
      mensagem: texto,
      autor: pedido.origem,
    });

    await publicar(canal, publicacao);
    console.log(formatar(`${NOME} PUBLICOU em ${canal}`, publicacao));

    disco.gravar("publicacao", { canal, mensagem: texto, autor: pedido.origem });
    return criar("publicar_resp", NOME, { mensagem: `publicado em ${canal}` }, "ok");
  }

  return criar("erro", NOME, { mensagem: `tipo desconhecido: ${tipo}` }, "erro");
}

async function main() {
  await publicador.connect(PROXY);
  console.log(`${NOME} publicador conectado ao proxy em ${PROXY}`);

  const socket = new zmq.Reply();
  await socket.connect(BROKER);
  console.log(`${NOME} conectado ao broker em ${BROKER}`);
  console.log(`${NOME} canais recuperados do disco: ${JSON.stringify(canais)}`);

  for await (const [quadro] of socket) {
    const pedido = desempacotar(quadro);
    console.log(formatar(`${NOME} RECEBEU`, pedido));

    const resposta = await tratar(pedido);
    console.log(formatar(`${NOME} ENVIOU`, resposta));

    await socket.send(empacotar(resposta));
  }
}

main();
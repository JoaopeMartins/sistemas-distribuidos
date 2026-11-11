const zmq = require("zeromq");
const { criar, empacotar, desempacotar, formatar } = require("./mensagem");
const { Armazenamento } = require("./armazenamento");

const NOME = process.env.NOME || "servidor_js";
const BROKER = process.env.BROKER || "tcp://broker:5556";
const PROXY_PUB = process.env.PROXY || "tcp://proxy:5557";
const PROXY_SUB = process.env.PROXY_SUB || "tcp://proxy:5558";
const REFERENCIA = process.env.REFERENCIA || "tcp://referencia:5559";
const PORTA_PARES = process.env.PORTA_PARES || "5560";

const TEMPO_LIMITE = 2000; // ms de espera por resposta de outro servidor
const TOPICO_SERVIDORES = "servers";

const REGRA_CANAL = /^[a-z0-9_-]{3,20}$/;

const disco = new Armazenamento(NOME);
const canais = disco.carregarCanais();
const usuarios = new Set();

let rank = null;
let coordenador = null;
let recebidas = 0;
let ajuste = 0;

const publicador = new zmq.Publisher();
const inscritor = new zmq.Subscriber();
const refSocket = new zmq.Request();
const clientes = new zmq.Reply();
const pares = new zmq.Reply();

function dormir(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function horaCorrigida() {
  return Date.now() / 1000 + ajuste;
}

async function publicar(canal, envelope) {
  await publicador.send([canal, empacotar(envelope)]);
}

async function perguntarReferencia(tipo, dados) {
  const pedido = criar(tipo, NOME, dados);
  console.log(formatar(`${NOME} ENVIOU A REFERENCIA`, pedido));
  await refSocket.send(empacotar(pedido));

  const [quadro] = await refSocket.receive();
  const resposta = desempacotar(quadro);
  console.log(formatar(`${NOME} RECEBEU DA REFERENCIA`, resposta));
  return resposta;
}

// Pergunta a outro servidor. Devolve null se nao responder a tempo.
// Usa um socket novo a cada chamada: o REQ fica em estado invalido
// quando um receive expira.
async function perguntarPar(servidor, tipo, dados) {
  const socket = new zmq.Request();
  socket.linger = 0;
  socket.receiveTimeout = TEMPO_LIMITE;
  socket.connect(`tcp://${servidor}:${PORTA_PARES}`);

  try {
    const pedido = criar(tipo, NOME, dados);
    console.log(formatar(`${NOME} ENVIOU A ${servidor}`, pedido));
    await socket.send(empacotar(pedido));

    const [quadro] = await socket.receive();
    const resposta = desempacotar(quadro);
    console.log(formatar(`${NOME} RECEBEU DE ${servidor}`, resposta));
    return resposta;
  } catch (erro) {
    console.log(`${NOME} nao obteve resposta de ${servidor}`);
    return null;
  } finally {
    socket.close();
  }
}

async function listarServidores() {
  const resposta = await perguntarReferencia("list", {});
  if (resposta.status !== "ok") return [];
  return resposta.dados.servidores;
}

// Publica no topico servers que este servidor foi eleito
async function anunciarCoordenador() {
  const aviso = criar("coordenador", NOME, { coordenador: NOME });
  await publicar(TOPICO_SERVIDORES, aviso);
  console.log(formatar(`${NOME} ANUNCIOU coordenador`, aviso));
}

// Algoritmo do valentao: quem tem o maior rank vence
async function iniciarEleicao() {
  if (coordenador === NOME) {
    await anunciarCoordenador();
    return;
  }
  console.log(`${NOME} iniciando eleicao`);
  const lista = await listarServidores();
  const maiores = lista.filter((s) => s.rank > (rank || 0));

  let respondeu = false;
  for (const s of maiores) {
    const resposta = await perguntarPar(s.servidor, "eleicao", {});
    if (resposta !== null && resposta.status === "ok") respondeu = true;
  }

  if (respondeu) {
    // Alguem com rank maior assumiu; o anuncio chega pelo topico
    console.log(`${NOME} cedeu a eleicao a um servidor de rank maior`);
    return;
  }

  coordenador = NOME;
  console.log(`${NOME} venceu a eleicao e e o novo coordenador`);
  await anunciarCoordenador();
}

// Coordenador: media das horas de todos os servidores conhecidos
async function berkeley() {
  const horas = [horaCorrigida()];
  const lista = await listarServidores();

  for (const s of lista) {
    if (s.servidor === NOME) continue;
    const resposta = await perguntarPar(s.servidor, "hora_local", {});
    if (resposta !== null && resposta.status === "ok") {
      horas.push(resposta.dados.hora);
    }
  }

  const media = horas.reduce((a, b) => a + b, 0) / horas.length;
  ajuste = media - Date.now() / 1000;
  console.log(
    `${NOME} (coordenador) calculou media de ${horas.length} relogios, ` +
      `ajuste proprio de ${ajuste.toFixed(6)}s`
  );
  return media;
}

// A cada 15 mensagens: pede a hora ao coordenador, ou faz Berkeley
async function sincronizarRelogio() {
  if (coordenador === null) {
    await iniciarEleicao();
    return;
  }

  if (coordenador === NOME) {
    await berkeley();
    return;
  }

  const resposta = await perguntarPar(coordenador, "hora", {});
  if (resposta === null || resposta.status !== "ok") {
    console.log(`${NOME} nao alcancou o coordenador ${coordenador}`);
    coordenador = null;
    await iniciarEleicao();
    return;
  }

  ajuste = resposta.dados.hora - Date.now() / 1000;
  console.log(
    `${NOME} ajustou o relogio em ${ajuste.toFixed(6)}s pelo coordenador ${coordenador}`
  );
}

async function heartbeat() {
  await perguntarReferencia("heartbeat", { servidor: NOME });
}

async function tratarCliente(pedido) {
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

// Pedidos vindos de outros servidores
async function tratarPar(pedido) {
  const { tipo } = pedido;

  if (tipo === "eleicao") {
    return criar("eleicao_resp", NOME, { mensagem: "OK" }, "ok");
  }

  if (tipo === "hora_local") {
    return criar("hora_local_resp", NOME, { hora: horaCorrigida() }, "ok");
  }

  if (tipo === "hora") {
    if (coordenador !== NOME) {
      return criar("hora_resp", NOME, { mensagem: "nao sou o coordenador" }, "erro");
    }
    return criar("hora_resp", NOME, { hora: horaCorrigida() }, "ok");
  }

  return criar("erro", NOME, { mensagem: `tipo desconhecido: ${tipo}` }, "erro");
}

// Cada socket roda no seu proprio laco assincrono
async function escutarTopico() {
  for await (const [, bruto] of inscritor) {
    const aviso = desempacotar(bruto);
    if (aviso.tipo === "coordenador") {
      coordenador = aviso.dados.coordenador;
      console.log(`${NOME} reconhece ${coordenador} como coordenador`);
    }
  }
}

async function escutarPares() {
  for await (const [quadro] of pares) {
    const pedido = desempacotar(quadro);
    console.log(formatar(`${NOME} RECEBEU DE UM PAR`, pedido));

    const resposta = await tratarPar(pedido);
    console.log(formatar(`${NOME} RESPONDEU AO PAR`, resposta));
    await pares.send(empacotar(resposta));

    // Rank maior assume a eleicao, conforme o algoritmo do valentao
    if (pedido.tipo === "eleicao") iniciarEleicao();
  }
}

async function escutarClientes() {
  for await (const [quadro] of clientes) {
    const pedido = desempacotar(quadro);
    console.log(formatar(`${NOME} RECEBEU`, pedido));

    const resposta = await tratarCliente(pedido);
    console.log(formatar(`${NOME} ENVIOU`, resposta));

    await clientes.send(empacotar(resposta));

    recebidas += 1;
    if (recebidas % 10 === 0) {
      console.log(`${NOME} atingiu ${recebidas} mensagens, enviando heartbeat`);
      await heartbeat();
    }
    if (recebidas % 15 === 0) {
      console.log(`${NOME} atingiu ${recebidas} mensagens, sincronizando relogio`);
      await sincronizarRelogio();
    }
  }
}

async function main() {
  await pares.bind(`tcp://*:${PORTA_PARES}`);
  console.log(`${NOME} escutando pares em ${PORTA_PARES}`);

  await refSocket.connect(REFERENCIA);
  console.log(`${NOME} conectado a referencia em ${REFERENCIA}`);

  const resposta = await perguntarReferencia("rank", { servidor: NOME });
  if (resposta.status === "ok") {
    rank = resposta.dados.rank;
    console.log(`${NOME} recebeu rank ${rank}`);
  }

  await publicador.connect(PROXY_PUB);
  console.log(`${NOME} publicador conectado ao proxy em ${PROXY_PUB}`);

  await inscritor.connect(PROXY_SUB);
  inscritor.subscribe(TOPICO_SERVIDORES);

  await clientes.connect(BROKER);
  console.log(`${NOME} conectado ao broker em ${BROKER}`);
  console.log(`${NOME} canais recuperados do disco: ${JSON.stringify(canais)}`);

  escutarTopico();
  escutarPares();
  escutarClientes();

  // Elege um coordenador assim que os outros ja tiveram tempo de subir
  await dormir(3000);
  await iniciarEleicao();
}

main();
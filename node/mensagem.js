const { encode, decode } = require("@msgpack/msgpack");

// Relogio logico de Lamport, um por processo
let relogio = 0;

// Regra 1: incrementa antes de cada envio
function tique() {
  relogio += 1;
  return relogio;
}

// Regra 2: ao receber, usa o maximo entre o proprio e o recebido
function sincronizar(recebido) {
  relogio = Math.max(relogio, recebido || 0);
  return relogio;
}

function valorRelogio() {
  return relogio;
}

// Monta o envelope padrao do projeto.
// timestamp em SEGUNDOS (Date.now() devolve milissegundos)
function criar(tipo, origem, dados = {}, status = null) {
  const envelope = {
    tipo,
    origem,
    timestamp: Date.now() / 1000,
    relogio: tique(),
    dados,
  };
  if (status !== null) envelope.status = status;
  return envelope;
}

function empacotar(envelope) {
  return Buffer.from(encode(envelope));
}

// Ao desempacotar, o relogio local ja e sincronizado
function desempacotar(buffer) {
  const envelope = decode(buffer);
  sincronizar(envelope.relogio);
  return envelope;
}

// Texto legivel para o log exigido pelo enunciado
function formatar(prefixo, envelope) {
  const hora = new Date(envelope.timestamp * 1000).toISOString();
  const status = envelope.status ? ` status=${envelope.status}` : "";
  return `[${prefixo}] ${envelope.tipo} origem=${envelope.origem} ts=${hora} relogio=${envelope.relogio} local=${relogio}${status} dados=${JSON.stringify(envelope.dados)}`;
}

module.exports = { criar, empacotar, desempacotar, formatar, valorRelogio };
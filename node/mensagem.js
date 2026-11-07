const { encode, decode } = require("@msgpack/msgpack");

// Monta o envelope padrao do projeto.
// timestamp em SEGUNDOS (Date.now() devolve milissegundos)
function criar(tipo, origem, dados = {}, status = null, relogio = 0) {
  const envelope = {
    tipo,
    origem,
    timestamp: Date.now() / 1000,
    relogio,
    dados,
  };
  if (status !== null) envelope.status = status;
  return envelope;
}

function empacotar(envelope) {
  return Buffer.from(encode(envelope));
}

function desempacotar(buffer) {
  return decode(buffer);
}

// Texto legivel para o log exigido pelo enunciado
function formatar(prefixo, envelope) {
  const hora = new Date(envelope.timestamp * 1000).toISOString();
  const status = envelope.status ? ` status=${envelope.status}` : "";
  return `[${prefixo}] ${envelope.tipo} origem=${envelope.origem} ts=${hora} relogio=${envelope.relogio}${status} dados=${JSON.stringify(envelope.dados)}`;
}

module.exports = { criar, empacotar, desempacotar, formatar };
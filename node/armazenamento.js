const fs = require("fs");
const path = require("path");

const PASTA = "/dados";

class Armazenamento {
  constructor(nomeServidor) {
    this.caminho = path.join(PASTA, `${nomeServidor}.jsonl`);
    fs.mkdirSync(PASTA, { recursive: true });
    if (!fs.existsSync(this.caminho)) fs.writeFileSync(this.caminho, "");
  }

  // Grava um evento no fim do arquivo
  gravar(evento, conteudo, id = null) {
    const linha = JSON.stringify({
      id,
      evento,
      timestamp: Date.now() / 1000,
      conteudo,
    });
    fs.appendFileSync(this.caminho, linha + "\n");
  }

  // Grava um registro ja montado, vindo da replicacao
  gravarRegistro(registro) {
    fs.appendFileSync(this.caminho, JSON.stringify(registro) + "\n");
  }

  // Le todos os eventos ja gravados
  ler() {
    const texto = fs.readFileSync(this.caminho, "utf8");
    return texto
      .split("\n")
      .filter((l) => l.trim() !== "")
      .map((l) => JSON.parse(l));
  }

  // Reconstroi a lista de canais a partir do historico
  carregarCanais() {
    const canais = [];
    for (const registro of this.ler()) {
      if (registro.evento === "canal_criado") {
        const nome = registro.conteudo.canal;
        if (!canais.includes(nome)) canais.push(nome);
      }
    }
    return canais;
  }

  // Ids ja aplicados, para nao duplicar eventos replicados
  carregarIds() {
    const ids = new Set();
    for (const registro of this.ler()) {
      if (registro.id) ids.add(registro.id);
    }
    return ids;
  }
}

module.exports = { Armazenamento };
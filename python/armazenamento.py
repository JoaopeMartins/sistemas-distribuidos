import json
import os
import time

PASTA = "/dados"


class Armazenamento:
    def __init__(self, nome_servidor):
        self.caminho = os.path.join(PASTA, f"{nome_servidor}.jsonl")
        os.makedirs(PASTA, exist_ok=True)
        if not os.path.exists(self.caminho):
            open(self.caminho, "w").close()

    def gravar(self, evento, conteudo, id=None):
        """Grava um evento no fim do arquivo."""
        linha = json.dumps({
            "id": id,
            "evento": evento,
            "timestamp": time.time(),
            "conteudo": conteudo,
        })
        with open(self.caminho, "a") as arquivo:
            arquivo.write(linha + "\n")

    def gravar_registro(self, registro):
        """Grava um registro ja montado, vindo da replicacao."""
        with open(self.caminho, "a") as arquivo:
            arquivo.write(json.dumps(registro) + "\n")

    def ler(self):
        """Le todos os eventos ja gravados."""
        with open(self.caminho) as arquivo:
            return [json.loads(l) for l in arquivo if l.strip()]

    def carregar_canais(self):
        """Reconstroi a lista de canais a partir do historico."""
        canais = []
        for registro in self.ler():
            if registro["evento"] == "canal_criado":
                nome = registro["conteudo"]["canal"]
                if nome not in canais:
                    canais.append(nome)
        return canais

    def carregar_ids(self):
        """Ids ja aplicados, para nao duplicar eventos replicados."""
        return {r["id"] for r in self.ler() if r.get("id")}
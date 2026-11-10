import os
import time

import zmq

from mensagem import criar, empacotar, desempacotar, formatar

NOME = os.environ.get("NOME", "referencia")
PORTA = os.environ.get("PORTA", "5559")

# Tempo sem heartbeat para o servidor sair da lista
LIMITE = float(os.environ.get("LIMITE", 30))

# nome do servidor -> {"rank": int, "visto": timestamp}
servidores = {}
proximo_rank = 0


def limpar():
    """Remove servidores que nao enviaram heartbeat dentro do limite."""
    agora = time.time()
    mortos = [n for n, s in servidores.items() if agora - s["visto"] > LIMITE]
    for nome in mortos:
        del servidores[nome]
        print(f"{NOME} removeu {nome} da lista (sem heartbeat)", flush=True)


def tratar(pedido):
    tipo = pedido["tipo"]
    dados = pedido["dados"]

    limpar()

    if tipo == "rank":
        nome = dados.get("servidor", "").strip()
        if nome == "":
            return criar("rank_resp", NOME, {"mensagem": "nome vazio"}, "erro")

        # Sem repeticoes de nome: quem ja tem rank recebe o mesmo
        if nome not in servidores:
            global proximo_rank
            proximo_rank += 1
            servidores[nome] = {"rank": proximo_rank, "visto": time.time()}
            print(f"{NOME} registrou {nome} com rank {proximo_rank}", flush=True)
        else:
            servidores[nome]["visto"] = time.time()

        return criar("rank_resp", NOME, {"rank": servidores[nome]["rank"]}, "ok")

    if tipo == "list":
        lista = [{"servidor": n, "rank": s["rank"]} for n, s in servidores.items()]
        return criar("list_resp", NOME, {"servidores": lista}, "ok")

    if tipo == "heartbeat":
        nome = dados.get("servidor", "").strip()
        if nome not in servidores:
            return criar("heartbeat_resp", NOME, {"mensagem": "servidor nao registrado"}, "erro")

        servidores[nome]["visto"] = time.time()
        # Aproveita o heartbeat para devolver a hora correta
        return criar("heartbeat_resp", NOME, {"mensagem": "OK", "hora": time.time()}, "ok")

    return criar("erro", NOME, {"mensagem": f"tipo desconhecido: {tipo}"}, "erro")


context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind(f"tcp://*:{PORTA}")

print(f"{NOME} escutando em {PORTA} (limite de heartbeat: {LIMITE}s)", flush=True)

while True:
    pedido = desempacotar(socket.recv())
    print(formatar(f"{NOME} RECEBEU", pedido), flush=True)

    resposta = tratar(pedido)
    print(formatar(f"{NOME} ENVIOU", resposta), flush=True)

    socket.send(empacotar(resposta))
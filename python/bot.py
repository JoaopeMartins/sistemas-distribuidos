import os
import random
import time

import zmq

from mensagem import criar, empacotar, desempacotar, formatar

NOME = os.environ.get("NOME", "bot_py")
BROKER = os.environ.get("BROKER", "tcp://broker:5555")
INTERVALO = float(os.environ.get("INTERVALO", 3))

context = zmq.Context()
socket = context.socket(zmq.REQ)


def pedir(tipo, dados):
    """Envia um pedido e devolve a resposta do servidor."""
    pedido = criar(tipo, NOME, dados)
    print(formatar(f"{NOME} ENVIOU", pedido), flush=True)
    socket.send(empacotar(pedido))

    resposta = desempacotar(socket.recv())
    print(formatar(f"{NOME} RECEBEU", resposta), flush=True)
    return resposta


def login():
    """Em caso de erro, o login e repetido."""
    while True:
        resposta = pedir("login", {"usuario": NOME})
        if resposta["status"] == "ok":
            return
        print(f"{NOME} login falhou, tentando de novo", flush=True)
        time.sleep(2)


def nome_aleatorio():
    return f"canal-{random.randint(0, 999)}"


socket.connect(BROKER)
print(f"{NOME} conectado ao broker em {BROKER}", flush=True)
time.sleep(1)

login()

while True:
    resposta = pedir("listar_canais", {})
    canais = resposta["dados"].get("canais", [])

    if len(canais) < 5:
        pedir("criar_canal", {"canal": nome_aleatorio()})

    time.sleep(INTERVALO)
import os
import random
import threading
import time
from datetime import datetime, timezone

import zmq

from mensagem import criar, empacotar, desempacotar, formatar

NOME = os.environ.get("NOME", "bot_py")
BROKER = os.environ.get("BROKER", "tcp://broker:5555")
PROXY = os.environ.get("PROXY", "tcp://proxy:5558")

PALAVRAS = ["oi", "tudo bem", "bom dia", "alguem ai", "testando", "ate mais"]

context = zmq.Context()
socket = context.socket(zmq.REQ)
inscritor = context.socket(zmq.SUB)

inscritos = []


def mensagem_aleatoria():
    return f"{random.choice(PALAVRAS)} #{random.randint(0, 999)}"


def pedir(tipo, dados):
    pedido = criar(tipo, NOME, dados)
    print(formatar(f"{NOME} ENVIOU", pedido), flush=True)
    socket.send(empacotar(pedido))

    resposta = desempacotar(socket.recv())
    print(formatar(f"{NOME} RECEBEU", resposta), flush=True)
    return resposta


def login():
    while True:
        resposta = pedir("login", {"usuario": NOME})
        if resposta["status"] == "ok":
            return
        print(f"{NOME} login falhou, tentando de novo", flush=True)
        time.sleep(2)


def escutar():
    """Escuta as publicacoes dos canais em que o bot esta inscrito."""
    while True:
        canal, bruto = inscritor.recv_multipart()
        publicacao = desempacotar(bruto)
        enviado = datetime.fromtimestamp(publicacao["timestamp"], timezone.utc).isoformat()
        recebido = datetime.now(timezone.utc).isoformat()
        print(
            f"[{NOME} RECEBEU PUBLICACAO] canal={canal.decode()} "
            f"autor={publicacao['dados']['autor']} "
            f"mensagem=\"{publicacao['dados']['mensagem']}\" "
            f"enviado={enviado} recebido={recebido}",
            flush=True,
        )


def inscrever(canal):
    inscritor.setsockopt_string(zmq.SUBSCRIBE, canal)
    inscritos.append(canal)
    print(f"{NOME} inscrito no canal {canal} (total: {len(inscritos)})", flush=True)


inscritor.connect(PROXY)
print(f"{NOME} inscritor conectado ao proxy em {PROXY}", flush=True)
threading.Thread(target=escutar, daemon=True).start()

socket.connect(BROKER)
print(f"{NOME} conectado ao broker em {BROKER}", flush=True)
time.sleep(1)

login()

while True:
    resposta = pedir("listar_canais", {})
    canais = resposta["dados"].get("canais", [])

    if len(canais) < 5:
        novo = f"canal-{random.randint(0, 999)}"
        r = pedir("criar_canal", {"canal": novo})
        if r["status"] == "ok":
            canais.append(novo)

    # Inscricao aleatoria, ate 3 canais
    livres = [c for c in canais if c not in inscritos]
    if len(inscritos) < 3 and livres:
        inscrever(random.choice(livres))
        time.sleep(0.5)

    if canais:
        canal = random.choice(canais)
        for _ in range(10):
            pedir("publicar", {"canal": canal, "mensagem": mensagem_aleatoria()})
            time.sleep(1)
    else:
        time.sleep(1)
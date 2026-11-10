import os
import re
import time

import zmq

from mensagem import criar, empacotar, desempacotar, formatar
from armazenamento import Armazenamento

NOME = os.environ.get("NOME", "servidor_py")
BROKER = os.environ.get("BROKER", "tcp://broker:5556")
PROXY = os.environ.get("PROXY", "tcp://proxy:5557")
REFERENCIA = os.environ.get("REFERENCIA", "tcp://referencia:5559")

REGRA_CANAL = re.compile(r"^[a-z0-9_-]{3,20}$")

disco = Armazenamento(NOME)
canais = disco.carregar_canais()
usuarios = set()

rank = None
recebidas = 0

# Diferenca entre a hora do servico de referencia e a local
ajuste = 0.0

context = zmq.Context()

publicador = context.socket(zmq.PUB)
publicador.connect(PROXY)

ref_socket = context.socket(zmq.REQ)
ref_socket.connect(REFERENCIA)


def hora_corrigida():
    return time.time() + ajuste


def perguntar_referencia(tipo, dados):
    """Conversa com o servico de referencia e devolve a resposta."""
    pedido = criar(tipo, NOME, dados)
    print(formatar(f"{NOME} ENVIOU A REFERENCIA", pedido), flush=True)
    ref_socket.send(empacotar(pedido))

    resposta = desempacotar(ref_socket.recv())
    print(formatar(f"{NOME} RECEBEU DA REFERENCIA", resposta), flush=True)
    return resposta


def pedir_rank():
    global rank
    resposta = perguntar_referencia("rank", {"servidor": NOME})
    if resposta["status"] == "ok":
        rank = resposta["dados"]["rank"]
        print(f"{NOME} recebeu rank {rank}", flush=True)


def heartbeat():
    global ajuste
    resposta = perguntar_referencia("heartbeat", {"servidor": NOME})
    if resposta["status"] == "ok" and "hora" in resposta["dados"]:
        ajuste = resposta["dados"]["hora"] - time.time()
        print(f"{NOME} ajustou o relogio fisico em {ajuste:.6f}s", flush=True)

    # Aproveita para ver quem mais esta no ar
    lista = perguntar_referencia("list", {})
    if lista["status"] == "ok":
        nomes = [f"{s['servidor']}({s['rank']})" for s in lista["dados"]["servidores"]]
        print(f"{NOME} servidores conhecidos: {', '.join(nomes)}", flush=True)


def publicar(canal, envelope):
    """Frame 1 e o topico em texto, frame 2 o envelope binario."""
    publicador.send_multipart([canal.encode(), empacotar(envelope)])


def tratar(pedido):
    tipo = pedido["tipo"]
    dados = pedido["dados"]

    if tipo == "login":
        usuario = dados.get("usuario", "").strip()
        if usuario == "":
            return criar("login_resp", NOME, {"mensagem": "nome de usuario vazio"}, "erro")
        usuarios.add(usuario)
        disco.gravar("login", {"usuario": usuario})
        return criar("login_resp", NOME, {"mensagem": f"bem-vindo, {usuario}"}, "ok")

    if tipo == "criar_canal":
        canal = dados.get("canal", "").strip()
        if not REGRA_CANAL.match(canal):
            return criar("criar_canal_resp", NOME, {"mensagem": "nome de canal invalido"}, "erro")
        if canal in canais:
            return criar("criar_canal_resp", NOME, {"mensagem": "canal ja existe"}, "erro")
        canais.append(canal)
        disco.gravar("canal_criado", {"canal": canal, "criado_por": pedido["origem"]})
        return criar("criar_canal_resp", NOME, {"mensagem": f"canal {canal} criado"}, "ok")

    if tipo == "listar_canais":
        return criar("listar_canais_resp", NOME, {"canais": canais}, "ok")

    if tipo == "publicar":
        canal = dados.get("canal", "").strip()
        texto = dados.get("mensagem", "")

        if not REGRA_CANAL.match(canal):
            return criar("publicar_resp", NOME, {"mensagem": "nome de canal invalido"}, "erro")
        if texto == "":
            return criar("publicar_resp", NOME, {"mensagem": "mensagem vazia"}, "erro")

        publicacao = criar("publicacao", NOME, {
            "canal": canal,
            "mensagem": texto,
            "autor": pedido["origem"],
        })

        publicar(canal, publicacao)
        print(formatar(f"{NOME} PUBLICOU em {canal}", publicacao), flush=True)

        disco.gravar("publicacao", {"canal": canal, "mensagem": texto, "autor": pedido["origem"]})
        return criar("publicar_resp", NOME, {"mensagem": f"publicado em {canal}"}, "ok")

    return criar("erro", NOME, {"mensagem": f"tipo desconhecido: {tipo}"}, "erro")


print(f"{NOME} conectado a referencia em {REFERENCIA}", flush=True)
pedir_rank()

socket = context.socket(zmq.REP)
socket.connect(BROKER)

print(f"{NOME} publicador conectado ao proxy em {PROXY}", flush=True)
print(f"{NOME} conectado ao broker em {BROKER}", flush=True)
print(f"{NOME} canais recuperados do disco: {canais}", flush=True)

while True:
    pedido = desempacotar(socket.recv())
    print(formatar(f"{NOME} RECEBEU", pedido), flush=True)

    resposta = tratar(pedido)
    print(formatar(f"{NOME} ENVIOU", resposta), flush=True)

    socket.send(empacotar(resposta))

    # Heartbeat a cada 10 mensagens de clientes
    recebidas += 1
    if recebidas % 10 == 0:
        print(f"{NOME} atingiu {recebidas} mensagens, enviando heartbeat", flush=True)
        heartbeat()
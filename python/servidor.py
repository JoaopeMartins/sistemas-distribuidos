import os
import re

import zmq

from mensagem import criar, empacotar, desempacotar, formatar
from armazenamento import Armazenamento

NOME = os.environ.get("NOME", "servidor_py")
BROKER = os.environ.get("BROKER", "tcp://broker:5556")

REGRA_CANAL = re.compile(r"^[a-z0-9_-]{3,20}$")

disco = Armazenamento(NOME)
canais = disco.carregar_canais()
usuarios = set()


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

    return criar("erro", NOME, {"mensagem": f"tipo desconhecido: {tipo}"}, "erro")


context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect(BROKER)

print(f"{NOME} conectado ao broker em {BROKER}", flush=True)
print(f"{NOME} canais recuperados do disco: {canais}", flush=True)

while True:
    pedido = desempacotar(socket.recv())
    print(formatar(f"{NOME} RECEBEU", pedido), flush=True)

    resposta = tratar(pedido)
    print(formatar(f"{NOME} ENVIOU", resposta), flush=True)

    socket.send(empacotar(resposta))
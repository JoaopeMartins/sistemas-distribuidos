import os
import re
import time
import threading
import zmq

from mensagem import criar, empacotar, desempacotar, formatar
from armazenamento import Armazenamento

NOME = os.environ.get("NOME", "servidor_py")
BROKER = os.environ.get("BROKER", "tcp://broker:5556")
PROXY_PUB = os.environ.get("PROXY", "tcp://proxy:5557")
PROXY_SUB = os.environ.get("PROXY_SUB", "tcp://proxy:5558")
REFERENCIA = os.environ.get("REFERENCIA", "tcp://referencia:5559")
PORTA_PARES = os.environ.get("PORTA_PARES", "5560")

TEMPO_LIMITE = 2000  # ms de espera por resposta de outro servidor
TOPICO_SERVIDORES = "servers"

REGRA_CANAL = re.compile(r"^[a-z0-9_-]{3,20}$")

disco = Armazenamento(NOME)
canais = disco.carregar_canais()
usuarios = set()

rank = None
coordenador = None
recebidas = 0
ajuste = 0.0
precisa_eleger = False

context = zmq.Context()

publicador = context.socket(zmq.PUB)
publicador.connect(PROXY_PUB)

inscritor = context.socket(zmq.SUB)
inscritor.connect(PROXY_SUB)
inscritor.setsockopt_string(zmq.SUBSCRIBE, TOPICO_SERVIDORES)

ref_socket = context.socket(zmq.REQ)
ref_socket.connect(REFERENCIA)

clientes = context.socket(zmq.REP)
clientes.connect(BROKER)

pares = context.socket(zmq.REP)
pares.bind(f"tcp://*:{PORTA_PARES}")


def hora_corrigida():
    return time.time() + ajuste


def publicar(canal, envelope):
    publicador.send_multipart([canal.encode(), empacotar(envelope)])


def perguntar_referencia(tipo, dados):
    pedido = criar(tipo, NOME, dados)
    print(formatar(f"{NOME} ENVIOU A REFERENCIA", pedido), flush=True)
    ref_socket.send(empacotar(pedido))
    resposta = desempacotar(ref_socket.recv())
    print(formatar(f"{NOME} RECEBEU DA REFERENCIA", resposta), flush=True)
    return resposta


def perguntar_par(servidor, tipo, dados):
    """Pergunta a outro servidor. Devolve None se nao responder a tempo.

    Usa um socket novo a cada chamada: o REQ entra em estado invalido
    quando um recv expira, e descartar o socket e mais simples do que
    tentar recupera-lo.
    """
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)
    socket.connect(f"tcp://{servidor}:{PORTA_PARES}")
    try:
        pedido = criar(tipo, NOME, dados)
        print(formatar(f"{NOME} ENVIOU A {servidor}", pedido), flush=True)
        socket.send(empacotar(pedido))

        if socket.poll(TEMPO_LIMITE) == 0:
            print(f"{NOME} nao obteve resposta de {servidor}", flush=True)
            return None

        resposta = desempacotar(socket.recv())
        print(formatar(f"{NOME} RECEBEU DE {servidor}", resposta), flush=True)
        return resposta
    finally:
        socket.close()


def listar_servidores():
    resposta = perguntar_referencia("list", {})
    if resposta["status"] != "ok":
        return []
    return resposta["dados"]["servidores"]


def anunciar_coordenador():
    """Publica no topico servers que este servidor foi eleito."""
    aviso = criar("coordenador", NOME, {"coordenador": NOME})
    publicar(TOPICO_SERVIDORES, aviso)
    print(formatar(f"{NOME} ANUNCIOU coordenador", aviso), flush=True)


def iniciar_eleicao():
    """Algoritmo do valentao: quem tem o maior rank vence."""
    global coordenador

    if coordenador == NOME:
        anunciar_coordenador()
        return

    print(f"{NOME} iniciando eleicao", flush=True)
    lista = listar_servidores()
    maiores = [s for s in lista if s["rank"] > (rank or 0)]

    respondeu = False
    for s in maiores:
        resposta = perguntar_par(s["servidor"], "eleicao", {})
        if resposta is not None and resposta["status"] == "ok":
            respondeu = True

    if respondeu:
        # Alguem com rank maior assumiu a eleicao; o anuncio chega pelo topico
        print(f"{NOME} cedeu a eleicao a um servidor de rank maior", flush=True)
        return

    coordenador = NOME
    print(f"{NOME} venceu a eleicao e e o novo coordenador", flush=True)
    anunciar_coordenador()


def berkeley():
    """Coordenador: media das horas de todos os servidores conhecidos."""
    global ajuste

    horas = [hora_corrigida()]
    for s in listar_servidores():
        if s["servidor"] == NOME:
            continue
        resposta = perguntar_par(s["servidor"], "hora_local", {})
        if resposta is not None and resposta["status"] == "ok":
            horas.append(resposta["dados"]["hora"])

    media = sum(horas) / len(horas)
    ajuste = media - time.time()
    print(f"{NOME} (coordenador) calculou media de {len(horas)} relogios, "
          f"ajuste proprio de {ajuste:.6f}s", flush=True)
    return media


def sincronizar_relogio():
    """A cada 15 mensagens: pede a hora ao coordenador, ou faz Berkeley."""
    global ajuste, coordenador

    if coordenador is None:
        iniciar_eleicao()
        return

    if coordenador == NOME:
        berkeley()
        return

    resposta = perguntar_par(coordenador, "hora", {})
    if resposta is None or resposta["status"] != "ok":
        print(f"{NOME} nao alcancou o coordenador {coordenador}", flush=True)
        coordenador = None
        iniciar_eleicao()
        return

    ajuste = resposta["dados"]["hora"] - time.time()
    print(f"{NOME} ajustou o relogio em {ajuste:.6f}s pelo coordenador "
          f"{coordenador}", flush=True)


def heartbeat():
    perguntar_referencia("heartbeat", {"servidor": NOME})


def tratar_cliente(pedido):
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


def tratar_par(pedido):
    """Pedidos vindos de outros servidores."""
    tipo = pedido["tipo"]

    if tipo == "eleicao":
        return criar("eleicao_resp", NOME, {"mensagem": "OK"}, "ok")

    if tipo == "hora_local":
        return criar("hora_local_resp", NOME, {"hora": hora_corrigida()}, "ok")

    if tipo == "hora":
        if coordenador != NOME:
            return criar("hora_resp", NOME, {"mensagem": "nao sou o coordenador"}, "erro")
        return criar("hora_resp", NOME, {"hora": hora_corrigida()}, "ok")

    return criar("erro", NOME, {"mensagem": f"tipo desconhecido: {tipo}"}, "erro")


print(f"{NOME} escutando pares em {PORTA_PARES}", flush=True)
print(f"{NOME} conectado a referencia em {REFERENCIA}", flush=True)

resposta = perguntar_referencia("rank", {"servidor": NOME})
if resposta["status"] == "ok":
    rank = resposta["dados"]["rank"]
    print(f"{NOME} recebeu rank {rank}", flush=True)

print(f"{NOME} publicador conectado ao proxy em {PROXY_PUB}", flush=True)
print(f"{NOME} conectado ao broker em {BROKER}", flush=True)
print(f"{NOME} canais recuperados do disco: {canais}", flush=True)

def atender_pares():
    """Thread dedicada: responde pares sem nunca bloquear em rede."""
    global precisa_eleger
    while True:
        pedido = desempacotar(pares.recv())
        print(formatar(f"{NOME} RECEBEU DE UM PAR", pedido), flush=True)

        resposta = tratar_par(pedido)
        print(formatar(f"{NOME} RESPONDEU AO PAR", resposta), flush=True)
        pares.send(empacotar(resposta))

        # A eleicao roda no laco principal, para nao travar esta thread
        if pedido["tipo"] == "eleicao":
            precisa_eleger = True


threading.Thread(target=atender_pares, daemon=True).start()

poller = zmq.Poller()
poller.register(clientes, zmq.POLLIN)
poller.register(inscritor, zmq.POLLIN)

# Elege um coordenador assim que os outros servidores ja tiveram tempo de subir
time.sleep(3)
iniciar_eleicao()

while True:
    eventos = dict(poller.poll(1000))

    if precisa_eleger:
        precisa_eleger = False
        iniciar_eleicao()

    if inscritor in eventos:
        topico, bruto = inscritor.recv_multipart()
        aviso = desempacotar(bruto)
        if aviso["tipo"] == "coordenador":
            coordenador = aviso["dados"]["coordenador"]
            print(f"{NOME} reconhece {coordenador} como coordenador", flush=True)

    if clientes in eventos:
        pedido = desempacotar(clientes.recv())
        print(formatar(f"{NOME} RECEBEU", pedido), flush=True)

        resposta = tratar_cliente(pedido)
        print(formatar(f"{NOME} ENVIOU", resposta), flush=True)

        clientes.send(empacotar(resposta))

        recebidas += 1
        if recebidas % 10 == 0:
            print(f"{NOME} atingiu {recebidas} mensagens, enviando heartbeat", flush=True)
            heartbeat()
        if recebidas % 15 == 0:
            print(f"{NOME} atingiu {recebidas} mensagens, sincronizando relogio", flush=True)
            sincronizar_relogio()
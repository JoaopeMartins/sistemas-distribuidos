import json
import time
from datetime import datetime, timezone

import msgpack

# Relogio logico de Lamport, um por processo
_relogio = 0


def tique():
    """Regra 1: incrementa antes de cada envio."""
    global _relogio
    _relogio += 1
    return _relogio


def sincronizar(recebido):
    """Regra 2: ao receber, usa o maximo entre o proprio e o recebido."""
    global _relogio
    _relogio = max(_relogio, recebido or 0)
    return _relogio


def valor_relogio():
    return _relogio


def criar(tipo, origem, dados=None, status=None):
    """Monta o envelope padrao do projeto. Timestamp em segundos."""
    envelope = {
        "tipo": tipo,
        "origem": origem,
        "timestamp": time.time(),
        "relogio": tique(),
        "dados": dados if dados is not None else {},
    }
    if status is not None:
        envelope["status"] = status
    return envelope


def empacotar(envelope):
    return msgpack.packb(envelope, use_bin_type=True)


def desempacotar(dados):
    """Ao desempacotar, o relogio local ja e sincronizado."""
    envelope = msgpack.unpackb(dados, raw=False)
    sincronizar(envelope.get("relogio"))
    return envelope


def formatar(prefixo, envelope):
    """Texto legivel para o log exigido pelo enunciado."""
    hora = datetime.fromtimestamp(envelope["timestamp"], timezone.utc).isoformat()
    status = f" status={envelope['status']}" if "status" in envelope else ""
    return (
        f"[{prefixo}] {envelope['tipo']} origem={envelope['origem']} "
        f"ts={hora} relogio={envelope['relogio']} local={_relogio}{status} "
        f"dados={json.dumps(envelope['dados'])}"
    )
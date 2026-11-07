import json
import time
from datetime import datetime, timezone

import msgpack


def criar(tipo, origem, dados=None, status=None, relogio=0):
    """Monta o envelope padrao do projeto. Timestamp em segundos."""
    envelope = {
        "tipo": tipo,
        "origem": origem,
        "timestamp": time.time(),
        "relogio": relogio,
        "dados": dados if dados is not None else {},
    }
    if status is not None:
        envelope["status"] = status
    return envelope


def empacotar(envelope):
    return msgpack.packb(envelope, use_bin_type=True)


def desempacotar(dados):
    return msgpack.unpackb(dados, raw=False)


def formatar(prefixo, envelope):
    """Texto legivel para o log exigido pelo enunciado."""
    hora = datetime.fromtimestamp(envelope["timestamp"], timezone.utc).isoformat()
    status = f" status={envelope['status']}" if "status" in envelope else ""
    return (
        f"[{prefixo}] {envelope['tipo']} origem={envelope['origem']} "
        f"ts={hora} relogio={envelope['relogio']}{status} "
        f"dados={json.dumps(envelope['dados'])}"
    )
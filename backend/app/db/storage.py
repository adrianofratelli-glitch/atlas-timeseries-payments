"""Comparação de armazenamento: o mesmo evento em coleção time series e normal.

`payment_events_flat` guarda uma fatia do mesmo dado numa coleção comum. Comparar
tamanho absoluto entre coleções de períodos diferentes seria desonesto, então a
comparação é por evento — bytes por evento e a razão entre elas — e o payload diz
quantos eventos cada uma cobre.
"""
from __future__ import annotations

import time

from .client import db, with_retry

# $collStats sobre dezenas de milhões de eventos custa segundos, e o painel de
# armazenamento não muda entre dois cliques do apresentador. Cache curto, com o
# instante da medição no payload para a tela poder dizer quando foi medido.
_CACHE: dict | None = None
_CACHE_AT = 0.0
CACHE_SECONDS = 600.0

COLECOES = ("payment_events", "payment_events_flat")


def _stats(nome: str) -> dict | None:
    d = db()
    try:
        st = next(d[nome].aggregate([{"$collStats": {"storageStats": {}}}]))["storageStats"]
    except Exception:  # noqa: BLE001 — coleção ausente é resposta, não erro
        return None
    docs = st.get("count") or d[nome].estimated_document_count()
    if not docs:
        return None
    storage = st.get("storageSize", 0)
    index = st.get("totalIndexSize", 0)
    return {
        "collection": nome,
        "documents": docs,
        "storage_bytes": storage,
        "index_bytes": index,
        "total_bytes": storage + index,
        "bytes_per_event": round(storage / docs, 2),
        "total_bytes_per_event": round((storage + index) / docs, 2),
        "timeseries": bool(st.get("timeseries")),
        "buckets": (st.get("timeseries") or {}).get("bucketCount"),
    }


def aquecer() -> None:
    """Roda uma vez no startup, numa thread.

    Medido: `$collStats` com storageStats sobre 2,6 milhões de buckets leva ~32 s
    na primeira chamada. Sem aquecer, a primeira vez que o apresentador abre a aba
    de armazenamento a tela fica meio minuto em "medindo…".
    """
    try:
        comparison(force=True)
    except Exception:  # noqa: BLE001 — aquecimento não pode derrubar o startup
        pass


def comparison(force: bool = False) -> dict:
    global _CACHE, _CACHE_AT
    if _CACHE and not force and time.time() - _CACHE_AT < CACHE_SECONDS:
        return {**_CACHE, "cached": True,
                "measured_seconds_ago": round(time.time() - _CACHE_AT, 1)}
    ts = with_retry(lambda: _stats("payment_events"))
    flat = with_retry(lambda: _stats("payment_events_flat"))
    if not ts or not flat:
        return {"available": False,
                "reason": "payment_events_flat ausente — rode o gerador com --flat"}
    resultado = {
        "available": True,
        "cached": False,
        "measured_seconds_ago": 0.0,
        "timeseries": ts,
        "flat": flat,
        "storage_ratio": round(flat["bytes_per_event"] / ts["bytes_per_event"], 2),
        "total_ratio": round(
            flat["total_bytes_per_event"] / ts["total_bytes_per_event"], 2),
        "note": "razão por evento; as coleções cobrem períodos diferentes de propósito",
    }
    _CACHE, _CACHE_AT = resultado, time.time()
    return resultado

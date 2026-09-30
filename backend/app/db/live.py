"""Evidência mínima da coleção time series que recebe a ingestão ao vivo."""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

from ..config import MAX_POINTS, MAX_TIME_MS
from .client import db, with_retry

COLLECTION = "payment_events_live"
_OPTIONS_CACHE: tuple[float, dict] = (0.0, {})


def _collection_options() -> dict:
    """Lê a configuração real da coleção, com cache para não consultar a cada poll."""
    global _OPTIONS_CACHE
    now = time.monotonic()
    if now - _OPTIONS_CACHE[0] < 30 and _OPTIONS_CACHE[1]:
        return _OPTIONS_CACHE[1]

    result = with_retry(lambda: db().command(
        "listCollections", filter={"name": COLLECTION}))
    rows = result.get("cursor", {}).get("firstBatch", [])
    options = rows[0].get("options", {}) if rows else {}
    timeseries = options.get("timeseries", {})
    proof = {
        "exists": bool(rows),
        "timeseries": bool(timeseries),
        "time_field": timeseries.get("timeField"),
        "meta_field": timeseries.get("metaField"),
        "bucket_max_span_seconds": timeseries.get("bucketMaxSpanSeconds"),
        "expire_after_seconds": options.get("expireAfterSeconds"),
    }
    _OPTIONS_CACHE = (now, proof)
    return proof


def _bucket_snapshot(last_document: dict | None) -> dict | None:
    """Lê o bucket físico que contém a amostra já confirmada pelo feed.

    A igualdade é feita campo a campo porque a ordem BSON do subdocumento `meta`
    não faz parte do contrato da API. A consulta usa os índices internos derivados
    dos índices da coleção time series e projeta apenas o cabeçalho do bucket.
    """
    if not last_document:
        return None
    timestamp = last_document.get("ts")
    meta = last_document.get("meta") or {}
    if not timestamp or not meta:
        return None

    bucket_collection = db()[f"system.buckets.{COLLECTION}"]
    bucket = with_retry(lambda: bucket_collection.find_one(
        {
            **{f"meta.{key}": value for key, value in meta.items()},
            "control.min.ts": {"$lte": timestamp},
            "control.max.ts": {"$gte": timestamp},
        },
        {
            "_id": 1,
            "meta": 1,
            "control.version": 1,
            "control.count": 1,
            "control.min.ts": 1,
            "control.max.ts": 1,
        },
    ))
    if not bucket:
        return None

    control = bucket.get("control", {})
    version = control.get("version")
    return {
        "id": str(bucket["_id"]),
        "meta": bucket.get("meta", {}),
        "min_ts": control.get("min", {}).get("ts"),
        "max_ts": control.get("max", {}).get("ts"),
        "measurements": control.get("count"),
        "control_version": version,
        "compressed": isinstance(version, int) and version >= 2,
        "source": f"system.buckets.{COLLECTION}",
    }


def overview(session_started_at: datetime | None = None,
             last_document: dict | None = None) -> dict:
    """Agrega o trilho completo em bins de um segundo sobre os últimos 60 s."""
    # O segundo corrente ainda está sendo escrito. Exibi-lo cria uma queda falsa no
    # último ponto (por exemplo, 16 contra 2,3 k) que desaparece no poll seguinte.
    end = datetime.now(timezone.utc).replace(microsecond=0)
    window_start = end - timedelta(seconds=60)
    # Uma nova execução começa uma curva nova sem apagar dado. O TTL continua
    # responsável pela retenção, como seria numa operação real.
    start = max(window_start, session_started_at) if session_started_at else window_start
    pipe = [
        {"$match": {"ts": {"$gte": start, "$lt": end}}},
        {"$group": {
            "_id": {"$dateTrunc": {"date": "$ts", "unit": "second"}},
            "eventos": {"$sum": 1},
            "volume": {"$sum": "$valor"},
        }},
        {"$set": {"ts": "$_id"}},
        {"$sort": {"ts": 1}},
        {"$project": {
            "_id": 0,
            "ts": 1,
            "eventos": 1,
            "volume": {"$round": ["$volume", 2]},
        }},
        {"$limit": min(MAX_POINTS, 60)},
    ]
    points = with_retry(lambda: list(
        db()[COLLECTION].aggregate(pipe, maxTimeMS=MAX_TIME_MS)))
    return {
        "namespace": f"{db().name}.{COLLECTION}",
        "from": start,
        "to": end,
        "granularity": {"unit": "second", "bin_size": 1, "label": "1 s"},
        "points": points,
        "window_events": sum(point["eventos"] for point in points),
        "collection": _collection_options(),
        "bucket": _bucket_snapshot(last_document),
        "pipeline": pipe,
    }

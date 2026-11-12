"""Evidência mínima da coleção time series que recebe a ingestão ao vivo."""
from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timedelta, timezone

from pymongo.errors import OperationFailure

from ..config import MAX_POINTS, MAX_TIME_MS
from .client import db, with_retry

log = logging.getLogger(__name__)

COLLECTION = "payment_events_live"
_OPTIONS_CACHE: tuple[float, dict] = (0.0, {})


def _collection_options() -> dict:
    """Lê a configuração real da coleção, com cache para não consultar a cada poll."""
    global _OPTIONS_CACHE
    now = time.monotonic()
    # Só a configuração de uma coleção existente vai para o cache: "ainda não existe"
    # cacheado por 30 s deixava timeField/metaField em branco logo depois do play.
    if now - _OPTIONS_CACHE[0] < 30 and _OPTIONS_CACHE[1].get("exists"):
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

    MongoDB 8.2+/9.0 proíbe ler `system.buckets.*` diretamente; o caminho suportado é
    o próprio namespace da coleção com `rawData: true`, que devolve o bucket em vez
    da medição desempacotada. Servidores anteriores ainda aceitam o namespace
    interno, usado como segunda tentativa. A igualdade em `meta` é campo a campo
    porque a ordem BSON do subdocumento não faz parte do contrato da API.
    """
    if not last_document:
        return None
    timestamp = last_document.get("ts")
    meta = last_document.get("meta") or {}
    if not timestamp or not meta:
        return None

    filtro = {
        **{f"meta.{key}": value for key, value in meta.items()},
        "control.min.ts": {"$lte": timestamp},
        "control.max.ts": {"$gte": timestamp},
    }
    bucket, source = None, None
    for leitor, origem in ((_find_bucket_raw, f"{COLLECTION} · rawData"),
                           (_find_bucket_legacy, f"system.buckets.{COLLECTION}")):
        try:
            bucket = leitor(filtro)
        except OperationFailure as exc:
            log.info("leitura de bucket via %s indisponível: %s", origem,
                     (exc.details or {}).get("errmsg", exc))
            continue
        source = origem
        break
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
        "source": source,
    }


_BUCKET_PROJECTION = {"_id": 1, "meta": 1, "control.version": 1, "control.count": 1,
                      "control.min.ts": 1, "control.max.ts": 1}


def _find_bucket_raw(filtro: dict) -> dict | None:
    resposta = with_retry(lambda: db().command(
        "find", COLLECTION, filter=filtro, projection=_BUCKET_PROJECTION, limit=1,
        singleBatch=True, maxTimeMS=MAX_TIME_MS, rawData=True))
    linhas = resposta.get("cursor", {}).get("firstBatch", [])
    return linhas[0] if linhas else None


def _find_bucket_legacy(filtro: dict) -> dict | None:
    return with_retry(lambda: db()[f"system.buckets.{COLLECTION}"].find_one(
        filtro, _BUCKET_PROJECTION, max_time_ms=MAX_TIME_MS))


def overview(session_started_at: datetime | None = None,
             last_document: dict | None = None) -> dict:
    """Agrega o trilho completo em bins de um segundo sobre os últimos 60 s."""
    # O segundo corrente ainda está sendo escrito. Exibi-lo cria uma queda falsa no
    # último ponto (por exemplo, 16 contra 2,3 k) que desaparece no poll seguinte.
    end = datetime.now(timezone.utc).replace(microsecond=0)
    # Também não passa do último lote confirmado: o lote em voo ainda vai preencher
    # o segundo em que o anterior terminou.
    ultimo = (last_document or {}).get("ts")
    if isinstance(ultimo, datetime):
        ultimo = ultimo if ultimo.tzinfo else ultimo.replace(tzinfo=timezone.utc)
        end = min(end, ultimo.replace(microsecond=0))
    window_start = end - timedelta(seconds=60)
    # Uma nova execução começa uma curva nova sem apagar dado. O TTL continua
    # responsável pela retenção, como seria numa operação real.
    start = max(window_start, session_started_at) if session_started_at else window_start
    pipe = _pipeline(start, end)
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


def _pipeline(start: datetime, end: datetime) -> list[dict]:
    """O pipeline do gráfico: o mesmo que roda, que a gaveta mostra e que o explain lê."""
    return [
        {"$match": {"ts": {"$gte": start, "$lt": end}}},
        {"$group": {
            "_id": {"$dateTrunc": {"date": "$ts", "unit": "second"}},
            "eventos": {"$sum": 1},
            "volume": {"$sum": "$valor"},
        }},
        # Ordena pela chave do `$group` e só depois renomeia. Medido no 9.0.7
        # (queries/block_processing.py, ADR 0003): `$set ts` seguido de `$sort ts`
        # tirava a agregação inteira do motor SBE e, com ela, o block processing.
        {"$sort": {"_id": 1}},
        {"$limit": min(MAX_POINTS, 60)},
        {"$project": {
            "_id": 0,
            "ts": "$_id",
            "eventos": 1,
            "volume": {"$round": ["$volume", 2]},
        }},
    ]


# --------------------------------------------------------------- plano de execução
_PLANO_CACHE: tuple[float, dict | None] = (0.0, None)
PLANO_CACHE_SECONDS = 60.0


def _estagios(no, saida: list[str]) -> list[str]:
    """Todos os valores de `stage` do explain, em ordem de aparição, sem repetir."""
    if isinstance(no, dict):
        estagio = no.get("stage")
        if isinstance(estagio, str) and estagio not in saida:
            saida.append(estagio)
        for valor in no.values():
            _estagios(valor, saida)
    elif isinstance(no, list):
        for item in no:
            _estagios(item, saida)
    return saida


_ESTAGIO_BLOCO = re.compile(r"\b(ts_bucket_to_cellblock|block_[a-z_]+)\b")


def _planos_sbe(no, saida: list[str]) -> list[str]:
    """Textos de `slotBasedPlan.stages` em qualquer nível do explain."""
    if isinstance(no, dict):
        sbp = no.get("slotBasedPlan")
        if isinstance(sbp, dict):
            saida.append(str(sbp.get("stages", "")))
        for valor in no.values():
            _planos_sbe(valor, saida)
    elif isinstance(no, list):
        for item in no:
            _planos_sbe(item, saida)
    return saida


def resumir_plano(explain: dict) -> dict:
    """Reduz o explain ao que a gaveta mostra: motor, block processing e estágios.

    Block processing (8.0+) aparece como estágios do plano SBE com `block` no nome
    (`ts_bucket_to_cellblock`, `block_group`, `block_to_row`). Sem eles a consulta
    roda linha a linha — e a tela diz isso em vez de presumir o ganho.
    """
    estagios = _estagios(explain.get("queryPlanner", explain), [])
    planos_sbe = _planos_sbe(explain, [])
    # Com verbosity queryPlanner os estágios SBE vêm como texto em
    # `slotBasedPlan.stages`; com executionStats, também como chaves `stage`.
    blocos = [e for e in estagios if "block" in e.lower()]
    for texto in planos_sbe:
        for nome in _ESTAGIO_BLOCO.findall(texto):
            if nome not in blocos:
                blocos.append(nome)
    sbe = bool(planos_sbe)
    return {
        "engine": "sbe" if sbe else "classic",
        "block_processing": bool(blocos),
        "block_stages": blocos,
        "stages": estagios,
        "server_version": (explain.get("serverInfo") or {}).get("version"),
    }


def plano() -> dict:
    """`explain` (queryPlanner: planeja sem executar) do pipeline da janela atual.

    Sob demanda e com cache: o plano não muda a cada segundo e um explain por poll
    dobraria as idas ao cluster do caminho interativo.
    """
    global _PLANO_CACHE
    agora = time.monotonic()
    if _PLANO_CACHE[1] is not None and agora - _PLANO_CACHE[0] < PLANO_CACHE_SECONDS:
        return {**_PLANO_CACHE[1], "cached": True}
    end = datetime.now(timezone.utc).replace(microsecond=0)
    pipe = _pipeline(end - timedelta(seconds=60), end)
    explain = with_retry(lambda: db().command(
        "explain", {"aggregate": COLLECTION, "pipeline": pipe, "cursor": {}},
        verbosity="queryPlanner", maxTimeMS=MAX_TIME_MS))
    resumo = {"namespace": f"{db().name}.{COLLECTION}", **resumir_plano(explain),
              "verbosity": "queryPlanner"}
    _PLANO_CACHE = (agora, resumo)
    return {**resumo, "cached": False}

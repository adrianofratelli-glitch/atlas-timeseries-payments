"""Latência do trilho por percentil, não por média.

Um trilho de pagamento não é julgado pela média: é julgado pela cauda. `$percentile`
(MongoDB 7.0+) devolve p50, p95 e p99 dentro do próprio pipeline, sobre o evento
bruto — sem contador pré-agregado e sem uma segunda base de métricas ao lado.

`$densify` e `$fill` reconstroem a janela em que o provedor **parou de reportar**.
Todo ponto reconstruído volta marcado: inventar latência sem dizer que inventou é
como um trilho de pagamento perde a confiança de quem opera ele.
"""
from __future__ import annotations

from datetime import datetime

from ..config import MAX_POINTS, MAX_TIME_MS
from .client import db, with_retry
from .ranges import RangeError, label, resolve

# Sem provedor, a consulta varre o canal inteiro: medido, 6,5 s em 24 h e acima do
# teto de 15 s em 7 dias. Recusar cedo com uma instrução é melhor que queimar quinze
# segundos e devolver 503 — e é a diferença entre um limite explicado e uma demo
# travada.
CANAL_MAX_HORAS = 24.0

PERCENTIS = [0.5, 0.95, 0.99]


def pipeline(match: dict, start: datetime, end: datetime, unit: str, size: int,
             fill: bool) -> list[dict]:
    pipe: list[dict] = [
        {"$match": {**match, "ts": {"$gte": start, "$lt": end}}},
        {"$group": {
            "_id": {"$dateTrunc": {"date": "$ts", "unit": unit, "binSize": size}},
            "eventos": {"$sum": 1},
            "aprovados": {"$sum": {"$cond": ["$aprovado", 1, 0]}},
            "volume": {"$sum": "$valor"},
            # Percentil aproximado (t-digest): é o modo suportado sobre um fluxo
            # deste tamanho, e a aproximação é irrelevante para decidir se um
            # provedor degradou.
            "lat": {"$percentile": {"input": "$latencia_ms", "p": PERCENTIS,
                                    "method": "approximate"}},
        }},
        {"$set": {
            "ts": "$_id",
            "p50": {"$arrayElemAt": ["$lat", 0]},
            "p95": {"$arrayElemAt": ["$lat", 1]},
            "p99": {"$arrayElemAt": ["$lat", 2]},
            "taxa_recusa": {"$cond": [
                {"$gt": ["$eventos", 0]},
                {"$multiply": [100, {"$divide": [
                    {"$subtract": ["$eventos", "$aprovados"]}, "$eventos"]}]}, 0]},
            "medido": True,
        }},
        {"$sort": {"ts": 1}},
    ]
    if fill:
        pipe += [
            {"$densify": {"field": "ts",
                          "range": {"step": size, "unit": unit, "bounds": [start, end]}}},
            {"$fill": {"sortBy": {"ts": 1},
                       "output": {"p50": {"method": "locf"}, "p95": {"method": "locf"},
                                  "p99": {"method": "locf"},
                                  "taxa_recusa": {"method": "locf"},
                                  "eventos": {"value": 0}, "volume": {"value": 0}}}},
            {"$set": {"reconstruido": {"$ne": ["$medido", True]},
                      "metodo": {"$cond": [{"$ne": ["$medido", True]}, "locf", None]}}},
        ]
    else:
        pipe += [{"$set": {"reconstruido": False, "metodo": None}}]

    pipe += [
        {"$project": {"_id": 0, "ts": 1, "eventos": 1,
                      "volume": {"$round": ["$volume", 2]},
                      "p50": {"$round": ["$p50", 1]},
                      "p95": {"$round": ["$p95", 1]},
                      "p99": {"$round": ["$p99", 1]},
                      "taxa_recusa": {"$round": ["$taxa_recusa", 3]},
                      "reconstruido": 1, "metodo": 1}},
        {"$limit": MAX_POINTS},
    ]
    return pipe


def serie(canal: str | None, provedor: str | None, hours: float, fill: bool) -> dict:
    if not provedor and hours > CANAL_MAX_HORAS:
        raise RangeError(
            f"janela de {hours:.0f}h sobre o canal inteiro não cabe no teto; "
            f"escolha um provedor ou use até {CANAL_MAX_HORAS:.0f}h")
    start, end, unit, size = resolve(hours)
    # O provedor já implica o canal. Filtrar pelos dois deixa passar um par
    # contraditório (canal=cartao com um PSP de PIX), que devolve zero eventos — e
    # aí o $fill "reconstrói" a janela inteira a partir de nada.
    match: dict = {}
    if provedor:
        match["meta.provedor"] = provedor
    elif canal:
        match["meta.canal"] = canal
    pipe = pipeline(match, start, end, unit, size, fill)
    pontos = with_retry(lambda: list(
        db().payment_events.aggregate(pipe, maxTimeMS=MAX_TIME_MS)))
    medidos = sum(1 for p in pontos if not p.get("reconstruido"))
    if fill and medidos == 0:
        # Nada medido na janela: preencher aqui seria inventar a série inteira, não
        # reconstruir uma lacuna. Devolve vazio e diz por quê.
        pontos = []
    return {
        "canal": canal, "provedor": provedor,
        "from": start, "to": end,
        "granularity": {"unit": unit, "bin_size": size, "label": label(unit, size)},
        "points": pontos,
        "point_count": len(pontos),
        "medidos": medidos,
        "reconstruidos": sum(1 for p in pontos if p.get("reconstruido")),
        "vazio": len(pontos) == 0,
        "truncated": len(pontos) >= MAX_POINTS,
        "pipeline": pipe,
    }

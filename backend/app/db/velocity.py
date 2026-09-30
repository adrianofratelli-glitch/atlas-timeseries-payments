"""Velocity da conta: a consulta que roda **dentro** da autorização.

Um painel pode levar 300 ms. Esta não pode: ela entra no caminho que decide se uma
transação passa, e o orçamento inteiro da decisão antifraude costuma ser de algumas
dezenas de milissegundos.

É também a resposta à objeção de cardinalidade. `conta_id` é campo de medição, não
metaField: são milhões de contas, e cada valor distinto de meta é uma série própria.
Com `conta_id` no meta, o número de séries salta de ~2.900 rotas para milhões — o
ADR 0002 mede o que isso faz com armazenamento e latência. O caminho aqui é o outro:
meta pequeno, índice secundário sobre o campo de medição.
"""
from __future__ import annotations

from datetime import timedelta

from ..config import MAX_TIME_MS, VELOCITY_WINDOWS
from .client import db, with_retry
from .ranges import anchor


def pipeline(conta_id: str, janelas: list[int]) -> list[dict]:
    fim = anchor()
    maior = max(janelas)
    inicio = fim - timedelta(hours=maior)
    ramos = []
    for h in sorted(janelas):
        corte = fim - timedelta(hours=h)
        ramos.append({
            "k": f"{h}h",
            "v": {
                "eventos": {"$sum": {"$cond": [{"$gte": ["$ts", corte]}, 1, 0]}},
                "valor": {"$sum": {"$cond": [{"$gte": ["$ts", corte]}, "$valor", 0]}},
                "recusados": {"$sum": {"$cond": [
                    {"$and": [{"$gte": ["$ts", corte]}, {"$eq": ["$aprovado", False]}]},
                    1, 0]}},
            },
        })
    # Uma passada só sobre a janela maior: cada janela menor vira um $cond dentro do
    # mesmo $group. Três consultas separadas seriam três varreduras e três round trips
    # dentro de um orçamento de dezenas de milissegundos.
    return [
        {"$match": {"conta_id": conta_id, "ts": {"$gte": inicio, "$lt": fim}}},
        {"$group": {"_id": None,
                    **{f'j_{r["k"]}_{campo}': expr
                       for r in ramos for campo, expr in r["v"].items()},
                    "canais": {"$addToSet": "$meta.canal"},
                    "ufs": {"$addToSet": "$meta.uf"}}},
        {"$project": {"_id": 0}},
    ]


def features(conta_id: str) -> dict:
    janelas = VELOCITY_WINDOWS
    pipe = pipeline(conta_id, janelas)
    linhas = with_retry(lambda: list(
        db().payment_events.aggregate(pipe, maxTimeMS=MAX_TIME_MS)))
    bruto = linhas[0] if linhas else {}

    saida = {}
    for h in sorted(janelas):
        eventos = bruto.get(f"j_{h}h_eventos", 0)
        recusados = bruto.get(f"j_{h}h_recusados", 0)
        saida[f"{h}h"] = {
            "eventos": eventos,
            "valor": round(bruto.get(f"j_{h}h_valor", 0.0), 2),
            "recusados": recusados,
            "taxa_recusa": round(100 * recusados / eventos, 2) if eventos else 0.0,
        }
    return {
        "conta_id": conta_id,
        "janelas": saida,
        "canais": sorted(bruto.get("canais", [])),
        "ufs": sorted(bruto.get("ufs", [])),
        "pipeline": pipe,
    }

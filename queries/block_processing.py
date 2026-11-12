"""Block processing (8.0+) no pipeline do gráfico ao vivo: medido, não presumido.

Compara o pipeline anterior (`$set ts` + `$sort ts`, que manda a agregação inteira
para o motor clássico) com o atual (`live._pipeline`: `$sort _id` + `$limit` +
`$project`, mesma saída). Para cada janela registra o motor e os estágios de bloco do
`explain` e a latência de ponta a ponta, com o piso de rede medido no mesmo run.

Só lê. Padrão: banco de teste (`trilho_pagamentos_test`), coleção `payment_events`.

    .venv/bin/python queries/block_processing.py --runs 15 --out /tmp/block.json
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "backend"))


def pipeline_anterior(start, end) -> list[dict]:
    """O pipeline até 2026-11-12 (saída idêntica; ordenava por um campo derivado)."""
    return [
        {"$match": {"ts": {"$gte": start, "$lt": end}}},
        {"$group": {"_id": {"$dateTrunc": {"date": "$ts", "unit": "second"}},
                    "eventos": {"$sum": 1}, "volume": {"$sum": "$valor"}}},
        {"$set": {"ts": "$_id"}},
        {"$sort": {"ts": 1}},
        {"$project": {"_id": 0, "ts": 1, "eventos": 1,
                      "volume": {"$round": ["$volume", 2]}}},
        {"$limit": 60},
    ]


def percentis(amostras: list[float]) -> dict:
    amostras = sorted(amostras)
    return {"p50": round(statistics.median(amostras), 1),
            "p95": round(amostras[max(int(len(amostras) * 0.95) - 1, 0)], 1),
            "min": round(amostras[0], 1)}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="trilho_pagamentos_test")
    ap.add_argument("--collection", default="payment_events")
    ap.add_argument("--hours", default="1,24,72")
    ap.add_argument("--runs", type=int, default=15)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    os.environ["MONGODB_DB"] = args.db

    from app.db import live  # noqa: PLC0415 — depende de MONGODB_DB acima
    from app.db.client import db  # noqa: PLC0415

    d = db()
    col = d[args.collection]
    info = d.dataset_info.find_one({"_id": "payment_events"}) or {}
    fim = info.get("last_ts") or datetime.now(timezone.utc)

    piso = []
    for _ in range(args.runs):
        t0 = time.perf_counter()
        d.command("ping")
        piso.append((time.perf_counter() - t0) * 1000)

    resultado = {"measured_at": datetime.now(timezone.utc).isoformat(),
                 "server_version": d.client.server_info()["version"],
                 "namespace": f"{args.db}.{args.collection}",
                 "network_floor_ms": percentis(piso), "windows": []}
    for horas in [float(h) for h in args.hours.split(",") if h.strip()]:
        inicio = fim - timedelta(hours=horas)
        linha = {"hours": horas}
        saidas = {}
        for nome, montar in (("anterior", pipeline_anterior), ("atual", live._pipeline)):
            pipe = montar(inicio, fim)
            explain = d.command("explain", {"aggregate": args.collection,
                                            "pipeline": pipe, "cursor": {}},
                                verbosity="queryPlanner")
            plano = live.resumir_plano(explain)
            saidas[nome] = list(col.aggregate(pipe))
            amostras = []
            for _ in range(args.runs):
                t0 = time.perf_counter()
                list(col.aggregate(pipe, maxTimeMS=60000))
                amostras.append((time.perf_counter() - t0) * 1000)
            linha[nome] = {"engine": plano["engine"],
                           "block_stages": plano["block_stages"],
                           "latency_ms": percentis(amostras)}
        linha["same_output"] = saidas["anterior"] == saidas["atual"]
        linha["p50_ratio"] = round(linha["anterior"]["latency_ms"]["p50"]
                                   / linha["atual"]["latency_ms"]["p50"], 2)
        resultado["windows"].append(linha)
        print(json.dumps(linha, ensure_ascii=False), flush=True)

    texto = json.dumps(resultado, ensure_ascii=False, indent=2)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(texto + "\n")
    print(texto)
    return 0 if all(w["same_output"] for w in resultado["windows"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())

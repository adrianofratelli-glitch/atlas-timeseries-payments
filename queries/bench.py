"""Medições no volume cheio. Regrava queries/bench-results.json.

Toda latência que aparece na frente de um cliente sai daqui, medida contra o cluster
real, com o piso de rede medido junto — sem ele, "12 ms" parece desempenho de banco
quando é, quase inteiro, ida e volta de rede.

Reusa os módulos de `backend/app/db/` em vez de reescrever os pipelines: medir uma
consulta parecida com a de produção, mas não igual, é como se descobre em campo que
o número do slide não valia.
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "data-generator"))
sys.path.insert(0, os.path.join(ROOT, "backend"))
import common  # noqa: E402


def percentis(amostras: list[float]) -> dict:
    amostras = sorted(amostras)
    return {"p50": round(statistics.median(amostras), 1),
            "p95": round(amostras[max(int(len(amostras) * 0.95) - 1, 0)], 1),
            "min": round(amostras[0], 1), "max": round(amostras[-1], 1)}


def medir(fn, runs: int) -> dict:
    from pymongo.errors import ExecutionTimeout  # noqa: PLC0415
    try:
        for _ in range(3):
            fn()
    except ExecutionTimeout:
        # Consulta que não cabe no teto é um resultado, não um crash: o teto existe
        # para a tela não travar, e o número que interessa é "não completa".
        return {"timeout": True, "max_time_ms": int(os.getenv("TS_MAX_TIME_MS", "15000"))}
    amostras = []
    for _ in range(runs):
        t0 = time.perf_counter()
        fn()
        amostras.append((time.perf_counter() - t0) * 1000)
    return percentis(amostras)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=20)
    ap.add_argument("--db", default=None)
    args = ap.parse_args()

    os.environ.setdefault("MONGODB_DB", args.db or os.getenv("MONGODB_DB", "trilho_pagamentos"))
    from app.db import latency, providers, velocity  # noqa: E402

    d = common.db(args.db)
    info = d.dataset_info.find_one({"_id": "payment_events"}) or {}
    cenario = d.degradation_scenarios.find_one({"kind": "recusa"})
    conta = d.demo_accounts.find_one(sort=[("eventos_24h", -1)])
    provedor = cenario["provedor_id"] if cenario else \
        d.provedores.find_one()["provedor_id"]

    resultados = {
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "database": os.environ["MONGODB_DB"],
        "events": info.get("events") or d.payment_events.estimated_document_count(),
        "days": info.get("days"),
        "accounts": info.get("accounts"),
        "providers": info.get("providers"),
        "runs": args.runs,
        "network_floor_ms": medir(lambda: d.command("hello"), args.runs),
        "cases": {},
    }

    casos = {
        "latencia_pix_1h": lambda: latency.serie("pix", None, 1, False),
        "latencia_pix_24h": lambda: latency.serie("pix", None, 24, False),
        "latencia_provedor_24h": lambda: latency.serie(None, provedor, 24, False),
        "latencia_provedor_24h_preenchida": lambda: latency.serie(None, provedor, 24, True),
        # Canal inteiro em 7 dias são ~27 M eventos: fica aqui de propósito, porque
        # o resultado ("não completa em 15 s") é um limite que o cliente precisa
        # ouvir. A interface nunca faz essa consulta — ela sempre escopa por provedor.
        "latencia_canal_7d_sem_provedor": lambda: latency.serie("pix", None, 168, False),
        "latencia_provedor_7d": lambda: latency.serie(None, provedor, 168, False),
        "saude_provedor_24h": lambda: providers.saude(provedor, 24),
        "saude_provedor_7d": lambda: providers.saude(provedor, 168),
        "ranking_1h": lambda: providers.ranking(1),
        "ranking_6h": lambda: providers.ranking(6),
        "velocity_conta": lambda: velocity.features(conta["conta_id"] if conta
                                                    else "C000000001"),
    }
    for nome, fn in casos.items():
        print(f"  {nome} ...", flush=True)
        resultados["cases"][nome] = medir(fn, args.runs)

    st = next(d.payment_events.aggregate(
        [{"$collStats": {"storageStats": {}}}]))["storageStats"]
    n = resultados["events"]
    resultados["storage"] = {
        "storage_bytes": st["storageSize"],
        "index_bytes": st.get("totalIndexSize", 0),
        "bytes_per_event": round(st["storageSize"] / max(n, 1), 2),
        "buckets": (st.get("timeseries") or {}).get("bucketCount"),
    }

    saida = os.path.join(ROOT, "queries", "bench-results.json")
    with open(saida, "w") as fh:
        json.dump(resultados, fh, indent=2)

    piso = resultados["network_floor_ms"]["p50"]
    print(f'\n{resultados["events"]:,} eventos · {resultados["days"]} dias · '
          f'{resultados["providers"]} provedores · piso de rede p50 {piso} ms\n')
    print(f'{"consulta":34} {"p50":>9} {"p95":>9} {"acima do piso":>15}')
    print("-" * 71)
    for nome, v in resultados["cases"].items():
        if v.get("timeout"):
            print(f'{nome:34} {"excedeu " + str(v["max_time_ms"]) + " ms":>27}')
        else:
            print(f'{nome:34} {v["p50"]:8.1f}ms {v["p95"]:8.1f}ms {v["p50"] - piso:14.1f}ms')
    print(f'\narmazenamento: {resultados["storage"]["bytes_per_event"]} B/evento, '
          f'{resultados["storage"]["storage_bytes"] / 1e6:,.0f} MB + '
          f'{resultados["storage"]["index_bytes"] / 1e6:,.0f} MB de índice, '
          f'{resultados["storage"]["buckets"]:,} buckets')
    print(saida)


if __name__ == "__main__":
    main()

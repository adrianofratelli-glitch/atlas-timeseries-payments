"""ADR 0001 — mede o span de bucket em vez de escolher por preferência.

Carrega a mesma amostra de eventos em quatro variantes de coleção time series e em
uma coleção normal, e mede o que o parâmetro decide de fato: armazenamento, taxa de
ingestão, a série de latência por percentil e a saúde do provedor.

`bucketMaxSpanSeconds` não pode ser alterado depois da criação da coleção. Por isso
esta medição vem antes de qualquer decisão de modelagem virar código.
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
sys.path.insert(0, os.path.join(ROOT, "data-generator"))
import common  # noqa: E402
import generate_events as ge  # noqa: E402

import numpy as np  # noqa: E402

VARIANTES = {
    "seconds": {"granularity": "seconds"},
    "minutes": {"granularity": "minutes"},
    "span1h": {"bucketMaxSpanSeconds": 3600, "bucketRoundingSeconds": 3600},
    "span1d": {"bucketMaxSpanSeconds": 86400, "bucketRoundingSeconds": 86400},
}


def amostra(d, alvo: int, seed: int):
    provedores = list(d.provedores.find())
    cenarios = list(d.degradation_scenarios.find())
    fim = common.utc_midnight(datetime.now(timezone.utc))
    for c in cenarios:
        if c["dia_offset"] is not None:
            c["_inicio"] = fim + timedelta(days=c["dia_offset"], hours=c["hora"])
    docs = []
    rng = np.random.default_rng(seed)
    for bloco in ge.gerar_dia(rng, fim - timedelta(days=1), provedores, cenarios,
                              150, 2_000_000):
        docs.extend(bloco)
        if len(docs) >= alvo:
            break
    return docs[:alvo], provedores[0]["provedor_id"]


def limpo(docs):
    """insert_many injeta _id no dicionário original; reusar a lista dá chave duplicada."""
    return [{k: v for k, v in x.items() if k != "_id"} for x in docs]


def medir(fn, runs: int) -> dict:
    for _ in range(3):
        fn()
    a = []
    for _ in range(runs):
        t0 = time.perf_counter()
        fn()
        a.append((time.perf_counter() - t0) * 1000)
    a.sort()
    return {"p50": round(statistics.median(a), 1),
            "p95": round(a[max(int(len(a) * 0.95) - 1, 0)], 1)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=400000)
    ap.add_argument("--runs", type=int, default=20)
    ap.add_argument("--batch", type=int, default=25000)
    ap.add_argument("--db", default=None)
    ap.add_argument("--seed", type=int, default=11)
    args = ap.parse_args()

    d = common.db(args.db)
    print(f"gerando {args.events:,} eventos...")
    docs, provedor = amostra(d, args.events, args.seed)
    inicio = min(x["ts"] for x in docs)
    fim = max(x["ts"] for x in docs) + timedelta(seconds=1)

    latencia_pipe = [
        {"$match": {"meta.canal": "pix", "ts": {"$gte": inicio, "$lt": fim}}},
        {"$group": {"_id": {"$dateTrunc": {"date": "$ts", "unit": "minute", "binSize": 5}},
                    "lat": {"$percentile": {"input": "$latencia_ms", "p": [0.5, 0.95, 0.99],
                                            "method": "approximate"}}}},
        {"$sort": {"_id": 1}},
    ]
    saude_pipe = [
        {"$match": {"meta.provedor": provedor, "ts": {"$gte": inicio, "$lt": fim}}},
        {"$group": {"_id": {"$dateTrunc": {"date": "$ts", "unit": "minute", "binSize": 5}},
                    "eventos": {"$sum": 1},
                    "recusados": {"$sum": {"$cond": ["$aprovado", 0, 1]}}}},
        {"$sort": {"_id": 1}},
    ]

    # "span1d_ordenado" carrega o mesmo dado ordenado por rota antes de inserir. Na
    # carga real os eventos chegam intercalados entre ~2.900 rotas e a coleção fechou
    # 2,6 milhões de buckets para 44,7 M eventos — 17 eventos por bucket. A ordem de
    # chegada decide a ocupação do bucket, e a ocupação decide compressão e ingestão.
    variantes = list(VARIANTES) + ["span1d_ordenado"]
    linhas, flat_bytes = [], None
    for nome in ["flat"] + variantes:
        col = f"bkt_{nome}"
        d[col].drop()
        if nome == "flat":
            d.create_collection(col)
        else:
            ts = {"timeField": "ts", "metaField": "meta"}
            ts.update(VARIANTES["span1d" if nome == "span1d_ordenado" else nome])
            d.create_collection(col, timeseries=ts)
        amostra_limpa = limpo(docs)
        if nome == "span1d_ordenado":
            amostra_limpa.sort(key=lambda x: (x["meta"]["canal"], x["meta"]["provedor"],
                                              x["meta"]["produto"], x["meta"]["uf"],
                                              x["ts"]))
        t0 = time.time()
        for i in range(0, len(amostra_limpa), args.batch):
            d[col].insert_many(amostra_limpa[i:i + args.batch], ordered=False)
        segundos = time.time() - t0
        d[col].create_index([("meta.provedor", 1), ("ts", 1)])
        d[col].create_index([("meta.canal", 1), ("ts", 1)])

        st = next(d[col].aggregate([{"$collStats": {"storageStats": {}}}]))["storageStats"]
        n = st.get("count") or d[col].estimated_document_count()
        linha = {
            "variante": nome,
            "eventos_por_bucket": round(n / max((st.get("timeseries") or {}).get("bucketCount") or 1, 1), 1),
            "eventos": n,
            "buckets": (st.get("timeseries") or {}).get("bucketCount"),
            "storage_mb": round(st["storageSize"] / 1e6, 2),
            "index_mb": round(st.get("totalIndexSize", 0) / 1e6, 2),
            "bytes_por_evento": round(st["storageSize"] / max(n, 1), 2),
            "ingestao_por_s": round(len(docs) / segundos),
            "latencia_ms": medir(lambda c=col: list(d[c].aggregate(latencia_pipe)), args.runs),
            "saude_ms": medir(lambda c=col: list(d[c].aggregate(saude_pipe)), args.runs),
        }
        if nome == "flat":
            flat_bytes = linha["bytes_por_evento"]
        linha["ratio_vs_flat"] = round(flat_bytes / linha["bytes_por_evento"], 2)
        linhas.append(linha)
        print(f'  {nome:9} carregada em {segundos:5.1f}s', flush=True)

    print(f"\namostra: {len(docs):,} eventos · provedor {provedor}\n")
    cab = (f'{"variante":16} {"buckets":>9} {"ev/bkt":>7} {"armazen.":>10} {"índice":>9} '
           f'{"B/evento":>9} {"ratio":>7} {"ingestão":>11} {"latência":>10} {"saúde":>9}')
    print(cab)
    print("-" * len(cab))
    for r in linhas:
        print(f'{r["variante"]:16} {r["buckets"] or 0:9,} {r["eventos_por_bucket"]:7.1f} '
              f'{r["storage_mb"]:9.2f}M {r["index_mb"]:8.2f}M {r["bytes_por_evento"]:9.2f} '
              f'{str(r["ratio_vs_flat"]) + "x":>7} {r["ingestao_por_s"]:10,}/s '
              f'{r["latencia_ms"]["p50"]:9.1f}ms {r["saude_ms"]["p50"]:8.1f}ms')

    saida = os.path.join(ROOT, "queries", "bucket-experiment.json")
    with open(saida, "w") as fh:
        json.dump({"measured_at": datetime.now(timezone.utc).isoformat(),
                   "events": len(docs), "provedor": provedor, "runs": args.runs,
                   "rows": linhas}, fh, indent=2)
    print(f"\n{saida}")
    for nome in ["flat"] + variantes:
        d[f"bkt_{nome}"].drop()


if __name__ == "__main__":
    main()

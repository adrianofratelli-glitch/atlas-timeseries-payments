"""ADR 0002 — o que acontece quando a conta entra no metaField.

É a primeira objeção de qualquer banco: "nós temos dezenas de milhões de contas,
isso não explode?". A resposta honesta não é uma opinião, é esta medição.

Duas modelagens da mesma amostra:

  A) `conta_id` como CAMPO de medição, `meta` só com a rota (canal, provedor,
     produto, UF) — poucos milhares de séries — mais um índice secundário
     `{conta_id: 1, ts: 1}`.
  B) `conta_id` dentro do `meta` — uma série por conta.

Mede armazenamento, número de buckets, taxa de ingestão e as duas consultas que
importam: o velocity de UMA conta (o caminho da autorização) e o p99 por provedor
(o caminho da observabilidade).
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "data-generator"))
import common  # noqa: E402
import generate_events as ge  # noqa: E402

import numpy as np  # noqa: E402


def montar_amostra(d, eventos_alvo: int, contas: int, seed: int):
    provedores = list(d.provedores.find())
    cenarios = list(d.degradation_scenarios.find())
    fim = common.utc_midnight(datetime.now(timezone.utc))
    for c in cenarios:
        if c["dia_offset"] is not None:
            c["_inicio"] = fim + timedelta(days=c["dia_offset"], hours=c["hora"])
    docs = []
    rng = np.random.default_rng(seed)
    for bloco in ge.gerar_dia(rng, fim - timedelta(days=1), provedores, cenarios,
                              150, contas):
        docs.extend(bloco)
        if len(docs) >= eventos_alvo:
            break
    return docs[:eventos_alvo]


def limpo(docs, meta_conta: bool):
    """insert_many injeta _id no dicionário original; reusar a lista dá chave duplicada."""
    saida = []
    for x in docs:
        meta = dict(x["meta"])
        if meta_conta:
            meta["conta_id"] = x["conta_id"]
        novo = {k: v for k, v in x.items() if k != "_id"}
        novo["meta"] = meta
        saida.append(novo)
    return saida


def carregar(d, nome: str, docs, meta_conta: bool, batch: int) -> float:
    d[nome].drop()
    d.create_collection(nome, timeseries={"timeField": "ts", "metaField": "meta",
                                          "bucketMaxSpanSeconds": 86400,
                                          "bucketRoundingSeconds": 86400})
    amostra = limpo(docs, meta_conta)
    t0 = time.time()
    for i in range(0, len(amostra), batch):
        d[nome].insert_many(amostra[i:i + batch], ordered=False)
    return time.time() - t0


def medir(fn, runs: int) -> dict:
    for _ in range(3):
        fn()
    amostras = []
    for _ in range(runs):
        t0 = time.perf_counter()
        fn()
        amostras.append((time.perf_counter() - t0) * 1000)
    amostras.sort()
    return {"p50": round(statistics.median(amostras), 1),
            "p95": round(amostras[max(int(len(amostras) * 0.95) - 1, 0)], 1)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=400000)
    ap.add_argument("--accounts", type=int, default=2000000)
    ap.add_argument("--runs", type=int, default=20)
    ap.add_argument("--batch", type=int, default=25000)
    ap.add_argument("--db", default=None)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    d = common.db(args.db)
    print(f"gerando {args.events:,} eventos sobre {args.accounts:,} contas...")
    docs = montar_amostra(d, args.events, args.accounts, args.seed)
    # Counter, não max() com um sum() por candidato: a versão ingênua é O(n²) sobre
    # 400 mil eventos e não termina.
    conta_alvo = Counter(x["conta_id"] for x in docs).most_common(1)[0][0]
    provedor_alvo = docs[0]["meta"]["provedor"]
    inicio = min(x["ts"] for x in docs)
    fim = max(x["ts"] for x in docs) + timedelta(seconds=1)

    linhas = []
    for nome, meta_conta, com_indice in (("card_campo", False, True),
                                         ("card_meta", True, False)):
        segundos = carregar(d, nome, docs, meta_conta, args.batch)
        if com_indice:
            t0 = time.time()
            d[nome].create_index([("conta_id", 1), ("ts", 1)])
            indice_s = time.time() - t0
        else:
            indice_s = 0.0
        st = next(d[nome].aggregate([{"$collStats": {"storageStats": {}}}]))["storageStats"]
        n = st.get("count") or d[nome].estimated_document_count()

        campo_conta = "meta.conta_id" if meta_conta else "conta_id"
        velocity = [
            {"$match": {campo_conta: conta_alvo, "ts": {"$gte": inicio, "$lt": fim}}},
            {"$group": {"_id": None, "eventos": {"$sum": 1},
                        "valor": {"$sum": "$valor"}}},
        ]
        p99 = [
            {"$match": {"meta.provedor": provedor_alvo, "ts": {"$gte": inicio, "$lt": fim}}},
            {"$group": {"_id": {"$dateTrunc": {"date": "$ts", "unit": "minute",
                                               "binSize": 5}},
                        "lat": {"$percentile": {"input": "$latencia_ms", "p": [0.99],
                                                "method": "approximate"}}}},
        ]

        linhas.append({
            "modelagem": "conta_id como campo + índice" if not meta_conta
                         else "conta_id dentro do metaField",
            "colecao": nome,
            "eventos": n,
            "buckets": (st.get("timeseries") or {}).get("bucketCount"),
            "storage_mb": round(st["storageSize"] / 1e6, 2),
            "index_mb": round(st.get("totalIndexSize", 0) / 1e6, 2),
            "bytes_por_evento": round(st["storageSize"] / max(n, 1), 2),
            "ingestao_por_s": round(len(docs) / segundos),
            "indice_s": round(indice_s, 1),
            "velocity_ms": medir(lambda c=nome, p=velocity: list(d[c].aggregate(p)),
                                 args.runs),
            "p99_provedor_ms": medir(lambda c=nome, p=p99: list(d[c].aggregate(p)),
                                     args.runs),
        })

    print(f"\namostra: {len(docs):,} eventos · {args.accounts:,} contas · "
          f"conta alvo {conta_alvo} · provedor {provedor_alvo}\n")
    cab = (f'{"modelagem":34} {"buckets":>9} {"armazen.":>10} {"índice":>9} '
           f'{"B/evento":>9} {"ingestão":>10} {"velocity":>10} {"p99 prov.":>10}')
    print(cab)
    print("-" * len(cab))
    for r in linhas:
        print(f'{r["modelagem"]:34} {r["buckets"] or 0:9,} {r["storage_mb"]:9.2f}M '
              f'{r["index_mb"]:8.2f}M {r["bytes_por_evento"]:9.2f} '
              f'{r["ingestao_por_s"]:9,}/s {r["velocity_ms"]["p50"]:9.1f}ms '
              f'{r["p99_provedor_ms"]["p50"]:9.1f}ms')

    saida = os.path.join(ROOT, "queries", "cardinality-experiment.json")
    with open(saida, "w") as fh:
        json.dump({"measured_at": datetime.now(timezone.utc).isoformat(),
                   "events": len(docs), "accounts": args.accounts,
                   "conta_alvo": conta_alvo, "provedor_alvo": provedor_alvo,
                   "runs": args.runs, "rows": linhas}, fh, indent=2)
    print(f"\n{saida}")

    for nome in ("card_campo", "card_meta"):
        d[nome].drop()


if __name__ == "__main__":
    main()

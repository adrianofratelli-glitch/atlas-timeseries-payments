"""Leituras de 15 em 15 minutos: medidores e medidor de fronteira do transformador.

Duas propriedades importam mais que a velocidade:

1. O medidor grava o que **registra**, não o que o cliente consumiu. Um medidor
   adulterado registra `register_factor` do consumo real, e a diferença é exatamente
   o que o balanço do transformador precisa encontrar.
2. O medidor de fronteira grava o que foi **entregue**: soma do consumo real abaixo
   dele mais a perda técnica. É a única fonte da verdade do balanço.

A coleção time series não tem _id controlado pelo cliente, logo não tem upsert:
recarregar leitura é --drop. Isso é propriedade do recurso, não descuido.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common  # noqa: E402

# Variantes de bucket comparadas no ADR 0001. `None` = deixa o servidor decidir
# pelo granularity.
VARIANTS = {
    "seconds":  {"granularity": "seconds"},                # span 1 h  →  4 medições
    "minutes":  {"granularity": "minutes"},                # span 24 h → 96 medições
    # Escolhida pelo ADR 0001; os valores vêm do .env para que schema e ambiente
    # não divirjam silenciosamente.
    "span1d":   {"bucketMaxSpanSeconds": int(os.getenv("TS_BUCKET_MAX_SPAN_SECONDS", "86400")),
                 "bucketRoundingSeconds": int(os.getenv("TS_BUCKET_ROUNDING_SECONDS", "86400"))},
    "span7d":   {"bucketMaxSpanSeconds": 604800, "bucketRoundingSeconds": 604800},
}


def ensure_collection(d, name: str, variant: str | None, flat: bool, drop: bool):
    if drop and name in d.list_collection_names():
        d[name].drop()
    if name in d.list_collection_names():
        return d[name]
    if flat:
        return d.create_collection(name)
    ts = {"timeField": "ts", "metaField": "meta"}
    ts.update(VARIANTS[variant])
    opts = {"timeseries": ts}
    expire = int(os.getenv("TS_EXPIRE_AFTER_SECONDS", "0"))
    if expire > 0:
        opts["expireAfterSeconds"] = expire
    return d.create_collection(name, **opts)


def day_docs(meters, transformers, day, rng):
    """Emite as medições de um dia: registradas por medidor + entregues por fronteira.

    Gerador, não lista: 20 000 medidores × 96 intervalos materializados de uma vez
    são cerca de 3 GB de dicionários em memória.
    """
    ts_slots = common.slots(day)
    weekday = day.weekday()
    temp = common.temperature_factor(day, rng)

    delivered = {t["transformer_id"]: np.zeros(common.PER_DAY) for t in transformers}

    for m in meters:
        real = common.day_curve(rng, m["customer_class"], m["kwh_dia_base"], weekday, temp)
        delivered[m["transformer_id"]] += real
        registered = real * m["register_factor"]

        outage = m.get("seeded_outage")
        sag = m.get("seeded_voltage_sag")
        skip = set()
        if outage:
            first = outage["start_hour"] * (60 // common.INTERVAL_MIN)
            skip = set(range(first, first + outage["hours"] * (60 // common.INTERVAL_MIN)))

        volts = rng.normal(220.0, 2.2, common.PER_DAY)
        if sag:
            first = sag["start_hour"] * (60 // common.INTERVAL_MIN)
            volts[first:first + sag["hours"] * (60 // common.INTERVAL_MIN)] = \
                rng.normal(sag["volts"], 1.5, sag["hours"] * (60 // common.INTERVAL_MIN))

        meta = {"meter_id": m["meter_id"], "transformer_id": m["transformer_id"],
                "feeder_id": m["feeder_id"], "phase": m["phase"], "kind": "medidor"}
        for i, t in enumerate(ts_slots):
            if i in skip:
                continue
            kwh = float(registered[i])
            yield {
                "ts": t, "meta": meta,
                "kwh": round(kwh, 4),
                "voltage": round(float(volts[i]), 1),
                # Corrente aparente a partir da energia do intervalo, fator ~0.92.
                "current": round(kwh * 4000.0 / max(float(volts[i]), 1.0) / 0.92, 3),
                "power_factor": round(float(rng.normal(0.93, 0.02)), 3),
                "quality": "ok",
            }

    for t in transformers:
        loss = 1.0 + t["technical_loss"]
        series = delivered[t["transformer_id"]] * loss
        meta = {"meter_id": t["boundary_meter_id"], "transformer_id": t["transformer_id"],
                "feeder_id": t["feeder_id"], "phase": "ABC", "kind": "fronteira"}
        for i, ts_at in enumerate(ts_slots):
            yield {
                "ts": ts_at, "meta": meta,
                "kwh": round(float(series[i]), 4),
                "voltage": round(float(rng.normal(13800.0, 60.0)), 1),
                "current": round(float(series[i]) * 4000.0 / 13800.0, 3),
                "power_factor": round(float(rng.normal(0.96, 0.01)), 3),
                "quality": "ok",
            }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=int(os.getenv("DAYS", "30")))
    ap.add_argument("--collection", default="readings")
    ap.add_argument("--variant", default="span1d", choices=list(VARIANTS))
    ap.add_argument("--flat", action="store_true", help="coleção normal, para comparação")
    ap.add_argument("--transformers", type=int, default=0, help="0 = todos")
    ap.add_argument("--drop", action="store_true")
    ap.add_argument("--batch", type=int, default=20000)
    ap.add_argument("--db", default=None)
    ap.add_argument("--seed", type=int, default=common.SEED)
    args = ap.parse_args()

    d = common.db(args.db)
    transformers = list(d.transformers.find().sort("transformer_id", 1))
    if args.transformers:
        transformers = transformers[:args.transformers]
    keep = {t["transformer_id"] for t in transformers}
    meters = [m for m in d.meters.find({"transformer_id": {"$in": list(keep)}})]
    if not meters:
        sys.exit("sem medidores: rode generate_assets.py antes")

    col = ensure_collection(d, args.collection, args.variant, args.flat, args.drop)

    end = common.utc_midnight(datetime.now(timezone.utc))
    start = end - timedelta(days=args.days)
    rng = np.random.default_rng(args.seed)

    total, t0, buf = 0, time.time(), []
    for n in range(args.days):
        day = start + timedelta(days=n)
        for doc in day_docs(meters, transformers, day, rng):
            buf.append(doc)
            if len(buf) >= args.batch:
                col.insert_many(buf, ordered=False)
                total += len(buf)
                buf = []
        rate = total / max(time.time() - t0, 1e-6)
        print(f"  dia {n + 1}/{args.days}  {total:>10,} docs  {rate:>8,.0f} docs/s", flush=True)
    if buf:
        col.insert_many(buf, ordered=False)
        total += len(buf)

    # Âncora do dataset: a faixa consultada precisa terminar na última medição, não
    # no relógio. Sem isso uma janela de 7 dias devolvia 146 horas em vez de 168,
    # porque as últimas horas do intervalo simplesmente não existem no dado.
    d.dataset_info.update_one(
        {"_id": args.collection},
        {"$set": {"collection": args.collection, "first_ts": start, "last_ts": end,
                  "days": args.days, "meters": len(meters),
                  "transformers": len(transformers), "measurements": total,
                  "interval_minutes": common.INTERVAL_MIN, "variant": args.variant,
                  "flat": bool(args.flat), "loaded_at": datetime.now(timezone.utc)}},
        upsert=True)

    elapsed = time.time() - t0
    print(f"\n{args.collection}: {total:,} medições em {elapsed:,.1f}s "
          f"({total / elapsed:,.0f} docs/s), {len(meters):,} medidores + "
          f"{len(transformers):,} fronteiras, {args.days} dias")


if __name__ == "__main__":
    main()

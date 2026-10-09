"""Mede no cluster conectado cada restrição de time series citada em LIMITATIONS.md.

    .venv/bin/python queries/feature_probes.py --out /tmp/feature_probes.json

Cria uma coleção descartável num banco `*_test` (padrão `trilho_feature_probes_test`),
executa a operação e registra o que o servidor respondeu — aceito ou o código de erro.
Apaga o banco no fim. O resultado vale para a versão do servidor que ele imprime; em
outra versão, rode de novo antes de repetir a afirmação.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(ROOT, ".env"))
sys.path.insert(0, os.path.join(ROOT, "data-generator"))
import common  # noqa: E402


def _try(fn):
    try:
        value = fn()
        return {"accepted": True, "result": value}
    except PyMongoError as exc:
        return {"accepted": False, "error": type(exc).__name__,
                "code": getattr(exc, "code", None), "message": str(exc)[:200]}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="trilho_feature_probes_test")
    p.add_argument("--out")
    a = p.parse_args()
    common.guard_write(a.db)
    client = MongoClient(os.environ["MONGODB_URI"])
    d = client[a.db]
    client.drop_database(a.db)
    now = datetime.now(timezone.utc)
    ts = {"timeField": "ts", "metaField": "meta",
          "bucketMaxSpanSeconds": 300, "bucketRoundingSeconds": 300}
    fcv = client.admin.command({"getParameter": 1, "featureCompatibilityVersion": 1})
    out: dict = {"server_version": client.server_info()["version"], "db": a.db,
                 "fcv": fcv.get("featureCompatibilityVersion", {}).get("version"),
                 "measured_at": now.isoformat(), "probes": {}}
    pr = out["probes"]
    try:
        d.create_collection("probe", timeseries=dict(ts), expireAfterSeconds=3600)
        col = d.probe

        def explicit_id():
            col.insert_one({"_id": "id-1", "ts": now, "meta": "r", "v": 1})
            return bool(col.find_one({"_id": "id-1"}))
        pr["explicit_id_insert"] = _try(explicit_id)

        def duplicate_id():
            col.insert_one({"_id": "id-1", "ts": now, "meta": "r", "v": 2})
            return col.count_documents({"_id": "id-1"})
        pr["duplicate_id_insert"] = _try(duplicate_id)

        pr["unique_index"] = _try(lambda: col.create_index("v", unique=True))

        def upsert():
            r = col.update_one({"meta": "novo", "ts": now}, {"$set": {"v": 9}}, upsert=True)
            return {"upserted_id": str(r.upserted_id),
                    "found": col.count_documents({"meta": "novo"})}
        pr["upsert"] = _try(upsert)

        def update_measurement():
            r = col.update_many({"_id": "id-1"}, {"$set": {"v": 3}})
            return {"modified": r.modified_count}
        pr["update_measurement_field"] = _try(update_measurement)

        def update_meta_field():
            r = col.update_many({"meta": "r"}, {"$set": {"meta": "r2"}})
            return {"modified": r.modified_count}
        pr["update_meta_field"] = _try(update_meta_field)

        def delete_by_id():
            r = col.delete_many({"_id": "id-1"})
            return {"deleted": r.deleted_count}
        pr["delete_by_measurement_field"] = _try(delete_by_id)

        def collmod(span):
            def run():
                d.command({"collMod": "probe", "timeseries": {
                    "bucketMaxSpanSeconds": span, "bucketRoundingSeconds": span}})
                return d.probe.options()["timeseries"]
            return run
        pr["collmod_bucket_increase_300_to_600"] = _try(collmod(600))
        pr["collmod_bucket_decrease_600_to_300"] = _try(collmod(300))

        def collection_watch():
            with col.watch(max_await_time_ms=500) as stream:
                col.insert_one({"ts": now, "meta": "r", "v": 4})
                return {"event": stream.try_next() is not None}
        pr["change_stream_on_collection"] = _try(collection_watch)

        def database_watch():
            seen = []
            with d.watch(max_await_time_ms=500) as stream:
                col.insert_one({"ts": now, "meta": "r", "v": 5})
                d.plain.insert_one({"x": 1})
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline and len(seen) < 3:
                    ev = stream.try_next()
                    if ev:
                        seen.append(ev["ns"]["coll"])
            return {"namespaces_seen": seen}
        pr["change_stream_on_database"] = _try(database_watch)

        def rename():
            d.probe.rename("probe_renamed")
            info = next(iter(d.list_collections(filter={"name": "probe_renamed"})), {})
            return {"type": info.get("type"),
                    "timeseries": bool(info.get("options", {}).get("timeseries"))}
        pr["rename"] = _try(rename)
    finally:
        client.drop_database(a.db)
    text = json.dumps(out, indent=2, ensure_ascii=False, default=str)
    print(text)
    if a.out:
        with open(a.out, "w") as fh:
            fh.write(text + "\n")


if __name__ == "__main__":
    main()

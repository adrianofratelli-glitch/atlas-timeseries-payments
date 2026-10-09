"""Regressão do ciclo de vida da ingestão ao vivo contra o cluster real (banco *_test).

    backend/venv/bin/python -m unittest tests/test_live_lifecycle.py -v

Cobre a corrida em que `clear()` voltava com o escritor ainda vivo e o insert atrasado
recriava `payment_events_live` como coleção **comum** (sem timeSeries e sem TTL), e a
corrida `start` + `clear` concorrentes que respondia HTTP 500
(`RuntimeError: cannot join thread before it is started`). Usa `LIFECYCLE_DB`
(padrão `trilho_lifecycle_test`), apagado no fim.
"""
from __future__ import annotations

import os
import sys
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.getenv("LIFECYCLE_DB", "trilho_lifecycle_test")
if not DB.endswith("_test"):
    raise SystemExit(f"recusado: {DB} não é um banco *_test")
os.environ["MONGODB_DB"] = DB
sys.path.insert(0, os.path.join(ROOT, "backend"))

from app.db.client import db, insert_idempotent  # noqa: E402
from app.services import live  # noqa: E402

PROVEDORES = [
    {"provedor_id": "PSP-T01", "canal": "pix", "participacao": 1.0, "recusa_base": 0.02},
    {"provedor_id": "ADQ-T01", "canal": "cartao", "participacao": 1.0, "recusa_base": 0.05},
    {"provedor_id": "TED-T01", "canal": "ted", "participacao": 1.0, "recusa_base": 0.01},
]


def _info():
    return next(iter(db().list_collections(filter={"name": live.COLLECTION})), None)


class LiveLifecycleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        assert db().name == DB
        db().client.drop_database(DB)
        db().provedores.insert_many([dict(p) for p in PROVEDORES])

    @classmethod
    def tearDownClass(cls):
        db().client.drop_database(DB)

    def setUp(self):
        self.original = live.insert_idempotent
        self.feed = live.LiveFeed()

    def tearDown(self):
        live.insert_idempotent = self.original
        self.feed.stop()

    def assertTimeseriesWithTtl(self):
        info = _info()
        self.assertIsNotNone(info, "coleção ao vivo ausente depois do clear")
        self.assertEqual(info["type"], "timeseries", info)
        self.assertEqual(info["options"]["timeseries"]["timeField"], "ts")
        self.assertEqual(info["options"].get("expireAfterSeconds"), live.LIVE_TTL_SECONDS)

    def test_clear_com_escrita_atrasada_nao_recria_colecao_comum(self):
        entered, release = threading.Event(), threading.Event()

        def atrasado(col, docs, *a, **kw):
            entered.set()
            release.wait(3)
            return self.original(col, docs, *a, **kw)

        live.insert_idempotent = atrasado
        starter = threading.Thread(target=lambda: self.feed.start(5))
        starter.start()
        self.assertTrue(entered.wait(20), "escritor não começou")
        t = time.monotonic()
        result = self.feed.clear()
        elapsed = time.monotonic() - t
        writer_alive = self.feed._thread.is_alive()
        release.set()
        starter.join(10)
        self.assertFalse(writer_alive, "clear voltou com o escritor vivo")
        self.assertEqual(result["state"], "parado")
        self.assertGreaterEqual(elapsed, 2.0, "clear não esperou o escritor")
        self.assertTimeseriesWithTtl()
        self.assertEqual(db()[live.COLLECTION].count_documents({}), 0,
                         "lote de época vencida foi gravado")

    def test_clear_espera_escrita_em_voo_no_portao(self):
        self.feed.start(5)
        segurou = threading.Event()

        def escrita_lenta():
            with self.feed._gate.admit(self.feed._epoch):
                segurou.set()
                time.sleep(2)

        th = threading.Thread(target=escrita_lenta)
        th.start()
        segurou.wait(5)
        t = time.monotonic()
        self.feed.clear()
        self.assertGreaterEqual(time.monotonic() - t, 1.5, "clear não esperou o portão")
        th.join()
        self.assertTimeseriesWithTtl()
        self.assertEqual(db()[live.COLLECTION].count_documents({}), 0)

    def test_insert_de_epoca_vencida_nunca_cria_colecao(self):
        db()[live.COLLECTION].drop()
        gate = live.WriteGate()
        epoch = gate.epoch
        with gate.exclusive(1):
            pass
        docs = [{"ts": datetime.now(timezone.utc), "meta": {"canal": "pix"}, "valor": 1.0}]
        gravados = insert_idempotent(db()[live.COLLECTION], docs,
                                     gate=lambda: gate.admit(epoch))
        self.assertEqual(gravados, 0)
        self.assertIsNone(_info(), "insert descartado criou coleção implícita")

    def test_ensure_collection_substitui_colecao_comum(self):
        db()[live.COLLECTION].drop()
        db()[live.COLLECTION].insert_one({"ts": datetime.now(timezone.utc)})
        self.assertEqual(_info()["type"], "collection")
        live.ensure_collection()
        self.assertTimeseriesWithTtl()

    def test_escritor_recusa_colecao_comum_criada_por_fora(self):
        self.feed.start(5)
        # Alguém troca a coleção por uma comum com o feed rodando (mesma época): o
        # próximo lote é recusado sob o portão e o feed para com erro, sem gravar.
        with self.feed._gate._lock:
            db()[live.COLLECTION].drop()
            db().create_collection(live.COLLECTION)
        deadline = time.monotonic() + 10
        while self.feed._thread.is_alive() and time.monotonic() < deadline:
            time.sleep(0.1)
        self.assertFalse(self.feed._thread.is_alive())
        self.assertEqual(self.feed.status()["state"], "erro")
        self.assertIn("não time series", self.feed.status()["last_error"])
        self.assertEqual(db()[live.COLLECTION].count_documents({}), 0)
        # O próximo start corrige a coleção para time series.
        self.feed.start(5)
        self.assertTimeseriesWithTtl()

    def test_start_e_clear_concorrentes_nao_quebram(self):
        erros = []

        def chamar(fn):
            try:
                return fn()
            except Exception as exc:  # noqa: BLE001
                erros.append(f"{type(exc).__name__}: {exc}")

        for _ in range(8):
            with ThreadPoolExecutor(max_workers=4) as ex:
                futs = [ex.submit(chamar, lambda: self.feed.start(20)),
                        ex.submit(chamar, self.feed.clear),
                        ex.submit(chamar, lambda: self.feed.start(20)),
                        ex.submit(chamar, self.feed.stop)]
                [f.result() for f in futs]
        self.assertEqual(erros, [])
        final = self.feed.clear()
        self.assertEqual(final["state"], "parado")
        self.assertEqual(final["written"], 0)
        self.assertFalse(self.feed._thread.is_alive())
        self.assertTimeseriesWithTtl()


if __name__ == "__main__":
    unittest.main()

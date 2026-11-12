"""Núcleo da prova ao vivo, sem cluster: idempotência, portão, pipeline e estados.

Roda no CI (sem Atlas). Cada teste fixa uma promessa do README que antes só era
verificada com o cluster ligado (`test_live_lifecycle.py`, suítes adversariais).
"""
import os
import sys
import threading
import unittest
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

os.environ.setdefault("MONGODB_URI", "mongodb://localhost:27017")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

from bson import ObjectId  # noqa: E402
from pymongo.errors import AutoReconnect, BulkWriteError  # noqa: E402

import app.db.client as client_mod  # noqa: E402
import app.db.live as live_db  # noqa: E402
import app.db.ranges as ranges  # noqa: E402
import app.services.live as live_svc  # noqa: E402


class FakeTimeSeries:
    """Coleção em memória sem índice único em `_id`, como uma time series."""

    def __init__(self, lose_ack_on_attempt: int | None = None, keep_first: int = 0):
        self.docs: list[dict] = []
        self.calls = 0
        self.lose_ack_on_attempt = lose_ack_on_attempt
        self.keep_first = keep_first

    def insert_many(self, docs, ordered=False):
        self.calls += 1
        if self.calls == self.lose_ack_on_attempt:
            # Parte do lote chegou ao servidor e o ack se perdeu na rede.
            self.docs.extend(dict(d) for d in docs[: self.keep_first])
            raise AutoReconnect("ack perdido")
        self.docs.extend(dict(d) for d in docs)

    def find(self, filtro, projection=None):
        ids = set(filtro["_id"]["$in"])
        return [{"_id": d["_id"]} for d in self.docs if d["_id"] in ids]


def _lote(n: int) -> list[dict]:
    base = datetime(2026, 11, 12, 10, tzinfo=timezone.utc)
    return [{"ts": base + timedelta(milliseconds=i), "valor": float(i)} for i in range(n)]


class InsertIdempotentTests(unittest.TestCase):
    def setUp(self):
        self._sleep = patch.object(client_mod.time, "sleep", lambda _s: None)
        self._sleep.start()

    def tearDown(self):
        self._sleep.stop()

    def test_retry_after_lost_ack_does_not_duplicate(self):
        col = FakeTimeSeries(lose_ack_on_attempt=1, keep_first=30)
        gravados = client_mod.insert_idempotent(col, _lote(100))
        self.assertEqual(gravados, 100)
        self.assertEqual(len(col.docs), 100)
        self.assertEqual(len({d["_id"] for d in col.docs}), 100)

    def test_ids_are_fixed_before_first_attempt(self):
        lote = _lote(5)
        client_mod.insert_idempotent(FakeTimeSeries(), lote)
        self.assertTrue(all(isinstance(d["_id"], ObjectId) for d in lote))

    def test_validation_error_is_not_retried(self):
        col = MagicMock()
        col.insert_many.side_effect = BulkWriteError(
            {"writeErrors": [{"index": 0, "code": 121, "errmsg": "inválido"}]})
        with self.assertRaises(BulkWriteError):
            client_mod.insert_idempotent(col, _lote(3))
        self.assertEqual(col.insert_many.call_count, 1)

    def test_closed_gate_discards_batch(self):
        col = FakeTimeSeries()

        @contextmanager
        def fechado():
            yield False

        self.assertEqual(client_mod.insert_idempotent(col, _lote(10), gate=fechado), 0)
        self.assertEqual(col.docs, [])


class WriteGateTests(unittest.TestCase):
    def test_stale_epoch_is_refused(self):
        gate = live_svc.WriteGate()
        epoch = gate.epoch
        with gate.exclusive(timeout=1):
            pass
        with gate.admit(epoch) as permitido:
            self.assertFalse(permitido)
        with gate.admit(gate.epoch) as permitido:
            self.assertTrue(permitido)

    def test_exclusive_times_out_with_live_busy_while_write_in_flight(self):
        gate = live_svc.WriteGate()
        em_voo, liberar = threading.Event(), threading.Event()

        def escritor():
            with gate.admit(gate.epoch):
                em_voo.set()
                liberar.wait(5)

        t = threading.Thread(target=escritor)
        t.start()
        em_voo.wait(5)
        try:
            with self.assertRaises(live_svc.LiveBusy):
                with gate.exclusive(timeout=0.05):
                    pass
        finally:
            liberar.set()
            t.join(5)


class OverviewPipelineTests(unittest.TestCase):
    """O pipeline exibido no drawer é o executado, e exclui o segundo em escrita."""

    def _overview(self, **kwargs):
        captured = {}
        col = MagicMock()

        def aggregate(pipe, maxTimeMS):
            captured["pipe"], captured["maxTimeMS"] = pipe, maxTimeMS
            return iter([])

        col.aggregate.side_effect = aggregate
        fake_db = MagicMock()
        fake_db.__getitem__.return_value = col
        fake_db.name = "trilho_test"
        with patch.object(live_db, "db", return_value=fake_db), \
             patch.object(live_db, "_collection_options", return_value={}), \
             patch.object(live_db, "_bucket_snapshot", return_value=None):
            out = live_db.overview(**kwargs)
        return out, captured

    def test_pipeline_returned_is_the_one_executed(self):
        out, captured = self._overview()
        self.assertIs(out["pipeline"], captured["pipe"])
        self.assertEqual(captured["maxTimeMS"], live_db.MAX_TIME_MS)

    def test_current_second_and_unconfirmed_batch_are_excluded(self):
        ultimo = datetime.now(timezone.utc) - timedelta(seconds=3, microseconds=500)
        out, captured = self._overview(last_document={"ts": ultimo})
        filtro = captured["pipe"][0]["$match"]["ts"]
        self.assertEqual(filtro["$lt"], ultimo.replace(microsecond=0))
        self.assertEqual(filtro["$lt"].microsecond, 0)
        self.assertEqual(filtro["$lt"] - filtro["$gte"], timedelta(seconds=60))

    def test_session_start_bounds_the_curve_and_points_are_capped(self):
        inicio = datetime.now(timezone.utc) - timedelta(seconds=10)
        out, captured = self._overview(session_started_at=inicio)
        self.assertEqual(captured["pipe"][0]["$match"]["ts"]["$gte"], inicio)
        limites = [etapa["$limit"] for etapa in captured["pipe"] if "$limit" in etapa]
        self.assertEqual(len(limites), 1)
        self.assertLessEqual(limites[0], 60)
        self.assertEqual(out["granularity"]["unit"], "second")

    def test_sort_uses_group_key_so_sbe_is_kept(self):
        # ADR 0003: `$sort` por campo criado em `$set` tirava o pipeline do SBE.
        _, captured = self._overview()
        pipe = captured["pipe"]
        nomes = [next(iter(etapa)) for etapa in pipe]
        self.assertNotIn("$set", nomes)
        self.assertEqual(pipe[nomes.index("$sort")]["$sort"], {"_id": 1})
        self.assertLess(nomes.index("$limit"), nomes.index("$project"))
        self.assertEqual(pipe[-1]["$project"]["ts"], "$_id")


class PlanSummaryTests(unittest.TestCase):
    """`resumir_plano` lê os dois formatos de explain que o 9.0 devolve."""

    def test_query_planner_sbe_text_reports_block_stages(self):
        explain = {"explainVersion": "2", "serverInfo": {"version": "9.0.7"},
                   "queryPlanner": {"winningPlan": {
                       "queryPlan": {"stage": "GROUP", "inputStage": {
                           "stage": "UNPACK_TS_BUCKET",
                           "inputStage": {"stage": "CLUSTERED_IXSCAN"}}},
                       "slotBasedPlan": {"stages": (
                           "[3] project [s25 = newBsonObj()]\n"
                           "[2] block_to_row blocks[s12]\n[2] block_group bitset = s9\n"
                           "[1] ts_bucket_to_cellblock s5 pathReqs[]")}}}}
        resumo = live_db.resumir_plano(explain)
        self.assertEqual(resumo["engine"], "sbe")
        self.assertTrue(resumo["block_processing"])
        self.assertEqual(resumo["block_stages"],
                         ["block_to_row", "block_group", "ts_bucket_to_cellblock"])
        self.assertEqual(resumo["server_version"], "9.0.7")

    def test_classic_plan_has_no_block_processing(self):
        explain = {"stages": [{"$cursor": {"queryPlanner": {"winningPlan": {
            "stage": "FETCH", "inputStage": {"stage": "IXSCAN"}}}}},
            {"$_internalUnpackBucket": {}}, {"$group": {}}]}
        resumo = live_db.resumir_plano(explain)
        self.assertEqual(resumo["engine"], "classic")
        self.assertFalse(resumo["block_processing"])
        self.assertEqual(resumo["stages"], ["FETCH", "IXSCAN"])


class RangesTests(unittest.TestCase):
    def test_server_chooses_granularity(self):
        fim = datetime(2026, 11, 12, tzinfo=timezone.utc)
        self.assertEqual(ranges.resolve(1, fim)[2:], ("minute", 1))
        self.assertEqual(ranges.resolve(6, fim)[2:], ("minute", 5))
        self.assertEqual(ranges.resolve(24, fim)[2:], ("minute", 15))
        self.assertEqual(ranges.resolve(72, fim)[2:], ("hour", 1))

    def test_invalid_windows_are_domain_errors(self):
        fim = datetime(2026, 11, 12, tzinfo=timezone.utc)
        with self.assertRaises(ranges.RangeError):
            ranges.resolve(0, fim)
        with self.assertRaises(ranges.RangeError):
            ranges.resolve(ranges.MAX_RANGE_DAYS * 24 + 1, fim)


class FeedStateTests(unittest.TestCase):
    def test_empty_database_is_an_error_not_a_stop(self):
        fake_db = MagicMock()
        fake_db.provedores.find.return_value = []
        feed = live_svc.LiveFeed()
        with patch.object(live_svc, "db", return_value=fake_db), \
             patch.object(live_svc, "ensure_collection", return_value=MagicMock()):
            status = feed.start(eps=10)
            feed._thread.join(5)
        self.assertEqual(status["state"], "erro")
        self.assertIn("sem provedores", status["last_error"])
        self.assertEqual(status["written"], 0)


if __name__ == "__main__":
    unittest.main()

"""Ingestão hostil direto no cluster, num banco *_test descartável.

    .venv/bin/python -m unittest discover -s tests/adversarial -p "test_ingest_*.py" -v

Cobre o que a API não deixa o usuário fazer, mas um produtor real faz: queda da
conexão no meio de um lote, lotes concorrentes fora de ordem, duplicatas, timestamps
em epoch 0 e no futuro, tipos errados no timeField, metaField hostil, e retomada do
gerador depois de uma carga interrompida. Usa o banco `ADVERSARIAL_DB`
(padrão `trilho_adversarial_test`), que é apagado no fim.
"""
from __future__ import annotations

import os
import random
import subprocess
import sys
import threading
import unittest
from datetime import datetime, timedelta, timezone

from pymongo.errors import AutoReconnect, BulkWriteError, OperationFailure

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "data-generator"))
os.environ.setdefault("MONGODB_DB", "trilho_adversarial_test")
sys.path.insert(0, os.path.join(ROOT, "backend"))

import common  # noqa: E402
from app.db.client import insert_idempotent  # noqa: E402

DB = os.getenv("ADVERSARIAL_DB", "trilho_adversarial_test")
common.guard_write(DB)


def _docs(n: int, inicio: datetime, provedor: str = "PSP-001") -> list[dict]:
    return [{"ts": inicio + timedelta(milliseconds=7 * i),
             "meta": {"canal": "pix", "provedor": provedor, "produto": "pix_qr", "uf": "SP"},
             "valor": 10.0, "latencia_ms": 80.0, "aprovado": True, "erro": None,
             "conta_id": f"C{i:09d}"} for i in range(n)]


class AckPerdido:
    """Proxy de coleção: grava de verdade e então simula a conexão caindo."""

    def __init__(self, col, falhas: int, antes_de_gravar: bool = False):
        self.col, self.falhas, self.antes = col, falhas, antes_de_gravar

    def insert_many(self, docs, ordered=True):
        if self.falhas:
            self.falhas -= 1
            if not self.antes:
                self.col.insert_many(docs, ordered=ordered)
            raise AutoReconnect("conexão caiu antes do ack")
        return self.col.insert_many(docs, ordered=ordered)

    def find(self, *a, **k):
        return self.col.find(*a, **k)


class IngestaoHostil(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.d = common.db(DB)
        cls.d.drop_collection("ts")
        cls.d.create_collection("ts", timeseries={"timeField": "ts", "metaField": "meta",
                                                  "granularity": "seconds"})
        cls.d.ts.create_index([("ts", 1)])
        cls.agora = datetime.now(timezone.utc).replace(microsecond=0)

    @classmethod
    def tearDownClass(cls):
        common.client().drop_database(DB)

    def _conta(self, inicio, fim):
        return self.d.ts.count_documents({"ts": {"$gte": inicio, "$lt": fim}})

    def test_queda_depois_do_write_nao_duplica(self):
        inicio = self.agora - timedelta(hours=1)
        docs = _docs(2000, inicio)
        gravados = insert_idempotent(AckPerdido(self.d.ts, falhas=2), docs)
        self.assertEqual(gravados, 2000)
        self.assertEqual(self._conta(inicio, inicio + timedelta(minutes=1)), 2000)

    def test_queda_antes_do_write_grava_tudo(self):
        inicio = self.agora - timedelta(hours=2)
        docs = _docs(1500, inicio)
        self.assertEqual(insert_idempotent(AckPerdido(self.d.ts, 1, antes_de_gravar=True),
                                           docs), 1500)
        self.assertEqual(self._conta(inicio, inicio + timedelta(minutes=1)), 1500)

    def test_queda_persistente_sobe_o_erro(self):
        with self.assertRaises(AutoReconnect):
            insert_idempotent(AckPerdido(self.d.ts, falhas=99, antes_de_gravar=True),
                              _docs(10, self.agora - timedelta(hours=3)), attempts=2)

    def test_duplicata_ingenua_dobra_a_contagem(self):
        # Comportamento da plataforma, documentado: time series não rejeita _id
        # repetido. É por isso que a idempotência mora no produtor.
        inicio = self.agora - timedelta(hours=4)
        docs = _docs(100, inicio)
        self.d.ts.insert_many(docs)
        self.d.ts.insert_many(docs)
        self.assertEqual(self._conta(inicio, inicio + timedelta(minutes=1)), 200)

    def test_lotes_concorrentes_fora_de_ordem(self):
        inicio = self.agora - timedelta(hours=5)
        docs = _docs(8000, inicio)
        random.Random(7).shuffle(docs)
        lotes = [docs[i::4] for i in range(4)]
        threads = [threading.Thread(target=self.d.ts.insert_many, args=(lote,),
                                    kwargs={"ordered": False}) for lote in lotes]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        por_segundo = list(self.d.ts.aggregate([
            {"$match": {"ts": {"$gte": inicio, "$lt": inicio + timedelta(minutes=2)}}},
            {"$group": {"_id": {"$dateTrunc": {"date": "$ts", "unit": "second"}},
                        "n": {"$sum": 1}}}]))
        self.assertEqual(sum(x["n"] for x in por_segundo), 8000)
        esperado = {}
        for i in range(8000):
            seg = (inicio + timedelta(milliseconds=7 * i)).replace(microsecond=0, tzinfo=None)
            esperado[seg] = esperado.get(seg, 0) + 1
        self.assertEqual({x["_id"]: x["n"] for x in por_segundo}, esperado)

    def test_epoch_zero_e_futuro_ficam_fora_da_janela_da_tela(self):
        extremos = [datetime(1970, 1, 1, tzinfo=timezone.utc),
                    datetime(2100, 1, 1, tzinfo=timezone.utc)]
        self.d.ts.insert_many([{**_docs(1, ts)[0], "conta_id": "EXTREMO"} for ts in extremos])
        fim = self.agora
        janela = self.d.ts.count_documents({"conta_id": "EXTREMO",
                                            "ts": {"$gte": fim - timedelta(seconds=60),
                                                   "$lt": fim}})
        self.assertEqual(janela, 0)
        self.assertEqual(self.d.ts.count_documents({"conta_id": "EXTREMO"}), 2)

    def test_timefield_com_tipo_errado_e_recusado_pelo_servidor(self):
        for ts in ("2026-13-45T99:00:00Z", "2026-10-06T10:00:00Z", 0, None):
            with self.subTest(ts=ts), self.assertRaises((BulkWriteError, OperationFailure)):
                self.d.ts.insert_many([{**_docs(1, self.agora)[0], "ts": ts}])

    def test_metafield_hostil_e_guardado_como_dado(self):
        hostil = {"canal": {"$gt": ""}, "provedor": "x‮​😀", "uf": "$where"}
        try:
            self.d.ts.insert_one({"ts": self.agora, "meta": hostil, "valor": 1.0})
        except (OperationFailure, BulkWriteError):
            return  # recusado pelo servidor: também é uma resposta segura
        # Se aceitou, é um valor literal: o filtro por um operador não casa com ele.
        self.assertEqual(self.d.ts.count_documents({"meta.uf": "$where"}), 1)
        self.assertEqual(self.d.ts.count_documents({"meta.canal": {"$eq": {"$gt": ""}}}), 1)

    def test_densify_explosivo_e_barrado_pelo_servidor(self):
        # O app nunca deixa o cliente escolher step/unit; aqui provamos o que o
        # servidor faz se alguém escrever esse pipeline: limite de memória ou
        # maxTimeMS, nunca um processo pendurado.
        pipe = [{"$match": {"conta_id": "C000000001"}},
                {"$densify": {"field": "ts", "range": {
                    "step": 1, "unit": "millisecond",
                    "bounds": [datetime(1970, 1, 1), datetime(2100, 1, 1)]}}},
                {"$count": "n"}]
        with self.assertRaises(OperationFailure):
            list(self.d.ts.aggregate(pipe, maxTimeMS=3000))


class RetomadaDoGerador(unittest.TestCase):
    """Carga interrompida no meio de um dia: a retomada não duplica nem perde evento."""

    DB = DB.replace("_test", "_resume_test")

    @classmethod
    def tearDownClass(cls):
        common.client().drop_database(cls.DB)

    def _gen(self, *args):
        subprocess.run([sys.executable, os.path.join(ROOT, "data-generator", args[0]),
                        *args[1:], "--db", self.DB], check=True, cwd=ROOT,
                       stdout=subprocess.DEVNULL)

    def test_retomada_sem_duplicar(self):
        self._gen("generate_registry.py", "--drop")
        self._gen("generate_events.py", "--days", "2", "--eps", "0.5", "--drop",
                  "--workers", "2")
        d = common.db(self.DB)
        info = d.dataset_info.find_one({"_id": "payment_events"})
        total = d.payment_events.count_documents({})
        self.assertEqual(total, info["events"])
        dia1 = info["planned_first_ts"] + timedelta(days=1)
        no_dia1 = d.payment_events.count_documents({"ts": {"$gte": dia1}})
        self.assertGreater(no_dia1, 100)

        # Simula a queda: o dia 1 não foi confirmado, chegou pela metade e com
        # um lote repetido por um retry ingênuo.
        metade = list(d.payment_events.find({"ts": {"$gte": dia1}}, {"_id": 0})
                      .sort("ts", 1).limit(no_dia1 // 2))
        d.payment_events.delete_many({"ts": {"$gte": dia1}})
        d.payment_events.insert_many(metade)
        d.payment_events.insert_many([dict(x) for x in metade[:50]])
        d.dataset_info.update_one({"_id": "payment_events"},
                                  {"$pull": {"days_done": 1},
                                   "$unset": {"day_events.1": ""}})

        self._gen("generate_events.py", "--resume", "--workers", "2", "--eps", "0.5")
        self.assertEqual(d.payment_events.count_documents({}), total)
        info2 = d.dataset_info.find_one({"_id": "payment_events"})
        self.assertEqual(info2["events"], total)
        self.assertEqual(sorted(info2["days_done"]), [0, 1])


if __name__ == "__main__":
    unittest.main(verbosity=2)

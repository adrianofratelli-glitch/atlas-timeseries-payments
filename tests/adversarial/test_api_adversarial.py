"""Suíte adversarial da API, contra o backend vivo apontado para um banco *_test.

    MONGODB_DB=trilho_pagamentos_test ./start.sh        # noutro terminal
    .venv/bin/python -m unittest discover -s tests/adversarial -p "test_api_*.py" -v

Recusa rodar se o /health disser que o banco não termina em `_test`: estes testes
abrem incidentes e ligam a ingestão ao vivo.
"""
from __future__ import annotations

import json
import os
import threading
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

BASE = os.getenv("API_BASE", "http://127.0.0.1:8400")


def call(path: str, params: dict | list | None = None, method: str = "GET",
         payload=None, raw: bytes | None = None, timeout: float = 60.0):
    url = BASE + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    data = raw if raw is not None else (
        json.dumps(payload).encode() if payload is not None else None)
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as res:
            return res.status, json.loads(res.read() or b"{}")
    except urllib.error.HTTPError as exc:
        body = exc.read()
        exc.close()
        try:
            return exc.code, json.loads(body or b"{}")
        except json.JSONDecodeError:
            return exc.code, {"raw": body.decode(errors="replace")}


def setUpModule():  # noqa: N802
    try:
        status, health = call("/health", timeout=30)
    except OSError as exc:
        raise unittest.SkipTest(f"backend fora do ar em {BASE}: {exc}") from exc
    if status != 200 or not str(health.get("database", "")).endswith("_test"):
        raise RuntimeError(f"recusado: backend aponta para {health.get('database')!r}, "
                           "não para um banco *_test")


def _um_provedor() -> str:
    _, body = call("/api/providers", {"canal": "pix", "limit": 1})
    return body["providers"][0]["provedor_id"]


class NoSqlInjection(unittest.TestCase):
    def test_operador_em_query_string_e_tratado_como_texto(self):
        for params in ({"provedor": '{"$gt": ""}'}, {"provedor[$ne]": "x"},
                       {"canal": '{"$where": "sleep(5000)"}'}):
            t0 = time.perf_counter()
            status, body = call("/api/latency", {**params, "hours": 1})
            self.assertEqual(status, 200, body)
            self.assertLess(time.perf_counter() - t0, 10)
            # Um operador interpretado devolveria a série inteira do trilho.
            if "provedor" in params or "canal" in params:
                self.assertEqual(body["medidos"], 0, params)

    def test_operador_em_path_e_texto(self):
        status, body = call("/api/velocity/" + urllib.parse.quote('{"$gt":""}'))
        self.assertEqual(status, 200)
        self.assertEqual(body["janelas"]["24h"]["eventos"], 0)

    def test_operador_no_corpo_e_recusado_pela_validacao(self):
        base = {"provedor_id": "PSP-001", "canal": "pix", "z_recusa": 4, "z_p99": 1,
                "janelas": 3, "taxa_recusa": 5, "p99_ms": 300, "eventos": 100}
        for campo, valor in (("provedor_id", {"$gt": ""}), ("canal", {"$ne": None}),
                             ("nota", {"$where": "1"}), ("eventos", {"$gt": 0})):
            status, _ = call("/api/incidents", method="POST", payload={**base, campo: valor})
            self.assertEqual(status, 422, campo)
        status, _ = call("/api/live/degrade", method="POST",
                         payload={"provedor_id": {"$gt": ""}})
        self.assertEqual(status, 422)


class JanelasHostis(unittest.TestCase):
    def test_janelas_invalidas_sao_422(self):
        for hours in ("0", "-1", "nan", "abc", "", "1e308", "inf", str(100 * 365 * 24)):
            with self.subTest(hours=hours):
                status, body = call("/api/latency", {"provedor": "PSP-001", "hours": hours})
                self.assertEqual(status, 422, (hours, body))
                status, _ = call("/api/providers/PSP-001/health", {"hours": hours})
                self.assertEqual(status, 422, hours)

    def test_janela_minuscula_nao_quebra(self):
        status, body = call("/api/latency", {"provedor": _um_provedor(), "hours": "1e-9"})
        self.assertEqual(status, 200, body)
        self.assertLessEqual(body["point_count"], 1)

    def test_parametros_de_inicio_fim_e_granularidade_sao_ignorados(self):
        # A API só aceita uma janela em horas; início > fim, epoch 0, ISO malformado,
        # fuso e granularidade vindos do cliente não chegam ao pipeline.
        prov = _um_provedor()
        status, body = call("/api/latency", {
            "provedor": prov, "hours": 1, "start": "2099-01-01T00:00:00Z",
            "end": "1970-01-01T00:00:00Z", "from": "2026-02-30T25:61", "tz": "America/Sao_Paulo",
            "unit": "millisecond", "binSize": "1000000000", "granularity": "nanoseconds"})
        self.assertEqual(status, 200, body)
        self.assertEqual(body["granularity"]["unit"], "minute")
        self.assertEqual(body["granularity"]["bin_size"], 1)
        self.assertLessEqual(len(body["points"]), 61)

    def test_ranking_limita_a_janela_e_declara(self):
        status, body = call("/api/ranking", {"hours": "1e9"})
        self.assertEqual(status, 200, body)
        self.assertEqual(body.get("hours_requested"), 1e9)
        self.assertLessEqual(body.get("hours_applied", 0), 6.0)

    def test_densify_com_janela_maxima_continua_limitado(self):
        status, body = call("/api/latency", {"provedor": _um_provedor(), "hours": 24 * 30,
                                             "fill": "true"}, timeout=90)
        self.assertIn(status, (200, 503), body)
        if status == 200:
            self.assertLessEqual(body["point_count"], 4000)
            self.assertEqual(body["granularity"]["unit"], "hour")


class EntradasGigantesEUnicode(unittest.TestCase):
    def test_conta_gigante_e_unicode(self):
        for conta in ("C" * 8000, "‮C000000001​", "😀💳", "C000000001\x00"):
            with self.subTest(conta=conta[:12]):
                status, body = call("/api/velocity/" + urllib.parse.quote(conta))
                self.assertIn(status, (200, 404, 414), body)
                if status == 200:
                    self.assertEqual(body["janelas"]["24h"]["eventos"], 0)

    def test_corpo_de_1mb_e_recusado(self):
        status, _ = call("/api/incidents", method="POST", payload={
            "provedor_id": "PSP-001", "canal": "pix", "z_recusa": 4, "z_p99": 1,
            "janelas": 3, "taxa_recusa": 5, "p99_ms": 300, "eventos": 100,
            "nota": "x" * 1_000_000})
        self.assertEqual(status, 422)

    def test_json_malformado(self):
        for raw in (b"{", b"\xff\xfe", b"[]", b'{"eps": "muito"}', b"null"):
            with self.subTest(raw=raw):
                status, _ = call("/api/live/start", method="POST", raw=raw)
                self.assertEqual(status, 422)


class IngestaoAoVivo(unittest.TestCase):
    @classmethod
    def tearDownClass(cls):
        call("/api/live/stop", method="POST")

    def test_ritmo_fora_do_limite(self):
        for eps in (0, -5, 1e9, "inf"):
            status, _ = call("/api/live/start", method="POST", payload={"eps": eps})
            self.assertEqual(status, 422, eps)
        status, _ = call("/api/live/start", method="POST", payload={"eps": 10, "x": 1})
        self.assertEqual(status, 422)

    def test_duplo_clique_e_abas_paralelas_iniciam_um_unico_feed(self):
        call("/api/live/stop", method="POST")
        respostas = []

        def iniciar():
            respostas.append(call("/api/live/start", method="POST", payload={}))

        threads = [threading.Thread(target=iniciar) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertTrue(all(s == 200 for s, _ in respostas), respostas)
        inicios = {b["started_at"] for _, b in respostas}
        self.assertEqual(len(inicios), 1, inicios)

        time.sleep(10)
        _, ov = call("/api/live/overview")
        feed = ov["feed"]
        # Regressão: sob backpressure a taxa exibida não pode passar da confirmada.
        decorrido = (datetime.now(timezone.utc)
                     - datetime.fromisoformat(feed["started_at"])).total_seconds()
        confirmada = feed["written"] / max(decorrido, 1)
        self.assertLess(feed["observed_eps"], confirmada * 1.6,
                        (feed["observed_eps"], confirmada))
        # Regressão: a curva não vira serra (segundos alternados quase vazios).
        cauda = [p["eventos"] for p in ov["points"][-8:]]
        if len(cauda) >= 6:
            mediana = sorted(cauda)[len(cauda) // 2]
            self.assertGreater(min(cauda), 0.2 * mediana, cauda)
        self.assertLessEqual(len(ov["points"]), 60)
        self.assertIsNone(ov["feed"]["last_error"])
        # Regressão P1: MongoDB 9 bloqueia system.buckets; a prova física vem de rawData.
        self.assertIsNotNone(ov["bucket"], "bucket físico indisponível")
        self.assertIn("rawData", ov["bucket"]["source"])
        self.assertGreaterEqual(ov["bucket"]["measurements"], 1)
        # A soma da curva nunca passa do que o feed confirmou (sem duplicata).
        self.assertLessEqual(ov["window_events"], ov["feed"]["written"])

        for _ in range(3):
            status, parado = call("/api/live/stop", method="POST")
            self.assertEqual(status, 200)
        self.assertEqual(parado["state"], "parado")


class Concorrencia(unittest.TestCase):
    def test_rajada_analitica_recusa_com_429_nunca_500(self):
        codigos = []

        def bater():
            codigos.append(call("/api/ranking", {"hours": 6}, timeout=90)[0])

        threads = [threading.Thread(target=bater) for _ in range(24)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertFalse([c for c in codigos if c >= 500 and c != 503], codigos)
        self.assertIn(429, codigos)

    def test_limites_de_paginacao(self):
        for path in ("/api/alerts", "/api/incidents"):
            self.assertEqual(call(path, {"limit": 10_000})[0], 422)
            self.assertEqual(call(path, {"limit": 0})[0], 422)


if __name__ == "__main__":
    unittest.main(verbosity=2)

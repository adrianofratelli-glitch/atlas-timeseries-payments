"""Ingestão ao vivo: o trilho pulsando na tela enquanto a demo acontece.

Escreve em `payment_events_live`, uma coleção time series **separada** com
`expireAfterSeconds`. Três razões para não escrever em `payment_events`:

1. A base histórica é a evidência conferida contra a verdade de terra. Injetar
   evento novo nela faria a detecção divergir do cenário plantado no meio da demo.
2. Apagar depois seria caro: em coleção time series o delete é restrito e a TTL
   expira o bucket inteiro, não o documento.
3. A TTL curta é o que permite rodar o roteiro várias vezes no mesmo dia sem
   limpeza manual — o dado ao vivo desaparece sozinho.

O timestamp gravado é o **real**. O relógio simulado só escolhe a forma do tráfego
(hora do dia). Ele é ancorado na data da base e abre às 10h de um dia útil. Carimbar
esse relógio, horas ou dias no passado, faria a TTL apagar a série em menos de um minuto.

Um único alimentador escreve PIX, cartão e TED no mesmo `insert_many`. Canal é uma
dimensão do evento e um filtro da análise, não uma escolha de pipeline de ingestão.

`degradar` liga uma degradação em um provedor com a ingestão rodando: é o momento em
que o apresentador vê a recusa se afastar da referência, o incidente abrir e o alerta
chegar, sem nada pré-gravado.
"""
from __future__ import annotations

import os
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

from ..config import (LIVE_MINUTES_PER_TICK, LIVE_TARGET_EPS, LIVE_TICK_SECONDS,
                      LIVE_TTL_SECONDS)
from ..db.client import db, with_retry

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))),
    "data-generator"))

COLLECTION = "payment_events_live"


def ensure_collection():
    d = db()
    if COLLECTION not in d.list_collection_names():
        d.create_collection(
            COLLECTION,
            timeseries={"timeField": "ts", "metaField": "meta",
                        # Bucket curto: aqui o objetivo é ver o dado chegar e expirar,
                        # não densidade de armazenamento.
                        "bucketMaxSpanSeconds": 300, "bucketRoundingSeconds": 300},
            expireAfterSeconds=LIVE_TTL_SECONDS)
    col = d[COLLECTION]
    # Provisionamento idempotente: uma coleção criada por versão anterior também
    # recebe os índices exigidos pelo workload atual. O overview filtra só por tempo;
    # depender do índice padrão meta+ts força a visitar todas as séries da janela.
    col.create_index([("ts", 1)], name="ts_1")
    col.create_index([("meta.provedor", 1), ("ts", 1)])
    col.create_index([("meta.canal", 1), ("ts", 1)])
    return col


class LiveFeed:
    """Um alimentador para o trilho inteiro; canal permanece apenas no documento."""

    def __init__(self) -> None:
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self.canais = ("pix", "cartao", "ted")
        self.eps: float = LIVE_TARGET_EPS
        self.degradado: str | None = None
        self.fator_recusa: float = 1.0
        self.fator_latencia: float = 1.0
        self.started_at: datetime | None = None
        self.simulated_now: datetime | None = None
        self.written = 0
        self.written_by_channel = {canal: 0 for canal in self.canais}
        self.last_tick_written = 0
        self.observed_eps = 0.0
        self.last_tick_duration_ms = 0.0
        self.last_document: dict | None = None
        self.ticks = 0
        self.state = "parado"
        self.last_error: str | None = None

    # ------------------------------------------------------------------ controle
    def start(self, eps: float = LIVE_TARGET_EPS) -> dict:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return self.status()
            self._stop.clear()
            self.eps = eps
            self.degradado = None
            self.fator_recusa = self.fator_latencia = 1.0
            self.started_at = datetime.now(timezone.utc)
            self.simulated_now = None
            self.written = self.ticks = 0
            self.written_by_channel = {canal: 0 for canal in self.canais}
            self.last_tick_written = 0
            self.observed_eps = 0.0
            self.last_tick_duration_ms = 0.0
            self.last_document = None
            self.last_error = None
            self._thread = threading.Thread(target=self._run, args=(eps,),
                                            name="live-feed", daemon=True)
            self._thread.start()
        for _ in range(60):
            if self.written or self.last_error:
                break
            time.sleep(0.1)
        return self.status()

    def degradar(self, provedor_id: str | None, fator_recusa: float = 6.0,
                 fator_latencia: float = 3.5) -> dict:
        """Liga (ou desliga, com provedor_id=None) uma degradação ao vivo."""
        self.degradado = provedor_id
        self.fator_recusa = fator_recusa if provedor_id else 1.0
        self.fator_latencia = fator_latencia if provedor_id else 1.0
        return self.status()

    def stop(self) -> dict:
        self._stop.set()
        t = self._thread
        if t:
            t.join(timeout=5)
        self.state = "parado"
        return self.status()

    def clear(self) -> dict:
        """Apaga o dado ao vivo agora, sem esperar a TTL."""
        self.stop()
        d = db()
        if COLLECTION in d.list_collection_names():
            d[COLLECTION].drop()
        self.written = self.ticks = 0
        self.written_by_channel = {canal: 0 for canal in self.canais}
        self.last_tick_written = 0
        self.observed_eps = 0.0
        self.last_tick_duration_ms = 0.0
        self.last_document = None
        self.simulated_now = None
        self.degradado = None
        return self.status()

    def status(self) -> dict:
        return {
            "state": self.state,
            "scope": "trilho_completo",
            "channels": list(self.canais),
            "eps": self.eps,
            "degradado": self.degradado,
            "fator_recusa": self.fator_recusa,
            "fator_latencia": self.fator_latencia,
            "started_at": self.started_at,
            "simulated_now": self.simulated_now,
            "written": self.written,
            "written_by_channel": dict(self.written_by_channel),
            "last_tick_written": self.last_tick_written,
            "observed_eps": round(self.observed_eps, 1),
            "last_tick_duration_ms": round(self.last_tick_duration_ms, 1),
            "last_document": self.last_document,
            "ticks": self.ticks,
            "minutes_per_tick": LIVE_MINUTES_PER_TICK,
            "tick_seconds": LIVE_TICK_SECONDS,
            "ttl_seconds": LIVE_TTL_SECONDS,
            "collection": COLLECTION,
            "last_error": self.last_error,
        }

    # ------------------------------------------------------------------ execução
    def _run(self, eps: float) -> None:
        try:
            # Import dentro do try: fora dele, um ImportError morria como traceback de
            # thread e o status continuava dizendo "sem erro".
            import numpy as np

            import common  # noqa: PLC0415 — o gerador vive fora do pacote do backend
            from generate_events import UF_PESO, UFS, ERROS  # noqa: PLC0415

            col = ensure_collection()
            d = db()
            cadastrados = list(d.provedores.find({"canal": {"$in": list(self.canais)}}))
            if not cadastrados:
                self.state = "parado"
                self.last_error = "trilho sem provedores"
                return
            provedores = {
                canal: [p for p in cadastrados if p["canal"] == canal]
                for canal in self.canais
            }
            ausentes = [canal for canal, itens in provedores.items() if not itens]
            if ausentes:
                self.state = "parado"
                self.last_error = f"canal sem provedores: {', '.join(ausentes)}"
                return
            pesos = {}
            for canal, itens in provedores.items():
                participacoes = np.array([p["participacao"] for p in itens], dtype=float)
                pesos[canal] = participacoes / participacoes.sum()
            rng = np.random.default_rng()

            info = d.dataset_info.find_one({"_id": "payment_events"}) or {}
            inicio = info.get("last_ts")
            referencia = (inicio.replace(tzinfo=timezone.utc) if inicio
                          else datetime.now(timezone.utc))
            # A demo sempre abre em horário bancário: iniciar à meia-noite deixava
            # o primeiro minuto visualmente vazio e quase eliminava TED.
            self.simulated_now = referencia.replace(
                hour=10, minute=0, second=0, microsecond=0)
            while self.simulated_now.weekday() >= 5:
                self.simulated_now += timedelta(days=1)
            self.state = "rodando"

            while not self._stop.is_set():
                tick_started = time.monotonic()
                agora = datetime.now(timezone.utc)
                dia = self.simulated_now
                docs = []
                por_canal = {canal: 0 for canal in self.canais}
                for canal in self.canais:
                    forma = common.volume_curve(canal, dia, eps, 60)
                    janela = (dia.hour * 60 + dia.minute) % len(forma)
                    # `volume_curve` já aplica a participação do canal sobre o
                    # ritmo total. Os três resultados entram no mesmo lote.
                    esperado = float(forma[janela]) * (LIVE_TICK_SECONDS / 60.0)
                    total = int(rng.poisson(max(esperado, 0.0)))
                    por_canal[canal] = total
                    if not total:
                        continue
                    reparticao = rng.multinomial(total, pesos[canal])
                    produtos = common.PRODUTOS[canal]
                    for prov, n in zip(provedores[canal], reparticao):
                        if n <= 0:
                            continue
                        alvo = prov["provedor_id"] == self.degradado
                        f_lat = self.fator_latencia if alvo else 1.0
                        f_rec = self.fator_recusa if alvo else 1.0
                        lat = common.latencia(rng, canal, n, f_lat)
                        taxa = min(prov["recusa_base"] * f_rec, 0.95)
                        recusado = rng.random(n) < taxa
                        lo, hi = common.CANAIS[canal]["ticket"]
                        valores = np.clip(rng.lognormal(np.log(lo * 2.2), 0.9, n), 1.0, hi * 12)
                        ufs = rng.choice(len(UFS), size=n, p=UF_PESO)
                        prods = rng.integers(0, len(produtos), size=n)
                        offs = rng.random(n) * LIVE_TICK_SECONDS
                        contas = rng.integers(0, 2_000_000, size=n)
                        erros_idx = rng.integers(0, len(ERROS[canal]), size=n)
                        for off, pi, ui, valor, latencia_ms, ok, ei, conta in zip(
                                offs.tolist(), prods.tolist(), ufs.tolist(),
                                np.round(valores, 2).tolist(), np.round(lat, 1).tolist(),
                                (~recusado).tolist(), erros_idx.tolist(), contas.tolist()):
                            docs.append({
                                "ts": agora - timedelta(seconds=float(off)),
                                "meta": {"canal": canal, "provedor": prov["provedor_id"],
                                         "produto": produtos[pi], "uf": UFS[ui]},
                                "valor": valor,
                                "latencia_ms": latencia_ms,
                                "aprovado": ok,
                                "erro": None if ok else ERROS[canal][ei],
                                "conta_id": f"C{conta:09d}",
                            })
                if docs:
                    # Um único lote mistura os três canais. Falha transitória de rede
                    # não pode matar a thread do feed, por isso usa o helper de retry.
                    with_retry(lambda docs=docs: col.insert_many(docs, ordered=False))
                    self.written += len(docs)
                    # A amostra só é publicada depois do insert_many retornar: ela
                    # pertence a um lote confirmado pelo cluster.
                    confirmado = max(docs, key=lambda item: item["ts"])
                    # `insert_many` acrescenta `_id: ObjectId` nos próprios dicts.
                    # A amostra da API não precisa desse detalhe e deve continuar
                    # serializável sem ensinar o módulo de serviço sobre BSON.
                    self.last_document = {
                        chave: valor for chave, valor in confirmado.items() if chave != "_id"
                    }
                    for canal, quantidade in por_canal.items():
                        self.written_by_channel[canal] += quantidade

                self.ticks += 1
                self.last_tick_written = len(docs)
                # EWMA curto: comunica o pulso sem fazer o número saltar a cada lote.
                instantaneo = len(docs) / LIVE_TICK_SECONDS
                self.observed_eps = (instantaneo if self.ticks == 1 else
                                     0.35 * instantaneo + 0.65 * self.observed_eps)
                self.simulated_now = dia + timedelta(minutes=LIVE_MINUTES_PER_TICK)
                elapsed = time.monotonic() - tick_started
                self.last_tick_duration_ms = elapsed * 1000
                # O tempo de escrita faz parte do tick; esperar um segundo adicional
                # deixava o ritmo visual progressivamente mais lento que o declarado.
                self._stop.wait(max(0.0, LIVE_TICK_SECONDS - elapsed))
            self.state = "parado"
        except Exception as exc:  # noqa: BLE001 — o alimentador não derruba a API
            self.last_error = f"{type(exc).__name__}: {exc}"
            self.state = "erro"


feed = LiveFeed()

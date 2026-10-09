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

Um único gerador escreve PIX, cartão e TED no mesmo `insert_many`. Canal é uma
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
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

from pymongo.errors import CollectionInvalid

from ..config import (LIVE_CLEAR_JOIN_SECONDS, LIVE_GATE_TIMEOUT_SECONDS,
                      LIVE_MINUTES_PER_TICK, LIVE_TARGET_EPS, LIVE_TICK_SECONDS,
                      LIVE_TTL_SECONDS)
from ..db.client import db, insert_idempotent

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))),
    "data-generator"))

COLLECTION = "payment_events_live"


_TIMESERIES = {"timeField": "ts", "metaField": "meta",
               # Bucket curto: aqui o objetivo é ver o dado chegar e expirar,
               # não densidade de armazenamento.
               "bucketMaxSpanSeconds": 300, "bucketRoundingSeconds": 300}


class LiveBusy(RuntimeError):
    """Uma escrita em voo não terminou dentro do prazo: o clear não apagou nada."""


def collection_kind(d=None) -> str:
    """`timeseries`, `ausente` ou o tipo errado com que a coleção existe."""
    d = d if d is not None else db()
    info = next(iter(d.list_collections(filter={"name": COLLECTION})), None)
    if info is None:
        return "ausente"
    if info.get("type") == "timeseries" and info.get("options", {}).get("timeseries"):
        return "timeseries"
    return info.get("type") or "desconhecido"


def ensure_collection():
    """Garante a coleção time series com TTL e índices; nunca aceita coleção comum.

    Um `insert_many` numa coleção que não existe cria uma coleção **comum**, sem
    timeSeries e sem TTL — o oposto do que a PoV prova. Por isso a coleção é criada
    explicitamente aqui e, se alguém a deixou como coleção comum (versão anterior
    com a corrida do clear, ou um insert manual), ela é recriada: o dado ao vivo é
    descartável por desenho (TTL de 1 h).
    """
    d = db()
    kind = collection_kind(d)
    if kind not in ("timeseries", "ausente"):
        d[COLLECTION].drop()
        kind = "ausente"
    if kind == "ausente":
        try:
            d.create_collection(COLLECTION, timeseries=dict(_TIMESERIES),
                                expireAfterSeconds=LIVE_TTL_SECONDS)
        except CollectionInvalid:
            # Outro processo criou entre a checagem e o create; confere o tipo.
            if collection_kind(d) != "timeseries":
                raise
    col = d[COLLECTION]
    # Provisionamento idempotente: uma coleção criada por versão anterior também
    # recebe os índices exigidos pelo workload atual. O overview filtra só por tempo;
    # depender do índice padrão meta+ts força a visitar todas as séries da janela.
    col.create_index([("ts", 1)], name="ts_1")
    col.create_index([("meta.provedor", 1), ("ts", 1)])
    col.create_index([("meta.canal", 1), ("ts", 1)])
    return col


class WriteGate:
    """Portão entre o escritor ao vivo e quem apaga/recria a coleção.

    O escritor captura a época ao nascer e só grava com o portão fechado e a época
    ainda vigente. `clear` segura o mesmo portão para avançar a época, apagar e
    recriar a coleção time series: uma escrita já em voo termina antes do drop, e
    uma escrita atrasada de época anterior é descartada em vez de recriar a coleção
    como coleção comum.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.epoch = 0

    @contextmanager
    def admit(self, epoch: int, check=None):
        with self._lock:
            yield epoch == self.epoch and (check is None or check())

    @contextmanager
    def exclusive(self, timeout: float):
        if not self._lock.acquire(timeout=timeout):
            raise LiveBusy("escrita ao vivo ainda em andamento; nada foi apagado")
        try:
            self.epoch += 1
            yield self.epoch
        finally:
            self._lock.release()


class LiveFeed:
    """Um gerador para o trilho inteiro; canal permanece apenas no documento."""

    def __init__(self) -> None:
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        # Serializa start/stop/clear: sem ele, um clear concorrente com um start
        # chegava a `join()` numa thread criada mas ainda não iniciada (HTTP 500).
        self._lock = threading.RLock()
        self._gate = WriteGate()
        self._epoch = 0
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
            self._epoch = self._gate.epoch
            thread = threading.Thread(target=self._run, args=(eps, self._epoch),
                                      name="live-feed", daemon=True)
            thread.start()
            # Publicado só depois de iniciado: nenhum outro caminho enxerga uma
            # thread que ainda não pode receber `join()`.
            self._thread = thread
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

    def _join(self, timeout: float) -> bool:
        """Espera o escritor terminar; devolve True se ele não está mais vivo."""
        t = self._thread
        if t is None or not t.is_alive():
            return True
        t.join(timeout=timeout)
        return not t.is_alive()

    def stop(self) -> dict:
        with self._lock:
            self._stop.set()
            parado = self._join(timeout=LIVE_CLEAR_JOIN_SECONDS)
            # Só diz "parado" quando a thread morreu de fato; senão a tela mostraria
            # parado com um lote ainda a caminho do cluster.
            self.state = "parado" if parado else "parando"
            return self.status()

    def clear(self) -> dict:
        """Apaga o dado ao vivo agora, sem esperar a TTL.

        Ordem que fecha a corrida com o escritor:
        1. sinaliza parada e avança a época sob o portão — espera a escrita em voo
           terminar, e qualquer lote posterior desta época é descartado;
        2. junta a thread do gerador (prazo `LIVE_CLEAR_JOIN_SECONDS`);
        3. ainda sob o portão, apaga e **recria** a coleção time series com TTL e
           índices antes de liberar qualquer escritor.
        Se uma escrita em voo não terminar no prazo do portão, nada é apagado e o
        chamador recebe `LiveBusy` (HTTP 409), em vez de um "limpo" falso.
        """
        with self._lock:
            self._stop.set()
            with self._gate.exclusive(LIVE_GATE_TIMEOUT_SECONDS):
                pass
            parado = self._join(timeout=LIVE_CLEAR_JOIN_SECONDS)
            with self._gate.exclusive(LIVE_GATE_TIMEOUT_SECONDS) as epoch:
                d = db()
                d[COLLECTION].drop()
                ensure_collection()
                self._epoch = epoch
            self.written = self.ticks = 0
            self.written_by_channel = {canal: 0 for canal in self.canais}
            self.last_tick_written = 0
            self.observed_eps = 0.0
            self.last_tick_duration_ms = 0.0
            self.last_document = None
            self.simulated_now = None
            self.degradado = None
            self.last_error = None
            # Um escritor que não saiu no prazo já não pode gravar (época vencida);
            # o estado reflete isso sem fingir que a thread morreu.
            self.state = "parado" if parado else "parando"
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
    def _vigente(self, epoch: int) -> bool:
        return epoch == self._gate.epoch

    def _colecao_ok(self) -> bool:
        """Checagem sob o portão: o insert nunca cria coleção implícita."""
        kind = collection_kind()
        if kind == "timeseries":
            return True
        if kind == "ausente":
            # Apagada por fora (shell, script): recria como time series antes do lote.
            ensure_collection()
            return True
        self.last_error = f"{COLLECTION} existe como '{kind}', não time series"
        self._stop.set()
        return False

    def _run(self, eps: float, epoch: int) -> None:
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
            anterior: datetime | None = None

            while not self._stop.is_set():
                tick_started = time.monotonic()
                agora = datetime.now(timezone.utc)
                # Os eventos do lote se espalham pelo intervalo REAL desde o lote
                # anterior. Com escrita mais lenta que o tick (backpressure), carimbar
                # só o último segundo deixava segundos alternados quase vazios e a
                # curva virava serra (2.364, 130, 2.352, 80…).
                intervalo = LIVE_TICK_SECONDS if anterior is None else min(
                    max((agora - anterior).total_seconds(), LIVE_TICK_SECONDS),
                    5 * LIVE_TICK_SECONDS)
                anterior = agora
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
                        offs = rng.random(n) * intervalo
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
                    # Um único lote mistura os três canais.
                    # Falha transitória no meio do lote não mata o feed nem duplica:
                    # a repetição confere o que o servidor já gravou.
                    gravados = insert_idempotent(
                        col, docs,
                        gate=lambda: self._gate.admit(epoch, self._colecao_ok))
                    if not self._vigente(epoch) or self._stop.is_set() and not gravados:
                        # Lote descartado pelo portão (clear em curso ou coleção
                        # inválida): nada foi gravado, nada é publicado.
                        break
                    self.written += gravados
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
                self.simulated_now = dia + timedelta(minutes=LIVE_MINUTES_PER_TICK)
                elapsed = time.monotonic() - tick_started
                # Taxa confirmada pelo relógio de parede do tick. Dividir pelo tick
                # nominal (1 s) inflava o número sob backpressure: com o lote levando
                # 2,4 s, a tela dizia 2.271/s quando o cluster confirmava ~1.000/s.
                # EWMA curto: comunica o pulso sem fazer o número saltar a cada lote.
                instantaneo = len(docs) / max(elapsed, LIVE_TICK_SECONDS)
                self.observed_eps = (instantaneo if self.ticks == 1 else
                                     0.35 * instantaneo + 0.65 * self.observed_eps)
                self.last_tick_duration_ms = elapsed * 1000
                # O tempo de escrita faz parte do tick; esperar um segundo adicional
                # deixava o ritmo visual progressivamente mais lento que o declarado.
                self._stop.wait(max(0.0, LIVE_TICK_SECONDS - elapsed))
            if self._vigente(epoch):
                self.state = "erro" if self.last_error else "parado"
        except Exception as exc:  # noqa: BLE001 — o gerador não derruba a API
            # Escritor de época vencida não sobrescreve o estado do feed atual.
            if self._vigente(epoch):
                self.last_error = f"{type(exc).__name__}: {exc}"
                self.state = "erro"


feed = LiveFeed()

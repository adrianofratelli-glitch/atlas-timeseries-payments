"""Cliente único e retry só para falha transitória de rede."""
from __future__ import annotations

import time
from threading import Lock
from typing import Callable, TypeVar

from bson import ObjectId
from pymongo import MongoClient
from pymongo.errors import (AutoReconnect, BulkWriteError, ConnectionFailure,
                            NetworkTimeout)

from ..config import MONGODB_DB, MONGODB_URI

T = TypeVar("T")

_client: MongoClient | None = None
_client_lock = Lock()


def client() -> MongoClient:
    global _client
    with _client_lock:
        if _client is None:
            _client = MongoClient(MONGODB_URI, retryWrites=True, w="majority",
                                  maxPoolSize=40, serverSelectionTimeoutMS=15000)
    return _client


def db():
    return client()[MONGODB_DB]


def with_retry(fn: Callable[[], T], attempts: int = 3) -> T:
    """Só falha transitória de rede é repetida.

    Erro de lógica ou de validação nunca: repetir esconde bug.
    """
    delay = 0.2
    for i in range(attempts):
        try:
            return fn()
        except (AutoReconnect, NetworkTimeout, ConnectionFailure):
            if i == attempts - 1:
                raise
            time.sleep(delay)
            delay *= 2
    raise RuntimeError("inalcançável")


_TRANSITORIOS = (AutoReconnect, NetworkTimeout, ConnectionFailure)


def insert_idempotent(col, docs: list[dict], attempts: int = 3, gate=None) -> int:
    """`insert_many` que pode ser repetido sem duplicar medições.

    Coleção time series não tem índice único em `_id`: repetir um lote cujo ack se
    perdeu na rede grava cada evento duas vezes, e a curva da tela mostra o dobro.
    O `_id` é fixado ANTES da primeira tentativa; numa repetição, o lote é conferido
    contra o servidor (faixa de `ts` + `_id`, coberta pelo índice `ts_1`) e só o que
    não chegou é reenviado. Devolve quantos documentos esta chamada gravou.

    `gate`, quando informado, é uma fábrica de context manager que devolve `True`
    se a escrita ainda pode acontecer. Cada tentativa de `insert_many` roda dentro
    dele: quem apaga a coleção (o `clear` da ingestão ao vivo) segura o mesmo
    portão, então nenhuma escrita fica em voo durante o drop e nenhuma escrita de
    uma época anterior recria a coleção como coleção comum depois dele.
    """
    if not docs:
        return 0
    for doc in docs:
        doc.setdefault("_id", ObjectId())
    pendentes = docs
    gravados = 0
    delay = 0.2
    for tentativa in range(attempts):
        try:
            if gate is None:
                col.insert_many(pendentes, ordered=False)
            else:
                with gate() as permitido:
                    if not permitido:
                        # Lote de uma época encerrada: descartado, nunca gravado.
                        return gravados
                    col.insert_many(pendentes, ordered=False)
            return gravados + len(pendentes)
        except (BulkWriteError, *_TRANSITORIOS) as exc:
            if isinstance(exc, BulkWriteError):
                erros = exc.details.get("writeErrors", [])
                # Erro de validação não se cura repetindo; só falha de rede no meio.
                if erros and not exc.details.get("writeConcernErrors"):
                    raise
            if tentativa == attempts - 1:
                raise
            time.sleep(delay)
            delay *= 2
            ids = [d["_id"] for d in pendentes]
            inicio = min(d["ts"] for d in pendentes)
            fim = max(d["ts"] for d in pendentes)
            presentes = {r["_id"] for r in with_retry(lambda: list(col.find(
                {"ts": {"$gte": inicio, "$lte": fim}, "_id": {"$in": ids}},
                {"_id": 1})))}
            gravados += len(presentes)
            pendentes = [d for d in pendentes if d["_id"] not in presentes]
            if not pendentes:
                return gravados
    raise RuntimeError("inalcançável")

"""Cliente único e retry só para falha transitória de rede."""
from __future__ import annotations

import time
from threading import Lock
from typing import Callable, TypeVar

from pymongo import MongoClient
from pymongo.errors import AutoReconnect, ConnectionFailure, NetworkTimeout

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

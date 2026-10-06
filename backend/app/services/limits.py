"""Concorrência limitada por classe de consulta, e o excesso é recusado.

A saúde de um provedor sobre 24 h e a série de latência de uma janela curta não podem
dividir a mesma fila: sob saturação, uma consulta analítica atrasa o caminho
interativo e a demo trava na tela.
Quem não consegue vaga em `ACQUIRE_TIMEOUT` recebe 429 com Retry-After — sistema
honesto sob carga recusa cedo.
"""
from __future__ import annotations

import threading
from contextlib import contextmanager

from fastapi import HTTPException

ACQUIRE_TIMEOUT = 0.75

_SLOTS = {
    "interativo": threading.BoundedSemaphore(12),   # latência, velocity, cadastro
    "analitico": threading.BoundedSemaphore(3),     # saúde do provedor, ranking, overview ao vivo
    "storage": threading.BoundedSemaphore(2),       # $collStats
}


@contextmanager
def slot(kind: str):
    sem = _SLOTS[kind]
    if not sem.acquire(timeout=ACQUIRE_TIMEOUT):
        raise HTTPException(status_code=429,
                            detail={"reason": f"fila {kind} saturada"},
                            headers={"Retry-After": "1"})
    try:
        yield
    finally:
        sem.release()

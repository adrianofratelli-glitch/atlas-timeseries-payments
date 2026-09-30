"""Cadastro: provedores, cenários plantados e contas de demonstração."""
from __future__ import annotations

from ..config import MAX_TIME_MS
from .client import db, with_retry


def provedores(canal: str | None = None, limite: int = 100) -> list[dict]:
    filtro = {"canal": canal} if canal else {}
    return with_retry(lambda: list(
        db().provedores.find(filtro, {"_id": 0}).sort("provedor_id", 1).limit(limite)))


def provedor(provedor_id: str) -> dict | None:
    return with_retry(lambda: db().provedores.find_one({"provedor_id": provedor_id},
                                                       {"_id": 0}))


def cenarios() -> list[dict]:
    """Verdade de terra. A tela confere a detecção contra ela, e diz que é seed."""
    return with_retry(lambda: list(
        db().degradation_scenarios.find({}, {"_id": 0}).sort("kind", 1)))


def contas_demo(limite: int = 8) -> list[dict]:
    """Contas plantadas para o painel de velocity, com a contagem já conhecida."""
    return with_retry(lambda: list(
        db().demo_accounts.find({}, {"_id": 0}).sort("eventos_24h", -1).limit(limite)))

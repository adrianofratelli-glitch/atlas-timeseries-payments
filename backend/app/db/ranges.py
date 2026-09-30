"""Faixa pedida pelo cliente e granularidade escolhida pelo servidor.

O cliente pede um intervalo; quem decide o `$dateTrunc` é o servidor. Um navegador
plotando milhões de pontos é um navegador que parou, e trocar a granularidade em
silêncio embaixo do apresentador produz pergunta que ninguém responde no palco —
por isso a granularidade escolhida volta no payload.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from ..config import MAX_RANGE_DAYS
from .client import db


class RangeError(ValueError):
    """Faixa inválida.

    Erro de domínio, não HTTP: a camada de dados não importa fastapi, do mesmo modo
    que nenhuma rota importa pymongo. `main.py` traduz em 422.
    """

    def __init__(self, detail: str):
        super().__init__(detail)
        self.detail = detail
        self.max_range_days = MAX_RANGE_DAYS


_ANCHOR: datetime | None = None


def anchor() -> datetime:
    """Fim do dataset, não o relógio.

    A carga cobre [hoje-N, última meia-noite). Ancorar no relógio devolvia janelas
    vazias no fim do intervalo, porque aquelas horas não existem no dado.
    """
    global _ANCHOR
    if _ANCHOR is None:
        info = db().dataset_info.find_one({"_id": "payment_events"})
        last = info.get("last_ts") if info else None
        _ANCHOR = last.replace(tzinfo=timezone.utc) if last else \
            datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return _ANCHOR


def resolve(hours: float, end: datetime | None = None) -> tuple[datetime, datetime, str, int]:
    """Devolve (início, fim, unidade, bin) para a janela pedida, em horas."""
    if hours <= 0:
        raise RangeError("janela deve ser positiva")
    if hours > MAX_RANGE_DAYS * 24:
        raise RangeError(f"janela acima do teto de {MAX_RANGE_DAYS} dias")

    end = end or anchor()
    start = end - timedelta(hours=hours)

    # Alvo: manter a série na casa das centenas de pontos, qualquer que seja a janela.
    if hours <= 2:
        unit, size = "minute", 1
    elif hours <= 12:
        unit, size = "minute", 5
    elif hours <= 48:
        unit, size = "minute", 15
    else:
        unit, size = "hour", 1
    return start, end, unit, size


def label(unit: str, size: int) -> str:
    return {"second": f"{size} s", "minute": f"{size} min",
            "hour": f"{size} h", "day": f"{size} d"}[unit]

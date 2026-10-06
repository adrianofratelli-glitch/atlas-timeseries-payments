"""Base do gerador: conexão, ids determinísticos e a forma do tráfego.

A curva é a parte que decide se a demo sobrevive a alguém de pagamentos na sala.
Volume de PIX não é uma senoide: tem vale às 4h, degrau às 8h quando o comércio
abre, pico no almoço, pico maior no fim da tarde, e sábado com perfil próprio.
Cartão de crédito segue o varejo; TED só existe em horário bancário.
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone

import numpy as np
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

NS = uuid.UUID("6f9b1c2e-0a4d-4f3b-9c11-2f7d5a8e4b60")

SEED = int(os.getenv("SEED", "42"))


def det_id(kind: str, *parts) -> str:
    """uuid5 sobre as chaves de negócio: rodar duas vezes reescreve o mesmo documento."""
    return str(uuid.uuid5(NS, kind + "|" + "|".join(str(p) for p in parts)))


_CLIENT: MongoClient | None = None


def client() -> MongoClient:
    """Cliente único por processo.

    Abrir um MongoClient por chamada re-resolve o registro SRV toda vez e derruba
    o DNS local no meio de um experimento.
    """
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = MongoClient(os.environ["MONGODB_URI"], retryWrites=True, w="majority",
                              serverSelectionTimeoutMS=30000)
    return _CLIENT


DEMO_DB = os.getenv("MONGODB_DB", "trilho_pagamentos")


def db(name: str | None = None):
    return client()[name or DEMO_DB]


class DemoWriteRefused(SystemExit):
    """Escrita destrutiva num banco que não é de teste, sem autorização explícita."""


def guard_write(name: str | None) -> str:
    """Recusa escrever fora de um banco `*_test` sem `ALLOW_DEMO_DB_WRITE=1`.

    Os geradores recriam coleções com `--drop`. Rodar um deles por engano contra o
    banco da demo apaga 45 milhões de eventos que levam mais de meia hora para voltar.
    O banco de teste é o padrão seguro; o da demo exige a variável, dita em voz alta.
    """
    alvo = name or DEMO_DB
    if alvo.endswith("_test_test"):
        raise DemoWriteRefused(f"banco '{alvo}' tem sufixo _test duplicado; corrija --db")
    if not alvo.endswith("_test") and os.getenv("ALLOW_DEMO_DB_WRITE") != "1":
        raise DemoWriteRefused(
            f"recusado: '{alvo}' não é um banco *_test. Para recriar a demo rode com "
            "ALLOW_DEMO_DB_WRITE=1, ou use --db <nome>_test.")
    return alvo


# Única fonte dos índices do PoV (antes em schema/indexes.js, que exigia mongosh).
# Índice em coleção time series indexa buckets, não eventos. `conta_id` é CAMPO DE
# MEDIÇÃO, não metaField: com índice secundário, o velocity da conta é uma consulta
# pontual sem multiplicar séries (docs/adr/0002-cardinalidade.md).
#
# Deliberadamente ausentes: índice em `valor`/`latencia_ms` ("todo evento acima de X"
# não é pergunta deste workload) e {meta.uf, ts} (UF sempre vem com canal/provedor).
INDEXES: dict[str, list[tuple[list[tuple[str, int]], dict]]] = {
    "payment_events": [
        ([("meta.provedor", 1), ("ts", 1)], {}),
        ([("meta.canal", 1), ("ts", 1)], {}),
        ([("conta_id", 1), ("ts", 1)], {}),
    ],
    "provedores": [
        ([("provedor_id", 1)], {"unique": True}),
        ([("canal", 1)], {}),
        ([("em_incidente", 1)], {"sparse": True}),
    ],
    "degradation_scenarios": [([("provedor_id", 1), ("kind", 1)], {"unique": True})],
    "demo_accounts": [([("conta_id", 1)], {"unique": True})],
    "incidents": [([("provedor_id", 1), ("status", 1)], {}), ([("opened_at", -1)], {})],
    "incident_alerts": [([("at", -1)], {})],
}


def ensure_indexes(d) -> dict[str, list[str]]:
    """Idempotente: create_index é no-op quando o índice já existe."""
    criados = {}
    for nome, specs in INDEXES.items():
        criados[nome] = [d[nome].create_index(chaves, **opts) for chaves, opts in specs]
    return criados


# ------------------------------------------------------------------------ canais
# share: fatia do volume total. p_aprovado: taxa de aprovação de referência.
# latencia: (mediana_ms, sigma_log) — latência é lognormal, não normal; é isso que
# produz a cauda que o p99 enxerga e a média esconde.
CANAIS = {
    "pix":    {"share": 0.62, "p_aprovado": 0.9955, "latencia": (78.0, 0.42), "ticket": (30, 900)},
    "cartao": {"share": 0.31, "p_aprovado": 0.9380, "latencia": (240.0, 0.55), "ticket": (25, 600)},
    "ted":    {"share": 0.07, "p_aprovado": 0.9880, "latencia": (420.0, 0.38), "ticket": (800, 25000)},
}

PRODUTOS = {
    "pix": ["pix_chave", "pix_qr", "pix_copia_cola"],
    "cartao": ["credito", "debito", "credito_parcelado"],
    "ted": ["ted"],
}

# Forma horária do volume (0..23), por canal, antes de normalizar.
_SHAPES = {
    "pix": [
        0.28, 0.17, 0.11, 0.09, 0.10, 0.16, 0.34, 0.62, 0.95, 1.18, 1.30, 1.42,
        1.38, 1.26, 1.30, 1.36, 1.44, 1.58, 1.62, 1.44, 1.18, 0.90, 0.62, 0.40,
    ],
    "cartao": [
        0.20, 0.12, 0.08, 0.06, 0.06, 0.10, 0.22, 0.44, 0.78, 1.10, 1.32, 1.46,
        1.40, 1.30, 1.38, 1.48, 1.56, 1.66, 1.74, 1.60, 1.30, 0.96, 0.62, 0.36,
    ],
    # TED vive em horário bancário e morre às 17h30.
    "ted": [
        0.02, 0.01, 0.01, 0.01, 0.01, 0.02, 0.06, 0.22, 0.95, 1.60, 1.85, 1.80,
        1.30, 1.55, 1.80, 1.75, 1.50, 0.85, 0.12, 0.04, 0.03, 0.02, 0.02, 0.02,
    ],
}

# Multiplicador por dia da semana (0=segunda).
_WEEKDAY = {
    "pix":    [1.00, 0.99, 1.00, 1.02, 1.12, 1.05, 0.82],
    "cartao": [0.92, 0.90, 0.94, 1.00, 1.18, 1.24, 0.88],
    "ted":    [1.05, 1.02, 1.02, 1.04, 1.10, 0.10, 0.02],
}


def volume_curve(canal: str, dia: datetime, eventos_por_segundo: float,
                 passo_segundos: int) -> np.ndarray:
    """Eventos esperados por janela, para um dia inteiro de um canal."""
    forma = np.array(_SHAPES[canal], dtype=float)
    forma = forma / forma.mean()
    janelas_por_hora = 3600 // passo_segundos
    por_janela = np.repeat(forma, janelas_por_hora)
    base = eventos_por_segundo * CANAIS[canal]["share"] * passo_segundos
    return por_janela * base * _WEEKDAY[canal][dia.weekday()]


def latencia(rng: np.random.Generator, canal: str, n: int, fator: float = 1.0) -> np.ndarray:
    """Latência lognormal. `fator` multiplica a mediana durante uma degradação."""
    mediana, sigma = CANAIS[canal]["latencia"]
    return rng.lognormal(mean=np.log(mediana * fator), sigma=sigma, size=n)


def utc_midnight(d: datetime) -> datetime:
    return d.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)


def slots(dia: datetime, passo_segundos: int) -> list[datetime]:
    n = 86400 // passo_segundos
    return [dia + timedelta(seconds=passo_segundos * i) for i in range(n)]

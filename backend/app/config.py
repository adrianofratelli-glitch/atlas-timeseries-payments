"""Configuração por ambiente. Toda constante que a demo ajusta mora aqui."""
from __future__ import annotations

import os

from dotenv import load_dotenv

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(os.path.join(ROOT, ".env"))


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def _float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


MONGODB_URI = os.environ["MONGODB_URI"]
MONGODB_DB = os.getenv("MONGODB_DB", "trilho_pagamentos")

MAX_TIME_MS = _int("TS_MAX_TIME_MS", 15000)
MAX_RANGE_DAYS = _int("TS_MAX_RANGE_DAYS", 90)
MAX_POINTS = _int("TS_MAX_POINTS", 4000)

# Teto de janela para o placar de provedores: varrer todos os provedores em 24 h não
# cabe no orçamento de `MAX_TIME_MS`. Ver providers.ranking().
RANKING_MAX_HOURS = _float("RANKING_MAX_HOURS", 6.0)

# Detecção por desvio da própria linha de base, não por limiar absoluto: um
# adquirente de crédito recusa 23% e está saudável; um PSP de PIX que recusa 3% está
# em incidente. Ver o controle negativo plantado em degradation_scenarios.
Z_SCORE_THRESHOLD = _float("Z_SCORE_THRESHOLD", 3.0)
Z_MIN_WINDOWS = _int("Z_MIN_WINDOWS", 3)

# A linha de base olha longe e para antes da janela julgada. Medido: com base de 12
# janelas terminando em -1, uma degradação de duas horas entra na própria base e o
# z despenca depois de duas janelas — o cenário plantado deixava de ser detectado.
Z_BASELINE_WINDOWS = _int("Z_BASELINE_WINDOWS", 96)
Z_BASELINE_LAG = _int("Z_BASELINE_LAG", 4)

# Piso absoluto: num provedor muito estável o desvio padrão é minúsculo e qualquer
# ruído vira z alto. O controle negativo (recusa 23,5% estável) marcava janela
# anômala com z 6,2 sem nada ter acontecido.
# Piso absoluto E relativo. Só o absoluto não basta: com 23,5% de recusa e ~500
# eventos por janela, o desvio binomial já é ~1,9pp, então 1,5pp é ruído. O piso
# relativo escala com a linha de base do próprio provedor.
MIN_DELTA_PP = _float("MIN_DELTA_PP", 1.5)
MIN_DELTA_RATIO = _float("MIN_DELTA_RATIO", 0.35)
MIN_P99_RATIO = _float("MIN_P99_RATIO", 1.5)
MIN_EVENTS_PER_WINDOW = _int("MIN_EVENTS_PER_WINDOW", 30)

# Janelas do velocity da conta, em horas. Entram numa passada só sobre a maior.
VELOCITY_WINDOWS = [int(x) for x in
                    os.getenv("VELOCITY_WINDOWS", "1,6,24").split(",") if x.strip()]
ARCHIVE_ENABLED = os.getenv("ARCHIVE_ENABLED", "false").lower() == "true"

# Ingestão ao vivo. O TTL curto é o que permite rodar o roteiro várias vezes no mesmo
# dia sem limpeza manual: o dado ao vivo expira sozinho.
LIVE_TTL_SECONDS = _int("LIVE_TTL_SECONDS", 3600)
LIVE_TICK_SECONDS = _float("LIVE_TICK_SECONDS", 1.0)
LIVE_MINUTES_PER_TICK = _int("LIVE_MINUTES_PER_TICK", 5)
# Média diária do trilho, repartida entre PIX, cartão e TED pelo mesmo modelo da
# base histórica. A demo abre às 10h e, pela curva intradiária, observa ~2,3 k/s com
# o padrão de 1,5 k/s. É um ritmo validado para o palco, não um sizing de produção.
LIVE_TARGET_EPS = _float("LIVE_TARGET_EPS", 1500.0)
# A tela ganha um ponto por segundo, mas a taxa de recusa usa uma janela móvel para
# não transformar poucos eventos de um provedor em uma serra ilegível.
LIVE_ROLLING_SECONDS = _int("LIVE_ROLLING_SECONDS", 15)
LIVE_MIN_EVENTS = _int("LIVE_MIN_EVENTS", 20)
LIVE_CONFIDENCE_SIGMAS = _float("LIVE_CONFIDENCE_SIGMAS", 3.0)

# NÃO são medições: são os números do cliente, e a tela os rotula como tais.
CUSTO_MINUTO_INDISPONIVEL = _float("CUSTO_MINUTO_INDISPONIVEL", 0.0)
TICKET_MEDIO_REFERENCIA = _float("TICKET_MEDIO_REFERENCIA", 0.0)
CURRENCY = os.getenv("CURRENCY", "R$")

BACKEND_PORT = _int("BACKEND_PORT", 8400)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

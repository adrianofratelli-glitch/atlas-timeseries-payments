"""Saúde do provedor: desvio da própria linha de base, não limiar absoluto.

Um adquirente de crédito com mix de risco recusa 23% das transações e está saudável;
um PSP de PIX que recusa 3% está em incidente. Limiar absoluto acusa o primeiro e
perde o segundo — foi exatamente o que o controle negativo plantado mostra.

O detector compara cada provedor consigo mesmo: `$setWindowFields` calcula média
móvel e desvio padrão sobre a janela anterior e o z-score diz de quantos desvios a
janela atual se afastou. É a mesma matemática de um SPC de chão de fábrica, feita
dentro do banco de dados sobre o evento bruto.
"""
from __future__ import annotations

from datetime import datetime

from ..config import (LIVE_CONFIDENCE_SIGMAS, LIVE_MIN_EVENTS, LIVE_ROLLING_SECONDS,
                      MAX_POINTS, MAX_TIME_MS, MIN_DELTA_PP, MIN_DELTA_RATIO,
                      MIN_EVENTS_PER_WINDOW,
                      MIN_P99_RATIO, RANKING_MAX_HOURS, Z_BASELINE_LAG,
                      Z_BASELINE_WINDOWS, Z_MIN_WINDOWS, Z_SCORE_THRESHOLD)
from .client import db, with_retry
from .ranges import label, resolve


def pipeline(provedor: str, start: datetime | None, end: datetime | None, unit: str,
             size: int, janela: int, lag: int = 1) -> list[dict]:
    match: dict = {"meta.provedor": provedor}
    if start and end:
        match["ts"] = {"$gte": start, "$lt": end}
    return [
        {"$match": match},
        {"$group": {
            "_id": {"$dateTrunc": {"date": "$ts", "unit": unit, "binSize": size}},
            "eventos": {"$sum": 1},
            "recusados": {"$sum": {"$cond": ["$aprovado", 0, 1]}},
            "volume": {"$sum": "$valor"},
            "lat": {"$percentile": {"input": "$latencia_ms", "p": [0.5, 0.99],
                                    "method": "approximate"}},
        }},
        {"$set": {"ts": "$_id",
                  "p50": {"$arrayElemAt": ["$lat", 0]},
                  "p99": {"$arrayElemAt": ["$lat", 1]},
                  "taxa_recusa": {"$cond": [{"$gt": ["$eventos", 0]},
                                            {"$multiply": [100, {"$divide": [
                                                "$recusados", "$eventos"]}]}, 0]}}},
        {"$setWindowFields": {
            "sortBy": {"ts": 1},
            "output": {
                # A base olha para trás `janela` posições e PARA em `-lag`, sem
                # encostar na janela julgada nem nas imediatamente anteriores. Com
                # base curta encostando em -1, uma degradação de duas horas entrava
                # na própria linha de base e o z despencava depois de duas janelas.
                "recusa_base": {"$avg": "$taxa_recusa",
                                "window": {"documents": [-janela, -lag]}},
                "recusa_desvio": {"$stdDevSamp": "$taxa_recusa",
                                  "window": {"documents": [-janela, -lag]}},
                "p99_base": {"$avg": "$p99", "window": {"documents": [-janela, -lag]}},
                "p99_desvio": {"$stdDevSamp": "$p99",
                               "window": {"documents": [-janela, -lag]}},
            }}},
        {"$set": {
            "z_recusa": {"$cond": [
                {"$gt": ["$recusa_desvio", 0]},
                {"$divide": [{"$subtract": ["$taxa_recusa", "$recusa_base"]},
                             "$recusa_desvio"]}, 0]},
            "z_p99": {"$cond": [
                {"$gt": ["$p99_desvio", 0]},
                {"$divide": [{"$subtract": ["$p99", "$p99_base"]}, "$p99_desvio"]}, 0]},
        }},
        # Anomalia exige z alto E um desvio que signifique alguma coisa no mundo: um
        # provedor estabilíssimo tem desvio padrão minúsculo, e sem o piso absoluto
        # qualquer ruído vira z de 6. Também exige volume mínimo na janela.
        {"$set": {
            "recusa_anomala": {"$and": [
                {"$gt": ["$z_recusa", Z_SCORE_THRESHOLD]},
                {"$gt": [{"$subtract": ["$taxa_recusa", "$recusa_base"]},
                         {"$max": [MIN_DELTA_PP,
                                   {"$multiply": ["$recusa_base", MIN_DELTA_RATIO]}]}]},
                {"$gte": ["$eventos", MIN_EVENTS_PER_WINDOW]}]},
            "p99_anomalo": {"$and": [
                {"$gt": ["$z_p99", Z_SCORE_THRESHOLD]},
                {"$gt": ["$p99", {"$multiply": ["$p99_base", MIN_P99_RATIO]}]},
                {"$gte": ["$eventos", MIN_EVENTS_PER_WINDOW]}]},
        }},
        {"$set": {"anomalo": {"$or": ["$recusa_anomala", "$p99_anomalo"]}}},
        {"$sort": {"ts": 1}},
        {"$project": {"_id": 0, "ts": 1, "eventos": 1, "recusados": 1,
                      "volume": {"$round": ["$volume", 2]},
                      "p50": {"$round": ["$p50", 1]}, "p99": {"$round": ["$p99", 1]},
                      "taxa_recusa": {"$round": ["$taxa_recusa", 3]},
                      "recusa_base": {"$round": ["$recusa_base", 3]},
                      "p99_base": {"$round": ["$p99_base", 1]},
                      "z_recusa": {"$round": ["$z_recusa", 2]},
                      "z_p99": {"$round": ["$z_p99", 2]},
                      "anomalo": 1, "recusa_anomala": 1, "p99_anomalo": 1}},
        {"$limit": MAX_POINTS},
    ]


def saude(provedor: str, hours: float) -> dict:
    start, end, unit, size = resolve(hours)
    janela = Z_BASELINE_WINDOWS
    pipe = pipeline(provedor, start, end, unit, size, janela, Z_BASELINE_LAG)
    linhas = with_retry(lambda: list(
        db().payment_events.aggregate(pipe, maxTimeMS=MAX_TIME_MS)))

    # Incidente = z acima do limiar por Z_MIN_WINDOWS janelas seguidas. Um pico
    # isolado é ruído de amostragem; o que acusa degradação é desvio que não volta.
    streak = melhor = 0
    inicio_pico = fim_pico = None
    atual_inicio = None
    for linha in linhas:
        if linha["anomalo"]:
            streak += 1
            atual_inicio = atual_inicio or linha["ts"]
            if streak > melhor:
                melhor, inicio_pico, fim_pico = streak, atual_inicio, linha["ts"]
        else:
            streak = 0
            atual_inicio = None

    eventos = sum(x["eventos"] for x in linhas)
    recusados = sum(x["recusados"] for x in linhas)
    anomalas = [x for x in linhas if x["anomalo"]]
    return {
        "provedor": provedor,
        "from": start, "to": end,
        "granularity": {"unit": unit, "bin_size": size, "label": label(unit, size)},
        "points": linhas,
        "totals": {"eventos": eventos, "recusados": recusados,
                   "taxa_recusa": round(100 * recusados / eventos, 3) if eventos else 0.0,
                   "volume": round(sum(x["volume"] for x in linhas), 2)},
        "z_threshold": Z_SCORE_THRESHOLD,
        "min_windows": Z_MIN_WINDOWS,
        "min_delta_pp": MIN_DELTA_PP,
        "min_delta_ratio": MIN_DELTA_RATIO,
        "baseline_windows": janela,
        "baseline_lag": Z_BASELINE_LAG,
        "longest_streak": melhor,
        "degradado": melhor >= Z_MIN_WINDOWS,
        "pico": {"inicio": inicio_pico, "fim": fim_pico,
                 "z_recusa_max": max((x["z_recusa"] for x in anomalas), default=0.0),
                 "z_p99_max": max((x["z_p99"] for x in anomalas), default=0.0)},
        "pipeline": pipe,
    }


def saude_ao_vivo(provedor: str) -> dict:
    """Saúde sobre `payment_events_live`, comparada à linha de base **cadastrada**.

    A série ao vivo nasce e morre dentro de uma hora. Aprender a linha de base dela
    mesma não funciona: medido, uma degradação de dez minutos entra na janela de
    base de oito minutos e vira a própria referência — a recusa marcava 47% com
    z 0,0. Aqui a referência é `provedores.recusa_base`, que é o que a operação
    sabe sobre aquele provedor antes de a demo começar.

    Na visão histórica a base continua sendo aprendida do dado: lá existem dias de
    passado e a degradação é um episódio dentro deles.
    """
    d = db()
    cadastro = d.provedores.find_one({"provedor_id": provedor}) or {}
    base_pct = float(cadastro.get("recusa_base", 0.0)) * 100
    limite_floor_pct = max(base_pct * (1 + MIN_DELTA_RATIO), base_pct + MIN_DELTA_PP)
    base_ratio = base_pct / 100

    janela_inicio = -(LIVE_ROLLING_SECONDS - 1)
    pipe = [
        {"$match": {"meta.provedor": provedor}},
        # Um ponto por segundo faz a curva acompanhar cada lote. A taxa de recusa
        # abaixo usa uma janela móvel maior para continuar estatisticamente legível.
        {"$group": {"_id": {"$dateTrunc": {"date": "$ts", "unit": "second"}},
                    "eventos": {"$sum": 1},
                    "recusados": {"$sum": {"$cond": ["$aprovado", 0, 1]}},
                    "volume": {"$sum": "$valor"},
                    "lat": {"$percentile": {"input": "$latencia_ms", "p": [0.5, 0.99],
                                            "method": "approximate"}}}},
        {"$set": {"ts": "$_id",
                  "p50": {"$arrayElemAt": ["$lat", 0]},
                  "p99": {"$arrayElemAt": ["$lat", 1]}}},
        {"$setWindowFields": {
            "sortBy": {"ts": 1},
            "output": {
                "eventos_janela": {"$sum": "$eventos", "window": {
                    "range": [janela_inicio, 0], "unit": "second"}},
                "recusados_janela": {"$sum": "$recusados", "window": {
                    "range": [janela_inicio, 0], "unit": "second"}},
            },
        }},
        {"$set": {
                  "recusa_base": base_pct,
                  # Margem binomial sobre o cadastro: uma amostra pequena não pode
                  # abrir incidente só porque recebeu duas recusas por acaso.
                  "margem_ruido_pct": {"$cond": [
                      {"$gt": ["$eventos_janela", 0]},
                      {"$multiply": [100, LIVE_CONFIDENCE_SIGMAS, {"$sqrt": {
                          "$divide": [base_ratio * (1 - base_ratio),
                                      "$eventos_janela"]}}]},
                      100,
                  ]},
                  "taxa_recusa": {"$cond": [{"$gt": ["$eventos_janela", 0]},
                                            {"$multiply": [100, {"$divide": [
                                                "$recusados_janela",
                                                "$eventos_janela"]}]}, 0]}}},
        {"$set": {"limite_recusa": {"$max": [
            limite_floor_pct, {"$add": [base_pct, "$margem_ruido_pct"]}]}}},
        # Não é z-score: aqui não há desvio-padrão de janela anterior para dividir (a
        # série ao vivo é curta demais para aprender a própria base — ver docstring).
        # É uma razão percentual contra a base cadastrada, por isso o nome não colide
        # com `z_recusa`/`z_p99` calculados de verdade em `pipeline()` (histórico).
        {"$set": {"delta_ratio_recusa": {"$cond": [
            {"$gt": [base_pct, 0]},
            {"$divide": [{"$subtract": ["$taxa_recusa", base_pct]}, base_pct]}, 0]},
            "p99_score_indisponivel": True}},
        {"$set": {"anomalo": {"$and": [
            {"$gt": ["$taxa_recusa", "$limite_recusa"]},
            {"$gte": ["$eventos_janela", LIVE_MIN_EVENTS]}]}}},
        {"$sort": {"ts": 1}},
        {"$project": {"_id": 0, "ts": 1, "eventos": 1, "recusados": 1,
                      "eventos_janela": 1, "recusados_janela": 1,
                      "volume": {"$round": ["$volume", 2]},
                      "p50": {"$round": ["$p50", 1]}, "p99": {"$round": ["$p99", 1]},
                      "taxa_recusa": {"$round": ["$taxa_recusa", 3]},
                      "recusa_base": {"$round": ["$recusa_base", 3]},
                      "limite_recusa": {"$round": ["$limite_recusa", 3]},
                      "delta_ratio_recusa": {"$round": ["$delta_ratio_recusa", 3]},
                      "p99_score_indisponivel": 1,
                      "anomalo": 1}},
        {"$limit": MAX_POINTS},
    ]
    linhas = with_retry(lambda: list(
        db().payment_events_live.aggregate(pipe, maxTimeMS=MAX_TIME_MS)))
    streak = melhor = 0
    for linha in linhas:
        streak = streak + 1 if linha["anomalo"] else 0
        melhor = max(melhor, streak)
    eventos = sum(x["eventos"] for x in linhas)
    recusados = sum(x["recusados"] for x in linhas)
    anomalas = [x for x in linhas if x["anomalo"]]
    return {
        "provedor": provedor, "live": True, "collection": "payment_events_live",
        "from": linhas[0]["ts"] if linhas else None,
        "to": linhas[-1]["ts"] if linhas else None,
        "granularity": {"unit": "second", "bin_size": 1, "label": "1 s"},
        "rolling_window_seconds": LIVE_ROLLING_SECONDS,
        "min_events_live": LIVE_MIN_EVENTS,
        "points": linhas,
        "totals": {"eventos": eventos, "recusados": recusados,
                   "taxa_recusa": round(100 * recusados / eventos, 3) if eventos else 0.0,
                   "volume": round(sum(x["volume"] for x in linhas), 2)},
        "baseline_source": "cadastro do provedor",
        "recusa_base_pct": round(base_pct, 3),
        "limite_pct": (linhas[-1]["limite_recusa"] if linhas else
                       round(limite_floor_pct, 3)),
        "limite_floor_pct": round(limite_floor_pct, 3),
        "confidence_sigmas": LIVE_CONFIDENCE_SIGMAS,
        "z_threshold": Z_SCORE_THRESHOLD, "min_windows": Z_MIN_WINDOWS,
        "min_delta_pp": MIN_DELTA_PP, "min_delta_ratio": MIN_DELTA_RATIO,
        "current_streak": streak,
        "longest_streak": melhor,
        # Ao vivo o veredito representa a janela atual e volta ao normal quando a
        # degradação é removida. `longest_streak` permanece como evidência histórica.
        "degradado": streak >= Z_MIN_WINDOWS,
        "pico": {"inicio": None, "fim": None,
                 "delta_ratio_recusa_max": max(
                     (x["delta_ratio_recusa"] for x in anomalas), default=0.0),
                 "p99_score_indisponivel": True},
        "pipeline": pipe,
    }


def ranking(hours: float, limite: int = 40) -> dict:
    """Placar dos provedores na janela: volume, recusa e p99 lado a lado."""
    hours = min(hours, RANKING_MAX_HOURS)
    start, end, _, _ = resolve(hours)
    pipe = [
        {"$match": {"ts": {"$gte": start, "$lt": end}}},
        {"$group": {"_id": {"provedor": "$meta.provedor", "canal": "$meta.canal"},
                    "eventos": {"$sum": 1},
                    "recusados": {"$sum": {"$cond": ["$aprovado", 0, 1]}},
                    "volume": {"$sum": "$valor"},
                    "lat": {"$percentile": {"input": "$latencia_ms", "p": [0.5, 0.99],
                                            "method": "approximate"}}}},
        {"$set": {"provedor": "$_id.provedor", "canal": "$_id.canal",
                  "p50": {"$arrayElemAt": ["$lat", 0]},
                  "p99": {"$arrayElemAt": ["$lat", 1]},
                  "taxa_recusa": {"$cond": [{"$gt": ["$eventos", 0]},
                                            {"$multiply": [100, {"$divide": [
                                                "$recusados", "$eventos"]}]}, 0]}}},
        {"$lookup": {"from": "provedores", "localField": "provedor",
                     "foreignField": "provedor_id", "as": "cadastro"}},
        {"$set": {"sla_p99_ms": {"$first": "$cadastro.sla_p99_ms"}}},
        {"$set": {"fora_do_sla": {"$gt": ["$p99", "$sla_p99_ms"]}}},
        {"$project": {"_id": 0, "cadastro": 0, "lat": 0}},
        {"$sort": {"eventos": -1}},
        {"$limit": limite},
    ]
    linhas = with_retry(lambda: list(
        db().payment_events.aggregate(pipe, maxTimeMS=MAX_TIME_MS)))
    for linha in linhas:
        for campo in ("p50", "p99", "taxa_recusa", "volume"):
            if linha.get(campo) is not None:
                linha[campo] = round(linha[campo], 2)
    return {"from": start, "to": end, "hours": hours,
            "max_hours": RANKING_MAX_HOURS, "providers": linhas, "pipeline": pipe}

"""Ativos da distribuidora fictícia: alimentadores, transformadores, medidores.

Idempotente: todo _id vem de det_id(), então rodar duas vezes reescreve os mesmos
documentos. Os cenários de perda são gravados em `loss_scenarios` — a demo verifica
o balanço contra uma resposta conhecida, nunca contra a sorte do gerador.
"""
from __future__ import annotations

import argparse
import os
import sys

import numpy as np
from pymongo import UpdateOne

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common  # noqa: E402

FEEDERS = 10

# Perda técnica do transformador + rede secundária. Faixa normal de operação.
TECH_LOSS = (0.040, 0.070)

# Cenários plantados. O alvo é a fração de **energia** desviada, não a fração de
# medidores: uma unidade comercial consome cerca de cinco vezes uma residencial, e
# parametrizar por contagem de medidores produziu um controle negativo com gap maior
# que um cenário de fraude — limiar nenhum separa os dois nessa configuração.
#
# gap = 1 - (1 - energia_desviada) / (1 + perda_técnica)
SCENARIOS = [
    # kind, rótulo, perda técnica, energia desviada alvo, fator de registro, estratégia
    ("severo",   "Furto concentrado em unidades comerciais",  0.055, 0.300, 0.20, "concentrado"),
    ("moderado", "Adulteração difusa no parque residencial",  0.050, 0.300, 0.55, "difuso"),
    ("sutil",    "Desvio difuso de baixa intensidade",        0.045, 0.130, 0.45, "difuso"),
    # Controle negativo: perda alta, porém inteiramente técnica. NÃO pode abrir caso.
    ("controle", "Perda técnica elevada, sem fraude",         0.075, 0.000, 1.00, None),
]

def build(meters_target: int, seed: int):
    rng = np.random.default_rng(seed)
    feeders, transformers, meters, scenarios = [], [], [], []

    n_tr = max(len(SCENARIOS), round(meters_target / 45))
    per_tr = max(8, meters_target // n_tr)

    classes = list(common.CLASSES)
    weights = np.array([common.CLASSES[c]["share"] for c in classes])
    weights = weights / weights.sum()

    for f in range(FEEDERS):
        feeders.append({
            "_id": common.det_id("feeder", f),
            "feeder_id": f"AL-{f:03d}",
            "substation": f"SE {['Vila Nova','Centro','Industrial','Jardim','Porto'][f % 5]}",
            "nominal_kv": 13.8,
        })

    for t in range(n_tr):
        tid = f"TR-{t:05d}"
        scenario = SCENARIOS[t] if t < len(SCENARIOS) else None
        tech = scenario[2] if scenario else float(rng.uniform(*TECH_LOSS))
        transformers.append({
            "_id": common.det_id("transformer", tid),
            "transformer_id": tid,
            "feeder_id": f"AL-{t % FEEDERS:03d}",
            "capacity_kva": int(rng.choice([45, 75, 112.5, 150, 225])),
            "installed_year": int(rng.integers(1998, 2024)),
            "technical_loss": round(tech, 4),
            "boundary_meter_id": f"FRT-{tid}",
            "location": {"type": "Point", "coordinates": [
                round(-46.75 + float(rng.uniform(0, 0.30)), 6),
                round(-23.65 + float(rng.uniform(0, 0.25)), 6)]},
        })

        n_meters = per_tr if scenario is None else max(per_tr, 30)
        block = []
        for m in range(n_meters):
            mid = f"MED-{t:05d}{m:03d}"
            klass = str(rng.choice(classes, p=weights))
            lo, hi = common.CLASSES[klass]["kwh_dia"]
            block.append({
                "_id": common.det_id("meter", mid),
                "meter_id": mid,
                "transformer_id": tid,
                "feeder_id": f"AL-{t % FEEDERS:03d}",
                "phase": "ABC"[m % 3],
                "customer_class": klass,
                "kwh_dia_base": round(float(rng.uniform(lo, hi)), 3),
                "tariff": {"residencial_b1": 0.78, "residencial_b1_alta": 0.82,
                           "comercial_b3": 0.71, "industrial_a4": 0.54}[klass],
                "installed_at": f"{int(rng.integers(2012, 2025))}-{int(rng.integers(1,13)):02d}-01",
                # Fração do consumo real que o medidor registra. 1.0 = íntegro.
                "register_factor": 1.0,
                "under_investigation": False,
                "location": {"type": "Point", "coordinates": [
                    round(-46.75 + float(rng.uniform(0, 0.30)), 6),
                    round(-23.65 + float(rng.uniform(0, 0.25)), 6)]},
            })

        if scenario:
            _kind, label, tech, energy_share, factor, strategy = scenario
            # Energia semanal, não consumo médio diário: ver common.mean_weekday().
            def weekly(x):
                return x["kwh_dia_base"] * common.mean_weekday(x["customer_class"])

            total_kwh = sum(weekly(x) for x in block)
            budget = energy_share * total_kwh
            # Só entram medidores que cabem sozinhos no orçamento de energia. Sem esse
            # filtro, um único cliente industrial responde por 70% do transformador e
            # todo cenário vira "um medidor", inclusive o que deveria ser difuso.
            fits = [x for x in block if weekly(x) <= budget]
            if strategy == "concentrado":
                # Maior consumo primeiro, comercial na frente: é como o furto que paga
                # uma inspeção de campo aparece de verdade.
                order = sorted(fits, key=lambda x: (x["customer_class"] != "comercial_b3",
                                                    -weekly(x)))
            elif strategy == "difuso":
                # Muitas unidades pequenas: nenhuma sozinha explica o gap.
                order = sorted(fits, key=weekly)
            else:
                order = []
            chosen, acc = [], 0.0
            for meter in order:
                if acc + weekly(meter) > budget:
                    continue
                meter["register_factor"] = factor
                chosen.append(meter)
                acc += weekly(meter)
            diverted = (acc / total_kwh) * (1 - factor) if chosen else 0.0
            gap = 1 - (1 - diverted) / (1 + tech)
            scenarios.append({
                "_id": common.det_id("scenario", tid),
                "transformer_id": tid,
                "kind": _kind,
                "label": label,
                "technical_loss": round(tech, 4),
                "fraud_meters": [x["meter_id"] for x in chosen],
                "fraud_energy_share": round(acc / total_kwh, 4),
                "register_factor": factor,
                "strategy": strategy,
                # Válido na média de uma semana inteira; um dia isolado desvia com o
                # peso de dia da semana da classe fraudada.
                "expected_gap_pct": round(gap * 100, 2),
                "expected_basis": "media_semanal",
                "should_open_case": len(chosen) > 0,
            })

        meters.extend(block)

    # Eventos plantados na série, escolhidos deterministicamente para que a demo
    # sempre encontre o mesmo medidor: uma falha de comunicação de 6 h (passo do
    # $densify/$fill) e uma excursão de tensão (o painel de qualidade).
    healthy = [m for m in meters if m["register_factor"] == 1.0]
    if healthy:
        healthy[len(healthy) // 3]["seeded_outage"] = {"start_hour": 2, "hours": 6}
        healthy[len(healthy) // 2]["seeded_voltage_sag"] = {"start_hour": 18, "hours": 3,
                                                            "volts": 196.0}

    return feeders, transformers, meters, scenarios


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--meters", type=int, default=int(os.getenv("METERS", "20000")))
    ap.add_argument("--seed", type=int, default=common.SEED)
    ap.add_argument("--drop", action="store_true")
    ap.add_argument("--db", default=None)
    args = ap.parse_args()

    d = common.db(args.db)
    feeders, transformers, meters, scenarios = build(args.meters, args.seed)

    for name, docs in (("feeders", feeders), ("transformers", transformers),
                       ("meters", meters), ("loss_scenarios", scenarios)):
        col = d[name]
        if args.drop:
            col.drop()
        col.bulk_write([UpdateOne({"_id": x["_id"]}, {"$set": x}, upsert=True) for x in docs],
                       ordered=False)
        print(f"{name:16} {len(docs):8,}")

    print("\ncenários plantados:")
    for s in scenarios:
        print(f"  {s['transformer_id']} {s['kind']:9} gap esperado {s['expected_gap_pct']:5.2f}% "
              f"caso={'sim' if s['should_open_case'] else 'NÃO (controle)'}  "
              f"{len(s['fraud_meters'])} medidor(es)  {s['label']}")


if __name__ == "__main__":
    main()

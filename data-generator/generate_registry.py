"""Cadastro do trilho: provedores, contas de demonstração e os cenários plantados.

Idempotente: todo _id vem de det_id(). Os cenários são a verdade de terra — a demo
verifica a detecção contra uma resposta conhecida, nunca contra a sorte do gerador.
"""
from __future__ import annotations

import argparse
import os
import sys

import numpy as np
from pymongo import UpdateOne

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common  # noqa: E402

# Provedores por canal. `recusa_base` é a taxa de recusa **própria** de cada um:
# um adquirente de crédito com mix de risco recusa mais que um de débito, e isso
# é o perfil dele, não uma falha.
PROVEDORES = {
    "pix": {"prefixo": "PSP", "n": 24, "recusa": (0.002, 0.008), "sla_p99": 350},
    "cartao": {"prefixo": "ADQ", "n": 8, "recusa": (0.040, 0.090), "sla_p99": 1200},
    "ted": {"prefixo": "BCO", "n": 12, "recusa": (0.008, 0.020), "sla_p99": 1500},
}

# Cenários plantados. Cada um é uma degradação com início, duração e intensidade.
# `controle` é o caso que NÃO pode abrir incidente.
CENARIOS = [
    {
        "kind": "latencia",
        "canal": "pix",
        "provedor_idx": 14,
        "label": "PSP com p99 quadruplicado por três horas",
        "dia_offset": -2, "hora": 14, "duracao_h": 3,
        "fator_latencia": 4.2, "fator_recusa": 1.0,
        "deve_abrir": True,
    },
    {
        "kind": "recusa",
        "canal": "cartao",
        "provedor_idx": 3,
        "label": "Adquirente com recusa quatro vezes acima da própria linha de base",
        "dia_offset": -1, "hora": 10, "duracao_h": 2,
        "fator_latencia": 1.1, "fator_recusa": 4.0,
        "deve_abrir": True,
    },
    {
        "kind": "apagao",
        "canal": "pix",
        "provedor_idx": 21,
        "label": "PSP para de reportar telemetria por 40 minutos",
        # Última hora do dataset de propósito: a lacuna tem 40 minutos e some em
        # bins de 15 min. Plantada às 23h, a janela de 1 hora (bins de 1 min) mostra
        # as 40 janelas reconstruídas uma a uma.
        "dia_offset": -1, "hora": 23, "duracao_h": 0.67,
        "fator_latencia": 1.0, "fator_recusa": 1.0,
        "deve_abrir": False,
        "apagao": True,
    },
    {
        # Controle negativo: recusa alta o tempo todo, por mix de produto. Um limiar
        # absoluto acusaria; um detector que compara o provedor com a própria linha
        # de base, não.
        "kind": "controle",
        "canal": "cartao",
        "provedor_idx": 6,
        "label": "Adquirente com recusa estruturalmente alta, estável",
        "dia_offset": None, "hora": None, "duracao_h": None,
        "fator_latencia": 1.0, "fator_recusa": 1.0,
        "recusa_base_forcada": 0.235,
        "deve_abrir": False,
    },
]


def build(seed: int):
    rng = np.random.default_rng(seed)
    provedores, cenarios = [], []

    forcada = {(c["canal"], c["provedor_idx"]): c["recusa_base_forcada"]
               for c in CENARIOS if "recusa_base_forcada" in c}

    for canal, cfg in PROVEDORES.items():
        for i in range(cfg["n"]):
            pid = f'{cfg["prefixo"]}-{i:03d}'
            base = forcada.get((canal, i), float(rng.uniform(*cfg["recusa"])))
            provedores.append({
                "_id": common.det_id("provedor", pid),
                "provedor_id": pid,
                "canal": canal,
                "nome": f'{cfg["prefixo"]} {i:03d}',
                "recusa_base": round(base, 5),
                "sla_p99_ms": cfg["sla_p99"],
                "participacao": round(float(rng.uniform(0.4, 1.6)), 3),
            })

    idx = {(p["canal"], p["provedor_id"]): p for p in provedores}
    for c in CENARIOS:
        pid = f'{PROVEDORES[c["canal"]]["prefixo"]}-{c["provedor_idx"]:03d}'
        prov = idx[(c["canal"], pid)]
        cenarios.append({
            "_id": common.det_id("cenario", pid, c["kind"]),
            "provedor_id": pid,
            "canal": c["canal"],
            "kind": c["kind"],
            "label": c["label"],
            "dia_offset": c["dia_offset"],
            "hora": c["hora"],
            "duracao_h": c["duracao_h"],
            "fator_latencia": c["fator_latencia"],
            "fator_recusa": c["fator_recusa"],
            "apagao": bool(c.get("apagao")),
            "recusa_base": prov["recusa_base"],
            "recusa_esperada": round(prov["recusa_base"] * c["fator_recusa"], 5),
            "deve_abrir_incidente": c["deve_abrir"],
        })

    return provedores, cenarios


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=common.SEED)
    ap.add_argument("--drop", action="store_true")
    ap.add_argument("--db", default=None)
    args = ap.parse_args()

    d = common.db(args.db)
    provedores, cenarios = build(args.seed)

    for nome, docs in (("provedores", provedores), ("degradation_scenarios", cenarios)):
        col = d[nome]
        if args.drop:
            col.drop()
        col.bulk_write([UpdateOne({"_id": x["_id"]}, {"$set": x}, upsert=True) for x in docs],
                       ordered=False)
        print(f"{nome:22} {len(docs):6,}")

    print("\ncenários plantados:")
    for c in cenarios:
        alvo = "incidente" if c["deve_abrir_incidente"] else "SEM incidente"
        print(f'  {c["provedor_id"]:8} {c["kind"]:9} recusa base {c["recusa_base"]*100:5.2f}% '
              f'-> {c["recusa_esperada"]*100:5.2f}%  lat x{c["fator_latencia"]:.1f}  '
              f'{alvo:13} {c["label"]}')


if __name__ == "__main__":
    main()

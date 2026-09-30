"""Contas de demonstração do painel de velocity, com verdade de terra.

Com 2 milhões de contas e 45 milhões de eventos, a conta média faz ~3 transações
por dia. Isso não é uma história de velocity — é ruído. Então algumas contas são
plantadas com o padrão que o antifraude procura de verdade: uma rajada curta,
muitos canais, valores subindo.

As contas plantadas ficam em `demo_accounts` com a contagem esperada. A tela
confere o que o velocity devolve contra esse número.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
from pymongo import UpdateOne

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common  # noqa: E402
from generate_events import ERROS, UF_PESO, UFS  # noqa: E402

PERFIS = [
    # conta, rótulo, eventos na rajada, minutos de rajada, canais, recusa
    ("C000000001", "Rajada de 3 h em três canais, valores crescentes", 240, 180,
     ["pix", "cartao", "ted"], 0.22),
    ("C000000002", "Teste de cartão: muitas tentativas, quase tudo recusado", 180, 45,
     ["cartao"], 0.78),
    ("C000000003", "Conta legítima de alto giro (comércio)", 320, 720,
     ["pix", "cartao"], 0.03),
]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--collection", default="payment_events")
    ap.add_argument("--drop", action="store_true")
    ap.add_argument("--db", default=None)
    ap.add_argument("--seed", type=int, default=common.SEED)
    args = ap.parse_args()

    d = common.db(args.db)
    info = d.dataset_info.find_one({"_id": args.collection})
    if not info:
        sys.exit(f"sem dataset_info para {args.collection}: rode generate_events.py antes")
    fim = info["last_ts"].replace(tzinfo=timezone.utc)

    provedores = {}
    for p in d.provedores.find():
        provedores.setdefault(p["canal"], []).append(p)

    rng = np.random.default_rng(args.seed)
    if args.drop:
        d.demo_accounts.drop()

    docs_evento, registros = [], []
    for conta, rotulo, n_eventos, minutos, canais, taxa_recusa in PERFIS:
        # A rajada termina junto com o dataset: a janela de 24 h do velocity precisa
        # alcançá-la, e ancorar no relógio deixaria a rajada fora da janela.
        inicio = fim - timedelta(minutes=minutos)
        offsets = np.sort(rng.random(n_eventos)) * minutos
        escala = np.linspace(1.0, 3.2, n_eventos)  # valores subindo ao longo da rajada
        for i in range(n_eventos):
            canal = canais[i % len(canais)]
            prov = provedores[canal][int(rng.integers(0, len(provedores[canal])))]
            produtos = common.PRODUTOS[canal]
            lo, hi = common.CANAIS[canal]["ticket"]
            recusado = bool(rng.random() < taxa_recusa)
            docs_evento.append({
                "ts": inicio + timedelta(minutes=float(offsets[i])),
                "meta": {"canal": canal, "provedor": prov["provedor_id"],
                         "produto": produtos[int(rng.integers(0, len(produtos)))],
                         "uf": UFS[int(rng.choice(len(UFS), p=UF_PESO))]},
                "valor": round(float(np.clip(rng.lognormal(np.log(lo * 2.2), 0.6)
                                             * escala[i], 1.0, hi * 12)), 2),
                "latencia_ms": round(float(common.latencia(rng, canal, 1)[0]), 1),
                "aprovado": not recusado,
                "erro": (ERROS[canal][int(rng.integers(0, len(ERROS[canal])))]
                         if recusado else None),
                "conta_id": conta,
            })
        registros.append({
            "_id": common.det_id("demo_account", conta),
            "conta_id": conta,
            "label": rotulo,
            "eventos_plantados": n_eventos,
            "minutos": minutos,
            "canais": canais,
            "taxa_recusa_plantada": taxa_recusa,
            "inicio": inicio,
            "fim": fim,
            # A contagem de 24 h inclui o tráfego aleatório que a conta já tinha.
            "eventos_24h": n_eventos,
            "base": "plantado por generate_demo_accounts.py",
        })

    d[args.collection].insert_many(docs_evento, ordered=False)
    d.demo_accounts.bulk_write(
        [UpdateOne({"_id": r["_id"]}, {"$set": r}, upsert=True) for r in registros],
        ordered=False)

    print(f"{len(docs_evento):,} eventos plantados em {len(registros)} contas")
    for r in registros:
        print(f'  {r["conta_id"]}  {r["eventos_plantados"]:4d} eventos em '
              f'{r["minutos"]:4d} min  recusa {r["taxa_recusa_plantada"]*100:5.1f}%  {r["label"]}')


if __name__ == "__main__":
    main()

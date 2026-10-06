"""Reset único da PoV: recria TODOS os dados e índices de um banco.

    # banco de teste em escala reduzida (padrão seguro)
    .venv/bin/python scripts/reset_demo.py --db trilho_pagamentos_test --days 3 --eps 10

    # banco da demo, escala completa (~45 M eventos, mais de meia hora no M20)
    ALLOW_DEMO_DB_WRITE=1 .venv/bin/python scripts/reset_demo.py

    # carga interrompida (queda de rede, Ctrl+C): retoma sem duplicar
    ALLOW_DEMO_DB_WRITE=1 .venv/bin/python scripts/reset_demo.py --resume

O que é recriado, nesta ordem:
  1. provedores e degradation_scenarios (cadastro determinístico, `--drop`);
  2. payment_events — time series, `bucketMaxSpanSeconds=86400`, `--days` × `--eps`;
  3. payment_events_flat — 1 dia na coleção normal, para a comparação de storage;
  4. demo_accounts e a rajada plantada em payment_events (velocity);
  5. estado de execução: incidents, incident_alerts e payment_events_live (o feed
     recria a coleção ao vivo, com TTL, no próximo play);
  6. sobras de experimento (`bkt_*`, `card_*`, `srt_*`) e os índices de
     `common.INDEXES`.

Idempotente: rodar duas vezes produz o mesmo banco. Recusa qualquer banco que não
termine em `_test` sem `ALLOW_DEMO_DB_WRITE=1` (ver `common.guard_write`).
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GEN = os.path.join(ROOT, "data-generator")
sys.path.insert(0, GEN)
import common  # noqa: E402

RUNTIME = ("incidents", "incident_alerts", "payment_events_live")
SOBRAS = ("bkt_", "card_", "srt_")


def _run(etapa: str, *cmd: str) -> None:
    t0 = time.time()
    print(f"== {etapa}", flush=True)
    subprocess.run([sys.executable, *cmd], cwd=ROOT, check=True)
    print(f"   {etapa}: {time.time() - t0:,.1f}s", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--db", default=None, help="padrão: MONGODB_DB do .env")
    ap.add_argument("--days", type=int, default=int(os.getenv("DAYS", "7")))
    ap.add_argument("--eps", type=float, default=float(os.getenv("EVENTS_PER_SECOND", "75")))
    ap.add_argument("--accounts", type=int, default=int(os.getenv("ACCOUNTS", "2000000")))
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--resume", action="store_true",
                    help="retoma payment_events a partir do checkpoint diário")
    args = ap.parse_args()

    alvo = common.guard_write(args.db)
    if args.days < 3:
        # Os cenários plantados começam dois dias antes do fim e a linha de base do
        # detector olha 24 h para trás: com menos de 3 dias a demo não tem o que provar.
        sys.exit("--days precisa ser >= 3 para cobrir os cenários plantados")
    t0 = time.time()
    db_arg = ("--db", alvo)
    gen = lambda nome: os.path.join("data-generator", nome)  # noqa: E731

    _run("cadastro de provedores e cenários", gen("generate_registry.py"), "--drop", *db_arg)
    eventos = [gen("generate_events.py"), "--days", str(args.days), "--eps", str(args.eps),
               "--accounts", str(args.accounts), "--collection", "payment_events",
               "--variant", "span1d", "--workers", str(args.workers), *db_arg]
    eventos.append("--resume" if args.resume else "--drop")
    _run(f"eventos ({args.days} dias a {args.eps:g}/s)", *eventos)
    _run("amostra de comparação (coleção normal, 1 dia)", gen("generate_events.py"),
         "--days", "1", "--eps", str(args.eps), "--accounts", str(args.accounts),
         "--collection", "payment_events_flat", "--flat", "--drop",
         "--workers", str(args.workers), *db_arg)
    _run("contas de demonstração (velocity)", gen("generate_demo_accounts.py"), "--drop",
         *db_arg)

    d = common.db(alvo)
    print("== estado de execução e sobras", flush=True)
    for nome in d.list_collection_names():
        if nome in RUNTIME or nome.startswith(SOBRAS):
            d[nome].drop()
            print(f"   drop {nome}")
    print("== índices", flush=True)
    for colecao, nomes in common.ensure_indexes(d).items():
        print(f"   {colecao}: {', '.join(nomes)}")

    info = d.dataset_info.find_one({"_id": "payment_events"}) or {}
    print(f"\n✓ {alvo}: {info.get('events', 0):,} eventos em {info.get('days')} dias · "
          f"reset em {time.time() - t0:,.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env bash
# Atalho histórico para o reset único da PoV. Toda a lógica (ordem das cargas,
# índices, limpeza do estado de execução e a guarda do banco da demo) vive em
# scripts/reset_demo.py.
#
#   bash data-generator/run_all.sh --db trilho_pagamentos_test --days 3 --eps 10
#   ALLOW_DEMO_DB_WRITE=1 bash data-generator/run_all.sh          # demo: 7 dias, 75/s
#   ALLOW_DEMO_DB_WRITE=1 DAYS=3 EVENTS_PER_SECOND=40 bash data-generator/run_all.sh
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
exec .venv/bin/python scripts/reset_demo.py "$@"

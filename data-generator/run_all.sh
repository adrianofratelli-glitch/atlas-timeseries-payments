#!/usr/bin/env bash
# Carga completa. Idempotente no cadastro; os eventos são a exceção — coleção time
# series não tem _id controlado pelo cliente, logo não tem upsert, e recarregar é
# sempre --drop.
#
#   bash data-generator/run_all.sh                        # 7 dias, 75 eventos/s
#   DAYS=2 EVENTS_PER_SECOND=40 bash data-generator/run_all.sh
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
PY=.venv/bin/python
DAYS="${DAYS:-7}"
EPS="${EVENTS_PER_SECOND:-75}"

echo "== cadastro de provedores e cenários"
$PY data-generator/generate_registry.py --drop

echo "== eventos (${DAYS} dias a ${EPS}/s) — a carga longa"
$PY data-generator/generate_events.py --days "$DAYS" --eps "$EPS" \
    --collection payment_events --variant span1d --drop --workers 4

echo "== amostra de comparação (coleção normal)"
$PY data-generator/generate_events.py --days 1 --eps "$EPS" \
    --collection payment_events_flat --flat --drop --workers 4

echo "== contas de demonstração (velocity)"
$PY data-generator/generate_demo_accounts.py --drop

echo "== índices"
if command -v mongosh >/dev/null 2>&1; then
  set -a; . ./.env; set +a
  mongosh "$MONGODB_URI" schema/indexes.js
else
  echo "mongosh não encontrado — rode: mongosh \"\$MONGODB_URI\" schema/indexes.js"
fi

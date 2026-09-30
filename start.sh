#!/bin/bash
# Sobe backend (8400) e frontend (5400).
#
# Este script é o launcher que o PoV Portfolio executa: ele roda `bash start.sh`
# com o cwd no repositório, numa sessão nova, e espera a URL do frontend e o
# /health/live do backend responderem. Ao encerrar, manda SIGTERM para o grupo do
# launcher e depois para quem escuta nas portas com cwd dentro deste repositório.
#
# Daí três exigências, e o script existe para cumprir as três:
#   1. ficar em primeiro plano, para o grupo de processos segurar os filhos;
#   2. derrubar toda a árvore no SIGTERM — `npm run preview` gera um neto (vite)
#      que continua escutando se só o npm morrer;
#   3. tolerar uma porta que ainda está fechando de uma ativação anterior, em vez
#      de falhar na primeira tentativa.
set -u

BASE="$(cd "$(dirname "$0")" && pwd)"
BACKEND_LOG="${TMPDIR:-/tmp}/trilho-pagamentos-backend.log"
FRONTEND_LOG="${TMPDIR:-/tmp}/trilho-pagamentos-frontend.log"
BACKEND_PORT=8400
FRONTEND_PORT=5400
PORT_WAIT_SECONDS=10

BACKEND_PID=""
FRONTEND_PID=""

fail() { echo "❌ $1" >&2; exit 1; }

# Mata o processo e seus descendentes. `npm run preview` é um invólucro: matar só
# ele deixa o vite segurando a 5400, e a próxima ativação encontra a porta ocupada.
kill_tree() {
  local pid="$1" sig="${2:-TERM}"
  [[ -z "$pid" ]] && return 0
  local kids
  kids="$(pgrep -P "$pid" 2>/dev/null || true)"
  for kid in $kids; do kill_tree "$kid" "$sig"; done
  kill -"$sig" "$pid" 2>/dev/null || true
}

cleanup() {
  trap - EXIT INT TERM
  kill_tree "$FRONTEND_PID"
  kill_tree "$BACKEND_PID"
  # Segunda passada: dá um instante para saírem sozinhos e insiste no que restou.
  sleep 1
  kill_tree "$FRONTEND_PID" KILL
  kill_tree "$BACKEND_PID" KILL
}
# Armado ANTES de subir qualquer coisa: um SIGTERM durante a partida também tem
# que limpar, senão a ativação seguinte encontra órfãos nas portas.
trap cleanup EXIT INT TERM

wait_for_url() {
  local url="$1" attempts="${2:-40}"
  for ((i = 1; i <= attempts; i++)); do
    curl --fail --silent --max-time 2 "$url" >/dev/null && return 0
    sleep 1
  done
  return 1
}

wait_port_free() {
  local port="$1"
  for ((i = 0; i < PORT_WAIT_SECONDS; i++)); do
    lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1 || return 0
    sleep 1
  done
  return 1
}

command -v curl >/dev/null || fail "curl não encontrado."
command -v npm >/dev/null || fail "npm não encontrado."
[[ -f "$BASE/.env" ]] || fail "Falta .env. Copie .env.example e preencha MONGODB_URI."
[[ -x "$BASE/backend/venv/bin/uvicorn" ]] || fail "Virtualenv do backend ausente. Veja Setup no README."
[[ -d "$BASE/frontend/node_modules" ]] || fail "Dependências do frontend ausentes. Rode npm install em frontend/."

# Porta ocupada não é morta por este script — a política do workspace é nunca matar
# o que não é seu. Mas uma ativação logo após um encerramento pega a porta ainda
# fechando, e falhar aí seria falhar no ciclo normal do portal.
for port in "$BACKEND_PORT" "$FRONTEND_PORT"; do
  wait_port_free "$port" || fail "Porta $port segue ocupada após ${PORT_WAIT_SECONDS}s; o processo existente foi preservado."
done

echo "💳 Trilho de pagamentos & saúde de provedor · MongoDB Atlas"
echo "=========================================================="

echo "▶ backend (porta $BACKEND_PORT)..."
cd "$BASE/backend"
UVICORN_ARGS=(main:app --host 127.0.0.1 --port "$BACKEND_PORT")
[[ "${POV_DEV:-${DEV:-0}}" == "1" ]] && UVICORN_ARGS+=(--reload)
venv/bin/uvicorn "${UVICORN_ARGS[@]}" > "$BACKEND_LOG" 2>&1 &
BACKEND_PID=$!

if ! wait_for_url "http://127.0.0.1:$BACKEND_PORT/health/live" 40; then
  echo "❌ Backend não ficou pronto. Últimas linhas do log:" >&2
  tail -n 25 "$BACKEND_LOG" >&2
  exit 1
fi

echo "▶ frontend (porta $FRONTEND_PORT)..."
cd "$BASE/frontend"
if [[ "${POV_DEV:-${DEV:-0}}" == "1" ]]; then
  npm run dev > "$FRONTEND_LOG" 2>&1 &
else
  # Reconstrói só quando a fonte mudou: o bundle pronto evita manter um watcher
  # de arquivos por PoV aberta.
  if [[ ! -d dist ]] || [[ -n "$(find src index.html vite.config.js package-lock.json -newer dist -print -quit 2>/dev/null)" ]]; then
    npm run build > "$FRONTEND_LOG" 2>&1 || { echo "❌ build do frontend falhou" >&2; tail -n 20 "$FRONTEND_LOG" >&2; exit 1; }
  fi
  npm run preview >> "$FRONTEND_LOG" 2>&1 &
fi
FRONTEND_PID=$!

if ! wait_for_url "http://127.0.0.1:$FRONTEND_PORT/" 40; then
  echo "❌ Frontend não ficou pronto." >&2
  tail -n 25 "$FRONTEND_LOG" >&2
  exit 1
fi

echo
echo "✅ pronto"
echo "   UI      http://127.0.0.1:$FRONTEND_PORT"
echo "   API     http://127.0.0.1:$BACKEND_PORT/docs"
echo "   health  http://127.0.0.1:$BACKEND_PORT/health"
echo
echo "Antes de apresentar, rode o checklist em docs/demo-script.md."

# Primeiro plano de propósito: o portal segura este processo como o grupo da PoV.
wait

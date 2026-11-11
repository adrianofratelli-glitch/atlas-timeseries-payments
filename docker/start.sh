#!/bin/sh
set -e

cd /app/backend
# Um worker: o feed ao vivo (Play/Stop), o hub do change stream e os caches de
# storage vivem em memória do processo; com 2+ workers o Stop cairia num worker
# que não tem o feed rodando.
uvicorn main:app --host 127.0.0.1 --port 8000 --workers 1 &
nginx -g 'daemon off;'

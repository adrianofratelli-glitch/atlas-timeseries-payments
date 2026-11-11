# syntax=docker/dockerfile:1

FROM node:22-alpine AS frontend-build
WORKDIR /build
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
# VITE_API vazio: o bundle chama a API na mesma origem e o nginx faz o proxy.
ENV VITE_API=""
RUN npm run build

FROM python:3.12-slim AS runtime
RUN apt-get update \
  && apt-get install -y --no-install-recommends nginx curl \
  && rm -rf /var/lib/apt/lists/* \
  && useradd --create-home --uid 10001 app

WORKDIR /app

COPY backend/requirements.txt ./backend/requirements.txt
RUN pip install --no-cache-dir -r backend/requirements.txt

# Configuração só por variável de ambiente em runtime: nenhum .env entra na imagem.
COPY backend/ ./backend/
# O feed ao vivo (app/services/live.py) reusa os geradores de data-generator/.
COPY data-generator/ ./data-generator/
COPY --from=frontend-build /build/dist ./frontend/dist
COPY nginx.conf /etc/nginx/nginx.conf
COPY docker/start.sh /start.sh
RUN chmod +x /start.sh && chown -R app:app /app
USER app

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
  CMD curl -fsS http://127.0.0.1:8080/health/live || exit 1

CMD ["/start.sh"]

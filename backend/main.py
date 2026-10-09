"""API do PoV. Só expõe HTTP e traduz exceção em status.

Nenhuma rota importa pymongo: toda consulta vive em app/db/. É o que permite trocar
driver ou versão sem tocar em rota.
"""
from __future__ import annotations

import json
import os
import queue
import threading
import time
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from pymongo.errors import DuplicateKeyError, ExecutionTimeout

from app import config
from app.db import incidents, latency, live as live_db, providers, registry, storage, velocity
from app.db.client import db
from app.db.ranges import RangeError
from app.services import limits
from app.services.alerts import hub
from app.services.live import LiveBusy, feed

app = FastAPI(title="Telemetria do trilho de pagamentos · MongoDB Atlas time series",
              version="1.1.1")
app.add_middleware(CORSMiddleware, allow_origins=[
    f"http://127.0.0.1:{os.getenv('FRONTEND_PORT', '5400')}",
    f"http://localhost:{os.getenv('FRONTEND_PORT', '5400')}",
], allow_methods=["*"],
                   allow_headers=["*"])


@app.on_event("startup")
def _startup() -> None:
    hub.start()
    # Aquece o $collStats numa thread: a primeira medição leva ~32 s sobre 2,6 M
    # buckets e não pode acontecer com a plateia olhando.
    threading.Thread(target=storage.aquecer, name="storage-warmup",
                     daemon=True).start()


@app.on_event("shutdown")
def _shutdown() -> None:
    hub.stop()


def _json(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


_COLECOES: tuple[float, set[str]] = (0.0, set())


def _colecoes(d) -> set[str]:
    global _COLECOES
    agora = time.time()
    if agora - _COLECOES[0] > 30.0:
        _COLECOES = (agora, set(d.list_collection_names()))
    return _COLECOES[1]


def _timed(fn, kind: str):
    """Executa medindo o tempo de servidor e devolve (payload, ms)."""
    t0 = time.perf_counter()
    with limits.slot(kind):
        try:
            out = fn()
        except RangeError as exc:
            raise HTTPException(status_code=422, detail={
                "reason": exc.detail, "max_range_days": exc.max_range_days}) from exc
        except ExecutionTimeout as exc:
            raise HTTPException(status_code=503, detail={
                "reason": "consulta excedeu maxTimeMS",
                "max_time_ms": config.MAX_TIME_MS, "detail": str(exc)}) from exc
    out["elapsed_ms"] = round((time.perf_counter() - t0) * 1000, 1)
    return out


# ----------------------------------------------------------------------- saúde
@app.get("/health")
def health():
    d = db()
    try:
        d.command("ping")
        # list_collection_names() sobre este banco leva ~1 s e o health é a primeira
        # chamada da tela. O conjunto de coleções não muda durante uma demo.
        colecoes = _colecoes(d)
        # A contagem vem de dataset_info: estimated_document_count() sobre dezenas de
        # milhões custa segundos, e o health é a primeira chamada da tela.
        info = d.dataset_info.find_one({"_id": "payment_events"}) or {}
        eventos = info.get("events")
        if eventos is None:
            eventos = (d.payment_events.estimated_document_count()
                       if "payment_events" in colecoes else 0)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail={"reason": str(exc)}) from exc
    return {
        "status": "ok",
        "database": config.MONGODB_DB,
        "events": eventos,
        "days": info.get("days"),
        "accounts": info.get("accounts"),
        "flat_sample": "payment_events_flat" in colecoes,
        "change_stream": hub.state,
        "change_stream_error": hub.last_error,
        "live": feed.status(),
        "archive_enabled": config.ARCHIVE_ENABLED,
        "detector": {"z_threshold": config.Z_SCORE_THRESHOLD,
                     "min_windows": config.Z_MIN_WINDOWS},
    }


@app.get("/health/live")
def live_probe():
    return {"status": "ok"}


# ---------------------------------------------------------------------- cadastro
@app.get("/api/providers")
def list_providers(canal: str | None = None, limit: int = Query(100, ge=1, le=500)):
    return {"providers": registry.provedores(canal, limit)}


@app.get("/api/providers/{provedor_id}")
def get_provider(provedor_id: str):
    p = registry.provedor(provedor_id)
    if not p:
        raise HTTPException(status_code=404, detail={"reason": "provedor não encontrado"})
    return p


@app.get("/api/scenarios")
def list_scenarios():
    """Verdade de terra dos cenários plantados. Rotulada como tal na tela."""
    return {"scenarios": registry.cenarios(), "demo_accounts": registry.contas_demo(),
            "aviso": "verdade de terra do gerador; a tela confere a detecção contra ela"}


# -------------------------------------------------------------------- telemetria
@app.get("/api/latency")
def latency_series(canal: str | None = None, provedor: str | None = None,
                   hours: float = Query(24.0, gt=0, allow_inf_nan=False), fill: bool = False):
    return _timed(lambda: latency.serie(canal, provedor, hours, fill), "interativo")


@app.get("/api/providers/{provedor_id}/health")
def provider_health(provedor_id: str, hours: float = Query(24.0, gt=0, allow_inf_nan=False)):
    return _timed(lambda: providers.saude(provedor_id, hours), "analitico")


@app.get("/api/ranking")
def provider_ranking(hours: float = Query(1.0, gt=0, allow_inf_nan=False),
                     limit: int = Query(40, ge=1, le=100)):
    return _timed(lambda: providers.ranking(hours, limit), "analitico")


@app.get("/api/velocity/{conta_id}")
def account_velocity(conta_id: str):
    """Feature de antifraude: roda dentro do fluxo que decide a autorização."""
    return _timed(lambda: velocity.features(conta_id), "interativo")


@app.get("/api/storage")
def storage_comparison(force: bool = False):
    return _timed(lambda: storage.comparison(force), "storage")


# ------------------------------------------------------------------- incidentes
class AbrirIncidente(BaseModel):
    provedor_id: str = Field(min_length=3, max_length=64)
    canal: str = Field(min_length=2, max_length=32)
    z_recusa: float = Field(ge=-100, le=1000)
    z_p99: float = Field(ge=-100, le=1000)
    janelas: int = Field(ge=0, le=10000)
    taxa_recusa: float = Field(ge=0, le=100)
    p99_ms: float = Field(ge=0, le=600000)
    eventos: int = Field(ge=0)
    aberto_por: str = Field(default="demo", max_length=64)
    nota: str | None = Field(default=None, max_length=500)

    model_config = {"extra": "forbid", "allow_inf_nan": False}


@app.post("/api/incidents")
def create_incident(body: AbrirIncidente):
    try:
        return incidents.abrir(body.provedor_id, body.canal, body.z_recusa, body.z_p99,
                               body.janelas, body.taxa_recusa, body.p99_ms,
                               body.eventos, body.aberto_por, body.nota)
    except DuplicateKeyError as exc:
        raise HTTPException(status_code=409, detail={"reason": str(exc)}) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail={"reason": str(exc)}) from exc


@app.get("/api/incidents")
def list_incidents(limit: int = Query(50, ge=1, le=200)):
    return {"incidents": incidents.recentes(limit)}


@app.post("/api/incidents/{incident_id}/close")
def close_incident(incident_id: str, outcome: str = Query("confirmado", max_length=64),
                   by: str = Query("demo", max_length=64)):
    inc = incidents.encerrar(incident_id, outcome, by)
    if not inc:
        raise HTTPException(status_code=404,
                            detail={"reason": "incidente aberto não encontrado"})
    return inc


@app.post("/api/demo/reset")
def reset():
    # Reiniciar a demo também para e apaga a ingestão ao vivo: senão o próximo
    # roteiro começa com a série da apresentação anterior ainda na tela.
    resultado = incidents.reset_demo()
    resultado["ao_vivo"] = _clear_live()
    return resultado


def _clear_live() -> dict:
    try:
        return feed.clear()
    except LiveBusy as exc:
        raise HTTPException(status_code=409, detail={"reason": str(exc)}) from exc


# ---------------------------------------------------------------------- ao vivo
class LiveStart(BaseModel):
    model_config = {"extra": "forbid", "allow_inf_nan": False}

    # Ritmo do trilho completo. Canal não entra aqui: PIX, cartão e TED são
    # dimensões dos eventos produzidos pelo mesmo processo. O campo opcional existe
    # somente para uma interface nova conseguir conversar com a API anterior durante
    # o reinício; seu valor é deliberadamente ignorado.
    canal: str | None = Field(default=None, min_length=2, max_length=32,
                              deprecated=True, exclude=True)
    eps: float = Field(default=config.LIVE_TARGET_EPS, gt=0, le=5000)


class LiveDegrade(BaseModel):
    model_config = {"extra": "forbid", "allow_inf_nan": False}

    provedor_id: str | None = Field(default=None, min_length=3, max_length=64)
    fator_recusa: float = Field(default=6.0, ge=1, le=200)
    fator_latencia: float = Field(default=3.5, ge=1, le=200)


@app.post("/api/live/start")
def live_start(body: LiveStart):
    return feed.start(body.eps)


@app.post("/api/live/degrade")
def live_degrade(body: LiveDegrade):
    if body.provedor_id and not registry.provedor(body.provedor_id):
        raise HTTPException(status_code=404, detail={"reason": "provedor não encontrado"})
    return feed.degradar(body.provedor_id, body.fator_recusa, body.fator_latencia)


@app.post("/api/live/stop")
def live_stop():
    return feed.stop()


@app.post("/api/live/clear")
def live_clear():
    return _clear_live()


@app.get("/api/live/status")
def live_status():
    return feed.status()


@app.get("/api/live/overview")
def live_overview():
    """Throughput do trilho e configuração real da coleção na mesma fotografia."""
    # O feed pode confirmar outro lote enquanto a agregação roda. Congelar o estado
    # aqui garante que bucket e documento exibidos pertencem ao mesmo evento.
    feed_snapshot = feed.status()
    payload = _timed(
        lambda: live_db.overview(
            feed_snapshot["started_at"], feed_snapshot["last_document"]),
        "analitico",
    )
    payload["feed"] = feed_snapshot
    return payload


@app.get("/api/live/health/{provedor_id}")
def live_provider_health(provedor_id: str):
    """Série de 1 s e estado da ingestão na mesma fotografia da tela."""
    payload = _timed(lambda: providers.saude_ao_vivo(provedor_id), "analitico")
    payload["feed"] = feed.status()
    return payload


# ---------------------------------------------------------------------- alertas
@app.get("/api/alerts/stream")
def alert_stream():
    def events():
        q = hub.subscribe()
        yield "retry: 3000\n\n"
        yield f"event: hello\ndata: {json.dumps({'state': hub.state})}\n\n"
        try:
            while True:
                try:
                    data = q.get(timeout=15)
                    yield f"event: alert\ndata: {data}\n\n"
                except queue.Empty:
                    # Keepalive: proxy que fecha conexão ociosa mata a tira de alertas.
                    yield f": keepalive {int(time.time())}\n\n"
        finally:
            hub.unsubscribe(q)

    return StreamingResponse(events(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache",
                                      "X-Accel-Buffering": "no"})


@app.get("/api/alerts")
def list_alerts(limit: int = Query(50, ge=1, le=200)):
    rows = list(db().incident_alerts.find({}, {"_id": 0}, max_time_ms=config.MAX_TIME_MS)
                .sort("at", -1).limit(limit))
    return {"alerts": json.loads(json.dumps(rows, default=_json))}

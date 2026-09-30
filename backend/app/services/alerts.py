"""Change stream em `incidents` virando SSE.

Observa a coleção de incidentes, não a série. Um change stream sobre `payment_events`
dispara por transação — dezenas por segundo, útil para pipeline, inútil para acordar
uma tela.

Uma transação que abre um incidente produz mais de um evento; o hub coalesce por
`incident_id` dentro de uma janela curta para que a tira de alertas mostre uma linha,
não três.
"""
from __future__ import annotations

import json
import queue
import threading
import time
from datetime import datetime, timezone

from ..db.client import db

COALESCE_SECONDS = 2.0


def _json(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


class AlertHub:
    def __init__(self) -> None:
        self._subscribers: set[queue.Queue] = set()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self.state = "parado"
        self.last_error: str | None = None
        self._seen: dict[str, float] = {}

    # ---------------------------------------------------------------- assinatura
    def subscribe(self) -> queue.Queue:
        q: queue.Queue = queue.Queue(maxsize=100)
        with self._lock:
            self._subscribers.add(q)
        return q

    def unsubscribe(self, q: queue.Queue) -> None:
        with self._lock:
            self._subscribers.discard(q)

    def _publish(self, payload: dict) -> None:
        data = json.dumps(payload, default=_json)
        with self._lock:
            targets = list(self._subscribers)
        for q in targets:
            try:
                q.put_nowait(data)
            except queue.Full:
                # Assinante lento não segura o hub; perde evento e segue.
                pass

    # ------------------------------------------------------------------- listener
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="alert-hub", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                d = db()
                pipeline = [{"$match": {"operationType": {"$in": ["insert", "update"]}}}]
                with d.incidents.watch(pipeline, full_document="updateLookup") as stream:
                    self.state = "ativo"
                    self.last_error = None
                    backoff = 1.0
                    for change in stream:
                        if self._stop.is_set():
                            break
                        now = time.time()
                        # `_seen` só serve para coalescer dentro de uma janela curta;
                        # sem poda ele cresce sem limite pela vida do processo.
                        poda = COALESCE_SECONDS * 10
                        self._seen = {k: v for k, v in self._seen.items()
                                     if now - v <= poda}
                        doc = change.get("fullDocument") or {}
                        incident_id = doc.get("incident_id")
                        if incident_id and now - self._seen.get(incident_id, 0) < COALESCE_SECONDS:
                            continue
                        if incident_id:
                            self._seen[incident_id] = now
                        ev = doc.get("evidencia") or {}
                        alert = {
                            "incident_id": incident_id,
                            "provedor_id": doc.get("provedor_id"),
                            "canal": doc.get("canal"),
                            "status": doc.get("status"),
                            "z_recusa": ev.get("z_recusa"),
                            "z_p99": ev.get("z_p99"),
                            "taxa_recusa_pct": ev.get("taxa_recusa_pct"),
                            "at": datetime.now(timezone.utc),
                            "operation": change.get("operationType"),
                        }
                        d.incident_alerts.insert_one(dict(alert))
                        alert.pop("_id", None)
                        self._publish(alert)
            except Exception as exc:  # noqa: BLE001 — o stream não pode derrubar a API
                self.state = "reconectando"
                self.last_error = f"{type(exc).__name__}: {exc}"
                time.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
        self.state = "parado"


hub = AlertHub()

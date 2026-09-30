#!/usr/bin/env python3
"""Rampa limitada sobre o mesmo fluxo de ingestão e consulta usado pela POV.

Não apaga dados. Cada estágio abre uma nova sessão lógica e os eventos expiram pelo
TTL da própria coleção time series.
"""
from __future__ import annotations

import argparse
import json
import math
import statistics
import time
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def request(base_url: str, path: str, payload: dict | None = None,
            timeout: float = 20.0) -> tuple[dict, float]:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    req = Request(
        f"{base_url}{path}",
        data=body,
        headers={"Content-Type": "application/json"} if body else {},
        method="POST" if body is not None else "GET",
    )
    started = time.perf_counter()
    with urlopen(req, timeout=timeout) as response:  # noqa: S310 — localhost explícito
        result = json.load(response)
    return result, (time.perf_counter() - started) * 1000


def round_or_none(value: float | None) -> float | None:
    return None if value is None else round(value, 1)


def run_stage(base_url: str, requested_eps: int, duration: float,
              poll_seconds: float) -> dict:
    start, start_http_ms = request(base_url, "/api/live/start", {"eps": requested_eps})
    seen_tick = -1
    tick_durations: list[float] = []
    tick_sizes: list[float] = []
    query_server: list[float] = []
    query_http: list[float] = []
    errors: list[str] = []
    last = start
    deadline = time.monotonic() + duration

    while time.monotonic() < deadline:
        try:
            overview, http_ms = request(base_url, "/api/live/overview")
        except (HTTPError, URLError, TimeoutError) as exc:
            errors.append(f"{type(exc).__name__}: {exc}")
            break

        last = overview["feed"]
        query_server.append(float(overview.get("elapsed_ms", 0.0)))
        query_http.append(http_ms)
        tick = int(last.get("ticks", 0))
        if tick != seen_tick:
            seen_tick = tick
            tick_durations.append(float(last.get("last_tick_duration_ms", 0.0)))
            tick_sizes.append(float(last.get("last_tick_written", 0.0)))
        if last.get("state") == "erro" or last.get("last_error"):
            errors.append(last.get("last_error") or "feed em estado de erro")
            break
        # A interface agenda a próxima consulta depois da resposta anterior.
        time.sleep(poll_seconds)

    final, stop_http_ms = request(base_url, "/api/live/stop", {})
    started_at = datetime.fromisoformat(str(final["started_at"]))
    elapsed = max((datetime.now(timezone.utc) - started_at).total_seconds(), 0.001)
    confirmed_eps = float(final["written"]) / elapsed
    generated_per_tick = statistics.mean(tick_sizes) if tick_sizes else 0.0
    sustainability = (confirmed_eps / generated_per_tick * 100.0
                      if generated_per_tick else 0.0)

    return {
        "requested_eps": requested_eps,
        "confirmed_eps": round(confirmed_eps, 1),
        "generated_per_tick_mean": round(generated_per_tick, 1),
        "sustainability_pct": round(sustainability, 1),
        "written": int(final["written"]),
        "ticks": int(final["ticks"]),
        "tick_ms_p50": round_or_none(percentile(tick_durations, 0.50)),
        "tick_ms_p95": round_or_none(percentile(tick_durations, 0.95)),
        "tick_ms_max": round_or_none(max(tick_durations) if tick_durations else None),
        "query_server_ms_p50": round_or_none(percentile(query_server, 0.50)),
        "query_server_ms_p95": round_or_none(percentile(query_server, 0.95)),
        "query_http_ms_p95": round_or_none(percentile(query_http, 0.95)),
        "start_http_ms": round(start_http_ms, 1),
        "stop_http_ms": round(stop_http_ms, 1),
        "state": final["state"],
        "errors": errors,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8400")
    parser.add_argument("--duration", type=float, default=12.0)
    parser.add_argument("--stages", default="300,600,1200,2000,3000,4000,5000")
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--max-tick-p95-ms", type=float, default=1000.0)
    parser.add_argument("--max-query-p95-ms", type=float, default=1500.0)
    args = parser.parse_args()

    stages = [int(value) for value in args.stages.split(",") if value.strip()]
    results: list[dict] = []
    stopped_reason = "todos os estágios concluídos"

    try:
        status, _ = request(args.base_url, "/api/live/status")
        if status.get("state") == "rodando":
            request(args.base_url, "/api/live/stop", {})

        for stage in stages:
            result = run_stage(args.base_url, stage, args.duration, args.poll_seconds)
            results.append(result)
            print(json.dumps({"stage": result}, ensure_ascii=False), flush=True)

            tick_p95 = result["tick_ms_p95"] or 0.0
            query_p95 = result["query_server_ms_p95"] or 0.0
            if result["errors"]:
                stopped_reason = f"erro no estágio nominal {stage}"
                break
            if tick_p95 > args.max_tick_p95_ms:
                stopped_reason = (
                    f"p95 do ciclo {tick_p95:.1f} ms excedeu "
                    f"{args.max_tick_p95_ms:.0f} ms no estágio nominal {stage}"
                )
                break
            if query_p95 > args.max_query_p95_ms:
                stopped_reason = (
                    f"p95 da consulta {query_p95:.1f} ms excedeu "
                    f"{args.max_query_p95_ms:.0f} ms no estágio nominal {stage}"
                )
                break
    except KeyboardInterrupt:
        stopped_reason = "interrompido"
        try:
            request(args.base_url, "/api/live/stop", {})
        except Exception:  # noqa: BLE001 — melhor esforço no encerramento
            pass

    summary = {
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "base_url": args.base_url,
        "duration_seconds_per_stage": args.duration,
        "poll_seconds": args.poll_seconds,
        "stop_limits": {
            "tick_p95_ms": args.max_tick_p95_ms,
            "query_p95_ms": args.max_query_p95_ms,
        },
        "stopped_reason": stopped_reason,
        "results": results,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if results and not results[-1]["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Rampa limitada sobre o mesmo fluxo de ingestão e consulta usado pela POV.

Não apaga dados. Cada estágio abre uma nova sessão lógica e os eventos expiram pelo
TTL da própria coleção time series.

`--hold SEGUNDOS` troca a rampa por um único estágio longo no ritmo do palco,
fatiado por `--slice-seconds`: mede se a taxa confirmada e o p95 da consulta
derivam enquanto a coleção cresce (ingestão sustentada, não pico). O RTT de rede
que o driver mede vai junto, porque é o piso de cada lote.
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


def _fatia(inicio: float, fim: float, escritos: int, consultas: list[float],
           ticks: list[float], rtts: list[float]) -> dict:
    return {
        "seconds": round(fim - inicio, 1),
        "confirmed_eps": round(escritos / max(fim - inicio, 0.001), 1),
        "query_server_ms_p95": round_or_none(percentile(consultas, 0.95)),
        "tick_ms_p95": round_or_none(percentile(ticks, 0.95)),
        "network_rtt_ms_p50": round_or_none(percentile(rtts, 0.50)),
    }


def run_stage(base_url: str, requested_eps: int, duration: float,
              poll_seconds: float, slice_seconds: float | None = None) -> dict:
    start, start_http_ms = request(base_url, "/api/live/start", {"eps": requested_eps})
    seen_tick = -1
    tick_durations: list[float] = []
    tick_sizes: list[float] = []
    query_server: list[float] = []
    query_http: list[float] = []
    rtts: list[float] = []
    errors: list[str] = []
    last = start
    slices: list[dict] = []
    slice_start = time.monotonic()
    slice_written = int(start.get("written", 0))
    slice_queries: list[float] = []
    slice_ticks: list[float] = []
    slice_rtts: list[float] = []
    deadline = time.monotonic() + duration

    while time.monotonic() < deadline:
        try:
            overview, http_ms = request(base_url, "/api/live/overview")
        except (HTTPError, URLError, TimeoutError) as exc:
            errors.append(f"{type(exc).__name__}: {exc}")
            break

        last = overview["feed"]
        query_server.append(float(overview.get("elapsed_ms", 0.0)))
        slice_queries.append(query_server[-1])
        query_http.append(http_ms)
        if last.get("network_rtt_ms") is not None:
            rtts.append(float(last["network_rtt_ms"]))
            slice_rtts.append(rtts[-1])
        tick = int(last.get("ticks", 0))
        if tick != seen_tick:
            seen_tick = tick
            tick_durations.append(float(last.get("last_tick_duration_ms", 0.0)))
            slice_ticks.append(tick_durations[-1])
            tick_sizes.append(float(last.get("last_tick_written", 0.0)))
        agora = time.monotonic()
        if slice_seconds and agora - slice_start >= slice_seconds:
            escrito = int(last.get("written", 0))
            slices.append(_fatia(slice_start, agora, escrito - slice_written,
                                 slice_queries, slice_ticks, slice_rtts))
            slice_start, slice_written = agora, escrito
            slice_queries, slice_ticks, slice_rtts = [], [], []
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
        "network_rtt_ms_p50": round_or_none(percentile(rtts, 0.50)),
        "state": final["state"],
        "errors": errors,
        **({"slices": slices, "drift": deriva(slices)} if slice_seconds else {}),
    }


def deriva(slices: list[dict]) -> dict | None:
    """Último contra primeiro minuto: taxa (razão) e p95 da consulta (razão).

    Exclui a primeira fatia se houver três ou mais: ela inclui o aquecimento do
    feed (primeiro lote, criação de buckets).
    """
    uteis = slices[1:] if len(slices) >= 3 else slices
    if len(uteis) < 2:
        return None
    primeira, ultima = uteis[0], uteis[-1]
    taxas = [f["confirmed_eps"] for f in uteis]

    def razao(a, b):
        return round(b / a, 2) if a and b is not None else None

    return {
        "slices_compared": len(uteis),
        "rate_last_over_first": razao(primeira["confirmed_eps"], ultima["confirmed_eps"]),
        "rate_cv_pct": round(statistics.pstdev(taxas) / statistics.mean(taxas) * 100, 1)
        if statistics.mean(taxas) else None,
        "query_p95_last_over_first": razao(primeira["query_server_ms_p95"],
                                           ultima["query_server_ms_p95"]),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8400")
    parser.add_argument("--duration", type=float, default=12.0)
    parser.add_argument("--stages", default="300,600,1200,2000,3000,4000,5000")
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--max-tick-p95-ms", type=float, default=1000.0)
    parser.add_argument("--max-query-p95-ms", type=float, default=1500.0)
    parser.add_argument("--hold", type=float, default=None,
                        help="segundos de um único estágio sustentado (sem rampa)")
    parser.add_argument("--hold-eps", type=int, default=1500,
                        help="ritmo nominal do estágio sustentado (padrão do palco)")
    parser.add_argument("--slice-seconds", type=float, default=60.0)
    args = parser.parse_args()

    if args.hold:
        return hold(args)

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


def hold(args) -> int:
    """Estágio único e longo: estabilidade, não teto. Não aplica os limites da rampa."""
    status, _ = request(args.base_url, "/api/live/status")
    if status.get("state") == "rodando":
        request(args.base_url, "/api/live/stop", {})
    try:
        result = run_stage(args.base_url, args.hold_eps, args.hold, args.poll_seconds,
                           slice_seconds=args.slice_seconds)
    except KeyboardInterrupt:
        request(args.base_url, "/api/live/stop", {})
        return 1
    summary = {
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "base_url": args.base_url,
        "mode": "hold",
        "hold_seconds": args.hold,
        "slice_seconds": args.slice_seconds,
        "poll_seconds": args.poll_seconds,
        "result": result,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if not result["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

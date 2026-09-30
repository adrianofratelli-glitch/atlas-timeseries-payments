"""Carga misturada: velocity interativo concorrendo com saúde de provedor analítica.

O ponto não é achar o teto de throughput — é verificar que a consulta analítica não
sequestra o caminho interativo. Sob saturação, um sistema honesto recusa cedo em vez
de enfileirar até a tela travar.

    .venv/bin/python tests/stress.py
    .venv/bin/python tests/stress.py --max 128 --seconds 20
"""
from __future__ import annotations

import argparse
import json
import os
import random
import statistics
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = "http://127.0.0.1:8400"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def call(path: str, params: dict) -> tuple[int, float]:
    url = BASE + path + ("?" + urllib.parse.urlencode(params) if params else "")
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(url, timeout=60) as res:
            res.read()
            return res.status, (time.perf_counter() - t0) * 1000
    except urllib.error.HTTPError as exc:
        exc.read()
        return exc.code, (time.perf_counter() - t0) * 1000
    except Exception:  # noqa: BLE001
        return 0, (time.perf_counter() - t0) * 1000


def percentis(v: list[float]) -> dict:
    if not v:
        return {}
    v = sorted(v)
    return {"p50": round(statistics.median(v), 1),
            "p95": round(v[max(int(len(v) * 0.95) - 1, 0)], 1),
            "max": round(v[-1], 1)}


def rodada(clientes: int, segundos: float, contas: list[str],
           provedores: list[str]) -> dict:
    parar = threading.Event()
    lock = threading.Lock()
    interativo: list[float] = []
    analitico: list[float] = []
    status: dict[int, int] = {}

    def worker(i: int):
        while not parar.is_set():
            # Três de cada quatro clientes fazem o caminho interativo; é a proporção
            # de uma tela real, e é ele que não pode ser atrasado.
            if i % 4 == 0:
                code, ms = call(f"/api/providers/{random.choice(provedores)}/health",
                                {"hours": random.choice([24, 168])})
                alvo = analitico
            else:
                code, ms = call(f"/api/velocity/{random.choice(contas)}", {})
                alvo = interativo
            with lock:
                alvo.append(ms)
                status[code] = status.get(code, 0) + 1

    threads = [threading.Thread(target=worker, args=(i,), daemon=True)
               for i in range(clientes)]
    t0 = time.time()
    for t in threads:
        t.start()
    time.sleep(segundos)
    parar.set()
    for t in threads:
        t.join(timeout=65)
    elapsed = time.time() - t0
    total = sum(status.values())

    return {"clientes": clientes,
            "requisicoes": total,
            "rps": round(total / elapsed, 1),
            "status": status,
            "interativo_ms": percentis(interativo),
            "analitico_ms": percentis(analitico)}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max", type=int, default=64)
    ap.add_argument("--seconds", type=float, default=12)
    args = ap.parse_args()

    with urllib.request.urlopen(BASE + "/api/scenarios", timeout=30) as res:
        cen = json.loads(res.read())
    provedores = [s["provedor_id"] for s in cen["scenarios"]]
    contas = [c["conta_id"] for c in cen.get("demo_accounts", [])] or ["C000000001"]

    resultados = []
    n = 4
    while n <= args.max:
        r = rodada(n, args.seconds, contas, provedores)
        resultados.append(r)
        print(f'{n:4d} clientes  {r["rps"]:7.1f} rps  '
              f'interativo p50 {r["interativo_ms"].get("p50", 0):7.1f}ms '
              f'p95 {r["interativo_ms"].get("p95", 0):7.1f}ms  '
              f'analítico p50 {r["analitico_ms"].get("p50", 0):8.1f}ms  '
              f'status {r["status"]}', flush=True)
        n *= 2

    out = os.path.join(ROOT, "tests", "stress-results.json")
    with open(out, "w") as fh:
        json.dump(resultados, fh, indent=2)

    # 429 (fila cheia) e 503 (maxTimeMS) são recusas desenhadas: sob saturação o
    # sistema recusa cedo em vez de pendurar. O que não pode aparecer é 500 nem 0
    # (timeout de socket / conexão morta).
    erros = {c for r in resultados for c in r["status"] if c not in (200, 429, 503)}
    pico = resultados[-1]
    print(f"\n{out}")
    if erros:
        print(f"FALHA: status inesperados {sorted(erros)}")
        return 1
    p95 = pico["interativo_ms"].get("p95", 0)
    recusas = sum(v for r in resultados for c, v in r["status"].items() if int(c) in (429, 503))
    total = sum(v for r in resultados for v in r["status"].values())
    print(f'sem 500 e sem conexão morta até {pico["clientes"]} clientes; '
          f'p95 interativo no pico: {p95} ms; '
          f'{recusas} recusas em {total} chamadas '
          f'({100*recusas/max(total,1):.1f}% — 429 fila, 503 maxTimeMS)')
    return 0


if __name__ == "__main__":
    sys.exit(main())

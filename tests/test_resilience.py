"""Suíte hostil: entrada inválida, cenário negativo, duplicata, concorrência.

Roda contra a API viva. Não usa mock — o objetivo é descobrir o que quebra com o
cluster real do outro lado, antes que um cliente descubra.

    .venv/bin/python tests/test_resilience.py
    .venv/bin/python tests/test_resilience.py --quick   # sem SSE, ao vivo e carga
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = "http://127.0.0.1:8400"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
falhas: list[str] = []
passou = 0


def call(path: str, params: dict | None = None, method: str = "GET",
         payload: dict | None = None, timeout: float = 60.0):
    url = BASE + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as res:
            return res.status, json.loads(res.read() or b"{}")
    except urllib.error.HTTPError as exc:
        body = exc.read()
        try:
            return exc.code, json.loads(body or b"{}")
        except json.JSONDecodeError:
            return exc.code, {"raw": body.decode(errors="replace")}


def check(nome: str, condicao: bool, detalhe: str = "") -> None:
    global passou
    if condicao:
        passou += 1
        print(f"  ok   {nome}")
    else:
        falhas.append(f"{nome} — {detalhe}")
        print(f"  FALHA {nome} — {detalhe}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    args = ap.parse_args()

    print("saúde")
    status, health = call("/health")
    check("health responde 200", status == 200, str(status))
    check("banco reporta eventos", health.get("events", 0) > 0, str(health.get("events")))
    check("change stream ativo", health.get("change_stream") == "ativo",
          str(health.get("change_stream")))

    status, cen = call("/api/scenarios")
    cenarios = cen.get("scenarios", [])
    check("cenários plantados presentes", len(cenarios) >= 4, str(len(cenarios)))
    degradado = next((c for c in cenarios if c["kind"] == "recusa"), None)
    controle = next((c for c in cenarios if c["kind"] == "controle"), None)
    apagao = next((c for c in cenarios if c["kind"] == "apagao"), None)
    latencia = next((c for c in cenarios if c["kind"] == "latencia"), None)
    contas = cen.get("demo_accounts", [])
    check("contas de demonstração plantadas", len(contas) >= 1, str(len(contas)))

    print("\njanela e granularidade")
    # Escopado por provedor, que é o que a tela faz. Sem provedor, uma janela longa
    # varre o canal inteiro e é recusada de propósito — testado logo abaixo.
    for hours, unidade in ((1, "minute"), (24, "minute"), (168, "hour")):
        status, body = call("/api/latency",
                            {"provedor": degradado["provedor_id"], "hours": hours})
        check(f"servidor escolhe {unidade} em {hours}h",
              status == 200 and body["granularity"]["unit"] == unidade,
              f'{status} {body.get("granularity")}')
    status, _ = call("/api/latency", {"canal": "pix", "hours": 168})
    check("canal inteiro em janela longa é recusado com 422, não 503",
          status == 422, str(status))
    for hours in (0, -1, 100000):
        status, _ = call("/api/latency", {"canal": "pix", "hours": hours})
        check(f"janela {hours} recusada", status == 422, str(status))

    print("\npercentis")
    status, serie = call("/api/latency", {"canal": "pix", "hours": 24})
    pontos = [p for p in serie["points"] if p.get("p99") is not None]
    check("p99 presente na série", len(pontos) > 0, str(len(serie["points"])))
    check("p50 <= p95 <= p99 em toda janela",
          all(p["p50"] <= p["p95"] <= p["p99"] for p in pontos),
          "ordem de percentis violada")

    print("\ndetecção contra a verdade de terra")
    status, deg = call(f'/api/providers/{degradado["provedor_id"]}/health', {"hours": 24})
    check("provedor degradado é detectado", deg["degradado"] is True, str(deg["degradado"]))
    check("z de recusa acima do limiar",
          deg["pico"]["z_recusa_max"] > deg["z_threshold"],
          f'{deg["pico"]["z_recusa_max"]} vs {deg["z_threshold"]}')

    status, ctl = call(f'/api/providers/{controle["provedor_id"]}/health', {"hours": 24})
    check("controle negativo NÃO é detectado", ctl["degradado"] is False, str(ctl["degradado"]))
    check("controle tem recusa maior que o degradado — e mesmo assim não abre",
          ctl["totals"]["taxa_recusa"] > deg["totals"]["taxa_recusa"],
          f'{ctl["totals"]["taxa_recusa"]} vs {deg["totals"]["taxa_recusa"]}')

    # A degradação de latência foi plantada há dois dias: 24 h não a alcança, e é
    # assim de propósito — o cenário existe para exercitar a janela mais larga.
    status, lat = call(f'/api/providers/{latencia["provedor_id"]}/health', {"hours": 72})
    check("degradação de latência é detectada pelo z de p99",
          lat["pico"]["z_p99_max"] > lat["z_threshold"],
          f'{lat["pico"]["z_p99_max"]} vs {lat["z_threshold"]}')

    status, vazio = call("/api/providers/PSP-INEXISTENTE/health", {"hours": 24})
    check("provedor inexistente devolve série vazia, não erro",
          status == 200 and vazio["points"] == [], str(status))

    print("\nreconstrução de lacuna")
    status, sem = call("/api/latency", {"provedor": apagao["provedor_id"], "hours": 24,
                                        "fill": "false"})
    status, com = call("/api/latency", {"provedor": apagao["provedor_id"], "hours": 24,
                                        "fill": "true"})
    check("sem preenchimento não inventa ponto", sem["reconstruidos"] == 0,
          str(sem["reconstruidos"]))
    check("preenchimento cobre o apagão", com["reconstruidos"] > 0,
          str(com["reconstruidos"]))
    check("preenchido volta rotulado",
          all(p["metodo"] for p in com["points"] if p["reconstruido"]), "método ausente")
    check("preenchimento aumenta a série", com["point_count"] > sem["point_count"],
          f'{com["point_count"]} vs {sem["point_count"]}')

    print("\nvelocity da conta")
    conta = contas[0]["conta_id"]
    status, vel = call(f"/api/velocity/{conta}")
    check("velocity responde", status == 200 and vel["janelas"], str(status))
    janelas = vel["janelas"]
    check("janelas são cumulativas (1h <= 6h <= 24h)",
          janelas["1h"]["eventos"] <= janelas["6h"]["eventos"] <= janelas["24h"]["eventos"],
          str({k: v["eventos"] for k, v in janelas.items()}))
    check("conta plantada tem volume compatível com a rajada",
          janelas["24h"]["eventos"] >= contas[0]["eventos_plantados"] * 0.9,
          f'{janelas["24h"]["eventos"]} vs {contas[0]["eventos_plantados"]}')
    status, vazia = call("/api/velocity/C999999999")
    check("conta inexistente devolve zeros, não erro",
          status == 200 and vazia["janelas"]["24h"]["eventos"] == 0, str(status))

    print("\narmazenamento")
    status, st = call("/api/storage")
    check("comparação disponível", st.get("available") is True, str(st.get("reason")))
    check("time series comprime mais que coleção normal", st["storage_ratio"] > 1,
          str(st.get("storage_ratio")))
    check("razão contando índice é maior ainda",
          st["total_ratio"] >= st["storage_ratio"],
          f'{st["total_ratio"]} vs {st["storage_ratio"]}')

    print("\nincidentes e transação")
    call("/api/demo/reset", method="POST")
    corpo = {"provedor_id": degradado["provedor_id"], "canal": degradado["canal"],
             "z_recusa": deg["pico"]["z_recusa_max"], "z_p99": deg["pico"]["z_p99_max"],
             "janelas": deg["longest_streak"],
             "taxa_recusa": deg["totals"]["taxa_recusa"],
             "p99_ms": 900.0, "eventos": deg["totals"]["eventos"]}
    status, inc = call("/api/incidents", method="POST", payload=corpo)
    check("incidente aberto", status == 200 and inc.get("incident_id"), str(status))
    status, _ = call("/api/incidents", method="POST", payload=corpo)
    check("duplicata recusada com 409", status == 409, str(status))
    status, _ = call("/api/incidents", method="POST",
                     payload={**corpo, "provedor_id": "PSP-INEXISTENTE"})
    check("provedor inexistente recusado com 404", status == 404, str(status))

    print("\nvalidação de entrada")
    invalidos = [
        ("taxa de recusa acima de 100", {**corpo, "taxa_recusa": 180}),
        ("janelas absurdas", {**corpo, "janelas": 10**9}),
        ("provedor como lista", {**corpo, "provedor_id": ["a", "b"]}),
        ("p99 negativo", {**corpo, "p99_ms": -1}),
        ("campo obrigatório ausente", {"provedor_id": degradado["provedor_id"]}),
    ]
    for nome, payload in invalidos:
        status, _ = call("/api/incidents", method="POST", payload=payload)
        check(f"{nome} recusado com 422", status == 422, str(status))

    if not args.quick:
        print("\ningestão ao vivo")
        call("/api/live/clear", method="POST")
        status, live = call("/api/live/start", method="POST", payload={"eps": 300})
        check("ingestão inicia", status == 200 and live["state"] == "rodando", str(live))
        check("ingestão cobre um único trilho com os três canais",
              live.get("scope") == "trilho_completo"
              and set(live.get("channels", [])) == {"pix", "cartao", "ted"}, str(live))
        status, compat = call("/api/live/start", method="POST", payload={"canal": "cartao"})
        check("canal legado não altera o escopo da ingestão",
              status == 200 and compat.get("scope") == "trilho_completo"
              and set(compat.get("channels", [])) == {"pix", "cartao", "ted"},
              f"{status} {compat}")
        status, deg_live = call("/api/live/degrade", method="POST",
                                payload={"provedor_id": degradado["provedor_id"]})
        check("degradação injetada", deg_live["degradado"] == degradado["provedor_id"],
              str(deg_live.get("degradado")))
        time.sleep(10)
        status, st_live = call("/api/live/status")
        check("ingestão grava eventos", st_live["written"] > 0, str(st_live["written"]))
        check("contadores por canal fecham com o lote único",
              sum(st_live["written_by_channel"].values()) == st_live["written"]
              and all(st_live["written_by_channel"][c] > 0 for c in ("pix", "cartao", "ted")),
              str(st_live["written_by_channel"]))
        check("ingestão sem erro", st_live["last_error"] is None, str(st_live["last_error"]))
        status, overview = call("/api/live/overview")
        check("overview agrega o trilho inteiro em um segundo",
              status == 200 and overview.get("granularity", {}).get("bin_size") == 1
              and len(overview.get("points", [])) > 0,
              f"{status} {overview.get('granularity')} {len(overview.get('points', []))}")
        check("configuração time series é lida da coleção real",
              overview.get("collection", {}).get("timeseries") is True
              and overview["collection"].get("time_field") == "ts"
              and overview["collection"].get("meta_field") == "meta"
              and overview["collection"].get("expire_after_seconds", 0) > 0,
              str(overview.get("collection")))
        check("amostra do lote confirmado é serializável",
              overview.get("feed", {}).get("last_document", {}).get("ts") is not None,
              str(overview.get("feed", {}).get("last_document")))
        bucket = overview.get("bucket") or {}
        documento = overview.get("feed", {}).get("last_document", {})
        check("bucket físico pertence ao documento exibido",
              bucket.get("meta") == documento.get("meta")
              and bucket.get("measurements", 0) > 0
              and bucket.get("compressed") is True,
              f"bucket={bucket} documento={documento}")
        status, saude_live = call(f'/api/live/health/{degradado["provedor_id"]}')
        check("saúde ao vivo lê payment_events_live",
              saude_live.get("collection") == "payment_events_live"
              and len(saude_live["points"]) > 0
              and saude_live.get("granularity", {}).get("bin_size") == 1
              and "current_streak" in saude_live
              and saude_live.get("feed", {}).get("written", 0) >= st_live["written"],
              f'{saude_live.get("collection")} {len(saude_live.get("points", []))}')

        from pymongo import MongoClient  # noqa: PLC0415 — só o teste precisa do driver
        uri = None
        for linha in open(os.path.join(ROOT, ".env")):
            if linha.startswith("MONGODB_URI="):
                uri = linha.split("=", 1)[1].strip()
        cli = MongoClient(uri)
        banco = cli[os.getenv("MONGODB_DB", "trilho_pagamentos")]
        opcoes = next((c.get("options", {}) for c in banco.list_collections()
                       if c["name"] == "payment_events_live"), {})
        check("payment_events_live tem TTL", opcoes.get("expireAfterSeconds", 0) > 0,
              str(opcoes.get("expireAfterSeconds")))
        check("payment_events_live é time series", "timeseries" in opcoes, str(list(opcoes)))
        cli.close()

        status, limpo = call("/api/live/clear", method="POST")
        check("limpeza remove o dado ao vivo", limpo["written"] == 0, str(limpo["written"]))

        print("\nchange stream")
        recebidos: list[dict] = []

        def escuta():
            req = urllib.request.Request(BASE + "/api/alerts/stream")
            try:
                with urllib.request.urlopen(req, timeout=25) as res:
                    for linha in res:
                        texto = linha.decode(errors="replace").strip()
                        if texto.startswith("data:") and "incident_id" in texto:
                            recebidos.append(json.loads(texto[5:].strip()))
                            return
            except Exception:  # noqa: BLE001 — o teste falha pela ausência do evento
                pass

        t = threading.Thread(target=escuta, daemon=True)
        t.start()
        time.sleep(2)
        call("/api/demo/reset", method="POST")
        call("/api/incidents", method="POST", payload=corpo)
        t.join(timeout=20)
        check("alerta chega pelo change stream", len(recebidos) == 1, str(len(recebidos)))

        print("\nconcorrência")
        respostas: list[int] = []
        trava = threading.Lock()

        def carga():
            # 24 h é a janela do roteiro. Em 7 dias a consulta sozinha leva ~7,5 s e
            # sob 24 clientes estoura o maxTimeMS — comportamento real, documentado
            # no LIMITATIONS, e não o que este teste está medindo.
            s, _ = call(f'/api/providers/{degradado["provedor_id"]}/health', {"hours": 24})
            with trava:
                respostas.append(s)

        threads = [threading.Thread(target=carga) for _ in range(24)]
        t0 = time.time()
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        elapsed = time.time() - t0
        ok = sum(1 for s in respostas if s == 200)
        recusadas = sum(1 for s in respostas if s == 429)
        erros = [s for s in respostas if s not in (200, 429)]
        check("nenhum 500 sob concorrência", not erros, str(erros))
        check("saturação recusa em vez de enfileirar sem fim",
              ok + recusadas == len(respostas), f"{ok} ok / {recusadas} 429")
        print(f"       24 chamadas em {elapsed:.1f}s — {ok} ok, {recusadas} recusadas")

    status, reset = call("/api/demo/reset", method="POST")
    check("reset também limpa a ingestão ao vivo",
          reset.get("ao_vivo", {}).get("state") == "parado", str(reset.get("ao_vivo")))

    print(f"\n{passou} passaram, {len(falhas)} falharam")
    for f in falhas:
        print(f"  - {f}")
    return 1 if falhas else 0


if __name__ == "__main__":
    sys.exit(main())

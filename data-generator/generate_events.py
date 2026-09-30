"""Eventos do trilho de pagamentos: um documento por transação autorizada.

O evento bruto é gravado, não o agregado. É a tese: o banco guarda a transação e
pergunta depois — p99 por provedor, recusa por janela, velocity de uma conta — em
vez de manter uma malha de contadores pré-agregados que só responde as perguntas
que alguém previu.

`meta` carrega apenas identidade de **rota** (canal, provedor, produto, UF), que é
baixa cardinalidade. `conta_id` é campo de medição, não meta: são milhões de contas,
e colocá-las no metaField multiplicaria as séries por milhões. Essa decisão está
medida no ADR 0002.
"""
from __future__ import annotations

import argparse
import os
import queue
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common  # noqa: E402

PASSO = 60  # segundos por janela de geração

UFS = ["SP", "MG", "RJ", "BA", "PR", "RS", "PE", "CE", "SC", "GO", "PA", "MA"]
UF_PESO = np.array([0.28, 0.11, 0.10, 0.07, 0.06, 0.06, 0.05, 0.05, 0.04, 0.04, 0.03, 0.03])
UF_PESO = UF_PESO / UF_PESO.sum()

ERROS = {
    "pix": ["AB03", "AM04", "BE01", "AM18"],
    "cartao": ["05", "51", "57", "62"],
    "ted": ["X99", "X12"],
}

VARIANTES = {
    "seconds": {"granularity": "seconds"},
    "minutes": {"granularity": "minutes"},
    "span1h": {"bucketMaxSpanSeconds": 3600, "bucketRoundingSeconds": 3600},
    "span1d": {"bucketMaxSpanSeconds": 86400, "bucketRoundingSeconds": 86400},
}


def _chave_serie(doc):
    m = doc["meta"]
    return (m["canal"], m["provedor"], m["produto"], m["uf"], doc["ts"])


def ensure_collection(d, nome: str, variante: str, flat: bool, drop: bool,
                      meta_conta: bool = False):
    if drop and nome in d.list_collection_names():
        d[nome].drop()
    if nome in d.list_collection_names():
        return d[nome]
    if flat:
        return d.create_collection(nome)
    ts = {"timeField": "ts", "metaField": "meta"}
    ts.update(VARIANTES[variante])
    opts = {"timeseries": ts}
    expire = int(os.getenv("TS_EXPIRE_AFTER_SECONDS", "0"))
    if expire > 0:
        opts["expireAfterSeconds"] = expire
    return d.create_collection(nome, **opts)


def cenario_ativo(cenarios, provedor_id, ts):
    for c in cenarios:
        if c["provedor_id"] != provedor_id or c["dia_offset"] is None:
            continue
        inicio = c["_inicio"]
        if inicio <= ts < inicio + timedelta(hours=c["duracao_h"]):
            return c
    return None


def gerar_dia(rng, dia, provedores, cenarios, eps, contas):
    """Emite os eventos de um dia inteiro, em blocos de 10 minutos."""
    por_canal = {canal: common.volume_curve(canal, dia, eps, PASSO)
                 for canal in common.CANAIS}
    provedores_por_canal = {}
    for p in provedores:
        provedores_por_canal.setdefault(p["canal"], []).append(p)
    pesos = {c: np.array([p["participacao"] for p in ps])
             for c, ps in provedores_por_canal.items()}
    pesos = {c: w / w.sum() for c, w in pesos.items()}

    # Um dicionário de meta por rota (canal × provedor × produto × UF), reusado por
    # todos os eventos daquela rota. São ~2.900 rotas contra 90 milhões de eventos.
    meta_cache = {}
    for canal, ps in provedores_por_canal.items():
        for prov in ps:
            tabela = []
            for produto in common.PRODUTOS[canal]:
                for uf in UFS:
                    tabela.append({"canal": canal, "provedor": prov["provedor_id"],
                                   "produto": produto, "uf": uf})
            meta_cache[(canal, prov["provedor_id"])] = tabela

    janelas = 86400 // PASSO
    for bloco_inicio in range(0, janelas, 10):
        docs = []
        for j in range(bloco_inicio, min(bloco_inicio + 10, janelas)):
            inicio = dia + timedelta(seconds=PASSO * j)
            for canal, curva in por_canal.items():
                total = rng.poisson(curva[j])
                if total <= 0:
                    continue
                ps = provedores_por_canal[canal]
                reparticao = rng.multinomial(total, pesos[canal])
                produtos = common.PRODUTOS[canal]
                for prov, n in zip(ps, reparticao):
                    if n <= 0:
                        continue
                    cen = cenario_ativo(cenarios, prov["provedor_id"], inicio)
                    if cen and cen["apagao"]:
                        # Apagão de telemetria: o provedor não reporta. Os eventos
                        # não são recusados — eles simplesmente não chegam ao trilho
                        # de observabilidade, que é o que $densify precisa enxergar.
                        continue
                    f_lat = cen["fator_latencia"] if cen else 1.0
                    f_rec = cen["fator_recusa"] if cen else 1.0

                    lat = common.latencia(rng, canal, n, f_lat)
                    taxa = min(prov["recusa_base"] * f_rec, 0.95)
                    recusado = rng.random(n) < taxa
                    ticket_lo, ticket_hi = common.CANAIS[canal]["ticket"]
                    valores = np.clip(rng.lognormal(np.log(ticket_lo * 2.2), 0.9, n),
                                      1.0, ticket_hi * 12)
                    ufs = rng.choice(len(UFS), size=n, p=UF_PESO)
                    prods = rng.integers(0, len(produtos), size=n)
                    segundos = rng.random(n) * PASSO
                    ids_conta = rng.integers(0, contas, size=n)
                    erros_idx = rng.integers(0, len(ERROS[canal]), size=n)

                    # Tudo em listas de Python de uma vez: indexar numpy elemento a
                    # elemento e chamar rng.choice por evento derrubava a geração de
                    # ~45 k/s para 8,6 k/s. O metaField também é compartilhado por
                    # rota em vez de recriado por documento.
                    metas = meta_cache[(canal, prov["provedor_id"])]
                    erros_canal = ERROS[canal]
                    for ts_off, meta_i, valor, latencia_ms, ok, erro_i, conta in zip(
                            segundos.tolist(), (prods * len(UFS) + ufs).tolist(),
                            np.round(valores, 2).tolist(), np.round(lat, 1).tolist(),
                            (~recusado).tolist(), erros_idx.tolist(),
                            ids_conta.tolist()):
                        docs.append({
                            "ts": inicio + timedelta(seconds=ts_off),
                            "meta": metas[meta_i],
                            "valor": valor,
                            "latencia_ms": latencia_ms,
                            "aprovado": ok,
                            "erro": None if ok else erros_canal[erro_i],
                            "conta_id": f"C{conta:09d}",
                        })
        if docs:
            yield docs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=int(os.getenv("DAYS", "7")))
    ap.add_argument("--eps", type=float, default=float(os.getenv("EVENTS_PER_SECOND", "150")))
    ap.add_argument("--accounts", type=int, default=int(os.getenv("ACCOUNTS", "2000000")))
    ap.add_argument("--collection", default="payment_events")
    ap.add_argument("--variant", default="span1d", choices=list(VARIANTES))
    ap.add_argument("--flat", action="store_true")
    ap.add_argument("--drop", action="store_true")
    ap.add_argument("--batch", type=int, default=25000)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--no-sort", dest="sort", action="store_false",
                    help="insere na ordem de geração (para reproduzir o ADR 0001)")
    ap.set_defaults(sort=True)
    ap.add_argument("--db", default=None)
    ap.add_argument("--seed", type=int, default=common.SEED)
    args = ap.parse_args()

    d = common.db(args.db)
    provedores = list(d.provedores.find())
    cenarios = list(d.degradation_scenarios.find())
    if not provedores:
        sys.exit("sem provedores: rode generate_registry.py antes")

    fim = common.utc_midnight(datetime.now(timezone.utc))
    inicio = fim - timedelta(days=args.days)
    for c in cenarios:
        if c["dia_offset"] is not None:
            c["_inicio"] = fim + timedelta(days=c["dia_offset"], hours=c["hora"])

    col = ensure_collection(d, args.collection, args.variant, args.flat, args.drop)
    rng = np.random.default_rng(args.seed)

    # Medido neste cluster: uma thread insere ~10 k eventos/s e quatro chegam a
    # ~20 k/s; acima disso o M20 (2 vCPU) não escala e w=1 não muda nada. A geração
    # em si faz 565 k/s, então o gargalo é inteiramente o servidor.
    #
    # Cada worker é dono de um conjunto de rotas (hash do meta), e ordena o próprio
    # lote por série e tempo. Ordenar sem particionar não adianta: quatro workers
    # escrevendo lotes ordenados mas sobrepostos re-intercalam as mesmas séries no
    # servidor, e a compressão delta do bucket volta a degradar — medido, 24 B/evento
    # em vez de 11. É a mesma razão pela qual um consumer group particionado por rota
    # ganha isso de graça e um que faz fan-out não.
    filas: list[queue.Queue] = [queue.Queue(maxsize=3) for _ in range(args.workers)]
    escritos = [0]
    trava = threading.Lock()
    falha: list[BaseException] = []

    def escritor(qi: int):
        fila = filas[qi]
        while True:
            lote = fila.get()
            if lote is None:
                fila.task_done()
                return
            try:
                col.insert_many(lote, ordered=False)
                with trava:
                    escritos[0] += len(lote)
            except BaseException as exc:  # noqa: BLE001 — reportado no fim
                falha.append(exc)
            finally:
                fila.task_done()

    threads = [threading.Thread(target=escritor, args=(i,), daemon=True)
               for i in range(args.workers)]
    for t in threads:
        t.start()

    def particao(rota) -> int:
        return hash(rota) % args.workers

    t0 = time.time()
    # Um balde por ROTA (canal × provedor × produto × UF), não por partição. Medido no
    # ADR 0001: agrupar por partição ainda deixa 81 rotas intercaladas dentro dela e o
    # ganho evapora (26 B/evento). Agrupar pela série inteira derruba para ~17.
    baldes: dict[tuple, list] = {}
    por_rota = max(args.batch // 40, 300)
    # Rotas prontas vão para um lote grande antes de virar insert_many. Enviar cada
    # rota isolada (300 docs) multiplicou os round trips e derrubou a carga para
    # ~1 k eventos/s; o que importa é a rota ficar contígua DENTRO do lote, não o
    # lote conter uma rota só.
    pendentes: list[list] = [[] for _ in range(args.workers)]
    tamanho_pendente = [0] * args.workers

    def despachar(rota, docs_rota):
        if args.sort:
            docs_rota.sort(key=lambda x: x["ts"])
        i = particao(rota)
        pendentes[i].append(docs_rota)
        tamanho_pendente[i] += len(docs_rota)
        if tamanho_pendente[i] >= args.batch:
            lote = [doc for grupo in pendentes[i] for doc in grupo]
            filas[i].put(lote)
            pendentes[i] = []
            tamanho_pendente[i] = 0

    def drenar():
        for i in range(args.workers):
            if pendentes[i]:
                filas[i].put([doc for grupo in pendentes[i] for doc in grupo])
                pendentes[i] = []
                tamanho_pendente[i] = 0

    for n in range(args.days):
        dia = inicio + timedelta(days=n)
        for bloco in gerar_dia(rng, dia, provedores, cenarios, args.eps, args.accounts):
            for doc in bloco:
                m = doc["meta"]
                rota = (m["canal"], m["provedor"], m["produto"], m["uf"])
                balde = baldes.get(rota)
                if balde is None:
                    balde = baldes[rota] = []
                balde.append(doc)
                if len(balde) >= por_rota:
                    despachar(rota, balde)
                    baldes[rota] = []
            if falha:
                break
        # Fecha o dia: um bucket é diário, então nada deve atravessar a virada.
        for rota, balde in list(baldes.items()):
            if balde:
                despachar(rota, balde)
                baldes[rota] = []
        drenar()
        print(f"  dia {n+1}/{args.days}  {escritos[0]:>12,} eventos  "
              f"{escritos[0]/max(time.time()-t0,1e-6):>9,.0f}/s", flush=True)
        if falha:
            break
    for i in range(args.workers):
        filas[i].put(None)
    for fila in filas:
        fila.join()
    if falha:
        raise falha[0]
    total = escritos[0]

    elapsed = time.time() - t0
    d.dataset_info.update_one(
        {"_id": args.collection},
        {"$set": {"collection": args.collection, "first_ts": inicio, "last_ts": fim,
                  "days": args.days, "events": total, "eps": args.eps,
                  "accounts": args.accounts, "providers": len(provedores),
                  "variant": args.variant, "flat": bool(args.flat),
                  "sorted_insert": bool(args.sort),
                  "loaded_at": datetime.now(timezone.utc)}},
        upsert=True)

    print(f"\n{args.collection}: {total:,} eventos em {elapsed:,.1f}s "
          f"({total/elapsed:,.0f}/s), {len(provedores)} provedores, {args.days} dias")


if __name__ == "__main__":
    main()

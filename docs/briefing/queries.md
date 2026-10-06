# Queries, pipelines e índices — atlas-timeseries-payments

Referência rápida para "onde está a query X" e "por que esse índice existe". Todo
trecho abaixo é código real do repositório, com arquivo:linha. Nenhum dado real ou
sensível — tudo é sintético.

## Coleções

| Coleção | Tipo | Papel | Quem escreve |
|---|---|---|---|
| `payment_events` | **time series** | um documento por transação autorizada (histórico) | `data-generator/generate_events.py` |
| `payment_events_flat` | normal | fatia do mesmo dado, só para comparar armazenamento | idem |
| `payment_events_live` | **time series**, TTL 1h | ingestão ao vivo por trás do botão Play | `backend/app/services/live.py` |
| `provedores` | normal | PSPs/adquirentes/bancos, SLA e recusa-base | `generate_registry.py` |
| `degradation_scenarios` | normal | verdade de terra dos cenários plantados | idem |
| `demo_accounts` | normal | contas plantadas com velocity conhecido | `generate_demo_accounts.py` |
| `incidents` | normal | incidentes abertos pela transação ACID | backend |
| `incident_alerts` | normal | eventos do change stream | backend |
| `dataset_info` | normal | primeiro/último evento de cada carga | `generate_events.py` |

---

## Time series: configuração e motivação

### `payment_events` (histórico)

Arquivo: `data-generator/generate_events.py:53-67`

```python
def ensure_collection(d, nome, variante, flat, drop, meta_conta=False):
    ...
    ts = {"timeField": "ts", "metaField": "meta"}
    ts.update(VARIANTES[variante])         # granularity ou bucketMaxSpanSeconds/bucketRoundingSeconds
    opts = {"timeseries": ts}
    expire = int(os.getenv("TS_EXPIRE_AFTER_SECONDS", "0"))
    if expire > 0:
        opts["expireAfterSeconds"] = expire
    return d.create_collection(nome, **opts)
```

Variantes de bucketing testadas experimentalmente (`generate_events.py:40-45`,
usadas pelo ADR 0001):

```python
VARIANTES = {
    "seconds": {"granularity": "seconds"},
    "minutes": {"granularity": "minutes"},
    "span1h":  {"bucketMaxSpanSeconds": 3600,  "bucketRoundingSeconds": 3600},
    "span1d":  {"bucketMaxSpanSeconds": 86400, "bucketRoundingSeconds": 86400},
}
```

A collection de produção (documentada em `schema/collections.md`,
confirmada no schema) usa:

```js
db.createCollection("payment_events", {
  timeseries: { timeField: "ts", metaField: "meta",
                bucketMaxSpanSeconds: 86400, bucketRoundingSeconds: 86400 }
})
```

**Por quê `bucketMaxSpanSeconds: 86400` (1 dia) e não o padrão de `granularity`:**
o ADR 0001 (`docs/adr/0001-bucketing.md`) mede o efeito do span do bucket e da
ordem de escrita no armazenamento — um bucket de um dia por rota comprime melhor
porque agrupa mais medições da mesma série sem estourar o limite de 1000
medições/bucket do MongoDB de forma prematura.

**`metaField: "meta"`** carrega só a rota (`canal`, `provedor`, `produto`, `uf`) —
cerca de 2 900 combinações. Isso é o que mantém a camada de buckets pequena: cada
valor distinto de `meta` é uma série física própria.

### `payment_events_live` (ao vivo)

Arquivo: `backend/app/services/live.py:43-60`

```python
def ensure_collection():
    d = db()
    if COLLECTION not in d.list_collection_names():
        d.create_collection(
            COLLECTION,
            timeseries={"timeField": "ts", "metaField": "meta",
                        "bucketMaxSpanSeconds": 300, "bucketRoundingSeconds": 300},
            expireAfterSeconds=LIVE_TTL_SECONDS)   # default 3600s = 1h
    col = d[COLLECTION]
    col.create_index([("ts", 1)], name="ts_1")
    col.create_index([("meta.provedor", 1), ("ts", 1)])
    col.create_index([("meta.canal", 1), ("ts", 1)])
    return col
```

**Por que `bucketMaxSpanSeconds: 300` (5 min) aqui e 1 dia na coleção
histórica:** o objetivo da coleção ao vivo é ver o dado chegar e expirar rápido —
não densidade de armazenamento. Bucket curto = evidência mais "fresca" no palco.

**Por que `expireAfterSeconds` (TTL) só existe aqui:** permite rodar o roteiro de
demo várias vezes no mesmo dia sem limpeza manual — o dado ao vivo apaga sozinho.
Numa time series o TTL expira o **bucket inteiro**, não documento a documento, e o
delete manual é restrito — por isso a decisão de não escrever eventos ao vivo na
coleção histórica (ver `queries.md#a-transação-de-incidente` e
`architecture.md`).

**Índice explícito `{ts: 1}`** (além do índice implícito da time series sobre
`meta`+`ts`): o `overview()` (abaixo) filtra só por tempo, sem `meta`; sem esse
índice a consulta visitaria todas as séries da janela.

---

## Índices

Fonte única: `common.INDEXES` em `data-generator/common.py`, aplicada por
`common.ensure_indexes()` no fim de `scripts/reset_demo.py` (idempotente —
`create_index` é no-op se já existe; não depende de mongosh). Os índices 12–14 são
criados por `services/live.py::ensure_collection()` no primeiro play.

| # | Coleção | Índice | Por quê |
|---|---|---|---|
| 1 | `payment_events` | `{"meta.provedor": 1, ts: 1}` | saúde do provedor, caminho analítico |
| 2 | `payment_events` | `{"meta.canal": 1, ts: 1}` | percentil de latência por canal inteiro |
| 3 | `payment_events` | `{conta_id: 1, ts: 1}` | velocity de conta, caminho de autorização — **campo de medição indexado**, não metaField |
| 4 | `provedores` | `{provedor_id: 1}` único | lookup pontual atrás de cada tela |
| 5 | `provedores` | `{canal: 1}` | listagem de provedores por canal |
| 6 | `provedores` | `{em_incidente: 1}` esparso | fila de abertos + reset determinístico da demo |
| 7 | `degradation_scenarios` | `{provedor_id: 1, kind: 1}` único | consulta de verdade de terra |
| 8 | `demo_accounts` | `{conta_id: 1}` único | lookup de conta plantada |
| 9 | `incidents` | `{provedor_id: 1, status: 1}` | fila de incidentes abertos |
| 10 | `incidents` | `{opened_at: -1}` | feed de alertas |
| 11 | `incident_alerts` | `{at: -1}` | histórico de alertas via `GET /api/alerts` |
| 12 | `payment_events_live` | `{ts: 1}` | overview ao vivo filtra só por tempo (ver acima) |
| 13 | `payment_events_live` | `{"meta.provedor": 1, ts: 1}` | saúde ao vivo por provedor |
| 14 | `payment_events_live` | `{"meta.canal": 1, ts: 1}` | consulta por canal na série ao vivo |

**Índice em coleção time series indexa os buckets, não os eventos individuais** —
por isso `{conta_id: 1, ts: 1}` (índice 3) é o índice "interessante": indexa um
campo de **medição**, o que é o que torna a dimensão de alta cardinalidade
(milhões de contas) consultável sem transformar cada conta em série própria.

### Dois índices deliberadamente ausentes

- **Nenhum índice em `valor` ou `latencia_ms`.** "Todo evento acima de X" não é
  pergunta deste workload, e o índice teria o tamanho do dado que indexa.
- **Nenhum `{meta.uf: 1, ts: 1}`.** Um corte por UF sempre acompanha canal ou
  provedor, que já prefixam um índice existente.

---

## As queries e pipelines

### 1. Latência por percentil — `backend/app/db/latency.py:28-81`

O que faz: agrupa por bin de tempo (`$dateTrunc`, granularidade decidida pelo
servidor) e calcula p50/p95/p99 com `$percentile` sobre o evento bruto, direto no
pipeline — sem contador pré-agregado.

```python
{"$match": {**match, "ts": {"$gte": start, "$lt": end}}},
{"$group": {
    "_id": {"$dateTrunc": {"date": "$ts", "unit": unit, "binSize": size}},
    "eventos": {"$sum": 1},
    "aprovados": {"$sum": {"$cond": ["$aprovado", 1, 0]}},
    "volume": {"$sum": "$valor"},
    "lat": {"$percentile": {"input": "$latencia_ms", "p": [0.5, 0.95, 0.99],
                             "method": "approximate"}},
}},
```

Onde é chamada: `serie()` em `latency.py:84-117`, exposta em
`GET /api/latency?canal=&provedor=&hours=&fill=`.

**Por que `method: "approximate"` (t-digest):** modo suportado sobre um fluxo
deste tamanho — a aproximação é irrelevante para decidir se um provedor degradou.

**Por que percentil e nunca média:** um trilho de pagamento é julgado pela cauda;
a média esconde exatamente o cliente que esperou 4 segundos.

Teto explícito: sem provedor, a busca varre o canal inteiro — medido em 6,5 s para
24h, acima do teto de 15 s (`MAX_TIME_MS`) em 7 dias. `CANAL_MAX_HORAS = 24.0`
(`latency.py:23`) recusa cedo com uma instrução, em vez de queimar 15s e devolver
503.

### 2. Reconstrução de lacuna (`$densify`/`$fill`) — `latency.py:56-69`

O que faz: quando `fill=true`, completa a série com `$densify` (cria os pontos que
faltam) e `$fill` com `method: "locf"` (carrega o último valor observado) para
percentis e taxa de recusa; zera contagem/volume nos pontos preenchidos.

```python
{"$densify": {"field": "ts",
              "range": {"step": size, "unit": unit, "bounds": [start, end]}}},
{"$fill": {"sortBy": {"ts": 1},
           "output": {"p50": {"method": "locf"}, "p95": {"method": "locf"},
                      "p99": {"method": "locf"}, "taxa_recusa": {"method": "locf"},
                      "eventos": {"value": 0}, "volume": {"value": 0}}}},
{"$set": {"reconstruido": {"$ne": ["$medido", True]},
          "metodo": {"$cond": [{"$ne": ["$medido", True]}, "locf", None]}}},
```

Por que existe: simula o PSP-021 que para de reportar telemetria por 40 minutos
(cenário plantado — ver `docs/briefing/queries.md#cenários-plantados` abaixo). O
backend nunca "tapa buraco" em Python — a reconstrução é sempre pipeline, e todo
ponto inventado volta marcado `reconstruido: true` + `metodo: "locf"`, e o
frontend desenha esse trecho tracejado (`frontend/src/Chart.jsx`, série
`reconstruido` sobre p99).

### 3. Saúde do provedor (histórico) — `backend/app/db/providers.py:25-97`

O que faz: agrupa por janela, calcula recusa e p99 por bin, depois usa
`$setWindowFields` para média móvel e desvio-padrão sobre uma janela que **termina
antes** da janela julgada, e daí o z-score.

```python
{"$setWindowFields": {
    "sortBy": {"ts": 1},
    "output": {
        "recusa_base": {"$avg": "$taxa_recusa",
                         "window": {"documents": [-janela, -lag]}},
        "recusa_desvio": {"$stdDevSamp": "$taxa_recusa",
                           "window": {"documents": [-janela, -lag]}},
        "p99_base": {"$avg": "$p99", "window": {"documents": [-janela, -lag]}},
        "p99_desvio": {"$stdDevSamp": "$p99", "window": {"documents": [-janela, -lag]}},
    }}},
{"$set": {
    "z_recusa": {"$cond": [{"$gt": ["$recusa_desvio", 0]},
        {"$divide": [{"$subtract": ["$taxa_recusa", "$recusa_base"]}, "$recusa_desvio"]}, 0]},
    "z_p99": {"$cond": [{"$gt": ["$p99_desvio", 0]},
        {"$divide": [{"$subtract": ["$p99", "$p99_base"]}, "$p99_desvio"]}, 0]},
}},
```

Anomalia exige três condições simultâneas (`providers.py:73-84`): z acima do
limiar (`Z_SCORE_THRESHOLD`, default 3.0), um desvio absoluto que também bate um
piso mínimo (`MIN_DELTA_PP` ou `MIN_DELTA_RATIO` — sem piso, um provedor muito
estável tem desvio-padrão minúsculo e vira z=6 com puro ruído), e volume mínimo de
eventos na janela (`MIN_EVENTS_PER_WINDOW`).

Incidente = `Z_MIN_WINDOWS` (default 3) janelas anômalas **seguidas**
(`providers.py:109-120`, contagem de streak em Python sobre o resultado da
query — pico isolado é ruído de amostragem).

**Por que a janela de base termina em `-lag` e não em `-1`:** com base terminando
em `-1`, uma degradação de 2h entrava na própria base e o z despencava depois de
2 janelas — o cenário plantado deixava de ser detectado. `Z_BASELINE_LAG=4`,
`Z_BASELINE_WINDOWS=96` (`backend/app/config.py:40-41`).

Onde é chamada: `saude()`, exposta em `GET /api/providers/{id}/health?hours=`.

### 4. Saúde do provedor (ao vivo) — `providers.py:148-268`

O que faz: mesma ideia, mas sobre `payment_events_live`, comparando contra a
**linha de base cadastrada** (`provedores.recusa_base`) em vez de aprendida da
série — a série ao vivo dura no máximo 1h e é curta demais para aprender a própria
base (medido: uma degradação de 10 min entrava na janela de base de 8 min e virava
sua própria referência, recusa marcando 47% com z 0,0).

```python
{"$setWindowFields": {
    "sortBy": {"ts": 1},
    "output": {
        "eventos_janela": {"$sum": "$eventos",
            "window": {"range": [janela_inicio, 0], "unit": "second"}},
        "recusados_janela": {"$sum": "$recusados",
            "window": {"range": [janela_inicio, 0], "unit": "second"}},
    }}},
```

Margem de ruído binomial sobre a base cadastrada (`providers.py:190-199`):
`LIVE_CONFIDENCE_SIGMAS` (default 3.0) desvios-padrão binomiais — uma amostra
pequena não pode abrir incidente só por ter recebido duas recusas por acaso.

Campo renomeado deliberadamente: `delta_ratio_recusa` (não `z_recusa`) porque
**não é** um z-score de verdade — não há desvio-padrão de janela anterior aqui, é
razão percentual contra a base cadastrada. Correção de auditoria: o nome anterior, `z_recusa`, era enganoso.

Onde é chamada: `saude_ao_vivo()`, exposta em
`GET /api/live/health/{provedor_id}`.

### 5. Velocity de conta — `backend/app/db/velocity.py:22-50`

O que faz: uma única passada sobre a janela mais larga (ex.: 24h), com cada janela
menor (1h, 6h) calculada como `$cond` dentro do mesmo `$group`.

```python
{"$match": {"conta_id": conta_id, "ts": {"$gte": inicio, "$lt": fim}}},
{"$group": {"_id": None,
            "j_1h_eventos": {"$sum": {"$cond": [{"$gte": ["$ts", corte_1h]}, 1, 0]}},
            "j_1h_valor": {"$sum": {"$cond": [{"$gte": ["$ts", corte_1h]}, "$valor", 0]}},
            "j_1h_recusados": {"$sum": {"$cond": [
                {"$and": [{"$gte": ["$ts", corte_1h]}, {"$eq": ["$aprovado", False]}]}, 1, 0]}},
            # ... mesmo padrão para 6h e 24h ...
            "canais": {"$addToSet": "$meta.canal"},
            "ufs": {"$addToSet": "$meta.uf"}}},
```

Por que é uma passada e não três queries: esta consulta roda **dentro** do
caminho de autorização, com orçamento de dezenas de milissegundos. Três queries
seriam três varreduras e três round trips.

Índice usado: `{conta_id: 1, ts: 1}` — o mesmo índice 3 da tabela acima, sobre
campo de medição.

Onde é chamada: `features()`, exposta em `GET /api/velocity/{conta_id}`.

### 6. Comparação de armazenamento — `backend/app/db/storage.py:24-46`

O que faz: `$collStats` com `storageStats` sobre `payment_events` (time series) e
`payment_events_flat` (normal), comparando bytes por evento.

```python
st = next(d[nome].aggregate([{"$collStats": {"storageStats": {}}}]))["storageStats"]
...
"bytes_per_event": round(storage / docs, 2),
"total_bytes_per_event": round((storage + index) / docs, 2),
"timeseries": bool(st.get("timeseries")),
"buckets": (st.get("timeseries") or {}).get("bucketCount"),
```

Por que comparação é **por evento** e não em bytes absolutos: as duas coleções
cobrem períodos diferentes de propósito; comparar tamanho bruto seria desonesto.

Custo medido: `$collStats` sobre 2,6 milhões de buckets leva ~32s na primeira
chamada — por isso há aquecimento em thread separada no startup
(`storage.aquecer()`, chamado em `main.py:_startup`) e cache de 10 minutos
(`CACHE_SECONDS = 600.0`).

Onde é chamada: `comparison()`, exposta em `GET /api/storage?force=`.

### 7. Ranking de provedores — `providers.py:271-305`

O que faz: mesma agregação de percentil, agrupada por provedor, com `$lookup` na
coleção `provedores` para trazer o SLA e marcar quem está fora dele.

```python
{"$group": {"_id": {"provedor": "$meta.provedor", "canal": "$meta.canal"}, ...}},
{"$lookup": {"from": "provedores", "localField": "provedor",
             "foreignField": "provedor_id", "as": "cadastro"}},
{"$set": {"sla_p99_ms": {"$first": "$cadastro.sla_p99_ms"}}},
{"$set": {"fora_do_sla": {"$gt": ["$p99", "$sla_p99_ms"]}}},
```

Por que existe: esse `$lookup` — join operacional dentro do próprio banco — é
o tipo de consulta que uma stack de métricas separada (Prometheus/InfluxDB) não
faz sem exportar para um terceiro sistema.

Teto: `hours = min(hours, RANKING_MAX_HOURS)` (default 6.0) — varrer todos os
provedores em 24h não cabe no orçamento de `MAX_TIME_MS`.

Onde é chamada: `ranking()`, exposta em `GET /api/ranking?hours=&limit=`.

### 8. Overview da ingestão ao vivo — `backend/app/db/live.py:85-124`

O que faz: agrega os eventos de `payment_events_live` em bins de 1 segundo, sobre
os últimos 60s, e devolve junto a configuração real da coleção e o bucket físico
que contém o último documento confirmado.

```python
end = datetime.now(timezone.utc).replace(microsecond=0)   # exclui o segundo corrente
window_start = end - timedelta(seconds=60)
start = max(window_start, session_started_at) if session_started_at else window_start
pipe = [
    {"$match": {"ts": {"$gte": start, "$lt": end}}},
    {"$group": {"_id": {"$dateTrunc": {"date": "$ts", "unit": "second"}},
                "eventos": {"$sum": 1}, "volume": {"$sum": "$valor"}}},
    {"$set": {"ts": "$_id"}},
    {"$sort": {"ts": 1}},
    ...
]
```

Por que exclui o segundo corrente: ele ainda está sendo escrito — exibi-lo cria
uma queda falsa no último ponto que desaparece no poll seguinte.

Config real da coleção (`live.py:14-35`), via `listCollections`, cache de 30s:

```python
result = with_retry(lambda: db().command("listCollections", filter={"name": COLLECTION}))
timeseries = result["cursor"]["firstBatch"][0]["options"]["timeseries"]
```

Bucket físico (`live.py::_bucket_snapshot`). MongoDB 8.2+/9.0 recusa ler
`system.buckets.*` ("Direct access to timeseries buckets namespaces is not allowed
anymore"); o caminho suportado é o próprio namespace com `rawData: true`, e o
namespace interno fica como segunda tentativa para servidores antigos:

```python
db().command("find", "payment_events_live",
    filter={**{f"meta.{k}": v for k, v in meta.items()},
            "control.min.ts": {"$lte": timestamp}, "control.max.ts": {"$gte": timestamp}},
    projection={"_id": 1, "meta": 1, "control.version": 1, "control.count": 1,
                "control.min.ts": 1, "control.max.ts": 1},
    limit=1, singleBatch=True, maxTimeMS=MAX_TIME_MS, rawData=True)
```

Expõe só `meta` e `control.min/max/count/version` — o cabeçalho do bucket, nunca
o conteúdo bruto do sistema. `control.version >= 2` indica bucket comprimido.

Onde é chamada: `overview()`, exposta em `GET /api/live/overview`.

---

## A transação de incidente — `backend/app/db/incidents.py:22-84`

O que faz: abre um incidente com três escritas ou nenhuma, dentro de uma sessão
transacional.

```python
with client().start_session() as sessao:
    with sessao.start_transaction():
        resultado = d.provedores.update_one(
            {"provedor_id": provedor_id, "em_incidente": {"$ne": True}},
            {"$set": {"em_incidente": True, "incident_id": incidente_id, "flagged_at": agora}},
            session=sessao)
        if resultado.modified_count == 0:
            raise DuplicateKeyError(f"provedor {provedor_id} já está em incidente")
        d.incidents.insert_one(dict(doc), session=sessao)
        d.provedores.update_one({"provedor_id": provedor_id},
            {"$set": {"last_event": {"kind": "incidente_aberto",
                                      "incident_id": incidente_id, "at": agora}}},
            session=sessao)
```

Por que é transação: provedor marcado como degradado sem incidente correspondente
é achado de auditoria. A checagem `em_incidente` no início é só fast-path — a
checagem real de corrida é o `modified_count` dentro da transação (correção de
auditoria: sem isso, uma corrida concorrente podia abrir
dois incidentes "abertos" para o mesmo provedor).

O evento que acorda o change stream é a própria marcação em `provedores` +
`incidents.insert_one` — nunca uma escrita sintética em coleção separada. O
listener (`app/services/alerts.py`) observa `incidents`, não `payment_events`: lá
o change stream dispara uma vez por transação (dezenas por segundo), útil para uma
pipeline, inútil para acordar uma tela.

---

## Cenários plantados (verdade de terra)

Consulta: `registry.cenarios()` (`backend/app/db/registry.py:19-22`), exposta em
`GET /api/scenarios`, rotulada explicitamente como seed na própria API.

| Provedor | Tipo | O que faz |
|---|---|---|
| ADQ-003 | `recusa` | recusa 4× a própria base por 2h |
| PSP-014 | `latencia` | p99 4,2× por 3h |
| PSP-021 | `apagao` | para de reportar telemetria por 40 min |
| ADQ-006 | `controle` | recusa estruturalmente alta e **estável** — não deve abrir incidente |

O controle negativo (ADQ-006) é o ponto: sua recusa é **maior** que o pico do
adquirente realmente degradado, então um limiar absoluto acusaria o saudável e
perderia o doente. Só um detector que compara cada provedor com sua própria
história recente acerta os dois — e é por isso que o pipeline #3 é construído do
jeito que é.

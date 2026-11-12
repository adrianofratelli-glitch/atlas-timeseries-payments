# Mapa do código — atlas-timeseries-payments

> Também conhecida como: **Time Series pagamentos** (pasta antiga `atlas-timeseries-pagamentos`). Portas: frontend **5400** / backend **8400**. Em uma frase: mostra o MongoDB gravando e agregando uma coleção time series (PIX + cartão + TED) enquanto ela nasce, com o bucket físico e o pipeline executado visíveis na tela.

Atalhos: [architecture](briefing/architecture.md) · [queries](briefing/queries.md) · [ui-flows](briefing/ui-flows.md). Este mapa só aponta arquivo e linha.

Duas coisas que ajudam em call:
- A **tela de palco é uma só**, com um botão (Iniciar/Parar ingestão). O backend tem muito mais rotas (provedores, detecção de degradação, velocity, incidentes, ranking), mas elas pertencem à "engenharia arquivada": existem, têm teste e benchmark, e **nenhum botão da tela atual as chama**.
- Regra do backend (`backend/main.py:1-5`): nenhuma rota importa pymongo. Toda consulta mora em `backend/app/db/`; o ciclo de ingestão mora em `backend/app/services/`.

## 1. Árvore anotada

```
.
├── start.sh                    Launcher: sobe backend (8400) e frontend (5400) em primeiro plano
├── .env.example                Variáveis locais (nomes apenas; ver seção 4)
├── backend/
│   ├── main.py                 FastAPI: TODAS as rotas (332 linhas). Só HTTP; delega para app/db e app/services
│   └── app/
│       ├── config.py           Lê as variáveis de ambiente (limiares, TTL, ritmo do feed, portas)
│       ├── db/
│       │   ├── client.py       Único MongoClient, `with_retry`, `insert_idempotent` (insert_many que não duplica na repetição)
│       │   ├── live.py         Pipeline do gráfico ao vivo (`overview`) + configuração real da coleção + bucket físico
│       │   ├── storage.py      `$collStats`: comparação de armazenamento (time series × coleção comum)
│       │   ├── latency.py      Percentis por janela; `$densify`/`$fill` para lacunas (engenharia arquivada)
│       │   ├── providers.py    Saúde do provedor (`$setWindowFields`, z-score) e ranking (arquivado)
│       │   ├── velocity.py     Features de velocity de conta para antifraude (arquivado)
│       │   ├── incidents.py    Transação que abre/encerra incidente (arquivado)
│       │   ├── registry.py     Leitura de cadastro: provedores, cenários, contas de demo
│       │   └── ranges.py       Resolve janela de tempo e valida limites
│       └── services/
│           ├── live.py         `LiveFeed`: thread que gera eventos e grava lotes; cria a coleção time series (`ensure_collection`)
│           ├── alerts.py       `AlertHub`: change stream em `incidents` -> SSE de alertas (arquivado)
│           └── limits.py       Limite de concorrência por tipo de consulta (`limits.slot`)
├── frontend/
│   ├── vite.config.js          Porta 5400
│   └── src/
│       ├── App.jsx             A tela única: estado, polling de 1 s, layout, botão
│       ├── api.js              Cliente HTTP (todas as chamadas ao backend, com prazo e erro traduzido)
│       ├── Chart.jsx           Gráfico uPlot (modo ativo: `throughput_live`)
│       ├── QueryDetails.jsx    Gaveta "Ver query / chamada executada"
│       └── pov-signature.css   Visual comum do portfólio (cópia)
├── data-generator/             Gera o histórico sintético (`payment_events`) e o cadastro
│   ├── common.py               Conexão, guarda de escrita (recusa banco sem `_test`), índices
│   ├── generate_registry.py    Provedores e cenários de degradação plantados
│   ├── generate_events.py      Eventos históricos em lote
│   ├── generate_demo_accounts.py  Contas de demonstração (velocity)
│   └── run_all.sh              Atalho para scripts/reset_demo.py
├── scripts/reset_demo.py       RESET ÚNICO: recria cadastro, eventos, amostra plana e contas
├── queries/                    Benchmarks e experimentos (bench.py, bucket_experiment.py, cardinality_experiment.py, feature_probes.py + JSON de resultados)
├── schema/collections.md       Descrição das coleções
├── tests/                      Testes (ver seção 6)
└── docs/                       briefing/, adr/ (bucketing, cardinalidade), business-case, demo-script, screenshots
```

## 2. "Apertei o botão X, o que roda?"

Na tela de palco existe **um botão**. Além dele, há chamadas automáticas ao abrir a página e o polling.

| Tela / ação | Componente (arquivo:linha) | Chamada HTTP | Handler | Função de negócio | MongoDB |
|---|---|---|---|---|---|
| Abrir a página (selo "Atlas conectado") | `App.jsx:105` (`api.health`, `api.liveStatus`) | `GET /health`, `GET /api/live/status` | `main.py:89` (`health`), `main.py:286` (`live_status`) | `health`: ping + lê `dataset_info`; `feed.status()` | `ping`; `dataset_info`: `find_one`; `payment_events`: `estimated_document_count` (só se faltar o dado) |
| Abrir a página (faixa de bucketização) | `App.jsx:117` (`api.storage`) | `GET /api/storage` | `main.py:172` (`storage_comparison`) | `app/db/storage.py::comparison` (`:62`); aquecido no startup por `storage.aquecer` (`:49`, chamado em `main.py:39`) | `$collStats` `storageStats` em `payment_events` e `payment_events_flat` (`storage.py:28`) |
| **Iniciar ingestão** | `App.jsx:134` (`alternarIngestao`), botão `App.jsx:208` | `POST /api/live/start` (`api.js` `liveStart`) | `main.py:257` (`live_start`) | `LiveFeed.start` (`services/live.py:159`) abre a thread `live-feed` que roda `LiveFeed._run` (`:299`); `ensure_collection` (`:68`) | cria `payment_events_live` (`create_collection` com `timeseries` e `expireAfterSeconds`, `:84`) + 3 índices (`:94-96`); lê `provedores` (`:310`) e `dataset_info` (`:333`); grava lotes com `insert_idempotent` (`:406`) -> `insert_many(ordered=False)` (`client.py:93`) |
| (automático a cada 1 s enquanto roda) Curva, métricas, bucket físico, documento | `App.jsx:122` (efeito) + `atualizar` (`App.jsx:82`, `api.liveOverview`) | `GET /api/live/overview` | `main.py:291` (`live_overview`) | `app/db/live.py::overview` (`:112`) | `payment_events_live`: `aggregate` (`:132`) com `$dateTrunc`; `listCollections` (`_collection_options` `:20`); `find` com `rawData:true` no bucket (`_find_bucket_raw` `:99`, legado `:107`) |
| **Parar ingestão** | `App.jsx:138` | `POST /api/live/stop` | `main.py:276` (`live_stop`) | `LiveFeed.stop` (`services/live.py:208`) | nenhuma query nova |
| Abrir "Ver query / chamada executada" | `QueryDetails.jsx` (recebe `overview.pipeline`, `App.jsx:312`; pede o plano no `onToggle`) | `GET /api/live/plan` (`api.js` `livePlan`), uma vez por abertura | `main.py:306` (`live_plan`) | `app/db/live.py::plano` (`:234`), resumido por `resumir_plano` (`:208`) e `evidence.js::resumoDoPlano` | `explain` do mesmo pipeline com `verbosity: "queryPlanner"` (não executa), cache de 60 s |
| Aviso "API anterior" | `App.jsx:57` (`fallbackAnterior`) | `GET /api/live/status` | `main.py:286` | `feed.status()` | - |

Rotas que **existem mas nenhum botão da tela atual aciona** (ficam para engenharia/benchmark; `api.js` ainda expõe várias delas):

| Rota | Handler | Função | MongoDB |
|---|---|---|---|
| `POST /api/live/degrade` | `main.py:269` | `LiveFeed.degradar` (`services/live.py:192`) | só altera o gerador; sem query |
| `POST /api/live/clear` | `main.py:281` | `LiveFeed.clear` (`:215`) | `drop` de `payment_events_live` (`:234`) |
| `GET /api/live/health/{provedor_id}` | `main.py:312` | `providers.saude_ao_vivo` (`providers.py:148`) | `payment_events_live`: aggregate com `$setWindowFields` (`:231`); `provedores`: `find_one` |
| `GET /api/providers`, `/api/providers/{id}`, `/api/scenarios` | `main.py:128,132,140` | `registry.provedores/provedor/cenarios/contas_demo` | `provedores`, `degradation_scenarios`, `demo_accounts`: `find` |
| `GET /api/latency` | `main.py:149` | `latency.serie` (`latency.py:84`) | `payment_events`: aggregate com `$densify`/`$fill` (`:58`,`:60`) |
| `GET /api/providers/{id}/health` | `main.py:155` | `providers.saude` (`providers.py:100`) | `payment_events`: aggregate com `$setWindowFields` (`:46`) |
| `GET /api/ranking` | `main.py:160` | `providers.ranking` (`providers.py:271`) | `payment_events`: aggregate (`:301`) |
| `GET /api/velocity/{conta_id}` | `main.py:166` | `velocity.features` (`velocity.py:53`) | `payment_events`: aggregate (`:57`) |
| `POST /api/incidents`, `GET /api/incidents`, `POST /api/incidents/{id}/close` | `main.py:193,204,209` | `incidents.abrir` (`incidents.py:22`), `recentes` (`:111`), `encerrar` (`:87`) | transação com `provedores`/`incidents` (`:59-77`) |
| `POST /api/demo/reset` | `main.py:220` | `incidents.reset_demo` (`incidents.py:116`) + `feed.clear()` | `delete_many` em `incidents` e `incident_alerts`; `update_many` em `provedores` |
| `GET /api/alerts/stream` (SSE), `GET /api/alerts` | `main.py:321,329` | `AlertHub` (`services/alerts.py:30`) | change stream `incidents.watch` (`alerts.py:79`); `incident_alerts`: insert (`:110`) e find |
| `GET /health/live` | `main.py:122` | sonda simples | nenhuma |

## 3. Onde estão as queries MongoDB

Texto completo de cada pipeline: [queries.md](briefing/queries.md). Não há `$regex`, `$search` nem `$vectorSearch` nesta PoV.

| O que faz | arquivo::função (linha) | Operador / pipeline principal | Collection(s) |
|---|---|---|---|
| Criar a coleção time series ao vivo | `services/live.py::ensure_collection` (`:68`, create `:84`) | `create_collection(timeseries={timeField:ts, metaField:meta, bucketMaxSpanSeconds:300, bucketRoundingSeconds:300}, expireAfterSeconds)` (`_TIMESERIES` `:47`) | `payment_events_live` |
| Índices da coleção ao vivo | `ensure_collection` (`:94-96`) | `create_index` em `ts`, `meta.provedor+ts`, `meta.canal+ts` | `payment_events_live` |
| Gravar lote | `db/client.py::insert_idempotent` (`:68`, `insert_many` `:93`,`:99`) chamado de `services/live.py::LiveFeed._run` (`:406`) | `insert_many(ordered=False)` com conferência de `_id` na repetição (`:100`) | `payment_events_live` |
| Curva de throughput (o gráfico) | `db/live.py::overview` (`:112`, pipeline `:128`, aggregate `:132`) | `$match` + `$group` por `$dateTrunc` de 1 s + `$sort` + `$project` + `$limit` | `payment_events_live` |
| Configuração real da coleção (prova do time series) | `db/live.py::_collection_options` (`:20`, command `:29`) | `listCollections` | `payment_events_live` |
| Bucket físico do último documento | `db/live.py::_bucket_snapshot` (`:46`), `_find_bucket_raw` (`:99`), `_find_bucket_legacy` (`:107`) | `find` com `rawData: true` (servidor novo) ou leitura de `system.buckets.*` (servidor antigo) | `payment_events_live` |
| Comparação de armazenamento | `db/storage.py::_stats` (`:25`, `:28`) | `$collStats` com `storageStats` | `payment_events`, `payment_events_flat` |
| Percentis / lacuna | `db/latency.py::pipeline` (`:28`), `serie` (`:84`) | `$densify` + `$fill` | `payment_events` |
| Saúde do provedor (histórico) | `db/providers.py::pipeline` (`:25`), `saude` (`:100`) | `$setWindowFields` (z-score contra a própria linha de base) | `payment_events` |
| Saúde do provedor (ao vivo) | `db/providers.py::saude_ao_vivo` (`:148`, aggregate `:231`) | `$setWindowFields` | `payment_events_live`, `provedores` |
| Ranking | `db/providers.py::ranking` (`:271`) | aggregate | `payment_events` |
| Velocity de conta | `db/velocity.py::pipeline` (`:22`), `features` (`:53`) | aggregate por janelas | `payment_events` |
| Incidente (transação) | `db/incidents.py::abrir` (`:22`), `encerrar` (`:87`), `reset_demo` (`:116`) | transação com `update_one`/`insert_one` em sessão (`:59-77`) | `provedores`, `incidents` |
| Alertas por change stream | `services/alerts.py::AlertHub._run` (`:73`, watch `:79`) | `watch` com `updateLookup` | `incidents`, `incident_alerts` |
| Cadastro | `db/registry.py` (`:8-28`) | `find` ordenado | `provedores`, `degradation_scenarios`, `demo_accounts` |
| Âncora de tempo do histórico | `db/ranges.py::anchor` (`:32`, `:40`) | `find_one` | `dataset_info` |
| Carga do histórico | `data-generator/generate_events.py` (`insert_many` `:243`, `delete_many` por dia `:299`) | `insert_many(ordered=False)` | `payment_events` (e `payment_events_flat` na amostra) |
| Índices do histórico | `data-generator/common.py::ensure_indexes` (`:99`) | `create_index` | coleções do histórico |
| Cadastro e contas | `generate_registry.py` (`:122`), `generate_demo_accounts.py` (`:100`) | `drop` + inserção | `provedores`, `degradation_scenarios`, `demo_accounts` |
| Reset | `scripts/reset_demo.py` (`drop` `:86`) | chama os geradores na ordem | várias |
| Benchmarks | `queries/bench.py`, `bucket_experiment.py`, `cardinality_experiment.py`, `feature_probes.py` | reusam `backend/app/db/*` | coleções de experimento |

## 4. Compartilhado × só desta PoV

**`_shared`:** nenhum módulo Python/JS é importado (`rg` não acha import de `_shared`, `grove_client` nem `sync_env` no código). Só o visual: `frontend/src/pov-signature.css` é cópia da assinatura visual do portfólio (a própria linha 3 do arquivo diz que a fonte é `PoVs/_shared/ui/pov-signature.css`).

**Variáveis de ambiente** (`.env.example`, só nomes)
- Comum (vem do `.env.shared` via `_shared/sync_env.py`): `MONGODB_URI` (está na lista comum do `sync_env.py`, linha 33).
- Locais: `MONGODB_DB` (padrão `trilho_pagamentos`), `TS_EXPIRE_AFTER_SECONDS`, `TS_MAX_TIME_MS`, `TS_MAX_RANGE_DAYS`, `TS_MAX_POINTS`; detector `Z_SCORE_THRESHOLD`, `Z_MIN_WINDOWS`, `Z_BASELINE_WINDOWS`, `Z_BASELINE_LAG`, `MIN_DELTA_PP`, `MIN_DELTA_RATIO`, `MIN_P99_RATIO`, `MIN_EVENTS_PER_WINDOW`; `VELOCITY_WINDOWS`; gerador `DAYS`, `EVENTS_PER_SECOND`, `ACCOUNTS`, `SEED`; feed ao vivo `LIVE_TTL_SECONDS`, `LIVE_TICK_SECONDS`, `LIVE_MINUTES_PER_TICK`, `LIVE_TARGET_EPS`, `LIVE_ROLLING_SECONDS`, `LIVE_MIN_EVENTS`, `LIVE_CONFIDENCE_SIGMAS`; `STORAGE_CACHE_SECONDS`; `ARCHIVE_ENABLED`, `FEDERATED_URI`; `BACKEND_PORT`, `FRONTEND_PORT`, `LOG_LEVEL`; números do cliente `CUSTO_MINUTO_INDISPONIVEL`, `TICKET_MEDIO_REFERENCIA`, `CURRENCY`; `ALLOW_DEMO_DB_WRITE` (guarda de escrita). Frontend: `VITE_API` (`frontend/src/api.js`, padrão `http://127.0.0.1:8400`). Tudo lido em `backend/app/config.py`.
- Portas: backend 8400 (`start.sh`, `config.py`), frontend 5400 (`start.sh`, `vite.config.js`, CORS em `main.py:32`).

**Bancos/coleções:** banco próprio `trilho_pagamentos` (`MONGODB_DB`), exclusivo desta PoV: `payment_events` (histórico), `payment_events_flat` (amostra em coleção comum), `payment_events_live` (ao vivo, TTL curto), `provedores`, `degradation_scenarios`, `demo_accounts`, `dataset_info`, `incidents`, `incident_alerts`. Nada compartilhado com outras PoVs. Scripts que escrevem recusam banco sem `_test` sem `ALLOW_DEMO_DB_WRITE=1` (`data-generator/common.py::guard_write`, `:57`).

**Chamadas externas:** nenhuma além do Atlas (sem LLM: não há Grove, Voyage nem Langfuse). `FEDERATED_URI` e `ARCHIVE_ENABLED` só habilitam um caminho opcional de arquivo.

## 5. "Quero mexer em..." (atalhos)

- Ritmo de eventos do feed -> `LIVE_TARGET_EPS` no `.env` (padrão em `backend/app/config.py:73`); o corpo de `POST /api/live/start` aceita `eps` (`main.py:LiveStart`).
- Formato do evento gerado ao vivo (campos, canais, valores) -> `backend/app/services/live.py::LiveFeed._run` (`:299`, montagem do documento em `:392`).
- Configuração da coleção time series (bucket, TTL, meta) -> `_TIMESERIES` em `services/live.py:47` e `LIVE_TTL_SECONDS`.
- Pipeline da curva do gráfico -> `backend/app/db/live.py::overview` (`:128`).
- Texto do título, do botão e dos rótulos -> `frontend/src/App.jsx` (cabeçalho em torno de `:192-208`).
- Números do bloco "Resultado da bucketização" (benchmark estático) -> constante `BUCKETIZATION_BENCHMARK` em `frontend/src/evidence.js:27`; o número medido ao vivo vem de `bucketizacaoMedida` (`frontend/src/evidence.js:35`) e `storage.py`.
- Visual do gráfico -> `frontend/src/Chart.jsx` (tabela `MODOS`).
- Adicionar uma rota -> função em `backend/main.py`; a consulta vai num módulo novo em `backend/app/db/`.
- Limiares do detector de degradação -> `.env` (`Z_*`, `MIN_*`) e `backend/app/db/providers.py`.
- Mudar o volume do histórico -> `DAYS`/`EVENTS_PER_SECOND` e rodar `scripts/reset_demo.py` (README descreve os flags).
- Mudar porta -> `start.sh`, `frontend/vite.config.js`, `BACKEND_PORT`/`FRONTEND_PORT`.
- Mudar o cadastro de provedores e cenários plantados -> `data-generator/generate_registry.py`.

## 6. Testes

Comandos (do README): `python3 -m compileall -q backend tests scripts data-generator queries`; `(cd frontend && npm run build && node --test tests/*.test.mjs)`; `backend/venv/bin/python -m unittest tests/test_cors.py tests/test_client_concurrency.py`; `backend/venv/bin/python -m unittest tests/test_live_lifecycle.py -v`; `.venv/bin/python -m unittest discover -s tests/adversarial -p "test_*_adversarial.py" -v`.

| Arquivo | O que cobre |
|---|---|
| `tests/test_live_lifecycle.py` | Ciclo do feed: start, stop, clear, portão de escrita |
| `tests/test_client_concurrency.py` | Cliente Mongo único sob concorrência |
| `tests/test_cors.py` | Origens permitidas |
| `tests/adversarial/test_api_adversarial.py` | Entradas hostis nas rotas |
| `tests/adversarial/test_ingest_adversarial.py` | Falhas e duplicação na ingestão |
| `tests/test_resilience.py` | Roteiro de resiliência contra API de teste (script; recusa API que não seja `*_test`) |
| `tests/stress.py`, `tests/live_ingest_capacity.py` | Estresse e capacidade de ingestão (scripts, cluster real) |
| `frontend/tests/http-deadline.test.mjs` | Prazo e erro traduzido de `api.js` |

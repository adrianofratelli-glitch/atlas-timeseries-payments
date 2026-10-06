# Arquitetura — atlas-timeseries-payments

PoV de portfólio, dado 100% sintético. A tese: **o MongoDB Atlas recebe e agrega
uma série temporal de pagamentos enquanto ela nasce**, com percentil de latência,
detecção de degradação por desvio da própria linha de base do provedor e velocity
de conta rodando dentro do orçamento de uma autorização.

## Stack

| Camada | Tecnologia | Porta |
|---|---|---|
| Frontend | React + Vite, gráficos com `uPlot` (não uma lib React de chart — o canvas nunca é re-renderizado pelo React, só recebe dados novos via `setData`) | 5400 |
| Backend | FastAPI (Python), `pymongo` | 8400 |
| Banco | MongoDB Atlas, database `trilho_pagamentos` | — |
| Gerador de dados | scripts Python standalone (`data-generator/`) | — |

## Camadas e limite de dependência (regra dura do repo)

```
┌─────────────────────────┐   ┌──────────────────────────┐   ┌───────────────────────────┐
│ React + Vite (5400)     │──▶│ FastAPI (8400)           │──▶│ MongoDB Atlas             │
│ um botão Play           │◀──│ main.py: só HTTP         │◀──│ payment_events (time      │
│ uPlot, janela de 60 s   │   │ app/db/*: só consulta    │   │ series) + payment_events_ │
│ amostra BSON confirmada │   │ app/services/*: regra    │   │ live (time series + TTL)  │
└─────────────────────────┘   └──────────────────────────┘   └───────────────────────────┘
                                                                          ▲
                                                              ┌───────────┴───────────┐
                                                              │ data-generator/        │
                                                              │ pagamentos sintéticos  │
                                                              └────────────────────────┘
```

**Nenhuma rota em `backend/main.py` importa `pymongo`; nenhum módulo em
`backend/app/db/` importa `fastapi`.** `main.py` (arquivo único, 321 linhas) expõe
HTTP e traduz erro de domínio em status code; toda query mora em `backend/app/db/`.
Essa simetria é o que permite `queries/bench.py` reusar os pipelines de produção
para medir performance sem subir o framework web inteiro.

Módulos de `app/db/`:

| Módulo | Responsabilidade |
|---|---|
| `client.py` | conexão, `with_retry()` |
| `latency.py` | percentil de latência por rota + reconstrução de lacuna |
| `providers.py` | saúde do provedor (histórico e ao vivo) + ranking |
| `velocity.py` | velocity de conta em uma passada |
| `storage.py` | comparação time series vs. coleção normal via `$collStats` |
| `incidents.py` | abertura/encerramento de incidente (transação ACID) |
| `registry.py` | cadastro de provedores, cenários plantados, contas demo |
| `live.py` | evidência da coleção `payment_events_live` (config real + bucket físico) |
| `ranges.py` | resolve janela pedida em horas → `(start, end, unit, bin_size)` |

`app/services/`:

| Módulo | Responsabilidade |
|---|---|
| `live.py` | `LiveFeed` — thread que gera e grava o trilho ao vivo |
| `limits.py` | semáforos por classe de consulta (`interativo`/`analitico`/`storage`) |
| `alerts.py` | `AlertHub` — change stream sobre `incidents`, distribui para SSE |

## Componentes de produto

1. **Gerador sintético** (`data-generator/`) — popula `provedores`,
   `degradation_scenarios`, `demo_accounts`, `payment_events` (time series) e
   `payment_events_flat` (coleção normal, para a comparação de armazenamento). Roda
   uma vez, fora do runtime da demo.
2. **Backend FastAPI** — expõe os pipelines como HTTP, mede tempo de servidor
   (`_timed()` em `main.py`), limita concorrência por classe de query, controla o
   feed de ingestão ao vivo e abre/fecha incidentes em transação.
3. **Frontend React** — hoje é um "palco" de uma tela: um botão (**Iniciar
   ingestão**), o feed de lotes chegando, o gráfico de 60 s e a evidência técnica
   (config real da coleção, bucket físico do último documento, pipeline executado).
   Ver `ui-flows.md`.

## Fluxo de dados (modo "palco", o que roda hoje)

```
Play (frontend)
  → POST /api/live/start
  → services/live.py: LiveFeed._run() em thread própria
      gera PIX + cartão + TED no mesmo lote (Poisson por canal, distribuição por
      provedor com peso de participação, latência lognormal, recusa por taxa base)
      → insert_idempotent() em payment_events_live (time series, TTL 1h): _id fixado
        antes do envio; numa repetição após queda de rede, confere o que já chegou
        e reenvia só o resto (time series não tem _id único)
  → frontend faz poll de 1 s em GET /api/live/overview
      → db/live.py: agrega os últimos 60 s por segundo, exclui o segundo corrente
        (ainda sendo escrito), lê a config real via listCollections (cache 30 s) e
        o bucket físico via find(rawData: true) em payment_events_live (MongoDB
        8.2+ bloqueia system.buckets.*; servidores antigos caem nesse namespace)
  → uPlot.setData() — o canvas nunca é recriado a cada poll
```

## Decisões de design (por quê, não só o quê)

- **`meta` carrega a rota, nunca a conta.** `{canal, provedor, produto, uf}` — cerca
  de 2 900 combinações. `conta_id` é campo de **medição** com índice secundário.
  Colocar a conta no `metaField` multiplicaria as séries por milhões — medido em
  `docs/adr/0002-cardinalidade.md`.
- **Percentil, nunca média, para latência.** Um trilho de pagamento é julgado pela
  cauda; a média esconde exatamente o cliente que esperou quatro segundos.
- **Detecção é desvio da própria linha de base do provedor**, com
  `$setWindowFields` calculando média e desvio-padrão móveis sobre uma janela que
  termina em `-lag` (nunca inclui a janela julgada). Um limiar absoluto acerta o
  adquirente de crédito saudável com 23% de recusa e erra o PSP de PIX doente com
  3% — o controle negativo plantado no dataset prova isso.
- **Gap filling acontece na pipeline**, com `$densify` + `$fill` (LOCF), nunca em
  Python. Todo ponto reconstruído volta marcado (`reconstruido: true` + método) e o
  gráfico desenha tracejado — inventar dado sem dizer que inventou quebra a
  confiança de quem opera o sistema.
- **O servidor decide a granularidade** (`$dateTrunc` bin), o cliente só pede a
  janela em horas; a granularidade escolhida volta no payload e é exibida.
- **Toda agregação é limitada em tempo e em span**: `maxTimeMS` (`TS_MAX_TIME_MS`,
  15 s) e teto de dias (`TS_MAX_RANGE_DAYS`). Sem isso, uma janela grande sobre
  dezenas de milhões de eventos trava a demo ao vivo.
- **Abrir um incidente é atômico** — flag no provedor, gravação do incidente e
  evento que acorda o change stream acontecem na mesma transação ACID
  (`incidents.py:abrir`). Provedor marcado sem incidente correspondente é achado de
  auditoria.
- **O change stream observa `incidents`, não `payment_events`** — a coleção time
  series dispara por transação (dezenas por segundo); a coleção de incidentes
  dispara uma vez por degradação real.
- **Ingestão ao vivo nunca toca `payment_events`.** Escreve em
  `payment_events_live`, TTL de 1 h, carimbada com tempo **real** (o relógio
  simulado só escolhe a forma do tráfego, ancorado no dia útil às 10h — carimbar o
  passado faria a TTL apagar a série em menos de um minuto).
- **Concorrência é limitada por classe de consulta** (`app/services/limits.py`):
  12 slots para o caminho interativo, 3 para o analítico, 2 para `$collStats`.
  Quem não consegue vaga em 750 ms recebe `429` — um sistema sob saturação que
  recusa cedo é mais honesto que um que trava a demo inteira.
- **Retry só para falha transitória de rede** (`with_retry()`: `AutoReconnect`,
  `NetworkTimeout`, `ConnectionFailure`, no máximo 3 tentativas). Erro de lógica
  nunca é retentado — retentar esconderia bug.

## Variáveis de ambiente relevantes

| Variável | Default | Papel |
|---|---|---|
| `MONGODB_URI` | — | obrigatória |
| `MONGODB_DB` | `trilho_pagamentos` | database |
| `TS_MAX_TIME_MS` | `15000` | teto de toda agregação |
| `TS_MAX_RANGE_DAYS` | `90` (config.py) | teto da janela pedida |
| `TS_MAX_POINTS` | `4000` | pontos retornados por série |
| `Z_SCORE_THRESHOLD` | `3.0` | desvios da própria linha de base do provedor |
| `Z_MIN_WINDOWS` | `3` | janelas anômalas seguidas para abrir incidente |
| `Z_BASELINE_WINDOWS` / `Z_BASELINE_LAG` | `96` / `4` | tamanho e recuo da janela de base |
| `VELOCITY_WINDOWS` | `1,6,24` | janelas de velocity, em horas |
| `LIVE_TTL_SECONDS` | `3600` | retenção do dado ao vivo |
| `LIVE_TARGET_EPS` | `1500` | ritmo alvo do trilho completo |
| `RANKING_MAX_HOURS` | `6.0` | teto de janela do placar de provedores |

`.env.example` está versionado; `.env` não.

## Como subir

```bash
python3 -m venv .venv && .venv/bin/pip install -r data-generator/requirements.txt
python3 -m venv backend/venv && backend/venv/bin/pip install -r backend/requirements.txt
(cd frontend && npm install)

# reset único (recusa banco que não termina em _test sem ALLOW_DEMO_DB_WRITE=1)
.venv/bin/python scripts/reset_demo.py --db trilho_pagamentos_test --days 3 --eps 10
ALLOW_DEMO_DB_WRITE=1 .venv/bin/python scripts/reset_demo.py          # demo, 7 d × 75/s
ALLOW_DEMO_DB_WRITE=1 .venv/bin/python scripts/reset_demo.py --resume # retoma carga
./start.sh                         # 8400 + 5400
MONGODB_DB=trilho_pagamentos_test ./start.sh   # mesma app no banco de teste
POV_DEV=1 ./start.sh               # HMR + uvicorn --reload
```

O gerador é idempotente via `det_id(kind, *parts)` (`uuid5` sobre os atributos
chave) em tudo, exceto nos eventos: time series não tem `_id` controlável pelo
usuário, logo não tem upsert. Recarregar eventos exige `--drop`; uma carga
interrompida é retomada com `--resume` a partir do checkpoint diário em
`dataset_info` (`days_done`, `day_events`, janela planejada): dias confirmados são
pulados e o dia parcial é apagado e regravado.

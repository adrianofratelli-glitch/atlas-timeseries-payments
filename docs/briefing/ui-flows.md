# Interface e fluxos — atlas-timeseries-payments

A interface de palco hoje é **uma tela, uma ação, uma tese**: provar que o
MongoDB Atlas grava e agrega uma série temporal enquanto ela nasce. Provedores,
detecção, incidentes, velocity, ranking e comparação de armazenamento continuam
existindo no backend e na engenharia, mas não pertencem mais à experiência de
palco (arquivada — ver seção final).

## Componentes do frontend

| Arquivo | Papel |
|---|---|
| `frontend/src/App.jsx` (408 linhas) | tela única: estado, polling, layout completo |
| `frontend/src/Chart.jsx` (164 linhas) | wrapper de `uPlot`; modos declarativos por tipo de série |
| `frontend/src/QueryDetails.jsx` (15 linhas) | gaveta `<details>` com o pipeline executado |
| `frontend/src/api.js` | client HTTP fino para o backend |

## O layout

```text
┌─ MongoDB Time Series ─────────────────────────────── Atlas conectado ─┐
│ Veja a série temporal nascer.                    [Iniciar ingestão]   │
│                                                                        │
│ Gerador de eventos ── { } { } { } ── lote confirmado ──▶ bucket/rota  │
│                                                                        │
│ eventos nesta execução │ throughput │ confirmação do lote │ agregação │
│                                                                        │
│ Resultado da bucketização ($collStats deste cluster; antes, benchmark) │
│ N medições → B buckets │ X× menos dados │ Y× total com índices        │
│                                                                        │
│ Eventos persistidos por segundo ── curva cresce por 60 s              │
│ ▸ Ver query / chamada executada                                       │
├────────────────────────────────────────────────────┬──────────────────┤
│  (coluna principal, rola internamente se aberta)    │ evidence-rail   │
│                                                      │ - time series   │
│                                                      │   nativa        │
│                                                      │ - bucket físico │
│                                                      │ - documento     │
│                                                      │   confirmado    │
└──────────────────────────────────────────────────────┴──────────────────┘
```

Layout responsivo: acima de 900px, palco largo + rail estreito lado a lado.
Abaixo de 900px empilham e o documento rola verticalmente. Em 360px o trilho de
ingestão permanece horizontal dentro do próprio contêiner, as métricas viram grid
2×2 e o botão único ocupa a largura inteira.

## Fluxo único: Iniciar ingestão

Handler: `alternarIngestao()`, `frontend/src/App.jsx:149-169`.

1. Usuário clica **Iniciar ingestão** (único botão visível na tela).
2. Frontend chama `POST /api/live/start` (`api.liveStart()`), zera o `overview`
   local (a curva sempre recomeça vazia, sem apagar dado anterior — a retenção
   continua sendo só o TTL).
3. Backend (`live_start` em `main.py:244-246`) delega para
   `services/live.py: LiveFeed.start()`, que sobe uma thread `live-feed` gerando
   PIX + cartão + TED no mesmo lote.
4. Frontend entra em polling de 1s (`useEffect` em `App.jsx:137-147`) chamando
   `GET /api/live/overview` enquanto `live.state === 'rodando'`.
5. Cada resposta atualiza:
   - **Trilho de ingestão** (`ingestion-flow`, `App.jsx:220-252`): pacotes `{ }`
     animados fluindo do "gerador" até a "pilha de buckets por rota"; a animação
     só se move enquanto o feed está rodando, e o número do lote só é publicado
     depois que `insert_many` retorna.
   - **Métricas de prova** (`proof-metrics`, `App.jsx:254-277`): eventos nesta
     execução, throughput observado, tempo de confirmação do lote (`insert_many`),
     tempo de agregação no Atlas.
   - **Gráfico** (`chart-panel`, modo `throughput_live` no `Chart.jsx`): janela
     fixa de 60s desde o início da sessão — a linha cresce visivelmente em vez de
     esticar poucos pontos pelo painel inteiro.
   - **Bucket físico** (evidence rail, `collection-proof`): namespace, `timeField`,
     `metaField`, retenção, e o cabeçalho do bucket que contém o último documento
     (`control.min.ts`/`control.max.ts`/medições/versão de compressão).
   - **Documento confirmado** (`document-proof`): JSON bruto do último documento do
     lote, sem `_id`.
6. Usuário clica **Parar ingestão** → `POST /api/live/stop`, o polling para.

### Gaveta de transparência técnica

`QueryDetails.jsx` renderiza, fechada por padrão, o pipeline de agregação
exatamente como foi executado (`JSON.stringify(pipeline, null, 2)`), junto com
namespace e tempo de resposta medido no servidor. É a mesma pipeline #8 descrita
em `queries.md` (`db/live.py: overview()`), devolvida verbatim — o arquiteto do
cliente lê exatamente o que rodou, não uma versão editada para a demo.

### Bloco de bucketização (benchmark, não ao vivo)

`bucketization-result` (`App.jsx:279-294`) é deliberadamente **não** telemetria em
tempo real — é rotulado como "benchmark medido · mesmo schema · não é esta
execução ao vivo" e reproduz um resultado histórico versionado
(`queries/bench-results.json` e `queries/benchmarks.md`): 44.733.964 medições,
2.613.915 buckets, 17,1 medições/bucket, 2,26× menos dados por evento e 3,73×
menos armazenamento total por evento incluindo índices.

### Integridade da evidência

Nota fixa no rodapé do rail (`integrity-note`, `App.jsx:392-396`): "o movimento
representa apenas lotes confirmados... são números desta execução, não um
benchmark de capacidade." Nenhum número na tela é hard-coded como resultado de
sucesso — bucket e config de coleção vêm de `listCollections` e de
`find(rawData: true)` em `payment_events_live` ao vivo (ver `queries.md`). A faixa
"Resultado da bucketização" usa `$collStats` do cluster conectado (`GET /api/storage`)
assim que a medição chega; até lá mostra o benchmark histórico, rotulado como tal.

### Fallback de API anterior

`fallbackAnterior()` (`App.jsx:76-99`) detecta quando o backend responde 404 em
`/api/live/overview` (versão de API anterior à unificação de canais) e mostra um
aviso (`legacy-note`) pedindo reinício pelo portal — a interface nunca finge que
uma resposta antiga é dado real.

## `Chart.jsx` — modos declarativos

O gráfico usa `uPlot` diretamente (não uma lib de chart React) porque a série
pode chegar a milhares de pontos e é redesenhada a cada mudança de janela; React
nunca recria o canvas, só entrega dados novos via `setData` (comentário em
`Chart.jsx:5-7`).

Modos definidos em `MODOS` (`Chart.jsx:26-64`), cada um com sua tabela de séries —
usados pelas telas de engenharia arquivadas, exceto `throughput_live` que é o
modo ativo no palco atual:

| Modo | Uso |
|---|---|
| `latencia` | p50/p95/p99 + série tracejada de pontos reconstruídos por `$fill` |
| `saude` | taxa de recusa vs. linha de base histórica |
| `saude_live` | throughput + recusa móvel vs. base cadastrada (escala dupla) |
| `throughput_live` | **modo ativo no palco**: eventos persistidos por segundo, janela de 60s |

## Screenshots (`docs/screenshots/`)

Capturados em sessão 1600×1000 contra o cluster real de demo, com o feed
sintético rodando. Nenhum dado de cliente, hostname de cluster, connection string
ou segredo.

**Interface de palco atual:**

| Arquivo | O que mostra |
|---|---|
| `08-prova-ao-vivo.png` | palco completo: ingestão confirmada, agregação, bucket físico e resultado de bucketização |
| `09-bucket-fisico.png` | cabeçalho do bucket físico do último documento confirmado |
| `10-pipeline-executado.png` | pipeline de agregação completo executado pelo gráfico |

**Arquivo de engenharia (não é a UI atual do cliente):** `01` a `07` documentam a
interface mais ampla usada antes de o palco ser reduzido a um Play e uma tese —
telas de provedores, incidentes, velocity, ranking, comparação de armazenamento.
As APIs, benchmarks e ADRs por trás dessas telas continuam no repositório e
podem ser reativados; só não fazem parte do roteiro de apresentação padrão.

| Arquivo | Conteúdo arquivado |
|---|---|
| `01-armazenamento.png` | comparação de armazenamento time series vs. normal |
| `02-latencia-percentis.png` | painel de p50/p95/p99 por rota |
| `03-lacuna-densify-fill.png` | gap reconstruído com `$densify`/`$fill` |
| `04-degradacao-provedor.png` | detecção de degradação (z-score) |
| `05-controle-negativo.png` | controle negativo (ADQ-006) não abrindo incidente |
| `06-velocity-conta.png` | painel de velocity de conta |
| `07-ingestao-ao-vivo.png` | versão anterior do palco de ingestão |

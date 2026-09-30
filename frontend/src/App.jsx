import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import Banner from '@leafygreen-ui/banner'
import Button from '@leafygreen-ui/button'
import { api } from './api.js'
import Chart from './Chart.jsx'
import QueryDetails from './QueryDetails.jsx'

function numero(valor, casas = 0) {
  if (valor == null || Number.isNaN(valor)) return '—'
  return valor.toLocaleString('pt-BR', {
    minimumFractionDigits: casas,
    maximumFractionDigits: casas,
  })
}

function ms(valor) {
  return valor == null ? '—' : `${numero(valor, valor < 10 ? 1 : 0)} ms`
}

function hora(valor) {
  if (!valor) return '—'
  return new Date(valor).toLocaleTimeString('pt-BR', {
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
  })
}

function documentoVisivel(documento) {
  if (!documento) return null
  return {
    ts: documento.ts,
    meta: documento.meta,
    valor: documento.valor,
    latencia_ms: documento.latencia_ms,
    aprovado: documento.aprovado,
  }
}

const PACOTES = Array.from({ length: 11 }, (_, index) => index)

// Evidência reproduzível da carga histórica versionada em queries/. Não representa
// a execução curta da tela; os números e o método estão em queries/benchmarks.md.
const BUCKETIZATION_BENCHMARK = Object.freeze({
  events: 44_733_964,
  buckets: 2_613_915,
  eventsPerBucket: 17.1,
  dataReduction: 2.26,
  totalReduction: 3.73,
})

export default function App() {
  const [health, setHealth] = useState(null)
  const [live, setLive] = useState(null)
  const [overview, setOverview] = useState(null)
  const [ocupado, setOcupado] = useState(false)
  const [erro, setErro] = useState(null)
  const [backendAnterior, setBackendAnterior] = useState(false)
  const backendAnteriorRef = useRef(false)

  const aoVivo = live?.state === 'rodando'
  const pontos = overview?.points ?? []
  const ultimoPonto = pontos.at(-1)
  const colecao = overview?.collection ?? {}
  const bucket = overview?.bucket ?? null
  const documento = documentoVisivel(live?.last_document)
  const metaBucket = bucket?.meta ?? documento?.meta
  const rotaBucket = metaBucket
    ? [metaBucket.canal, metaBucket.provedor, metaBucket.produto, metaBucket.uf]
        .filter(Boolean).join(' / ')
    : null
  const bucketMaximoMinutos = colecao.bucket_max_span_seconds
    ? colecao.bucket_max_span_seconds / 60
    : null
  const [databaseName, collectionName] = (overview?.namespace
    ?? 'trilho_pagamentos.payment_events_live').split('.')

  const fallbackAnterior = useCallback(async (statusConhecido = null) => {
    const status = statusConhecido ?? await api.liveStatus()
    setLive(status)
    backendAnteriorRef.current = true
    setBackendAnterior(true)
    setOverview((anterior) => {
      const ponto = status.state === 'rodando' && status.last_tick_written != null
        ? { ts: new Date().toISOString(), eventos: status.last_tick_written }
        : null
      const anteriores = anterior?.points ?? []
      return {
        namespace: 'trilho_pagamentos.payment_events_live',
        points: ponto ? [...anteriores, ponto].slice(-60) : anteriores,
        collection: {
          exists: null,
          timeseries: null,
          time_field: null,
          meta_field: null,
          expire_after_seconds: status.ttl_seconds,
        },
        feed: status,
      }
    })
  }, [])

  const atualizar = useCallback(async () => {
    if (backendAnteriorRef.current) {
      await fallbackAnterior()
      return
    }
    try {
      const resultado = await api.liveOverview()
      setOverview(resultado)
      setLive(resultado.feed)
      backendAnteriorRef.current = false
      setBackendAnterior(false)
      setErro(null)
    } catch (e) {
      if (e.status === 404) {
        await fallbackAnterior()
        return
      }
      setErro(e.message)
    }
  }, [fallbackAnterior])

  useEffect(() => {
    let ativo = true
    Promise.all([api.health(), api.liveStatus()])
      .then(([h, status]) => {
        if (!ativo) return
        setHealth(h)
        setLive(status)
        if (status.scope === 'trilho_completo') atualizar()
        else fallbackAnterior(status)
      })
      .catch((e) => ativo && setErro(e.message))

    return () => { ativo = false }
  }, [atualizar, fallbackAnterior])

  useEffect(() => {
    if (!aoVivo) return undefined
    let cancelado = false
    let timer
    const proximo = async () => {
      await atualizar()
      if (!cancelado) timer = setTimeout(proximo, 1000)
    }
    timer = setTimeout(proximo, 1000)
    return () => { cancelado = true; clearTimeout(timer) }
  }, [aoVivo, atualizar])

  async function alternarIngestao() {
    setOcupado(true)
    try {
      if (aoVivo) {
        setLive(await api.liveStop())
      } else {
        // O backend delimita a curva por `started_at`: uma nova prova começa vazia
        // sem apagar os eventos anteriores, que seguem a retenção por TTL.
        setOverview(null)
        setLive(await api.liveStart())
        await atualizar()
      }
      setErro(null)
    } catch (e) {
      setErro(e.status === 422
        ? `API e interface estão em versões diferentes (${e.message}). Reinicie a PoV pelo portal.`
        : e.message)
    } finally {
      setOcupado(false)
    }
  }

  const prova = useMemo(() => ({
    eventos: live?.written,
    ritmo: live?.observed_eps ?? live?.eps,
    escrita: live?.last_tick_duration_ms,
    consulta: overview?.elapsed_ms,
  }), [live, overview])

  return (
    <div className="app" data-pov-shell>
      <a className="pov-skip-link" href="#conteudo-principal">Pular para o conteúdo</a>

      <header className="topbar">
        <div className="brand">
          <span className="leaf" aria-hidden="true" />
          <strong>MongoDB Time Series</strong>
          <span>prova de ingestão e consulta em tempo real</span>
        </div>
        <div className="connection-state" data-ok={health?.status === 'ok'}>
          <span aria-hidden="true" />
          {health?.status === 'ok' ? 'Atlas conectado' : erro ? 'Atlas indisponível' : 'Conectando ao Atlas'}
        </div>
      </header>

      {erro && (
        <Banner variant="danger" className="erro">
          {erro} — verifique a API em {api.base}
        </Banner>
      )}

      <main className="enterprise-shell" id="conteudo-principal" tabIndex={-1}>
        <section className="primary-proof" aria-live="polite">
          <header className="proof-heading">
            <div>
              <p className={`live-kicker${aoVivo ? ' running' : ''}`}>
                <span aria-hidden="true" />
                {aoVivo ? 'Gravando no Atlas agora' : 'Coleção pronta para receber eventos'}
              </p>
              <h1>Veja a série temporal nascer.</h1>
              <p className="proof-copy">
                Um único play inicia eventos reais, lotes confirmados e agregações de 1 segundo
                sobre a coleção time series.
              </p>
            </div>
            <Button variant={aoVivo ? 'default' : 'primary'} disabled={ocupado}
                    onClick={alternarIngestao} className="play-action">
              {ocupado ? 'Conectando…' : aoVivo ? 'Parar ingestão' : 'Iniciar ingestão'}
            </Button>
          </header>

          <div className={`ingestion-flow${aoVivo ? ' running' : ''}`}>
            <div className="flow-node source-node">
              <span>Gerador de eventos</span>
              <strong>{aoVivo ? `${numero(prova.ritmo)} /s` : 'em espera'}</strong>
            </div>

            <div className="event-lane" aria-hidden="true">
              <div className="lane-line" />
              {PACOTES.map((index) => (
                <i key={index} style={{ '--packet': index, '--lane': index % 3 }}><b>{'{ }'}</b></i>
              ))}
              <div className="batch-readout">
                <strong>{numero(live?.last_tick_written)}</strong>
                <span>documentos confirmados no último lote</span>
              </div>
            </div>

            <div className="flow-node collection-node">
              <div className="bucket-stack" data-active={Boolean(bucket)} aria-hidden="true">
                <i /><i />
                <div key={bucket?.max_ts ?? 'empty'} className="bucket-face bucket-update">
                  <strong>{numero(bucket?.measurements)}</strong>
                  <small>medições</small>
                </div>
              </div>
              <div className="bucket-node-copy">
                <span>Bucket físico desta rota</span>
                <strong>{rotaBucket ?? 'aguardando evento'}</strong>
                <small>mesmo meta · janela de até {bucketMaximoMinutos
                  ? `${numero(bucketMaximoMinutos)} min` : '5 min'}</small>
              </div>
            </div>
          </div>

          <div className="proof-metrics" aria-label="Resultados da prova">
            <article>
              <span>Eventos nesta execução</span>
              <strong key={`eventos-${prova.eventos}`} className="value-bump">
                {numero(prova.eventos)}
              </strong>
            </article>
            <article>
              <span>Throughput observado</span>
              <strong key={`ritmo-${prova.ritmo}`} className="value-bump">
                {numero(prova.ritmo)} <small>eventos/s</small>
              </strong>
            </article>
            <article>
              <span>Confirmação do lote</span>
              <strong>{ms(prova.escrita)}</strong>
              <small>{numero(live?.last_tick_written)} documentos em <code>insert_many</code></small>
            </article>
            <article>
              <span>Agregação no Atlas</span>
              <strong>{ms(prova.consulta)}</strong>
              <small>janela de 1 segundo</small>
            </article>
          </div>

          <section className="bucketization-result" aria-label="Resultado medido da bucketização">
            <header>
              <strong>Resultado da bucketização</strong>
              <span>benchmark medido · mesmo schema · não é esta execução ao vivo</span>
            </header>
            <div className="bucketization-conversion">
              <span><strong>{numero(BUCKETIZATION_BENCHMARK.events)}</strong> medições</span>
              <i aria-hidden="true">→</i>
              <span><strong>{numero(BUCKETIZATION_BENCHMARK.buckets)}</strong> buckets</span>
              <small>{numero(BUCKETIZATION_BENCHMARK.eventsPerBucket, 1)} medições/bucket</small>
            </div>
            <div className="bucketization-gain">
              <span><strong>{numero(BUCKETIZATION_BENCHMARK.dataReduction, 2)}×</strong> menos dados</span>
              <span><strong>{numero(BUCKETIZATION_BENCHMARK.totalReduction, 2)}×</strong> menos com índices</span>
            </div>
          </section>

          <section className="chart-panel">
            <header>
              <div>
                <h2>Eventos persistidos por segundo</h2>
                <p>{aoVivo ? 'A curva avança somente após o lote ser aceito pelo cluster.'
                  : 'Pressione iniciar para acompanhar os lotes chegando.'}</p>
              </div>
              <span className="chart-now">
                {ultimoPonto ? `${numero(ultimoPonto.eventos)} eventos no último segundo` : 'janela móvel de 60 s'}
              </span>
            </header>
            {pontos.length
              ? <Chart mode="throughput_live" points={pontos} minHeight={260} />
              : <div className="chart-empty" aria-hidden="true">
                  <span /><span /><span /><span /><span /><span /><span />
                </div>}
          </section>

          <QueryDetails pipeline={overview?.pipeline}
                        namespace={overview?.namespace ?? 'trilho_pagamentos.payment_events_live'}
                        elapsedMs={overview?.elapsed_ms} />
        </section>

        <aside className="evidence-rail">
          <section className="collection-proof">
            <header>
              <span className="proof-check" data-warning={backendAnterior} aria-hidden="true">
                {backendAnterior ? '!' : '✓'}
              </span>
              <div>
                <h2>{backendAnterior ? 'API anterior detectada' : 'Time series nativa'}</h2>
                <p>{backendAnterior
                  ? 'Reinicie para verificar a coleção'
                  : colecao.timeseries ? 'Configuração lida do Atlas' : 'Criada automaticamente no play'}</p>
              </div>
            </header>
            <dl>
              <div><dt>namespace</dt><dd className="namespace-value">
                <span>{databaseName}</span><span>{collectionName}</span>
              </dd></div>
              <div><dt>timeField</dt><dd>{colecao.time_field ?? '—'}</dd></div>
              <div><dt>metaField</dt><dd>{colecao.meta_field ?? '—'}</dd></div>
              <div><dt>retenção</dt><dd>{colecao.expire_after_seconds
                ? `${numero(colecao.expire_after_seconds / 60)} minutos` : '60 minutos'}</dd></div>
            </dl>

            <div className="bucket-snapshot" data-ready={Boolean(bucket)}>
              <header>
                <div>
                  <strong>Bucket observado agora</strong>
                  <span>cabeçalho físico do último evento</span>
                </div>
                <b>{bucket?.compressed ? `v${bucket.control_version} comprimido` : 'aguardando'}</b>
              </header>

              <div className="bucket-route" title={rotaBucket ?? undefined}>
                {metaBucket ? Object.entries(metaBucket).map(([chave, valor]) => (
                  <span key={chave}><small>{chave}</small>{valor}</span>
                )) : <span className="bucket-route-empty">o primeiro evento revelará sua rota</span>}
              </div>

              <div className="bucket-window" aria-label="Janela temporal do bucket observado">
                <div><small>control.min.ts</small><strong>{hora(bucket?.min_ts)}</strong></div>
                <span className="bucket-window-line"><i /></span>
                <div><small>control.max.ts</small><strong>{hora(bucket?.max_ts)}</strong></div>
              </div>

              <footer>
                <span><strong>{numero(bucket?.measurements)}</strong> medições neste bucket</span>
                <span>limite temporal ≤ {bucketMaximoMinutos
                  ? `${numero(bucketMaximoMinutos)} min` : '5 min'}</span>
              </footer>
              <p>Mesmo <code>meta</code> e mesma janela compartilham o bucket. Outras rotas
                são distribuídas automaticamente em outros buckets.</p>
            </div>
          </section>

          <section className="document-proof">
            <header>
              <div>
                <h2>Documento confirmado</h2>
                <p>{documento ? 'Amostra do último lote aceito' : 'Aguardando o primeiro lote'}</p>
              </div>
              {aoVivo && <span className="document-live">ao vivo</span>}
            </header>
            {documento ? (
              <pre key={documento.ts} className="document-arrival">
                {JSON.stringify(documento, null, 2)}
              </pre>
            ) : (
              <div className="document-empty">
                <code>{'{ "ts": …, "meta": … }'}</code>
              </div>
            )}
          </section>

          <p className="integrity-note">
            O movimento representa apenas lotes confirmados. Contadores, curva, documento e
            tempo da agregação vêm da API conectada ao Atlas. São números desta execução,
            não um benchmark de capacidade.
          </p>

          {backendAnterior && (
            <p className="legacy-note">
              API anterior detectada. Reinicie a PoV pelo portal para carregar a agregação
              unificada e a configuração lida do cluster.
            </p>
          )}
        </aside>
      </main>
    </div>
  )
}

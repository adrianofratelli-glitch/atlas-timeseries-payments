// Regras puras da tela (sem React): o que é medido, o que é benchmark e o que é falha.
// Ficam fora do App.jsx para serem testadas com `node --test` sem transpilar JSX.

export function numero(valor, casas = 0) {
  if (valor == null || Number.isNaN(valor)) return '—'
  return valor.toLocaleString('pt-BR', {
    minimumFractionDigits: casas,
    maximumFractionDigits: casas,
  })
}

// Só os campos que a tela mostra: `_id`, `erro` e `conta_id` ficam fora do card.
export function documentoVisivel(documento) {
  if (!documento) return null
  return {
    ts: documento.ts,
    meta: documento.meta,
    valor: documento.valor,
    latencia_ms: documento.latencia_ms,
    aprovado: documento.aprovado,
  }
}

// Evidência reproduzível da carga histórica versionada em queries/. Só aparece
// enquanto a medição do cluster conectado ($collStats via /api/storage) não chega,
// e é rotulada como benchmark; os números e o método estão em queries/benchmarks.md.
export const BUCKETIZATION_BENCHMARK = Object.freeze({
  events: 44_733_964,
  buckets: 2_613_915,
  eventsPerBucket: 17.1,
  dataReduction: 2.26,
  totalReduction: 3.73,
})

export function bucketizacaoMedida(storage, agora = Date.now()) {
  const ts = storage?.timeseries
  if (!storage?.available || !ts?.documents || !ts?.buckets) return null
  return {
    events: ts.documents,
    buckets: ts.buckets,
    eventsPerBucket: ts.documents / ts.buckets,
    dataReduction: storage.storage_ratio,
    totalReduction: storage.total_ratio,
    measuredAt: new Date(agora - (storage.measured_seconds_ago ?? 0) * 1000),
  }
}

// O escritor ao vivo distingue `parado` de `erro`; a tela precisa dizer qual dos dois.
// Sem isto, uma falha no meio da ingestão só fazia o botão voltar a "Iniciar".
export function falhaDaIngestao(live) {
  if (live?.state !== 'erro') return null
  return live.last_error || 'a ingestão ao vivo parou com erro'
}

// Uma linha para a gaveta da query, a partir de GET /api/live/plan. Diz o motor e,
// quando há block processing (8.0+), quais estágios de bloco o servidor planejou.
export function resumoDoPlano(plano) {
  if (!plano) return null
  const versao = plano.server_version ? `MongoDB ${plano.server_version}` : 'servidor conectado'
  if (plano.block_processing) {
    return { versao, texto: `SBE com block processing · ${plano.block_stages.join(', ')}` }
  }
  return {
    versao,
    texto: plano.engine === 'sbe' ? 'SBE sem block processing' : 'motor clássico, sem block processing',
  }
}

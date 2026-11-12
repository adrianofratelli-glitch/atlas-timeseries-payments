import test from 'node:test'
import assert from 'node:assert/strict'
import {
  BUCKETIZATION_BENCHMARK, bucketizacaoMedida, documentoVisivel, falhaDaIngestao, numero,
} from '../src/evidence.js'

test('numero formats pt-BR and never prints NaN', () => {
  assert.equal(numero(2281.4), '2.281')
  assert.equal(numero(2.26, 2), '2,26')
  assert.equal(numero(null), '—')
  assert.equal(numero(Number.NaN), '—')
})

test('documentoVisivel exposes only the fields the card claims', () => {
  const doc = documentoVisivel({
    ts: '2026-11-12T10:00:00Z', meta: { canal: 'pix' }, valor: 10, latencia_ms: 80,
    aprovado: true, erro: null, conta_id: 'C000000001', _id: 'x',
  })
  assert.deepEqual(Object.keys(doc).sort(), ['aprovado', 'latencia_ms', 'meta', 'ts', 'valor'])
  assert.equal(documentoVisivel(null), null)
})

test('bucketizacaoMedida uses the connected cluster only when $collStats answered', () => {
  assert.equal(bucketizacaoMedida(null), null)
  assert.equal(bucketizacaoMedida({ available: false, timeseries: { documents: 1, buckets: 1 } }), null)
  assert.equal(bucketizacaoMedida({ available: true, timeseries: { documents: 10, buckets: 0 } }), null)
  const agora = Date.parse('2026-11-12T12:00:00Z')
  const medida = bucketizacaoMedida({
    available: true, storage_ratio: 2.08, total_ratio: 2.29, measured_seconds_ago: 60,
    timeseries: { documents: 2_400_000, buckets: 150_000 },
  }, agora)
  assert.equal(medida.eventsPerBucket, 16)
  assert.equal(medida.dataReduction, 2.08)
  assert.equal(medida.measuredAt.toISOString(), '2026-11-12T11:59:00.000Z')
})

test('historical benchmark stays frozen and internally consistent', () => {
  assert.ok(Object.isFrozen(BUCKETIZATION_BENCHMARK))
  const ratio = BUCKETIZATION_BENCHMARK.events / BUCKETIZATION_BENCHMARK.buckets
  assert.equal(Math.round(ratio * 10) / 10, BUCKETIZATION_BENCHMARK.eventsPerBucket)
})

test('falhaDaIngestao separates a stopped feed from a failed one', () => {
  assert.equal(falhaDaIngestao({ state: 'parado', last_error: null }), null)
  assert.equal(falhaDaIngestao({ state: 'rodando' }), null)
  assert.equal(falhaDaIngestao(null), null)
  assert.match(falhaDaIngestao({ state: 'erro', last_error: 'trilho sem provedores' }), /provedores/)
  assert.ok(falhaDaIngestao({ state: 'erro', last_error: null }))
})

test('resumoDoPlano states block processing only when the server planned it', async () => {
  const { resumoDoPlano } = await import('../src/evidence.js')
  assert.equal(resumoDoPlano(null), null)
  const comBloco = resumoDoPlano({ engine: 'sbe', block_processing: true, server_version: '9.0.7',
    block_stages: ['block_group', 'ts_bucket_to_cellblock'] })
  assert.match(comBloco.texto, /block processing · block_group, ts_bucket_to_cellblock/)
  assert.equal(comBloco.versao, 'MongoDB 9.0.7')
  assert.match(resumoDoPlano({ engine: 'classic', block_processing: false, block_stages: [] }).texto, /clássico/)
  assert.match(resumoDoPlano({ engine: 'sbe', block_processing: false, block_stages: [] }).texto, /SBE sem/)
})

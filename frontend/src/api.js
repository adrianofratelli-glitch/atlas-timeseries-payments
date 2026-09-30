// O prazo cobre headers e corpo; nenhuma escrita é reenviada automaticamente.
async function boundedRequest(work, timeoutMs = 30000) {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)
  try { return await work(controller.signal) }
  finally { clearTimeout(timer) }
}

const BASE = import.meta.env.VITE_API ?? 'http://127.0.0.1:8400'

async function get(path, params) {
  return boundedRequest(async (signal) => {
    const url = new URL(BASE + path)
    Object.entries(params ?? {}).forEach(([k, v]) => {
      if (v !== undefined && v !== null && v !== '') url.searchParams.set(k, v)
    })
    const res = await fetch(url, { signal })
    const body = await res.json().catch(() => {
      if (res.ok) throw new Error('Resposta incompleta ou inválida do backend. Tente novamente.')
      return {}
    })
    if (!res.ok) throw Object.assign(new Error(body?.detail?.reason ?? res.statusText), { status: res.status, body })
    return body
  }, 30000)
}

async function post(path, payload) {
  return boundedRequest(async (signal) => {
    const res = await fetch(BASE + path, {
      method: 'POST',
      signal,
      headers: { 'content-type': 'application/json' },
      body: payload ? JSON.stringify(payload) : undefined,
    })
    const body = await res.json().catch(() => {
      if (res.ok) throw new Error('Resposta incompleta ou inválida do backend. Tente novamente.')
      return {}
    })
    if (!res.ok) {
      const validacao = Array.isArray(body?.detail)
        ? body.detail.map((item) => `${item.loc?.at(-1) ?? 'campo'}: ${item.msg}`).join('; ')
        : null
      throw Object.assign(
        new Error(body?.detail?.reason ?? validacao ?? res.statusText),
        { status: res.status, body },
      )
    }
    return body
  }, 60000)
}

export const api = {
  base: BASE,
  health: () => get('/health'),
  providers: (canal) => get('/api/providers', { canal, limit: 200 }),
  scenarios: () => get('/api/scenarios'),
  latency: (canal, provedor, hours, fill) =>
    get('/api/latency', { canal, provedor, hours, fill }),
  providerHealth: (id, hours) => get(`/api/providers/${id}/health`, { hours }),
  ranking: (hours = 1) => get('/api/ranking', { hours }),
  velocity: (contaId) => get(`/api/velocity/${contaId}`),
  storage: () => get('/api/storage'),
  incidents: () => get('/api/incidents'),
  openIncident: (payload) => post('/api/incidents', payload),
  reset: () => post('/api/demo/reset'),
  // O backend é a fonte do ritmo validado para este cluster. A tela apenas dispara
  // o processo único; o throughput exibido continua sendo o observado após o ack.
  liveStart: () => post('/api/live/start', {}),
  liveDegrade: (provedorId) => post('/api/live/degrade', { provedor_id: provedorId }),
  liveStop: () => post('/api/live/stop'),
  liveStatus: () => get('/api/live/status'),
  liveOverview: () => get('/api/live/overview'),
  liveHealth: (id) => get(`/api/live/health/${id}`),
  stream: () => new EventSource(`${BASE}/api/alerts/stream`),
}

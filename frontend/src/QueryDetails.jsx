import { useState } from 'react'
import { resumoDoPlano } from './evidence.js'

// Gaveta de transparência técnica: o pipeline que realmente rodou, fechado por
// padrão, imediatamente abaixo do resultado. O plano de execução (explain) só é
// pedido quando a gaveta abre: ele não muda a cada segundo (ADR 0003).
export default function QueryDetails({ pipeline, namespace, elapsedMs, loadPlan }) {
  const [plano, setPlano] = useState(null)
  const [erroPlano, setErroPlano] = useState(null)
  if (!pipeline) return null

  function aoAbrir(evento) {
    if (!evento.currentTarget.open || !loadPlan || plano) return
    setErroPlano(null)
    loadPlan().then(setPlano).catch((e) => setErroPlano(e.message))
  }

  const resumo = resumoDoPlano(plano)
  return (
    <details className="query-details" onToggle={aoAbrir}>
      <summary>Ver query / chamada executada</summary>
      <div className="query-meta">
        <span>namespace: <code>{namespace}</code></span>
        {elapsedMs != null && <span>tempo de resposta: <code>{elapsedMs} ms</code></span>}
      </div>
      <div className="query-meta" aria-live="polite">
        {resumo
          ? <span>plano de execução ({resumo.versao}): <code>{resumo.texto}</code></span>
          : <span>{erroPlano ? `plano indisponível: ${erroPlano}` : 'lendo o plano de execução (explain)…'}</span>}
      </div>
      <pre>{JSON.stringify(pipeline, null, 2)}</pre>
    </details>
  )
}

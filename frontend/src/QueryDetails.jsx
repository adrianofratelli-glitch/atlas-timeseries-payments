// Gaveta de transparência técnica: o pipeline que realmente rodou, fechado por
// padrão, imediatamente abaixo do resultado.
export default function QueryDetails({ pipeline, namespace, elapsedMs }) {
  if (!pipeline) return null
  return (
    <details className="query-details">
      <summary>Ver query / chamada executada</summary>
      <div className="query-meta">
        <span>namespace: <code>{namespace}</code></span>
        {elapsedMs != null && <span>tempo de resposta: <code>{elapsedMs} ms</code></span>}
      </div>
      <pre>{JSON.stringify(pipeline, null, 2)}</pre>
    </details>
  )
}

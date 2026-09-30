import { useEffect, useMemo, useRef } from 'react'
import uPlot from 'uplot'
import 'uplot/dist/uPlot.min.css'

// uPlot e não uma biblioteca React de gráfico: a série chega a milhares de pontos e
// redesenha a cada mudança de janela. React nunca re-renderiza o canvas — só entrega
// dados novos ao ref.

const CORES = {
  p50: '#00ed64',
  p95: '#0498ec',
  p99: '#ffc010',
  recusa: '#ff6960',
  base: '#889397',
  reconstruido: '#ffc010',
}

function fmt(ts) {
  return new Date(ts).toLocaleString('pt-BR', {
    day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit',
  })
}

// Cada modo declara suas séries e como extrair os dados dos pontos. Manter isso numa
// tabela evita um `if` por série espalhado pelo componente.
const MODOS = {
  latencia: {
    series: [
      { key: 'p50', label: 'p50 (ms)', stroke: CORES.p50 },
      { key: 'p95', label: 'p95 (ms)', stroke: CORES.p95 },
      { key: 'p99', label: 'p99 (ms)', stroke: CORES.p99, width: 2.5 },
      // Invariante 6: ponto reconstruído nunca parece medido. Série própria,
      // tracejada, sobre o p99 — sem ela o platô do $fill passa por medição.
      { key: 'p99', label: 'reconstruído', stroke: CORES.reconstruido, width: 3,
        dash: [4, 4], apenasReconstruido: true },
    ],
  },
  saude: {
    series: [
      { key: 'taxa_recusa', label: 'recusa (%)', stroke: CORES.recusa, width: 2.5 },
      { key: 'recusa_base', label: 'linha de base (%)', stroke: CORES.base, dash: [5, 4] },
    ],
  },
  saude_live: {
    // O throughput fica ao fundo e em outra escala: ele torna cada lote visível
    // sem distorcer a comparação entre recusa e baseline.
    windowSeconds: 60,
    throughputAxis: true,
    series: [
      { key: 'eventos', label: 'eventos/s', stroke: CORES.p95, width: 1.5,
        fill: 'rgba(4, 152, 236, 0.12)', scale: 'throughput' },
      { key: 'recusa_base', label: 'linha de base (%)', stroke: CORES.base, dash: [5, 4] },
      { key: 'taxa_recusa', label: 'recusa móvel (%)', stroke: CORES.recusa, width: 2.5 },
    ],
  },
  throughput_live: {
    windowSeconds: 60,
    series: [
      { key: 'eventos', label: 'eventos persistidos/s', stroke: CORES.p95, width: 2.5,
        fill: 'rgba(4, 152, 236, 0.16)' },
    ],
  },
  volume: {
    series: [
      { key: 'eventos', label: 'eventos', stroke: CORES.p95, width: 2 },
    ],
  },
}

export default function Chart({ mode, points, minHeight = 240, onHover }) {
  const hostRef = useRef(null)
  const plotRef = useRef(null)

  const hoverRef = useRef(onHover)
  hoverRef.current = onHover
  const pointsRef = useRef(points)
  pointsRef.current = points

  const spec = MODOS[mode] ?? MODOS.latencia
  const dados = useMemo(() => {
    const x = points.map((p) => new Date(p.ts).getTime() / 1000)
    return [x, ...spec.series.map((s) => points.map((p, i) => {
      if (!s.apenasReconstruido) return p[s.key] ?? null
      // Os vizinhos entram também, senão o trecho tracejado flutua solto no ar.
      const vizinho = points[i - 1]?.reconstruido || points[i + 1]?.reconstruido
      return p.reconstruido || vizinho ? (p[s.key] ?? null) : null
    }))]
  }, [points, spec])

  useEffect(() => {
    const host = hostRef.current
    if (!host) return undefined

    // A legenda do uPlot é irmã do canvas dentro do mesmo host: sem descontar a
    // altura dela, ela some atrás da gaveta de query.
    const ALTURA_LEGENDA = 34
    const medir = () => ({
      width: host.clientWidth || 800,
      height: Math.max((host.clientHeight || 0) - ALTURA_LEGENDA, minHeight),
    })

    const opts = {
      ...medir(),
      padding: [12, 12, 0, 0],
      cursor: { drag: { x: true, y: false } },
      legend: { live: true },
      scales: spec.throughputAxis ? {
        throughput: { range: (u, min, max) => [0, Math.max(1, max * 1.15)] },
      } : {},
      series: [
        { label: 'instante', value: (u, v) => (v == null ? '—' : fmt(v * 1000)) },
        ...spec.series.map((s) => ({
          label: s.label, stroke: s.stroke, width: s.width ?? 2, dash: s.dash,
          fill: s.fill, scale: s.scale,
        })),
      ],
      axes: [
        { stroke: '#889397', grid: { stroke: 'rgba(61,79,88,0.5)' }, ticks: { stroke: '#3d4f58' } },
        { scale: 'y', stroke: '#889397', grid: { stroke: 'rgba(61,79,88,0.35)' },
          ticks: { stroke: '#3d4f58' } },
        ...(spec.throughputAxis ? [{
          scale: 'throughput', side: 1, stroke: CORES.p95,
          grid: { show: false }, ticks: { stroke: '#3d4f58' },
        }] : []),
      ],
      // Leitura do instante sob o cursor: a legenda fica no rodapé e o apresentador
      // precisa do valor junto das métricas, no alto da tela.
      hooks: {
        setCursor: [(u) => {
          const i = u.cursor.idx
          const ponto = i == null ? null : pointsRef.current[i]
          hoverRef.current?.(ponto ? { index: i, ...ponto } : null)
        }],
      },
    }

    plotRef.current = new uPlot(opts, dados, host)
    const observer = new ResizeObserver(() => plotRef.current?.setSize(medir()))
    observer.observe(host)
    return () => {
      observer.disconnect()
      plotRef.current?.destroy()
      plotRef.current = null
      hoverRef.current?.(null)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mode, minHeight])

  // Dado novo não recria o gráfico: só troca a série. Recriar a cada poll matava
  // cursor e zoom no meio da apresentação.
  useEffect(() => {
    const plot = plotRef.current
    if (!plot) return
    plot.setData(dados)
    if (spec.windowSeconds && dados[0].length) {
      const primeiro = dados[0][0]
      const ultimo = dados[0].at(-1)
      const max = Math.max(primeiro + spec.windowSeconds, ultimo)
      plot.setScale('x', { min: max - spec.windowSeconds, max })
    }
  }, [dados, spec])

  return <div className="chart-host" ref={hostRef} />
}

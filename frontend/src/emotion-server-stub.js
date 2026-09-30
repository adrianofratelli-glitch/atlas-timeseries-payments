// `@leafygreen-ui/emotion` importa `@emotion/server/create-instance` só para expor
// `renderStylesToNodeStream` — API de SSR que nenhum componente usa no navegador.
// Esse import arrasta streams do Node para o bundle e quebra a página com
// "ReferenceError: Buffer is not defined" antes do primeiro render.
//
// O alias em vite.config.js troca aquele módulo por este. Se algum dia esta PoV
// renderizar no servidor, remova o alias — e não silencie o erro no console.
export default function createEmotionServer() {
  const naoSuportado = () => {
    throw new Error('SSR do emotion não está disponível neste bundle de navegador')
  }
  return {
    extractCritical: naoSuportado,
    renderStylesToString: naoSuportado,
    renderStylesToNodeStream: naoSuportado,
  }
}

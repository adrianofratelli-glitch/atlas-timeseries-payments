import { fileURLToPath, URL } from 'node:url'
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Portas estritas: o registro do workspace é o PORTS.md, e o launcher preserva
// quem já estiver escutando em vez de trocar de porta em silêncio. Host fixo em
// 127.0.0.1: `localhost` pode resolver só para ::1 (ordem de DNS do Node), e aí o
// start.sh, que sonda 127.0.0.1, dava "Frontend não ficou pronto" com o vite no ar.
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      // Ver src/emotion-server-stub.js: sem isso o bundle carrega streams do Node
      // e a página morre com "Buffer is not defined" antes de renderizar.
      '@emotion/server/create-instance': fileURLToPath(
        new URL('./src/emotion-server-stub.js', import.meta.url),
      ),
    },
  },
  server: { host: '127.0.0.1', port: 5400, strictPort: true },
  preview: { host: '127.0.0.1', port: 5400, strictPort: true },
})

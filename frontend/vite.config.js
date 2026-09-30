import { fileURLToPath, URL } from 'node:url'
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Portas estritas: o registro do workspace é o PORTS.md, e o launcher preserva
// quem já estiver escutando em vez de trocar de porta em silêncio.
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
  server: { port: 5400, strictPort: true },
  preview: { port: 5400, strictPort: true },
})

import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/query': 'http://localhost:8080',
      '/tables': 'http://localhost:8080',
      '/samples': 'http://localhost:8080',
      '/health': 'http://localhost:8080'
    }
  }
})

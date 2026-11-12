import adapter from '@sveltejs/adapter-static';
import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [
    // A single-page app: every path gets index.html, whose router takes it.
    sveltekit({
      adapter: adapter({ pages: '../dist', assets: '../dist', fallback: 'index.html', strict: false })
    })
  ],
  // `npm run dev` against a manager's console, e.g. CONSOLE=https://127.0.0.1:7480
  server: process.env.CONSOLE
    ? { proxy: { '/api': { target: process.env.CONSOLE, secure: false, ws: true } } }
    : undefined
});

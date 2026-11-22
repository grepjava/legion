import adapter from '@sveltejs/adapter-static';
import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vitest/config';

// `npm run dev` against a manager's console, e.g. CONSOLE=https://127.0.0.1:7480.
// The manager refuses a request whose Origin is not its own host, so the
// proxy says it comes from there: the dev server stands in for the page.
const target = process.env.CONSOLE;

export default defineConfig({
  // Nothing inlined as a `data:` address: the console's policy loads fonts
  // and images from itself only, so an inlined font (a small subset of
  // Inter's) would be refused.
  build: { assetsInlineLimit: 0 },
  // `npm test`: the pure helpers, Svelte's runes as a browser runs them.
  test: { include: ['src/**/*.test.ts'] },
  resolve: process.env.VITEST ? { conditions: ['browser'] } : undefined,
  plugins: [
    // A single-page app: every path gets index.html, whose router takes it.
    sveltekit({
      adapter: adapter({ pages: '../dist', assets: '../dist', fallback: 'index.html', strict: false })
    })
  ],
  server: target
    ? {
        proxy: {
          '/api': {
            target,
            secure: false,
            ws: true,
            changeOrigin: true,
            configure: (proxy) => {
              const origin = new URL(target).origin;
              proxy.on('proxyReq', (req) => {
                if (req.getHeader('origin')) req.setHeader('origin', origin);
              });
              proxy.on('proxyReqWs', (req) => {
                if (req.getHeader('origin')) req.setHeader('origin', origin);
              });
            }
          }
        }
      }
    : undefined
});

# legion-console

The web console's pages (DESIGN.md §3.9, CONSOLE.md): a Svelte app in
`web/`, built into `dist/`, which this crate embeds in the `legion` binary.
A manager serves them with `--console-listen`.

`dist/` is kept in the repository, so building Legion, its release tarballs
and the port needs no Node. Change the app in `web/`, then build it again
and commit `dist/` with the change:

    cd web
    npm ci
    npm run check   # svelte-check: types and Svelte diagnostics
    npm run build   # writes ../dist

The app is Svelte 5 with SvelteKit 3 (static adapter, a single-page app:
the gateway serves `index.html` for every path that is not a file), kept on
their latest releases. `npm run dev` with `CONSOLE=https://<manager>:7480`
serves the app with Vite and sends `/api` to that manager's console.

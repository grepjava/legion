<script lang="ts">
  // A plugin's page (CONSOLE.md phase 6), framed in a sandbox of its own
  // origin, which the gateway's policy also gives it: no cookie, no
  // session, no address it may reach. It asks this page in messages, and
  // gets reads as the user, in its own namespace alone.
  import { page } from '$app/state';
  import { api } from '#lib/api.ts';
  import { i18n, t } from '#lib/i18n.svelte.ts';
  import { session } from '#lib/session.svelte.ts';

  const namespace = $derived(page.params.namespace ?? '');
  const name = $derived(page.params.name ?? '');
  let frame: HTMLIFrameElement | undefined = $state();

  type Ask = { legion: 1; id: unknown; op: string; kind?: unknown; name?: unknown };

  function send(message: object) {
    // Its origin is opaque ("null"): '*' is the only target there is; the
    // window is the frame's own.
    frame?.contentWindow?.postMessage({ legion: 1, ...message }, '*');
  }

  /** A namespaced kind the API knows, or why not. */
  function kindOf(kind: unknown): string {
    const k = session.kinds.find((x) => x.kind === kind);
    if (!k) throw new Error(t('no such kind'));
    if (k.clusterScoped) throw new Error(t('a plugin reads its own namespace only'));
    return k.kind;
  }

  async function answer(ask: Ask): Promise<unknown> {
    switch (ask.op) {
      case 'list':
        return (await api.list(kindOf(ask.kind), namespace)).objects;
      case 'get':
        if (typeof ask.name !== 'string' || !ask.name) throw new Error(t('which object?'));
        return await api.get(kindOf(ask.kind), namespace, ask.name);
      case 'me': {
        const me = session.me;
        if (!me) throw new Error(t('not signed in'));
        return { name: me.name, role: me.namespaces[namespace] ?? (me.all ? me.role : null) };
      }
      default:
        throw new Error(t('no such request'));
    }
  }

  $effect(() => {
    const listen = (e: MessageEvent) => {
      if (!frame || e.source !== frame.contentWindow) return;
      const ask = e.data as Ask;
      if (!ask || ask.legion !== 1 || typeof ask.op !== 'string') return;
      answer(ask).then(
        (result) => send({ id: ask.id, ok: true, result }),
        (err) => send({ id: ask.id, ok: false, error: (err as Error).message })
      );
    };
    window.addEventListener('message', listen);
    return () => window.removeEventListener('message', listen);
  });

  const hello = () => send({ op: 'hello', namespace, language: i18n.language });
</script>

<header class="bar">
  <h1>{name} <span class="muted">{t('plugin in {namespace}', { namespace })}</span></h1>
</header>
<p class="muted">
  {t('A page added in {namespace}. It reads that namespace as you, and nothing else; it cannot change anything.', {
    namespace
  })}
</p>
{#key `${namespace}/${name}`}
  <iframe
    bind:this={frame}
    src="/api/plugin/{encodeURIComponent(namespace)}/{encodeURIComponent(name)}"
    sandbox="allow-scripts"
    referrerpolicy="no-referrer"
    title={t('Plugin {name}', { name })}
    onload={hello}
  ></iframe>
{/key}

<style>
  iframe {
    width: 100%;
    height: 75vh;
    border: 1px solid var(--line);
    border-radius: 6px;
    background: var(--panel);
  }
</style>

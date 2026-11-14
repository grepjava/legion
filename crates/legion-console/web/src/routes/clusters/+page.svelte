<script lang="ts">
  // The clusters this console knows (CONSOLE.md phase 6): this one, and
  // each other cluster's console, asked who the user is there. Each is
  // signed into on its own; rights are each cluster's bindings.
  import { meIn, type Me } from '#lib/api.ts';
  import { session } from '#lib/session.svelte.ts';
  import { t } from '#lib/i18n.svelte.ts';

  type Answer = Me | 'signed-out' | string | undefined;
  let answers: Answer[] = $state([]);

  $effect(() => {
    const peers = session.settings.clusters ?? [];
    answers = peers.map(() => undefined);
    peers.forEach((p, i) => meIn(p).then((a) => (answers[i] = a)));
  });

  function access(me: Me): string {
    if (me.all) return me.role === 'admin' ? t('admin everywhere') : t('reads everywhere');
    const ns = Object.entries(me.namespaces);
    if (!ns.length) return t('no namespaces');
    return ns.map(([n, r]) => `${n}: ${r}`).join(', ');
  }
</script>

<header class="bar"><h1>{t('Clusters')}</h1></header>
<p class="muted">{t('Each cluster is signed into on its own, with the access its bindings give.')}</p>

<table>
  <thead>
    <tr><th>{t('Cluster')}</th><th>{t('Signed in as')}</th><th>{t('Access')}</th></tr>
  </thead>
  <tbody>
    {#if session.me}
      <tr aria-label={t('Cluster {name}', { name: session.settings.name ?? t('this one') })}>
        <td>{session.settings.name ?? t('this cluster')} <span class="muted">{t('(here)')}</span></td>
        <td>{session.me.name}</td>
        <td>{access(session.me)}</td>
      </tr>
    {/if}
    {#each session.settings.clusters ?? [] as c, i (i)}
      {@const a = answers[i]}
      <tr aria-label={t('Cluster {name}', { name: c.name })}>
        <td><a href={c.url}>{c.name}</a></td>
        {#if a === undefined}
          <td colspan="2" class="muted">{t('Asking…')}</td>
        {:else if a === 'signed-out'}
          <td colspan="2"><a href={new URL('/login', c.url).href}>{t('Sign in')}</a></td>
        {:else if typeof a === 'string'}
          <td colspan="2" class="muted">{t('Cannot tell: {why}.', { why: a })} <a href={c.url}>{t('Open it')}</a></td>
        {:else}
          <td>{a.name}</td>
          <td>{access(a)}</td>
        {/if}
      </tr>
    {/each}
  </tbody>
</table>

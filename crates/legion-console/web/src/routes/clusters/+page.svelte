<script lang="ts">
  // The clusters this console knows (CONSOLE.md phase 6): this one, and
  // each other cluster's console, asked who the user is there. Each is
  // signed into on its own; rights are each cluster's bindings.
  import { meIn, type Me } from '#lib/api.ts';
  import { session } from '#lib/session.svelte.ts';

  type Answer = Me | 'signed-out' | string | undefined;
  let answers: Answer[] = $state([]);

  $effect(() => {
    const peers = session.settings.clusters ?? [];
    answers = peers.map(() => undefined);
    peers.forEach((p, i) => meIn(p).then((a) => (answers[i] = a)));
  });

  function access(me: Me): string {
    if (me.all) return `${me.role === 'admin' ? 'admin' : 'reads'} everywhere`;
    const ns = Object.entries(me.namespaces);
    if (!ns.length) return 'no namespaces';
    return ns.map(([n, r]) => `${n}: ${r}`).join(', ');
  }
</script>

<header class="bar"><h1>Clusters</h1></header>
<p class="muted">Each cluster is signed into on its own, with the access its bindings give.</p>

<table>
  <thead>
    <tr><th>Cluster</th><th>Signed in as</th><th>Access</th></tr>
  </thead>
  <tbody>
    {#if session.me}
      <tr aria-label="Cluster {session.settings.name ?? 'this one'}">
        <td>{session.settings.name ?? 'this cluster'} <span class="muted">(here)</span></td>
        <td>{session.me.name}</td>
        <td>{access(session.me)}</td>
      </tr>
    {/if}
    {#each session.settings.clusters ?? [] as c, i (i)}
      {@const a = answers[i]}
      <tr aria-label="Cluster {c.name}">
        <td><a href={c.url}>{c.name}</a></td>
        {#if a === undefined}
          <td colspan="2" class="muted">Asking…</td>
        {:else if a === 'signed-out'}
          <td colspan="2"><a href={new URL('/login', c.url).href}>Sign in</a></td>
        {:else if typeof a === 'string'}
          <td colspan="2" class="muted">Cannot tell: {a}. <a href={c.url}>Open it</a></td>
        {:else}
          <td>{a.name}</td>
          <td>{access(a)}</td>
        {/if}
      </tr>
    {/each}
  </tbody>
</table>

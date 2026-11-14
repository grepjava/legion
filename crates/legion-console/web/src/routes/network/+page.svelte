<script lang="ts">
  // Who reaches whom in a namespace, as the nodes enforce it: its
  // isolation and its NetworkPolicies, the destination's ingress and the
  // source's egress, decided by the API (`legion reach`), not here.
  import { api, type ReachEdge } from '#lib/api.ts';
  import { namespaces, session } from '#lib/session.svelte.ts';

  let chosen = $state('');
  let typed = $state('');
  let services: string[] = $state([]);
  let edges: ReachEdge[] = $state([]);
  let error = $state('');

  // The namespace picked in the sidebar, else the first the user has.
  const mine = $derived(namespaces([]));
  const namespace = $derived(chosen || session.namespace || mine[0] || 'default');

  $effect(() => {
    const ns = namespace;
    error = '';
    services = [];
    edges = [];
    api
      .reach(ns)
      .then((r) => {
        services = r.services;
        edges = r.edges;
      })
      .catch((e) => (error = e.message));
  });

  const sources = $derived(['', ...services]);
  const cell = (from: string, to: string) => {
    const e = edges.find((x) => x.from === from && x.to === to);
    if (!e) return null;
    return e.all_ports ? 'all' : e.ports.map((p) => `${p.port}/${p.protocol}`).join(', ');
  };
</script>

<header class="bar">
  <h1>Network: {namespace}</h1>
  <form
    class="inline"
    onsubmit={(e) => {
      e.preventDefault();
      chosen = typed.trim();
    }}
  >
    <input placeholder="another namespace" bind:value={typed} aria-label="Namespace" />
    <button>Show</button>
  </form>
</header>
<p class="muted">
  What each source may reach of each service, on which of its tasks' ports: the namespace's isolation
  and its NetworkPolicies, ingress and egress, as the nodes enforce them.
</p>
{#if error}<p class="error" role="alert">{error}</p>{/if}

{#if services.length}
  <table class="matrix">
    <thead>
      <tr>
        <th>from \ to</th>
        {#each services as to (to)}<th><a href="/k/Service/{namespace}/{to}">{to}</a></th>{/each}
      </tr>
    </thead>
    <tbody>
      {#each sources as from (from)}
        <tr>
          <th>{#if from}<a href="/k/Service/{namespace}/{from}">{from}</a>{:else}<span class="muted">other namespaces</span>{/if}</th>
          {#each services as to (to)}
            {@const c = cell(from, to)}
            <td class={c === null ? 'no' : c === 'all' ? 'all' : 'some'}>{c ?? '—'}</td>
          {/each}
        </tr>
      {/each}
    </tbody>
  </table>
{:else if !error}
  <p class="muted">No services in {namespace}.</p>
{/if}

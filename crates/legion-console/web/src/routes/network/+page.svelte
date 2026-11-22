<script lang="ts">
  // Who reaches whom in a namespace, as the nodes enforce it: its
  // isolation and its NetworkPolicies, the destination's ingress and the
  // source's egress, decided by the API (`legion reach`), not here; and
  // its load balancer addresses with the node holding each (`legion lb`),
  // followed, as CARP moves them on its own.
  import { api, followLoadBalancers, type Balancer, type ReachEdge } from '#lib/api.ts';
  import { namespaces, session } from '#lib/session.svelte.ts';
  import { t } from '#lib/i18n.svelte.ts';
  import { Table } from '#lib/table.svelte.ts';
  import SortTh from '#lib/SortTh.svelte';
  import TableBar from '#lib/TableBar.svelte';
  import Pager from '#lib/Pager.svelte';

  let chosen = $state('');
  let typed = $state('');
  let services: string[] = $state([]);
  let edges: ReachEdge[] = $state([]);
  let balancers: Balancer[] = $state([]);
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

  $effect(() => {
    const ns = namespace;
    balancers = [];
    return followLoadBalancers(ns, (b) => (balancers = b));
  });
  const holders = (b: Balancer) => b.nodes.filter((n) => n.state === 'master').map((n) => n.node);
  // The addresses by service; an address sorts by its numbers, the
  // collator putting 10.0.0.9 before 10.0.0.10.
  const lbs = new Table(() => balancers, {
    sorts: {
      service: (b) => b.service,
      address: (b) => b.address,
      held: (b) => holders(b).join(', '),
      nodes: (b) => b.nodes.map((n) => n.node).join(', ')
    },
    sortBy: 'service'
  });

  const sources = $derived(['', ...services]);
  const cell = (from: string, to: string) => {
    const e = edges.find((x) => x.from === from && x.to === to);
    if (!e) return null;
    return e.all_ports ? 'all' : e.ports.map((p) => `${p.port}/${p.protocol}`).join(', ');
  };
</script>

<header class="bar">
  <h1>{t('Network: {namespace}', { namespace })}</h1>
  <form
    class="inline"
    onsubmit={(e) => {
      e.preventDefault();
      chosen = typed.trim();
    }}
  >
    <input placeholder={t('another namespace')} bind:value={typed} aria-label={t('Namespace')} />
    <button>{t('Show')}</button>
  </form>
</header>
<p class="muted">
  {t(
    "What each source may reach of each service, on which of its tasks' ports: the namespace's isolation and its NetworkPolicies, ingress and egress, as the nodes enforce them."
  )}
</p>
{#if error}<p class="error" role="alert">{error}</p>{/if}

{#if services.length}
  <table class="matrix">
    <thead>
      <tr>
        <th scope="col">{t('from \\ to')}</th>
        {#each services as to (to)}<th scope="col"><a href="/k/Service/{namespace}/{to}">{to}</a></th>{/each}
      </tr>
    </thead>
    <tbody>
      {#each sources as from (from)}
        <tr>
          <th scope="row">{#if from}<a href="/k/Service/{namespace}/{from}">{from}</a>{:else}<span class="muted">{t('other namespaces')}</span>{/if}</th>
          {#each services as to (to)}
            {@const c = cell(from, to)}
            <td class={c === null ? 'no' : c === 'all' ? 'all' : 'some'}>{c === 'all' ? t('all') : (c ?? '—')}</td>
          {/each}
        </tr>
      {/each}
    </tbody>
  </table>
{:else if !error}
  <p class="empty">{t('No services in {namespace}.', { namespace })}</p>
{/if}

{#if balancers.length}
  <h2>{t('Load balancer addresses')}</h2>
  <TableBar table={lbs} label={t('Search load balancer addresses')} />
  <table>
    <thead>
      <tr>
        <SortTh table={lbs} col="service">{t('Service')}</SortTh>
        <SortTh table={lbs} col="address">{t('Address')}</SortTh>
        <SortTh table={lbs} col="held">{t('Held by')}</SortTh>
        <SortTh table={lbs} col="nodes">{t('Nodes that may hold it')}</SortTh>
      </tr>
    </thead>
    <tbody>
      {#each lbs.rows as b (b.service)}
        {@const held = holders(b)}
        <tr>
          <td><a href="/k/Service/{namespace}/{b.service}">{b.service}</a></td>
          <td>{b.address} <span class="muted">{t('vhid {vhid}', { vhid: b.vhid })}</span></td>
          <td aria-label={t('{address} held by', { address: b.address })}
            >{#if held.length}{held.join(', ')}{:else}<span class="muted">{t('no node')}</span>{/if}</td
          >
          <td
            >{#each b.nodes as n, i (n.node)}{#if i}, {/if}{n.node}
              <span class="muted">({n.state || t('not reported')})</span>{/each}</td
          >
        </tr>
      {:else}
        <tr><td colspan="4" class="muted">{t('Nothing matches.')}</td></tr>
      {/each}
    </tbody>
  </table>
  <Pager table={lbs} label={t('Load balancer addresses')} />
{/if}

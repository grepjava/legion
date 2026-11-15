<script lang="ts">
  // A service's ports as links a browser can open: through the routing
  // mesh, on the nodes running it, at its load balancer, and by its Routes.
  import { api, type LObject } from '#lib/api.ts';
  import { t } from '#lib/i18n.svelte.ts';
  import { hostOf, reach } from '#lib/ports.ts';

  let { service }: { service: LObject } = $props();

  let tasks: LObject[] = $state([]);
  let routes: LObject[] = $state([]);
  let nodes = $state(new Map<string, string>());

  $effect(() => {
    const ns = service.metadata.namespace;
    api.list('Task', ns).then((l) => (tasks = l.objects), () => (tasks = []));
    api.list('Route', ns).then((l) => (routes = l.objects), () => (routes = []));
    // Nodes are the cluster's: a user of one namespace sees none, and
    // host ports are then named by node without a link.
    api.list('Node').then(
      (l) => (nodes = new Map(l.objects.map((n) => [n.metadata.name, hostOf(n.status?.addr ?? '')]))),
      () => (nodes = new Map())
    );
  });

  const ways = $derived(reach(service, location.hostname, tasks, nodes, routes));
  const how: Record<string, string> = $derived({
    ingress: t('any node'),
    host: t('nodes running it'),
    'load balancer': t('load balancer'),
    route: t('Route')
  });
</script>

{#if ways.length}
  <h3>{t('Open')}</h3>
  <ul class="ports">
    {#each ways as w, i (i)}
      <li>
        <span class="mono">{w.what}</span>
        <span class="muted">{how[w.how] ?? w.how}</span>
        {#each w.links as l (l.href)}
          <a href={l.href} target="_blank" rel="noopener noreferrer" class="mono">{l.text}</a>
        {/each}
        {#if w.note}<span class="muted">{w.note}</span>{/if}
      </li>
    {/each}
  </ul>
{/if}

<style>
  .ports {
    list-style: none;
    padding: 0;
    display: grid;
    gap: 0.35rem;
  }
  .ports li {
    display: flex;
    flex-wrap: wrap;
    gap: 0.75rem;
    align-items: baseline;
  }
</style>

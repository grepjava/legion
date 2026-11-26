<script lang="ts">
  // The Topology page's side panel: for one Service, what pf lets in and
  // out, the connections the nodes saw, its CARP addresses, published
  // ports, Routes and Volumes.
  import type { Balancer, ConnectionEdge, LObject, ReachEdge } from './api.ts';
  import { t } from './i18n.svelte.ts';
  import { conns, endName, ports, portWhere, svcNote, viaNote, type Published, type Use } from './topology.ts';

  let {
    service,
    namespace,
    edges,
    seen,
    balancers,
    routes,
    published,
    used
  }: {
    service: LObject;
    namespace: string;
    edges: ReachEdge[];
    seen: ConnectionEdge[];
    balancers: Balancer[];
    routes: LObject[];
    published: Published[];
    used: Use[];
  } = $props();

  const name = $derived(service.metadata.name as string);
  const link = (kind: string, n: string) => `/k/${kind}/${namespace}/${n}`;
  const inbound = $derived(edges.filter((e) => e.to === name));
  const outbound = $derived(edges.filter((e) => e.from === name));
  const seenInbound = $derived(seen.filter((e) => e.to.kind === 'service' && e.to.name === name));
  const seenOutbound = $derived(
    seen.filter(
      (e) => e.from.kind === 'service' && e.from.name === name && !(e.to.kind === 'service' && e.to.name === name)
    )
  );
  const vips = $derived(balancers.filter((b) => b.service === name));
  const itsRoutes = $derived(routes.filter((r) => r.spec?.service === name));
  const itsPublished = $derived(published.filter((p) => p.service === name));
  const itsVolumes = $derived(used.filter((u) => u.service === name).map((u) => u.volume));
</script>

<aside class="card pf-inspector" aria-label={t('Packet filter inspector')}>
  <h2>
    <a href={link('Service', name)}>{name}</a>
  </h2>
  <p class="muted">{svcNote(service)}</p>

  <h3>{t('Inbound PF allow rules')}</h3>
  {#if inbound.length}
    <ul class="plain mono">
      {#each inbound as e (`${e.from}->${e.to}`)}
        <li>← {e.from || t('other namespaces')}: {ports(e)}</li>
      {/each}
    </ul>
  {:else}
    <p class="muted">{t('Isolated: no inbound peers allowed.')}</p>
  {/if}

  <h3>{t('Outbound PF allow rules')}</h3>
  {#if outbound.length}
    <ul class="plain mono">
      {#each outbound as e (`${e.from}->${e.to}`)}
        <li>→ {e.to}: {ports(e)}</li>
      {/each}
    </ul>
  {:else}
    <p class="muted">{t('No intra-namespace outbound edges.')}</p>
  {/if}

  <h3>{t('Connections seen')}</h3>
  {#if seenInbound.length || seenOutbound.length}
    <ul class="plain mono">
      {#each seenInbound as e (`${e.from.kind}:${e.from.name}>${e.port}/${e.protocol}/${e.via}`)}
        <li>
          ← {endName(e.from)}{e.from.kind === 'outside' && e.clients > 1 ? ` (${t('{n} clients', { n: e.clients })})` : ''}: {e.port}/{e.protocol}
          <span class="muted">{viaNote(e)} · {conns(e.connections)}</span>
        </li>
      {/each}
      {#each seenOutbound as e (`${e.to.kind}:${e.to.name}>${e.port}/${e.protocol}/${e.via}`)}
        <li>
          → {endName(e.to)}: {e.port}/{e.protocol}
          <span class="muted">{viaNote(e)} · {conns(e.connections)}</span>
        </li>
      {/each}
    </ul>
  {:else}
    <p class="muted">{t('None in the last minute and a half.')}</p>
  {/if}

  {#if vips.length}
    <h3>{t('CARP VIPs')}</h3>
    <ul class="plain mono">
      {#each vips as b (b.address)}
        <li>{b.address} (vhid {b.vhid})</li>
      {/each}
    </ul>
  {/if}

  {#if itsPublished.length}
    <h3>{t('Published ports')}</h3>
    <ul class="plain mono">
      {#each itsPublished as p (p.what)}
        <li>
          {p.at === null ? '—' : p.at} → {p.what}
          <span class="muted">{portWhere(p)}</span>
        </li>
      {/each}
    </ul>
  {/if}

  {#if itsRoutes.length}
    <h3>{t('Ingress Routes')}</h3>
    <ul class="plain">
      {#each itsRoutes as r (r.metadata.name)}
        <li>
          <a href={link('Route', r.metadata.name)}>
            {r.spec?.host ?? ''}{r.spec?.path ?? '/'}
          </a>
        </li>
      {/each}
    </ul>
  {/if}

  {#if itsVolumes.length}
    <h3>{t('Mounted Volumes')}</h3>
    <ul class="plain">
      {#each itsVolumes as v (v)}
        <li><a href={link('Volume', v)}>{v}</a></li>
      {/each}
    </ul>
  {/if}
</aside>

<style>
  /* The name and how ready it is read as one heading. */
  .pf-inspector h2 {
    margin-bottom: 0.1rem;
  }
  .pf-inspector h2 + p {
    margin-top: 0;
  }
  .pf-inspector {
    border-right: 1px solid var(--line);
    background: var(--panel);
    height: auto;
    overflow: visible;
  }
</style>

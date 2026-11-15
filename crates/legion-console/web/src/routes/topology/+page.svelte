<script lang="ts">
  // Interactive Topology & PF Reachability Inspector: visualises a
  // namespace's Routes, Services, PF packet-filter reachability arcs, CARP
  // VIPs, and ZFS/NFS/iSCSI Volumes with zoom controls and click-to-inspect
  // packet filter rules.
  import { api, follow, type Balancer, type LObject, type ReachEdge } from '#lib/api.ts';
  import { t } from '#lib/i18n.svelte.ts';
  import { namespaces, session } from '#lib/session.svelte.ts';

  const mine = $derived(namespaces([]));
  let chosen = $state('');
  let typed = $state('');
  const namespace = $derived(chosen || session.namespace || mine[0] || 'default');

  let services: LObject[] = $state([]);
  let routes: LObject[] = $state([]);
  let volumes: LObject[] = $state([]);
  let edges: ReachEdge[] = $state([]);
  let balancers: Balancer[] = $state([]);
  let selectedService = $state('');
  let zoom = $state(100);
  let error = $state('');

  $effect(() => {
    const ns = namespace;
    error = '';
    const failed = (e: Error) => (error = e.message);
    const stops = [
      follow('Service', ns, (o) => (services = o), failed),
      follow('Route', ns, (o) => (routes = o), failed),
      follow('Volume', ns, (o) => (volumes = o), failed)
    ];
    return () => stops.forEach((s) => s());
  });

  // Reach & CARP VIPs are refreshed when services change.
  const names = $derived(services.map((s) => s.metadata.name).join(','));
  $effect(() => {
    const ns = namespace;
    void names;
    api
      .reach(ns)
      .then((r) => (edges = r.edges))
      .catch((e) => (error = e.message));
    api
      .loadBalancers(ns)
      .then((b) => (balancers = b.balancers))
      .catch(() => (balancers = []));
  });

  const ROW = 56;
  const BOX_W = 190;
  const BOX_H = 40;
  const X_ROUTE = 10;
  const X_SVC = 290;
  const ARCS = 170;
  const X_VOL = X_SVC + BOX_W + ARCS;

  const svcRow = $derived(new Map(services.map((s, i) => [s.metadata.name, i])));
  const used = $derived(
    services.flatMap((s) =>
      ((s.spec?.pod?.volumes ?? []) as { volume?: { name: string } }[])
        .filter((v) => v.volume)
        .map((v) => ({ service: s.metadata.name, volume: v.volume!.name }))
    )
  );
  const volumeNames = $derived(
    [...new Set([...volumes.map((v) => v.metadata.name), ...used.map((u) => u.volume)])].sort()
  );
  const volRow = $derived(new Map(volumeNames.map((v, i) => [v, i])));
  const shownRoutes = $derived(routes.filter((r) => svcRow.has(r.spec?.service)));
  const outside = $derived(edges.filter((e) => e.from === '' && svcRow.has(e.to)));
  const between = $derived(
    edges.filter((e) => e.from !== '' && e.from !== e.to && svcRow.has(e.from) && svcRow.has(e.to))
  );
  const everyone = $derived(
    services.length > 2 && between.length === services.length * (services.length - 1)
  );
  const leftRows = $derived(shownRoutes.length + (outside.length ? 1 : 0));
  const svgW = X_VOL + BOX_W + 10;
  const height = $derived(Math.max(services.length, volumeNames.length, leftRows, 1) * ROW + 20);

  const y = (row: number) => 10 + row * ROW + BOX_H / 2;
  const health = (s: LObject) => {
    const st = s.status;
    if (!st) return 'wait';
    if ((st.desired ?? 0) === 0) return 'idle';
    return (st.ready ?? 0) >= st.desired ? 'good' : (st.ready ?? 0) === 0 ? 'bad' : 'wait';
  };
  const volumeHealth = (name: string) => {
    const v = volumes.find((v) => v.metadata.name === name);
    if (!v) return 'bad';
    return v.status?.lost ? 'bad' : 'good';
  };
  const volumeNote = (name: string) => {
    const v = volumes.find((v) => v.metadata.name === name);
    if (!v) return t('missing');
    if (v.status?.lost) return t('lost');
    return v.status?.node ? t('Volume on {node}', { node: v.status.node }) : t('Volume');
  };
  const svcNote = (s: LObject) =>
    s.status
      ? t('{ready}/{desired} ready', { ready: s.status.ready ?? 0, desired: s.status.desired ?? 0 })
      : t('not yet placed');
  const ports = (e: ReachEdge) =>
    e.all_ports ? t('all ports') : e.ports.map((p) => `${p.port}/${p.protocol}`).join(', ');
  const arc = (from: number, to: number) => {
    const x = X_SVC + BOX_W;
    const bulge = Math.min(ARCS - 20, 30 + Math.abs(to - from) * 22);
    return `M ${x} ${y(from) - 6} C ${x + bulge} ${y(from) - 6}, ${x + bulge} ${y(to) + 6}, ${x + 6} ${y(to) + 6}`;
  };
  const link = (kind: string, name: string) => `/k/${kind}/${namespace}/${name}`;

  const inspectedSvc = $derived(
    services.find((s) => s.metadata.name === selectedService) ?? services[0] ?? null
  );
  const inboundEdges = $derived(
    inspectedSvc ? edges.filter((e) => e.to === inspectedSvc.metadata.name) : []
  );
  const outboundEdges = $derived(
    inspectedSvc ? edges.filter((e) => e.from === inspectedSvc.metadata.name) : []
  );
  const svcVips = $derived(
    inspectedSvc ? balancers.filter((b) => b.service === inspectedSvc.metadata.name) : []
  );
  const svcRoutes = $derived(
    inspectedSvc ? shownRoutes.filter((r) => r.spec?.service === inspectedSvc.metadata.name) : []
  );
  const svcVols = $derived(
    inspectedSvc ? used.filter((u) => u.service === inspectedSvc.metadata.name).map((u) => u.volume) : []
  );
</script>

<header class="bar">
  <h1>{t('Topology: {namespace}', { namespace })}</h1>
  <div class="controls">
    <button type="button" class="sm" onclick={() => (zoom = Math.max(60, zoom - 20))}>−</button>
    <span class="mono muted">{zoom}%</span>
    <button type="button" class="sm" onclick={() => (zoom = Math.min(160, zoom + 20))}>+</button>
    <button type="button" class="sm" onclick={() => (zoom = 100)}>{t('Reset')}</button>
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
  </div>
</header>
<p class="muted">
  {t(
    "Routes to the namespace's Services, what may reach what among them as the nodes enforce it, and the Volumes they use. Green: every task ready; amber: some; red: none, or a Volume lost."
  )}
</p>
{#if error}<p class="error" role="alert">{error}</p>{/if}
{#if everyone}
  <p class="muted">{t('Every service may reach every other (the namespace is not isolated by policy).')}</p>
{/if}

{#if services.length}
  <p class="sr-only">
    {t('What may reach what is listed on the')} <a href="/network">{t('Network')}</a> {t('page too.')}
  </p>

  <div class="topo-layout">
    <div class="card canvas-card">
      <div class="controls">
        <span class="muted">{t('Inspect PF rules for Service:')}</span>
        {#each services as s (s.metadata.name)}
          <button
            type="button"
            class="sm"
            class:primary={(inspectedSvc?.metadata.name ?? '') === s.metadata.name}
            onclick={() => (selectedService = s.metadata.name)}
          >
            {s.metadata.name}
          </button>
        {/each}
      </div>

      <!-- svelte-ignore a11y_no_noninteractive_tabindex -->
      <div class="table-scroll" tabindex="0" role="region" aria-label={t('Topology graph')}>
        <svg
          class="topology"
          width={Math.round((svgW * zoom) / 100)}
          height={Math.round((height * zoom) / 100)}
          viewBox="0 0 {svgW} {height}"
          role="group"
          aria-label={t('Topology of {namespace}', { namespace })}
        >
          <defs>
            <marker id="arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto">
              <path d="M0,0 L8,4 L0,8 z" class="head" />
            </marker>
          </defs>

          {#each shownRoutes as r, i (r.metadata.name)}
            <line
              class="edge"
              x1={X_ROUTE + BOX_W}
              y1={y(i)}
              x2={X_SVC}
              y2={y(svcRow.get(r.spec.service)!)}
              marker-end="url(#arrow)"
            />
          {/each}
          {#if outside.length}
            {#each outside as e (e.to)}
              <line
                class="edge reach"
                x1={X_ROUTE + BOX_W}
                y1={y(shownRoutes.length)}
                x2={X_SVC}
                y2={y(svcRow.get(e.to)!)}
                marker-end="url(#arrow)"
              >
                <title>{t('other namespaces → {service}: {ports}', { service: e.to, ports: ports(e) })}</title>
              </line>
            {/each}
          {/if}
          {#if !everyone}
            {#each between as e (`${e.from}>${e.to}`)}
              <path
                class="edge reach"
                d={arc(svcRow.get(e.from)!, svcRow.get(e.to)!)}
                marker-end="url(#arrow)"
              >
                <title>{e.from} → {e.to}: {ports(e)}</title>
              </path>
            {/each}
          {/if}
          {#each used as u (`${u.service}>${u.volume}`)}
            <line
              class="edge"
              x1={X_SVC + BOX_W}
              y1={y(svcRow.get(u.service)!)}
              x2={X_VOL}
              y2={y(volRow.get(u.volume)!)}
            />
          {/each}

          {#each shownRoutes as r, i (r.metadata.name)}
            <a
              href={link('Route', r.metadata.name)}
              aria-label={t('Route {name}: {host}{path} to {service}', {
                name: r.metadata.name,
                host: r.spec.host ?? '',
                path: r.spec.path ?? '',
                service: r.spec.service
              })}
            >
              <rect class="box route" x={X_ROUTE} y={10 + i * ROW} width={BOX_W} height={BOX_H} rx="6" />
              <text x={X_ROUTE + 10} y={10 + i * ROW + 17}>
                {r.spec.host}{r.spec.path && r.spec.path !== '/' ? r.spec.path : ''}
              </text>
              <text class="sub" x={X_ROUTE + 10} y={10 + i * ROW + 32}>
                {t('Route {name}', { name: r.metadata.name })}
              </text>
            </a>
          {/each}
          {#if outside.length}
            <rect class="box outside" x={X_ROUTE} y={10 + shownRoutes.length * ROW} width={BOX_W} height={BOX_H} rx="6" />
            <text x={X_ROUTE + 10} y={10 + shownRoutes.length * ROW + 25}>{t('other namespaces')}</text>
          {/if}
          {#each services as s, i (s.metadata.name)}
            <a
              href={link('Service', s.metadata.name)}
              aria-label={t('Service {name}: {note}', { name: s.metadata.name, note: svcNote(s) })}
            >
              <rect
                class="box svc {health(s)}"
                class:picked={inspectedSvc?.metadata.name === s.metadata.name}
                x={X_SVC}
                y={10 + i * ROW}
                width={BOX_W}
                height={BOX_H}
                rx="6"
              />
              <text x={X_SVC + 10} y={10 + i * ROW + 17}>{s.metadata.name}</text>
              <text class="sub" x={X_SVC + 10} y={10 + i * ROW + 32}>{svcNote(s)}</text>
            </a>
          {/each}
          {#each volumeNames as v, i (v)}
            <a href={link('Volume', v)} aria-label={t('Volume {name}: {note}', { name: v, note: volumeNote(v) })}>
              <rect class="box vol {volumeHealth(v)}" x={X_VOL} y={10 + i * ROW} width={BOX_W} height={BOX_H} rx="6" />
              <text x={X_VOL + 10} y={10 + i * ROW + 17}>{v}</text>
              <text class="sub" x={X_VOL + 10} y={10 + i * ROW + 32}>{volumeNote(v)}</text>
            </a>
          {/each}
        </svg>
      </div>
    </div>

    {#if inspectedSvc}
      <aside class="card pf-inspector" aria-label={t('Packet filter inspector')}>
        <h2>
          <a href={link('Service', inspectedSvc.metadata.name)}>{inspectedSvc.metadata.name}</a>
        </h2>
        <p class="muted">{svcNote(inspectedSvc)}</p>

        <h3>{t('Inbound PF allow rules')}</h3>
        {#if inboundEdges.length}
          <ul class="plain mono">
            {#each inboundEdges as e (`${e.from}->${e.to}`)}
              <li>← {e.from || t('other namespaces')}: {ports(e)}</li>
            {/each}
          </ul>
        {:else}
          <p class="muted">{t('Isolated: no inbound peers allowed.')}</p>
        {/if}

        <h3>{t('Outbound PF allow rules')}</h3>
        {#if outboundEdges.length}
          <ul class="plain mono">
            {#each outboundEdges as e (`${e.from}->${e.to}`)}
              <li>→ {e.to}: {ports(e)}</li>
            {/each}
          </ul>
        {:else}
          <p class="muted">{t('No intra-namespace outbound edges.')}</p>
        {/if}

        {#if svcVips.length}
          <h3>{t('CARP VIPs')}</h3>
          <ul class="plain mono">
            {#each svcVips as b (b.address)}
              <li>{b.address} (vhid {b.vhid})</li>
            {/each}
          </ul>
        {/if}

        {#if svcRoutes.length}
          <h3>{t('Ingress Routes')}</h3>
          <ul class="plain">
            {#each svcRoutes as r (r.metadata.name)}
              <li>
                <a href={link('Route', r.metadata.name)}>
                  {r.spec?.host ?? ''}{r.spec?.path ?? '/'}
                </a>
              </li>
            {/each}
          </ul>
        {/if}

        {#if svcVols.length}
          <h3>{t('Mounted Volumes')}</h3>
          <ul class="plain">
            {#each svcVols as v (v)}
              <li><a href={link('Volume', v)}>{v}</a></li>
            {/each}
          </ul>
        {/if}
      </aside>
    {/if}
  </div>
{:else if !error}
  <p class="muted">{t('No services in {namespace}.', { namespace })}</p>
{/if}

<style>
  .topo-layout {
    display: grid;
    grid-template-columns: minmax(0, 1fr) 20rem;
    gap: 1rem;
    align-items: start;
  }
  @media (max-width: 64rem) {
    .topo-layout {
      grid-template-columns: 1fr;
    }
  }
  .pf-inspector {
    border-right: 1px solid var(--line);
    background: var(--panel);
  }
  .topology {
    display: block;
    overflow: visible;
  }
  .box {
    fill: var(--panel);
    stroke: var(--control);
    stroke-width: 1.5;
  }
  .box.good {
    stroke: var(--good);
  }
  .box.wait {
    stroke: var(--wait);
  }
  .box.bad {
    stroke: var(--bad);
  }
  .box.picked {
    stroke-width: 2.8;
  }
  .box.idle,
  .box.outside {
    stroke-dasharray: 4 3;
  }
  text {
    fill: var(--text);
    font-size: 13px;
  }
  text.sub {
    fill: var(--muted);
    font-size: 11px;
  }
  .edge {
    stroke: var(--muted);
    stroke-width: 1.2;
    fill: none;
  }
  .edge.reach {
    stroke: var(--accent);
    stroke-dasharray: 5 3;
  }
  .head {
    fill: var(--muted);
  }
  a:focus-visible rect {
    stroke-width: 3;
  }
</style>

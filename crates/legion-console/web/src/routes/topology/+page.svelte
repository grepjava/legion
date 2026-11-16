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
  let tasks: LObject[] = $state([]);
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
      follow('Task', ns, (o) => (tasks = o), failed),
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

  const ROW = 112;
  const HEADER_H = 34;
  const X_ROUTE = 24;
  const ROUTE_W = 168;
  const X_SVC_CX = 300;
  const SVC_R = 30;
  const X_TASK = 440;
  const TASK_W = 188;
  const TASK_H = 44;
  const X_VOL = 700;
  const VOL_W = 172;
  const BOX_H = 46;

  const svcRow = $derived(new Map(services.map((s, i) => [s.metadata.name, i])));
  const activeTasks = $derived(tasks.filter((tk) => tk.status?.state !== 'shutdown'));
  const tasksBySvc = $derived.by(() => {
    const m = new Map<string, LObject[]>();
    for (const tk of activeTasks) {
      const s = tk.spec?.service;
      if (!s) continue;
      const list = m.get(s) ?? [];
      list.push(tk);
      m.set(s, list);
    }
    return m;
  });
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
  const svgW = X_VOL + VOL_W + 28;
  const height = $derived(
    HEADER_H + Math.max(services.length, volumeNames.length, leftRows, 1) * ROW + 36
  );

  const y = (row: number) => HEADER_H + row * ROW + ROW / 2;
  const isVmSvc = (s: LObject) =>
    Boolean((s.spec?.pod?.containers ?? []).some((c: { vm?: unknown }) => c.vm));
  const isDbSvc = (s: LObject) =>
    /postgres|mysql|maria|redis|mongo|sqlite|db/i.test(
      `${s.metadata.name} ${s.spec?.pod?.containers?.[0]?.image ?? ''}`
    );
  const svcPorts = (s: LObject) => {
    const list: number[] = [];
    for (const c of s.spec?.pod?.containers ?? []) {
      for (const p of c.ports ?? []) {
        if (p.containerPort) list.push(p.containerPort);
      }
    }
    return list;
  };
  const taskHealth = (tk: LObject) => {
    const st = tk.status?.state;
    if (st === 'running') return 'good';
    if (st === 'failed' || st === 'rejected') return 'bad';
    return 'wait';
  };
  const arc = (from: number, to: number) => {
    const x = X_SVC_CX + SVC_R;
    const bulge = Math.min(110, 34 + Math.abs(to - from) * 24);
    return `M ${x} ${y(from) - 8} C ${x + bulge} ${y(from) - 8}, ${x + bulge} ${y(to) + 8}, ${x + 8} ${y(to) + 8}`;
  };
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
      <div class="table-scroll topo-canvas" tabindex="0" role="region" aria-label={t('Topology graph')}>
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

          <!-- Tier Headers -->
          <text class="tier-head" x={X_ROUTE} y={20}>{t('INGRESS / ROUTES')}</text>
          <text class="tier-head" x={X_SVC_CX - 34} y={20}>{t('SERVICES')}</text>
          <text class="tier-head" x={X_TASK} y={20}>{t('PODS (JAILS / VMS)')}</text>
          <text class="tier-head" x={X_VOL} y={20}>{t('VOLUMES')}</text>

          <!-- Route -> Service edges -->
          {#each shownRoutes as r, i (r.metadata.name)}
            <path
              class="edge"
              d="M {X_ROUTE + ROUTE_W} {y(i)} C {X_ROUTE + ROUTE_W + 45} {y(i)}, {X_SVC_CX - SVC_R - 45} {y(svcRow.get(r.spec.service)!)}, {X_SVC_CX - SVC_R - 6} {y(svcRow.get(r.spec.service)!)}"
              marker-end="url(#arrow)"
            />
          {/each}
          {#if outside.length}
            {#each outside as e (e.to)}
              <path
                class="edge reach"
                d="M {X_ROUTE + ROUTE_W} {y(shownRoutes.length)} C {X_ROUTE + ROUTE_W + 45} {y(shownRoutes.length)}, {X_SVC_CX - SVC_R - 45} {y(svcRow.get(e.to)!)}, {X_SVC_CX - SVC_R - 6} {y(svcRow.get(e.to)!)}"
                marker-end="url(#arrow)"
              >
                <title>{t('other namespaces → {service}: {ports}', { service: e.to, ports: ports(e) })}</title>
              </path>
            {/each}
          {/if}

          <!-- Service <-> Service PF reachability arcs -->
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

          <!-- Service -> Task (Pod / VM) edges -->
          {#each services as s, i (s.metadata.name)}
            {@const svcTasks = tasksBySvc.get(s.metadata.name) ?? []}
            {#each svcTasks as tk, ti (tk.metadata.name)}
              {@const ty = y(i) + (ti - (svcTasks.length - 1) / 2) * 52}
              <path
                class="edge pod-link"
                d="M {X_SVC_CX + SVC_R} {y(i)} C {X_SVC_CX + SVC_R + 36} {y(i)}, {X_TASK - 36} {ty}, {X_TASK} {ty}"
              />
            {/each}
          {/each}

          <!-- Service -> Volume edges -->
          {#each used as u (`${u.service}>${u.volume}`)}
            <path
              class="edge vol-link"
              d="M {X_TASK + TASK_W} {y(svcRow.get(u.service)!)} C {X_TASK + TASK_W + 30} {y(svcRow.get(u.service)!)}, {X_VOL - 30} {y(volRow.get(u.volume)!)}, {X_VOL} {y(volRow.get(u.volume)!)}"
            />
          {/each}

          <!-- Route Nodes with Globe Icon -->
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
              <rect class="box route" x={X_ROUTE} y={y(i) - BOX_H / 2} width={ROUTE_W} height={BOX_H} rx="8" />
              <circle cx={X_ROUTE + 18} cy={y(i)} r="10" class="icon-bg route-bg" />
              <circle cx={X_ROUTE + 18} cy={y(i)} r="5.5" class="icon-stroke" />
              <line x1={X_ROUTE + 12.5} y1={y(i)} x2={X_ROUTE + 23.5} y2={y(i)} class="icon-stroke" />
              <ellipse cx={X_ROUTE + 18} cy={y(i)} rx="2.6" ry="5.5" class="icon-stroke" />
              <text x={X_ROUTE + 34} y={y(i) - 3}>{r.spec.host}{r.spec.path && r.spec.path !== '/' ? r.spec.path : ''}</text>
              <text class="sub" x={X_ROUTE + 34} y={y(i) + 12}>{t('Route {name}', { name: r.metadata.name })}</text>
            </a>
          {/each}
          {#if outside.length}
            <rect class="box outside" x={X_ROUTE} y={y(shownRoutes.length) - BOX_H / 2} width={ROUTE_W} height={BOX_H} rx="8" />
            <text x={X_ROUTE + 14} y={y(shownRoutes.length) + 4}>{t('other namespaces')}</text>
          {/if}

          <!-- OpenShift-style Circular Service Nodes with Pod Ring & Workload Icon -->
          {#each services as s, i (s.metadata.name)}
            {@const cy = y(i)}
            {@const h = health(s)}
            {@const vm = isVmSvc(s)}
            {@const db = isDbSvc(s)}
            {@const pList = svcPorts(s)}
            <a
              href={link('Service', s.metadata.name)}
              aria-label={t('Service {name}: {note}', { name: s.metadata.name, note: svcNote(s) })}
            >
              <circle
                class="svc-halo {h}"
                class:picked={inspectedSvc?.metadata.name === s.metadata.name}
                cx={X_SVC_CX}
                cy={cy}
                r={SVC_R + 5}
              />
              <circle class="svc-ring {h}" cx={X_SVC_CX} cy={cy} r={SVC_R} />
              <circle class="svc-core" cx={X_SVC_CX} cy={cy} r={SVC_R - 6} />

              {#if vm}
                <!-- bhyve VM monitor icon -->
                <rect x={X_SVC_CX - 10} y={cy - 9} width="20" height="14" rx="2" class="icon-stroke" />
                <line x1={X_SVC_CX - 5} y1={cy + 9} x2={X_SVC_CX + 5} y2={cy + 9} class="icon-stroke" />
                <line x1={X_SVC_CX} y1={cy + 5} x2={X_SVC_CX} y2={cy + 9} class="icon-stroke" />
              {:else if db}
                <!-- Database cylinder icon -->
                <ellipse cx={X_SVC_CX} cy={cy - 7} rx="9" ry="3.5" class="icon-stroke" />
                <path d="M {X_SVC_CX - 9} {cy - 7} v 14 c 0 2 4 3.5 9 3.5 s 9 -1.5 9 -3.5 v -14" class="icon-stroke" />
                <path d="M {X_SVC_CX - 9} {cy} c 0 2 4 3.5 9 3.5 s 9 -1.5 9 -3.5" class="icon-stroke" />
              {:else}
                <!-- FreeBSD Jail cube/pod icon -->
                <polygon points="{X_SVC_CX},{cy - 11} {X_SVC_CX + 10},{cy - 5} {X_SVC_CX + 10},{cy + 6} {X_SVC_CX},{cy + 12} {X_SVC_CX - 10},{cy + 6} {X_SVC_CX - 10},{cy - 5}" class="icon-stroke" />
                <polyline points="{X_SVC_CX - 10},{cy - 5} {X_SVC_CX},{cy + 1} {X_SVC_CX + 10},{cy - 5}" class="icon-stroke" />
                <line x1={X_SVC_CX} y1={cy + 1} x2={X_SVC_CX} y2={cy + 12} class="icon-stroke" />
              {/if}

              <!-- Ready badge pill -->
              <rect x={X_SVC_CX + 14} y={cy - 34} width="32" height="16" rx="8" class="badge-pill {h}" />
              <text class="badge-text" x={X_SVC_CX + 30} y={cy - 22} text-anchor="middle">
                {s.status?.ready ?? 0}/{s.status?.desired ?? 0}
              </text>

              <!-- Service label & ports -->
              <text class="svc-title" x={X_SVC_CX} y={cy + SVC_R + 18} text-anchor="middle">
                {s.metadata.name}{pList.length ? ` :${pList[0]}` : ''}
              </text>
            </a>
          {/each}

          <!-- Task (Jail Pod / bhyve VM) Nodes -->
          {#each services as s, i (s.metadata.name)}
            {@const svcTasks = tasksBySvc.get(s.metadata.name) ?? []}
            {#each svcTasks as tk, ti (tk.metadata.name)}
              {@const ty = y(i) + (ti - (svcTasks.length - 1) / 2) * 52}
              {@const th = taskHealth(tk)}
              {@const vm = isVmSvc(tk)}
              <a
                href={link('Task', tk.metadata.name)}
                aria-label={t('Task {name}: {state}', { name: tk.metadata.name, state: tk.status?.state ?? 'unknown' })}
              >
                <rect class="box task {th}" x={X_TASK} y={ty - TASK_H / 2} width={TASK_W} height={TASK_H} rx="8" />
                <circle cx={X_TASK + 16} cy={ty} r="5" class="dot-status {th}" />
                <text x={X_TASK + 28} y={ty - 3}>{tk.metadata.name}</text>
                <text class="sub" x={X_TASK + 28} y={ty + 12}>
                  {vm ? 'bhyve' : 'jail'} · {tk.status?.ip ?? tk.status?.state ?? ''} · {tk.spec?.node ?? ''}
                </text>
              </a>
            {/each}
          {/each}

          <!-- Volume Nodes with Storage Icon -->
          {#each volumeNames as v, i (v)}
            {@const vh = volumeHealth(v)}
            <a href={link('Volume', v)} aria-label={t('Volume {name}: {note}', { name: v, note: volumeNote(v) })}>
              <rect class="box vol {vh}" x={X_VOL} y={y(i) - BOX_H / 2} width={VOL_W} height={BOX_H} rx="8" />
              <ellipse cx={X_VOL + 18} cy={y(i) - 5} rx="7" ry="2.8" class="icon-stroke" />
              <path d="M {X_VOL + 11} {y(i) - 5} v 10 c 0 1.6 3.1 2.8 7 2.8 s 7 -1.2 7 -2.8 v -10" class="icon-stroke" />
              <text x={X_VOL + 32} y={y(i) - 3}>{v}</text>
              <text class="sub" x={X_VOL + 32} y={y(i) + 12}>{volumeNote(v)}</text>
            </a>
          {/each}
        </svg>
      </div>
      <div class="topo-legend muted">
        <span><span class="leg-dot good"></span>{t('Ready / Running')}</span>
        <span><span class="leg-dot wait"></span>{t('Starting / Partial')}</span>
        <span><span class="leg-dot bad"></span>{t('Failed / Lost')}</span>
        <span>{t('Click any Service, Pod/VM, Route, or Volume to drill down.')}</span>
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
    height: auto;
    overflow: visible;
  }
  .topo-canvas {
    margin-top: 0.75rem;
    padding: 0.5rem;
    border: 1px solid var(--line);
    border-radius: 6px;
    background-color: var(--bg);
    background-image: radial-gradient(var(--line) 1px, transparent 1px);
    background-size: 18px 18px;
  }
  .topology {
    display: block;
    overflow: visible;
  }
  .tier-head {
    fill: var(--muted);
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 0.08em;
  }
  .svc-halo {
    fill: none;
    stroke: transparent;
    stroke-width: 2;
  }
  .svc-halo.picked {
    stroke: var(--link);
    stroke-dasharray: 4 3;
  }
  .canvas-card .controls button.primary {
    background: var(--link);
    border-color: var(--link);
    color: light-dark(#ffffff, #06112e);
  }
  .svc-ring {
    fill: var(--panel);
    stroke: var(--control);
    stroke-width: 3.5;
  }
  .svc-ring.good {
    stroke: var(--good);
  }
  .svc-ring.wait {
    stroke: var(--wait);
  }
  .svc-ring.bad {
    stroke: var(--bad);
  }
  .svc-core {
    fill: var(--surface2);
  }
  .icon-bg.route-bg {
    fill: var(--surface2);
  }
  .icon-stroke {
    fill: none;
    stroke: var(--text);
    stroke-width: 1.5;
    stroke-linecap: round;
    stroke-linejoin: round;
  }
  .badge-pill {
    fill: var(--surface2);
    stroke: var(--line);
    stroke-width: 1;
  }
  .badge-pill.good {
    fill: var(--good);
    stroke: var(--good);
  }
  .badge-pill.wait {
    fill: var(--wait);
    stroke: var(--wait);
  }
  .badge-pill.bad {
    fill: var(--bad);
    stroke: var(--bad);
  }
  .badge-text {
    fill: #ffffff;
    font-size: 10px;
    font-weight: 700;
  }
  .svc-title {
    fill: var(--text);
    font-size: 12.5px;
    font-weight: 600;
  }
  .dot-status {
    fill: var(--control);
  }
  .dot-status.good {
    fill: var(--good);
  }
  .dot-status.wait {
    fill: var(--wait);
  }
  .dot-status.bad {
    fill: var(--bad);
  }
  .edge.pod-link {
    stroke: var(--control);
    stroke-width: 1.5;
  }
  .edge.vol-link {
    stroke: var(--zfs);
    stroke-width: 1.4;
    stroke-dasharray: 3 3;
  }
  .topo-legend {
    display: flex;
    flex-wrap: wrap;
    gap: 1rem;
    align-items: center;
    margin-top: 0.65rem;
    font-size: 0.8rem;
  }
  .leg-dot {
    display: inline-block;
    width: 9px;
    height: 9px;
    border-radius: 50%;
    margin-right: 0.35rem;
  }
  .leg-dot.good {
    background: var(--good);
  }
  .leg-dot.wait {
    background: var(--wait);
  }
  .leg-dot.bad {
    background: var(--bad);
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
    stroke: var(--link);
    stroke-dasharray: 5 3;
  }
  .head {
    fill: var(--muted);
  }
  a:focus-visible rect {
    stroke-width: 3;
  }
</style>

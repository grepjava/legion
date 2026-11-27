<script lang="ts">
  // Interactive Topology & PF Reachability Inspector: visualises a
  // namespace's Routes, Services, PF packet-filter reachability arcs, CARP
  // VIPs, and ZFS/NFS/iSCSI Volumes with zoom controls and click-to-inspect
  // packet filter rules.
  import {
    api,
    follow,
    followConnections,
    followLoadBalancers,
    followStatus,
    type Balancer,
    type ConnectionEdge,
    type LObject,
    type ReachEdge
  } from '#lib/api.ts';
  import { t, tn } from '#lib/i18n.svelte.ts';
  import { cannotWrite, namespaces, session } from '#lib/session.svelte.ts';
  import ContextMenu, { type MenuItem } from '#lib/ContextMenu.svelte';
  import {
    moveService,
    moveVolume,
    nodeMenu,
    serviceMenu,
    taskMenu,
    volumeMenu,
    type Scene,
    portMenu,
    routeMenu
  } from '#lib/topology-menu.svelte.ts';
  import {
    BAND_HEAD,
    BAND_W,
    BAND_X,
    BOX_H,
    HEADER_H,
    NODE_HUES,
    ROUTE_W,
    ROW,
    SVC_R,
    TASK_H,
    TASK_STEP,
    TASK_W,
    VOL_W,
    X_ROUTE,
    X_SVC_CX,
    X_TASK,
    X_VOL,
    conns,
    containerPorts,
    currentTasks,
    isVm,
    nodeBands,
    nodeHealth,
    nodeNote,
    portShown,
    portWhere,
    ports,
    publishedPorts,
    routeAddress,
    seenSummary,
    serviceHealth,
    svcNote,
    taskHealth,
    tops,
    volumeUses,
    width,
    type Band
  } from '#lib/topology.ts';
  import TopologyInspector from '#lib/TopologyInspector.svelte';
  import TopologyServiceIcon from '#lib/TopologyServiceIcon.svelte';

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
  let nodes: LObject[] = $state([]);
  let selectedService = $state('');
  // Pods drawn under their services, or in a band for each node; kept for
  // the next visit.
  const GROUP_KEY = 'legion.topology-group';
  let groupBy: 'service' | 'node' = $state(
    typeof localStorage !== 'undefined' && localStorage.getItem(GROUP_KEY) === 'node' ? 'node' : 'service'
  );
  function setGroup(g: 'service' | 'node') {
    groupBy = g;
    try {
      localStorage.setItem(GROUP_KEY, g);
    } catch {
      // Private mode: kept for this page only.
    }
  }
  const byNode = $derived(groupBy === 'node');
  // Arcs between services for what may connect, or for the connections
  // the nodes saw; kept for the next visit.
  const ARCS_KEY = 'legion.topology-arcs';
  let arcs: 'allowed' | 'seen' = $state(
    typeof localStorage !== 'undefined' && localStorage.getItem(ARCS_KEY) === 'allowed' ? 'allowed' : 'seen'
  );
  function setArcs(a: 'allowed' | 'seen') {
    arcs = a;
    try {
      localStorage.setItem(ARCS_KEY, a);
    } catch {
      // Private mode: kept for this page only.
    }
  }
  const showSeen = $derived(arcs === 'seen');
  let zoom = $state(100);
  const zoomBy = (step: number) => (zoom = Math.min(160, Math.max(60, zoom + step)));

  // The canvas panned by dragging it, as a map is; a press that does not
  // move stays a click on what is under it.
  let canvas: HTMLDivElement | undefined = $state();
  let panning = $state(false);
  let press: { x: number; y: number; left: number; top: number; id: number } | null = null;
  // A Service or Volume pressed on is dragged instead: dropped on a node's
  // band, it moves there, as its menu's Move to does (asked first).
  let drag: { kind: 'Service' | 'Volume'; name: string } | null = $state(null);
  let dragging = $state(false);
  let ghost = $state({ x: 0, y: 0 });
  let dropOn = $state('');
  function panDown(e: PointerEvent) {
    if (e.button !== 0 || !canvas) return;
    const item = (e.target as Element).closest<HTMLElement | SVGElement>('[data-drag]');
    drag =
      item && !cannotWrite(namespace)
        ? { kind: item.dataset.drag as 'Service' | 'Volume', name: item.dataset.name ?? '' }
        : null;
    press = { x: e.clientX, y: e.clientY, left: canvas.scrollLeft, top: canvas.scrollTop, id: e.pointerId };
  }
  function panMove(e: PointerEvent) {
    if (!press || !canvas || e.pointerId !== press.id) return;
    const dx = e.clientX - press.x;
    const dy = e.clientY - press.y;
    if (!panning && !dragging && Math.hypot(dx, dy) < 5) return;
    if (drag) {
      if (!dragging) {
        dragging = true;
        canvas.setPointerCapture(e.pointerId);
      }
      ghost = { x: e.clientX, y: e.clientY };
      const under = document.elementFromPoint(e.clientX, e.clientY)?.closest<SVGElement>('[data-node]');
      dropOn = under?.dataset.node ?? '';
      return;
    }
    if (!panning) {
      panning = true;
      canvas.setPointerCapture(e.pointerId);
    }
    canvas.scrollLeft = press.left - dx;
    canvas.scrollTop = press.top - dy;
  }
  function panUp(e: PointerEvent) {
    if (!press || e.pointerId !== press.id) return;
    press = null;
    if (dragging && drag) {
      const { kind, name } = drag;
      const to = dropOn;
      drag = null;
      dragging = false;
      dropOn = '';
      canvas?.addEventListener('click', (c) => (c.preventDefault(), c.stopPropagation()), {
        capture: true,
        once: true
      });
      if (to && kind === 'Service') {
        const s = services.find((x) => x.metadata.name === name);
        if (s) void moveService(s, to, scene);
      } else if (to && kind === 'Volume' && to !== volumeNode(name)) {
        void moveVolume(name, to, scene);
      }
      return;
    }
    drag = null;
    if (!panning) return;
    // The click that ends a drag opens nothing.
    canvas?.addEventListener('click', (c) => (c.preventDefault(), c.stopPropagation()), {
      capture: true,
      once: true
    });
    requestAnimationFrame(() => (panning = false));
  }
  // Ctrl (or a pinch) with the wheel zooms, as elsewhere.
  function wheel(e: WheelEvent) {
    if (!e.ctrlKey) return;
    e.preventDefault();
    zoomBy(e.deltaY < 0 ? 10 : -10);
  }
  // Not passive, as the page must not scroll or zoom as well.
  $effect(() => {
    const c = canvas;
    if (!c) return;
    c.addEventListener('wheel', wheel, { passive: false });
    return () => c.removeEventListener('wheel', wheel);
  });
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
    // Nodes are the cluster's: only those who see everything list them;
    // others still see each pod's node by name.
    if (session.me?.all) stops.push(follow('Node', '', (o) => (nodes = o), () => (nodes = [])));
    return () => stops.forEach((s) => s());
  });

  // The connections the nodes' pf keeps, followed: the leader asks every
  // node again each 10 s and sends them when they changed.
  let seen: ConnectionEdge[] = $state([]);
  let unanswered: { node: string; error: string }[] = $state([]);
  let seenError = $state('');
  $effect(() => {
    const ns = namespace;
    seen = [];
    unanswered = [];
    return followConnections(
      ns,
      (r) => {
        seen = r.edges;
        unanswered = r.unanswered;
        seenError = '';
      },
      (why) => (seenError = why)
    );
  });
  // Reach is read again when services change; the CARP addresses are
  // followed, as CARP moves them.
  const names = $derived(services.map((s) => s.metadata.name).join(','));
  $effect(() => {
    const ns = namespace;
    void names;
    api
      .reach(ns)
      .then((r) => (edges = r.edges))
      .catch((e) => (error = e.message));
  });
  $effect(() => {
    const ns = namespace;
    balancers = [];
    return followLoadBalancers(ns, (b) => (balancers = b));
  });

  const svcRow = $derived(new Map(services.map((s, i) => [s.metadata.name, i])));
  const activeTasks = $derived(currentTasks(tasks));
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
  const used = $derived(volumeUses(services));
  const volumeNames = $derived(
    [...new Set([...volumes.map((v) => v.metadata.name), ...used.map((u) => u.volume)])].sort()
  );
  const volRow = $derived(new Map(volumeNames.map((v, i) => [v, i])));
  const shownRoutes = $derived(routes.filter((r) => svcRow.has(r.spec?.service)));
  const published = $derived(publishedPorts(services));
  const outside = $derived(edges.filter((e) => e.from === '' && svcRow.has(e.to)));
  const between = $derived(
    edges.filter((e) => e.from !== '' && e.from !== e.to && svcRow.has(e.from) && svcRow.has(e.to))
  );
  // What the nodes saw: between two services, and into each from anything
  // but another of them.
  const seenSummed = $derived(seenSummary(seen, (s) => svcRow.has(s)));
  const seenBetween = $derived(seenSummed.between);
  const seenIn = $derived(seenSummed.into);

  // All may reach all: one note rather than an arc each way between every
  // two of them, however many there are.
  const everyone = $derived(
    services.length >= 2 && between.length === services.length * (services.length - 1)
  );
  // The left column: Routes, then published ports, then other namespaces.
  const outsideRow = $derived(shownRoutes.length + published.length);
  const leftRows = $derived(outsideRow + ((showSeen ? seenIn.length : outside.length) ? 1 : 0));
  // Without Volumes, no column for them: the graph ends with the pods.
  const svgW = $derived(volumeNames.length ? X_VOL + VOL_W + 28 : X_TASK + TASK_W + 28);
  const rowCount = $derived(Math.max(services.length, volumeNames.length, leftRows, 1));
  // Each row's height, and where each starts: routes, services and
  // Volumes share the rows. Pods grow their service's row only when drawn
  // beside it.
  const rowHeights = $derived(
    Array.from({ length: rowCount }, (_, i) => {
      const n = byNode ? 0 : (tasksBySvc.get(services[i]?.metadata.name ?? '')?.length ?? 0);
      return Math.max(ROW, n * TASK_STEP + 24);
    })
  );
  const rowTops = $derived(tops(rowHeights));

  const y = (row: number) => (rowTops[row] ?? HEADER_H + row * ROW) + (rowHeights[row] ?? ROW) / 2;
  const taskY = (row: number, ti: number, n: number) => y(row) + (ti - (n - 1) / 2) * TASK_STEP;

  // The cluster's nodes, and those the namespace's pods name, in order;
  // each with a colour of its own, so a pod's node shows in either view.
  const nodeNames = $derived.by(() => {
    const names = new Set(nodes.map((n) => n.metadata.name as string));
    for (const tk of activeTasks) if (tk.spec?.node) names.add(tk.spec.node);
    return [...names].sort();
  });
  const multiNode = $derived(nodeNames.length > 1);
  const nodeHue = (name: string) => {
    const i = nodeNames.indexOf(name);
    return i < 0 ? '' : `n${i % NODE_HUES}`;
  };
  const nodeOf = (name: string) => nodes.find((n) => n.metadata.name === name);

  // What the menus need: the nodes that take tasks, the managers and which
  // leads.
  let leader = $state('');
  $effect(() => followStatus((st) => (leader = st.leaderName ?? '')));
  const scene: Scene = $derived({
    namespace,
    nodes: nodes
      .filter((n) => n.status?.state === 'ready' && (n.spec?.availability ?? 'active') === 'active')
      .map((n) => n.metadata.name as string)
      .sort(),
    leader,
    managers: nodes.filter((n) => n.spec?.role === 'manager').map((n) => n.metadata.name as string),
    services
  });
  let menu: { x: number; y: number; items: MenuItem[]; label: string } | null = $state(null);
  /** Right-click: the menu at the pointer. */
  function openMenu(e: MouseEvent, items: MenuItem[], label: string) {
    e.preventDefault();
    e.stopPropagation();
    menu = { x: e.clientX, y: e.clientY, items, label };
  }
  /** The menu key or Shift+F10 on what has focus: the menu beside it. */
  function menuKey(e: KeyboardEvent, items: () => MenuItem[], label: string) {
    if (!(e.key === 'ContextMenu' || (e.shiftKey && e.key === 'F10'))) return;
    e.preventDefault();
    const r = (e.currentTarget as Element).getBoundingClientRect();
    menu = { x: r.left + Math.min(r.width, 40), y: r.top + r.height / 2, items: items(), label };
  }

  // Node bands, in the pods' column: each node's pods, then those not
  // placed on one yet.
  const bands: Band[] = $derived(byNode ? nodeBands(activeTasks, nodeNames, svcRow) : []);
  // Where each pod is drawn: beside its service, or in its node's band.
  const taskPos = $derived.by(() => {
    const at = new Map<string, number>();
    if (byNode) {
      for (const b of bands) {
        b.tasks.forEach((tk, j) => at.set(tk.metadata.name, b.top + BAND_HEAD + j * TASK_STEP + TASK_STEP / 2));
      }
    } else {
      services.forEach((s, i) => {
        const list = tasksBySvc.get(s.metadata.name) ?? [];
        list.forEach((tk, ti) => at.set(tk.metadata.name, taskY(i, ti, list.length)));
      });
    }
    return at;
  });
  const shownTasks = $derived(activeTasks.filter((tk) => taskPos.has(tk.metadata.name)));
  const bandsEnd = $derived(bands.length ? bands[bands.length - 1].top + bands[bands.length - 1].h : 0);
  const height = $derived(Math.max(HEADER_H + rowHeights.reduce((a, h) => a + h, 0), bandsEnd) + 36);

  // A ZFS Volume's data is on one node: a pod on another cannot mount it
  // there (until it is moved). The node it is on, when that is so.
  const elsewhere = (volume: string, tk: LObject) => {
    const v = volumes.find((v) => v.metadata.name === volume);
    const on: string = v?.status?.node ?? '';
    const driver = v?.spec?.driver ?? 'zfs';
    const at: string = tk.spec?.node ?? '';
    return driver === 'zfs' && on && at && on !== at && !v?.status?.migration ? on : '';
  };
  // Lines to each Volume: from each pod using it, or from the service
  // itself while it has none.
  const volLinks = $derived(
    used.flatMap((u) => {
      const y1 = y(volRow.get(u.volume)!);
      const list = (tasksBySvc.get(u.service) ?? []).filter((tk) => taskPos.has(tk.metadata.name));
      if (!list.length) {
        return [{ key: `${u.service}>${u.volume}`, x0: X_SVC_CX + SVC_R, y0: y(svcRow.get(u.service)!), y1, far: '', title: '' }];
      }
      return list.map((tk) => {
        const far = elsewhere(u.volume, tk);
        return {
          key: `${tk.metadata.name}>${u.volume}`,
          x0: X_TASK + TASK_W,
          y0: taskPos.get(tk.metadata.name)!,
          y1,
          far,
          title: far
            ? t('{volume} is on {on}, {task} on {node}', {
                volume: u.volume,
                on: far,
                task: tk.metadata.name,
                node: tk.spec?.node ?? ''
              })
            : ''
        };
      });
    })
  );
  const arc = (from: number, to: number) => {
    const x = X_SVC_CX + SVC_R;
    const bulge = Math.min(110, 34 + Math.abs(to - from) * 24);
    return `M ${x} ${y(from) - 8} C ${x + bulge} ${y(from) - 8}, ${x + bulge} ${y(to) + 8}, ${x + 8} ${y(to) + 8}`;
  };
  const volumeHealth = (name: string) => {
    const v = volumes.find((v) => v.metadata.name === name);
    if (!v) return 'bad';
    return v.status?.lost ? 'bad' : 'good';
  };
  const volumeNode = (name: string): string =>
    volumes.find((v) => v.metadata.name === name)?.status?.node ?? '';
  const volumeNote = (name: string) => {
    const v = volumes.find((v) => v.metadata.name === name);
    if (!v) return t('missing');
    if (v.status?.lost) return t('lost');
    return v.status?.node ? t('Volume on {node}', { node: v.status.node }) : t('Volume');
  };
  // What a task shows on its second line, and so what its link is named
  // by first.
  const taskLine = (tk: LObject) =>
    `${isVm(tk) ? 'bhyve' : 'jail'} · ${tk.status?.paused ? 'paused' : (tk.status?.ip ?? tk.status?.state ?? '')}${
      byNode ? '' : ` · ${tk.spec?.node ?? ''}`
    }`;
  const link = (kind: string, name: string) => `/k/${kind}/${namespace}/${name}`;

  // The service picked for the panel, ringed; until one is, the panel
  // shows the first, unringed.
  const picked = $derived(services.some((s) => s.metadata.name === selectedService) ? selectedService : '');
  const inspectedSvc = $derived(
    services.find((s) => s.metadata.name === selectedService) ?? services[0] ?? null
  );
</script>

<header class="bar">
  <h1>{t('Topology: {namespace}', { namespace })}</h1>
  <div class="controls">
    <button type="button" class="sm" aria-label={t('Zoom out')} title={t('Zoom out (Ctrl+wheel)')} onclick={() => zoomBy(-20)}>−</button>
    <span class="muted" aria-live="polite">{zoom}%</span>
    <button type="button" class="sm" aria-label={t('Zoom in')} title={t('Zoom in (Ctrl+wheel)')} onclick={() => zoomBy(20)}>+</button>
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
    "Routes to the namespace's Services, what may reach what among them as the nodes enforce it, or the connections the nodes saw, and the Volumes they use. A ring or box turns amber when some tasks are not ready, red when none are or a Volume is lost."
  )}
</p>
{#if error}<p class="error" role="alert">{error}</p>{/if}

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
            aria-pressed={(inspectedSvc?.metadata.name ?? '') === s.metadata.name}
            onclick={() => (selectedService = selectedService === s.metadata.name ? '' : s.metadata.name)}
          >
            {s.metadata.name}
          </button>
        {/each}
      </div>
      <div class="controls">
        <span class="muted" id="group-by">{t('Group pods by:')}</span>
        <div class="segmented" role="group" aria-labelledby="group-by">
          <button type="button" aria-pressed={!byNode} onclick={() => setGroup('service')}>
            {t('Service')}
          </button>
          <button type="button" aria-pressed={byNode} onclick={() => setGroup('node')}>
            {t('Node')}
          </button>
        </div>
        <span class="muted" id="arcs-of">{t('Arcs:')}</span>
        <div class="segmented" role="group" aria-labelledby="arcs-of">
          <button type="button" aria-pressed={!showSeen} onclick={() => setArcs('allowed')}>
            {t('Allowed')}
          </button>
          <button type="button" aria-pressed={showSeen} onclick={() => setArcs('seen')}>
            {t('Seen')}
          </button>
        </div>
        {#if multiNode && !byNode}
          <span class="node-key muted" aria-label={t('Node colours')}>
            {#each nodeNames as n (n)}
              <span><span class="key-swatch {nodeHue(n)}"></span>{n}</span>
            {/each}
          </span>
        {/if}
      </div>
      {#if showSeen}
        {#if seenError}
          <p class="error note" role="alert">{t('Connections: {error}', { error: seenError })}</p>
        {/if}
        {#each unanswered as u (u.node)}
          <p class="muted note">{t('Node {node} did not say its connections: {error}', { node: u.node, error: u.error })}</p>
        {/each}
        {#if !seenError && !seenBetween.length && !seenIn.length}
          <p class="muted note">{t('No connections of these services seen in the last minute and a half.')}</p>
        {/if}
      {:else if everyone}
        <p class="muted note">
          {t('Every service may reach every other (the namespace is not isolated by policy).')}
        </p>
      {/if}

      <!-- svelte-ignore a11y_no_noninteractive_tabindex -->
      <div
        class="table-scroll topo-canvas pannable"
        class:panning
        bind:this={canvas}
        tabindex="0"
        role="region"
        aria-label={t('Topology graph')}
        onpointerdown={panDown}
        onpointermove={panMove}
        onpointerup={panUp}
        onpointercancel={panUp}
        title={t('Drag to move around; drag a Service or Volume onto a node to move it there; right-click for more')}
      >
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
            <!-- Sized in user units: a wide arc's would grow with it. -->
            <marker id="arrow-seen" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="9" markerHeight="9" markerUnits="userSpaceOnUse" orient="auto">
              <path d="M0,0 L8,4 L0,8 z" class="head seen" />
            </marker>
          </defs>

          <!-- Tier Headers -->
          <text class="tier-head" x={X_ROUTE} y={20}>{t('INGRESS / ROUTES')}</text>
          <text class="tier-head" x={X_SVC_CX - 34} y={20}>{t('SERVICES')}</text>
          <text class="tier-head" x={X_TASK} y={20}>{byNode ? t('NODES / PODS') : t('PODS (JAILS / VMS)')}</text>
          {#if volumeNames.length}
            <text class="tier-head" x={X_VOL} y={20}>{t('VOLUMES')}</text>
          {/if}

          <!-- Node bands, behind their pods; a band opens its node. -->
          {#snippet band(b: Band)}
            {@const nh = nodeHealth(nodeOf(b.node))}
            <rect class="band {nh}" class:unplaced={!b.node} x={BAND_X} y={b.top} width={BAND_W} height={b.h} rx="10" />
            {#if b.node}
              <!-- Its node's colour, as on its pods grouped by service. -->
              <circle class="node-tag {nodeHue(b.node)}" cx={BAND_X + 16} cy={b.top + 14} r="4.5" />
            {/if}
            <!-- One text, its lines spaced, so what it shows reads as words
                 its link's name holds (WCAG 2.5.3). -->
            <text
              ><tspan class="band-title" x={BAND_X + (b.node ? 26 : 12)} y={b.top + 18}>{b.node || t('not placed yet')}</tspan
              >{#if nodeNote(nodeOf(b.node))}{' '}<tspan
                  class="sub band-note {nh}"
                  x={BAND_X + BAND_W - 12}
                  y={b.top + 18}
                  text-anchor="end">{nodeNote(nodeOf(b.node))}</tspan
                >{/if}{#if !b.tasks.length}{' '}<tspan class="sub" x={X_TASK + 4} y={b.top + BAND_HEAD + TASK_STEP / 2 + 4}
                  >{t('no pods of this namespace')}</tspan
                >{/if}</text
            >
          {/snippet}
          {#each bands as b (b.node)}
            {#if b.node}
              <a
                href="/k/Node/_/{b.node}"
                data-node={b.node}
                class:drop-target={dragging && dropOn === b.node}
                oncontextmenu={(e) => openMenu(e, nodeMenu(nodeOf(b.node), b.node, scene), t('Node {name}', { name: b.node }))}
                onkeydown={(e) => menuKey(e, () => nodeMenu(nodeOf(b.node), b.node, scene), t('Node {name}', { name: b.node }))}
                aria-label={t('Node {shown}, {pods}', {
                  shown: [b.node, nodeNote(nodeOf(b.node)), b.tasks.length ? '' : t('no pods of this namespace')]
                    .filter((x) => x)
                    .join(' '),
                  pods: tn('1 pod', '{n} pods', b.tasks.length)
                })}
              >
                {@render band(b)}
              </a>
            {:else}
              <g>{@render band(b)}</g>
            {/if}
          {/each}

          <!-- Route -> Service edges -->
          {#each shownRoutes as r, i (r.metadata.name)}
            <path
              class="edge"
              d="M {X_ROUTE + ROUTE_W} {y(i)} C {X_ROUTE + ROUTE_W + 45} {y(i)}, {X_SVC_CX - SVC_R - 45} {y(svcRow.get(r.spec.service)!)}, {X_SVC_CX - SVC_R - 6} {y(svcRow.get(r.spec.service)!)}"
              marker-end="url(#arrow)"
            />
          {/each}
          {#each published as p, i (`${p.service}>${p.what}`)}
            {@const py = y(shownRoutes.length + i)}
            {@const sy = y(svcRow.get(p.service)!)}
            <path
              class="edge"
              d="M {X_ROUTE + ROUTE_W} {py} C {X_ROUTE + ROUTE_W + 45} {py}, {X_SVC_CX - SVC_R - 45} {sy}, {X_SVC_CX - SVC_R - 6} {sy}"
              marker-end="url(#arrow)"
            />
          {/each}
          {#if showSeen}
            {#each seenIn as e (e.to)}
              <path
                class="edge seen"
                style:stroke-width={width(e.n)}
                d="M {X_ROUTE + ROUTE_W} {y(outsideRow)} C {X_ROUTE + ROUTE_W + 45} {y(outsideRow)}, {X_SVC_CX - SVC_R - 45} {y(svcRow.get(e.to)!)}, {X_SVC_CX - SVC_R - 6} {y(svcRow.get(e.to)!)}"
                marker-end="url(#arrow-seen)"
              >
                <title>{[...new Set(e.what)].join(', ')} → {e.to}: {conns(e.n)}</title>
              </path>
            {/each}
          {:else if outside.length}
            {#each outside as e (e.to)}
              <path
                class="edge reach"
                d="M {X_ROUTE + ROUTE_W} {y(outsideRow)} C {X_ROUTE + ROUTE_W + 45} {y(outsideRow)}, {X_SVC_CX - SVC_R - 45} {y(svcRow.get(e.to)!)}, {X_SVC_CX - SVC_R - 6} {y(svcRow.get(e.to)!)}"
                marker-end="url(#arrow)"
              >
                <title>{t('other namespaces → {service}: {ports}', { service: e.to, ports: ports(e) })}</title>
              </path>
            {/each}
          {/if}

          <!-- Service <-> Service connections seen, or PF reachability arcs -->
          {#if showSeen}
            {#each seenBetween as e (`${e.from}>${e.to}`)}
              <path
                class="edge seen"
                style:stroke-width={width(e.n)}
                d={arc(svcRow.get(e.from)!, svcRow.get(e.to)!)}
                marker-end="url(#arrow-seen)"
              >
                <title>{e.from} → {e.to}: {[...new Set(e.what)].join(', ')}, {conns(e.n)}</title>
              </path>
            {/each}
          {:else if !everyone}
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

          <!-- Service -> Task (Pod / VM) edges; by node they fan out to
               each node running one. -->
          {#each shownTasks as tk (tk.metadata.name)}
            {@const sy = y(svcRow.get(tk.spec.service)!)}
            {@const ty = taskPos.get(tk.metadata.name)!}
            <path
              class="edge pod-link"
              d="M {X_SVC_CX + SVC_R} {sy} C {X_SVC_CX + SVC_R + 36} {sy}, {X_TASK - 36} {ty}, {X_TASK} {ty}"
            />
          {/each}

          <!-- Pod -> Volume edges; one to a node the data is not on is
               flagged. -->
          {#each volLinks as l (l.key)}
            <path
              class="edge vol-link"
              class:far={l.far}
              d="M {l.x0} {l.y0} C {l.x0 + 30} {l.y0}, {X_VOL - 30} {l.y1}, {X_VOL} {l.y1}"
            >
              {#if l.title}<title>{l.title}</title>{/if}
            </path>
          {/each}

          <!-- Route Nodes with Globe Icon -->
          {#each shownRoutes as r, i (r.metadata.name)}
            <a
              href={link('Route', r.metadata.name)}
              oncontextmenu={(e) => openMenu(e, routeMenu(r, scene), t('Route {name}', { name: r.metadata.name }))}
              onkeydown={(e) => menuKey(e, () => routeMenu(r, scene), t('Route {name}', { name: r.metadata.name }))}
              aria-label={t('{address} {route}, to {service}', {
                address: routeAddress(r),
                route: t('Route {name}', { name: r.metadata.name }),
                service: r.spec.service
              })}
            >
              <rect class="box route" x={X_ROUTE} y={y(i) - BOX_H / 2} width={ROUTE_W} height={BOX_H} rx="8" />
              <circle cx={X_ROUTE + 18} cy={y(i)} r="10" class="icon-bg route-bg" />
              <circle cx={X_ROUTE + 18} cy={y(i)} r="5.5" class="icon-stroke" />
              <line x1={X_ROUTE + 12.5} y1={y(i)} x2={X_ROUTE + 23.5} y2={y(i)} class="icon-stroke" />
              <ellipse cx={X_ROUTE + 18} cy={y(i)} rx="2.6" ry="5.5" class="icon-stroke" />
              <text
                ><tspan x={X_ROUTE + 34} y={y(i) - 3}>{routeAddress(r)}</tspan>{' '}<tspan
                  class="sub"
                  x={X_ROUTE + 34}
                  y={y(i) + 12}>{t('Route {name}', { name: r.metadata.name })}</tspan
                ></text
              >
            </a>
          {/each}
          <!-- Published ports: a plug icon, the port and where it is answered. -->
          {#each published as p, i (`${p.service}>${p.what}`)}
            {@const py = y(shownRoutes.length + i)}
            {@const where = portWhere(p)}
            <a
              href={link('Service', p.service)}
              oncontextmenu={(e) => openMenu(e, portMenu(p, scene), t('Port {shown}', { shown: portShown(p) }))}
              onkeydown={(e) => menuKey(e, () => portMenu(p, scene), t('Port {shown}', { shown: portShown(p) }))}
              aria-label={t('Port {shown} {where}, to {service}', {
                shown: portShown(p),
                service: p.service,
                where
              })}
            >
              <rect class="box route" class:idle={p.at === null} x={X_ROUTE} y={py - BOX_H / 2} width={ROUTE_W} height={BOX_H} rx="8" />
              <circle cx={X_ROUTE + 18} cy={py} r="10" class="icon-bg route-bg" />
              <rect x={X_ROUTE + 13} y={py - 4} width="10" height="7" rx="1.5" class="icon-stroke" />
              <line x1={X_ROUTE + 16} y1={py - 7} x2={X_ROUTE + 16} y2={py - 4} class="icon-stroke" />
              <line x1={X_ROUTE + 20} y1={py - 7} x2={X_ROUTE + 20} y2={py - 4} class="icon-stroke" />
              <line x1={X_ROUTE + 18} y1={py + 3} x2={X_ROUTE + 18} y2={py + 7} class="icon-stroke" />
              <text
                ><tspan x={X_ROUTE + 34} y={py - 3}>{portShown(p)}</tspan>{' '}<tspan class="sub" x={X_ROUTE + 34} y={py + 12}
                  >{where}</tspan
                ></text
              >
            </a>
          {/each}
          {#if showSeen ? seenIn.length : outside.length}
            <rect class="box outside" x={X_ROUTE} y={y(outsideRow) - BOX_H / 2} width={ROUTE_W} height={BOX_H} rx="8" />
            <text x={X_ROUTE + 14} y={y(outsideRow) + 4}>{showSeen ? t('outside the namespace') : t('other namespaces')}</text>
          {/if}

          <!-- OpenShift-style Circular Service Nodes with Pod Ring & Workload Icon -->
          {#each services as s, i (s.metadata.name)}
            {@const cy = y(i)}
            {@const h = serviceHealth(s)}
            {@const pList = containerPorts(s)}
            <a
              href={link('Service', s.metadata.name)}
              data-drag="Service"
              data-name={s.metadata.name}
              oncontextmenu={(e) => openMenu(e, serviceMenu(s, scene), t('Service {name}', { name: s.metadata.name }))}
              onkeydown={(e) => menuKey(e, () => serviceMenu(s, scene), t('Service {name}', { name: s.metadata.name }))}
              aria-label={t('Service {name} {note}', {
                name: `${s.metadata.name}${pList.length ? ` :${pList[0]}` : ''}`,
                note: svcNote(s)
              })}
            >
              <circle
                class="svc-halo {h}"
                class:picked={picked === s.metadata.name}
                cx={X_SVC_CX}
                cy={cy}
                r={SVC_R + 5}
              />
              <circle class="svc-ring {h}" cx={X_SVC_CX} cy={cy} r={SVC_R} />
              <circle class="svc-core" cx={X_SVC_CX} cy={cy} r={SVC_R - 6} />

              <TopologyServiceIcon service={s} x={X_SVC_CX} y={cy} />

              <!-- Its health as a lamp; the count is said once, under its
                   name, so what is shown is what its link is named
                   (WCAG 2.5.3). -->
              <rect x={X_SVC_CX + 18} y={cy - 32} width="14" height="14" rx="7" class="badge-pill {h}" />

              <!-- Service label & ports -->
              <text text-anchor="middle"
                ><tspan class="svc-title" x={X_SVC_CX} y={cy + SVC_R + 18}
                  >{s.metadata.name}{pList.length ? ` :${pList[0]}` : ''}</tspan
                >{' '}<tspan class="sub" x={X_SVC_CX} y={cy + SVC_R + 33}>{svcNote(s)}</tspan></text
              >
            </a>
          {/each}

          <!-- Task (Jail Pod / bhyve VM) Nodes -->
          {#each shownTasks as tk (tk.metadata.name)}
            {@const ty = taskPos.get(tk.metadata.name)!}
            {@const th = taskHealth(tk)}
            {@const node = tk.spec?.node ?? ''}
            <a
              href={link('Task', tk.metadata.name)}
              oncontextmenu={(e) => openMenu(e, taskMenu(tk, scene), t('Task {name}', { name: tk.metadata.name }))}
              onkeydown={(e) => menuKey(e, () => taskMenu(tk, scene), t('Task {name}', { name: tk.metadata.name }))}
              aria-label={t('Task {shown}, {state}, on {node}', {
                shown: `${tk.metadata.name} ${taskLine(tk)}`,
                state: tk.status?.paused ? 'paused' : (tk.status?.state ?? 'unknown'),
                node: node || t('no node yet')
              })}
            >
              <rect class="box task {th}" x={X_TASK} y={ty - TASK_H / 2} width={TASK_W} height={TASK_H} rx="8" />
              {#if multiNode && node && !byNode}
                <!-- Its node's colour, as in the key above. -->
                <rect class="node-tag {nodeHue(node)}" x={X_TASK + 1} y={ty - TASK_H / 2 + 6} width="4" height={TASK_H - 12} rx="2" />
              {/if}
              <circle cx={X_TASK + 16} cy={ty} r="5" class="dot-status {th}" />
              <text
                ><tspan x={X_TASK + 28} y={ty - 3}>{tk.metadata.name}</tspan>{' '}<tspan
                  class="sub"
                  x={X_TASK + 28}
                  y={ty + 12}>{taskLine(tk)}</tspan
                ></text
              >
            </a>
          {/each}

          <!-- Volume Nodes with Storage Icon -->
          {#each volumeNames as v, i (v)}
            {@const vh = volumeHealth(v)}
            <a
              href={link('Volume', v)}
              data-drag="Volume"
              data-name={v}
              oncontextmenu={(e) => openMenu(e, volumeMenu(v, volumeNode(v), scene), t('Volume {name}', { name: v }))}
              onkeydown={(e) => menuKey(e, () => volumeMenu(v, volumeNode(v), scene), t('Volume {name}', { name: v }))}
              aria-label={t('Volume {name}: {note}', { name: v, note: volumeNote(v) })}
            >
              <rect class="box vol {vh}" x={X_VOL} y={y(i) - BOX_H / 2} width={VOL_W} height={BOX_H} rx="8" />
              {#if multiNode && volumeNode(v)}
                <rect class="node-tag {nodeHue(volumeNode(v))}" x={X_VOL + 1} y={y(i) - BOX_H / 2 + 6} width="4" height={BOX_H - 12} rx="2" />
              {/if}
              <ellipse cx={X_VOL + 18} cy={y(i) - 5} rx="7" ry="2.8" class="icon-stroke" />
              <path d="M {X_VOL + 11} {y(i) - 5} v 10 c 0 1.6 3.1 2.8 7 2.8 s 7 -1.2 7 -2.8 v -10" class="icon-stroke" />
              <text
                ><tspan x={X_VOL + 32} y={y(i) - 3}>{v}</tspan>{' '}<tspan class="sub" x={X_VOL + 32} y={y(i) + 12}
                  >{volumeNote(v)}</tspan
                ></text
              >
            </a>
          {/each}
        </svg>
      </div>
      <div class="topo-legend muted">
        <span><span class="leg-dot good"></span>{t('Ready / Running')}</span>
        <span><span class="leg-dot wait"></span>{t('Starting / Partial')}</span>
        <span><span class="leg-dot bad"></span>{t('Failed / Lost')}</span>
        {#if volLinks.some((l) => l.far)}
          <span><span class="leg-far"></span>{t('Volume on another node than its pod')}</span>
        {/if}
        {#if picked}
          <span><span class="leg-picked"></span>{t('Shown in the panel')}</span>
        {/if}
        {#if showSeen}
          <span><span class="leg-seen"></span>{t('Connections open or closed in the last minute and a half; wider for more')}</span>
        {:else}
          <span><span class="leg-reach"></span>{t('May connect, as the nodes enforce it')}</span>
        {/if}
        <span>{t('Click any Service, Pod/VM, Route, Volume or node to drill down.')}</span>
      </div>
    </div>

    {#if inspectedSvc}
      <TopologyInspector
        service={inspectedSvc}
        {namespace}
        {edges}
        {seen}
        {balancers}
        routes={shownRoutes}
        {published}
        {used}
      />
    {/if}
  </div>
{:else if !error}
  <p class="empty">{t('No services in {namespace}.', { namespace })}</p>
{/if}

{#if menu}
  <ContextMenu x={menu.x} y={menu.y} items={menu.items} label={menu.label} onclose={() => (menu = null)} />
{/if}
{#if dragging && drag}
  <div class="drag-ghost card" style:left="{ghost.x + 14}px" style:top="{ghost.y + 10}px" aria-hidden="true">
    {drag.name} → {dropOn || t('drop on a node')}
  </div>
{/if}

<style>
  .drag-ghost {
    position: fixed;
    z-index: 40;
    pointer-events: none;
    padding: 0.3rem 0.6rem;
    font-size: 0.85rem;
  }
  :global(a.drop-target rect.band) {
    stroke: var(--primary);
    stroke-width: 3;
  }
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
    background: var(--primary);
    border-color: var(--primary);
    color: var(--on-primary);
  }
  .svc-ring {
    fill: var(--panel);
    stroke: var(--control);
    stroke-width: 3.5;
  }
  /* Quiet while all is well: the ready badge and dots say so, the logos
     say what each is, and only a ring or box needing a look is coloured. */
  .svc-ring.good {
    stroke: var(--control);
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
  .edge.vol-link.far {
    stroke: var(--bad);
    stroke-dasharray: 2 4;
    stroke-width: 2;
  }
  .leg-picked {
    display: inline-block;
    width: 10px;
    height: 10px;
    margin-right: 0.35rem;
    border: 2px dashed var(--link);
    border-radius: 50%;
    vertical-align: middle;
  }
  .leg-far {
    display: inline-block;
    width: 18px;
    margin-right: 0.35rem;
    border-top: 2px dotted var(--bad);
    vertical-align: middle;
  }
  .leg-seen,
  .leg-reach {
    display: inline-block;
    width: 18px;
    margin-right: 0.35rem;
    vertical-align: middle;
  }
  .leg-seen {
    border-top: 3px solid var(--link);
  }
  .leg-reach {
    border-top: 1.5px dashed var(--link);
  }
  .canvas-card .note {
    margin: 0.5rem 0 0;
  }
  .node-key {
    display: inline-flex;
    flex-wrap: wrap;
    gap: 0.75rem;
    font-size: 0.8rem;
  }
  .key-swatch {
    display: inline-block;
    width: 10px;
    height: 10px;
    border-radius: 3px;
    margin-right: 0.3rem;
    vertical-align: -1px;
    background: var(--hue);
  }
  /* One colour per node, none of them the health colours. */
  .n0 {
    --hue: light-dark(#2557d6, #7aa2ff);
  }
  .n1 {
    --hue: light-dark(#0e7490, #22d3ee);
  }
  .n2 {
    --hue: light-dark(#7c3aed, #c4b5fd);
  }
  .n3 {
    --hue: light-dark(#be185d, #f472b6);
  }
  .n4 {
    --hue: light-dark(#4d5b12, #bef264);
  }
  .n5 {
    --hue: light-dark(#7c4a1e, #e0a370);
  }
  .node-tag {
    fill: var(--hue, var(--control));
  }
  /* Neutral, as the rings are: only a node that is down is coloured. */
  .band {
    fill: color-mix(in srgb, var(--control) 6%, transparent);
    stroke: var(--control);
    stroke-width: 1.5;
  }
  .band.bad {
    stroke: var(--bad);
    stroke-width: 2.5;
    fill: color-mix(in srgb, var(--bad) 9%, transparent);
  }
  .band.wait {
    stroke: var(--wait);
  }
  .band.unplaced {
    stroke-dasharray: 5 4;
  }
  .band-title {
    font-size: 12px;
    font-weight: 700;
  }
  .sub {
    fill: var(--muted);
    font-size: 11px;
  }
  .band-note.bad {
    fill: var(--bad);
    font-weight: 700;
  }
  .band-note.wait {
    fill: var(--wait);
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
    stroke: var(--control);
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
  .edge {
    stroke: var(--muted);
    stroke-width: 1.2;
    fill: none;
  }
  .edge.reach {
    stroke: var(--link);
    stroke-dasharray: 5 3;
  }
  .edge.seen {
    stroke: var(--link);
    stroke-linecap: round;
    opacity: 0.85;
  }
  .head.seen {
    fill: var(--link);
  }
  .head {
    fill: var(--muted);
  }
  a:focus-visible rect {
    stroke-width: 3;
  }
</style>

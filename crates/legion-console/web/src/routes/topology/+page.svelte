<script lang="ts">
  // Interactive Topology & PF Reachability Inspector: visualises a
  // namespace's Routes, Services, PF packet-filter reachability arcs, CARP
  // VIPs, and ZFS/NFS/iSCSI Volumes with zoom controls and click-to-inspect
  // packet filter rules.
  import {
    api,
    follow,
    type Balancer,
    type ConnectionEdge,
    type ConnectionEnd,
    type LObject,
    type ReachEdge
  } from '#lib/api.ts';
  import { t, tn } from '#lib/i18n.svelte.ts';
  import { logoFor } from '#lib/logos.ts';
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
  function panDown(e: PointerEvent) {
    if (e.button !== 0 || !canvas) return;
    press = { x: e.clientX, y: e.clientY, left: canvas.scrollLeft, top: canvas.scrollTop, id: e.pointerId };
  }
  function panMove(e: PointerEvent) {
    if (!press || !canvas || e.pointerId !== press.id) return;
    const dx = e.clientX - press.x;
    const dy = e.clientY - press.y;
    if (!panning && Math.hypot(dx, dy) < 5) return;
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

  // The connections the nodes' pf keeps, asked again every 10 s: each ask
  // has every node list its states.
  let seen: ConnectionEdge[] = $state([]);
  let unanswered: { node: string; error: string }[] = $state([]);
  let seenError = $state('');
  $effect(() => {
    const ns = namespace;
    let gone = false;
    const load = () =>
      api
        .connections(ns)
        .then((r) => {
          if (gone) return;
          seen = r.edges;
          unanswered = r.unanswered;
          seenError = '';
        })
        .catch((e) => {
          if (!gone) seenError = e.message;
        });
    seen = [];
    unanswered = [];
    load();
    const every = setInterval(load, 10_000);
    return () => {
      gone = true;
      clearInterval(every);
    };
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

  // A row's least height; a service's row grows with its tasks, each
  // TASK_STEP below the last, so they never reach into the next row.
  const ROW = 112;
  const TASK_STEP = 52;
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
  // Each port a service publishes outside the cluster: through the routing
  // mesh on every node (`ingress`), or on the nodes running its tasks.
  type Published = { service: string; what: string; at: number | null; host: boolean };
  const published: Published[] = $derived(
    services.flatMap((s) => {
      const got: { port: number; protocol?: string; publishedPort: number }[] =
        s.status?.publishedPorts ?? [];
      return ((s.spec?.endpoint?.ports ?? []) as { port: number; protocol?: string; publish?: string }[])
        .filter((p) => p.publish === 'ingress' || p.publish === 'host')
        .map((p) => {
          const protocol = p.protocol ?? 'tcp';
          const pub = got.find((g) => g.port === p.port && (g.protocol ?? 'tcp') === protocol);
          return {
            service: s.metadata.name,
            what: `${p.port}/${protocol}`,
            at: pub?.publishedPort ?? null,
            host: p.publish === 'host'
          };
        });
    })
  );
  const outside = $derived(edges.filter((e) => e.from === '' && svcRow.has(e.to)));
  const between = $derived(
    edges.filter((e) => e.from !== '' && e.from !== e.to && svcRow.has(e.from) && svcRow.has(e.to))
  );
  // What the nodes saw: between two services, summed over ports and ways;
  // and into each service from anything but another of them.
  const isSvc = (e: ConnectionEnd) => e.kind === 'service' && svcRow.has(e.name);
  type Seen = { from: string; to: string; n: number; what: string[] };
  const seenBetween: Seen[] = $derived.by(() => {
    const by = new Map<string, Seen>();
    for (const e of seen) {
      if (!isSvc(e.from) || !isSvc(e.to) || e.from.name === e.to.name) continue;
      const key = `${e.from.name}>${e.to.name}`;
      const s = by.get(key) ?? { from: e.from.name, to: e.to.name, n: 0, what: [] };
      s.n += e.connections;
      s.what.push(`${e.port}/${e.protocol}`);
      by.set(key, s);
    }
    return [...by.values()];
  });
  const seenIn: Seen[] = $derived.by(() => {
    const by = new Map<string, Seen>();
    for (const e of seen) {
      if (isSvc(e.from) || !isSvc(e.to)) continue;
      const s = by.get(e.to.name) ?? { from: '', to: e.to.name, n: 0, what: [] };
      s.n += e.connections;
      s.what.push(endName(e.from));
      by.set(e.to.name, s);
    }
    return [...by.values()];
  });
  // Wider for more connections, up to about four times as wide.
  const width = (n: number) => 1.4 + Math.min(3.6, Math.log2(n + 1));
  const conns = (n: number) => tn('1 connection', '{n} connections', n);
  function endName(e: ConnectionEnd) {
    switch (e.kind) {
      case 'service':
        return e.name;
      case 'job':
        return t('Job {name}', { name: e.name });
      case 'node':
        return t('node {name}', { name: e.name });
      case 'namespace':
        return t('other namespaces');
      default:
        return t('outside');
    }
  }
  const viaNote = (e: ConnectionEdge) =>
    e.via === 'service'
      ? t('to its service address')
      : e.via === 'published'
        ? t('to a published port')
        : t('to the task');

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
  const rowTops = $derived.by(() => {
    const tops: number[] = [];
    let top = HEADER_H;
    for (const h of rowHeights) {
      tops.push(top);
      top += h;
    }
    return tops;
  });

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
  const NODE_HUES = 6;
  const nodeHue = (name: string) => {
    const i = nodeNames.indexOf(name);
    return i < 0 ? '' : `n${i % NODE_HUES}`;
  };
  const nodeOf = (name: string) => nodes.find((n) => n.metadata.name === name);
  const nodeHealth = (name: string) => {
    const st = nodeOf(name)?.status?.state;
    if (st === 'ready') return 'good';
    if (st === 'down') return 'bad';
    return st ? 'wait' : '';
  };
  const nodeNote = (name: string) => {
    const n = nodeOf(name);
    if (!n) return '';
    const parts = [n.spec?.role ?? 'worker', n.status?.state ?? 'unknown'];
    const av = n.spec?.availability;
    if (av && av !== 'active') parts.push(av);
    return parts.join(' · ');
  };

  // Node bands, in the pods' column: each node's pods, then those not
  // placed on one yet.
  const BAND_HEAD = 28;
  const BAND_PAD = 10;
  const BAND_GAP = 14;
  const BAND_X = X_TASK - 12;
  const BAND_W = TASK_W + 24;
  type Band = { node: string; tasks: LObject[]; top: number; h: number };
  const bands: Band[] = $derived.by(() => {
    if (!byNode) return [];
    const shown = activeTasks
      .filter((tk) => svcRow.has(tk.spec?.service))
      .sort(
        (a, b) =>
          svcRow.get(a.spec.service)! - svcRow.get(b.spec.service)! ||
          a.metadata.name.localeCompare(b.metadata.name)
      );
    const groups = nodeNames.map((node) => ({ node, tasks: shown.filter((tk) => tk.spec?.node === node) }));
    const unplaced = shown.filter((tk) => !tk.spec?.node);
    if (unplaced.length) groups.push({ node: '', tasks: unplaced });
    let top = HEADER_H;
    return groups.map((g) => {
      const h = BAND_HEAD + Math.max(1, g.tasks.length) * TASK_STEP + BAND_PAD;
      const band = { ...g, top, h };
      top += h + BAND_GAP;
      return band;
    });
  });
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
  const volumeNode = (name: string): string =>
    volumes.find((v) => v.metadata.name === name)?.status?.node ?? '';
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

  // The service picked for the panel, ringed; until one is, the panel
  // shows the first, unringed.
  const picked = $derived(services.some((s) => s.metadata.name === selectedService) ? selectedService : '');
  const inspectedSvc = $derived(
    services.find((s) => s.metadata.name === selectedService) ?? services[0] ?? null
  );
  const inboundEdges = $derived(
    inspectedSvc ? edges.filter((e) => e.to === inspectedSvc.metadata.name) : []
  );
  const outboundEdges = $derived(
    inspectedSvc ? edges.filter((e) => e.from === inspectedSvc.metadata.name) : []
  );
  const seenInbound = $derived(
    inspectedSvc ? seen.filter((e) => e.to.kind === 'service' && e.to.name === inspectedSvc.metadata.name) : []
  );
  const seenOutbound = $derived(
    inspectedSvc
      ? seen.filter(
          (e) =>
            e.from.kind === 'service' &&
            e.from.name === inspectedSvc.metadata.name &&
            !(e.to.kind === 'service' && e.to.name === inspectedSvc.metadata.name)
        )
      : []
  );
  const svcVips = $derived(
    inspectedSvc ? balancers.filter((b) => b.service === inspectedSvc.metadata.name) : []
  );
  const svcRoutes = $derived(
    inspectedSvc ? shownRoutes.filter((r) => r.spec?.service === inspectedSvc.metadata.name) : []
  );
  const svcPublished = $derived(
    inspectedSvc ? published.filter((p) => p.service === inspectedSvc.metadata.name) : []
  );
  const svcVols = $derived(
    inspectedSvc ? used.filter((u) => u.service === inspectedSvc.metadata.name).map((u) => u.volume) : []
  );
</script>

<header class="bar">
  <h1>{t('Topology: {namespace}', { namespace })}</h1>
  <div class="controls">
    <button type="button" class="sm" aria-label={t('Zoom out')} title={t('Zoom out (Ctrl+wheel)')} onclick={() => zoomBy(-20)}>−</button>
    <span class="mono muted" aria-live="polite">{zoom}%</span>
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
        <div class="seg" role="group" aria-labelledby="group-by">
          <button type="button" class="sm" class:primary={!byNode} aria-pressed={!byNode} onclick={() => setGroup('service')}>
            {t('Service')}
          </button>
          <button type="button" class="sm" class:primary={byNode} aria-pressed={byNode} onclick={() => setGroup('node')}>
            {t('Node')}
          </button>
        </div>
        <span class="muted" id="arcs-of">{t('Arcs:')}</span>
        <div class="seg" role="group" aria-labelledby="arcs-of">
          <button type="button" class="sm" class:primary={!showSeen} aria-pressed={!showSeen} onclick={() => setArcs('allowed')}>
            {t('Allowed')}
          </button>
          <button type="button" class="sm" class:primary={showSeen} aria-pressed={showSeen} onclick={() => setArcs('seen')}>
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
        title={t('Drag to move around; Ctrl+wheel to zoom')}
        onpointerdown={panDown}
        onpointermove={panMove}
        onpointerup={panUp}
        onpointercancel={panUp}
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
            {@const nh = nodeHealth(b.node)}
            <rect class="band {nh}" class:unplaced={!b.node} x={BAND_X} y={b.top} width={BAND_W} height={b.h} rx="10" />
            {#if b.node}
              <!-- Its node's colour, as on its pods grouped by service. -->
              <circle class="node-tag {nodeHue(b.node)}" cx={BAND_X + 16} cy={b.top + 14} r="4.5" />
            {/if}
            <text class="band-title" x={BAND_X + (b.node ? 26 : 12)} y={b.top + 18}>{b.node || t('not placed yet')}</text>
            {#if nodeNote(b.node)}
              <text class="sub band-note {nh}" x={BAND_X + BAND_W - 12} y={b.top + 18} text-anchor="end">{nodeNote(b.node)}</text>
            {/if}
            {#if !b.tasks.length}
              <text class="sub" x={X_TASK + 4} y={b.top + BAND_HEAD + TASK_STEP / 2 + 4}>{t('no pods of this namespace')}</text>
            {/if}
          {/snippet}
          {#each bands as b (b.node)}
            {#if b.node}
              <a
                href="/k/Node/_/{b.node}"
                aria-label={t('Node {name}: {note}', {
                  name: b.node,
                  note: nodeNote(b.node) || tn('1 pod', '{n} pods', b.tasks.length)
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
          <!-- Published ports: a plug icon, the port and where it is answered. -->
          {#each published as p, i (`${p.service}>${p.what}`)}
            {@const py = y(shownRoutes.length + i)}
            {@const where = p.at === null ? t('not published yet') : p.host ? t('on nodes running it') : t('on every node')}
            <a
              href={link('Service', p.service)}
              aria-label={t('Port {port} published to {service}, {where}', {
                port: p.at ?? p.what,
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
              <text x={X_ROUTE + 34} y={py - 3}>{p.at === null ? p.what : `:${p.at}`} → {p.what.split('/')[0]}</text>
              <text class="sub" x={X_ROUTE + 34} y={py + 12}>{where}</text>
            </a>
          {/each}
          {#if showSeen ? seenIn.length : outside.length}
            <rect class="box outside" x={X_ROUTE} y={y(outsideRow) - BOX_H / 2} width={ROUTE_W} height={BOX_H} rx="8" />
            <text x={X_ROUTE + 14} y={y(outsideRow) + 4}>{showSeen ? t('outside the namespace') : t('other namespaces')}</text>
          {/if}

          <!-- OpenShift-style Circular Service Nodes with Pod Ring & Workload Icon -->
          {#each services as s, i (s.metadata.name)}
            {@const cy = y(i)}
            {@const h = health(s)}
            {@const vm = isVmSvc(s)}
            {@const db = isDbSvc(s)}
            {@const logo = logoFor(`${s.metadata.name} ${s.spec?.pod?.containers?.[0]?.image ?? ''}`)}
            {@const pList = svcPorts(s)}
            <a
              href={link('Service', s.metadata.name)}
              aria-label={t('Service {name}: {note}', { name: s.metadata.name, note: svcNote(s) })}
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

              {#if vm}
                <!-- bhyve VM monitor icon -->
                <rect x={X_SVC_CX - 10} y={cy - 9} width="20" height="14" rx="2" class="icon-stroke" />
                <line x1={X_SVC_CX - 5} y1={cy + 9} x2={X_SVC_CX + 5} y2={cy + 9} class="icon-stroke" />
                <line x1={X_SVC_CX} y1={cy + 5} x2={X_SVC_CX} y2={cy + 9} class="icon-stroke" />
              {:else if logo}
                <!-- What it runs, by its product's mark. -->
                <path
                  class="logo"
                  style:fill="light-dark({logo.light}, {logo.dark})"
                  d={logo.path}
                  transform="translate({X_SVC_CX - 11} {cy - 11}) scale({22 / 24})"
                ><title>{logo.title}</title></path>
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
              <text class="sub" x={X_SVC_CX} y={cy + SVC_R + 33} text-anchor="middle">{svcNote(s)}</text>
            </a>
          {/each}

          <!-- Task (Jail Pod / bhyve VM) Nodes -->
          {#each shownTasks as tk (tk.metadata.name)}
            {@const ty = taskPos.get(tk.metadata.name)!}
            {@const th = taskHealth(tk)}
            {@const vm = isVmSvc(tk)}
            {@const node = tk.spec?.node ?? ''}
            <a
              href={link('Task', tk.metadata.name)}
              aria-label={t('Task {name} on {node}: {state}', {
                name: tk.metadata.name,
                node: node || t('no node yet'),
                state: tk.status?.paused ? 'paused' : (tk.status?.state ?? 'unknown')
              })}
            >
              <rect class="box task {th}" x={X_TASK} y={ty - TASK_H / 2} width={TASK_W} height={TASK_H} rx="8" />
              {#if multiNode && node && !byNode}
                <!-- Its node's colour, as in the key above. -->
                <rect class="node-tag {nodeHue(node)}" x={X_TASK + 1} y={ty - TASK_H / 2 + 6} width="4" height={TASK_H - 12} rx="2" />
              {/if}
              <circle cx={X_TASK + 16} cy={ty} r="5" class="dot-status {th}" />
              <text x={X_TASK + 28} y={ty - 3}>{tk.metadata.name}</text>
              <text class="sub" x={X_TASK + 28} y={ty + 12}>
                {vm ? 'bhyve' : 'jail'} · {tk.status?.paused ? 'paused' : (tk.status?.ip ?? tk.status?.state ?? '')}{byNode ? '' : ` · ${node}`}
              </text>
            </a>
          {/each}

          <!-- Volume Nodes with Storage Icon -->
          {#each volumeNames as v, i (v)}
            {@const vh = volumeHealth(v)}
            <a href={link('Volume', v)} aria-label={t('Volume {name}: {note}', { name: v, note: volumeNote(v) })}>
              <rect class="box vol {vh}" x={X_VOL} y={y(i) - BOX_H / 2} width={VOL_W} height={BOX_H} rx="8" />
              {#if multiNode && volumeNode(v)}
                <rect class="node-tag {nodeHue(volumeNode(v))}" x={X_VOL + 1} y={y(i) - BOX_H / 2 + 6} width="4" height={BOX_H - 12} rx="2" />
              {/if}
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

        {#if svcVips.length}
          <h3>{t('CARP VIPs')}</h3>
          <ul class="plain mono">
            {#each svcVips as b (b.address)}
              <li>{b.address} (vhid {b.vhid})</li>
            {/each}
          </ul>
        {/if}

        {#if svcPublished.length}
          <h3>{t('Published ports')}</h3>
          <ul class="plain mono">
            {#each svcPublished as p (p.what)}
              <li>
                {p.at === null ? '—' : p.at} → {p.what}
                <span class="muted">
                  {p.at === null ? t('not published yet') : p.host ? t('on nodes running it') : t('on every node')}
                </span>
              </li>
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
  <p class="empty">{t('No services in {namespace}.', { namespace })}</p>
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
  .logo {
    fill: var(--text);
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
  .seg {
    display: inline-flex;
  }
  .seg button + button {
    margin-left: -1px;
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
  text.band-note.bad {
    fill: var(--bad);
    font-weight: 700;
  }
  text.band-note.wait {
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

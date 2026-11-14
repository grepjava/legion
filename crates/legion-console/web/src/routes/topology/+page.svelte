<script lang="ts">
  // Topology (CONSOLE.md phase 5): a namespace's Services with their
  // health, the Routes to them, what may reach what among them (`legion
  // reach`, as the nodes enforce it) and the Volumes they use, followed
  // live. Each box leads to its object.
  import { api, follow, type LObject, type ReachEdge } from '#lib/api.ts';
  import { namespaces, session } from '#lib/session.svelte.ts';

  const mine = $derived(namespaces([]));
  let chosen = $state('');
  let typed = $state('');
  const namespace = $derived(chosen || session.namespace || mine[0] || 'default');

  let services: LObject[] = $state([]);
  let routes: LObject[] = $state([]);
  let volumes: LObject[] = $state([]);
  let edges: ReachEdge[] = $state([]);
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

  // Reach is worked out again when the set of services changes.
  const names = $derived(services.map((s) => s.metadata.name).join(','));
  $effect(() => {
    const ns = namespace;
    void names;
    api
      .reach(ns)
      .then((r) => (edges = r.edges))
      .catch((e) => (error = e.message));
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
  const between = $derived(edges.filter((e) => e.from !== '' && e.from !== e.to && svcRow.has(e.from) && svcRow.has(e.to)));
  // Every service reaching every other is drawn as a note, not n² arcs.
  const everyone = $derived(services.length > 2 && between.length === services.length * (services.length - 1));
  const leftRows = $derived(shownRoutes.length + (outside.length ? 1 : 0));
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
    if (!v) return 'missing';
    if (v.status?.lost) return 'lost';
    return v.status?.node ? `Volume on ${v.status.node}` : 'Volume';
  };
  const svcNote = (s: LObject) =>
    s.status ? `${s.status.ready ?? 0}/${s.status.desired ?? 0} ready` : 'not yet placed';
  const ports = (e: ReachEdge) => (e.all_ports ? 'all ports' : e.ports.map((p) => `${p.port}/${p.protocol}`).join(', '));
  const arc = (from: number, to: number) => {
    const x = X_SVC + BOX_W;
    const bulge = Math.min(ARCS - 20, 30 + Math.abs(to - from) * 22);
    return `M ${x} ${y(from) - 6} C ${x + bulge} ${y(from) - 6}, ${x + bulge} ${y(to) + 6}, ${x + 6} ${y(to) + 6}`;
  };
  const link = (kind: string, name: string) => `/k/${kind}/${namespace}/${name}`;
</script>

<header class="bar">
  <h1>Topology: {namespace}</h1>
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
  Routes to the namespace's Services, what may reach what among them as the nodes enforce it, and the Volumes they
  use. Green: every task ready; amber: some; red: none, or a Volume lost.
</p>
{#if error}<p class="error" role="alert">{error}</p>{/if}
{#if everyone}<p class="muted">Every service may reach every other (the namespace is not isolated by policy).</p>{/if}

{#if services.length}
  <p class="sr-only">
    What may reach what is listed on the <a href="/network">Network</a> page too.
  </p>
  <svg class="topology" width={X_VOL + BOX_W + 10} {height} role="group" aria-label="Topology of {namespace}">
    <defs>
      <marker id="arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto">
        <path d="M0,0 L8,4 L0,8 z" class="head" />
      </marker>
    </defs>

    {#each shownRoutes as r, i (r.metadata.name)}
      <line class="edge" x1={X_ROUTE + BOX_W} y1={y(i)} x2={X_SVC} y2={y(svcRow.get(r.spec.service)!)} marker-end="url(#arrow)" />
    {/each}
    {#if outside.length}
      {#each outside as e (e.to)}
        <line class="edge reach" x1={X_ROUTE + BOX_W} y1={y(shownRoutes.length)} x2={X_SVC} y2={y(svcRow.get(e.to)!)} marker-end="url(#arrow)"
          ><title>other namespaces → {e.to}: {ports(e)}</title></line
        >
      {/each}
    {/if}
    {#if !everyone}
      {#each between as e (`${e.from}>${e.to}`)}
        <path class="edge reach" d={arc(svcRow.get(e.from)!, svcRow.get(e.to)!)} marker-end="url(#arrow)"
          ><title>{e.from} → {e.to}: {ports(e)}</title></path
        >
      {/each}
    {/if}
    {#each used as u (`${u.service}>${u.volume}`)}
      <line class="edge" x1={X_SVC + BOX_W} y1={y(svcRow.get(u.service)!)} x2={X_VOL} y2={y(volRow.get(u.volume)!)} />
    {/each}

    {#each shownRoutes as r, i (r.metadata.name)}
      <a href={link('Route', r.metadata.name)} aria-label="Route {r.metadata.name}: {r.spec.host}{r.spec.path ?? ''} to {r.spec.service}">
        <rect class="box route" x={X_ROUTE} y={10 + i * ROW} width={BOX_W} height={BOX_H} rx="6" />
        <text x={X_ROUTE + 10} y={10 + i * ROW + 17}>{r.spec.host}{r.spec.path && r.spec.path !== '/' ? r.spec.path : ''}</text>
        <text class="sub" x={X_ROUTE + 10} y={10 + i * ROW + 32}>Route {r.metadata.name}</text>
      </a>
    {/each}
    {#if outside.length}
      <rect class="box outside" x={X_ROUTE} y={10 + shownRoutes.length * ROW} width={BOX_W} height={BOX_H} rx="6" />
      <text x={X_ROUTE + 10} y={10 + shownRoutes.length * ROW + 25}>other namespaces</text>
    {/if}
    {#each services as s, i (s.metadata.name)}
      <a href={link('Service', s.metadata.name)} aria-label="Service {s.metadata.name}: {svcNote(s)}">
        <rect class="box svc {health(s)}" x={X_SVC} y={10 + i * ROW} width={BOX_W} height={BOX_H} rx="6" />
        <text x={X_SVC + 10} y={10 + i * ROW + 17}>{s.metadata.name}</text>
        <text class="sub" x={X_SVC + 10} y={10 + i * ROW + 32}
          >{svcNote(s)}</text
        >
      </a>
    {/each}
    {#each volumeNames as v, i (v)}
      <a href={link('Volume', v)} aria-label="Volume {v}: {volumeNote(v)}">
        <rect class="box vol {volumeHealth(v)}" x={X_VOL} y={10 + i * ROW} width={BOX_W} height={BOX_H} rx="6" />
        <text x={X_VOL + 10} y={10 + i * ROW + 17}>{v}</text>
        <text class="sub" x={X_VOL + 10} y={10 + i * ROW + 32}
          >{volumeNote(v)}</text
        >
      </a>
    {/each}
  </svg>
{:else if !error}
  <p class="muted">No services in {namespace}.</p>
{/if}

<style>
  .topology {
    max-width: 100%;
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

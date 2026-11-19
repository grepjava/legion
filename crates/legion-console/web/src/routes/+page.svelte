<script lang="ts">
  // Mission-Control Overview HUD: 4-subsystem live telemetry (CPU, Memory,
  // ZFS pool, Network RX/TX), Node heatmap matrix, CARP VIP ownership,
  // active jail workloads with 1-click Workbench docking, and Quotas.
  import {
    alertHref,
    api,
    bytes,
    follow,
    loadText,
    type Balancer,
    type LObject,
    type NodeUsage,
    type Status
  } from '#lib/api.ts';
  import { byteCount, cores } from '#lib/quantity.ts';
  import { tone } from '#lib/columns.ts';
  import Quotas from '#lib/Quotas.svelte';
  import UsageCharts from '#lib/UsageCharts.svelte';
  import { t, tn } from '#lib/i18n.svelte.ts';
  import { cannotWrite, openWorkbench, session } from '#lib/session.svelte.ts';
  import Ico from '#lib/Ico.svelte';
  import { CircleCheck, OctagonX, TriangleExclamationPoint } from '@lucide/svelte';

  let status: Status | null = $state(null);
  let statusError = $state('');
  let nodes: LObject[] = $state([]);
  let services: LObject[] = $state([]);
  let tasks: LObject[] = $state([]);
  let volumes: LObject[] = $state([]);
  let nodeUsage: NodeUsage[] = $state([]);
  let balancers: Balancer[] = $state([]);
  let errors: Record<string, string> = $state({});
  let sampled: Date | null = $state(null);

  // The cluster's health in a word, and why: no leader or a critical alert
  // is unhealthy; a warning or a node not ready, degraded.
  const st = $derived(status as Status | null);
  const voters = $derived(st ? st.members.filter((m) => m.voter).length : 0);
  const quorum = $derived(Math.floor(voters / 2) + 1);
  const notReady = $derived(nodes.filter((n) => n.status?.state !== 'ready'));
  const managersOut = $derived(
    notReady.filter((n) => n.spec?.role === 'manager' || n.status?.role === 'manager').length
  );
  const critical = $derived(session.alerts.filter((a) => a.severity === 'critical').length);
  const warnings = $derived(session.alerts.length - critical);
  const health = $derived(
    (st && !st.leader) || critical
      ? 'bad'
      : warnings || notReady.length
        ? 'wait'
        : 'good'
  );

  $effect(() => {
    api
      .status()
      .then((s) => (status = s))
      .catch((e) => (statusError = e.message));
  });

  $effect(() => {
    const ns = session.namespace;
    const fail = (kind: string) => (e: Error) => (errors[kind] = e.message);
    const stops = [
      follow('Service', ns, (o) => (services = o), fail('Service')),
      follow('Task', ns, (o) => (tasks = o), fail('Task')),
      follow('Volume', ns, (o) => (volumes = o), fail('Volume'))
    ];
    if (session.me?.all) stops.push(follow('Node', '', (o) => (nodes = o), fail('Node')));
    return () => stops.forEach((s) => s());
  });

  // Live node CPU / Memory / ZFS / Network snapshot + CARP VIPs.
  $effect(() => {
    const ns = session.namespace;
    let stopped = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    const poll = async () => {
      try {
        const u = await api.usage(ns);
        if (!stopped) {
          nodeUsage = u.nodes;
          sampled = new Date();
        }
      } catch {
        // Ignore transient poll errors.
      }
      if (ns) {
        api
          .loadBalancers(ns)
          .then((b) => {
            if (!stopped) balancers = b.balancers;
          })
          .catch(() => {});
      } else {
        balancers = [];
      }
      if (!stopped) timer = setTimeout(poll, 5000);
    };
    poll();
    return () => {
      stopped = true;
      if (timer) clearTimeout(timer);
    };
  });

  function count(objects: LObject[], of: (o: LObject) => string): [string, number][] {
    const by = new Map<string, number>();
    for (const o of objects) by.set(of(o) || '—', (by.get(of(o) || '—') ?? 0) + 1);
    return [...by.entries()].sort();
  }

  const live = $derived(tasks.filter((task) => task.spec?.desiredState === 'running'));
  const usageByNode = $derived(new Map(nodeUsage.map((n) => [n.node, n])));

  const totals = $derived.by(() => {
    let cpu = 0;
    let mem = 0;
    let zfsUsed = 0;
    let zfsAvail = 0;
    let rx = 0;
    let tx = 0;
    for (const n of nodeUsage) {
      for (const tk of n.tasks) {
        cpu += tk.cpu_percent;
        mem += tk.memory_bytes;
      }
      zfsUsed += n.zfs_used_bytes ?? 0;
      zfsAvail += n.zfs_avail_bytes ?? 0;
      rx += n.net_rx_bps ?? 0;
      tx += n.net_tx_bps ?? 0;
    }
    return { cpu, mem, zfsUsed, zfsAvail, rx, tx };
  });

  // What the cluster has, against what its tasks use: the nodes' CPU and
  // memory for tasks, and their pools' space.
  const capacity = $derived.by(() => {
    let cpu = 0;
    let mem = 0;
    let disk = 0;
    let diskUsed = 0;
    for (const n of nodes) {
      cpu += cores(n.status?.allocatable?.cpu);
      mem += byteCount(n.status?.allocatable?.memory);
      for (const p of (n.status?.pools ?? []) as { sizeBytes: number; allocBytes: number }[]) {
        disk += p.sizeBytes ?? 0;
        diskUsed += p.allocBytes ?? 0;
      }
    }
    return { cpu, mem, disk, diskUsed };
  });
  const pct = (a: number, b: number) => (b > 0 ? Math.round((a / b) * 100) : 0);

  function dockLogs(task: LObject) {
    const ns = task.metadata.namespace ?? '';
    openWorkbench({
      id: `logs:${ns}/${task.metadata.name}`,
      kind: 'logs',
      title: `${task.metadata.name} · ${t('Logs')}`,
      namespace: ns,
      name: task.metadata.name,
      object: task
    });
  }

  function dockShell(task: LObject) {
    const ns = task.metadata.namespace ?? '';
    openWorkbench({
      id: `term:${ns}/${task.metadata.name}`,
      kind: 'terminal',
      title: `${task.metadata.name} · ${t('Terminal')}`,
      namespace: ns,
      name: task.metadata.name,
      object: task
    });
  }
</script>

<header class="bar">
  <h1>{t('Overview')}</h1>
  {#if sampled}
    <span class="tag" title={t('When what nodes report was last read')}
      >{t('sampled {time}', { time: sampled.toLocaleTimeString() })}</span
    >
  {/if}
  <div class="controls">
    <a class="button sm" href="/volumes">{t('Volumes Studio')}</a>
    <a class="button sm" href="/images">{t('Images & GHCR Hub')}</a>
    <a class="button sm primary" href="/deploy">{t('Deploy')}</a>
  </div>
</header>

<section class="card health {health}" aria-labelledby="health-title">
  <div class="health-word">
    <Ico icon={health === 'good' ? CircleCheck : health === 'wait' ? TriangleExclamationPoint : OctagonX} size={36} />
    <div>
      <h2 id="health-title">
        {health === 'good' ? t('Healthy') : health === 'wait' ? t('Degraded') : t('Unhealthy')}
      </h2>
      <p class="muted">
        {#if status && !status.leader}
          {t('No manager leads: the cluster takes no changes until a majority of managers reach each other.')}
        {:else if critical || warnings}
          {t('{critical} critical and {warnings} warning alerts: see them below.', { critical, warnings })}
        {:else if notReady.length}
          {tn('1 node not ready.', '{n} nodes not ready.', notReady.length)}
        {:else}
          {t('Every node ready, a leader elected, nothing to act on.')}
        {/if}
      </p>
    </div>
  </div>
  <dl class="health-facts">
    {#if status}
      <div>
        <dt>{t('Quorum')}</dt>
        <dd>
          {tn('1 manager, quorum {quorum}, can lose {spare}', '{n} managers, quorum {quorum}, can lose {spare}', voters, {
            quorum,
            spare: Math.max(0, voters - quorum)
          })}{#if managersOut}<span class="wait">{t(', {n} not ready', { n: managersOut })}</span>{/if}
        </dd>
      </div>
    {/if}
    {#if session.me?.all}
      <div>
        <dt>{t('Nodes')}</dt>
        <dd>
          <span class="good">{t('{n} ready', { n: nodes.length - notReady.length })}</span>
          {#if notReady.length}· <span class="bad">{t('{n} not', { n: notReady.length })}</span>{/if}
        </dd>
      </div>
    {/if}
    {#if session.me?.all && capacity.cpu > 0}
      <div>
        <dt>{t('CPU')}</dt>
        <dd title={t('What tasks use now, of the CPU the nodes offer them')}>
          {t('{pct}% of {n} cores', { pct: pct(totals.cpu / 100, capacity.cpu), n: capacity.cpu })}
        </dd>
      </div>
      <div>
        <dt>{t('Memory')}</dt>
        <dd>{t('{used} of {total}', { used: bytes(totals.mem), total: bytes(capacity.mem) })}</dd>
      </div>
      {#if capacity.disk > 0}
        <div>
          <dt>{t('Disk')}</dt>
          <dd title={t('All the nodes’ pools together')}>
            {t('{pct}% of {total}', { pct: pct(capacity.diskUsed, capacity.disk), total: bytes(capacity.disk) })}
          </dd>
        </div>
      {/if}
    {/if}
    <div>
      <dt>{t('Alerts')}</dt>
      <dd>
        {#if session.alerts.length}<a href="/alerts">{t('{n} open', { n: session.alerts.length })}</a>{:else}{t('none')}{/if}
      </dd>
    </div>
  </dl>
</section>

{#if session.alerts.length}
  <section class="card alert-strip">
    <h2><a href="/alerts">{t('Alerts')}</a></h2>
    <ul class="plain">
      {#each session.alerts.slice(0, 5) as a (a.kind + a.namespace + a.object)}
        <li>
          <span class="state {a.severity === 'critical' ? 'bad' : 'wait'}">{a.severity}</span>
          <a href={alertHref(a)}>{a.message}</a>
        </li>
      {/each}
    </ul>
    {#if session.alerts.length > 5}
      <p><a href="/alerts">{t('All {n}', { n: session.alerts.length })}</a></p>
    {/if}
  </section>
{/if}

<section class="cards">
  <div class="card">
    <h2>{t('Managers')}</h2>
    {#if status}
      <p>
        <span class="state {status.leader ? 'good' : 'bad'}"
          >{status.leader ? t('leader elected') : t('no leader')}</span
        >
        {t('term {term}, store version {version}', {
          term: status.term,
          version: status.version
        })}
      </p>
      <ul class="plain">
        {#each status.members as m (m.id)}
          <li class="mono">
            {m.addr}
            {#if m.id === status.leader}<span class="tag">{t('leader')}</span>{/if}
            {#if !m.voter}<span class="tag">{t('learner')}</span>{/if}
          </li>
        {/each}
      </ul>
    {:else if statusError}
      <p class="error" role="alert">{statusError}</p>
    {:else}
      <p>…</p>
    {/if}
  </div>

  {#if session.me?.all}
    <div class="card">
      <h2><a href="/k/Node">{t('Nodes')}</a></h2>
      <p class="big">{nodes.length}</p>
      {#each count(nodes, (n) => n.status?.state) as [state, n] (state)}
        <span class="state {tone(state)}">{n} {state}</span>
      {/each}
      {#if errors.Node}<p class="error" role="alert">{errors.Node}</p>{/if}
    </div>
  {/if}

  <div class="card">
    <h2><a href="/k/Service">{t('Services')}</a></h2>
    <p class="big">{services.length}</p>
    <p class="muted">
      <a href="/volumes">{tn('1 ZFS/storage volume', '{n} ZFS/storage volumes', volumes.length)}</a>
    </p>
    {#if errors.Service}<p class="error" role="alert">{errors.Service}</p>{/if}
  </div>

  <div class="card">
    <h2><a href="/k/Task">{t('Tasks')}</a></h2>
    <p class="big">{live.length}</p>
    {#each count(live, (task) => task.status?.state) as [state, n] (state)}
      <span class="state {tone(state)}">{n} {state}</span>
    {/each}
    {#if errors.Task}<p class="error" role="alert">{errors.Task}</p>{/if}
  </div>

  <div class="card">
    <h2>{t('Live telemetry')}</h2>
    <dl class="facts compact-facts">
      <dt>{t('CPU')}</dt>
      <dd class="mono">{totals.cpu.toFixed(1)}%</dd>
      <dt>{t('Memory')}</dt>
      <dd class="mono">{bytes(totals.mem)}</dd>
      {#if totals.zfsUsed + totals.zfsAvail > 0}
        <dt>{t('ZFS pool')}</dt>
        <dd class="mono">
          {bytes(totals.zfsUsed)} / {bytes(totals.zfsUsed + totals.zfsAvail)}
        </dd>
      {/if}
      {#if totals.rx + totals.tx > 0}
        <dt>{t('Net RX / TX')}</dt>
        <dd class="mono">{bytes(totals.rx)}/s · {bytes(totals.tx)}/s</dd>
      {/if}
    </dl>
  </div>
</section>

<h2>{t('Telemetry history (1h)')}</h2>
<UsageCharts namespace={session.namespace} compact={true} />

{#if session.me?.all && nodes.length}
  <h2>{t('Node heatmap')}</h2>
  <section class="cards">
    {#each nodes as n (n.metadata.name)}
      {@const nu = usageByNode.get(n.metadata.name)}
      {@const nCpu = nu ? nu.tasks.reduce((a, x) => a + x.cpu_percent, 0) : 0}
      {@const nMem = nu ? nu.tasks.reduce((a, x) => a + x.memory_bytes, 0) : 0}
      {@const zUsed = nu?.zfs_used_bytes ?? 0}
      {@const zTot = zUsed + (nu?.zfs_avail_bytes ?? 0)}
      {@const zPct = zTot > 0 ? Math.min(100, Math.round((zUsed / zTot) * 100)) : 0}
      <div class="card node-tile">
        <div class="node-head">
          <strong><a href="/k/Node/_/{n.metadata.name}">{n.metadata.name}</a></strong>
          <span class="state {tone(n.status?.state)}">{n.status?.state ?? '—'}</span>
        </div>
        <p class="muted mono node-sub">
          {n.spec?.role ?? 'worker'} · {n.status?.addr ?? ''}
          {#if n.spec?.availability && n.spec.availability !== 'active'}
            · <span class="wait">{n.spec.availability}</span>
          {/if}
        </p>
        <dl class="facts compact-facts">
          <dt>{t('CPU')}</dt>
          <dd class="mono">{nCpu.toFixed(1)}%</dd>
          <dt>{t('Memory')}</dt>
          <dd class="mono">{bytes(nMem)}</dd>
          {#if loadText(nu?.load)}
            <dt>{t('Load')}</dt>
            <dd class="mono" title={t('Over 1, 5 and 15 minutes')}>{loadText(nu?.load)}</dd>
          {/if}
          <dt>{t('Tasks')}</dt>
          <dd class="mono">{nu?.tasks.length ?? 0}</dd>
          {#if zTot > 0}
            <dt>{t('ZFS')}</dt>
            <dd class="mono">{bytes(zUsed)} ({zPct}%)</dd>
          {/if}
          {#if (nu?.net_rx_bps ?? 0) + (nu?.net_tx_bps ?? 0) > 0}
            <dt>{t('Net')}</dt>
            <dd class="mono">↓{bytes(nu?.net_rx_bps ?? 0)}/s ↑{bytes(nu?.net_tx_bps ?? 0)}/s</dd>
          {/if}
        </dl>
        {#if zTot > 0}
          <div class="meter" role="img" aria-label={t('ZFS usage {pct}%', { pct: zPct })}>
            <div
              class="meter-fill {zPct >= 90 ? 'bad' : zPct >= 75 ? 'wait' : 'zfs'}"
              style:width="{zPct}%"
            ></div>
          </div>
        {/if}
      </div>
    {/each}
  </section>
{/if}

{#if balancers.length}
  <h2>{t('CARP virtual IPs')}</h2>
  <table>
    <caption class="sr-only">{t('CARP load balancers')}</caption>
    <thead>
      <tr>
        <th scope="col">{t('Service')}</th>
        <th scope="col">{t('Address')}</th>
        <th scope="col">{t('VHID')}</th>
        <th scope="col">{t('Nodes')}</th>
      </tr>
    </thead>
    <tbody>
      {#each balancers as b (b.service + b.address)}
        <tr>
          <td><a href="/k/Service/{session.namespace}/{b.service}">{b.service}</a></td>
          <td class="mono">{b.address}</td>
          <td class="mono">{b.vhid}</td>
          <td>
            {#each b.nodes as n (n.node)}
              <span class="state {n.state === 'master' ? 'good' : 'wait'}">
                {n.node} ({n.state || 'init'}, skew {n.skew})
              </span>
            {/each}
          </td>
        </tr>
      {/each}
    </tbody>
  </table>
{/if}

{#if live.some((task) => task.status?.state === 'pending' || task.status?.state === 'failed')}
  <h2>{t('Needs attention')}</h2>
  <table>
    <caption class="sr-only">{t('Tasks needing attention')}</caption>
    <thead>
      <tr>
        <th scope="col">{t('Task')}</th>
        <th scope="col">{t('State')}</th>
        <th scope="col">{t('Why')}</th>
        <th scope="col">{t('Workbench')}</th>
      </tr>
    </thead>
    <tbody>
      {#each live.filter((task) => ['pending', 'failed'].includes(task.status?.state)) as task (task.metadata.uid)}
        <tr>
          <td>
            <a href="/k/Task/{task.metadata.namespace}/{task.metadata.name}">
              {task.metadata.namespace}/{task.metadata.name}
            </a>
          </td>
          <td><span class="state {tone(task.status?.state)}">{task.status?.state}</span></td>
          <td>{task.status?.message ?? ''}</td>
          <td>
            <button
              type="button"
              class="sm"
              data-look
              onclick={() => dockLogs(task)}
              aria-label={t('Logs for {name}', { name: task.metadata.name })}
            >
              {t('Logs')}
            </button>
          </td>
        </tr>
      {/each}
    </tbody>
  </table>
{/if}

{#if live.some((task) => task.status?.state === 'running')}
  <h2>{t('Running jail tasks')}</h2>
  <table>
    <caption class="sr-only">{t('Running jail tasks')}</caption>
    <thead>
      <tr>
        <th scope="col">{t('Task')}</th>
        <th scope="col">{t('Service')}</th>
        <th scope="col">{t('On node')}</th>
        <th scope="col">{t('Address')}</th>
        <th scope="col">{t('Workbench')}</th>
      </tr>
    </thead>
    <tbody>
      {#each live.filter((task) => task.status?.state === 'running').slice(0, 12) as task (task.metadata.uid)}
        <tr>
          <td>
            <a href="/k/Task/{task.metadata.namespace}/{task.metadata.name}">
              {task.metadata.namespace}/{task.metadata.name}
            </a>
          </td>
          <td>{task.spec?.service ?? '—'}</td>
          <td class="mono">{task.spec?.node ?? '—'}</td>
          <td class="mono">{task.status?.ip ?? '—'}</td>
          <td class="controls">
            <button
              type="button"
              class="sm"
              data-look
              onclick={() => dockLogs(task)}
              aria-label={t('Logs for {name}', { name: task.metadata.name })}
            >
              {t('Logs')}
            </button>
            <button
              type="button"
              class="sm"
              disabled={!!cannotWrite(task.metadata.namespace ?? '')}
              title={cannotWrite(task.metadata.namespace ?? '') ?? ''}
              onclick={() => dockShell(task)}
              aria-label={t('Shell in {name}', { name: task.metadata.name })}
            >
              {t('Shell')}
            </button>
          </td>
        </tr>
      {/each}
    </tbody>
  </table>
{/if}

<h2>{t('Quotas')}</h2>
<Quotas namespace={session.namespace} />

<style>
  .alert-strip {
    margin-bottom: 0.85rem;
    border-color: var(--wait);
  }
  .compact-facts {
    font-size: 0.82rem;
    gap: 0.2rem 0.9rem;
    margin: 0.25rem 0 0.45rem;
  }
  .node-head {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 0.5rem;
  }
  .node-sub {
    font-size: 0.78rem;
    margin: 0.15rem 0 0.4rem;
  }
</style>

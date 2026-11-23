<script lang="ts">
  // The Overview: the cluster's health in a word; a status card for each
  // thing it has (nodes, managers, workloads, CPU, memory, storage, pool
  // I/O, network); their history over a range picked; each node's share used;
  // CARP addresses; tasks needing attention and those running; quotas.
  import {
    alertHref,
    api,
    bytes,
    follow,
    followLoadBalancers,
    followStatus,
    followUsage,
    loadText,
    type HaReport,
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
  import Tile from '#lib/Tile.svelte';
  import { Table } from '#lib/table.svelte.ts';
  import SortTh from '#lib/SortTh.svelte';
  import TableBar from '#lib/TableBar.svelte';
  import Pager from '#lib/Pager.svelte';
  import {
    Activity,
    ArrowDownUp,
    Boxes,
    CircleCheck,
    Cpu,
    Crown,
    LifeBuoy,
    HardDrive,
    MemoryStick,
    OctagonX,
    Server,
    TriangleExclamationPoint
  } from '@lucide/svelte';

  let status: Status | null = $state(null);
  let statusError = $state('');
  // The single points of failure (`legion ha`), worked out again a moment
  // after what they rest on changes.
  let availability = $state<HaReport | null>(null);
  const availabilityRisks = $derived(
    availability?.findings.filter((f) => f.severity === 'risk').length ?? 0
  );
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

  $effect(() =>
    followStatus(
      (s) => {
        status = s;
        statusError = '';
      },
      (why) => {
        if (why) statusError = why;
      }
    )
  );

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

  $effect(() => {
    const ns = session.namespace;
    // Read again a second after the services, Volumes or nodes change.
    void [services, volumes, nodes];
    const soon = setTimeout(() => {
      api
        .availability(ns)
        .then((r) => (availability = r))
        .catch(() => (availability = null));
    }, 1000);
    return () => clearTimeout(soon);
  });

  // What nodes and tasks use now, as each report comes.
  $effect(() =>
    followUsage(session.namespace, (nodes) => {
      nodeUsage = nodes;
      sampled = new Date();
    })
  );
  // The namespace's virtual addresses and who holds them, as CARP moves
  // them.
  $effect(() => {
    const ns = session.namespace;
    balancers = [];
    if (!ns) return;
    return followLoadBalancers(ns, (b) => (balancers = b));
  });

  function count(objects: LObject[], of: (o: LObject) => string): [string, number][] {
    const by = new Map<string, number>();
    for (const o of objects) by.set(of(o) || '—', (by.get(of(o) || '—') ?? 0) + 1);
    return [...by.entries()].sort();
  }

  const live = $derived(tasks.filter((task) => task.spec?.desiredState === 'running'));

  // The tables under the cards: the CARP addresses by Service; the tasks
  // needing attention, the failed before the pending; the running tasks by
  // name, twelve a page. An address sorts by its numbers.
  const taskName = (task: LObject) => `${task.metadata.namespace ?? ''}/${task.metadata.name}`;
  const vipTable = new Table(() => balancers, {
    sorts: {
      service: (b) => b.service,
      address: (b) => b.address,
      vhid: (b) => b.vhid,
      nodes: (b) => b.nodes.find((n) => n.state === 'master')?.node ?? ''
    },
    sortBy: 'service',
    text: (b) => `${b.service} ${b.address} ${b.vhid} ${b.nodes.map((n) => n.node).join(' ')}`
  });
  const attention = $derived(live.filter((task) => ['pending', 'failed'].includes(task.status?.state)));
  const attentionTable = new Table(() => attention, {
    sorts: {
      task: taskName,
      state: (task) => task.status?.state ?? '',
      why: (task) => task.status?.message ?? ''
    },
    sortBy: 'state'
  });
  const running = $derived(live.filter((task) => task.status?.state === 'running'));
  const runningTable = new Table(() => running, {
    sorts: {
      task: taskName,
      service: (task) => task.spec?.service ?? '',
      node: (task) => task.spec?.node ?? '',
      address: (task) => task.status?.ip ?? ''
    },
    sortBy: 'task',
    size: 12
  });
  const usageByNode = $derived(new Map(nodeUsage.map((n) => [n.node, n])));

  const totals = $derived.by(() => {
    let cpu = 0;
    let mem = 0;
    let zfsUsed = 0;
    let zfsAvail = 0;
    let rx = 0;
    let tx = 0;
    let read = 0;
    let written = 0;
    let readOps = 0;
    let writeOps = 0;
    for (const n of nodeUsage) {
      for (const tk of n.tasks) {
        cpu += tk.cpu_percent;
        mem += tk.memory_bytes;
      }
      zfsUsed += n.zfs_used_bytes ?? 0;
      zfsAvail += n.zfs_avail_bytes ?? 0;
      rx += n.net_rx_bps ?? 0;
      tx += n.net_tx_bps ?? 0;
      read += n.zfs_read_bps ?? 0;
      written += n.zfs_write_bps ?? 0;
      readOps += n.zfs_read_iops ?? 0;
      writeOps += n.zfs_write_iops ?? 0;
    }
    return { cpu, mem, zfsUsed, zfsAvail, rx, tx, read, written, readOps, writeOps };
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
    <a class="button primary" href="/deploy">{t('Deploy')}</a>
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
          {t('{critical} critical and {warnings} warning alerts.', { critical, warnings })}
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
    <div>
      <dt>{t('Alerts')}</dt>
      <dd>
        {#if session.alerts.length}<a href="/alerts">{t('{n} open', { n: session.alerts.length })}</a>{:else}{t('none')}{/if}
      </dd>
    </div>
  </dl>
  {#if session.alerts.length}
    <!-- What is wrong, under the word for it: the first five, the rest a
         click away. -->
    <ul class="plain health-alerts" aria-label={t('Alerts')}>
      {#each session.alerts.slice(0, 5) as a (a.kind + a.namespace + a.object)}
        <li>
          <span class="state {a.severity === 'critical' ? 'bad' : 'wait'}">{a.severity}</span>
          <a href={alertHref(a)}>{a.message}</a>
        </li>
      {/each}
      {#if session.alerts.length > 5}
        <li><a href="/alerts">{t('All {n}', { n: session.alerts.length })}</a></li>
      {/if}
    </ul>
  {/if}
</section>

<!-- The cluster at a glance: a card for each thing it has, with the
     figure to read, what it means, a meter where it is a share, and where
     more is shown. -->
<div class="tiles">
  {#if session.me?.all}
    <Tile
      label={t('Nodes')}
      icon={Server}
      value={t('{ready} of {n} ready', { ready: nodes.length - notReady.length, n: nodes.length })}
      tone={notReady.length ? 'bad' : nodes.length ? 'good' : ''}
      note={notReady.length
        ? t('Not ready: {names}', { names: notReady.map((n) => n.metadata.name).join(', ') })
        : t('Every node takes tasks.')}
      href="/k/Node"
      more={t('All nodes')}
    />
  {/if}
  <Tile
    label={t('Managers')}
    icon={Crown}
    value={status ? (status.leader ? t('Leader elected') : t('No leader')) : '…'}
    tone={status ? (status.leader ? 'good' : 'bad') : ''}
    note={status
      ? t('{n} of them, term {term}, store version {version}', {
          n: status.members.length,
          term: status.term,
          version: status.version
        })
      : statusError}
    href={session.me?.all ? '/cluster' : ''}
    more={t('Operations')}
  >
    {#if status}
      <ul class="plain managers">
        {#each status.members as m (m.id)}
          <li>
            <span class="mono">{m.addr}</span>
            {#if m.id === status.leader}<span class="tag">{t('leader')}</span>{/if}
            {#if !m.voter}<span class="tag">{t('learner')}</span>{/if}
            {#if m.witness}<span class="tag">{t('witness')}</span>{/if}
          </li>
        {/each}
      </ul>
    {/if}
  </Tile>
  {#if availability}
    <Tile
      label={t('Availability')}
      icon={LifeBuoy}
      value={availabilityRisks
        ? tn('1 risk', '{n} risks', availabilityRisks)
        : availability.findings.length
          ? tn('1 warning', '{n} warnings', availability.findings.length)
          : t('No single point of failure')}
      tone={availabilityRisks ? 'bad' : availability.findings.length ? 'wait' : 'good'}
      note={availability.findings[0]?.message ?? t('Losing any one machine stops nothing.')}
      href="/availability"
      more={t('What and how to fix')}
    />
  {/if}
  <Tile
    label={t('Workloads')}
    icon={Boxes}
    value={tn('1 service', '{n} services', services.length)}
    note={[
      t('{n} tasks running', { n: live.filter((x) => x.status?.state === 'running').length }),
      ...count(live, (x) => x.status?.state)
        .filter(([state]) => state !== 'running')
        .map(([state, n]) => `${n} ${state}`),
      tn('1 Volume', '{n} Volumes', volumes.length)
    ].join(' · ')}
    tone={live.some((x) => x.status?.state === 'failed') ? 'bad' : ''}
    href="/k/Service"
    more={t('Services')}
  />
  {#if session.me?.all && capacity.cpu > 0}
    <Tile
      label={t('CPU')}
      icon={Cpu}
      value={t('{pct}% used', { pct: pct(totals.cpu / 100, capacity.cpu) })}
      note={t('{used} of {n} cores, by tasks now', { used: (totals.cpu / 100).toFixed(1), n: capacity.cpu })}
      pct={(totals.cpu / 100 / capacity.cpu) * 100}
      meterLabel={t('CPU used')}
      href="/k/Node"
      more={t('By node')}
    />
    <Tile
      label={t('Memory')}
      icon={MemoryStick}
      value={t('{pct}% used', { pct: pct(totals.mem, capacity.mem) })}
      note={t('{used} of {total}, by tasks now', { used: bytes(totals.mem), total: bytes(capacity.mem) })}
      pct={capacity.mem ? (totals.mem / capacity.mem) * 100 : 0}
      meterLabel={t('Memory used')}
      href="/k/Node"
      more={t('By node')}
    />
  {:else}
    <Tile
      label={t('CPU')}
      icon={Cpu}
      value={`${totals.cpu.toFixed(1)}%`}
      note={t('Of one CPU, by the tasks you can see')}
      href="/k/Task"
      more={t('Tasks')}
    />
    <Tile
      label={t('Memory')}
      icon={MemoryStick}
      value={bytes(totals.mem)}
      note={t('Used by the tasks you can see')}
      href="/k/Task"
      more={t('Tasks')}
    />
  {/if}
  {#if session.me?.all && capacity.disk > 0}
    <Tile
      label={t('Storage')}
      icon={HardDrive}
      value={t('{pct}% used', { pct: pct(capacity.diskUsed, capacity.disk) })}
      note={t('{used} of {total} in the nodes’ pools', { used: bytes(capacity.diskUsed), total: bytes(capacity.disk) })}
      pct={(capacity.diskUsed / capacity.disk) * 100}
      meterLabel={t('Pool space used')}
      href="/volumes"
      more={t('Volumes')}
    />
  {:else if totals.zfsUsed + totals.zfsAvail > 0}
    <Tile
      label={t('Storage')}
      icon={HardDrive}
      value={t('{pct}% used', { pct: pct(totals.zfsUsed, totals.zfsUsed + totals.zfsAvail) })}
      note={t('{used} of {total} in the pools', {
        used: bytes(totals.zfsUsed),
        total: bytes(totals.zfsUsed + totals.zfsAvail)
      })}
      pct={(totals.zfsUsed / (totals.zfsUsed + totals.zfsAvail)) * 100}
      meterLabel={t('Pool space used')}
      href="/volumes"
      more={t('Volumes')}
    />
  {/if}
  {#if totals.read + totals.written > 0}
    <Tile
      label={t('Pool I/O')}
      icon={Activity}
      tone="zfs"
      value={`${bytes(totals.read + totals.written)}/s`}
      note={t('{read}/s read, {written}/s written, {iops} IOPS, over every node’s pool now', {
        read: bytes(totals.read),
        written: bytes(totals.written),
        iops: Math.round(totals.readOps + totals.writeOps)
      })}
      href="/disk"
      more={t('Disk usage')}
    />
  {/if}
  {#if totals.rx + totals.tx > 0}
    <Tile
      label={t('Network')}
      icon={ArrowDownUp}
      value={`${bytes(totals.rx + totals.tx)}/s`}
      note={t('{rx}/s in, {tx}/s out, over every node now', { rx: bytes(totals.rx), tx: bytes(totals.tx) })}
      href="/network"
      more={t('Network')}
    />
  {/if}
</div>

<UsageCharts namespace={session.namespace} compact={true} heading={t('History')} />

{#if session.me?.all && nodes.length}
  <h2>{t('Nodes')}</h2>
  <!-- As wide as the page gives a few nodes, not left narrow at the side. -->
  <section class="node-cards" style:max-width="{Math.max(nodes.length, 2) * 30}rem">
    {#each nodes as n (n.metadata.name)}
      {@const nu = usageByNode.get(n.metadata.name)}
      {@const nCpu = nu ? nu.tasks.reduce((a, x) => a + x.cpu_percent, 0) : 0}
      {@const nMem = nu ? nu.tasks.reduce((a, x) => a + x.memory_bytes, 0) : 0}
      {@const cpuCap = cores(n.status?.allocatable?.cpu)}
      {@const memCap = byteCount(n.status?.allocatable?.memory)}
      {@const zUsed = nu?.zfs_used_bytes ?? 0}
      {@const zTot = zUsed + (nu?.zfs_avail_bytes ?? 0)}
      {@const shares = [
        { label: t('CPU'), text: cpuCap ? `${pct(nCpu / 100, cpuCap)}%` : `${nCpu.toFixed(1)}%`, share: cpuCap ? (nCpu / 100 / cpuCap) * 100 : undefined },
        { label: t('Memory'), text: bytes(nMem), share: memCap ? (nMem / memCap) * 100 : undefined },
        ...(zTot > 0 ? [{ label: t('ZFS'), text: `${bytes(zUsed)} / ${bytes(zTot)}`, share: (zUsed / zTot) * 100 }] : [])
      ]}
      <div class="card node-tile">
        <div class="node-head">
          <h3><a href="/k/Node/_/{n.metadata.name}">{n.metadata.name}</a></h3>
          <span class="state {tone(n.status?.state)}">{n.status?.state ?? '—'}</span>
        </div>
        <ul class="plain muted node-sub">
          <li>{n.spec?.role ?? 'worker'}</li>
          {#if n.status?.addr}<li class="mono">{n.status.addr}</li>{/if}
          {#if n.spec?.availability && n.spec.availability !== 'active'}
            <li class="wait">{n.spec.availability}</li>
          {/if}
        </ul>
        <ul class="plain node-meters">
          {#each shares as m (m.label)}
            <li>
              <span class="what">{m.label}</span>
              <span class="figure">{m.text}</span>
              {#if m.share !== undefined}
                {@const f = Math.min(100, Math.max(0, m.share))}
                <div
                  class="meter"
                  role="meter"
                  aria-valuemin={0}
                  aria-valuemax={100}
                  aria-valuenow={Math.round(f)}
                  aria-label={t('{what} on {node}', { what: m.label, node: n.metadata.name })}
                >
                  <div class="meter-fill {f >= 90 ? 'bad' : f >= 75 ? 'wait' : 'good'}" style:width="{f}%"></div>
                </div>
              {/if}
            </li>
          {/each}
        </ul>
        <ul class="plain muted node-sub">
          <li>{tn('1 task', '{n} tasks', nu?.tasks.length ?? 0)}</li>
          {#if loadText(nu?.load)}
            <li title={t('Over 1, 5 and 15 minutes')}>{t('load {load}', { load: loadText(nu?.load) })}</li>
          {/if}
          {#if (nu?.net_rx_bps ?? 0) + (nu?.net_tx_bps ?? 0) > 0}
            <li title={t('Received and sent')}>↓{bytes(nu?.net_rx_bps ?? 0)}/s ↑{bytes(nu?.net_tx_bps ?? 0)}/s</li>
          {/if}
        </ul>
      </div>
    {/each}
  </section>
{/if}

{#if balancers.length}
  <h2>{t('CARP virtual IPs')}</h2>
  <TableBar table={vipTable} label={t('Search virtual IPs')} />
  <table>
    <caption class="sr-only">{t('CARP load balancers')}</caption>
    <thead>
      <tr>
        <SortTh table={vipTable} col="service">{t('Service')}</SortTh>
        <SortTh table={vipTable} col="address">{t('Address')}</SortTh>
        <SortTh table={vipTable} col="vhid">{t('VHID')}</SortTh>
        <SortTh table={vipTable} col="nodes">{t('Nodes')}</SortTh>
      </tr>
    </thead>
    <tbody>
      {#each vipTable.rows as b (b.service + b.address)}
        <tr>
          <td><a href="/k/Service/{session.namespace}/{b.service}">{b.service}</a></td>
          <td class="mono">{b.address}</td>
          <td>{b.vhid}</td>
          <td>
            {#each b.nodes as n (n.node)}
              <span class="state {n.state === 'master' ? 'good' : 'wait'}">
                {n.node} ({n.state || 'init'}, skew {n.skew})
              </span>
            {/each}
          </td>
        </tr>
      {:else}
        <tr><td colspan="4" class="muted">{t('Nothing matches.')}</td></tr>
      {/each}
    </tbody>
  </table>
  <Pager table={vipTable} label={t('Virtual IPs')} />
{/if}

{#if attention.length}
  <h2>{t('Needs attention')}</h2>
  <TableBar table={attentionTable} label={t('Search tasks needing attention')} />
  <table>
    <caption class="sr-only">{t('Tasks needing attention')}</caption>
    <thead>
      <tr>
        <SortTh table={attentionTable} col="task">{t('Task')}</SortTh>
        <SortTh table={attentionTable} col="state">{t('State')}</SortTh>
        <SortTh table={attentionTable} col="why">{t('Why')}</SortTh>
        <th scope="col">{t('Workbench')}</th>
      </tr>
    </thead>
    <tbody>
      {#each attentionTable.rows as task (task.metadata.uid)}
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
      {:else}
        <tr><td colspan="4" class="muted">{t('Nothing matches.')}</td></tr>
      {/each}
    </tbody>
  </table>
  <Pager table={attentionTable} label={t('Tasks needing attention')} />
{/if}

{#if running.length}
  <h2>{t('Running jail tasks')}</h2>
  <TableBar table={runningTable} label={t('Search running tasks')} />
  <table>
    <caption class="sr-only">{t('Running jail tasks')}</caption>
    <thead>
      <tr>
        <SortTh table={runningTable} col="task">{t('Task')}</SortTh>
        <SortTh table={runningTable} col="service">{t('Service')}</SortTh>
        <SortTh table={runningTable} col="node">{t('On node')}</SortTh>
        <SortTh table={runningTable} col="address">{t('Address')}</SortTh>
        <th scope="col">{t('Workbench')}</th>
      </tr>
    </thead>
    <tbody>
      {#each runningTable.rows as task (task.metadata.uid)}
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
      {:else}
        <tr><td colspan="5" class="muted">{t('Nothing matches.')}</td></tr>
      {/each}
    </tbody>
  </table>
  <Pager table={runningTable} label={t('Running jail tasks')} />
{/if}

<h2>{t('Quotas')}</h2>
<Quotas namespace={session.namespace} />

<style>
  .health-alerts {
    flex-basis: 100%;
    display: flex;
    flex-direction: column;
    gap: 0.35rem;
    padding-top: 0.75rem;
    border-top: 1px solid var(--line);
  }
  .health-alerts .state {
    margin: 0 0.5rem 0 0;
  }
  .node-head {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 0.5rem;
  }
  .node-head h3 {
    margin: 0;
    font-size: 1rem;
  }
  .node-head h3 a {
    color: var(--text);
    text-decoration: none;
  }
  .node-head h3 a:hover {
    color: var(--link);
    text-decoration: underline;
  }
  .node-cards {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(17rem, 1fr));
    gap: 0.85rem;
  }
  /* A node's facts, each whole on its line: a role, an address, how it
     takes tasks; its tasks, load and traffic. */
  .node-sub {
    display: flex;
    flex-wrap: wrap;
    gap: 0.1rem 0.9rem;
    font-size: 0.8rem;
    margin: 0.15rem 0 0.6rem;
  }
  .node-sub > li {
    white-space: nowrap;
  }
  .node-meters {
    display: grid;
    gap: 0.5rem;
    margin: 0 0 0.5rem;
  }
  .node-meters > li {
    display: grid;
    grid-template-columns: 1fr auto;
    gap: 0.15rem 0.5rem;
    font-size: 0.84rem;
  }
  .node-meters .what {
    color: var(--muted);
  }
  .node-meters .figure {
    font-weight: 600;
  }
  .node-meters .meter {
    grid-column: 1 / -1;
  }
  .managers {
    font-size: 0.8rem;
    margin-top: 0.2rem;
  }
</style>

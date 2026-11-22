<script lang="ts">
  import { ask } from '#lib/confirm.svelte.ts';
  // The Volumes page: per-node ZFS pool capacity meters,
  // 1-click ZFS/NFS/iSCSI volume creation wizard, live per-dataset space
  // tracking, ZFS Snapshot Manager modal, cross-node ZFS migration, and
  // streaming .zfs backup download & restore.
  import { droppable } from '#lib/drop.svelte.ts';
  import {
    api,
    bytes,
    downloads,
    follow,
    type LObject,
    type NodeUsage,
    type VolumeSpace
  } from '#lib/api.ts';
  import { t, tn } from '#lib/i18n.svelte.ts';
  import { cannotWrite, cannotWriteAnywhere, namespaces, session } from '#lib/session.svelte.ts';
  import Tile from '#lib/Tile.svelte';
  import { Table } from '#lib/table.svelte.ts';
  import SortTh from '#lib/SortTh.svelte';
  import TableBar from '#lib/TableBar.svelte';
  import Pager from '#lib/Pager.svelte';
  import { Activity, Database, HardDrive, ShieldCheck } from '@lucide/svelte';

  let adminNamespaces: string[] = $state([]);
  let volumes: LObject[] = $state([]);
  let services: LObject[] = $state([]);
  let nodes: LObject[] = $state([]);
  let nodeUsage: NodeUsage[] = $state([]);
  let error = $state('');
  let message = $state('');
  let busy = $state(false);

  $effect(() => {
    if (session.me?.all) {
      api
        .list('Namespace')
        .then((r) => (adminNamespaces = r.objects.map((o) => o.metadata.name).sort()))
        .catch(() => {});
    }
  });

  const nsChoices = $derived(namespaces(adminNamespaces));

  $effect(() => {
    const ns = session.namespace;
    const fail = (e: Error) => (error = e.message);
    const stops = [
      follow('Volume', ns, (o) => (volumes = o), fail),
      follow('Service', ns, (o) => (services = o), () => {})
    ];
    if (session.me?.all) stops.push(follow('Node', '', (o) => (nodes = o), () => {}));
    return () => stops.forEach((s) => s());
  });

  $effect(() => {
    const ns = session.namespace;
    let stopped = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    const poll = async () => {
      try {
        const u = await api.usage(ns);
        if (!stopped) nodeUsage = u.nodes;
      } catch {
        // Ignore transient usage poll error.
      }
      if (!stopped) timer = setTimeout(poll, 6000);
    };
    poll();
    return () => {
      stopped = true;
      if (timer) clearTimeout(timer);
    };
  });

  const spaceMap = $derived.by(() => {
    const m = new Map<string, VolumeSpace>();
    for (const nu of nodeUsage) {
      for (const vs of nu.volumes ?? []) {
        m.set(`${vs.namespace}/${vs.volume}`, vs);
      }
    }
    return m;
  });

  const attachedBy = $derived.by(() => {
    const m = new Map<string, string[]>();
    for (const s of services) {
      const ns = s.metadata.namespace ?? '';
      const vols = (s.spec?.pod?.volumes ?? []) as { volume?: { name: string } }[];
      for (const v of vols) {
        if (!v.volume?.name) continue;
        const k = `${ns}/${v.volume.name}`;
        m.set(k, [...(m.get(k) ?? []), s.metadata.name]);
      }
    }
    return m;
  });

  // The Volumes by namespace and name; the space used sorts as a number,
  // a Volume with none reported below every one that has; "Mounted by" on
  // the Services' names, the detached first. A search looks in the names,
  // the node, the driver and the Services.
  const key = (v: LObject) => `${v.metadata.namespace ?? ''}/${v.metadata.name}`;
  const nodeOf = (v: LObject) => String(v.status?.node ?? v.spec?.node ?? '');
  const table = new Table(() => volumes, {
    sorts: {
      name: key,
      driver: (v) => String(v.spec?.driver ?? 'zfs'),
      node: nodeOf,
      used: (v) => spaceMap.get(key(v))?.used_bytes ?? -1,
      mounted: (v) => (attachedBy.get(key(v)) ?? []).join(', ')
    },
    sortBy: 'name',
    text: (v) =>
      `${key(v)} ${nodeOf(v)} ${v.spec?.driver ?? 'zfs'} ${(attachedBy.get(key(v)) ?? []).join(' ')}`
  });

  // Create Volume dialog
  let createDialog: HTMLDialogElement | undefined = $state();
  let cNamespace = $state('');
  let cName = $state('');
  let cDriver: 'zfs' | 'nfs' | 'iscsi' = $state('zfs');
  let cSize = $state('');
  let cNode = $state('');
  let cNfsServer = $state('');
  let cNfsPath = $state('');
  let cIscsiPortal = $state('');
  let cIscsiTarget = $state('');
  let cIscsiLun = $state(0);

  function openCreate() {
    cNamespace = session.namespace || nsChoices[0] || 'default';
    cName = '';
    cDriver = 'zfs';
    cSize = '';
    cNode = '';
    createDialog?.showModal();
  }

  function lightDismiss(e: MouseEvent, dlg: HTMLDialogElement | undefined) {
    if (!dlg || 'closedBy' in HTMLDialogElement.prototype) return;
    if (e.target !== dlg) return;
    const r = dlg.getBoundingClientRect();
    const inside =
      r.top <= e.clientY &&
      e.clientY <= r.top + r.height &&
      r.left <= e.clientX &&
      e.clientX <= r.left + r.width;
    if (!inside) dlg.close();
  }

  async function submitCreate(e: SubmitEvent) {
    e.preventDefault();
    busy = true;
    error = '';
    message = '';
    const spec: Record<string, unknown> = { driver: cDriver };
    if (cDriver === 'zfs') {
      if (cSize.trim()) spec.size = cSize.trim();
      if (cNode.trim()) spec.node = cNode.trim();
    } else if (cDriver === 'nfs') {
      spec.nfs = { server: cNfsServer.trim(), path: cNfsPath.trim() };
    } else if (cDriver === 'iscsi') {
      spec.iscsi = { portal: cIscsiPortal.trim(), target: cIscsiTarget.trim(), lun: cIscsiLun };
    }
    try {
      await api.apply({
        object: {
          apiVersion: 'legion/v1',
          kind: 'Volume',
          metadata: { name: cName.trim(), namespace: cNamespace.trim() },
          spec
        }
      });
      createDialog?.close();
      message = t('Created Volume {namespace}/{name}', {
        namespace: cNamespace.trim(),
        name: cName.trim()
      });
    } catch (err) {
      error = (err as Error).message;
    } finally {
      busy = false;
    }
  }

  // ZFS Snapshot Manager & Live Migrate modal
  let snapDialog: HTMLDialogElement | undefined = $state();
  let activeVol: LObject | null = $state(null);
  let snapshots: string[] = $state([]);
  let snapInput = $state('');
  let migrateNode = $state('');
  let snapBusy = $state(false);
  let snapError = $state('');
  // In the order ZFS made them, oldest first, until sorted by name.
  const snapTable = new Table(() => snapshots, { sorts: { name: (s) => s } });

  async function openManage(v: LObject) {
    activeVol = v;
    snapshots = [];
    snapTable.search('');
    snapInput = '';
    migrateNode = v.spec?.node ?? '';
    snapError = '';
    snapDialog?.showModal();
    if ((v.spec?.driver ?? 'zfs') === 'zfs' && v.status?.node && !v.status?.lost) {
      snapBusy = true;
      try {
        const r = await api.volumeSnapshots(v.metadata.namespace ?? '', v.metadata.name);
        snapshots = r.snapshots;
      } catch (err) {
        snapError = (err as Error).message;
      } finally {
        snapBusy = false;
      }
    }
  }

  async function takeSnap(e: SubmitEvent) {
    e.preventDefault();
    if (!activeVol || !snapInput.trim()) return;
    snapBusy = true;
    snapError = '';
    try {
      const r = await api.volumeSnapshotOp(
        activeVol.metadata.namespace ?? '',
        activeVol.metadata.name,
        'create',
        snapInput.trim()
      );
      snapshots = r.snapshots;
      snapInput = '';
    } catch (err) {
      snapError = (err as Error).message;
    } finally {
      snapBusy = false;
    }
  }

  async function rollbackSnap(snap: string) {
    if (!activeVol) return;
    if (
      !(await ask(
        t(
          'Roll {name} back to @{snapshot}? Newer data and newer snapshots on this dataset will be discarded.',
          { name: activeVol.metadata.name, snapshot: snap }
        )
      ))
    ) {
      return;
    }
    snapBusy = true;
    snapError = '';
    try {
      const r = await api.volumeSnapshotOp(
        activeVol.metadata.namespace ?? '',
        activeVol.metadata.name,
        'rollback',
        snap
      );
      snapshots = r.snapshots;
    } catch (err) {
      snapError = (err as Error).message;
    } finally {
      snapBusy = false;
    }
  }

  async function removeSnap(snap: string) {
    if (!activeVol) return;
    if (
      !(await ask(
        t('Delete snapshot @{snapshot} from {name}?', {
          snapshot: snap,
          name: activeVol.metadata.name
        })
      ))
    ) {
      return;
    }
    snapBusy = true;
    snapError = '';
    try {
      const r = await api.volumeSnapshotOp(
        activeVol.metadata.namespace ?? '',
        activeVol.metadata.name,
        'delete',
        snap
      );
      snapshots = r.snapshots;
    } catch (err) {
      snapError = (err as Error).message;
    } finally {
      snapBusy = false;
    }
  }

  async function migrateActive(e: SubmitEvent) {
    e.preventDefault();
    if (!activeVol || !migrateNode.trim()) return;
    const ns = activeVol.metadata.namespace ?? '';
    const nm = activeVol.metadata.name;
    if (
      !(await ask(
        t('Move {name} to {node}? Its tasks stop while its data moves.', {
          name: nm,
          node: migrateNode.trim()
        }),
        { check: [{ change: 'move-volume', namespace: ns, volume: nm, node: migrateNode.trim() }] }
      ))
    ) {
      return;
    }
    snapBusy = true;
    snapError = '';
    try {
      await api.set('Volume', ns, nm, 'node', migrateNode.trim());
      message = t('{name} moves to {node}', { name: nm, node: migrateNode.trim() });
      snapDialog?.close();
    } catch (err) {
      snapError = (err as Error).message;
    } finally {
      snapBusy = false;
    }
  }

  // Restore archive form
  let restoreFiles: FileList | undefined = $state();
  let restoreNode = $state('');

  async function uploadRestore(e: SubmitEvent) {
    e.preventDefault();
    const f = restoreFiles?.[0];
    if (!f || !restoreNode.trim()) return;
    busy = true;
    error = '';
    message = '';
    try {
      const r = await api.restoreVolume(f, { node: restoreNode.trim() });
      message = tn(
        'restored {namespace}/{name} on {node} ({n} byte)',
        'restored {namespace}/{name} on {node} ({n} bytes)',
        r.bytes,
        { namespace: r.namespace, name: r.name, node: r.node }
      );
    } catch (err) {
      error = (err as Error).message;
    } finally {
      busy = false;
    }
  }
  // A Volume is made or restored in a namespace one may write in.
  const anyWhy = $derived(cannotWriteAnywhere());

  // The status cards: the pools' health, their space, when they were last
  // verified (scrubbed), and the Volumes on them. The pools are the nodes'
  // (for those who may see nodes); else the space the nodes report.
  type Pool = {
    node: string;
    name: string;
    state: string;
    sizeBytes: number;
    allocBytes: number;
    readErrors?: number;
    writeErrors?: number;
    checksumErrors?: number;
    scan?: { function: string; state: string; endMs?: number; errors?: number };
  };
  const pools = $derived(
    nodes.flatMap((n) =>
      ((n.status?.pools ?? []) as Omit<Pool, 'node'>[]).map((p) => ({ ...p, node: n.metadata.name }))
    )
  );
  const poolErrors = (p: Pool) => (p.readErrors ?? 0) + (p.writeErrors ?? 0) + (p.checksumErrors ?? 0);
  const space = $derived.by(() => {
    if (pools.length) {
      const size = pools.reduce((a, p) => a + (p.sizeBytes ?? 0), 0);
      const used = pools.reduce((a, p) => a + (p.allocBytes ?? 0), 0);
      return { used, size };
    }
    const used = nodeUsage.reduce((a, n) => a + (n.zfs_used_bytes ?? 0), 0);
    return { used, size: used + nodeUsage.reduce((a, n) => a + (n.zfs_avail_bytes ?? 0), 0) };
  });
  // The pool longest without a scrub decides how verified the storage is.
  const verified = $derived.by(() => {
    if (!pools.length) return null;
    const scanning = pools.filter((p) => p.scan?.state === 'scanning');
    const last = pools.map((p) =>
      p.scan?.function === 'scrub' && p.scan.state === 'finished' && p.scan.endMs ? p.scan.endMs : 0
    );
    const stalest = Math.min(...last);
    const found = pools.reduce((a, p) => a + (p.scan?.errors ?? 0), 0);
    return { scanning: scanning.length, stalest, never: last.filter((x) => !x).length, found };
  });
  function ago(ms: number) {
    const d = Math.floor((Date.now() - ms) / 86_400_000);
    return d < 1 ? t('today') : tn('1 day ago', '{n} days ago', d);
  }
  const detached = $derived(
    volumes.filter((v) => !attachedBy.get(`${v.metadata.namespace ?? ''}/${v.metadata.name}`)?.length).length
  );
  const lost = $derived(volumes.filter((v) => v.status?.lost).length);
  const replicated = $derived(volumes.filter((v) => v.spec?.replica).length);
</script>

<header class="bar">
  <h1>{t('Volumes')}</h1>
  <div class="controls">
    <button type="button" class="primary" disabled={!!anyWhy} title={anyWhy ?? ''} onclick={openCreate}
      >{t('New Volume')}</button
    >
  </div>
</header>
<p class="muted">
  {t(
    'A Volume keeps a service’s data across restarts and moves: a ZFS dataset on a node, an NFS export or an iSCSI LUN. Snapshot, move, back up and restore them here.'
  )}
</p>

<div class="tiles">
  {#if pools.length}
    {@const online = pools.filter((p) => p.state === 'ONLINE').length}
    {@const errs = pools.reduce((a, p) => a + poolErrors(p), 0)}
    <Tile
      label={t('Storage health')}
      icon={ShieldCheck}
      value={t('{n} of {total} online', { n: online, total: pools.length })}
      tone={online < pools.length ? 'bad' : errs ? 'wait' : 'good'}
      note={errs
        ? tn('1 read, write or checksum error', '{n} read, write or checksum errors', errs)
        : t('No known pool or device errors')}
    />
  {/if}
  {#if space.size > 0}
    <Tile
      label={t('Capacity')}
      icon={Database}
      value={t('{pct}% used', { pct: Math.round((space.used / space.size) * 100) })}
      note={t('{used} of {total} · {free} free', {
        used: bytes(space.used),
        total: bytes(space.size),
        free: bytes(space.size - space.used)
      })}
      pct={(space.used / space.size) * 100}
      meterLabel={t('Pool space used')}
    />
  {/if}
  {#if verified}
    <Tile
      label={t('Pool verification')}
      icon={Activity}
      value={verified.scanning
        ? t('Scrub running')
        : verified.never
          ? tn('1 pool never scrubbed', '{n} pools never scrubbed', verified.never)
          : verified.found
            ? t('Last scrub found errors')
            : t('Last scrub clean')}
      tone={verified.found ? 'bad' : verified.never ? 'wait' : 'good'}
      note={verified.stalest
        ? t('The stalest pool scrubbed {when}', { when: ago(verified.stalest) })
        : t('Schedule scrubs on a node’s Host tab')}
    />
  {/if}
  <Tile
    label={t('Volumes')}
    icon={HardDrive}
    value={tn('1 Volume', '{n} Volumes', volumes.length)}
    tone={lost ? 'bad' : ''}
    note={[
      tn('1 in use', '{n} in use', volumes.length - detached),
      tn('1 detached', '{n} detached', detached),
      tn('1 with a replica', '{n} with a replica', replicated),
      ...(lost ? [tn('1 lost', '{n} lost', lost)] : [])
    ].join(' · ')}
  />
</div>

{#if error}<p class="error" role="alert">{error}</p>{/if}
{#if message}<p class="ok" role="status">{message}</p>{/if}

{#if nodeUsage.some((n) => (n.zfs_used_bytes ?? 0) + (n.zfs_avail_bytes ?? 0) > 0)}
  <h2>{t('Pools by node')}</h2>
  <section class="cards">
    {#each nodeUsage as nu (nu.node)}
      {@const used = nu.zfs_used_bytes ?? 0}
      {@const avail = nu.zfs_avail_bytes ?? 0}
      {@const tot = used + avail}
      {#if tot > 0}
        {@const pct = Math.min(100, Math.round((used / tot) * 100))}
        <div class="card">
          <div class="pool-head">
            <h3>{nu.node}</h3>
            <span class="state {pct >= 90 ? 'bad' : pct >= 75 ? 'wait' : 'zfs'}">{pct}%</span>
          </div>
          <p class="muted">
            {t('{used} used · {avail} free', { used: bytes(used), avail: bytes(avail) })}
          </p>
          <div class="meter" role="img" aria-label={t('ZFS pool on {node}: {pct}%', { node: nu.node, pct })}>
            <div
              class="meter-fill {pct >= 90 ? 'bad' : pct >= 75 ? 'wait' : 'zfs'}"
              style:width="{pct}%"
            ></div>
          </div>
        </div>
      {/if}
    {/each}
  </section>
{/if}

<h2>{t('Volumes ({n})', { n: volumes.length })}</h2>
{#if volumes.length}
  <TableBar {table} label={t('Filter volumes')} placeholder={t('Filter volumes or nodes…')} />
  <!-- svelte-ignore a11y_no_noninteractive_tabindex -->
  <div class="table-scroll" tabindex="0" role="region" aria-label={t('Volumes inventory')}>
    <table>
      <caption class="sr-only">{t('Volumes inventory')}</caption>
      <thead>
        <tr>
          <SortTh {table} col="name">{t('Volume')}</SortTh>
          <SortTh {table} col="driver">{t('Driver')}</SortTh>
          <SortTh {table} col="node">{t('Node')}</SortTh>
          <SortTh {table} col="used">{t('ZFS space / Quota')}</SortTh>
          <SortTh {table} col="mounted">{t('Mounted by')}</SortTh>
          <th scope="col">{t('Actions')}</th>
        </tr>
      </thead>
      <tbody>
        {#each table.rows as v (v.metadata.uid)}
          {@const ns = v.metadata.namespace ?? ''}
          {@const drv = v.spec?.driver ?? 'zfs'}
          {@const sp = spaceMap.get(`${ns}/${v.metadata.name}`)}
          {@const users = attachedBy.get(`${ns}/${v.metadata.name}`) ?? []}
          {@const ro = cannotWrite(ns)}
          <tr>
            <td>
              <a href="/k/Volume/{ns}/{v.metadata.name}">{ns}/{v.metadata.name}</a>
              {#if v.status?.lost}<span class="state bad">{t('lost')}</span>{/if}
            </td>
            <td><span class="tag mono">{drv}</span></td>
            <td>{v.status?.node ?? v.spec?.node ?? '—'}</td>
            <td>
              {#if sp}
                {@const stot = sp.used_bytes + sp.avail_bytes}
                {@const spct = stot > 0 ? Math.min(100, Math.round((sp.used_bytes / stot) * 100)) : 0}
                <div>
                  {bytes(sp.used_bytes)} / {bytes(stot)} ({spct}%)
                </div>
                <div class="meter">
                  <div
                    class="meter-fill {spct >= 90 ? 'bad' : spct >= 75 ? 'wait' : 'zfs'}"
                    style:width="{spct}%"
                  ></div>
                </div>
              {:else if v.spec?.size}
                <span>{v.spec.size}</span>
              {:else}
                <span class="muted">—</span>
              {/if}
            </td>
            <td>
              {#if users.length}
                {#each users as svc (svc)}
                  <a class="tag" href="/k/Service/{ns}/{svc}">{svc}</a>
                {/each}
              {:else}
                <span class="muted">{t('detached')}</span>
              {/if}
            </td>
            <td class="controls">
              {#if drv === 'zfs'}
                <button type="button" class="sm" onclick={() => openManage(v)}>
                  {t('Snapshots & Migrate')}
                </button>
                <a class="button sm" href={downloads.volume(ns, v.metadata.name)} download>
                  {t('Backup')}
                </a>
              {/if}
              <button
                type="button"
                class="sm danger"
                disabled={!!ro || busy || users.length > 0}
                title={ro ?? (users.length ? t('In use by a Service') : '')}
                onclick={async () => {
                  if (!(await ask(t('Delete Volume {name}? Its data is destroyed.', { name: `${ns}/${v.metadata.name}` }), {
                      action: t('Delete'),
                      typed: v.metadata.name
                    }))) return;
                  busy = true;
                  try {
                    await api.remove('Volume', ns, v.metadata.name, v.metadata.version);
                  } catch (e) {
                    error = (e as Error).message;
                  } finally {
                    busy = false;
                  }
                }}
              >
                {t('Delete')}
              </button>
            </td>
          </tr>
        {:else}
          <tr><td colspan="6" class="muted">{t('Nothing matches.')}</td></tr>
        {/each}
      </tbody>
    </table>
  </div>
  <Pager {table} label={t('Volumes')} />
{:else}
  <p class="empty">{t('No Volumes found in this scope.')}</p>
{/if}

<section class="card restore-card">
  <h2>{t('Restore Volume from ZFS backup stream')}</h2>
  <form class="controls" onsubmit={uploadRestore}>
    <span class="dropzone" use:droppable><input type="file" bind:files={restoreFiles} aria-label={t('Backup file')} /><span class="hint" aria-hidden="true">{t('or drop it here')}</span></span>
    {#if nodes.length}
      <select bind:value={restoreNode} aria-label={t('Target node')} required>
        <option value="">{t('Choose node…')}</option>
        {#each nodes as n (n.metadata.name)}
          <option value={n.metadata.name}>{n.metadata.name}</option>
        {/each}
      </select>
    {:else}
      <input placeholder={t('onto node')} bind:value={restoreNode} aria-label={t('Node')} required />
    {/if}
    <button
      class="primary"
      disabled={!!anyWhy || busy || !restoreFiles?.length || !restoreNode.trim()}
      title={anyWhy ?? ''}
    >
      {t('Upload & Restore')}
    </button>
  </form>
</section>

<dialog
  bind:this={createDialog}
  closedby="any"
  aria-labelledby="create-vol-title"
  onclick={(e) => lightDismiss(e, createDialog)}
>
  <header class="bar">
    <h2 id="create-vol-title">{t('Create Volume')}</h2>
    <button type="button" class="sm" onclick={() => createDialog?.close()}>{t('Close')}</button>
  </header>
  <form class="wizard" onsubmit={submitCreate}>
    <label>
      {t('Namespace')}
      <select bind:value={cNamespace} required>
        {#each nsChoices.includes(cNamespace) ? nsChoices : [cNamespace, ...nsChoices] as n (n)}
          {#if n}<option value={n}>{n}</option>{/if}
        {/each}
      </select>
    </label>
    <label>
      {t('Volume name')}
      <input bind:value={cName} placeholder="pgdata" required />
    </label>
    <label>
      {t('Storage driver')}
      <select bind:value={cDriver}>
        <option value="zfs">ZFS dataset (copy-on-write, snapshots, live migration)</option>
        <option value="nfs">NFSv4 shared export</option>
        <option value="iscsi">iSCSI block LUN</option>
      </select>
    </label>
    {#if cDriver === 'zfs'}
      <label>
        {t('Quota size')} <span class="muted">{t('(optional, e.g. 10Gi)')}</span>
        <input bind:value={cSize} placeholder="10Gi" />
      </label>
      <label>
        {t('Pin to node')} <span class="muted">{t('(optional; empty places with first task)')}</span>
        <input bind:value={cNode} placeholder="n1" />
      </label>
    {:else if cDriver === 'nfs'}
      <label>
        {t('NFS server')}
        <input bind:value={cNfsServer} placeholder="nas.internal" required />
      </label>
      <label>
        {t('Export path')}
        <input bind:value={cNfsPath} placeholder="/exports/shared" required />
      </label>
    {:else}
      <label>
        {t('iSCSI portal')}
        <input bind:value={cIscsiPortal} placeholder="10.0.0.50:3260" required />
      </label>
      <label>
        {t('Target IQN')}
        <input bind:value={cIscsiTarget} placeholder="iqn.2026-01.internal:lun0" required />
      </label>
      <label>
        {t('LUN')}
        <input type="number" min="0" bind:value={cIscsiLun} />
      </label>
    {/if}
    <div class="controls">
      <button class="primary" disabled={busy || !cName.trim()}>{t('Create Volume')}</button>
      <button type="button" onclick={() => createDialog?.close()}>{t('Cancel')}</button>
    </div>
  </form>
</dialog>

<dialog
  bind:this={snapDialog}
  closedby="any"
  aria-labelledby="snap-vol-title"
  onclick={(e) => lightDismiss(e, snapDialog)}
>
  {#if activeVol}
    {@const ans = activeVol.metadata.namespace ?? ''}
    {@const aro = cannotWrite(ans)}
    <header class="bar">
      <h2 id="snap-vol-title">
        {t('ZFS Studio: {name}', { name: `${ans}/${activeVol.metadata.name}` })}
      </h2>
      <button type="button" class="sm" onclick={() => snapDialog?.close()}>{t('Close')}</button>
    </header>
    {#if snapError}<p class="error" role="alert">{snapError}</p>{/if}

    <h3>{t('Instant ZFS snapshots')}</h3>
    {#if !activeVol.status?.node}
      <p class="muted">{t('Volume is not placed on a node yet.')}</p>
    {:else}
      <form class="controls" onsubmit={takeSnap}>
        <input
          placeholder={t('snapshot name (e.g. before-migration)')}
          bind:value={snapInput}
          aria-label={t('Snapshot name')}
          disabled={!!aro || snapBusy}
        />
        <button class="primary" disabled={!!aro || snapBusy || !snapInput.trim()}>
          {t('Take snapshot')}
        </button>
      </form>
      {#if snapshots.length}
        <TableBar table={snapTable} label={t('Search snapshots')} />
        <table>
          <caption class="sr-only">{t('Snapshots')}</caption>
          <thead>
            <tr>
              <SortTh table={snapTable} col="name">{t('Snapshot')}</SortTh>
              <th scope="col">{t('Actions')}</th>
            </tr>
          </thead>
          <tbody>
            {#each snapTable.rows as s (s)}
              <tr>
                <td class="mono">@{s}</td>
                <td class="controls">
                  <button
                    type="button"
                    class="sm"
                    disabled={!!aro || snapBusy}
                    onclick={() => rollbackSnap(s)}>{t('Roll back')}</button
                  >
                  <button
                    type="button"
                    class="sm danger"
                    disabled={!!aro || snapBusy}
                    onclick={() => removeSnap(s)}>{t('Delete')}</button
                  >
                </td>
              </tr>
            {:else}
              <tr><td colspan="2" class="muted">{t('Nothing matches.')}</td></tr>
            {/each}
          </tbody>
        </table>
        <Pager table={snapTable} label={t('Snapshots')} />
      {:else if !snapBusy}
        <p class="empty">{t('No ZFS snapshots yet.')}</p>
      {/if}
    {/if}

    <h3>{t('Cross-node live ZFS migration')}</h3>
    <p class="muted">
      {t(
        'Streams the ZFS dataset to another node and moves attached tasks with it.'
      )}
    </p>
    <form class="controls" onsubmit={migrateActive}>
      {#if nodes.length}
        <select bind:value={migrateNode} aria-label={t('Move to node')} disabled={!!aro || snapBusy}>
          <option value="">{t('Choose target node…')}</option>
          {#each nodes as n (n.metadata.name)}
            <option value={n.metadata.name}>{n.metadata.name}</option>
          {/each}
        </select>
      {:else}
        <input
          placeholder={t('target node')}
          bind:value={migrateNode}
          aria-label={t('Move to node')}
          disabled={!!aro || snapBusy}
        />
      {/if}
      <button class="primary" disabled={!!aro || snapBusy || !migrateNode.trim()}>
        {t('Migrate dataset')}
      </button>
    </form>
  {/if}
</dialog>

<style>
  .pool-head {
    display: flex;
    justify-content: space-between;
    align-items: center;
  }
  .pool-head h3 {
    margin: 0;
    font-size: 1rem;
  }
  .restore-card {
    margin-top: 1.25rem;
  }
  .wizard {
    display: flex;
    flex-direction: column;
    gap: 0.65rem;
  }
  .wizard label {
    display: flex;
    flex-direction: column;
    gap: 0.2rem;
  }
</style>

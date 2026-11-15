<script lang="ts">
  // Docker-Desktop ZFS Volumes Studio: per-node ZFS pool capacity meters,
  // 1-click ZFS/NFS/iSCSI volume creation wizard, live per-dataset space
  // tracking, ZFS Snapshot Manager modal, cross-node ZFS migration, and
  // streaming .zfs backup download & restore.
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
  import { cannotWrite, namespaces, session } from '#lib/session.svelte.ts';

  let adminNamespaces: string[] = $state([]);
  let volumes: LObject[] = $state([]);
  let services: LObject[] = $state([]);
  let nodes: LObject[] = $state([]);
  let nodeUsage: NodeUsage[] = $state([]);
  let error = $state('');
  let message = $state('');
  let busy = $state(false);
  let filterText = $state('');

  // Sortable table state (mwg: sortable-table)
  type SortKey = 'name' | 'driver' | 'node' | 'used';
  let sortBy: SortKey = $state('name');
  let sortDir: 'ascending' | 'descending' = $state('ascending');
  const collator = new Intl.Collator(undefined, { numeric: true, sensitivity: 'base' });

  function toggleSort(col: SortKey) {
    if (sortBy === col) {
      sortDir = sortDir === 'ascending' ? 'descending' : 'ascending';
    } else {
      sortBy = col;
      sortDir = 'ascending';
    }
  }

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
        m.set(`${vs.namespace}/${vs.name}`, vs);
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

  const sortedVolumes = $derived.by(() => {
    const q = filterText.trim().toLowerCase();
    const list = q
      ? volumes.filter(
          (v) =>
            v.metadata.name.toLowerCase().includes(q) ||
            (v.metadata.namespace ?? '').toLowerCase().includes(q) ||
            (v.status?.node ?? '').toLowerCase().includes(q)
        )
      : [...volumes];
    const factor = sortDir === 'ascending' ? 1 : -1;
    return list.sort((a, b) => {
      if (sortBy === 'used') {
        const ua = spaceMap.get(`${a.metadata.namespace ?? ''}/${a.metadata.name}`)?.used_bytes ?? 0;
        const ub = spaceMap.get(`${b.metadata.namespace ?? ''}/${b.metadata.name}`)?.used_bytes ?? 0;
        return factor * (ua - ub);
      }
      const va =
        sortBy === 'driver'
          ? String(a.spec?.driver ?? 'zfs')
          : sortBy === 'node'
            ? String(a.status?.node ?? a.spec?.node ?? '')
            : `${a.metadata.namespace ?? ''}/${a.metadata.name}`;
      const vb =
        sortBy === 'driver'
          ? String(b.spec?.driver ?? 'zfs')
          : sortBy === 'node'
            ? String(b.status?.node ?? b.spec?.node ?? '')
            : `${b.metadata.namespace ?? ''}/${b.metadata.name}`;
      return factor * collator.compare(va, vb);
    });
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

  async function openManage(v: LObject) {
    activeVol = v;
    snapshots = [];
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
      !confirm(
        t(
          'Roll {name} back to @{snapshot}? Newer data and newer snapshots on this dataset will be discarded.',
          { name: activeVol.metadata.name, snapshot: snap }
        )
      )
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
      !confirm(
        t('Delete snapshot @{snapshot} from {name}?', {
          snapshot: snap,
          name: activeVol.metadata.name
        })
      )
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
      !confirm(
        t('Move {name} to {node}? Its tasks stop while its data moves.', {
          name: nm,
          node: migrateNode.trim()
        })
      )
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
</script>

<header class="bar">
  <h1>{t('Volumes Studio')}</h1>
  <div class="controls">
    <input
      type="search"
      bind:value={filterText}
      placeholder={t('Filter volumes or nodes…')}
      aria-label={t('Filter volumes')}
    />
    <button type="button" class="primary" onclick={openCreate}>{t('New Volume')}</button>
  </div>
</header>
<p class="muted">
  {t(
    'Manage persistent FreeBSD ZFS datasets, NFS exports, and iSCSI LUNs: inspect live ZFS pool space, take instant copy-on-write snapshots, migrate datasets across nodes, or download and restore ZFS stream archives.'
  )}
</p>

{#if error}<p class="error" role="alert">{error}</p>{/if}
{#if message}<p class="ok" role="status">{message}</p>{/if}

{#if nodeUsage.some((n) => (n.zfs_used_bytes ?? 0) + (n.zfs_avail_bytes ?? 0) > 0)}
  <h2>{t('Node ZFS pools')}</h2>
  <section class="cards">
    {#each nodeUsage as nu (nu.node)}
      {@const used = nu.zfs_used_bytes ?? 0}
      {@const avail = nu.zfs_avail_bytes ?? 0}
      {@const tot = used + avail}
      {#if tot > 0}
        {@const pct = Math.min(100, Math.round((used / tot) * 100))}
        <div class="card">
          <div class="pool-head">
            <strong>{nu.node}</strong>
            <span class="state {pct >= 90 ? 'bad' : pct >= 75 ? 'wait' : 'zfs'}">{pct}%</span>
          </div>
          <p class="muted mono">
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

<h2>{t('Volumes ({n})', { n: sortedVolumes.length })}</h2>
{#if sortedVolumes.length}
  <!-- svelte-ignore a11y_no_noninteractive_tabindex -->
  <div class="table-scroll" tabindex="0" role="region" aria-label={t('Volumes inventory')}>
    <table>
      <caption class="sr-only">{t('Volumes inventory')}</caption>
      <thead>
        <tr>
          <th scope="col" aria-sort={sortBy === 'name' ? sortDir : undefined}>
            <button type="button" class="sort-btn" onclick={() => toggleSort('name')}>
              {t('Volume')}
            </button>
          </th>
          <th scope="col" aria-sort={sortBy === 'driver' ? sortDir : undefined}>
            <button type="button" class="sort-btn" onclick={() => toggleSort('driver')}>
              {t('Driver')}
            </button>
          </th>
          <th scope="col" aria-sort={sortBy === 'node' ? sortDir : undefined}>
            <button type="button" class="sort-btn" onclick={() => toggleSort('node')}>
              {t('Node')}
            </button>
          </th>
          <th scope="col" aria-sort={sortBy === 'used' ? sortDir : undefined}>
            <button type="button" class="sort-btn" onclick={() => toggleSort('used')}>
              {t('ZFS space / Quota')}
            </button>
          </th>
          <th scope="col">{t('Mounted by')}</th>
          <th scope="col">{t('Actions')}</th>
        </tr>
      </thead>
      <tbody>
        {#each sortedVolumes as v (v.metadata.uid)}
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
            <td class="mono">{v.status?.node ?? v.spec?.node ?? '—'}</td>
            <td>
              {#if sp}
                {@const stot = sp.used_bytes + sp.avail_bytes}
                {@const spct = stot > 0 ? Math.min(100, Math.round((sp.used_bytes / stot) * 100)) : 0}
                <div class="mono">
                  {bytes(sp.used_bytes)} / {bytes(stot)} ({spct}%)
                </div>
                <div class="meter">
                  <div
                    class="meter-fill {spct >= 90 ? 'bad' : spct >= 75 ? 'wait' : 'zfs'}"
                    style:width="{spct}%"
                  ></div>
                </div>
              {:else if v.spec?.size}
                <span class="mono">{v.spec.size}</span>
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
                  if (!confirm(t('Delete Volume {name}? Its data is destroyed.', { name: `${ns}/${v.metadata.name}` }))) return;
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
        {/each}
      </tbody>
    </table>
  </div>
{:else}
  <p class="muted">{t('No Volumes found in this scope.')}</p>
{/if}

<section class="card restore-card">
  <h2>{t('Restore Volume from ZFS backup stream')}</h2>
  <form class="controls" onsubmit={uploadRestore}>
    <input type="file" bind:files={restoreFiles} aria-label={t('Backup file')} />
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
    <button class="primary" disabled={busy || !restoreFiles?.length || !restoreNode.trim()}>
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
        <table>
          <caption class="sr-only">{t('Snapshots')}</caption>
          <thead>
            <tr>
              <th scope="col">{t('Snapshot')}</th>
              <th scope="col">{t('Actions')}</th>
            </tr>
          </thead>
          <tbody>
            {#each snapshots as s (s)}
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
            {/each}
          </tbody>
        </table>
      {:else if !snapBusy}
        <p class="muted">{t('No ZFS snapshots yet.')}</p>
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

<script lang="ts">
  import { ask } from './confirm.svelte.ts';
  // A Volume's data, as `legion volume backup`, `restore`, `snapshot` and
  // `clone` manage it: its snapshot schedules and how they went, live ZFS
  // snapshots (create, rollback, delete, copy into a new Volume), a
  // crash-consistent backup downloaded as a file, and a backup file
  // restored onto a node.
  import { droppable } from '#lib/drop.svelte.ts';
  import { api, downloads, type LObject } from './api.ts';
  import { cannotWrite } from './session.svelte.ts';
  import { t, tn } from './i18n.svelte.ts';
  import { Table } from './table.svelte.ts';
  import SortTh from './SortTh.svelte';
  import TableBar from './TableBar.svelte';
  import Pager from './Pager.svelte';

  let { volume }: { volume: LObject } = $props();
  const ns = $derived(volume.metadata.namespace ?? '');
  const name = $derived(volume.metadata.name);
  const driver = $derived(volume.spec?.driver ?? 'zfs');
  const placedNode = $derived(volume.status?.node ?? '');
  const lost = $derived(Boolean(volume.status?.lost));
  const why = $derived(cannotWrite(ns));

  let file: FileList | undefined = $state();
  let node = $state('');
  let busy = $state(false);
  let message = $state('');
  let error = $state('');

  let snapshots: string[] = $state([]);
  let snapLoading = $state(false);
  let newSnap = $state('');
  // In the order ZFS made them, oldest first, until sorted by name.
  const table = new Table(() => snapshots, { sorts: { name: (s) => s } });

  async function loadSnapshots() {
    if (driver !== 'zfs' || !placedNode || lost) {
      snapshots = [];
      return;
    }
    snapLoading = true;
    try {
      const r = await api.volumeSnapshots(ns, name);
      snapshots = r.snapshots;
    } catch {
      snapshots = [];
    } finally {
      snapLoading = false;
    }
  }

  $effect(() => {
    void ns;
    void name;
    void driver;
    void placedNode;
    void lost;
    loadSnapshots();
  });

  async function createSnapshot(e: SubmitEvent) {
    e.preventDefault();
    const snap = newSnap.trim();
    if (!snap) return;
    busy = true;
    error = '';
    message = '';
    try {
      const r = await api.volumeSnapshotOp(ns, name, 'create', snap);
      snapshots = r.snapshots;
      newSnap = '';
      message = t('Created snapshot @{snapshot}', { snapshot: snap });
    } catch (err) {
      error = (err as Error).message;
    } finally {
      busy = false;
    }
  }

  // The snapshot schedules the spec asks for, each with how it went.
  type Schedule = { every: string; keep: number };
  type ScheduleStatus = { every: string; last?: string; takenMs?: number; kept?: number; message?: string };
  const schedules = $derived(
    ((volume.spec?.snapshots ?? []) as Schedule[]).map((s) => ({
      ...s,
      status: ((volume.status?.snapshots ?? []) as ScheduleStatus[]).find((st) => st.every === s.every)
    }))
  );
  function ago(ms: number) {
    const s = Math.max(0, Math.round((Date.now() - ms) / 1000));
    if (s < 120) return t('{n} s ago', { n: s });
    if (s < 7200) return t('{n} min ago', { n: Math.round(s / 60) });
    if (s < 172_800) return t('{n} h ago', { n: Math.round(s / 3600) });
    return t('{n} days ago', { n: Math.round(s / 86_400) });
  }

  // Its backups: as the leader last ran them, what the target holds, and
  // one restored onto a node.
  type BackupStatus = {
    target: string;
    last?: string;
    takenMs?: number;
    message?: string;
    runs?: { atMs: number; tookMs: number; bytes: number; whole?: boolean; error?: string }[];
  };
  const backupSpec = $derived(volume.spec?.backup as { target: string; every: string; keep: number } | undefined);
  const backupStatus = $derived(volume.status?.backup as BackupStatus | undefined);
  let held: string[] = $state([]);
  let heldError = $state('');
  let restoring = $state('');
  let restoreNode = $state('');
  let restoreAs = $state('');
  const heldName = (h: string) => h.split(/[@/]/).pop()?.replace(/\.zfs$/, '') ?? h;
  async function loadBackups() {
    if (!backupSpec) return;
    heldError = '';
    try {
      held = (await api.volumeBackups(ns, name)).backups;
    } catch (err) {
      heldError = (err as Error).message;
    }
  }
  $effect(() => {
    void backupSpec?.target;
    loadBackups();
  });
  async function restoreBackup(e: SubmitEvent) {
    e.preventDefault();
    if (!restoring || !restoreNode.trim() || !restoreAs.trim()) return;
    busy = true;
    error = '';
    message = '';
    try {
      await api.volumeRestoreFrom(ns, name, {
        backup: restoring,
        node: restoreNode.trim(),
        into: restoreAs.trim()
      });
      message = t('Restored {backup} as Volume {into} on {node}', {
        backup: heldName(restoring),
        into: restoreAs.trim(),
        node: restoreNode.trim()
      });
      restoring = '';
    } catch (err) {
      error = (err as Error).message;
    } finally {
      busy = false;
    }
  }

  // A snapshot copied into a new Volume: which, and its name.
  let cloning = $state('');
  let cloneName = $state('');
  async function cloneSnapshot(e: SubmitEvent) {
    e.preventDefault();
    const into = cloneName.trim();
    if (!cloning || !into) return;
    busy = true;
    error = '';
    message = '';
    try {
      await api.volumeSnapshotOp(ns, name, 'clone', cloning, into);
      message = t('Made Volume {into} from @{snapshot}', { into, snapshot: cloning });
      cloning = '';
      cloneName = '';
    } catch (err) {
      error = (err as Error).message;
    } finally {
      busy = false;
    }
  }

  async function rollbackSnapshot(snap: string) {
    if (
      !(await ask(
        t(
          'Roll {name} back to @{snapshot}? Newer data and newer snapshots on this dataset will be discarded.',
          { name, snapshot: snap }
        )
      ))
    ) {
      return;
    }
    busy = true;
    error = '';
    message = '';
    try {
      const r = await api.volumeSnapshotOp(ns, name, 'rollback', snap);
      snapshots = r.snapshots;
      message = t('Rolled {name} back to @{snapshot}', { name, snapshot: snap });
    } catch (err) {
      error = (err as Error).message;
    } finally {
      busy = false;
    }
  }

  async function deleteSnapshot(snap: string) {
    if (!(await ask(t('Delete snapshot @{snapshot} from {name}?', { snapshot: snap, name })))) return;
    busy = true;
    error = '';
    message = '';
    try {
      const r = await api.volumeSnapshotOp(ns, name, 'delete', snap);
      snapshots = r.snapshots;
      message = t('Deleted snapshot @{snapshot}', { snapshot: snap });
    } catch (err) {
      error = (err as Error).message;
    } finally {
      busy = false;
    }
  }

  async function restore(e: SubmitEvent) {
    e.preventDefault();
    const f = file?.[0];
    if (!f || !node) return;
    const what = lost
      ? t('Restore {file} into {name}, lost with its node, on {node}?', { file: f.name, name, node })
      : t('Restore {file} on {node}? It makes the Volume the backup names, which must not exist yet.', {
          file: f.name,
          node
        });
    if (!(await ask(what))) return;
    busy = true;
    error = '';
    message = '';
    try {
      const r = await api.restoreVolume(
        f,
        lost ? { node, namespace: ns, name, replace: true } : { node }
      );
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

{#if driver === 'zfs'}
  <h2>{t('Snapshot schedules')}</h2>
  {#if schedules.length}
    <ul class="plain schedules">
      {#each schedules as s (s.every)}
        <li>
          <strong>{t('Every {every}, the last {keep} kept', { every: s.every, keep: s.keep })}</strong>
          {#if s.status?.message}
            <span class="error">{s.status.message}</span>
          {:else if s.status?.takenMs}
            <span class="muted"
              >{t('last @{snapshot}, {when}; {n} kept', {
                snapshot: s.status.last ?? '',
                when: ago(s.status.takenMs),
                n: s.status.kept ?? 0
              })}</span
            >
          {:else}
            <span class="muted">{t('none taken yet')}</span>
          {/if}
        </li>
      {/each}
    </ul>
  {:else}
    <p class="muted">
      {t('None. Set them in its YAML, for example hourly kept for a day and daily for a month:')}
      <code>snapshots: [{'{'}every: 1h, keep: 24{'}'}, {'{'}every: 1d, keep: 30{'}'}]</code>
    </p>
  {/if}

  <h2>{t('ZFS snapshots')}</h2>
  {#if !placedNode}
    <p class="muted">{t('Snapshots become available once the Volume is placed on a node.')}</p>
  {:else if lost}
    <p class="muted">{t('Volume node is lost; restore a backup first.')}</p>
  {:else}
    <form class="controls" onsubmit={createSnapshot}>
      <input
        placeholder={t('snapshot name (e.g. pre-upgrade)')}
        bind:value={newSnap}
        aria-label={t('Snapshot name')}
        disabled={!!why || busy}
      />
      <button class="primary" disabled={!!why || busy || !newSnap.trim()} title={why ?? ''}>
        {t('Take snapshot')}
      </button>
      <button type="button" onclick={loadSnapshots} disabled={snapLoading || busy}>
        {t('Refresh')}
      </button>
    </form>

    {#if snapshots.length}
      <TableBar {table} label={t('Search snapshots')} />
      <table>
        <caption class="sr-only">{t('ZFS snapshots of {name}', { name })}</caption>
        <thead>
          <tr>
            <SortTh {table} col="name">{t('Snapshot')}</SortTh>
            <th scope="col">{t('Actions')}</th>
          </tr>
        </thead>
        <tbody>
          {#each table.rows as snap (snap)}
            <tr>
              <td class="mono">@{snap}</td>
              <td class="controls">
                <button
                  type="button"
                  class="sm"
                  disabled={!!why || busy}
                  title={why ?? ''}
                  onclick={() => rollbackSnapshot(snap)}
                >
                  {t('Roll back')}
                </button>
                <button
                  type="button"
                  class="sm"
                  disabled={!!why || busy}
                  title={why ?? t('Copy its data into a new Volume, leaving this one as it is')}
                  onclick={() => {
                    cloning = snap;
                    cloneName = `${name}-copy`;
                  }}
                >
                  {t('Copy to a new Volume')}
                </button>
                <button
                  type="button"
                  class="sm danger"
                  disabled={!!why || busy}
                  title={why ?? ''}
                  onclick={() => deleteSnapshot(snap)}
                >
                  {t('Delete')}
                </button>
              </td>
            </tr>
          {:else}
            <tr><td colspan="2" class="muted">{t('Nothing matches.')}</td></tr>
          {/each}
        </tbody>
      </table>
      <Pager {table} label={t('Snapshots')} />
      {#if cloning}
        <form class="controls clone" onsubmit={cloneSnapshot}>
          <label
            >{t('A new Volume from @{snapshot}, named', { snapshot: cloning })}
            <input bind:value={cloneName} aria-label={t('The new Volume’s name')} /></label
          >
          <button class="primary" disabled={busy || !cloneName.trim()}>{t('Copy')}</button>
          <button type="button" onclick={() => (cloning = '')}>{t('Cancel')}</button>
        </form>
      {/if}
    {:else if !snapLoading}
      <p class="empty">{t('No ZFS snapshots yet.')}</p>
    {/if}
  {/if}
{/if}

<h2>{t('Scheduled backups')}</h2>
{#if backupSpec}
  <p>
    {t('To {target}, every {every}, the last {keep} kept there.', {
      target: backupSpec.target,
      every: backupSpec.every,
      keep: backupSpec.keep
    })}
    {#if backupStatus?.message}
      <span class="error">{backupStatus.message}</span>
    {:else if backupStatus?.takenMs}
      <span class="muted">{t('The last {when}.', { when: ago(backupStatus.takenMs) })}</span>
    {:else}
      <span class="muted">{t('None taken yet.')}</span>
    {/if}
  </p>
  {#if heldError}<p class="error">{heldError}</p>{/if}
  {#if held.length}
    <table>
      <caption class="sr-only">{t('Backups of {name} on {target}', { name, target: backupSpec.target })}</caption>
      <thead>
        <tr>
          <th scope="col">{t('Backup')}</th>
          <th scope="col">{t('Actions')}</th>
        </tr>
      </thead>
      <tbody>
        {#each [...held].reverse() as h (h)}
          <tr>
            <td class="mono">{heldName(h)}</td>
            <td>
              <button
                type="button"
                class="sm"
                disabled={!!why || busy}
                title={why ?? ''}
                onclick={() => {
                  restoring = h;
                  restoreAs = `${name}-restored`;
                }}>{t('Restore…')}</button
              >
            </td>
          </tr>
        {/each}
      </tbody>
    </table>
    {#if restoring}
      <form class="controls clone" onsubmit={restoreBackup}>
        <label
          >{t('Restore {backup} onto node', { backup: heldName(restoring) })}
          <input bind:value={restoreNode} aria-label={t('Node to restore onto')} placeholder={placedNode} /></label
        >
        <label
          >{t('as the Volume')}
          <input bind:value={restoreAs} aria-label={t('Restored Volume’s name')} /></label
        >
        <button class="primary" disabled={busy || !restoreNode.trim() || !restoreAs.trim()}>{t('Restore')}</button>
        <button type="button" onclick={() => (restoring = '')}>{t('Cancel')}</button>
      </form>
    {/if}
  {:else if !heldError}
    <p class="empty">{t('The target holds none of its backups yet.')}</p>
  {/if}
{:else}
  <p class="muted">
    {t('None. Name a BackupTarget in its YAML, for example:')}
    <code>backup: {'{'}target: offsite, every: 1h, keep: 48{'}'}</code>
  </p>
{/if}

<h2>{t('Backup')}</h2>
<p class="muted">
  {t(
    'A snapshot taken as its tasks run (crash-consistent): an application whose files must agree, such as a database, recovers as from a power cut.'
  )}
</p>
<a class="button" href={downloads.volume(ns, name)} download>{t('Download a backup')}</a>

<h2>{t('Restore')}</h2>
{#if lost}
  <p class="error" role="alert">{t('Lost with its node: restore a backup into it on another node.')}</p>
{/if}
<form class="controls" onsubmit={restore}>
  <span class="dropzone" use:droppable><input type="file" bind:files={file} aria-label={t('Backup file')} disabled={!!why} /><span class="hint" aria-hidden="true">{t('or drop it here')}</span></span>
  <input placeholder={t('onto node')} bind:value={node} aria-label={t('Node')} disabled={!!why} />
  <button class="primary" disabled={!!why || busy || !file?.length || !node} title={why ?? ''}
    >{lost ? t('Restore into it') : t('Restore')}</button
  >
  {#if busy}<span class="muted">{t('uploading…')}</span>{/if}
</form>
{#if why}<p class="muted">{t('Read only: {why}.', { why })}</p>{/if}
{#if error}<p class="error" role="alert">{error}</p>{/if}
{#if message}<p class="ok" role="status">{message}</p>{/if}

<style>
  .schedules li {
    display: flex;
    flex-wrap: wrap;
    gap: 0.25rem 0.75rem;
    padding: 0.3rem 0;
  }
  form.clone {
    margin-top: 0.6rem;
  }
  form.clone label {
    display: inline-flex;
    align-items: center;
    gap: 0.5rem;
  }
</style>

<script lang="ts">
  import { ask } from './confirm.svelte.ts';
  // A Volume's data, as `legion volume backup`, `restore`, and `snapshot`
  // manage it: live ZFS snapshots (create, rollback, delete), a crash-consistent
  // backup downloaded as a file, and a backup file restored onto a node.
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
    {:else if !snapLoading}
      <p class="empty">{t('No ZFS snapshots yet.')}</p>
    {/if}
  {/if}
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

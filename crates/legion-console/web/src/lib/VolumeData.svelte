<script lang="ts">
  // A Volume's data, as `legion volume backup` and `restore` move it: a
  // backup downloaded as a file, and a backup file restored onto a node.
  // A Volume lost with its node is restored in place (`--replace`), kept
  // with what uses it.
  import { api, downloads, type LObject } from './api.ts';
  import { cannotWrite } from './session.svelte.ts';

  let { volume }: { volume: LObject } = $props();
  const ns = $derived(volume.metadata.namespace ?? '');
  const name = $derived(volume.metadata.name);
  const lost = $derived(Boolean(volume.status?.lost));
  const why = $derived(cannotWrite(ns));
  let file: FileList | undefined = $state();
  let node = $state('');
  let busy = $state(false);
  let message = $state('');
  let error = $state('');

  async function restore(e: SubmitEvent) {
    e.preventDefault();
    const f = file?.[0];
    if (!f || !node) return;
    const what = lost
      ? `Restore ${f.name} into ${name}, lost with its node, on ${node}?`
      : `Restore ${f.name} on ${node}? It makes the Volume the backup names, which must not exist yet.`;
    if (!confirm(what)) return;
    busy = true;
    error = '';
    message = '';
    try {
      const r = await api.restoreVolume(f, lost ? { node, namespace: ns, name, replace: true } : { node });
      message = `restored ${r.namespace}/${r.name} on ${r.node} (${r.bytes} bytes)`;
    } catch (err) {
      error = (err as Error).message;
    } finally {
      busy = false;
    }
  }
</script>

<h2>Backup</h2>
<p class="muted">
  A snapshot taken as its tasks run (crash-consistent): an application whose files must agree,
  such as a database, recovers as from a power cut.
</p>
<a class="button" href={downloads.volume(ns, name)} download>Download a backup</a>

<h2>Restore</h2>
{#if lost}
  <p class="error">Lost with its node: restore a backup into it on another node.</p>
{/if}
<form class="controls" onsubmit={restore}>
  <input type="file" bind:files={file} aria-label="Backup file" disabled={!!why} />
  <input placeholder="onto node" bind:value={node} aria-label="Node" disabled={!!why} />
  <button class="primary" disabled={!!why || busy || !file?.length || !node} title={why ?? ''}
    >{lost ? 'Restore into it' : 'Restore'}</button
  >
  {#if busy}<span class="muted">uploading…</span>{/if}
</form>
{#if why}<p class="muted">Read only: {why}.</p>{/if}
{#if error}<p class="error">{error}</p>{/if}
{#if message}<p class="ok">{message}</p>{/if}

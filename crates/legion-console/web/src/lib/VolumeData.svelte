<script lang="ts">
  // A Volume's data, as `legion volume backup` and `restore` move it: a
  // backup downloaded as a file, and a backup file restored onto a node.
  // A Volume lost with its node is restored in place (`--replace`), kept
  // with what uses it.
  import { api, downloads, type LObject } from './api.ts';
  import { cannotWrite } from './session.svelte.ts';
  import { t, tn } from './i18n.svelte.ts';

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
      ? t('Restore {file} into {name}, lost with its node, on {node}?', { file: f.name, name, node })
      : t('Restore {file} on {node}? It makes the Volume the backup names, which must not exist yet.', {
          file: f.name,
          node
        });
    if (!confirm(what)) return;
    busy = true;
    error = '';
    message = '';
    try {
      const r = await api.restoreVolume(f, lost ? { node, namespace: ns, name, replace: true } : { node });
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
  <input type="file" bind:files={file} aria-label={t('Backup file')} disabled={!!why} />
  <input placeholder={t('onto node')} bind:value={node} aria-label={t('Node')} disabled={!!why} />
  <button class="primary" disabled={!!why || busy || !file?.length || !node} title={why ?? ''}
    >{lost ? t('Restore into it') : t('Restore')}</button
  >
  {#if busy}<span class="muted">{t('uploading…')}</span>{/if}
</form>
{#if why}<p class="muted">{t('Read only: {why}.', { why })}</p>{/if}
{#if error}<p class="error" role="alert">{error}</p>{/if}
{#if message}<p class="ok" role="status">{message}</p>{/if}

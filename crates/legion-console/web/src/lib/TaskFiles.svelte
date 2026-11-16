<script lang="ts">
  // A task's files: its container's directories browsed, a file downloaded
  // or uploaded in place (`legion files`, `legion cp`). Its Volumes are
  // there at their mount paths. Links are shown, never followed.
  import { droppable } from '#lib/drop.svelte.ts';
  import { api, bytes, downloads, type FileEntry, type LObject } from '#lib/api.ts';
  import { t } from '#lib/i18n.svelte.ts';
  import { cannotWrite } from '#lib/session.svelte.ts';

  let { task }: { task: LObject } = $props();

  const ns = $derived(task.metadata.namespace ?? '');
  // Reading a task's files takes what a shell in it takes.
  const why = $derived(cannotWrite(ns));
  const name = $derived(task.metadata.name);
  const containers: string[] = $derived(
    (task.spec?.pod?.containers ?? []).map((c: { name: string }) => c.name)
  );
  let container = $state('');
  const chosen = $derived(container || containers[0] || '');

  let path = $state('/');
  let entries: FileEntry[] = $state([]);
  let error = $state('');
  let message = $state('');
  let busy = $state(false);
  let upload: FileList | undefined = $state();

  const crumbs = $derived(
    path
      .split('/')
      .filter(Boolean)
      .map((part, i, all) => ({ part, to: '/' + all.slice(0, i + 1).join('/') }))
  );
  const join = (dir: string, entry: string) => (dir.endsWith('/') ? dir : dir + '/') + entry;

  async function load(to = path) {
    busy = true;
    error = '';
    try {
      entries = (await api.files(ns, name, to, chosen)).entries;
      path = to;
    } catch (err) {
      error = (err as Error).message;
    } finally {
      busy = false;
    }
  }

  $effect(() => {
    // Again when the container chosen changes.
    void chosen;
    if (!why) load('/');
  });

  async function put(e: SubmitEvent) {
    e.preventDefault();
    const file = upload?.[0];
    if (!file) return;
    const to = join(path, file.name);
    if (entries.some((x) => x.name === file.name) && !confirm(t('Replace {path}?', { path: to }))) return;
    busy = true;
    message = '';
    try {
      const r = await api.putFile(ns, name, to, file, chosen);
      message = t('{path} written ({size})', { path: r.path, size: bytes(r.bytes) });
      await load(path);
    } catch (err) {
      error = (err as Error).message;
    } finally {
      busy = false;
    }
  }
</script>

{#if why}
  <p class="muted">{t('Read only: {why}.', { why })}</p>
{:else if task.status?.state !== 'running'}
  <p class="muted">{t('A task’s files are reached while it runs.')}</p>
{:else}
  <div class="controls">
    {#if containers.length > 1}
      <label>
        {t('Container')}
        <select bind:value={container}>
          {#each containers as c (c)}<option value={c}>{c}</option>{/each}
        </select>
      </label>
    {/if}
    <nav aria-label={t('Where in the container')} class="mono">
      <button type="button" class="link" onclick={() => load('/')}>/</button>
      {#each crumbs as c (c.to)}
        <button type="button" class="link" onclick={() => load(c.to)}>{c.part}</button>/
      {/each}
    </nav>
    <button type="button" class="sm" disabled={busy} onclick={() => load(path)}>{t('Refresh')}</button>
  </div>
  {#if error}<p class="error" role="alert">{error}</p>{/if}
  {#if message}<p class="notice" role="status">{message}</p>{/if}

  <!-- svelte-ignore a11y_no_noninteractive_tabindex -->
  <div class="table-scroll" tabindex="0" role="region" aria-label={t('Files in {path}', { path })}>
    <table>
      <caption class="sr-only">{t('Files in {path}', { path })}</caption>
      <thead>
        <tr>
          <th scope="col">{t('Name')}</th>
          <th scope="col">{t('Size')}</th>
          <th scope="col">{t('Mode')}</th>
          <th scope="col">{t('Modified')}</th>
        </tr>
      </thead>
      <tbody>
        {#if path !== '/'}
          <tr>
            <td colspan="4">
              <button type="button" class="link mono" onclick={() => load(crumbs.at(-2)?.to ?? '/')}>..</button>
            </td>
          </tr>
        {/if}
        {#each entries as e (e.name)}
          <tr>
            <td class="mono">
              {#if e.kind === 'dir'}
                <button type="button" class="link" onclick={() => load(join(path, e.name))}>{e.name}/</button>
              {:else if e.kind === 'file'}
                <a href={downloads.file(ns, name, join(path, e.name), chosen)} download={e.name}>{e.name}</a>
              {:else if e.kind === 'link'}
                {e.name} <span class="muted">→ {e.target}</span>
              {:else}
                {e.name}
              {/if}
            </td>
            <td class="mono">{e.kind === 'file' ? bytes(e.size) : ''}</td>
            <td class="mono">{e.mode}</td>
            <td class="mono muted">{new Date(e.modified * 1000).toLocaleString()}</td>
          </tr>
        {:else}
          <tr><td colspan="4" class="muted">{busy ? t('Reading…') : t('Empty')}</td></tr>
        {/each}
      </tbody>
    </table>
  </div>

  <form class="controls" onsubmit={put}>
    <label>
      {t('Upload into {path}', { path })}
      <span class="dropzone" use:droppable><input type="file" bind:files={upload} /><span class="hint" aria-hidden="true">{t('or drop it here')}</span></span>
    </label>
    <button disabled={busy || !upload?.length}>{t('Upload')}</button>
  </form>
{/if}

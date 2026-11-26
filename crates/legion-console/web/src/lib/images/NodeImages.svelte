<script lang="ts">
  // The Images page's first tab, for those who see the whole cluster: the
  // images each node holds (jails and bhyve VMs), pulled ahead, loaded
  // from a .tar, downloaded, pushed, scanned, removed or pruned.
  import { goto } from '$app/navigation';
  import { api, downloads, type CachedNodeImage } from '../api.ts';
  import { ask } from '../confirm.svelte.ts';
  import { droppable } from '../drop.svelte.ts';
  import { t, tn } from '../i18n.svelte.ts';
  import Pager from '../Pager.svelte';
  import RowMenu from '../RowMenu.svelte';
  import SortTh from '../SortTh.svelte';
  import { Table } from '../table.svelte.ts';
  import TableBar from '../TableBar.svelte';
  import type { Studio } from './studio.svelte.ts';

  let {
    studio,
    ondetails,
    onscan,
    onpush
  }: {
    studio: Studio;
    ondetails: (img: CachedNodeImage) => void;
    onscan: (img: CachedNodeImage) => void;
    onpush: (img: CachedNodeImage) => void;
  } = $props();

  let cached: CachedNodeImage[] = $state([]);
  let loading = $state(false);
  let nodeFilter = $state('');
  let pullRef = $state('');
  let pullNode = $state('');
  let loadFiles: FileList | undefined = $state();
  let loadNode = $state('');
  let loadRef = $state('');

  async function refresh() {
    loading = true;
    try {
      cached = (await api.nodeImages(nodeFilter)).images;
    } catch (e) {
      studio.error = (e as Error).message;
    } finally {
      loading = false;
    }
  }

  $effect(() => {
    void nodeFilter;
    refresh();
  });

  // Jails, bhyve VMs (`legion.vm.image`), or both, as the node says.
  let kindFilter: 'all' | 'jail' | 'vm' = $state('all');
  const shown = $derived(cached.filter((i) => kindFilter === 'all' || (kindFilter === 'vm') === !!i.vm));
  // Those of the kind chosen, by reference; the search looks in the
  // reference, the node and the digest.
  const table = new Table(() => shown, {
    sorts: {
      reference: (i) => i.reference,
      node: (i) => i.node,
      platform: (i) => `${i.os}/${i.arch}`,
      layers: (i) => i.layers,
      used: (i) => i.used_by.join(', '),
      digest: (i) => i.digest
    },
    sortBy: 'reference',
    text: (i) => `${i.reference} ${i.node} ${i.digest}`
  });

  async function pull(e: SubmitEvent) {
    e.preventDefault();
    const image = pullRef.trim();
    if (!image) return;
    await studio.act(async () => {
      const r = await api.nodeImageOp({
        op: 'pull',
        node: pullNode,
        image,
        namespace: studio.namespace,
        secret: studio.pullSecret
      });
      pullRef = '';
      await refresh();
      return t('Pulled {image} ({digest})', { image, digest: r.digest ? r.digest.slice(0, 19) : 'ok' });
    });
  }

  async function upload(e: SubmitEvent) {
    e.preventDefault();
    const f = loadFiles?.[0];
    if (!f || !loadNode.trim()) return;
    await studio.act(async () => {
      const r = await api.loadImage(f, { node: loadNode.trim(), reference: loadRef.trim() || undefined });
      await refresh();
      return tn('Loaded archive onto {node} ({n} byte)', 'Loaded archive onto {node} ({n} bytes)', r.bytes, {
        node: r.node
      });
    });
  }

  async function prune() {
    if (!(await ask(t('Prune unreferenced image layers on {target}?', { target: nodeFilter || t('all ready nodes') })))) {
      return;
    }
    await studio.act(async () => {
      const r = await api.nodeImageOp({ op: 'prune', node: nodeFilter });
      await refresh();
      return tn('Pruned {n} unused layer/image', 'Pruned {n} unused layers/images', r.removed);
    });
  }

  async function remove(img: CachedNodeImage) {
    if (!(await ask(t('Remove cached image {image} from {node}?', { image: img.reference, node: img.node })))) return;
    await studio.act(async () => {
      await api.nodeImageOp({ op: 'remove', node: img.node, image: img.reference });
      await refresh();
    });
  }

  const deploy = (image: string) =>
    goto(`/deploy?namespace=${encodeURIComponent(studio.namespace)}&image=${encodeURIComponent(image)}`);
</script>

<section class="grid-2 studio-top">
  <div class="card">
    <h2>{t('Pre-pull image to cluster')}</h2>
    <form class="controls" onsubmit={pull}>
      <input
        bind:value={pullRef}
        placeholder="ghcr.io/freebsd/freebsd-runtime:15.1"
        aria-label={t('Image reference')}
        required
      />
      <select bind:value={pullNode} aria-label={t('Target node')}>
        <option value="">{t('All ready nodes')}</option>
        {#each studio.nodes as n (n.metadata.name)}
          <option value={n.metadata.name}>{n.metadata.name}</option>
        {/each}
      </select>
      <select bind:value={studio.pullSecret} aria-label={t('Registry login Secret')}>
        <option value="">{t('Public (no login)')}</option>
        {#each studio.secrets as s (s)}<option value={s}>{s}</option>{/each}
      </select>
      <button class="primary" disabled={studio.busy || !pullRef.trim()}>{t('Pull')}</button>
    </form>
  </div>

  <div class="card">
    <h2>{t('Load OCI / Docker .tar archive')}</h2>
    <form class="controls" onsubmit={upload}>
      <span class="dropzone" use:droppable
        ><input type="file" accept=".tar,application/x-tar" bind:files={loadFiles} aria-label={t('Image archive')} /><span
          class="hint"
          aria-hidden="true">{t('or drop it here')}</span
        ></span
      >
      <select bind:value={loadNode} aria-label={t('Node')} required>
        <option value="">{t('Choose node…')}</option>
        {#each studio.nodes as n (n.metadata.name)}
          <option value={n.metadata.name}>{n.metadata.name}</option>
        {/each}
      </select>
      <input bind:value={loadRef} placeholder={t('tag override (optional)')} aria-label={t('Tag override')} />
      <button class="primary" disabled={studio.busy || !loadFiles?.length || !loadNode}>
        {t('Upload .tar')}
      </button>
    </form>
  </div>
</section>

<div class="bar">
  <h2>{t('Cached node images ({n})', { n: table.found.length })}</h2>
</div>
<TableBar {table} label={t('Filter cached images')} placeholder={t('Filter reference, digest, node…')}>
  <div class="controls">
    <div class="pill-row" role="group" aria-label={t('Filter by workload')}>
      {#each [{ id: 'all', label: t('Jails & VMs') }, { id: 'jail', label: t('Jails') }, { id: 'vm', label: t('bhyve VMs') }] as const as f (f.id)}
        <button
          type="button"
          class="pill"
          class:active={kindFilter === f.id}
          aria-pressed={kindFilter === f.id}
          onclick={() => (kindFilter = f.id)}>{f.label}</button
        >
      {/each}
    </div>
    <select bind:value={nodeFilter} aria-label={t('Filter by node')}>
      <option value="">{t('All nodes')}</option>
      {#each studio.nodes as n (n.metadata.name)}
        <option value={n.metadata.name}>{n.metadata.name}</option>
      {/each}
    </select>
    <button type="button" class="sm" onclick={refresh} disabled={loading}>
      {t('Refresh')}
    </button>
    <button type="button" class="sm danger" onclick={prune} disabled={studio.busy}>
      {t('Prune unused')}
    </button>
  </div>
</TableBar>

{#if shown.length}
  <!-- svelte-ignore a11y_no_noninteractive_tabindex -->
  <div class="table-scroll" tabindex="0" role="region" aria-label={t('Cached node images')}>
    <table>
      <caption class="sr-only">{t('Cached node images')}</caption>
      <thead>
        <tr>
          <SortTh {table} col="reference">{t('Reference')}</SortTh>
          <SortTh {table} col="node">{t('Node')}</SortTh>
          <SortTh {table} col="platform">{t('Platform')}</SortTh>
          <SortTh {table} col="layers">{t('Layers')}</SortTh>
          <SortTh {table} col="used">{t('Used by')}</SortTh>
          <SortTh {table} col="digest">{t('Digest')}</SortTh>
          <th scope="col">{t('Actions')}</th>
        </tr>
      </thead>
      <tbody>
        {#each table.rows as img (`${img.node}:${img.reference}`)}
          <tr>
            <td>
              <strong>{img.reference}</strong>
              {#if img.vm}<span class="tag">{t('bhyve VM')}</span>{/if}
            </td>
            <td>{img.node}</td>
            <td class="muted">{img.os}/{img.arch}</td>
            <td>{img.layers}</td>
            <td>
              {#each img.used_by as who (who)}
                <div>{who}</div>
              {:else}
                <span class="muted">{t('nothing')}</span>
              {/each}
            </td>
            <td class="mono muted" title={img.digest}>
              {img.digest ? img.digest.slice(0, 19) : '—'}
            </td>
            <td class="controls">
              <button type="button" class="sm" onclick={() => ondetails(img)}>
                {t('Details')}
              </button>
              <button type="button" class="sm" onclick={() => deploy(img.reference)}>
                {t('Deploy')}
              </button>
              <RowMenu label={t('More actions for {image} on {node}', { image: img.reference, node: img.node })}>
                <button type="button" class="sm" onclick={() => onscan(img)}>
                  {t('Scan')}
                </button>
                <a class="button sm" href={downloads.image(img.node, img.reference)} download>
                  {t('Download .tar')}
                </a>
                <button type="button" class="sm" onclick={() => onpush(img)}>
                  {t('Push to GHCR')}
                </button>
                <button type="button" class="sm danger" disabled={studio.busy} onclick={() => remove(img)}>
                  {t('Remove')}
                </button>
              </RowMenu>
            </td>
          </tr>
        {:else}
          <tr><td colspan="7" class="muted">{t('Nothing matches.')}</td></tr>
        {/each}
      </tbody>
    </table>
  </div>
  <Pager {table} label={t('Cached node images')} />
{:else if !loading}
  <p class="empty">{t('No cached images reported on the selected nodes.')}</p>
{/if}

<style>
  .studio-top {
    margin-bottom: 1rem;
  }
</style>

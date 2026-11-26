<script lang="ts">
  // The Images page's package tab: a Build from a Containerfile or a Git
  // repository, pushed to GHCR or another registry and, if asked, rolled
  // out to a Service; and the namespace's Build runs.
  import { api, follow, type LObject } from '../api.ts';
  import { tone } from '../columns.ts';
  import { droppable } from '../drop.svelte.ts';
  import { t } from '../i18n.svelte.ts';
  import Pager from '../Pager.svelte';
  import { openWorkbench } from '../session.svelte.ts';
  import SortTh from '../SortTh.svelte';
  import { Table } from '../table.svelte.ts';
  import TableBar from '../TableBar.svelte';
  import { TEMPLATES, type Studio } from './studio.svelte.ts';

  let { studio, onnewsecret }: { studio: Studio; onnewsecret: () => void } = $props();

  const d = $derived(studio.build);
  let builds: LObject[] = $state([]);
  let services: LObject[] = $state([]);

  $effect(() => {
    const ns = studio.namespace;
    if (!ns) return;
    const stops = [
      follow('Build', ns, (o) => (builds = o.slice().reverse()), () => {}),
      follow('Service', ns, (o) => (services = o), () => {})
    ];
    return () => stops.forEach((s) => s());
  });

  // The namespace's Build runs, newest first as they came until a header
  // sorts them; a page holds 15, as many as the card showed before.
  const table = new Table(() => builds, {
    sorts: {
      build: (b) => b.metadata.name,
      image: (b) => b.spec?.image ?? '',
      phase: (b) => b.status?.phase ?? 'Pending'
    },
    size: 15
  });

  const watch = (namespace: string, name: string, object: LObject) =>
    openWorkbench({ id: `build:${namespace}/${name}`, kind: 'build', title: `${name} · ${t('Build')}`, namespace, name, object });

  async function start(e: SubmitEvent) {
    e.preventDefault();
    const image = d.image.trim();
    const namespace = studio.namespace;
    const baseName =
      image
        .split('@')[0]
        .split(':')[0]
        .split('/')
        .pop()
        ?.toLowerCase()
        .replace(/[^a-z0-9-]/g, '-')
        .replace(/^-+|-+$/g, '') || 'image';
    const name = `${baseName}-${Date.now().toString(36)}`.slice(0, 63);
    const source =
      d.from === 'git'
        ? {
            git: {
              url: d.gitUrl.trim(),
              ...(d.gitRef.trim() ? { ref: d.gitRef.trim() } : {}),
              ...(d.gitPath.trim() ? { path: d.gitPath.trim() } : {})
            }
          }
        : { containerfile: d.containerfile };
    const spec: Record<string, unknown> = {
      source,
      image,
      ...(studio.buildSecret ? { pushSecret: studio.buildSecret } : {}),
      ...(d.rollout ? { rollout: { service: d.rollout } } : {})
    };
    await studio.act(async () => {
      await api.apply({ object: { apiVersion: 'legion/v1', kind: 'Build', metadata: { name, namespace }, spec } });
      const created = await api.get('Build', namespace, name).catch(() => null);
      if (created) watch(namespace, name, created);
      return t('Started Build {namespace}/{name} → {image}', { namespace, name, image });
    });
  }
</script>

<div class="split-pane">
  <section class="card">
    <h2>{t('Package jail image & push to GHCR')}</h2>
    <form class="stacked-form" onsubmit={start}>
      <div class="controls">
        <label class="choice">
          <input type="radio" bind:group={d.from} value="containerfile" />
          {t('Containerfile')}
        </label>
        <label class="choice">
          <input type="radio" bind:group={d.from} value="git" />
          {t('Git repository')}
        </label>
        {#if d.from === 'containerfile'}
          <span class="muted">{t('Starter:')}</span>
          <button type="button" class="sm" onclick={() => (d.containerfile = TEMPLATES.minimal)}>
            {t('FreeBSD minimal')}
          </button>
          <button type="button" class="sm" onclick={() => (d.containerfile = TEMPLATES.pkg)}>
            {t('FreeBSD nginx pkg')}
          </button>
          <button type="button" class="sm" onclick={() => (d.containerfile = TEMPLATES.daemon)}>
            {t('App daemon')}
          </button>
        {/if}
      </div>

      {#if d.from === 'containerfile'}
        <label>
          {t('Containerfile')}
          <textarea class="editor" use:droppable rows="10" bind:value={d.containerfile} spellcheck="false"></textarea>
        </label>
      {:else}
        <label>
          {t('Git URL')}
          <input bind:value={d.gitUrl} placeholder="https://github.com/org/repo.git" required />
        </label>
        <div class="grid-2">
          <label>
            {t('Branch or tag')}
            <input bind:value={d.gitRef} placeholder="main" />
          </label>
          <label>
            {t('Subdirectory')}
            <input bind:value={d.gitPath} placeholder="." />
          </label>
        </div>
      {/if}

      <label>
        {t('Push destination (GHCR or OCI registry)')}
        <input bind:value={d.image} placeholder="ghcr.io/your-org/jail-service:v1" required />
      </label>

      <div class="controls">
        <label style="flex: 1">
          {t('Registry credentials Secret')}
          <select bind:value={studio.buildSecret}>
            <option value="">{t('none')}</option>
            {#each studio.secrets as s (s)}<option value={s}>{s}</option>{/each}
          </select>
        </label>
        <button type="button" class="sm" onclick={onnewsecret}>
          {t('New GHCR Secret…')}
        </button>
      </div>

      <label>
        {t('Auto-rollout Service after push')} <span class="muted">{t('(optional)')}</span>
        <select bind:value={d.rollout}>
          <option value="">{t('Do not roll out a Service (package & push only)')}</option>
          {#each services as s (s.metadata.name)}
            <option value={s.metadata.name}>{s.metadata.name}</option>
          {/each}
        </select>
      </label>

      <div class="controls">
        <button class="primary" disabled={!!studio.writeWhy || studio.busy || !d.image.trim()} title={studio.writeWhy ?? ''}>
          {t('Build & Push Image')}
        </button>
        {#if studio.writeWhy}<span class="muted">{studio.writeWhy}</span>{/if}
      </div>
    </form>
  </section>

  <section class="card">
    <h2>{t('Recent Build runs in {namespace}', { namespace: studio.namespace })}</h2>
    {#if builds.length}
      <TableBar {table} label={t('Search Build runs')} />
      <table>
        <caption class="sr-only">{t('Recent Build runs')}</caption>
        <thead>
          <tr>
            <SortTh {table} col="build">{t('Build')}</SortTh>
            <SortTh {table} col="image">{t('Target image')}</SortTh>
            <SortTh {table} col="phase">{t('Phase')}</SortTh>
            <th scope="col">{t('Workbench')}</th>
          </tr>
        </thead>
        <tbody>
          {#each table.rows as b (b.metadata.uid)}
            <tr>
              <td>
                <a href="/k/Build/{studio.namespace}/{b.metadata.name}?tab=buildlog">{b.metadata.name}</a>
              </td>
              <td class="mono">{b.spec?.image ?? '—'}</td>
              <td>
                <span class="state {tone(b.status?.phase?.toLowerCase?.() ?? '')}">
                  {b.status?.phase ?? 'Pending'}
                </span>
              </td>
              <td>
                <button
                  type="button"
                  class="sm"
                  data-look
                  onclick={() => watch(studio.namespace, b.metadata.name, b)}
                >
                  {t('Keep log open')}
                </button>
              </td>
            </tr>
          {:else}
            <tr><td colspan="4" class="muted">{t('Nothing matches.')}</td></tr>
          {/each}
        </tbody>
      </table>
      <Pager {table} label={t('Build runs')} />
    {:else}
      <p class="muted">
        {t(
          'No Builds in this namespace yet. Label at least one FreeBSD node with legion.builder=true to execute jailer image builds.'
        )}
      </p>
    {/if}
  </section>
</div>

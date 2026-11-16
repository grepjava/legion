<script lang="ts">
  // Split YAML & Live Dry-Run Diff Studio: paste or edit YAML on the left,
  // inspect server-evaluated `legion diff` unified changes on the right,
  // and apply with optimistic concurrency protection (`expectedVersion`).
  import { droppable, dropped, takeDropped } from '#lib/drop.svelte.ts';
  import { page } from '$app/state';
  import { api, ApiError, type ApplyResult } from '#lib/api.ts';
  import Diff from '#lib/Diff.svelte';
  import { t } from '#lib/i18n.svelte.ts';
  import { cannotWriteAnywhere, session } from '#lib/session.svelte.ts';

  const kind = $derived(page.url.searchParams.get('kind') ?? '');
  const namespace = $derived(page.url.searchParams.get('namespace') ?? '');
  const name = $derived(page.url.searchParams.get('name') ?? '');
  const editing = $derived(Boolean(kind && name));

  let yaml = $state('');
  let version: number | undefined = $state();
  let results: ApplyResult[] = $state([]);
  let dry = $state(false);
  let autoPreview = $state(true);
  let error = $state('');
  let busy = $state(false);
  const why = $derived(cannotWriteAnywhere());

  $effect(() => {
    results = [];
    error = '';
    version = undefined;
    if (!editing) {
      yaml = '';
      return;
    }
    api
      .manifest(kind, namespace, name)
      .then((m) => {
        yaml = m.yaml;
        version = m.version;
      })
      .catch((e) => (error = e.message));
  });

  // A YAML file dropped elsewhere in the console, opened here.
  $effect(() => {
    if (dropped.apply) yaml = takeDropped('apply');
  });

  // The YAML last applied: the live preview leaves its result on show
  // until the YAML changes.
  let applied: string | null = $state(null);
  // Each request's number; only the latest one started shows its answer,
  // so a preview that started before an Apply can't replace its result.
  let latest = 0;

  // Debounced live server dry-run diff preview when autoPreview is checked.
  $effect(() => {
    const text = yaml;
    if (!autoPreview || !!why || !text.trim() || text === applied) return;
    const timer = setTimeout(() => {
      run(true, true);
    }, 550);
    return () => clearTimeout(timer);
  });

  async function run(dryRun: boolean, quiet = false) {
    const mine = ++latest;
    const text = yaml;
    // Applying cancels a preview still waiting (the effect re-runs), and
    // none starts again for this YAML: its result or error stays shown.
    if (!dryRun) applied = text;
    if (!quiet) busy = true;
    error = '';
    try {
      const r = await api.apply({
        yaml: text,
        dryRun,
        expectedVersion: editing ? version : undefined
      });
      if (mine !== latest) return;
      results = r.results;
      dry = dryRun;
      if (!dryRun && editing && results.length === 1) version = results[0].version;
    } catch (e) {
      if (mine !== latest) return;
      results = [];
      error =
        e instanceof ApiError && e.status === 409 && editing
          ? t(
              '{message}. It changed after you opened it; reload the page to edit what is stored now.',
              { message: e.message }
            )
          : (e as Error).message;
    } finally {
      if (!quiet) busy = false;
    }
  }

  function insertSnippet(which: 'service' | 'volume' | 'build') {
    const ns = session.namespace || 'default';
    if (which === 'service') {
      yaml = `apiVersion: legion/v1\nkind: Service\nmetadata:\n  name: web\n  namespace: ${ns}\nspec:\n  replicas: 2\n  pod:\n    containers:\n      - name: web\n        image: ghcr.io/daemonless/nginx:latest\n        ports:\n          - name: http\n            containerPort: 80\n`;
    } else if (which === 'volume') {
      yaml = `apiVersion: legion/v1\nkind: Volume\nmetadata:\n  name: pgdata\n  namespace: ${ns}\nspec:\n  driver: zfs\n  size: 10Gi\n`;
    } else {
      yaml = `apiVersion: legion/v1\nkind: Build\nmetadata:\n  name: custom-jail-1\n  namespace: ${ns}\nspec:\n  image: ghcr.io/your-org/jail-app:v1\n  source:\n    containerfile: |\n      FROM ghcr.io/freebsd/freebsd-runtime:14.2\n      CMD ["/bin/sh", "-c", "sleep 3600"]\n`;
    }
  }

  const verb = (r: ApplyResult) =>
    dry
      ? t('would be {result}', { result: r.result })
      : t('{result}, version {version}', { result: r.result, version: r.version });
</script>

<header class="bar">
  <h1>
    {#if editing}
      {t('Edit')}
      <a href="/k/{kind}/{namespace || '_'}/{name}">{kind} {namespace ? `${namespace}/` : ''}{name}</a>
    {:else}
      {t('Apply YAML')}
    {/if}
  </h1>
  {#if !editing}
    <div class="controls">
      <span class="muted">{t('Insert starter:')}</span>
      <button type="button" class="sm" disabled={!!why} title={why ?? ''} onclick={() => insertSnippet('service')}
        >{t('Service')}</button
      >
      <button type="button" class="sm" disabled={!!why} title={why ?? ''} onclick={() => insertSnippet('volume')}
        >{t('ZFS Volume')}</button
      >
      <button type="button" class="sm" disabled={!!why} title={why ?? ''} onclick={() => insertSnippet('build')}
        >{t('GHCR Build')}</button
      >
    </div>
  {/if}
</header>
<p class="muted">
  {#if editing}
    {t('Applied only over version {version}: if it changes meanwhile, nothing is written.', {
      version: version ?? '…'
    })}
  {:else}
    {t('One or more objects, as')} <code>legion apply</code> {t('takes them, separated by')} <code>---</code>.
  {/if}
</p>

<div class="split-pane">
  <div>
    <textarea
      class="editor"
      use:droppable
      bind:value={yaml}
      spellcheck="false"
      rows="24"
      aria-label={t('YAML')}
    ></textarea>
    <div class="controls" style="margin-top: 0.5rem">
      <button
        type="button"
        onclick={() => run(true)}
        disabled={!!why || busy || !yaml.trim()}
        title={why ?? ''}>{t('Show changes')}</button
      >
      <button
        type="button"
        class="primary"
        onclick={() => run(false)}
        disabled={!!why || busy || !yaml.trim()}
        title={why ?? ''}>{t('Apply')}</button
      >
      <label class="inline-check">
        <input type="checkbox" bind:checked={autoPreview} />
        {t('Live dry-run diff')}
      </label>
      {#if busy}<span class="muted">…</span>{/if}
    </div>
    {#if why}<p class="muted">{t('Read only: {why}.', { why })}</p>{/if}
    {#if error}<p class="error" role="alert">{error}</p>{/if}
  </div>

  <section class="card" aria-label={t('Dry-run diff preview')}>
    <h2>{dry ? t('Dry-run preview') : t('Apply result')}</h2>
    {#if results.length}
      {#each results as r (r.key)}
        <h3>{r.key}: <span class="muted">{verb(r)}</span></h3>
        {#if r.diff}
          <Diff text={r.diff} />
        {:else}
          <p class="muted">{t('No manifest changes.')}</p>
        {/if}
      {/each}
    {:else}
      <p class="muted">
        {t('Write or paste YAML on the left to preview the unified server diff before applying.')}
      </p>
    {/if}
  </section>
</div>

<style>
  .inline-check {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    font-size: 0.84rem;
    color: var(--muted);
  }
</style>

<script lang="ts">
  // Deploy from an image (CONSOLE.md phase 5): a form writing the YAML,
  // shown alongside as it is written, applied as `legion apply` would.
  import { droppable } from '#lib/drop.svelte.ts';
  import { page } from '$app/state';
  import { api, type ApplyResult } from '#lib/api.ts';
  import Diff from '#lib/Diff.svelte';
  import { blank, objects } from '#lib/deploy.ts';
  import { cannotWrite, namespaces, session } from '#lib/session.svelte.ts';
  import { t } from '#lib/i18n.svelte.ts';
  import { toYaml } from '#lib/yaml.ts';
  import { dropDraft, loadDraft, saveDraft } from '#lib/drafts.svelte.ts';
  import type { DeployForm } from '#lib/deploy.ts';
  import Ico from '#lib/Ico.svelte';
  import Tabs from '#lib/Tabs.svelte';
  import { RotateCcw } from '@lucide/svelte';

  let all: string[] = $state([]);
  $effect(() => {
    if (session.me?.all)
      api.list('Namespace').then((r) => (all = r.objects.map((o) => o.metadata.name).sort()));
  });
  const choices = $derived(namespaces(all));
  // What was typed last, unless a link brings an image of its own.
  const fresh = () => blank(session.namespace || 'default');
  let form = $state<DeployForm>(
    (!page.url.searchParams.get('image') && loadDraft<DeployForm>('deploy')) || fresh()
  );
  $effect(() => saveDraft('deploy', $state.snapshot(form), fresh()));
  function startOver() {
    form = fresh();
    results = [];
    error = '';
    dropDraft('deploy');
  }
  $effect(() => {
    const qImg = page.url.searchParams.get('image');
    const qNs = page.url.searchParams.get('namespace');
    if (qNs) form.namespace = qNs;
    if (qImg && !form.image) {
      form.image = qImg;
      const base = qImg.split('@')[0].split(':')[0].split('/').pop() ?? '';
      const clean = base.toLowerCase().replace(/[^a-z0-9-]/g, '-').replace(/^-+|-+$/g, '');
      if (clean && !form.name) form.name = clean;
    }
  });
  const built = $derived(objects(form));
  const yaml = $derived(typeof built === 'string' ? '' : built.map(toYaml).join('---\n'));
  const why = $derived(cannotWrite(form.namespace));
  let results: ApplyResult[] = $state([]);
  let dry = $state(false);
  let error = $state('');
  let busy = $state(false);

  async function run(dryRun: boolean) {
    busy = true;
    error = '';
    try {
      results = (await api.apply({ yaml, dryRun })).results;
      dry = dryRun;
      // Deployed, the form has done its work: no draft to come back to.
      if (!dryRun) dropDraft('deploy');
    } catch (e) {
      results = [];
      error = (e as Error).message;
    } finally {
      busy = false;
    }
  }
  const deployed = $derived(!dry && results.some((r) => r.key.startsWith('Service/')));

  // The form in parts, as tabs, with Next and Back between them; a tab is
  // marked where something on it differs from a blank form.
  const empty = blank('');
  const STEPS = $derived([
    { id: 'basic', label: t('Basic'), filled: () => true },
    {
      id: 'network',
      label: t('Network'),
      filled: () => !!form.port || !!form.publishedPort || form.probe !== empty.probe
    },
    { id: 'env', label: t('Environment'), filled: () => !!form.env.trim() },
    { id: 'storage', label: t('Storage'), filled: () => !!form.mountPath.trim() },
    { id: 'resources', label: t('Resources'), filled: () => !!form.memory.trim() || !!form.cpu.trim() }
  ]);
  let step = $state('basic');
  const stepAt = $derived(STEPS.findIndex((s) => s.id === step));
</script>

<header class="bar">
  <h1>{t('Deploy from an image')}</h1>
  <button type="button" class="sm" onclick={startOver} title={t('Empty the form and forget the draft')}
    ><Ico icon={RotateCcw} size={14} />{t('Start over')}</button
  >
</header>
<p class="muted intro">
  {t('A Service from an image, written as YAML alongside; change the YAML further in')}
  <a href="/apply">{t('Apply YAML')}</a>.
</p>

<div class="split">
  <form class="card deploy" onsubmit={(e) => e.preventDefault()}>
    <Tabs
      tabs={STEPS.map((s) => ({ id: s.id, label: s.label, marked: s.id !== 'basic' && s.filled() }))}
      bind:tab={step}
      label={t('Parts of the service')}
      idPrefix="deploy-tab"
      panelId="deploy-panel"
      markLabel={t('filled in')}
    />
    <div class="panel" role="tabpanel" id="deploy-panel" aria-labelledby="deploy-tab-{step}">
      {#if step === 'basic'}
        <label>{t('Name')} <input bind:value={form.name} aria-label={t('Name')} placeholder="web" /></label>
        <label
          >{t('Namespace')}
          <select bind:value={form.namespace} aria-label={t('Namespace')}>
            {#each choices.includes(form.namespace) ? choices : [form.namespace, ...choices] as n (n)}<option
                value={n}>{n}</option
              >{/each}
          </select>
        </label>
        <label
          >{t('Image')}
          <input bind:value={form.image} aria-label={t('Image')} placeholder="registry.example/web:1" /></label
        >
        <p class="note muted">
          {t('An image from a registry, or one built here:')} <a href="/images">{t('Images')}</a>.
        </p>
        <label
          >{t('Replicas')}
          <input type="number" min="0" bind:value={form.replicas} aria-label={t('Replicas')} /></label
        >
        <label
          >{t('Command')} <span class="muted">{t("(one argument a line; empty keeps the image's)")}</span>
          <textarea rows="3" bind:value={form.command} aria-label={t('Command')}></textarea>
        </label>
      {:else if step === 'network'}
        <label
          >{t('Container port')} <span class="muted">{t('(empty: it listens on none)')}</span>
          <input
            type="number"
            min="1"
            max="65535"
            placeholder="8080"
            bind:value={() => form.port || null, (v) => (form.port = Number(v) || 0)}
            aria-label={t('Container port')}
          />
        </label>
        <label
          >{t('Published on every node at')} <span class="muted">{t('(empty: inside the cluster only)')}</span>
          <input
            type="number"
            min="1"
            max="65535"
            placeholder="80"
            bind:value={() => form.publishedPort || null, (v) => (form.publishedPort = Number(v) || 0)}
            aria-label={t('Published port')}
          />
        </label>
        <label
          >{t('Readiness probe')}
          <select bind:value={form.probe} aria-label={t('Readiness probe')}>
            <option value="none">{t('none')}</option>
            <option value="http">{t('HTTP GET')}</option>
            <option value="tcp">{t('TCP connect')}</option>
          </select>
        </label>
        {#if form.probe === 'http'}<label
            >{t('Probe path')} <input bind:value={form.probePath} aria-label={t('Probe path')} /></label
          >{/if}
      {:else if step === 'env'}
        <label
          >{t('Environment')} <span class="muted">{t('(NAME=value a line)')}</span>
          <textarea rows="8" use:droppable bind:value={form.env} aria-label={t('Environment')}></textarea>
        </label>
        <p class="note muted">{t('Drop a .env file on the field to fill it.')}</p>
      {:else if step === 'storage'}
        <label
          >{t('Mount a Volume at')} <span class="muted">{t('(empty: no Volume)')}</span>
          <input bind:value={form.mountPath} aria-label={t('Mount path')} placeholder="/data" /></label
        >
        {#if form.mountPath.trim()}
          <label
            >{t('Volume')}
            <input bind:value={form.volume} aria-label={t('Volume')} placeholder={form.name || t('name')} /></label
          >
          <label
            >{t("New Volume's size")} <span class="muted">{t('(empty: one there is)')}</span>
            <input bind:value={form.volumeSize} aria-label={t('Volume size')} placeholder="1Gi" />
          </label>
          {#if form.volumeSize.trim()}
            <label
              >{t('Copy to another node every')}
              <select bind:value={form.replicaEvery} aria-label={t('Replica')}>
                <option value={0}>{t('never (no replica)')}</option>
                <option value={10}>{t('10 seconds')}</option>
                <option value={60}>{t('minute')}</option>
                <option value={300}>{t('5 minutes')}</option>
                <option value={3600}>{t('hour')}</option>
              </select>
            </label>
            {#if form.replicaEvery}
              <label
                >{t('Failover')}
                <select bind:value={form.failover} aria-label={t('Failover')}>
                  <option value="manual">{t('manual: when its node is removed')}</option>
                  <option value="automatic">{t('automatic: once its node is down and its tasks there stopped')}</option>
                </select>
              </label>
              <p class="muted note">
                {form.failover === 'automatic'
                  ? t('Its tasks stop where they are cut off from the cluster (onPartition: stop), so only one copy is ever written to. What was written since the last copy is lost with the node.')
                  : t('A copy is kept on another node; removing this node (legion delete node) carries the Volume on from it. What was written since the last copy is lost.')}
              </p>
            {/if}
          {/if}
        {/if}
      {:else}
        <label
          >{t('Memory')} <span class="muted">{t('(empty: no limit)')}</span>
          <input bind:value={form.memory} aria-label={t('Memory')} placeholder="256Mi" /></label
        >
        <label
          >{t('CPU')} <span class="muted">{t('(empty: none asked for)')}</span>
          <input bind:value={form.cpu} aria-label={t('CPU')} placeholder="500m" /></label
        >
      {/if}
    </div>
    <div class="steps">
      {#if stepAt > 0}
        <button type="button" onclick={() => (step = STEPS[stepAt - 1].id)}
          >← {STEPS[stepAt - 1].label}</button
        >
      {/if}
      {#if stepAt < STEPS.length - 1}
        <button type="button" class="next" onclick={() => (step = STEPS[stepAt + 1].id)}
          >{STEPS[stepAt + 1].label} →</button
        >
      {/if}
    </div>
  </form>

  <section class="card yaml-side" aria-labelledby="yaml-title">
    <h2 id="yaml-title">{t('What will be applied')}</h2>
    {#if typeof built === 'string'}
      <p class="muted">{built}.</p>
    {:else}
      <pre class="yaml" aria-label={t('Generated YAML')}>{yaml}</pre>
    {/if}
    <div class="controls">
      <button onclick={() => run(true)} disabled={!yaml || !!why || busy} title={why ?? ''}>{t('Show changes')}</button>
      <button class="primary" onclick={() => run(false)} disabled={!yaml || !!why || busy} title={why ?? ''}
        >{t('Deploy')}</button
      >
      {#if busy}<span class="muted">…</span>{/if}
    </div>
    {#if why && yaml}<p class="muted">{why}.</p>{/if}
    {#if error}<p class="error" role="alert">{error}</p>{/if}
    {#each results as r (r.key)}
      <h3>{r.key}: <span class="muted"
          >{dry
            ? t('would be {result}', { result: r.result })
            : t('{result}, version {version}', { result: r.result, version: r.version })}</span
        ></h3>
      {#if r.diff}<Diff text={r.diff} />{/if}
    {/each}
    {#if deployed}
      <p><a href="/k/Service/{form.namespace}/{form.name.trim()}">{t('See {name} roll out', { name: form.name.trim() })}</a></p>
    {/if}
  </section>
</div>

<style>
  .split {
    display: grid;
    grid-template-columns: minmax(26rem, 36rem) 1fr;
    gap: 1.25rem;
    align-items: start;
  }
  @media (max-width: 64rem) {
    .split {
      grid-template-columns: 1fr;
    }
  }
  .deploy {
    display: flex;
    flex-direction: column;
    padding-top: 0.25rem;
  }
  .deploy :global(.tabs) {
    margin-top: 0;
  }
  .deploy :global(.tabs button) {
    padding-inline: 0.6rem;
  }
  .panel {
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
    min-height: 16rem;
  }
  .panel label {
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
    font-weight: 500;
  }
  .panel label .muted {
    font-weight: 400;
    font-size: 0.84rem;
  }
  .panel .note {
    margin-top: -0.4rem;
  }
  .steps {
    display: flex;
    gap: 0.5rem;
    margin-top: 1rem;
    padding-top: 0.75rem;
    border-top: 1px solid var(--line);
  }
  .steps .next {
    margin-left: auto;
  }
  .yaml-side h2 {
    font-size: 0.9rem;
  }
  pre.yaml {
    min-height: 12rem;
    background: var(--bg);
  }
</style>

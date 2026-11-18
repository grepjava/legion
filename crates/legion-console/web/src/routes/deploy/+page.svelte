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
</script>

<header class="bar">
  <h1>{t('Deploy from an image')}</h1>
  <button type="button" class="sm" onclick={startOver} title={t('Empty the form and forget the draft')}
    ><Ico icon={RotateCcw} size={14} />{t('Start over')}</button
  >
</header>
<p class="muted">
  {t('A Service from an image, written as YAML alongside; change the YAML further in')}
  <a href="/apply">{t('Apply YAML')}</a>.
</p>

<div class="split">
  <form class="deploy" onsubmit={(e) => e.preventDefault()}>
    <label>{t('Name')} <input bind:value={form.name} aria-label={t('Name')} placeholder="web" /></label>
    <label
      >{t('Namespace')}
      <select bind:value={form.namespace} aria-label={t('Namespace')}>
        {#each choices.includes(form.namespace) ? choices : [form.namespace, ...choices] as n (n)}<option
            value={n}>{n}</option
          >{/each}
      </select>
    </label>
    <label>{t('Image')} <input bind:value={form.image} aria-label={t('Image')} placeholder="registry.example/web:1" /></label>
    <label>{t('Replicas')} <input type="number" min="0" bind:value={form.replicas} aria-label={t('Replicas')} /></label>
    <label
      >{t('Command')} <span class="muted">{t("(one argument a line; empty keeps the image's)")}</span>
      <textarea rows="2" bind:value={form.command} aria-label={t('Command')}></textarea>
    </label>
    <fieldset>
      <legend>{t('Network')}</legend>
      <label
        >{t('Container port')} <input type="number" min="0" max="65535" bind:value={form.port} aria-label={t('Container port')} /></label
      >
      <label
        >{t('Published on every node at')} <span class="muted">{t('(0: inside the cluster only)')}</span>
        <input type="number" min="0" max="65535" bind:value={form.publishedPort} aria-label={t('Published port')} />
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
    </fieldset>
    <label
      >{t('Environment')} <span class="muted">{t('(NAME=value a line)')}</span>
      <textarea rows="3" use:droppable bind:value={form.env} aria-label={t('Environment')}></textarea>
    </label>
    <fieldset>
      <legend>{t('Storage')}</legend>
      <label>{t('Mount a Volume at')} <input bind:value={form.mountPath} aria-label={t('Mount path')} placeholder="/data" /></label>
      {#if form.mountPath.trim()}
        <label
          >{t('Volume')} <input bind:value={form.volume} aria-label={t('Volume')} placeholder={form.name || t('name')} /></label
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
    </fieldset>
    <fieldset>
      <legend>{t('Resources')}</legend>
      <label>{t('Memory')} <input bind:value={form.memory} aria-label={t('Memory')} placeholder="256Mi" /></label>
      <label>{t('CPU')} <input bind:value={form.cpu} aria-label={t('CPU')} placeholder="500m" /></label>
    </fieldset>
  </form>

  <div>
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
      <h2>{r.key}: <span class="muted"
          >{dry
            ? t('would be {result}', { result: r.result })
            : t('{result}, version {version}', { result: r.result, version: r.version })}</span
        ></h2>
      {#if r.diff}<Diff text={r.diff} />{/if}
    {/each}
    {#if deployed}
      <p><a href="/k/Service/{form.namespace}/{form.name.trim()}">{t('See {name} roll out', { name: form.name.trim() })}</a></p>
    {/if}
  </div>
</div>

<style>
  .split {
    display: grid;
    grid-template-columns: minmax(18rem, 26rem) 1fr;
    gap: 1.5rem;
    align-items: start;
  }
  .deploy {
    display: flex;
    flex-direction: column;
    gap: 0.6rem;
  }
  .deploy label {
    display: flex;
    flex-direction: column;
    gap: 0.2rem;
  }
  fieldset {
    border: 1px solid var(--line);
    border-radius: 6px;
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }
  pre.yaml {
    background: var(--panel);
    border: 1px solid var(--line);
    border-radius: 6px;
    padding: 0.75rem;
    min-height: 12rem;
  }
</style>

<script lang="ts">
  // Deploy from an image (CONSOLE.md phase 5): a form writing the YAML,
  // shown alongside as it is written, applied as `legion apply` would.
  import { api, type ApplyResult } from '#lib/api.ts';
  import Diff from '#lib/Diff.svelte';
  import { blank, objects } from '#lib/deploy.ts';
  import { cannotWrite, namespaces, session } from '#lib/session.svelte.ts';
  import { t } from '#lib/i18n.svelte.ts';
  import { toYaml } from '#lib/yaml.ts';

  let all: string[] = $state([]);
  $effect(() => {
    if (session.me?.all)
      api.list('Namespace').then((r) => (all = r.objects.map((o) => o.metadata.name).sort()));
  });
  const choices = $derived(namespaces(all));
  let form = $state(blank(session.namespace || 'default'));
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
    } catch (e) {
      results = [];
      error = (e as Error).message;
    } finally {
      busy = false;
    }
  }
  const deployed = $derived(!dry && results.some((r) => r.key.startsWith('Service/')));
</script>

<header class="bar"><h1>{t('Deploy from an image')}</h1></header>
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
      <textarea rows="3" bind:value={form.env} aria-label={t('Environment')}></textarea>
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

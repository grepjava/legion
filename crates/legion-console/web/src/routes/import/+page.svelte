<script lang="ts">
  // Compose import (CONSOLE.md phase 5): a `docker-compose.yml` as the
  // objects `legion stack deploy` would apply, shown before applying,
  // with what Legion cannot honour and the file's variables as fields. A
  // template opens here; a compose file is saved as one.
  import { droppable, dropped, takeDropped } from '#lib/drop.svelte.ts';
  import { page } from '$app/state';
  import { api, compose, templates, type ApplyResult, type ComposeVariable } from '#lib/api.ts';
  import Diff from '#lib/Diff.svelte';
  import { t } from '#lib/i18n.svelte.ts';
  import { cannotWrite, session } from '#lib/session.svelte.ts';
  import { toYaml } from '#lib/yaml.ts';

  let text = $state('');
  let stack = $state('');
  let namespace = $state('');
  let values: Record<string, string> = $state({});
  let variables: ComposeVariable[] = $state([]);
  let warnings: string[] = $state([]);
  let yaml = $state('');
  let convertError = $state('');
  let results: ApplyResult[] = $state([]);
  let dry = $state(false);
  let error = $state('');
  let busy = $state(false);
  let saveName = $state('');
  let saveNamespace = $state(session.namespace || 'default');
  let saved = $state('');

  // An app opened from the Apps page: its compose file, and what its
  // variables are, with their defaults (a secret made up) to look over.
  let app: { name: string; description: string; labels: Record<string, string> } | null = $state(null);
  $effect(() => {
    const id = page.url.searchParams.get('app');
    if (!id) return;
    api
      .app(id, page.url.searchParams.get('variant') ?? '')
      .then((a) => {
        text = a.compose;
        stack = a.id;
        values = { ...a.values };
        app = {
          name: a.name || a.id,
          description: a.description,
          labels: Object.fromEntries(a.variables.filter((v) => v.label).map((v) => [v.name, v.label ?? '']))
        };
      })
      .catch((e) => (convertError = (e as Error).message));
  });

  // A template opened from the catalog.
  $effect(() => {
    const name = page.url.searchParams.get('template');
    if (!name) return;
    const from = page.url.searchParams.get('from') ?? '';
    templates()
      .then((r) => {
        const found = r.templates.find((x) => x.name === name && x.namespace === from);
        if (!found) {
          convertError = from
            ? t('no template {namespace}/{name}', { namespace: from, name })
            : t('no template {name}', { name });
          return;
        }
        text = found.compose;
        stack = found.name;
        saveName = found.name;
      })
      .catch((e) => (convertError = e.message));
  });

  // A compose file dropped elsewhere in the console, opened here.
  $effect(() => {
    if (dropped.compose) text = takeDropped('compose');
  });

  // Each conversion's number: only the latest one started is shown, so a
  // slow answer for older text can't replace the YAML that Deploy applies.
  let converting = 0;

  // Converted again a moment after each change.
  $effect(() => {
    const body = { compose: text, stack: stack.trim(), namespace: namespace.trim() || undefined, env: { ...values } };
    if (!body.compose.trim() || !body.stack) {
      yaml = '';
      warnings = [];
      convertError = '';
      return;
    }
    const mine = ++converting;
    const timer = setTimeout(async () => {
      try {
        const r = await compose(body);
        if (mine !== converting) return;
        yaml = r.objects.map(toYaml).join('---\n');
        warnings = r.warnings;
        variables = r.variables;
        convertError = '';
        for (const v of r.variables) if (!(v.name in values)) values[v.name] = v.default ?? '';
      } catch (e) {
        if (mine !== converting) return;
        yaml = '';
        convertError = (e as Error).message;
      }
    }, 300);
    return () => clearTimeout(timer);
  });

  const target = $derived(namespace.trim() || stack.trim());
  const why = $derived(cannotWrite(target));

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

  async function save() {
    saved = '';
    error = '';
    const config = {
      apiVersion: 'legion/v1',
      kind: 'Config',
      metadata: { name: saveName.trim(), namespace: saveNamespace.trim(), labels: { 'legion.template': 'compose' } },
      spec: { text }
    };
    try {
      await api.apply({ yaml: toYaml(config) });
      saved = t('Saved as Config {namespace}/{name}.', { namespace: saveNamespace.trim(), name: saveName.trim() });
    } catch (e) {
      error = (e as Error).message;
    }
  }
</script>

<header class="bar"><h1>{t('Import compose')}</h1></header>
<p class="muted">
  {t('A compose file as the objects')} <code>legion stack deploy</code>
  {t('applies, shown before they are. Files it names are not read here: put their content in it.')}
  {t('From the')} <a href="/templates">{t('templates')}</a>{t(', or pasted.')}
</p>
{#if app}
  <section class="card app-note" aria-label={t('The app')}>
    <h2>{app.name}</h2>
    {#if app.description}<p class="muted">{app.description}</p>{/if}
    <p class="muted">
      {t('From the daemonless catalog. Its data goes on the Volumes it names; a secret it asks for was made up here: note it before deploying.')}
    </p>
  </section>
{/if}

<div class="split">
  <div class="side">
    <label>{t('Stack')} <input bind:value={stack} aria-label={t('Stack')} placeholder="site" /></label>
    <label
      >{t('Namespace')} <span class="muted">{t("(empty: the stack's name)")}</span>
      <input bind:value={namespace} aria-label={t('Namespace')} placeholder={stack} />
    </label>
    {#if variables.length}
      <fieldset>
        <legend>{t('Variables')}</legend>
        {#each variables as v (v.name)}
          <label
            >{v.name}{#if app?.labels[v.name]}<span class="muted"> · {app.labels[v.name]}</span>{/if}{#if v.required}<span class="muted"> {t('(required)')}</span>{/if}
            <input bind:value={values[v.name]} aria-label={t('Variable {name}', { name: v.name })} placeholder={v.default ?? ''} />
          </label>
        {/each}
      </fieldset>
    {/if}
    <textarea class="editor" use:droppable bind:value={text} spellcheck="false" rows="22" aria-label={t('Compose file')}></textarea>
    <fieldset>
      <legend>{t('Save as a template')}</legend>
      <label>{t('Name')} <input bind:value={saveName} aria-label={t('Template name')} /></label>
      <label>{t('In namespace')} <input bind:value={saveNamespace} aria-label={t('Template namespace')} /></label>
      <button
        onclick={save}
        disabled={!text.trim() || !saveName.trim() || !!cannotWrite(saveNamespace.trim())}
        title={cannotWrite(saveNamespace.trim()) ?? ''}>{t('Save')}</button
      >
      {#if saved}<p class="ok" role="status">{saved}</p>{/if}
    </fieldset>
  </div>

  <div>
    {#if convertError}<p class="error" role="alert">{convertError}</p>{/if}
    {#each warnings as w (w)}<p class="muted">{t('Not honoured: {warning}', { warning: w })}</p>{/each}
    {#if yaml}<pre class="yaml" aria-label={t('Objects')}>{yaml}</pre>{/if}
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
      <h2>
        {#if !dry && r.key.startsWith('Service/')}<a href="/k/{r.key}">{r.key}</a>{:else}{r.key}{/if}:
        <span class="muted"
          >{dry
            ? t('would be {result}', { result: r.result })
            : t('{result}, version {version}', { result: r.result, version: r.version })}</span
        >
      </h2>
      {#if r.diff}<Diff text={r.diff} />{/if}
    {/each}
  </div>
</div>

<style>
  .app-note {
    margin-bottom: 0.85rem;
  }
  .app-note h2 {
    margin: 0 0 0.25rem;
  }
  .split {
    display: grid;
    grid-template-columns: minmax(20rem, 34rem) 1fr;
    gap: 1.5rem;
    align-items: start;
  }
  .side {
    display: flex;
    flex-direction: column;
    gap: 0.6rem;
  }
  .side label {
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
  }
</style>

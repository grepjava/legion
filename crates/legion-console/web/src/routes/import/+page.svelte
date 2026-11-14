<script lang="ts">
  // Compose import (CONSOLE.md phase 5): a `docker-compose.yml` as the
  // objects `legion stack deploy` would apply, shown before applying,
  // with what Legion cannot honour and the file's variables as fields. A
  // template opens here; a compose file is saved as one.
  import { page } from '$app/state';
  import { api, compose, templates, type ApplyResult, type ComposeVariable } from '#lib/api.ts';
  import Diff from '#lib/Diff.svelte';
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

  // A template opened from the catalog.
  $effect(() => {
    const name = page.url.searchParams.get('template');
    if (!name) return;
    const from = page.url.searchParams.get('from') ?? '';
    templates()
      .then((r) => {
        const t = r.templates.find((t) => t.name === name && t.namespace === from);
        if (!t) {
          convertError = `no template ${from ? `${from}/` : ''}${name}`;
          return;
        }
        text = t.compose;
        stack = t.name;
        saveName = t.name;
      })
      .catch((e) => (convertError = e.message));
  });

  // Converted again a moment after each change.
  $effect(() => {
    const body = { compose: text, stack: stack.trim(), namespace: namespace.trim() || undefined, env: { ...values } };
    if (!body.compose.trim() || !body.stack) {
      yaml = '';
      warnings = [];
      convertError = '';
      return;
    }
    const timer = setTimeout(async () => {
      try {
        const r = await compose(body);
        yaml = r.objects.map(toYaml).join('---\n');
        warnings = r.warnings;
        variables = r.variables;
        convertError = '';
        for (const v of r.variables) if (!(v.name in values)) values[v.name] = v.default ?? '';
      } catch (e) {
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
      saved = `Saved as Config ${saveNamespace.trim()}/${saveName.trim()}.`;
    } catch (e) {
      error = (e as Error).message;
    }
  }
</script>

<header class="bar"><h1>Import compose</h1></header>
<p class="muted">
  A compose file as the objects <code>legion stack deploy</code> applies, shown before they are. Files it names are
  not read here: put their content in it. From the <a href="/templates">templates</a>, or pasted.
</p>

<div class="split">
  <div class="side">
    <label>Stack <input bind:value={stack} aria-label="Stack" placeholder="site" /></label>
    <label
      >Namespace <span class="muted">(empty: the stack's name)</span>
      <input bind:value={namespace} aria-label="Namespace" placeholder={stack} />
    </label>
    {#if variables.length}
      <fieldset>
        <legend>Variables</legend>
        {#each variables as v (v.name)}
          <label
            >{v.name}{#if v.required}<span class="muted"> (required)</span>{/if}
            <input bind:value={values[v.name]} aria-label="Variable {v.name}" placeholder={v.default ?? ''} />
          </label>
        {/each}
      </fieldset>
    {/if}
    <textarea class="editor" bind:value={text} spellcheck="false" rows="22" aria-label="Compose file"></textarea>
    <fieldset>
      <legend>Save as a template</legend>
      <label>Name <input bind:value={saveName} aria-label="Template name" /></label>
      <label>In namespace <input bind:value={saveNamespace} aria-label="Template namespace" /></label>
      <button
        onclick={save}
        disabled={!text.trim() || !saveName.trim() || !!cannotWrite(saveNamespace.trim())}
        title={cannotWrite(saveNamespace.trim()) ?? ''}>Save</button
      >
      {#if saved}<p class="ok" role="status">{saved}</p>{/if}
    </fieldset>
  </div>

  <div>
    {#if convertError}<p class="error" role="alert">{convertError}</p>{/if}
    {#each warnings as w (w)}<p class="muted">Not honoured: {w}</p>{/each}
    {#if yaml}<pre class="yaml" aria-label="Objects">{yaml}</pre>{/if}
    <div class="controls">
      <button onclick={() => run(true)} disabled={!yaml || !!why || busy} title={why ?? ''}>Show changes</button>
      <button class="primary" onclick={() => run(false)} disabled={!yaml || !!why || busy} title={why ?? ''}
        >Deploy</button
      >
      {#if busy}<span class="muted">…</span>{/if}
    </div>
    {#if why && yaml}<p class="muted">{why}.</p>{/if}
    {#if error}<p class="error" role="alert">{error}</p>{/if}
    {#each results as r (r.key)}
      <h2>
        {#if !dry && r.key.startsWith('Service/')}<a href="/k/{r.key}">{r.key}</a>{:else}{r.key}{/if}:
        <span class="muted">{dry ? `would be ${r.result}` : `${r.result}, version ${r.version}`}</span>
      </h2>
      {#if r.diff}<Diff text={r.diff} />{/if}
    {/each}
  </div>
</div>

<style>
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

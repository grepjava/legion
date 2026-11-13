<script lang="ts">
  // Apply YAML, as `legion apply` takes it: new objects pasted in, or one
  // object edited from what is stored, applied only over the version it
  // was read at. "Show changes" is `legion diff`: a dry run on the server.
  import { page } from '$app/state';
  import { api, ApiError, type ApplyResult } from '#lib/api.ts';
  import Diff from '#lib/Diff.svelte';
  import { cannotWriteAnywhere } from '#lib/session.svelte.ts';

  const kind = $derived(page.url.searchParams.get('kind') ?? '');
  const namespace = $derived(page.url.searchParams.get('namespace') ?? '');
  const name = $derived(page.url.searchParams.get('name') ?? '');
  const editing = $derived(Boolean(kind && name));
  let yaml = $state('');
  let version: number | undefined = $state();
  let results: ApplyResult[] = $state([]);
  let dry = $state(false);
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

  async function run(dryRun: boolean) {
    busy = true;
    error = '';
    try {
      const r = await api.apply({ yaml, dryRun, expectedVersion: editing ? version : undefined });
      results = r.results;
      dry = dryRun;
      if (!dryRun && editing && results.length === 1) version = results[0].version;
    } catch (e) {
      results = [];
      error =
        e instanceof ApiError && e.status === 409 && editing
          ? `${e.message}. It changed after you opened it; reload the page to edit what is stored now.`
          : (e as Error).message;
    } finally {
      busy = false;
    }
  }

  const verb = (r: ApplyResult) => (dry ? `would be ${r.result}` : `${r.result}, version ${r.version}`);
</script>

<header class="bar">
  <h1>
    {#if editing}
      Edit <a href="/k/{kind}/{namespace || '_'}/{name}">{kind} {namespace ? `${namespace}/` : ''}{name}</a>
    {:else}
      Apply YAML
    {/if}
  </h1>
</header>
<p class="muted">
  {#if editing}
    Applied only over version {version ?? '…'}: if it changes meanwhile, nothing is written.
  {:else}
    One or more objects, as <code>legion apply</code> takes them, separated by <code>---</code>.
  {/if}
</p>

<textarea class="editor" bind:value={yaml} spellcheck="false" rows="26" aria-label="YAML"></textarea>
<div class="controls">
  <button onclick={() => run(true)} disabled={busy || !yaml.trim()}>Show changes</button>
  <button class="primary" onclick={() => run(false)} disabled={!!why || busy || !yaml.trim()} title={why ?? ''}
    >Apply</button
  >
  {#if busy}<span class="muted">…</span>{/if}
</div>

{#if why}<p class="muted">Read only: {why}.</p>{/if}
{#if error}<p class="error">{error}</p>{/if}

{#each results as r (r.key)}
  <h2>{r.key}: <span class="muted">{verb(r)}</span></h2>
  {#if r.diff}<Diff text={r.diff} />{/if}
{/each}

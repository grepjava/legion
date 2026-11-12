<script lang="ts">
  import { page } from '$app/state';
  import { api, follow, type LObject } from '#lib/api.ts';
  import { columns, tone } from '#lib/columns.ts';

  const kind = $derived(page.params.kind ?? '');
  const namespace = $derived(page.params.namespace === '_' ? '' : (page.params.namespace ?? ''));
  const name = $derived(page.params.name ?? '');
  let object: LObject | null = $state(null);
  let error = $state('');
  let gone = $state(false);

  $effect(() => {
    object = null;
    error = '';
    gone = false;
    api
      .get(kind, namespace, name)
      .then((o) => (object = o))
      .catch((e) => (error = e.message));
    // Kept current: the kind's watch, for this one.
    return follow(
      kind,
      namespace,
      (all) => {
        const found = all.find((o) => o.metadata.name === name);
        if (found) object = found;
        gone = !found && object !== null;
      },
      () => {}
    );
  });

  const cols = $derived(columns(kind));
  const json = (v: unknown) => JSON.stringify(v ?? null, null, 2);
</script>

<header class="bar">
  <h1>
    <a href="/k/{kind}">{kind}s</a> / {#if namespace}{namespace} / {/if}{name}
  </h1>
  {#if gone}<span class="state bad">deleted</span>{/if}
</header>

{#if error}
  <p class="error">{error}</p>
{:else if object}
  <dl class="facts">
    {#each cols as c (c.label)}
      {#if c.value(object)}
        <dt>{c.label}</dt>
        <dd>
          {#if c.state}<span class="state {tone(c.value(object))}">{c.value(object)}</span>
          {:else}{c.value(object)}{/if}
        </dd>
      {/if}
    {/each}
    <dt>Version</dt>
    <dd>{object.metadata.version} (generation {object.metadata.generation})</dd>
    <dt>UID</dt>
    <dd><code>{object.metadata.uid}</code></dd>
    {#if object.metadata.labels && Object.keys(object.metadata.labels).length}
      <dt>Labels</dt>
      <dd>
        {#each Object.entries(object.metadata.labels) as [k, v] (k)}
          <span class="tag">{k}={v}</span>
        {/each}
      </dd>
    {/if}
  </dl>

  <h2>Spec</h2>
  <pre>{json(object.spec)}</pre>
  {#if object.status !== null && object.status !== undefined}
    <h2>Status</h2>
    <pre>{json(object.status)}</pre>
  {/if}
{:else}
  <p class="muted">…</p>
{/if}

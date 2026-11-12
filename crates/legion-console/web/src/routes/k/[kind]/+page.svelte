<script lang="ts">
  import { page } from '$app/state';
  import { follow, type LObject } from '#lib/api.ts';
  import { columns, tone } from '#lib/columns.ts';
  import { session } from '#lib/session.svelte.ts';

  const kind = $derived(page.params.kind ?? '');
  const scoped = $derived(session.kinds.find((k) => k.kind === kind)?.clusterScoped ?? false);
  let objects: LObject[] = $state([]);
  let error = $state('');
  let filter = $state('');

  $effect(() => {
    objects = [];
    error = '';
    return follow(
      kind,
      scoped ? '' : session.namespace,
      (o) => {
        objects = o;
        error = '';
      },
      (e) => (error = e.message)
    );
  });

  const cols = $derived(columns(kind));
  const shown = $derived(
    filter
      ? objects.filter((o) => `${o.metadata.namespace ?? ''}/${o.metadata.name}`.includes(filter))
      : objects
  );
  const href = (o: LObject) => `/k/${kind}/${o.metadata.namespace || '_'}/${o.metadata.name}`;
</script>

<header class="bar">
  <h1>{kind}s</h1>
  <input type="search" placeholder="Filter by name" bind:value={filter} />
  <span class="muted">{shown.length} of {objects.length}, live</span>
</header>

{#if error}
  <p class="error">{error}</p>
{/if}

{#if shown.length}
  <table>
    <thead>
      <tr>
        <th>Name</th>
        {#if !scoped}<th>Namespace</th>{/if}
        {#each cols as c (c.label)}<th>{c.label}</th>{/each}
      </tr>
    </thead>
    <tbody>
      {#each shown as o (o.metadata.uid)}
        <tr>
          <td><a href={href(o)}>{o.metadata.name}</a></td>
          {#if !scoped}<td>{o.metadata.namespace}</td>{/if}
          {#each cols as c (c.label)}
            <td>
              {#if c.state}
                <span class="state {tone(c.value(o))}">{c.value(o)}</span>
              {:else}
                {c.value(o)}
              {/if}
            </td>
          {/each}
        </tr>
      {/each}
    </tbody>
  </table>
{:else if !error}
  <p class="muted">None.</p>
{/if}

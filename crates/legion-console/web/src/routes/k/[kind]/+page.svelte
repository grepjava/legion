<script lang="ts">
  import { page } from '$app/state';
  import { follow, type LObject } from '#lib/api.ts';
  import { columns, tone } from '#lib/columns.ts';
  import { matches, parse } from '#lib/filter.ts';
  import { plural } from '#lib/kinds.ts';
  import { session } from '#lib/session.svelte.ts';

  const kind = $derived(page.params.kind ?? '');
  const scoped = $derived(session.kinds.find((k) => k.kind === kind)?.clusterScoped ?? false);
  let objects: LObject[] = $state([]);
  let error = $state('');
  let filter = $state('');
  // A link may bring a filter (a label on an object's page); another kind
  // starts unfiltered.
  $effect(() => {
    void kind;
    filter = page.url.searchParams.get('filter') ?? '';
  });

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
  const terms = $derived(parse(filter));
  const shown = $derived(terms.length ? objects.filter((o) => matches(o, terms)) : objects);
  const href = (o: LObject) => `/k/${kind}/${o.metadata.namespace || '_'}/${o.metadata.name}`;
</script>

<header class="bar">
  <h1>{plural(kind)}</h1>
  <input type="search" aria-label="Filter" placeholder="Filter: name, app=web, !canary"
    title="Words match names; key=value, key!=value and !key (no such label) match labels" bind:value={filter} />
  <span class="muted">{shown.length} of {objects.length}, live</span>
</header>

{#if error}
  <p class="error" role="alert">{error}</p>
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

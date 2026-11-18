<script lang="ts">
  import { page } from '$app/state';
  import { follow, type LObject } from '#lib/api.ts';
  import { columns, tone } from '#lib/columns.ts';
  import { matches, parse } from '#lib/filter.ts';
  import { plural } from '#lib/kinds.ts';
  import Ico from '#lib/Ico.svelte';
  import { kindIcon } from '#lib/icons.ts';
  import { t } from '#lib/i18n.svelte.ts';
  import { session } from '#lib/session.svelte.ts';

  const kind = $derived(page.params.kind ?? '');
  const scoped = $derived(session.kinds.find((k) => k.kind === kind)?.clusterScoped ?? false);
  let objects: LObject[] = $state([]);
  let error = $state('');
  let filter = $state('');
  // Whether the watch has said what there is yet: until then, nothing
  // listed means nothing known, not none.
  let loaded = $state(false);
  // The column sorted on (-1 the name, -2 the namespace), and which way.
  let sortBy = $state(-1);
  let sortDir: 'ascending' | 'descending' = $state('ascending');
  function sortOn(col: number) {
    if (sortBy === col) sortDir = sortDir === 'ascending' ? 'descending' : 'ascending';
    else {
      sortBy = col;
      sortDir = 'ascending';
    }
  }
  // A link may bring a filter (a label on an object's page); another kind
  // starts unfiltered.
  $effect(() => {
    void kind;
    filter = page.url.searchParams.get('filter') ?? '';
  });

  $effect(() => {
    objects = [];
    error = '';
    loaded = false;
    return follow(
      kind,
      scoped ? '' : session.namespace,
      (o) => {
        objects = o;
        error = '';
        loaded = true;
      },
      (e) => {
        error = e.message;
        loaded = true;
      }
    );
  });

  const cols = $derived(columns(kind));
  const terms = $derived(parse(filter));
  // Kinds differ in their columns: a new kind is sorted by name.
  $effect(() => {
    void kind;
    sortBy = -1;
    sortDir = 'ascending';
  });
  const collator = new Intl.Collator(undefined, { numeric: true, sensitivity: 'base' });
  const cell = (o: LObject, col: number) =>
    col === -1 ? o.metadata.name : col === -2 ? (o.metadata.namespace ?? '') : (cols[col]?.value(o) ?? '');
  const shown = $derived.by(() => {
    const found = terms.length ? objects.filter((o) => matches(o, terms)) : objects;
    const sign = sortDir === 'ascending' ? 1 : -1;
    return [...found].sort(
      (a, b) =>
        sign * collator.compare(String(cell(a, sortBy)), String(cell(b, sortBy))) ||
        collator.compare(a.metadata.name, b.metadata.name)
    );
  });
  const sorted = (col: number) => (sortBy === col ? sortDir : undefined);
  const href = (o: LObject) => `/k/${kind}/${o.metadata.namespace || '_'}/${o.metadata.name}`;
</script>

<header class="bar">
  <h1 class="with-icon"><Ico icon={kindIcon(kind)} size={22} />{plural(kind)}</h1>
  <input type="search" aria-label={t('Filter')} placeholder={t('Filter: name, app=web, !canary')}
    title={t('Words match names; key=value, key!=value and !key (no such label) match labels')} bind:value={filter} />
  <span class="muted">{t('{shown} of {total}, live', { shown: shown.length, total: objects.length })}</span>
</header>

{#if error}
  <p class="error" role="alert">{error}</p>
{/if}

{#if shown.length}
  <table>
    <thead>
      <tr>
        <th scope="col" aria-sort={sorted(-1)}>
          <button type="button" class="sort-btn" data-look onclick={() => sortOn(-1)}>{t('Name')}</button>
        </th>
        {#if !scoped}
          <th scope="col" aria-sort={sorted(-2)}>
            <button type="button" class="sort-btn" data-look onclick={() => sortOn(-2)}>{t('Namespace')}</button>
          </th>
        {/if}
        {#each cols as c, i (c.label)}
          <th scope="col" aria-sort={sorted(i)}>
            <button type="button" class="sort-btn" data-look onclick={() => sortOn(i)}>{c.label}</button>
          </th>
        {/each}
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
              {:else if c.href?.(o)}
                <a href={c.href(o)}>{c.value(o)}</a>
              {:else}
                {c.value(o)}
              {/if}
            </td>
          {/each}
        </tr>
      {/each}
    </tbody>
  </table>
{:else if !loaded}
  <!-- Rows to come, in the table's shape, until the watch has answered. -->
  <div class="skeleton" role="status" aria-label={t('Loading…')}>
    {#each [0, 1, 2, 3, 4] as i (i)}<div class="skeleton-row" style:opacity={1 - i * 0.16}></div>{/each}
  </div>
{:else if !error}
  {#if terms.length && objects.length}
    <p class="empty">{t('None match the filter.')} <button type="button" class="link" onclick={() => (filter = '')}>{t('Clear the filter')}</button></p>
  {:else}
    <p class="empty">
      {t('No {kinds} here yet.', { kinds: plural(kind) })}
      {#if ['Service', 'Job', 'CronJob', 'Volume', 'Secret', 'Config', 'Route', 'NetworkPolicy'].includes(kind)}
        <a href={kind === 'Service' ? '/deploy' : '/apply'}
          >{kind === 'Service' ? t('Deploy one') : t('Write one')}</a
        >
      {/if}
    </p>
  {/if}
{/if}

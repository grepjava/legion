<script lang="ts" generics="T">
  // Over a list: a search through it, and how many it shows of how many.
  // Anything else acting on the list (a button, a choice) goes beside them.
  import type { Snippet } from 'svelte';
  import type { Table } from './table.svelte.ts';
  import { t } from './i18n.svelte.ts';

  let {
    table,
    label,
    placeholder = '',
    children
  }: {
    table: Table<T>;
    /** The search's name, as "Search alerts". */
    label: string;
    placeholder?: string;
    children?: Snippet;
  } = $props();
</script>

<div class="table-bar">
  <input
    type="search"
    value={table.query}
    oninput={(e) => table.search(e.currentTarget.value)}
    aria-label={label}
    placeholder={placeholder || t('Search…')}
  />
  {@render children?.()}
  <span class="table-count muted" role="status">
    {table.query
      ? t('{n} of {total}', { n: table.found.length, total: table.total })
      : t('{n} in all', { n: table.total })}
  </span>
</div>

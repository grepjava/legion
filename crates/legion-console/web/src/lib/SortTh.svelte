<script lang="ts" generics="T">
  // A column's header that sorts its table on it (lib/table.svelte.ts),
  // saying which way with aria-sort and the arrow the kit draws.
  import type { Snippet } from 'svelte';
  import type { Table } from './table.svelte.ts';

  let { table, col, children }: { table: Table<T>; col: string; children: Snippet } = $props();
</script>

<th scope="col" aria-sort={table.sorting(col)}>
  <button type="button" class="sort-btn" data-look onclick={() => table.sort(col)}>{@render children()}</button>
</th>

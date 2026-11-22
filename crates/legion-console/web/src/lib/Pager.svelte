<script lang="ts" generics="T">
  // Under a list longer than a page: which rows are shown, and the way to
  // the others. Nothing while one page holds them all. Its buttons only
  // look (data-look): a reader pages as anyone does.
  import type { Table } from './table.svelte.ts';
  import { t } from './i18n.svelte.ts';

  let { table, label }: { table: Table<T>; /** What the list is, as "Alerts". */ label: string } = $props();
  const from = $derived(table.at * table.size + 1);
  const to = $derived(Math.min(table.sorted.length, (table.at + 1) * table.size));
</script>

{#if table.pages > 1}
  <nav class="pager" aria-label={t('Pages of {what}', { what: label })}>
    <button type="button" data-look disabled={table.at === 0} onclick={() => (table.page = 0)}>{t('First')}</button>
    <button type="button" data-look disabled={table.at === 0} onclick={() => (table.page = table.at - 1)}
      >← {t('Previous')}</button
    >
    <span class="muted" aria-live="polite"
      >{t('{from}–{to} of {n}', { from, to, n: table.sorted.length })}</span
    >
    <button type="button" data-look disabled={table.at >= table.pages - 1} onclick={() => (table.page = table.at + 1)}
      >{t('Next')} →</button
    >
    <button type="button" data-look disabled={table.at >= table.pages - 1} onclick={() => (table.page = table.pages - 1)}
      >{t('Last')}</button
    >
  </nav>
{/if}

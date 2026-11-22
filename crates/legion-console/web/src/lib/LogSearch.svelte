<script lang="ts">
  // A Service's or Job's logs searched across its tasks (`legion
  // logs-search`): the lines of a time window holding some text, oldest
  // first, each with its time and task.
  import { logSearch, type FoundLine } from '#lib/api.ts';
  import { t } from '#lib/i18n.svelte.ts';
  import { Table } from '#lib/table.svelte.ts';
  import SortTh from '#lib/SortTh.svelte';
  import TableBar from '#lib/TableBar.svelte';
  import Pager from '#lib/Pager.svelte';

  let { namespace, service }: { namespace: string; service: string } = $props();

  const windows = $derived([
    { label: t('last 15 min'), ms: 15 * 60_000 },
    { label: t('last hour'), ms: 3_600_000 },
    { label: t('last 6 hours'), ms: 6 * 3_600_000 },
    { label: t('last day'), ms: 24 * 3_600_000 }
  ]);
  let text = $state('');
  let span = $state(3_600_000);
  let lines: FoundLine[] = $state([]);
  let truncated = $state(false);
  let unread: string[] = $state([]);
  let error = $state('');
  let busy = $state(false);
  let searched = $state(false);

  async function run(e: Event) {
    e.preventDefault();
    busy = true;
    error = '';
    try {
      const r = await logSearch({ namespace, service, since_ms: Date.now() - span, text });
      lines = r.lines;
      // A new search opens on its first page.
      table.page = 0;
      truncated = r.truncated;
      unread = r.unread;
      searched = true;
    } catch (err) {
      error = (err as Error).message;
    } finally {
      busy = false;
    }
  }
  const time = (ms: number) =>
    new Date(ms).toLocaleString([], {
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    });
  // The lines found, oldest first as they came (a stable sort keeps the
  // order of those of the same millisecond); the bar above them narrows
  // what was found without asking the tasks again.
  const table = new Table(() => lines, {
    sorts: {
      time: (l) => l.at_ms,
      task: (l) => l.task,
      line: (l) => l.text
    },
    sortBy: 'time',
    // Found lines read as a log: a long page, so lines from every task
    // searched are on the first.
    size: 200,
    text: (l) => `${time(l.at_ms)} ${l.task} ${l.text}`
  });
</script>

<form class="controls" onsubmit={run}>
  <input placeholder={t('text (every line when empty)')} bind:value={text} aria-label={t('Search text')} />
  <select bind:value={span} aria-label={t('Time window')}>
    {#each windows as w (w.ms)}<option value={w.ms}>{w.label}</option>{/each}
  </select>
  <button class="primary" disabled={busy}>{t('Search')}</button>
  {#if busy}<span class="muted">{t('searching every task…')}</span>{/if}
</form>
{#if error}<p class="error" role="alert">{error}</p>{/if}
{#each unread as u (u)}<p class="muted">{t('Not searched: {reason}', { reason: u })}</p>{/each}
{#if truncated}<p class="muted">{t('More lines matched than shown: the oldest were left out.')}</p>{/if}
{#if searched && !lines.length && !error}<p class="empty">{t('No line matched.')}</p>{/if}
{#if lines.length}
  <TableBar {table} label={t('Narrow the lines found')} placeholder={t('Narrow these lines…')} />
  <table class="found">
    <thead>
      <tr>
        <SortTh {table} col="time">{t('Time')}</SortTh>
        <SortTh {table} col="task">{t('Task')}</SortTh>
        <SortTh {table} col="line">{t('Line')}</SortTh>
      </tr>
    </thead>
    <tbody>
      {#each table.rows as l, i (i)}
        <tr>
          <td class="nowrap">{time(l.at_ms)}</td>
          <td class="nowrap"><a href="/k/Task/{namespace}/{l.task}">{l.task}</a></td>
          <td><code class:stderr={l.stderr}>{l.text}</code></td>
        </tr>
      {:else}
        <tr><td colspan="3" class="muted">{t('Nothing matches.')}</td></tr>
      {/each}
    </tbody>
  </table>
  <Pager {table} label={t('Lines found')} />
{/if}

<style>
  .nowrap {
    white-space: nowrap;
  }
  .found code {
    white-space: pre-wrap;
    word-break: break-word;
  }
  .stderr {
    color: var(--bad);
  }
</style>

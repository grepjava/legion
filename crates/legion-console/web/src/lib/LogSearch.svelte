<script lang="ts">
  // A Service's or Job's logs searched across its tasks (`legion
  // logs-search`): the lines of a time window holding some text, oldest
  // first, each with its time and task.
  import { logSearch, type FoundLine } from '#lib/api.ts';
  import { t } from '#lib/i18n.svelte.ts';

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
{#if searched && !lines.length && !error}<p class="muted">{t('No line matched.')}</p>{/if}
{#if lines.length}
  <table class="found">
    <thead><tr><th>{t('Time')}</th><th>{t('Task')}</th><th>{t('Line')}</th></tr></thead>
    <tbody>
      {#each lines as l, i (i)}
        <tr>
          <td class="nowrap">{time(l.at_ms)}</td>
          <td class="nowrap"><a href="/k/Task/{namespace}/{l.task}">{l.task}</a></td>
          <td><code class:stderr={l.stderr}>{l.text}</code></td>
        </tr>
      {/each}
    </tbody>
  </table>
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

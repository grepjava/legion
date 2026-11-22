<script lang="ts">
  // What happened to an object lately, newest first, followed as it
  // happens (CONSOLE.md phase 2): a Service's or Job's with its tasks'. The
  // manager answering works them out from what it sees change and keeps
  // an hour of them, so after it restarts there are fewer.
  import { followEvents, type ObjectEvent } from './api.ts';
  import { t } from './i18n.svelte.ts';
  import { Table } from './table.svelte.ts';
  import SortTh from './SortTh.svelte';
  import TableBar from './TableBar.svelte';
  import Pager from './Pager.svelte';

  let { kind, namespace, name }: { kind: string; namespace: string; name: string } = $props();
  let events: ObjectEvent[] = $state([]);
  let error = $state('');

  $effect(() => {
    events = [];
    return followEvents(
      namespace,
      kind,
      name,
      (all) => (events = all),
      (why) => (error = why)
    );
  });

  const time = (ms: number) => new Date(ms).toLocaleTimeString();
  // Newest first; "Object" sorts and is searched on what the column shows,
  // the object itself first as "this". The rows go in newest first, so
  // events of the same millisecond stay so too.
  const object = (e: ObjectEvent) => (e.kind === kind && e.name === name ? '' : `${e.kind}/${e.name}`);
  const table = new Table(() => [...events].reverse(), {
    sorts: {
      time: (e) => e.at_ms,
      object,
      reason: (e) => e.reason,
      message: (e) => e.message
    },
    sortBy: 'time',
    dir: 'descending',
    text: (e) => `${time(e.at_ms)} ${object(e) || t('this')} ${e.reason} ${e.message}`
  });
</script>

{#if error}<p class="error" role="alert">{error}</p>{/if}
{#if events.length}
  <TableBar {table} label={t('Search events')} />
  <table aria-label={t('Events of {name}', { name })}>
    <thead>
      <tr>
        <SortTh {table} col="time">{t('Time')}</SortTh>
        <SortTh {table} col="object">{t('Object')}</SortTh>
        <SortTh {table} col="reason">{t('What happened')}</SortTh>
        <SortTh {table} col="message">{t('Why')}</SortTh>
      </tr>
    </thead>
    <tbody>
      {#each table.rows as e, i (i)}
        <tr>
          <td title={new Date(e.at_ms).toISOString()}>{time(e.at_ms)}</td>
          <td
            >{#if e.kind === kind && e.name === name}<span class="muted">{t('this')}</span
              >{:else}<a href="/k/{e.kind}/{e.namespace || '_'}/{e.name}">{e.kind}/{e.name}</a>{/if}</td
          >
          <td>{e.reason}</td>
          <td>{e.message}</td>
        </tr>
      {:else}
        <tr><td colspan="4" class="muted">{t('Nothing matches.')}</td></tr>
      {/each}
    </tbody>
  </table>
  <Pager {table} label={t('Events')} />
{:else if !error}
  <p class="empty">{t('Nothing has happened to it lately that this manager saw.')}</p>
{/if}

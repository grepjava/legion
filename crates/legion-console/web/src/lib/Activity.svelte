<script lang="ts">
  // The workbench's Activity tab (CONSOLE.md phase 10): the cluster's long
  // operations as the leader follows them, those under way first, each
  // with its stage (a move's in turn), what it has sent, how long it ran,
  // who asked (for the cluster admin) and why it waits or how it ended;
  // opened, its stages with when and all it said, with Copy. Under them,
  // what was done in this console and how each ended.
  import { activity, MOVE_STAGES, outputText, sent, took } from './activity.svelte.ts';
  import type { Operation } from './api.ts';
  import { t } from './i18n.svelte.ts';
  import Ico from './Ico.svelte';
  import { Check, ChevronDown, ChevronRight, Copy } from '@lucide/svelte';

  let show: 'all' | 'running' = $state('all');
  let open: Record<string, boolean> = $state({});
  let copied = $state('');
  let now = $state(Date.now());

  // Durations of what runs move on.
  $effect(() => {
    const tick = setInterval(() => (now = Date.now()), 1000);
    return () => clearInterval(tick);
  });

  const shown = $derived(
    [...activity.ops]
      .filter((o) => show === 'all' || o.state === 'running')
      .sort((a, b) => {
        const run = Number(b.state === 'running') - Number(a.state === 'running');
        return run || b.beganMs - a.beganMs;
      })
  );
  const runningCount = $derived(activity.ops.filter((o) => o.state === 'running').length);
  const done = $derived([...activity.done].reverse());

  const tone = (o: Operation) =>
    ({ running: 'wait', succeeded: 'good', failed: 'bad', cancelled: '' })[o.state];
  const stateWord = (o: Operation) =>
    ({
      running: t('running'),
      succeeded: t('succeeded'),
      failed: t('failed'),
      cancelled: t('cancelled')
    })[o.state];
  const href = (o: Operation) =>
    o.target.kind && o.target.name
      ? `/k/${encodeURIComponent(o.target.kind)}/${encodeURIComponent(o.target.namespace || '_')}/${encodeURIComponent(o.target.name)}`
      : '';
  const time = (ms: number) => new Date(ms).toLocaleTimeString();
  const reached = (o: Operation, stage: string) => (o.stages ?? []).some((s) => s.stage === stage);

  async function copy(o: Operation) {
    await navigator.clipboard.writeText(outputText(o));
    copied = o.id;
    setTimeout(() => (copied = ''), 1500);
  }
</script>

<div class="activity">
  <div class="table-toolbar">
    <div class="pill-row" role="group" aria-label={t('Show')}>
      <button
        type="button"
        class="pill"
        class:active={show === 'all'}
        aria-pressed={show === 'all'}
        onclick={() => (show = 'all')}>{t('All')}</button
      >
      <button
        type="button"
        class="pill"
        class:active={show === 'running'}
        aria-pressed={show === 'running'}
        onclick={() => (show = 'running')}
        >{t('Running')}<span class="count">{runningCount}</span></button
      >
    </div>
    {#if activity.error}<span class="error" role="alert">{activity.error}</span>{/if}
  </div>

  {#if shown.length}
    <table aria-label={t('Long operations')}>
      <thead>
        <tr>
          <th><span class="sr-only">{t('Open')}</span></th>
          <th>{t('Began')}</th>
          <th>{t('What')}</th>
          <th>{t('Stage')}</th>
          <th>{t('Sent')}</th>
          <th>{t('State')}</th>
          <th>{t('By')}</th>
          <th>{t('Why')}</th>
        </tr>
      </thead>
      <tbody>
        {#each shown as o (o.id)}
          <tr data-op={o.id}>
            <td>
              <button
                type="button"
                class="link"
                aria-expanded={!!open[o.id]}
                aria-label={t('What {what} said', { what: o.what })}
                onclick={() => (open[o.id] = !open[o.id])}
                ><Ico icon={open[o.id] ? ChevronDown : ChevronRight} size={14} /></button
              >
            </td>
            <td title={new Date(o.beganMs).toISOString()}>{time(o.beganMs)}</td>
            <td>
              {#if href(o)}<a href={href(o)}>{o.what}</a>{:else}{o.what}{/if}
              {#if o.partOf}<span class="muted">{t('(part of a drain)')}</span>{/if}
            </td>
            <td>
              {#if o.kind === 'move'}
                <ol class="stages" aria-label={t('Stages')}>
                  {#each MOVE_STAGES as s (s)}
                    <li
                      class:now={o.stage === s && o.state === 'running'}
                      class:past={reached(o, s) && o.stage !== s}
                      class:last={o.stage === s && o.state !== 'running'}
                      aria-current={o.stage === s ? 'step' : undefined}
                    >
                      {s}
                    </li>
                  {/each}
                </ol>
              {:else}
                {o.stage}
              {/if}
            </td>
            <td class="num">{sent(o.bytes)}</td>
            <td>
              <span class="state {tone(o)}"
                >{stateWord(o)} {took((o.endedMs ?? now) - o.beganMs)}</span
              >
            </td>
            <td>{o.by ?? ''}</td>
            <td class="why">{o.message ?? ''}</td>
          </tr>
          {#if open[o.id]}
            <tr class="detail-row">
              <td></td>
              <td colspan="7">
                <div class="op-head">
                  <span class="muted">
                    {(o.stages ?? [])
                      .map((s) => `${s.stage} +${took(s.atMs - o.beganMs)}`)
                      .join(' › ')}
                  </span>
                  <button type="button" class="sm" onclick={() => copy(o)}>
                    <Ico icon={copied === o.id ? Check : Copy} size={14} />
                    {copied === o.id ? t('Copied') : t('Copy')}
                  </button>
                </div>
                <pre class="log op-output">{#each o.output ?? [] as l, i (i)}{time(l.atMs)}  {l.text}
{/each}</pre>
              </td>
            </tr>
          {/if}
        {/each}
      </tbody>
    </table>
  {:else}
    <p class="empty">
      {show === 'running'
        ? t('Nothing runs now. Drains, Volume moves, restores and replicas’ first copies show here as they go.')
        : t('No long operation in the last hour. Drains, Volume moves, restores and replicas’ first copies show here as they go.')}
    </p>
  {/if}

  <h3 class="done-head">{t('Done in this console')}</h3>
  {#if done.length}
    <ul class="done">
      {#each done as d (d.id)}
        <li>
          <span class="dot {d.error ? 'bad' : 'good'}" aria-hidden="true"></span>
          <span class="muted">{time(d.atMs)}</span>
          <span>{d.text}</span>
          <span class="sr-only">{d.error ? t('failed') : t('done')}</span>
        </li>
      {/each}
    </ul>
  {:else}
    <p class="empty">{t('Nothing done here yet: what you do in the console is listed here with how it ended.')}</p>
  {/if}
</div>

<style>
  .activity {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }
  .table-toolbar {
    display: flex;
    align-items: center;
    gap: 0.75rem;
  }
  .table-toolbar .pill-row {
    margin: 0;
  }
  .stages {
    display: flex;
    flex-wrap: wrap;
    gap: 0.15rem;
    list-style: none;
    margin: 0;
    padding: 0;
    font-size: 0.74rem;
  }
  .stages li {
    color: var(--muted);
    padding: 0 0.3rem;
    border-radius: 4px;
    border: 1px solid transparent;
  }
  .stages li.past {
    color: var(--text);
  }
  .stages li.now {
    color: var(--wait);
    border-color: currentColor;
    font-weight: 600;
  }
  .stages li.last {
    color: var(--text);
    border-color: var(--line);
    font-weight: 600;
  }
  .why {
    max-width: 28rem;
    overflow-wrap: anywhere;
  }
  .num {
    font-variant-numeric: tabular-nums;
    white-space: nowrap;
  }
  .op-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.5rem;
    margin-bottom: 0.35rem;
  }
  .op-output {
    max-height: 16rem;
    margin: 0;
  }
  .done-head {
    margin: 0.75rem 0 0.1rem;
    font-size: 0.85rem;
  }
  .done {
    list-style: none;
    margin: 0;
    padding: 0;
    display: flex;
    flex-direction: column;
    gap: 0.2rem;
    font-size: 0.85rem;
  }
  .done li {
    display: flex;
    gap: 0.5rem;
    align-items: center;
  }
</style>

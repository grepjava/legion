<script lang="ts">
  // A Volume's replica (phase 7): where its copy is kept and what it amounts
  // to, how old the last copy is against how often it is meant to be taken,
  // whether the Volume would fail over on its own now and if not why, and
  // its last copies. All of it from the Volume's status as the replicator
  // writes it; the ages tick here.
  import { api, bytes, follow, type LObject } from './api.ts';
  import { ask } from './confirm.svelte.ts';
  import { busyWhile, toast } from './toast.svelte.ts';
  import { cannotWrite } from './session.svelte.ts';
  import { t } from './i18n.svelte.ts';
  import Ico from './Ico.svelte';
  import { tone } from './columns.ts';
  import { CircleCheck, CircleX, Copy, ShieldCheck, ShieldExclamationPoint } from '@lucide/svelte';

  let { volume }: { volume: LObject } = $props();

  interface Copy_ {
    atMs: number;
    tookMs: number;
    bytes?: number;
    whole?: boolean;
    error?: string;
  }
  const want = $derived(volume.spec?.replica as { node?: string; everySeconds?: number; failover?: string } | undefined);
  const r = $derived(
    volume.status?.replica as
      | {
          node: string;
          snapshot?: string;
          from?: string;
          takenMs?: number;
          message?: string;
          state?: string;
          copyingMs?: number;
          copies?: Copy_[];
          blocked?: string[];
        }
      | undefined
  );
  const every = $derived((want?.everySeconds ?? 300) * 1000);
  const automatic = $derived(want?.failover === 'automatic');
  const shownState = $derived(r?.state ?? 'pending');

  let now = $state(Date.now());
  $effect(() => {
    const id = setInterval(() => (now = Date.now()), 1000);
    return () => clearInterval(id);
  });

  /** A span for people, in its two largest units, as the CLI says it. */
  function span(ms: number): string {
    const s = Math.max(0, Math.round(ms / 1000));
    const [d, h, m, sec] = [Math.floor(s / 86400), Math.floor(s / 3600) % 24, Math.floor(s / 60) % 60, s % 60];
    if (d) return h ? `${d}d ${h}h` : `${d}d`;
    if (h) return m ? `${h}h ${m}m` : `${h}h`;
    if (m) return sec ? `${m}m ${sec}s` : `${m}m`;
    return `${sec}s`;
  }
  const age = $derived(r?.takenMs ? now - r.takenMs : undefined);
  // How far into what it may lag before it is stale (three intervals).
  const lagPct = $derived(age === undefined ? 0 : Math.min(100, Math.round((age / (every * 3)) * 100)));
  const last = $derived(r?.copies?.findLast((c) => !c.error));
  const copies = $derived([...(r?.copies ?? [])].reverse());

  // The node holding the data, followed: promoting by hand is for one that
  // is down.
  let holderState = $state('');
  $effect(() => {
    const holder = volume.status?.node as string | undefined;
    if (!holder) return;
    return follow(
      'Node',
      '',
      (nodes) => (holderState = String(nodes.find((n) => n.metadata.name === holder)?.status?.state ?? '')),
      () => (holderState = '')
    );
  });
  const ns = $derived(volume.metadata.namespace ?? '');
  const readOnly = $derived(cannotWrite(ns));
  const promoteWhy = $derived(
    readOnly ??
      (!r?.snapshot || r.from !== volume.status?.node
        ? t("No copy of its node's data yet")
        : holderState !== 'down'
          ? t('Its node, {node}, is {state}: only a Volume whose node is down is handed over', {
              node: volume.status?.node ?? '',
              state: holderState || t('not known')
            })
          : null)
  );
  let promoting = $state(false);
  async function promote() {
    if (!r) return;
    const taken = r.takenMs ? t('{time} ago', { time: span(Date.now() - r.takenMs) }) : '';
    const yes = await ask(
      t(
        'Hand {name} to its replica on {node}? It carries on from the copy taken {taken}; what was written on {holder} since is lost, and what still runs there writes to a copy that is dropped once the node is back.',
        { name: volume.metadata.name, node: r.node, taken, holder: volume.status?.node ?? '' }
      ),
      { action: t('Promote'), danger: true, typed: volume.metadata.name }
    );
    if (!yes) return;
    promoting = true;
    try {
      const out = await busyWhile(() => api.volumePromote(ns, volume.metadata.name));
      toast(t('{name} carries on on {node}', { name: volume.metadata.name, node: out.node }));
    } catch (e) {
      toast((e as Error).message, true);
    } finally {
      promoting = false;
    }
  }

  let copied = $state(false);
  async function copyMessage() {
    if (!r?.message) return;
    await navigator.clipboard.writeText(r.message);
    copied = true;
    setTimeout(() => (copied = false), 1500);
  }
</script>

{#if !want && !r}
  <p class="empty">{t('No replica: add replica to the Volume, or to the claim of the service using it, to keep a copy on another node.')}</p>
{:else}
  <section class="replica-head">
    <span class="state {tone(shownState)}">{shownState}</span>
    {#if r?.copyingMs}
      <span class="state wait">{t('copying for {time}', { time: span(now - r.copyingMs) })}</span>
    {/if}
    <span>
      {#if r?.node}
        {t('kept on {node}', { node: r.node })}
      {:else}
        {t('no node keeps it yet')}
      {/if}
      · {t('every {time}', { time: span(every) })}
      · {automatic ? t('fails over on its own') : t('fails over when its node is removed')}
    </span>
  </section>

  {#if r?.message}
    <div class="replica-error" role="alert">
      <Ico icon={CircleX} class="bad" />
      <span>{r.message}</span>
      <button type="button" class="sm" onclick={copyMessage} aria-label={t('Copy the message')}
        ><Ico icon={Copy} size={14} />{copied ? t('Copied') : t('Copy')}</button
      >
    </div>
  {/if}

  <div class="tiles">
    <div class="tile">
      <span class="tile-label">{t('Last copy')}</span>
      <strong class="tile-value">{age === undefined ? t('none yet') : t('{time} ago', { time: span(age) })}</strong>
      <span class="tile-note">
        {#if age === undefined}
          {t('The first copy is whole; later ones send what changed.')}
        {:else}
          {t('What the Volume could lose with its node. Stale past {time}.', { time: span(every * 3) })}
        {/if}
      </span>
      <div class="meter" role="img" aria-label={t('{pct}% of the way to stale', { pct: lagPct })}>
        <div class="meter-fill {lagPct >= 100 ? 'bad' : lagPct >= 34 ? 'wait' : 'good'}" style:width="{lagPct}%"></div>
      </div>
    </div>
    <div class="tile">
      <span class="tile-label">{t('Last sent')}</span>
      <strong class="tile-value">{last ? bytes(last.bytes ?? 0) : '—'}</strong>
      <span class="tile-note"
        >{last
          ? last.whole
            ? t('a whole copy, in {time}', { time: span(last.tookMs) })
            : t('what changed, in {time}', { time: span(last.tookMs) })
          : t('Nothing sent yet.')}</span
      >
    </div>
    <div class="tile">
      <span class="tile-label">{t('Failover')}</span>
      {#if !automatic}
        <strong class="tile-value">{t('Manual')}</strong>
        <span class="tile-note"
          >{t('Removing its node (legion delete node) hands it to the replica. failover: automatic does it once the node is down and its pods have stopped.')}</span
        >
      {:else if !r?.blocked?.length}
        <strong class="tile-value good"><Ico icon={ShieldCheck} size={20} class="good" />{t('Ready')}</strong>
        <span class="tile-note">{t('If its node went down now, it would carry on from this copy once the pods there have stopped.')}</span>
      {:else}
        <strong class="tile-value wait"><Ico icon={ShieldExclamationPoint} size={20} class="wait" />{t('Blocked')}</strong>
        <ul class="tile-list">
          {#each r.blocked as why, i (i)}<li>{why}</li>{/each}
        </ul>
      {/if}
    </div>
  </div>

  {#if r?.node}
    <div class="promote">
      <button type="button" class="danger" disabled={!!promoteWhy || promoting} title={promoteWhy ?? ''} onclick={promote}
        >{t('Promote the replica')}</button
      >
      <span class="muted">{promoteWhy ?? t('Its node is down: the Volume can carry on from this copy now.')}</span>
    </div>
  {/if}

  <h3>{t('Copies')}</h3>
  {#if copies.length}
    <table>
      <thead>
        <tr>
          <th scope="col">{t('When')}</th>
          <th scope="col">{t('Took')}</th>
          <th scope="col">{t('Sent')}</th>
          <th scope="col">{t('Kind')}</th>
          <th scope="col">{t('Result')}</th>
        </tr>
      </thead>
      <tbody>
        {#each copies as c (c.atMs)}
          <tr>
            <td title={new Date(c.atMs).toLocaleString()}>{t('{time} ago', { time: span(now - c.atMs) })}</td>
            <td class="mono">{span(c.tookMs)}</td>
            <td>{c.error ? '—' : bytes(c.bytes ?? 0)}</td>
            <td>{c.whole ? t('whole') : t('changes')}</td>
            <td>
              {#if c.error}
                <span class="bad"><Ico icon={CircleX} size={14} class="bad" /> {c.error}</span>
              {:else}
                <span class="good"><Ico icon={CircleCheck} size={14} class="good" /> {t('copied')}</span>
              {/if}
            </td>
          </tr>
        {/each}
      </tbody>
    </table>
  {:else}
    <p class="empty">{t('No copy yet: the first starts once a node can keep it.')}</p>
  {/if}
{/if}

<style>
  .replica-head {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.5rem;
    margin: 0.4rem 0 0.9rem;
  }
  .replica-error {
    display: flex;
    align-items: flex-start;
    gap: 0.5rem;
    padding: 0.6rem 0.75rem;
    margin-bottom: 0.9rem;
    border: 1px solid var(--line);
    border-left: 3px solid var(--bad);
    border-radius: 6px;
    background: var(--panel);
  }
  .replica-error > span {
    flex: 1;
    overflow-wrap: anywhere;
  }
  .tile-value :global(.ico) {
    margin-right: 0.35rem;
  }
  .tile-value.good,
  .tile-value.good :global(.ico) {
    color: var(--good);
  }
  .tile-value.wait,
  .tile-value.wait :global(.ico) {
    color: var(--wait);
  }
  td :global(.ico.good) {
    color: var(--good);
  }
  td :global(.ico.bad),
  .replica-error :global(.ico.bad) {
    color: var(--bad);
  }
  h3 {
    margin: 1.3rem 0 0.5rem;
  }
  .promote {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.75rem;
    margin-top: 1rem;
  }
</style>

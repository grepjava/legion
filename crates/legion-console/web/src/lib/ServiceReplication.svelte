<script lang="ts">
  // A replicated database's members (DESIGN.md 6.9): the primary Legion
  // names and its epoch, the phase and why, each member's role, position
  // and lag as it last said, a change of primary under way and the last
  // one. A switchover or a failover is asked for as `legion replication`
  // asks: the Service's `replication.request`, reviewed before it is saved
  // (SpecChange), which the leader acts on once. A Service made by a
  // restore to a point in time says where the restore stands, before any
  // member runs too.
  import { bytes, type LObject } from './api.ts';
  import { cannotWrite } from './session.svelte.ts';
  import { t } from './i18n.svelte.ts';
  import { tone } from './columns.ts';
  import SpecChange from './SpecChange.svelte';

  let { service }: { service: LObject } = $props();

  interface Position {
    term: number;
    offset: string;
  }
  interface Member {
    task?: string;
    at: number;
    role?: string;
    healthy?: boolean;
    position?: Position;
    upstream?: string;
    streaming?: boolean;
    synchronous?: boolean;
    lag?: { bytes?: string; seconds?: number };
    diverged?: boolean;
    error?: string;
  }
  interface Change {
    kind: string;
    from?: string;
    to?: string;
    target?: string;
    reason: string;
    started: number;
    force?: boolean;
  }
  interface Status {
    epoch: number;
    primary?: string;
    phase: string;
    message?: string;
    members?: Record<string, Member>;
    synchronous?: string[];
    primaryPosition?: Position;
    change?: Change;
    lastChange?: { kind: string; from?: string; to: string; reason: string; at: number };
    handled?: number;
    refused?: string;
  }

  const ns = $derived(service.metadata.namespace ?? '');
  const name = $derived(service.metadata.name);
  const why = $derived(cannotWrite(ns));
  const st = $derived(service.status?.replication as Status | undefined);
  interface Restore {
    phase: string;
    backup?: string;
    takenMs?: number;
    node?: string;
    fetched?: number;
    failures?: number;
    message?: string;
  }
  const restore = $derived(service.status?.restore as Restore | undefined);
  const want = $derived(service.spec?.replication ?? {});
  const replicas = $derived((service.spec?.replicas ?? 1) as number);
  const members = $derived(Array.from({ length: replicas }, (_, i) => `${name}-${i}`));
  const others = $derived(members.filter((m) => m !== st?.primary));

  let now = $state(Date.now());
  $effect(() => {
    const id = setInterval(() => (now = Date.now()), 1000);
    return () => clearInterval(id);
  });

  /** A span for people, in its two largest units, as the CLI says it. */
  function span(ms: number): string {
    const s = Math.max(0, Math.round(ms / 1000));
    const [h, m, sec] = [Math.floor(s / 3600), Math.floor(s / 60) % 60, s % 60];
    if (h) return m ? `${h}h ${m}m` : `${h}h`;
    if (m) return sec ? `${m}m ${sec}s` : `${m}m`;
    return `${sec}s`;
  }

  function position(p?: Position): string {
    return p ? `${p.term}/${p.offset}` : '';
  }

  /** How far a replica is behind, as it said it or from the positions. */
  function lag(m: Member): string {
    if (m.role !== 'replica') return '';
    const parts: string[] = [];
    let b = m.lag?.bytes !== undefined ? Number(m.lag.bytes) : undefined;
    const p = st?.primaryPosition;
    if (b === undefined && p && m.position && m.position.term === p.term) {
      b = Math.max(0, Number(p.offset) - Number(m.position.offset));
    }
    if (b !== undefined) parts.push(bytes(b));
    if (m.lag?.seconds !== undefined) parts.push(`${m.lag.seconds.toFixed(1)} s`);
    return parts.join(', ');
  }

  // The request: what kind, to whom, forced; its moment taken when the
  // form opens and anew after each save, later than the last asked for.
  let kind = $state<'switchover' | 'failover' | ''>('');
  let to = $state('');
  let force = $state(false);
  let stamp = $state(Date.now());
  const at = $derived(Math.max(stamp, (want.request?.at ?? 0) + 1));
  const spec = $derived.by(() => {
    if (!kind) return null;
    const s = structuredClone($state.snapshot(service.spec));
    s.replication.request = {
      kind,
      ...(to ? { to } : {}),
      ...(kind === 'failover' && force ? { force: true } : {}),
      at
    };
    return s;
  });
  const notes = $derived(
    kind === 'switchover'
      ? [
          t('The primary {primary} is stopped cleanly; {to} is promoted once it holds all the primary wrote.', {
            primary: st?.primary ?? '?',
            to: to || t('the replica furthest ahead')
          }),
          t('Writes to {name} fail for the few seconds this takes.', { name })
        ]
      : [
          t('The primary {primary} is stopped, or waited for to stop, then {to} is promoted.', {
            primary: st?.primary ?? '?',
            to: to || '?'
          }),
          force
            ? t('Forced: promoted however far behind it is. Commits it lacks are lost.')
            : t('Refused while it is further behind than maximumLag, or not one the primary waited for.')
        ]
  );
  // What the leader made of the last request, once it acted on it.
  const outcome = $derived(
    want.request && st?.handled === want.request.at
      ? st?.refused
        ? t('Refused: {why}', { why: st.refused })
        : t('Taken up.')
      : want.request
        ? t('Asked for; not yet taken up.')
        : ''
  );
</script>

{#if restore && want.restore}
  <p class={restore.failures ? 'error' : 'muted'} role="status">
    {restore.backup
      ? t('Restore {phase}: {from} to {to}, from the backup {backup} taken {taken} on {node}; {n} files of log.', {
          phase: restore.phase,
          from: want.restore.from,
          to: want.restore.to ?? t('the end of its log'),
          backup: restore.backup,
          taken: restore.takenMs ? new Date(restore.takenMs).toISOString() : '?',
          node: restore.node ?? '?',
          n: restore.fetched ?? 0
        })
      : t('Restore {phase}: {from} to {to}.', {
          phase: restore.phase,
          from: want.restore.from,
          to: want.restore.to ?? t('the end of its log')
        })}
    {#if restore.message}{restore.message}{/if}
  </p>
{/if}
{#if !st}
  <p class="muted">{t('The leader has not recorded this database yet.')}</p>
{:else}
  <div class="cards">
    <div class="card">
      <h2>{t('Primary')}</h2>
      <p class="big">{st.primary ?? t('none')}</p>
      <p class="muted">{t('epoch {epoch}', { epoch: st.epoch })}</p>
    </div>
    <div class="card">
      <h2>{t('Phase')}</h2>
      <p class="big" aria-live="polite"><span class="state {tone(st.phase)}">{st.phase}</span></p>
      {#if st.message}<p class="muted">{st.message}</p>{/if}
    </div>
    <div class="card">
      <h2>{t('Synchronous')}</h2>
      {#if (want.synchronous ?? 0) > 0}
        <p class="big">{want.synchronous}</p>
        <p class="muted">
          {st.synchronous?.length
            ? t('of {members}', { members: st.synchronous.join(', ') })
            : t('none waited for: no replica healthy and streaming')}
        </p>
      {:else}
        <p class="big">{t('async')}</p>
        <p class="muted">{t('failover {mode}', { mode: want.failover ?? 'automatic' })}</p>
      {/if}
    </div>
  </div>

  {#if st.change}
    <p class="banner" role="status">
      {t('{kind} under way for {span}: from {from} to {to}. {reason}', {
        kind: st.change.kind,
        span: span(now - st.change.started),
        from: st.change.from ?? t('none'),
        to: st.change.to ?? st.change.target ?? t('(to choose)'),
        reason: st.change.reason
      })}
    </p>
  {/if}

  <table>
    <caption class="sr-only">{t('Members of {name}', { name })}</caption>
    <thead>
      <tr>
        <th>{t('Member')}</th>
        <th>{t('Role')}</th>
        <th>{t('Healthy')}</th>
        <th>{t('Position')}</th>
        <th>{t('Lag')}</th>
        <th>{t('Heard')}</th>
        <th>{t('Task')}</th>
      </tr>
    </thead>
    <tbody>
      {#each members as m (m)}
        {@const s = st.members?.[m]}
        <tr>
          <td>
            {m}
            {#if m === st.primary}<span class="tag">{t('named primary')}</span>{/if}
            {#if st.synchronous?.includes(m)}<span class="tag">{t('synchronous')}</span>{/if}
          </td>
          {#if s}
            <td>{s.role ?? 'unknown'}{s.role === 'replica' && s.streaming ? ` · ${t('streaming')}` : ''}</td>
            <td><span class="state {s.healthy ? 'good' : 'bad'}">{s.healthy ? t('yes') : t('no')}</span></td>
            <td><code>{position(s.position)}</code></td>
            <td>{lag(s)}</td>
            <td>{s.at ? t('{span} ago', { span: span(now - s.at) }) : ''}</td>
            <td>
              {#if s.task}<a href="/k/Task/{ns}/{s.task}">{s.task}</a>{/if}
              {#if s.diverged}<span class="state bad">{t('diverged')}</span>{/if}
              {#if s.error}<p class="error">{s.error}</p>{/if}
            </td>
          {:else}
            <td colspan="6" class="muted">{t('not heard from')}</td>
          {/if}
        </tr>
      {/each}
    </tbody>
  </table>

  {#if want.archive}
    {@const a = service.status?.archive}
    <p class="muted">
      {a
        ? t('Log archived to {target}: last {last}, shipped {shipped}, pruned {pruned}.', {
            target: a.target,
            last: a.last || t('none'),
            shipped: a.shippedMs ? t('{span} ago', { span: span(now - a.shippedMs) }) : t('never'),
            pruned: a.prunedMs ? t('{span} ago', { span: span(now - a.prunedMs) }) : t('never')
          })
        : t('Log archived to {target}: nothing shipped yet.', { target: want.archive.target })}
      {#if a?.pending}{t('{n} waiting.', { n: a.pending })}{/if}
    </p>
    {#if a?.failures}<p class="error">{t('{n} runs failed: {why}', { n: a.failures, why: a.message })}</p>{/if}
  {/if}

  {#if st.lastChange}
    <p class="muted">
      {t('Last change: {kind} from {from} to {to}, {when} ago: {reason}', {
        kind: st.lastChange.kind,
        from: st.lastChange.from ?? t('none'),
        to: st.lastChange.to,
        when: span(now - st.lastChange.at),
        reason: st.lastChange.reason
      })}
    </p>
  {/if}

  <form class="change" onsubmit={(e) => e.preventDefault()}>
    <fieldset disabled={!!why} title={why ?? ''}>
      <legend>{t('Change the primary')}</legend>
      <label>
        {t('Kind')}
        <select bind:value={kind}>
          <option value="">{t('choose')}</option>
          <option value="switchover">{t('Switchover (planned, loses nothing)')}</option>
          <option value="failover">{t('Failover (the primary is lost)')}</option>
        </select>
      </label>
      <label>
        {t('To')}
        <select bind:value={to}>
          <option value="">{kind === 'failover' ? t('choose') : t('the replica furthest ahead')}</option>
          {#each others as m (m)}<option value={m}>{m}</option>{/each}
        </select>
      </label>
      {#if kind === 'failover'}
        <label><input type="checkbox" bind:checked={force} /> {t('Force (pass over lag and synchronous checks)')}</label>
      {/if}
    </fieldset>
    {#if spec}
      <SpecChange
        object={service}
        {spec}
        {notes}
        disabled={!!why || !!st.change || (kind === 'failover' && !to)}
        save={kind === 'switchover' ? t('Ask for the switchover') : t('Ask for the failover')}
        onsaved={() => {
          stamp = Date.now();
          kind = '';
          to = '';
          force = false;
        }}
      />
      {#if st.change}<p class="muted">{t('A change is under way; ask once it is done.')}</p>{/if}
    {/if}
    {#if outcome}<p class="muted" role="status">{outcome}</p>{/if}
  </form>
{/if}

<style>
  .change {
    margin-top: 1.5rem;
  }
  .change label {
    display: inline-flex;
    gap: 0.4rem;
    align-items: center;
    margin-right: 1rem;
  }
</style>

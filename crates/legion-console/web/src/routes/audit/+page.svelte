<script lang="ts">
  // The audit log, filtered as `legion audit` filters it: who did what to
  // which object, and whether it went through.
  import { onMount } from 'svelte';
  import { api, type AuditEvent, type AuditFilter } from '#lib/api.ts';
  import { plural } from '#lib/kinds.ts';
  import { t } from '#lib/i18n.svelte.ts';
  import { Table } from '#lib/table.svelte.ts';
  import SortTh from '#lib/SortTh.svelte';
  import TableBar from '#lib/TableBar.svelte';
  import Pager from '#lib/Pager.svelte';

  let filter: AuditFilter = $state({ actor: '', action: '', namespace: '', kind: '', result: '', limit: 200 });
  let events: AuditEvent[] = $state([]);
  let truncated = $state(false);
  let error = $state('');
  let busy = $state(false);
  // How far back: the API leaves out the newest when more match than the
  // limit, so a window keeps what is shown recent.
  let span = $state(3_600_000);

  async function load(e?: SubmitEvent) {
    e?.preventDefault();
    busy = true;
    error = '';
    try {
      const since_ms = span ? Date.now() - span : undefined;
      const r = await api.audit({ ...$state.snapshot(filter), since_ms });
      events = r.events.toReversed();
      truncated = r.truncated;
    } catch (err) {
      error = (err as Error).message;
    } finally {
      busy = false;
    }
  }
  // Once on opening; then when asked, not on each key typed.
  onMount(() => {
    load();
  });

  const tone = (r: string) => ({ ok: 'good', denied: 'bad', error: 'bad' })[r] ?? '';
  const target = (e: AuditEvent) =>
    [e.target?.kind, e.target?.namespace, e.target?.name].filter(Boolean).join(' ');
  const link = (e: AuditEvent) =>
    e.target?.kind && e.target?.name
      ? `/k/${e.target.kind}/${e.target.namespace || '_'}/${e.target.name}`
      : null;
  const who = (e: AuditEvent) => e.actor.name || e.actor.kind;
  // The filters above ask the managers; the search below looks through
  // what they answered. Newest first, as a log is read.
  const table = new Table(() => events, {
    sorts: {
      when: (e) => e.timeMs,
      who,
      action: (e) => e.action,
      object: target,
      result: (e) => e.result,
      why: (e) => e.message ?? ''
    },
    sortBy: 'when',
    dir: 'descending',
    text: (e) => `${who(e)} ${e.action} ${target(e)} ${e.result} ${e.message ?? ''}`
  });
</script>

<header class="bar"><h1>{t('Audit')}</h1></header>
<form class="controls" onsubmit={load}>
  <input placeholder={t('who')} bind:value={filter.actor} aria-label={t('Actor')} />
  <input placeholder={t('action (apply, delete, exec…)')} bind:value={filter.action} aria-label={t('Action')} />
  <input placeholder={t('namespace')} bind:value={filter.namespace} aria-label={t('Namespace')} />
  <input placeholder={t('kind')} bind:value={filter.kind} aria-label={t('Kind')} />
  <select bind:value={filter.result} aria-label={t('Result')}>
    <option value="">{t('any result')}</option>
    <option value="ok">ok</option>
    <option value="denied">denied</option>
    <option value="error">error</option>
  </select>
  <select bind:value={span} aria-label={t('Since')}>
    <option value={3_600_000}>{t('last hour')}</option>
    <option value={86_400_000}>{t('last day')}</option>
    <option value={604_800_000}>{t('last week')}</option>
    <option value={0}>{t('all kept')}</option>
  </select>
  <button class="primary" disabled={busy}>{t('Show')}</button>
</form>
{#if error}<p class="error" role="alert">{error}</p>{/if}
{#if truncated}
  <p class="error" role="alert">{t('More matched than {limit}: the newest are left out. Narrow the filter or the time.', { limit: filter.limit ?? '' })}</p>
{/if}

{#if events.length}
  <TableBar {table} label={t('Search audit entries')} />
  <table>
    <thead>
      <tr>
        <SortTh {table} col="when">{t('When')}</SortTh>
        <SortTh {table} col="who">{t('Who')}</SortTh>
        <SortTh {table} col="action">{t('Action')}</SortTh>
        <SortTh {table} col="object">{t('Object')}</SortTh>
        <SortTh {table} col="result">{t('Result')}</SortTh>
        <SortTh {table} col="why">{t('Why')}</SortTh>
      </tr>
    </thead>
    <tbody>
      {#each table.rows as e, i (i)}
        {@const href = link(e)}
        <tr>
          <td>{new Date(e.timeMs).toLocaleString()}</td>
          <td>{who(e)}</td>
          <td>{e.action}</td>
          <td>
            {#if href}<a {href}>{target(e)}</a>{:else}{target(e)}{/if}
            {#if e.target?.kind && !e.target?.name}<span class="muted">({plural(e.target.kind)})</span>{/if}
          </td>
          <td><span class="state {tone(e.result)}">{e.result}</span></td>
          <td>{e.message ?? ''}</td>
        </tr>
      {:else}
        <tr><td colspan="6" class="muted">{t('Nothing matches.')}</td></tr>
      {/each}
    </tbody>
  </table>
  <Pager {table} label={t('Audit entries')} />
{:else if !error && !busy}
  <p class="empty">{t('Nothing matches.')}</p>
{/if}

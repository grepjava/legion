<script lang="ts">
  // What to act on now (`legion alerts`), as the managers work it out:
  // crash-looping tasks, nodes down or suspect, lost Volumes, the CA near
  // expiry, Raft without a leader or quorum. Kept current as a stream
  // by the layout; each links to its cause.
  import { alertHref } from '#lib/api.ts';
  import { session } from '#lib/session.svelte.ts';
  import { t } from '#lib/i18n.svelte.ts';
  import { Table } from '#lib/table.svelte.ts';
  import SortTh from '#lib/SortTh.svelte';
  import TableBar from '#lib/TableBar.svelte';
  import Pager from '#lib/Pager.svelte';

  let now = $state(Date.now());
  $effect(() => {
    const tick = setInterval(() => (now = Date.now()), 10_000);
    return () => clearInterval(tick);
  });
  const ago = (ms: number) => {
    const s = Math.max(0, Math.round((now - ms) / 1000));
    if (s < 60) return t('{n} s', { n: s });
    if (s < 3600) return t('{n} min', { n: Math.round(s / 60) });
    return t('{n} h', { n: Math.round(s / 3600) });
  };
  const shown = $derived(
    session.namespace
      ? session.alerts.filter((a) => !a.namespace || a.namespace === session.namespace)
      : session.alerts
  );
  const about = (a: (typeof shown)[number]) =>
    a.object ? `${a.namespace ? `${a.namespace}/` : ''}${a.object}` : t('the cluster');
  // Critical before warnings, the longest standing first within each; "For"
  // sorts on how long, so the later an alert began the shorter it has been.
  const table = new Table(() => shown, {
    sorts: {
      severity: (a) => (a.severity === 'critical' ? 0 : 1),
      kind: (a) => a.kind,
      about,
      for: (a) => -a.since_ms,
      message: (a) => a.message
    },
    sortBy: 'severity',
    text: (a) => `${a.severity} ${a.kind} ${about(a)} ${a.message}`
  });
</script>

<header class="bar">
  <h1>{t('Alerts')}</h1>
  {#if session.me?.all}
    <a class="button sm" href="/k/AlertReceiver">{t('Where they are sent')}</a>
  {/if}
</header>
{#if session.alertsError}<p class="error" role="alert">{session.alertsError}</p>{/if}

{#if shown.length}
  <TableBar {table} label={t('Search alerts')} />
  <table>
    <thead>
      <tr>
        <SortTh {table} col="severity">{t('Severity')}</SortTh>
        <SortTh {table} col="kind">{t('What')}</SortTh>
        <SortTh {table} col="about">{t('About')}</SortTh>
        <SortTh {table} col="for">{t('For')}</SortTh>
        <SortTh {table} col="message">{t('Message')}</SortTh>
      </tr>
    </thead>
    <tbody>
      {#each table.rows as a (a.kind + a.namespace + a.object)}
        <tr class="alert">
          <td><span class="state {a.severity === 'critical' ? 'bad' : 'wait'}">{a.severity}</span></td>
          <td>{a.kind}</td>
          <td><a href={alertHref(a)}>{about(a)}</a></td>
          <td>{ago(a.since_ms)}</td>
          <td>{a.message}</td>
        </tr>
      {:else}
        <tr><td colspan="5" class="muted">{t('Nothing matches.')}</td></tr>
      {/each}
    </tbody>
  </table>
  <Pager {table} label={t('Alerts')} />
{:else}
  <p class="empty">{t('Nothing to act on.')}</p>
{/if}

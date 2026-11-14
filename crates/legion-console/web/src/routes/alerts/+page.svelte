<script lang="ts">
  // What to act on now (`legion alerts`), as the managers work it out:
  // crash-looping tasks, nodes down or suspect, lost Volumes, the CA near
  // expiry, Raft without a leader or quorum. Kept current over a WebSocket
  // by the layout; each links to its cause.
  import { alertHref } from '#lib/api.ts';
  import { session } from '#lib/session.svelte.ts';
  import { t } from '#lib/i18n.svelte.ts';

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
</script>

<header class="bar"><h1>{t('Alerts')}</h1></header>
{#if session.alertsError}<p class="error" role="alert">{session.alertsError}</p>{/if}

{#if shown.length}
  <table>
    <thead>
      <tr><th>{t('Severity')}</th><th>{t('What')}</th><th>{t('About')}</th><th>{t('For')}</th><th>{t('Message')}</th></tr>
    </thead>
    <tbody>
      {#each shown as a (a.kind + a.namespace + a.object)}
        <tr class="alert">
          <td><span class="state {a.severity === 'critical' ? 'bad' : 'wait'}">{a.severity}</span></td>
          <td>{a.kind}</td>
          <td>
            <a href={alertHref(a)}
              >{a.object ? `${a.namespace ? `${a.namespace}/` : ''}${a.object}` : t('the cluster')}</a
            >
          </td>
          <td>{ago(a.since_ms)}</td>
          <td>{a.message}</td>
        </tr>
      {/each}
    </tbody>
  </table>
{:else}
  <p class="muted">{t('Nothing to act on.')}</p>
{/if}

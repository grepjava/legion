<script lang="ts">
  // What to act on now (`legion alerts`), as the managers work it out:
  // crash-looping tasks, nodes down or suspect, lost Volumes, the CA near
  // expiry, Raft without a leader or quorum. Kept current over a WebSocket
  // by the layout; each links to its cause.
  import { alertHref } from '#lib/api.ts';
  import { session } from '#lib/session.svelte.ts';

  let now = $state(Date.now());
  $effect(() => {
    const t = setInterval(() => (now = Date.now()), 10_000);
    return () => clearInterval(t);
  });
  const ago = (ms: number) => {
    const s = Math.max(0, Math.round((now - ms) / 1000));
    if (s < 60) return `${s} s`;
    if (s < 3600) return `${Math.round(s / 60)} min`;
    return `${Math.round(s / 3600)} h`;
  };
  const shown = $derived(
    session.namespace
      ? session.alerts.filter((a) => !a.namespace || a.namespace === session.namespace)
      : session.alerts
  );
</script>

<header class="bar"><h1>Alerts</h1></header>
{#if session.alertsError}<p class="error" role="alert">{session.alertsError}</p>{/if}

{#if shown.length}
  <table>
    <thead>
      <tr><th>Severity</th><th>What</th><th>About</th><th>For</th><th>Message</th></tr>
    </thead>
    <tbody>
      {#each shown as a (a.kind + a.namespace + a.object)}
        <tr class="alert">
          <td><span class="state {a.severity === 'critical' ? 'bad' : 'wait'}">{a.severity}</span></td>
          <td>{a.kind}</td>
          <td>
            <a href={alertHref(a)}
              >{a.object ? `${a.namespace ? `${a.namespace}/` : ''}${a.object}` : 'the cluster'}</a
            >
          </td>
          <td>{ago(a.since_ms)}</td>
          <td>{a.message}</td>
        </tr>
      {/each}
    </tbody>
  </table>
{:else}
  <p class="muted">Nothing to act on.</p>
{/if}

<script lang="ts">
  // The audit log, filtered as `legion audit` filters it: who did what to
  // which object, and whether it went through.
  import { onMount } from 'svelte';
  import { api, type AuditEvent, type AuditFilter } from '#lib/api.ts';
  import { plural } from '#lib/kinds.ts';

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
</script>

<header class="bar"><h1>Audit</h1></header>
<form class="controls" onsubmit={load}>
  <input placeholder="who" bind:value={filter.actor} aria-label="Actor" />
  <input placeholder="action (apply, delete, exec…)" bind:value={filter.action} aria-label="Action" />
  <input placeholder="namespace" bind:value={filter.namespace} aria-label="Namespace" />
  <input placeholder="kind" bind:value={filter.kind} aria-label="Kind" />
  <select bind:value={filter.result} aria-label="Result">
    <option value="">any result</option>
    <option value="ok">ok</option>
    <option value="denied">denied</option>
    <option value="error">error</option>
  </select>
  <select bind:value={span} aria-label="Since">
    <option value={3_600_000}>last hour</option>
    <option value={86_400_000}>last day</option>
    <option value={604_800_000}>last week</option>
    <option value={0}>all kept</option>
  </select>
  <button class="primary" disabled={busy}>Show</button>
</form>
{#if error}<p class="error" role="alert">{error}</p>{/if}
{#if truncated}
  <p class="error" role="alert">More matched than {filter.limit}: the newest are left out. Narrow the filter or the time.</p>
{/if}

{#if events.length}
  <table>
    <thead>
      <tr><th>When</th><th>Who</th><th>Action</th><th>Object</th><th>Result</th><th>Why</th></tr>
    </thead>
    <tbody>
      {#each events as e, i (i)}
        {@const href = link(e)}
        <tr>
          <td>{new Date(e.timeMs).toLocaleString()}</td>
          <td>{e.actor.name || e.actor.kind}</td>
          <td>{e.action}</td>
          <td>
            {#if href}<a {href}>{target(e)}</a>{:else}{target(e)}{/if}
            {#if e.target?.kind && !e.target?.name}<span class="muted">({plural(e.target.kind)})</span>{/if}
          </td>
          <td><span class="state {tone(e.result)}">{e.result}</span></td>
          <td>{e.message ?? ''}</td>
        </tr>
      {/each}
    </tbody>
  </table>
{:else if !error && !busy}
  <p class="muted">Nothing matches.</p>
{/if}

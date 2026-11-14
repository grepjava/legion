<script lang="ts">
  import { alertHref, api, follow, type LObject, type Status } from '#lib/api.ts';
  import { tone } from '#lib/columns.ts';
  import { session } from '#lib/session.svelte.ts';
  import Quotas from '#lib/Quotas.svelte';

  let status: Status | null = $state(null);
  let statusError = $state('');
  let nodes: LObject[] = $state([]);
  let services: LObject[] = $state([]);
  let tasks: LObject[] = $state([]);
  let errors: Record<string, string> = $state({});

  $effect(() => {
    api
      .status()
      .then((s) => (status = s))
      .catch((e) => (statusError = e.message));
  });

  $effect(() => {
    const ns = session.namespace;
    const fail = (kind: string) => (e: Error) => (errors[kind] = e.message);
    const stops = [
      follow('Service', ns, (o) => (services = o), fail('Service')),
      follow('Task', ns, (o) => (tasks = o), fail('Task'))
    ];
    if (session.me?.all) stops.push(follow('Node', '', (o) => (nodes = o), fail('Node')));
    return () => stops.forEach((s) => s());
  });

  function count(objects: LObject[], of: (o: LObject) => string): [string, number][] {
    const by = new Map<string, number>();
    for (const o of objects) by.set(of(o) || '—', (by.get(of(o) || '—') ?? 0) + 1);
    return [...by.entries()].sort();
  }
  const live = $derived(tasks.filter((t) => t.spec?.desiredState === 'running'));
</script>

<h1>Overview</h1>

{#if session.alerts.length}
  <section>
    <h2><a href="/alerts">Alerts</a></h2>
    <ul class="plain">
      {#each session.alerts.slice(0, 5) as a (a.kind + a.namespace + a.object)}
        <li>
          <span class="state {a.severity === 'critical' ? 'bad' : 'wait'}">{a.severity}</span>
          <a href={alertHref(a)}>{a.message}</a>
        </li>
      {/each}
    </ul>
    {#if session.alerts.length > 5}<p><a href="/alerts">All {session.alerts.length}</a></p>{/if}
  </section>
{/if}

<section class="cards">
  <div class="card">
    <h2>Managers</h2>
    {#if status}
      <p>
        <span class="state {status.leader ? 'good' : 'bad'}"
          >{status.leader ? 'leader elected' : 'no leader'}</span
        >
        term {status.term}, store version {status.version}
      </p>
      <ul class="plain">
        {#each status.members as m (m.id)}
          <li>
            {m.addr}
            {#if m.id === status.leader}<span class="tag">leader</span>{/if}
            {#if !m.voter}<span class="tag">learner</span>{/if}
          </li>
        {/each}
      </ul>
    {:else if statusError}
      <p class="error" role="alert">{statusError}</p>
    {:else}
      <p>…</p>
    {/if}
  </div>

  {#if session.me?.all}
    <div class="card">
      <h2><a href="/k/Node">Nodes</a></h2>
      <p class="big">{nodes.length}</p>
      {#each count(nodes, (n) => n.status?.state) as [state, n] (state)}
        <span class="state {tone(state)}">{n} {state}</span>
      {/each}
      {#if errors.Node}<p class="error" role="alert">{errors.Node}</p>{/if}
    </div>
  {/if}

  <div class="card">
    <h2><a href="/k/Service">Services</a></h2>
    <p class="big">{services.length}</p>
    {#if errors.Service}<p class="error" role="alert">{errors.Service}</p>{/if}
  </div>

  <div class="card">
    <h2><a href="/k/Task">Tasks</a></h2>
    <p class="big">{live.length}</p>
    {#each count(live, (t) => t.status?.state) as [state, n] (state)}
      <span class="state {tone(state)}">{n} {state}</span>
    {/each}
    {#if errors.Task}<p class="error" role="alert">{errors.Task}</p>{/if}
  </div>
</section>

{#if live.some((t) => t.status?.state === 'pending' || t.status?.state === 'failed')}
  <h2>Needs attention</h2>
  <table>
    <thead><tr><th>Task</th><th>State</th><th>Why</th></tr></thead>
    <tbody>
      {#each live.filter((t) => ['pending', 'failed'].includes(t.status?.state)) as t (t.metadata.uid)}
        <tr>
          <td><a href="/k/Task/{t.metadata.namespace}/{t.metadata.name}">{t.metadata.namespace}/{t.metadata.name}</a></td>
          <td><span class="state {tone(t.status?.state)}">{t.status?.state}</span></td>
          <td>{t.status?.message ?? ''}</td>
        </tr>
      {/each}
    </tbody>
  </table>
{/if}

<h2>Quotas</h2>
<Quotas namespace={session.namespace} />

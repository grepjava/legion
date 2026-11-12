<script lang="ts">
  // A node's capacity, what its tasks use now, and the tasks on it, live.
  import { api, bytes, follow, type LObject } from './api.ts';
  import { tone } from './columns.ts';

  let { node }: { node: LObject } = $props();
  const name = $derived(node.metadata.name);
  let tasks: LObject[] = $state([]);
  let use: Record<string, { cpu: number; memory: number }> = $state({});
  let error = $state('');

  $effect(() => {
    const n = name;
    tasks = [];
    return follow(
      'Task',
      '',
      (all) => (tasks = all.filter((t) => t.spec?.node === n)),
      (e) => (error = e.message)
    );
  });

  // Usage is reported with heartbeats, not stored: asked for again.
  $effect(() => {
    const n = name;
    let stopped = false;
    async function poll() {
      try {
        const u = await api.usage();
        const here = u.nodes.find((x) => x.node === n);
        use = Object.fromEntries(
          (here?.tasks ?? []).map((t) => [
            `${t.namespace}/${t.task}`,
            { cpu: t.cpu_percent, memory: t.memory_bytes }
          ])
        );
      } catch {
        use = {};
      }
      if (!stopped) timer = setTimeout(poll, 5000);
    }
    let timer = setTimeout(poll, 0);
    return () => {
      stopped = true;
      clearTimeout(timer);
    };
  });

  const total = $derived(
    Object.values(use).reduce((a, u) => ({ cpu: a.cpu + u.cpu, memory: a.memory + u.memory }), {
      cpu: 0,
      memory: 0
    })
  );
  const running = $derived(tasks.filter((t) => t.status?.state === 'running').length);
  const alloc = $derived(node.status?.allocatable);
</script>

<div class="cards">
  <div class="card">
    <h2>Allocatable</h2>
    <p class="big">{alloc?.cpu ?? '?'} CPU</p>
    <p class="muted">{alloc?.memory ?? '?'} memory</p>
  </div>
  <div class="card">
    <h2>In use now</h2>
    <p class="big">{total.cpu.toFixed(1)}% CPU</p>
    <p class="muted">{bytes(total.memory)} memory, of the tasks you can see</p>
  </div>
  <div class="card">
    <h2>Tasks</h2>
    <p class="big">{tasks.length}</p>
    <p class="muted">{running} running</p>
  </div>
</div>

{#if error}<p class="error">{error}</p>{/if}

{#if tasks.length}
  <table>
    <thead>
      <tr>
        <th>Task</th>
        <th>Namespace</th>
        <th>State</th>
        <th>IP</th>
        <th>CPU</th>
        <th>Memory</th>
      </tr>
    </thead>
    <tbody>
      {#each tasks as t (t.metadata.uid)}
        {@const u = use[`${t.metadata.namespace}/${t.metadata.name}`]}
        <tr>
          <td><a href="/k/Task/{t.metadata.namespace}/{t.metadata.name}">{t.metadata.name}</a></td>
          <td>{t.metadata.namespace}</td>
          <td><span class="state {tone(t.status?.state ?? '')}">{t.status?.state ?? ''}</span></td>
          <td>{t.status?.ip ?? ''}</td>
          <td>{u ? `${u.cpu.toFixed(1)}%` : ''}</td>
          <td>{u ? bytes(u.memory) : ''}</td>
        </tr>
      {/each}
    </tbody>
  </table>
{:else if !error}
  <p class="muted">No tasks on this node.</p>
{/if}

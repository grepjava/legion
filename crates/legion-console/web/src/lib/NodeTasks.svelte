<script lang="ts">
  // A node's capacity, what its tasks use now, and the tasks on it, live.
  import { bytes, follow, followUsage, type LObject } from './api.ts';
  import { tone } from './columns.ts';
  import { t } from './i18n.svelte.ts';
  import { Table } from './table.svelte.ts';
  import SortTh from './SortTh.svelte';
  import TableBar from './TableBar.svelte';
  import Pager from './Pager.svelte';

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

  // Usage is reported with heartbeats, not stored: followed as it comes.
  $effect(() => {
    const n = name;
    return followUsage(
      '',
      (nodes) => {
        const here = nodes.find((x) => x.node === n);
        use = Object.fromEntries(
          (here?.tasks ?? []).map((t) => [
            `${t.namespace}/${t.task}`,
            { cpu: t.cpu_percent, memory: t.memory_bytes }
          ])
        );
      },
      () => (use = {})
    );
  });

  const total = $derived(
    Object.values(use).reduce((a, u) => ({ cpu: a.cpu + u.cpu, memory: a.memory + u.memory }), {
      cpu: 0,
      memory: 0
    })
  );
  const running = $derived(tasks.filter((t) => t.status?.state === 'running').length);
  const alloc = $derived(node.status?.allocatable);

  // The tasks by name; CPU and memory sort as numbers, a task not reported
  // on below every one that is.
  const used = (x: LObject) => use[`${x.metadata.namespace}/${x.metadata.name}`];
  const table = new Table(() => tasks, {
    sorts: {
      name: (x) => x.metadata.name,
      namespace: (x) => x.metadata.namespace ?? '',
      state: (x) => x.status?.state ?? '',
      ip: (x) => x.status?.ip ?? '',
      cpu: (x) => used(x)?.cpu ?? -1,
      memory: (x) => used(x)?.memory ?? -1
    },
    sortBy: 'name',
    text: (x) => `${x.metadata.name} ${x.metadata.namespace ?? ''} ${x.status?.state ?? ''} ${x.status?.ip ?? ''}`
  });
</script>

<div class="cards">
  <div class="card">
    <h2>{t('Allocatable')}</h2>
    <p class="big">{t('{cpu} CPU', { cpu: alloc?.cpu ?? '?' })}</p>
    <p class="muted">{t('{memory} memory', { memory: alloc?.memory ?? '?' })}</p>
  </div>
  <div class="card">
    <h2>{t('In use now')}</h2>
    <p class="big">{t('{cpu}% CPU', { cpu: total.cpu.toFixed(1) })}</p>
    <p class="muted">{t('{memory} memory, of the tasks you can see', { memory: bytes(total.memory) })}</p>
  </div>
  <div class="card">
    <h2>{t('Tasks')}</h2>
    <p class="big">{tasks.length}</p>
    <p class="muted">{t('{n} running', { n: running })}</p>
  </div>
</div>

{#if error}<p class="error" role="alert">{error}</p>{/if}

{#if tasks.length}
  <TableBar {table} label={t('Search tasks')} />
  <table>
    <thead>
      <tr>
        <SortTh {table} col="name">{t('Task')}</SortTh>
        <SortTh {table} col="namespace">{t('Namespace')}</SortTh>
        <SortTh {table} col="state">{t('State')}</SortTh>
        <SortTh {table} col="ip">{t('IP')}</SortTh>
        <SortTh {table} col="cpu">{t('CPU')}</SortTh>
        <SortTh {table} col="memory">{t('Memory')}</SortTh>
      </tr>
    </thead>
    <tbody>
      {#each table.rows as task (task.metadata.uid)}
        {@const u = used(task)}
        <tr>
          <td><a href="/k/Task/{task.metadata.namespace}/{task.metadata.name}">{task.metadata.name}</a></td>
          <td>{task.metadata.namespace}</td>
          <td><span class="state {tone(task.status?.state ?? '')}">{task.status?.state ?? ''}</span></td>
          <td>{task.status?.ip ?? ''}</td>
          <td>{u ? `${u.cpu.toFixed(1)}%` : ''}</td>
          <td>{u ? bytes(u.memory) : ''}</td>
        </tr>
      {:else}
        <tr><td colspan="6" class="muted">{t('Nothing matches.')}</td></tr>
      {/each}
    </tbody>
  </table>
  <Pager {table} label={t('Tasks')} />
{:else if !error}
  <p class="empty">{t('No tasks on this node.')}</p>
{/if}

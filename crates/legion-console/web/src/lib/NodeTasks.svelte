<script lang="ts">
  // A node's capacity, what its tasks use now, and the tasks on it, live.
  import { api, bytes, follow, type LObject } from './api.ts';
  import { tone } from './columns.ts';
  import { t } from './i18n.svelte.ts';

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
  <table>
    <thead>
      <tr>
        <th>{t('Task')}</th>
        <th>{t('Namespace')}</th>
        <th>{t('State')}</th>
        <th>{t('IP')}</th>
        <th>{t('CPU')}</th>
        <th>{t('Memory')}</th>
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
  <p class="muted">{t('No tasks on this node.')}</p>
{/if}

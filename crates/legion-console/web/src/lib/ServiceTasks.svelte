<script lang="ts">
  // A Service's or Job's tasks, live: those meant to run first, then the
  // ended ones it keeps; each with its state, node and address.
  import { follow, type LObject } from './api.ts';
  import { tone } from './columns.ts';
  import { t } from './i18n.svelte.ts';
  import { Table } from './table.svelte.ts';
  import SortTh from './SortTh.svelte';
  import TableBar from './TableBar.svelte';
  import Pager from './Pager.svelte';

  let { owner }: { owner: LObject } = $props();
  const ns = $derived(owner.metadata.namespace ?? '');
  const name = $derived(owner.metadata.name);
  let tasks: LObject[] = $state([]);
  let error = $state('');
  let loaded = $state(false);

  $effect(() => {
    const [n, s] = [ns, name];
    tasks = [];
    loaded = false;
    return follow(
      'Task',
      n,
      (all) => {
        tasks = all.filter((x) => x.spec?.service === s);
        loaded = true;
      },
      (e) => (error = e.message)
    );
  });

  const meant = (x: LObject) => (x.spec?.desiredState === 'running' ? 0 : 1);
  // First those meant to run, then the ended ones, each by name: an order no
  // header gives, kept until a header is chosen.
  const table = new Table(() => tasks, {
    sorts: {
      meant: (x) => `${meant(x)} ${x.metadata.name}`,
      name: (x) => x.metadata.name,
      state: (x) => String(x.status?.state ?? ''),
      node: (x) => x.spec?.node ?? '',
      ip: (x) => x.status?.ip ?? '',
      restarts: (x) => Number(x.status?.restarts ?? 0)
    },
    sortBy: 'meant',
    text: (x) => `${x.metadata.name} ${x.status?.state ?? ''} ${x.spec?.node ?? ''} ${x.status?.ip ?? ''}`
  });
</script>

<h2>{t('Tasks')}</h2>
{#if error}<p class="error" role="alert">{error}</p>{/if}
{#if tasks.length}
  <TableBar {table} label={t('Search tasks')} />
  <table>
    <thead>
      <tr>
        <SortTh {table} col="name">{t('Task')}</SortTh>
        <SortTh {table} col="state">{t('State')}</SortTh>
        <SortTh {table} col="node">{t('Node')}</SortTh>
        <SortTh {table} col="ip">{t('IP')}</SortTh>
        <SortTh {table} col="restarts">{t('Restarts')}</SortTh>
      </tr>
    </thead>
    <tbody>
      {#each table.rows as x (x.metadata.uid)}
        {@const st = String(x.status?.state ?? '')}
        <tr class:ended={meant(x)}>
          <td><a href="/k/Task/{encodeURIComponent(ns)}/{encodeURIComponent(x.metadata.name)}">{x.metadata.name}</a></td>
          <td>{#if st}<span class="state {tone(st)}">{st}</span>{/if}</td>
          <td>
            {#if x.spec?.node}<a href="/k/Node/_/{encodeURIComponent(x.spec.node)}">{x.spec.node}</a>{/if}
          </td>
          <td class="mono">{x.status?.ip ?? ''}</td>
          <td>{x.status?.restarts ?? 0}</td>
        </tr>
      {:else}
        <tr><td colspan="5" class="muted">{t('Nothing matches.')}</td></tr>
      {/each}
    </tbody>
  </table>
  <Pager {table} label={t('Tasks')} />
{:else if loaded && !error}
  <p class="empty">{t('No tasks yet.')}</p>
{:else if !error}
  <div class="skeleton" aria-hidden="true"></div>
{/if}

<style>
  tr.ended td {
    color: var(--muted);
  }
</style>

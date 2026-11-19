<script lang="ts">
  // A Service's or Job's tasks, live: those meant to run first, then the
  // ended ones it keeps; each with its state, node and address.
  import { follow, type LObject } from './api.ts';
  import { tone } from './columns.ts';
  import { t } from './i18n.svelte.ts';

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
  const shown = $derived(
    [...tasks].sort((a, b) => meant(a) - meant(b) || a.metadata.name.localeCompare(b.metadata.name))
  );
</script>

<h2>{t('Tasks')}</h2>
{#if error}<p class="error" role="alert">{error}</p>{/if}
{#if shown.length}
  <table>
    <thead>
      <tr>
        <th scope="col">{t('Task')}</th>
        <th scope="col">{t('State')}</th>
        <th scope="col">{t('Node')}</th>
        <th scope="col">{t('IP')}</th>
        <th scope="col">{t('Restarts')}</th>
      </tr>
    </thead>
    <tbody>
      {#each shown as x (x.metadata.uid)}
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
      {/each}
    </tbody>
  </table>
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

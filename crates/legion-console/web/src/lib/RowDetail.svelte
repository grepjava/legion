<script lang="ts">
  // What a list's row opens on (CONSOLE.md, the console's feel, 7): what
  // happened to the object lately, and a Volume's snapshots, without
  // leaving the list.
  import { api, type LObject } from './api.ts';
  import Events from './Events.svelte';
  import { t } from './i18n.svelte.ts';

  let { object }: { object: LObject } = $props();
  const kind = $derived(object.kind);
  const ns = $derived(object.metadata.namespace ?? '');
  const name = $derived(object.metadata.name);

  let snapshots: string[] | null = $state(null);
  let snapError = $state('');
  $effect(() => {
    if (kind !== 'Volume') return;
    let stopped = false;
    snapshots = null;
    snapError = '';
    api.volumeSnapshots(ns, name).then(
      (r) => {
        if (!stopped) snapshots = r.snapshots;
      },
      (e: Error) => {
        if (!stopped) snapError = e.message;
      }
    );
    return () => {
      stopped = true;
    };
  });
</script>

<div class="row-detail">
  {#if kind === 'Volume'}
    <section>
      <h3>{t('Snapshots')}</h3>
      {#if snapError}
        <p class="error">{snapError}</p>
      {:else if snapshots === null}
        <div class="skeleton-row" aria-hidden="true"></div>
      {:else if snapshots.length}
        <ul class="plain mono">
          {#each snapshots as s (s)}<li>@{s}</li>{/each}
        </ul>
      {:else}
        <p class="muted">{t('No ZFS snapshots yet.')}</p>
      {/if}
    </section>
  {/if}
  <section>
    <h3>{t('Lately')}</h3>
    <Events {kind} namespace={ns} {name} />
  </section>
</div>

<style>
  .row-detail {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(18rem, 1fr));
    gap: 1rem;
    padding: 0.5rem 0.25rem 0.75rem 1.6rem;
  }
  .row-detail h3 {
    margin: 0 0 0.35rem;
  }
  .row-detail :global(table) {
    font-size: 0.85rem;
  }
  .row-detail :global(.empty) {
    padding: 0.6rem;
  }
</style>

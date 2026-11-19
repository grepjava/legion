<script lang="ts">
  // What happened to an object lately, newest first, followed as it
  // happens (CONSOLE.md phase 2): a Service's or Job's with its tasks'. The
  // manager answering works them out from what it sees change and keeps
  // an hour of them, so after it restarts there are fewer.
  import { followEvents, type ObjectEvent } from './api.ts';
  import { t } from './i18n.svelte.ts';

  let { kind, namespace, name }: { kind: string; namespace: string; name: string } = $props();
  let events: ObjectEvent[] = $state([]);
  let error = $state('');

  $effect(() => {
    events = [];
    return followEvents(
      namespace,
      kind,
      name,
      (all) => (events = all),
      (why) => (error = why)
    );
  });

  const newest = $derived([...events].reverse());
  const time = (ms: number) => new Date(ms).toLocaleTimeString();
</script>

{#if error}<p class="error" role="alert">{error}</p>{/if}
{#if newest.length}
  <table aria-label={t('Events of {name}', { name })}>
    <thead>
      <tr>
        <th>{t('Time')}</th>
        <th>{t('Object')}</th>
        <th>{t('What happened')}</th>
        <th>{t('Why')}</th>
      </tr>
    </thead>
    <tbody>
      {#each newest as e, i (i)}
        <tr>
          <td title={new Date(e.at_ms).toISOString()}>{time(e.at_ms)}</td>
          <td
            >{#if e.kind === kind && e.name === name}<span class="muted">{t('this')}</span
              >{:else}<a href="/k/{e.kind}/{e.namespace || '_'}/{e.name}">{e.kind}/{e.name}</a>{/if}</td
          >
          <td>{e.reason}</td>
          <td>{e.message}</td>
        </tr>
      {/each}
    </tbody>
  </table>
{:else if !error}
  <p class="empty">{t('Nothing has happened to it lately that this manager saw.')}</p>
{/if}

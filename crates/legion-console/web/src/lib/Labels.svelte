<script lang="ts">
  // An object's labels: each links to the list of its kind with that
  // label; a user who may change the object adds and removes them.
  import { api, type LObject } from './api.ts';
  import { cannotWrite } from './session.svelte.ts';
  import { t } from './i18n.svelte.ts';

  let { object, scoped }: { object: LObject; scoped: boolean } = $props();
  const ns = $derived(object.metadata.namespace ?? '');
  const why = $derived(cannotWrite(scoped ? '' : ns));
  const labels = $derived(Object.entries(object.metadata.labels ?? {}));
  let adding = $state('');
  let error = $state('');

  async function change(changes: Record<string, string | null>) {
    error = '';
    try {
      await api.labels(object.kind, ns, object.metadata.name, changes);
      adding = '';
    } catch (e) {
      error = (e as Error).message;
    }
  }

  function add(e: SubmitEvent) {
    e.preventDefault();
    const eq = adding.indexOf('=');
    if (eq < 1) {
      error = t('a label is key=value');
      return;
    }
    change({ [adding.slice(0, eq).trim()]: adding.slice(eq + 1).trim() });
  }
</script>

{#each labels as [k, v] (k)}
  <span class="tag">
    <a href="/k/{object.kind}?filter={encodeURIComponent(`${k}=${v}`)}">{k}={v}</a>
    {#if !why}
      <button class="link" title={t('Remove {item}', { item: k })} aria-label={t('Remove {item}', { item: k })} onclick={() => change({ [k]: null })}
        >×</button
      >
    {/if}
  </span>
{/each}
{#if !labels.length}<span class="muted">{t('none')}</span>{/if}
{#if !why}
  <form class="inline" onsubmit={add}>
    <input placeholder={t('key=value')} bind:value={adding} aria-label={t('New label')} />
    <button disabled={!adding}>{t('Add')}</button>
  </form>
{/if}
{#if error}<p class="error" role="alert">{error}</p>{/if}

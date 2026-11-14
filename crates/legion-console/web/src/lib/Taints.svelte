<script lang="ts">
  // A node's taints: NoSchedule keeps new tasks off, PreferNoSchedule
  // makes it a last choice, NoExecute moves running tasks away too. A task
  // goes there only with a toleration. Changed over the version shown.
  import { api, ApiError, type LObject } from './api.ts';
  import { cannotWrite } from './session.svelte.ts';
  import { t } from './i18n.svelte.ts';

  type Taint = { key: string; value?: string; effect: string };
  let { node }: { node: LObject } = $props();
  const taints: Taint[] = $derived(node.spec?.taints ?? []);
  const why = $derived(cannotWrite(''));
  let key = $state('');
  let value = $state('');
  let effect = $state('NoSchedule');
  let error = $state('');

  async function write(next: Taint[], what: string) {
    if (!confirm(what)) return;
    error = '';
    const spec = { ...$state.snapshot(node.spec), taints: next };
    try {
      await api.apply({
        object: {
          apiVersion: node.apiVersion,
          kind: 'Node',
          metadata: { name: node.metadata.name, labels: node.metadata.labels ?? {} },
          spec
        },
        expectedVersion: node.metadata.version
      });
      key = '';
      value = '';
    } catch (e) {
      error =
        e instanceof ApiError && e.status === 409
          ? t('The node changed meanwhile (its heartbeat, perhaps); try again.')
          : (e as Error).message;
    }
  }

  const show = (taint: Taint) => `${taint.key}${taint.value ? `=${taint.value}` : ''}:${taint.effect}`;

  function add(e: SubmitEvent) {
    e.preventDefault();
    const taint: Taint = { key: key.trim(), effect, ...(value.trim() ? { value: value.trim() } : {}) };
    const vars = { node: node.metadata.name, taint: show(taint) };
    const ask =
      effect === 'NoExecute'
        ? t('Taint {node} {taint}? Tasks without a toleration for it move away.', vars)
        : t('Taint {node} {taint}?', vars);
    write([...taints.filter((x) => !(x.key === taint.key && x.effect === taint.effect)), taint], ask);
  }
</script>

{#each taints as taint (show(taint))}
  <span class="tag">
    {show(taint)}
    {#if !why}
      <button
        class="link"
        aria-label={t('Remove {item}', { item: show(taint) })}
        onclick={() =>
          write(
            taints.filter((x) => x !== taint),
            t('Remove {taint} from {node}?', { taint: show(taint), node: node.metadata.name })
          )}
        >×</button
      >
    {/if}
  </span>
{/each}
{#if !taints.length}<span class="muted">{t('none')}</span>{/if}
{#if !why}
  <form class="inline" onsubmit={add}>
    <input placeholder={t('key')} bind:value={key} aria-label={t('Taint key')} required />
    <input placeholder={t('value')} bind:value={value} aria-label={t('Taint value')} />
    <select bind:value={effect} aria-label={t('Effect')}>
      <option>NoSchedule</option>
      <option>PreferNoSchedule</option>
      <option>NoExecute</option>
    </select>
    <button disabled={!key.trim()}>{t('Add')}</button>
  </form>
{/if}
{#if error}<p class="error" role="alert">{error}</p>{/if}

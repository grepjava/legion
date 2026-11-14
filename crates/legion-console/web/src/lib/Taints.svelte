<script lang="ts">
  // A node's taints: NoSchedule keeps new tasks off, PreferNoSchedule
  // makes it a last choice, NoExecute moves running tasks away too. A task
  // goes there only with a toleration. Changed over the version shown.
  import { api, ApiError, type LObject } from './api.ts';
  import { cannotWrite } from './session.svelte.ts';

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
          ? 'The node changed meanwhile (its heartbeat, perhaps); try again.'
          : (e as Error).message;
    }
  }

  const show = (t: Taint) => `${t.key}${t.value ? `=${t.value}` : ''}:${t.effect}`;

  function add(e: SubmitEvent) {
    e.preventDefault();
    const t: Taint = { key: key.trim(), effect, ...(value.trim() ? { value: value.trim() } : {}) };
    const warn = effect === 'NoExecute' ? ' Tasks without a toleration for it move away.' : '';
    write([...taints.filter((x) => !(x.key === t.key && x.effect === t.effect)), t], `Taint ${node.metadata.name} ${show(t)}?${warn}`);
  }
</script>

{#each taints as t (show(t))}
  <span class="tag">
    {show(t)}
    {#if !why}
      <button
        class="link"
        aria-label="Remove {show(t)}"
        onclick={() => write(taints.filter((x) => x !== t), `Remove ${show(t)} from ${node.metadata.name}?`)}
        >×</button
      >
    {/if}
  </span>
{/each}
{#if !taints.length}<span class="muted">none</span>{/if}
{#if !why}
  <form class="inline" onsubmit={add}>
    <input placeholder="key" bind:value={key} aria-label="Taint key" required />
    <input placeholder="value" bind:value={value} aria-label="Taint value" />
    <select bind:value={effect} aria-label="Effect">
      <option>NoSchedule</option>
      <option>PreferNoSchedule</option>
      <option>NoExecute</option>
    </select>
    <button disabled={!key.trim()}>Add</button>
  </form>
{/if}
{#if error}<p class="error" role="alert">{error}</p>{/if}

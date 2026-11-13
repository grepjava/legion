<script lang="ts">
  import { page } from '$app/state';
  import { api, follow, type LObject } from '#lib/api.ts';
  import { columns, tone } from '#lib/columns.ts';
  import { plural } from '#lib/kinds.ts';
  import Actions from '#lib/Actions.svelte';
  import Labels from '#lib/Labels.svelte';
  import NodeTasks from '#lib/NodeTasks.svelte';
  import Rollout from '#lib/Rollout.svelte';
  import Taints from '#lib/Taints.svelte';
  import TaskLogs from '#lib/TaskLogs.svelte';
  import Quotas from '#lib/Quotas.svelte';
  import UsageCharts from '#lib/UsageCharts.svelte';
  import VolumeData from '#lib/VolumeData.svelte';
  import { session } from '#lib/session.svelte.ts';
  import { toYaml } from '#lib/yaml.ts';

  const kind = $derived(page.params.kind ?? '');
  const namespace = $derived(page.params.namespace === '_' ? '' : (page.params.namespace ?? ''));
  const name = $derived(page.params.name ?? '');
  let object: LObject | null = $state(null);
  let error = $state('');
  let gone = $state(false);
  let tab: 'details' | 'rollout' | 'tasks' | 'usage' | 'quota' | 'logs' | 'terminal' | 'data' | 'yaml' =
    $state('details');

  $effect(() => {
    object = null;
    error = '';
    gone = false;
    tab = 'details';
    api
      .get(kind, namespace, name)
      .then((o) => (object = o))
      .catch((e) => (error = e.message));
    // Kept current: the kind's watch, for this one.
    return follow(
      kind,
      namespace,
      (all) => {
        const found = all.find((o) => o.metadata.name === name);
        if (found) object = found;
        gone = !found && object !== null;
      },
      () => {}
    );
  });

  const tabs = $derived([
    { id: 'details', label: 'Details' },
    ...(kind === 'Service' ? [{ id: 'rollout', label: 'Rollout' }] : []),
    ...(kind === 'Node' ? [{ id: 'tasks', label: 'Tasks' }] : []),
    ...(kind === 'Volume' ? [{ id: 'data', label: 'Data' }] : []),
    ...(kind === 'Namespace' ? [{ id: 'quota', label: 'Quota' }] : []),
    ...(kind === 'Task' || kind === 'Service' || kind === 'Node' ? [{ id: 'usage', label: 'Usage' }] : []),
    ...(kind === 'Task'
      ? [
          { id: 'logs', label: 'Logs' },
          { id: 'terminal', label: 'Terminal' }
        ]
      : []),
    { id: 'yaml', label: 'YAML' }
  ] as { id: typeof tab; label: string }[]);
  const cols = $derived(columns(kind));
  const scoped = $derived(session.kinds.find((k) => k.kind === kind)?.clusterScoped ?? false);
</script>

<header class="bar">
  <h1>
    <a href="/k/{kind}">{plural(kind)}</a> / {#if namespace}{namespace} / {/if}{name}
  </h1>
  {#if gone}<span class="state bad">deleted</span>{/if}
</header>

{#if error}
  <p class="error">{error}</p>
{:else if object}
  {#if !gone}<Actions {object} {scoped} />{/if}
  <div class="tabs" role="tablist">
    {#each tabs as t (t.id)}
      <button role="tab" aria-selected={tab === t.id} class:here={tab === t.id} onclick={() => (tab = t.id)}
        >{t.label}</button
      >
    {/each}
  </div>

  {#if tab === 'details'}
    <dl class="facts">
      {#each cols as c (c.label)}
        {#if c.value(object)}
          <dt>{c.label}</dt>
          <dd>
            {#if c.state}<span class="state {tone(c.value(object))}">{c.value(object)}</span>
            {:else}{c.value(object)}{/if}
          </dd>
        {/if}
      {/each}
      {#if kind === 'Task' && object.spec?.service}
        <dt>Service</dt>
        <dd>
          <a href="/k/{object.spec.job ? 'Job' : 'Service'}/{namespace}/{object.spec.service}"
            >{object.spec.service}</a
          >
        </dd>
      {/if}
      {#if kind === 'Task' && object.spec?.node}
        <dt>On node</dt>
        <dd><a href="/k/Node/_/{object.spec.node}">{object.spec.node}</a></dd>
      {/if}
      <dt>Version</dt>
      <dd>{object.metadata.version} (generation {object.metadata.generation})</dd>
      <dt>UID</dt>
      <dd><code>{object.metadata.uid}</code></dd>
      <dt>Labels</dt>
      <dd><Labels {object} {scoped} /></dd>
      {#if kind === 'Node'}
        <dt>Taints</dt>
        <dd><Taints node={object} /></dd>
      {/if}
    </dl>
  {:else if tab === 'tasks'}
    <NodeTasks node={object} />
  {:else if tab === 'quota'}
    <Quotas namespace={name} />
  {:else if tab === 'usage'}
    {#if kind === 'Task'}
      <UsageCharts {namespace} task={name} />
    {:else if kind === 'Service'}
      <UsageCharts {namespace} pick={(t) => t.service === name} />
    {:else}
      <UsageCharts namespace="" pick={(t) => t.node === name} />
    {/if}
  {:else if tab === 'rollout'}
    <Rollout service={object} />
  {:else if tab === 'data'}
    <VolumeData volume={object} />
  {:else if tab === 'logs'}
    <TaskLogs task={object} />
  {:else if tab === 'terminal'}
    <!-- Loaded when opened: the terminal is most of the console's size. -->
    {#await import('#lib/Terminal.svelte') then Terminal}
      <Terminal.default task={object} />
    {/await}
  {:else}
    <pre>{toYaml(object)}</pre>
  {/if}
{:else}
  <p class="muted">…</p>
{/if}

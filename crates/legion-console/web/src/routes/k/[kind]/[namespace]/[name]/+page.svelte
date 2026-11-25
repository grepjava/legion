<script lang="ts">
  import { untrack } from 'svelte';
  import Tabs from '#lib/Tabs.svelte';
  import { page } from '$app/state';
  import { fromAddress, keepInAddress } from '#lib/address.svelte.ts';
  import { api, follow, type LObject } from '#lib/api.ts';
  import { blocking, columns, serviceState, tone } from '#lib/columns.ts';
  import { plural } from '#lib/kinds.ts';
  import { t } from '#lib/i18n.svelte.ts';
  import Actions from '#lib/Actions.svelte';
  import Labels from '#lib/Labels.svelte';
  import Notes, { NOTE } from '#lib/Notes.svelte';
  import LogSearch from '#lib/LogSearch.svelte';
  import NodeTasks from '#lib/NodeTasks.svelte';
  import ServiceTasks from '#lib/ServiceTasks.svelte';
  import NodeHost from '#lib/NodeHost.svelte';
  import Rollout from '#lib/Rollout.svelte';
  import ServiceReplication from '#lib/ServiceReplication.svelte';
  import ServicePorts from '#lib/ServicePorts.svelte';
  import Taints from '#lib/Taints.svelte';
  import TaskLogs from '#lib/TaskLogs.svelte';
  import TaskFiles from '#lib/TaskFiles.svelte';
  import Quotas from '#lib/Quotas.svelte';
  import UsageCharts from '#lib/UsageCharts.svelte';
  import VolumeData from '#lib/VolumeData.svelte';
  import VolumeReplica from '#lib/VolumeReplica.svelte';
  import VolumeProtection from '#lib/VolumeProtection.svelte';
  import BuildLog from '#lib/BuildLog.svelte';
  import Events from '#lib/Events.svelte';
  import { cannotWrite, openWorkbench, session } from '#lib/session.svelte.ts';
  import { toYaml } from '#lib/yaml.ts';
  import Ico from '#lib/Ico.svelte';
  import { kindIcon, toolIcon } from '#lib/icons.ts';

  const kind = $derived(page.params.kind ?? '');
  const namespace = $derived(page.params.namespace === '_' ? '' : (page.params.namespace ?? ''));
  const name = $derived(page.params.name ?? '');
  let object: LObject | null = $state(null);
  let error = $state('');
  let gone = $state(false);
  let tab:
    | 'details'
    | 'rollout'
    | 'replication'
    | 'tasks'
    | 'usage'
    | 'quota'
    | 'search'
    | 'logs'
    | 'files'
    | 'terminal'
    | 'data'
    | 'replica'
    | 'protection'
    | 'buildlog'
    | 'events'
    | 'yaml' =
    $state('details');
  let fix = $state('');
  // The tab open, in the address, so a reload or a link keeps it.
  keepInAddress('tab', () => tab, 'details');

  $effect(() => {
    object = null;
    error = '';
    gone = false;
    // The tab the address names (`?tab=buildlog`), if the kind has it;
    // read as the page opens, not followed (the tab chosen is kept there).
    const wanted = fromAddress('tab');
    // What a fix on the Availability page asks the tab to fill in.
    fix = fromAddress('fix') ?? '';
    // Not followed: a tab that comes or goes with the object (a Volume's
    // Replica) must not read the page again.
    tab = untrack(() => tabs.find((x) => x.id === wanted)?.id ?? 'details');
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

  // A Service that replicates a database, or did, has a page for it.
  const databased = $derived.by(() => {
    const o = object as LObject | null;
    return !!(o?.spec?.replication || o?.status?.replication);
  });
  // A Volume that keeps a replica, or kept one, has a page for it.
  const replicated = $derived.by(() => {
    const o = object as LObject | null;
    return !!(o?.spec?.replica || o?.status?.replica);
  });
  const tabs = $derived([
    { id: 'details', label: t('Details') },
    ...(kind === 'Service' ? [{ id: 'rollout', label: t('Rollout') }] : []),
    ...(kind === 'Service' && databased ? [{ id: 'replication', label: t('Replication') }] : []),
    ...(kind === 'Service' || kind === 'Job' ? [{ id: 'search', label: t('Logs') }] : []),
    ...(kind === 'Node' ? [{ id: 'tasks', label: t('Tasks') }] : []),
    ...(kind === 'Volume' ? [{ id: 'data', label: t('Data') }] : []),
    ...(kind === 'Volume' ? [{ id: 'protection', label: t('Protection') }] : []),
    ...(kind === 'Volume' && replicated ? [{ id: 'replica', label: t('Replica') }] : []),
    ...(kind === 'Namespace' ? [{ id: 'quota', label: t('Quota') }] : []),
    ...(kind === 'Task' || kind === 'Service' || kind === 'Node' ? [{ id: 'usage', label: t('Telemetry') }] : []),
    ...(kind === 'Task'
      ? [
          { id: 'logs', label: t('Logs') },
          { id: 'files', label: t('Files') },
          { id: 'terminal', label: t('Console') }
        ]
      : []),
    ...(kind === 'Build' ? [{ id: 'buildlog', label: t('Log') }] : []),
    // The kinds events are worked out for (legion_api::event::KINDS).
    ...(['Task', 'Service', 'Job', 'Node', 'Volume', 'Build'].includes(kind)
      ? [{ id: 'events', label: t('Events') }]
      : []),
    { id: 'yaml', label: t('YAML') }
  ] as { id: typeof tab; label: string }[]);
  const cols = $derived(columns(kind));
  // What the strip shows of it: a VM's monitor for its kind, and its state.
  const isVm = $derived.by(() => {
    const o = object as LObject | null;
    return (o?.spec?.pod?.containers ?? []).some((c: { vm?: unknown }) => c.vm);
  });
  const objState = $derived.by(() => {
    const o = object as LObject | null;
    const st = o?.status?.state;
    if (typeof st === 'string') return st;
    // A Service has no state of its own: ready when all it wants are.
    if (o?.kind === 'Service') return serviceState(o);
    return '';
  });
  const scoped = $derived(session.kinds.find((k) => k.kind === kind)?.clusterScoped ?? false);
</script>

<!-- The object's strip: what it is, its actions and its views, held at the
     top as the page scrolls, so changing view never moves them. -->
<div class="object-head">
<header class="bar">
  <h1 class="object-title">
    <a class="crumb" href="/k/{kind}"><Ico icon={kindIcon(kind, isVm)} size={20} /><span>{plural(kind)}</span></a>
    <span class="sep" aria-hidden="true">/</span>
    {#if objState}<span class="dot {tone(objState)}" title={objState} aria-hidden="true"></span>{/if}
    <span class="obj-name">{name}</span>
    {#if namespace}<span class="ns-chip" title={t('Namespace')}>{namespace}</span>{/if}
    {#if objState}<span class="sr-only">, {objState}</span>{/if}
  </h1>
  {#if gone}
    <span class="state bad">{t('deleted')}</span>
  {:else if object}
    {@const obj = object}
    {@const vmSpec = (obj.spec?.pod?.containers ?? []).find((c: { vm?: Record<string, unknown> }) => c.vm)?.vm}
    <div class="bar">
      {#if kind === 'Task' || kind === 'Service'}
        <span class="tag">
          {#if vmSpec}
            {t('bhyve VM')} · {vmSpec.cpus ?? 1} vCPU · {vmSpec.memory ?? ''} · {vmSpec.boot === 'uefi' ? t('UEFI (cloud-init)') : t('FreeBSD loader (bhyveload)')}
          {:else}
            {t('FreeBSD Jail')}
          {/if}
        </span>
      {/if}
      {#if kind === 'Task'}
        <button
          type="button"
          class="sm"
          data-look
          onclick={() =>
            openWorkbench({
              id: `logs:${namespace}:${name}`,
              kind: 'logs',
              title: `${name} · logs`,
              namespace,
              name,
              object: obj
            })}><Ico icon={toolIcon('logs')} size={14} />{t('Keep logs open')}</button
        >
        {#if obj.status?.state === 'running'}
          <button
            type="button"
            class="sm"
            disabled={!!cannotWrite(namespace)}
            title={cannotWrite(namespace) ?? ''}
            onclick={() =>
              openWorkbench({
                id: `term:${namespace}:${name}`,
                kind: 'terminal',
                title: `${name} · shell`,
                namespace,
                name,
                object: obj
              })}><Ico icon={toolIcon('terminal')} size={14} />{t('Keep a shell open')}</button
          >
        {/if}
      {:else if kind === 'Build'}
        <button
          type="button"
          class="sm"
          data-look
          onclick={() =>
            openWorkbench({
              id: `build:${namespace}:${name}`,
              kind: 'build',
              title: `${name} · build`,
              namespace,
              name,
              object: obj
            })}><Ico icon={toolIcon('build')} size={14} />{t('Keep build log open')}</button
        >
      {/if}
      {#if ['Task', 'Service', 'Job', 'Node', 'Volume', 'Build'].includes(kind)}
        <button
          type="button"
          class="sm"
          data-look
          onclick={() =>
            openWorkbench({
              id: `events:${kind}:${namespace}:${name}`,
              kind: 'events',
              title: `${name} · events`,
              namespace,
              name,
              targetKind: kind,
              object: obj
            })}><Ico icon={toolIcon('events')} size={14} />{t('Keep events open')}</button
        >
      {/if}
    </div>
  {/if}
</header>

{#if !error && object}
  {#if !gone}<Actions {object} {scoped} />{/if}
  <Tabs {tabs} bind:tab label={t('Views of {name}', { name })} idPrefix="tab" panelId="panel" />
{/if}
</div>

{#if error}
  <p class="error" role="alert">{error}</p>
{:else if object}
  <div role="tabpanel" id="panel" aria-labelledby="tab-{tab}">

  {#if tab === 'details'}
    {#if kind !== 'Config' && kind !== 'Secret'}<Notes {kind} {namespace} {name} />{/if}
    {#if kind === 'Service' && Object.keys(object.status?.linuxImages ?? {}).length}
      <!-- A Linux image runs through FreeBSD's Linux ABI, which not every
           program does well on: the native recipe that does the same from
           FreeBSD's packages, where there is one. -->
      <div class="card moved" data-linux>
        <ul>
          {#each Object.entries(object.status.linuxImages as Record<string, string>) as [image, recipe] (image)}
            <li>
              <span class="state wait">{t('Linux image')}</span>
              {t('{image} runs through FreeBSD\'s Linux ABI.', { image })}
              {#if recipe}
                <a href="/import?template={encodeURIComponent(recipe)}&from=" data-recipe={recipe}
                  >{t('The {recipe} recipe runs it natively, from FreeBSD\'s packages', { recipe })}</a
                >
              {/if}
            </li>
          {/each}
        </ul>
        <p class="muted">
          {t('Native images start faster, are patched with FreeBSD, and avoid what the Linux ABI lacks (docs/LINUX.md).')}
        </p>
      </div>
    {/if}
    {#if kind === 'Service' && Object.keys(object.status?.images?.moved ?? {}).length}
      <div class="card moved" data-moved>
        <ul>
          {#each Object.entries(object.status.images.moved as Record<string, string>) as [image, digest] (image)}
            <li>
              <span class="state wait">{t('Tag moved since the rollout')}</span>
              {t('{image} names {digest} now, not the image its tasks run.', {
                image,
                digest: digest.replace(/^(sha256:.{12}).*$/, '$1…')
              })}
            </li>
          {/each}
        </ul>
        <p class="muted">
          {t('Restart it to run what the tag names now, or name the image by its digest to keep it as it is.')}
        </p>
      </div>
    {/if}
    <dl class="facts card details-card">
      {#each cols as c (c.label)}
        {#if c.value(object)}
          <dt>{c.label}</dt>
          <dd>
            {#if c.state}<span class="state {tone(c.value(object))}">{c.value(object)}</span>
            {:else if c.tone}<span class="state {c.tone(object)}">{c.value(object)}</span>
            {:else if c.href?.(object)}<a href={c.href(object)}>{c.value(object)}</a>
            {:else}{c.value(object)}{/if}
          </dd>
        {/if}
      {/each}
      {#if kind === 'Task' && object.spec?.service}
        <dt>{t('Service')}</dt>
        <dd>
          <a href="/k/{object.spec.job ? 'Job' : 'Service'}/{namespace}/{object.spec.service}"
            >{object.spec.service}</a
          >
        </dd>
      {/if}
      <dt>{t('Version')}</dt>
      <dd>{t('{version} (generation {generation})', { version: object.metadata.version, generation: object.metadata.generation })}</dd>
      <dt>{t('UID')}</dt>
      <dd><code>{object.metadata.uid}</code></dd>
      <dt>{t('Labels')}</dt>
      <dd><Labels {object} {scoped} /></dd>
      {#if kind === 'Config' && object.metadata.labels?.[NOTE]?.includes('/')}
        {@const [onKind, onName] = object.metadata.labels[NOTE].split('/', 2)}
        <dt>{t('A note on')}</dt>
        <dd>
          <a href="/k/{onKind}/{onKind === 'Node' ? '_' : namespace}/{onName}">{object.metadata.labels[NOTE]}</a>
        </dd>
      {/if}
      {#if kind === 'Node'}
        <dt>{t('Taints')}</dt>
        <dd><Taints node={object} /></dd>
        <dt>{t('Host')}</dt>
        <dd>
          {#if object.status?.lacks?.length}
            {#if blocking(object).length}
              <p class="bad">{t('Takes no new tasks until this is put right; what runs here stays.')}</p>
            {/if}
            <ul class="lacks">
              {#each object.status.lacks as l (l.id)}
                <li>
                  <code>{l.id}</code>
                  {l.need === 'required' || object.spec?.require?.includes(l.id) ? t('required') : t('advised')},
                  {l.state}: {l.detail}
                  {#if l.remedy}<br /><small>{t('fix: {remedy}', { remedy: l.remedy })}</small>{/if}
                </li>
              {/each}
            </ul>
          {:else}
            {t('has what Legion needs')}
          {/if}
          {#if object.spec?.require?.length}
            <br /><small>{t('Also required here: {ids}', { ids: object.spec.require.join(', ') })}</small>
          {/if}
        </dd>
      {/if}
    </dl>
    {#if kind === 'Service' || kind === 'Job'}<ServiceTasks owner={object} />{/if}
    {#if kind === 'Node'}<NodeHost node={object} />{/if}
    {#if kind === 'Service'}<ServicePorts service={object} />{/if}
  {:else if tab === 'buildlog'}
    <BuildLog build={object} />
  {:else if tab === 'events'}
    <Events {kind} {namespace} {name} />
  {:else if tab === 'tasks'}
    <NodeTasks node={object} />
  {:else if tab === 'search'}
    <LogSearch {namespace} service={name} />
  {:else if tab === 'quota'}
    <Quotas namespace={name} />
  {:else if tab === 'usage'}
    {#if kind === 'Task'}
      <UsageCharts {namespace} task={name} />
    {:else if kind === 'Service'}
      <UsageCharts {namespace} pick={(t) => t.service === name} />
    {:else}
      <UsageCharts namespace="" nodeFilter={name} pick={(t) => t.node === name} />
    {/if}
  {:else if tab === 'rollout'}
    <Rollout service={object} />
  {:else if tab === 'replication'}
    <ServiceReplication service={object} />
  {:else if tab === 'data'}
    <VolumeData volume={object} />
  {:else if tab === 'replica'}
    <VolumeReplica volume={object} />
  {:else if tab === 'protection'}
    <VolumeProtection volume={object} prefill={fix} />
  {:else if tab === 'logs'}
    <TaskLogs task={object} />
  {:else if tab === 'files'}
    <TaskFiles task={object} />
  {:else if tab === 'terminal'}
    <!-- Loaded when opened: the terminal is most of the console's size. -->
    {#await import('#lib/Terminal.svelte') then Terminal}
      <Terminal.default task={object} />
    {/await}
  {:else}
    <pre>{toYaml(object)}</pre>
  {/if}
  </div>
{:else}
  <p class="muted">…</p>
{/if}

<style>
  .moved {
    margin-bottom: 1rem;
  }
  .moved ul {
    margin: 0 0 0.35rem;
    padding-left: 1rem;
  }
</style>

<script lang="ts">
  // What a user can do to an object, as the CLI does it: edit, delete, and
  // each kind's own actions (scale and roll back a Service, suspend a
  // CronJob, drain or promote a Node, move a Volume). For a user who may
  // only look, the buttons are there, disabled, with the reason.
  import { goto } from '$app/navigation';
  import { api, type LObject } from './api.ts';
  import { cannotWrite } from './session.svelte.ts';
  import { t } from './i18n.svelte.ts';

  let { object, scoped }: { object: LObject; scoped: boolean } = $props();
  const kind = $derived(object.kind);
  const ns = $derived(object.metadata.namespace ?? '');
  const name = $derived(object.metadata.name);
  const why = $derived(cannotWrite(scoped ? '' : ns));
  const label = $derived(`${kind} ${ns ? `${ns}/` : ''}${name}`);
  let message = $state('');
  let error = $state('');
  let busy = $state(false);
  let replicas: number = $state(0);
  let moveTo = $state('');

  // Follows the stored count, not every new status of the Service.
  const stored = $derived(Number(object.spec?.replicas ?? 0));
  $effect(() => {
    replicas = stored;
  });

  async function act(what: () => Promise<string>) {
    busy = true;
    error = '';
    message = '';
    try {
      message = await what();
    } catch (e) {
      error = (e as Error).message;
    } finally {
      busy = false;
    }
  }

  const set = (field: string, value: unknown, done: string, same: string) =>
    act(async () => ((await api.set(kind, ns, name, field, value)).changed ? done : same));

  // What goes with it, said before it goes.
  const consequences = (): Record<string, string> => ({
    Service: t('Its tasks are stopped and removed.'),
    Job: t('Its tasks are stopped and removed.'),
    CronJob: t('It makes no more runs.'),
    Node: t('It leaves the cluster: its tasks are placed elsewhere, and it has to join again to come back.'),
    Volume: t('Its data is destroyed. The API refuses while a task uses it.'),
    Namespace: t('What is in it goes too.')
  });

  async function remove() {
    const consequence = consequences()[kind];
    const ask = consequence
      ? t('Delete {label}? {consequence}', { label, consequence })
      : t('Delete {label}?', { label });
    if (!confirm(ask)) return;
    await act(async () => {
      await api.remove(kind, ns, name, object.metadata.version);
      await goto(`/k/${kind}`);
      return t('{label} deleted', { label });
    });
  }

  function availability(to: 'active' | 'pause' | 'drain') {
    const done = {
      active: t('{name} activated', { name }),
      pause: t('{name} paused', { name }),
      drain: t('{name} draining', { name })
    }[to];
    if (to === 'drain' && !confirm(t('Drain {name}? Its tasks move to other nodes.', { name }))) return;
    return set('availability', to, done, t('{name} is already {to}', { name, to }));
  }

  function role(to: 'manager' | 'worker') {
    const ask =
      to === 'manager'
        ? t('Promote {name} to a manager? Its daemon takes the role up and starts again.', { name })
        : t('Demote {name} to a worker? Its daemon takes the role up and starts again.', { name });
    if (!confirm(ask)) return;
    return act(async () => {
      await api.nodeRole(name, to);
      return to === 'manager' ? t('{name} becomes a manager', { name }) : t('{name} becomes a worker', { name });
    });
  }
</script>

<div class="actions">
  {#if why}
    <button disabled title={why}>{t('Edit YAML')}</button>
  {:else}
    <a
      class="button"
      href="/apply?kind={encodeURIComponent(kind)}&namespace={encodeURIComponent(ns)}&name={encodeURIComponent(name)}"
      >{t('Edit YAML')}</a
    >
  {/if}

  {#if kind === 'Service'}
    <button
      disabled={!!why}
      title={why ?? ''}
      onclick={() =>
        goto(`/build?namespace=${encodeURIComponent(ns)}&service=${encodeURIComponent(name)}`)}
      >{t('Build a new image')}</button
    >
    <span class="group">
      <input type="number" min="0" bind:value={replicas} aria-label={t('Replicas')} disabled={!!why} />
      <button
        disabled={!!why || busy}
        title={why ?? ''}
        onclick={() =>
          set(
            'replicas',
            replicas,
            t('{name} scaled to {n}', { name, n: replicas }),
            t('{name} already has {n}', { name, n: replicas })
          )}
        >{t('Scale')}</button
      >
    </span>
    <button
      disabled={!!why || busy || !object.status?.update}
      title={why ?? (object.status?.update ? t('Back to the spec before the latest update') : t('No update to roll back'))}
      onclick={() =>
        confirm(t('Roll {name} back to the spec before its latest update?', { name })) &&
        act(async () => (await api.rollback(ns, name)).message)}>{t('Roll back')}</button
    >
  {:else if kind === 'CronJob'}
    <button
      disabled={!!why || busy}
      title={why ?? ''}
      onclick={() =>
        object.spec?.suspend
          ? set('suspend', false, t('{name} resumed', { name }), t('{name} already resumed', { name }))
          : set('suspend', true, t('{name} suspended', { name }), t('{name} already suspended', { name }))}
      >{object.spec?.suspend ? t('Resume') : t('Suspend')}</button
    >
  {:else if kind === 'Node'}
    {@const now = object.spec?.availability ?? 'active'}
    <span class="group">
      {#each ['active', 'pause', 'drain'] as const as to (to)}
        <button disabled={!!why || busy || now === to} title={why ?? ''} onclick={() => availability(to)}
          >{{ active: t('Activate'), pause: t('Pause'), drain: t('Drain') }[to]}</button
        >
      {/each}
    </span>
    {#if object.spec?.role === 'manager'}
      <button disabled={!!why || busy} title={why ?? ''} onclick={() => role('worker')}>{t('Demote')}</button>
    {:else}
      <button disabled={!!why || busy} title={why ?? ''} onclick={() => role('manager')}>{t('Promote')}</button>
    {/if}
  {:else if kind === 'Volume'}
    <span class="group">
      <input placeholder={t('node')} bind:value={moveTo} aria-label={t('Move to node')} disabled={!!why} />
      <button
        disabled={!!why || busy || !moveTo}
        title={why ?? t('Moves its data to that node, stopping its tasks meanwhile')}
        onclick={() =>
          confirm(t('Move {name} to {node}? Its tasks stop while its data moves.', { name, node: moveTo })) &&
          set(
            'node',
            moveTo,
            t('{name} moves to {node}', { name, node: moveTo }),
            t('{name} is already asked to be on {node}', { name, node: moveTo })
          )}
        >{t('Move')}</button
      >
    </span>
  {/if}

  <button class="danger" disabled={!!why || busy} title={why ?? ''} onclick={remove}>{t('Delete')}</button>
</div>
{#if why}<p class="muted">{t('Read only: {why}.', { why })}</p>{/if}
{#if error}<p class="error" role="alert">{error}</p>{/if}
{#if message}<p class="ok" role="status">{message}</p>{/if}

<script lang="ts">
  // What a user can do to an object, as the CLI does it: edit, delete, and
  // each kind's own actions (scale, restart and roll back a Service; run
  // a CronJob now or suspend it; drain or promote a Node, move a Volume). For a user who may
  // only look, the buttons are there, disabled, with the reason.
  import { goto } from '$app/navigation';
  import { api, type LObject } from './api.ts';
  import { cannotWrite } from './session.svelte.ts';
  import { t } from './i18n.svelte.ts';
  import { ask } from './confirm.svelte.ts';

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
    const ask_ = consequence
      ? t('Delete {label}? {consequence}', { label, consequence })
      : t('Delete {label}?', { label });
    // What can't be come back from asks for its name typed.
    const lasting = ['Volume', 'Namespace', 'Node'].includes(kind);
    if (!(await ask(ask_, { action: t('Delete'), typed: lasting ? name : undefined }))) return;
    await act(async () => {
      await api.remove(kind, ns, name, object.metadata.version);
      await goto(`/k/${kind}`);
      return t('{label} deleted', { label });
    });
  }

  async function availability(to: 'active' | 'pause' | 'drain') {
    const done = {
      active: t('{name} activated', { name }),
      pause: t('{name} paused', { name }),
      drain: t('{name} draining', { name })
    }[to];
    if (to === 'drain' && !(await ask(t('Drain {name}? Its tasks move to other nodes.', { name }), { action: t('Drain') })))
      return;
    return set('availability', to, done, t('{name} is already {to}', { name, to }));
  }

  async function role(to: 'manager' | 'worker') {
    const what =
      to === 'manager'
        ? t('Promote {name} to a manager? Its daemon takes the role up and starts again.', { name })
        : t('Demote {name} to a worker? Its daemon takes the role up and starts again.', { name });
    if (!(await ask(what, { action: to === 'manager' ? t('Promote') : t('Demote') }))) return;
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
      onclick={async () =>
        (await ask(t('Roll {name} back to the spec before its latest update?', { name }), { action: t('Roll back') })) &&
        act(async () => (await api.rollback(ns, name)).message)}>{t('Roll back')}</button
    >
    <button
      disabled={!!why || busy}
      title={why ?? t('New tasks on the same spec, replacing these as an update does')}
      onclick={async () =>
        (await ask(t('Restart {name}? Its tasks are replaced, as an update replaces them.', { name }), {
          action: t('Restart')
        })) &&
        act(async () => (await api.restart(ns, name)).message)}>{t('Restart')}</button
    >
    <button
      disabled={!!why || busy}
      title={why ??
        (object.spec?.stopped
          ? t('Run its tasks again, as the spec says')
          : t('No tasks until it is started; the spec stays'))}
      onclick={async () =>
        object.spec?.stopped
          ? set('stopped', false, t('{name} started', { name }), t('{name} already started', { name }))
          : (await ask(t('Stop {name}? Its tasks go until it is started.', { name }), { action: t('Stop') })) &&
            set('stopped', true, t('{name} stopped', { name }), t('{name} already stopped', { name }))}
      >{object.spec?.stopped ? t('Start') : t('Stop')}</button
    >
    <button
      disabled={!!why || busy || (!!object.spec?.stopped && !object.spec?.paused)}
      title={why ??
        (object.spec?.paused
          ? t('Let its tasks go on where they were')
          : object.spec?.stopped
            ? t('Stopped: it has no tasks to pause')
            : t('Freeze its tasks where they are, out of its endpoints, until resumed'))}
      onclick={async () =>
        object.spec?.paused
          ? set('paused', false, t('{name} resumed', { name }), t('{name} already resumed', { name }))
          : (await ask(t('Pause {name}? Its tasks are frozen and out of its endpoints until it is resumed.', { name }), {
              action: t('Pause')
            })) &&
            set('paused', true, t('{name} paused', { name }), t('{name} already paused', { name }))}
      >{object.spec?.paused ? t('Resume') : t('Pause')}</button
    >
  {:else if kind === 'CronJob'}
    <button
      disabled={!!why || busy}
      title={why ?? t('A run now, outside its schedule')}
      onclick={() => act(async () => (await api.runNow(ns, name)).message)}>{t('Run now')}</button
    >
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
        onclick={async () =>
          (await ask(t('Move {name} to {node}? Its tasks stop while its data moves.', { name, node: moveTo }), {
            action: t('Move'),
            danger: true
          })) &&
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

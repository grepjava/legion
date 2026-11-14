<script lang="ts">
  // What a user can do to an object, as the CLI does it: edit, delete, and
  // each kind's own actions (scale and roll back a Service, suspend a
  // CronJob, drain or promote a Node, move a Volume). For a user who may
  // only look, the buttons are there, disabled, with the reason.
  import { goto } from '$app/navigation';
  import { api, type LObject } from './api.ts';
  import { cannotWrite } from './session.svelte.ts';

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
  const consequences: Record<string, string> = {
    Service: 'Its tasks are stopped and removed.',
    Job: 'Its tasks are stopped and removed.',
    CronJob: 'It makes no more runs.',
    Node: 'It leaves the cluster: its tasks are placed elsewhere, and it has to join again to come back.',
    Volume: 'Its data is destroyed. The API refuses while a task uses it.',
    Namespace: 'What is in it goes too.'
  };

  async function remove() {
    if (!confirm(`Delete ${label}? ${consequences[kind] ?? ''}`.trim())) return;
    await act(async () => {
      await api.remove(kind, ns, name, object.metadata.version);
      await goto(`/k/${kind}`);
      return `${label} deleted`;
    });
  }

  function availability(to: 'active' | 'pause' | 'drain') {
    const what = { active: 'activated', pause: 'paused', drain: 'draining' }[to];
    if (to === 'drain' && !confirm(`Drain ${name}? Its tasks move to other nodes.`)) return;
    return set('availability', to, `${name} ${what}`, `${name} is already ${to}`);
  }

  function role(to: 'manager' | 'worker') {
    const doing = to === 'manager' ? 'Promote' : 'Demote';
    if (!confirm(`${doing} ${name} to a ${to}? Its daemon takes the role up and starts again.`)) return;
    return act(async () => {
      await api.nodeRole(name, to);
      return `${name} becomes a ${to}`;
    });
  }
</script>

<div class="actions">
  {#if why}
    <button disabled title={why}>Edit YAML</button>
  {:else}
    <a
      class="button"
      href="/apply?kind={encodeURIComponent(kind)}&namespace={encodeURIComponent(ns)}&name={encodeURIComponent(name)}"
      >Edit YAML</a
    >
  {/if}

  {#if kind === 'Service'}
    <span class="group">
      <input type="number" min="0" bind:value={replicas} aria-label="Replicas" disabled={!!why} />
      <button
        disabled={!!why || busy}
        title={why ?? ''}
        onclick={() => set('replicas', replicas, `${name} scaled to ${replicas}`, `${name} already has ${replicas}`)}
        >Scale</button
      >
    </span>
    <button
      disabled={!!why || busy || !object.status?.update}
      title={why ?? (object.status?.update ? 'Back to the spec before the latest update' : 'No update to roll back')}
      onclick={() =>
        confirm(`Roll ${name} back to the spec before its latest update?`) &&
        act(async () => (await api.rollback(ns, name)).message)}>Roll back</button
    >
  {:else if kind === 'CronJob'}
    <button
      disabled={!!why || busy}
      title={why ?? ''}
      onclick={() =>
        object.spec?.suspend
          ? set('suspend', false, `${name} resumed`, `${name} already resumed`)
          : set('suspend', true, `${name} suspended`, `${name} already suspended`)}
      >{object.spec?.suspend ? 'Resume' : 'Suspend'}</button
    >
  {:else if kind === 'Node'}
    {@const now = object.spec?.availability ?? 'active'}
    <span class="group">
      {#each ['active', 'pause', 'drain'] as const as to (to)}
        <button disabled={!!why || busy || now === to} title={why ?? ''} onclick={() => availability(to)}
          >{{ active: 'Activate', pause: 'Pause', drain: 'Drain' }[to]}</button
        >
      {/each}
    </span>
    {#if object.spec?.role === 'manager'}
      <button disabled={!!why || busy} title={why ?? ''} onclick={() => role('worker')}>Demote</button>
    {:else}
      <button disabled={!!why || busy} title={why ?? ''} onclick={() => role('manager')}>Promote</button>
    {/if}
  {:else if kind === 'Volume'}
    <span class="group">
      <input placeholder="node" bind:value={moveTo} aria-label="Move to node" disabled={!!why} />
      <button
        disabled={!!why || busy || !moveTo}
        title={why ?? 'Moves its data to that node, stopping its tasks meanwhile'}
        onclick={() =>
          confirm(`Move ${name} to ${moveTo}? Its tasks stop while its data moves.`) &&
          set('node', moveTo, `${name} moves to ${moveTo}`, `${name} is already asked to be on ${moveTo}`)}
        >Move</button
      >
    </span>
  {/if}

  <button class="danger" disabled={!!why || busy} title={why ?? ''} onclick={remove}>Delete</button>
</div>
{#if why}<p class="muted">Read only: {why}.</p>{/if}
{#if error}<p class="error" role="alert">{error}</p>{/if}
{#if message}<p class="ok" role="status">{message}</p>{/if}

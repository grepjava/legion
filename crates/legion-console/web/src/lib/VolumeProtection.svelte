<script lang="ts">
  // What keeps a Volume's data safe, in a form rather than its YAML: a
  // replica on another node (how often, failing over by hand or on its
  // own, where), snapshot schedules, backups to a target, and its size.
  // The change is reviewed before it is saved (SpecChange), with what it
  // would do said under it. Backup targets are made on their own page;
  // here one is picked.
  import { api, follow, type LObject } from '#lib/api.ts';
  import { cannotWrite, session } from '#lib/session.svelte.ts';
  import { t } from '#lib/i18n.svelte.ts';
  import SpecChange from '#lib/SpecChange.svelte';

  let { volume, prefill = '' }: { volume: LObject; prefill?: string } = $props();

  type Schedule = { every: string; keep: number };
  type Replica = { node?: string; everySeconds?: number; failover?: string };
  type Backup = { target: string; every: string; keep: number };

  const EVERY = [10, 30, 60, 300, 900, 3600];
  const zfs = $derived(!volume.spec?.driver || volume.spec.driver === 'zfs');
  const why = $derived(cannotWrite(volume.metadata.namespace ?? ''));

  // The form, filled from the Volume as read and again when it changes
  // under it (its version), unless the form was changed meanwhile.
  let replicaOn = $state(false);
  let every = $state(300);
  let failover = $state('manual');
  let replicaNode = $state('');
  let schedules: Schedule[] = $state([]);
  let backupOn = $state(false);
  let target = $state('');
  let backupEvery = $state('1d');
  let backupKeep = $state(7);
  let size = $state('');
  let filledFrom = -1;
  $effect(() => {
    const v = volume.metadata.version ?? 0;
    if (v === filledFrom) return;
    filledFrom = v;
    const r = volume.spec?.replica as Replica | undefined;
    replicaOn = !!r;
    every = r?.everySeconds ?? 300;
    failover = r?.failover ?? 'manual';
    replicaNode = r?.node ?? '';
    schedules = ((volume.spec?.snapshots ?? []) as Schedule[]).map((s) => ({ ...s }));
    const b = volume.spec?.backup as Backup | undefined;
    backupOn = !!b;
    target = b?.target ?? '';
    backupEvery = b?.every ?? '1d';
    backupKeep = b?.keep ?? 7;
    size = volume.spec?.size ?? '';
    // Asked for from the Availability page: what its fix needs.
    if (prefill === 'replica' && !r) {
      replicaOn = true;
      every = 30;
      failover = 'automatic';
    } else if (prefill === 'automatic' && r) {
      failover = 'automatic';
    }
  });

  // What there is to pick from: the nodes (for who may read them) and the
  // backup targets.
  let nodes: string[] = $state([]);
  let targets: string[] = $state([]);
  $effect(() => {
    if (session.me?.all)
      api
        .list('Node')
        .then((r) => (nodes = r.objects.map((n) => n.metadata.name).sort()))
        .catch(() => {});
    api
      .list('BackupTarget')
      .then((r) => (targets = r.objects.map((o) => o.metadata.name).sort()))
      .catch(() => (targets = []));
  });

  // The Services whose pods use it, for what automatic failover asks of
  // them.
  let users: LObject[] = $state([]);
  $effect(() =>
    follow(
      'Service',
      volume.metadata.namespace ?? '',
      (all) =>
        (users = all.filter((s) =>
          ((s.spec?.pod?.volumes ?? []) as { volume?: { name: string } }[]).some(
            (v) => v.volume?.name === volume.metadata.name
          )
        )),
      () => (users = [])
    )
  );
  const running = $derived(users.filter((s) => s.spec?.pod?.onPartition !== 'stop'));

  const holder = $derived((volume.status?.node as string | undefined) ?? volume.spec?.node ?? '');
  /** The node holding the data, in words. */
  const there = $derived(holder ? t('node {node}', { node: holder }) : t('its node'));
  const everyBad = $derived(replicaOn && (!Number.isInteger(every) || every < 10));
  const keepBad = $derived(
    schedules.some((s) => !s.every.trim() || !Number.isInteger(s.keep) || s.keep < 1 || s.keep > 1000) ||
      (backupOn && (!target || !backupEvery.trim() || backupKeep < 1))
  );

  // The spec as the form has it.
  const spec = $derived.by(() => {
    const next = { ...$state.snapshot(volume.spec) } as Record<string, unknown>;
    if (replicaOn) {
      const r: Replica = { everySeconds: every };
      if (failover === 'automatic') r.failover = 'automatic';
      if (replicaNode) r.node = replicaNode;
      next.replica = r;
    } else delete next.replica;
    if (schedules.length) next.snapshots = schedules.map((s) => ({ every: s.every.trim(), keep: s.keep }));
    else delete next.snapshots;
    if (backupOn) next.backup = { target, every: backupEvery.trim(), keep: backupKeep };
    else delete next.backup;
    if (size.trim()) next.size = size.trim();
    return next;
  });

  // What the change would do.
  const notes = $derived.by(() => {
    const out: string[] = [];
    const was = volume.spec?.replica as Replica | undefined;
    const where = replicaNode || t('a node the leader picks');
    if (replicaOn && !was) out.push(t('The first copy to {node} is whole: all of its data.', { node: where }));
    if (!replicaOn && was)
      out.push(t('Its replica is dropped: if {node} is lost, so is the data.', { node: there }));
    if (replicaOn && was && (was.node ?? '') !== replicaNode)
      out.push(t('A new whole copy to {node}; the old copy is dropped.', { node: where }));
    if (replicaOn && failover === 'automatic' && running.length)
      out.push(
        t('Refused while {services} may run on cut off: set onPartition: stop on them first (below).', {
          services: running.map((s) => s.metadata.name).join(', ')
        })
      );
    if (replicaOn) out.push(t('Up to {n} s of writes may be lost with {node}.', { n: every, node: there }));
    const wasBackup = volume.spec?.backup as Backup | undefined;
    if (!backupOn && wasBackup) out.push(t('No more backups are taken; those on {target} stay.', { target: wasBackup.target }));
    if ((volume.spec?.size ?? '') !== size.trim() && size.trim())
      out.push(t('A smaller size than its data is refused by its node.'));
    if (schedules.length) out.push(t("Snapshots count against the Volume's size."));
    return out;
  });

  /** A Service made to stop when its node is cut off. */
  function stopping(s: LObject): unknown {
    const pod = { ...$state.snapshot(s.spec.pod), onPartition: 'stop' };
    return { ...$state.snapshot(s.spec), pod };
  }
</script>

{#if !zfs}
  <p class="muted">
    {t('Its data is on {driver} storage, as available as what serves it: nothing to set here.', {
      driver: volume.spec.driver
    })}
  </p>
{:else}
  <form class="protection" onsubmit={(e) => e.preventDefault()}>
    <fieldset>
      <legend>{t('Replica')}</legend>
      <p class="muted">
        {t('A copy on another node, sent as often as asked: the most that can be lost if {node} is.', {
          node: there
        })}
      </p>
      <label class="check"><input type="checkbox" bind:checked={replicaOn} disabled={!!why} /> {t('Keep a replica')}</label>
      {#if replicaOn}
        <label>
          {t('Copy every (seconds, at least 10)')}
          <input type="number" min="10" step="1" bind:value={every} list="replica-every" aria-invalid={everyBad} disabled={!!why} />
          <datalist id="replica-every">
            {#each EVERY as s (s)}<option value={s}></option>{/each}
          </datalist>
        </label>
        {#if everyBad}<p class="error">{t('At least 10 seconds.')}</p>{/if}
        <label>
          {t('If {node} is lost', { node: there })}
          <select bind:value={failover} disabled={!!why}>
            <option value="manual">{t('Fail over by hand (legion volume promote)')}</option>
            <option value="automatic">{t('Fail over on its own')}</option>
          </select>
        </label>
        <label>
          {t('Keep it on')}
          <select bind:value={replicaNode} disabled={!!why}>
            <option value="">{t('A node the leader picks')}</option>
            {#each nodes.filter((n) => n !== holder) as n (n)}<option value={n}>{n}</option>{/each}
            {#if replicaNode && !nodes.includes(replicaNode)}<option value={replicaNode}>{replicaNode}</option>{/if}
          </select>
        </label>
        {#if failover === 'automatic' && running.length}
          <div class="callout">
            <p>
              {t(
                'Failing over on its own needs every task using it to stop when its node is cut off, so the old copy takes no writes after the new one does. Setting it rolls the Service out again.'
              )}
            </p>
            {#each running as s (s.metadata.name)}
              <p><strong>{s.metadata.name}</strong></p>
              <SpecChange
                object={s}
                spec={stopping(s)}
                save={t('Set onPartition: stop on {service}', { service: s.metadata.name })}
                notes={[t('Its tasks are replaced, one at a time as its update allows.')]}
              />
            {/each}
          </div>
        {/if}
      {/if}
    </fieldset>

    <fieldset>
      <legend>{t('Snapshots')}</legend>
      {#each schedules as s, i (i)}
        <div class="row">
          <label>{t('Every')} <input bind:value={s.every} placeholder="1h" size="6" disabled={!!why} /></label>
          <label>{t('Keep')} <input type="number" min="1" max="1000" bind:value={s.keep} disabled={!!why} /></label>
          <button type="button" class="sm" onclick={() => schedules.splice(i, 1)} disabled={!!why}>{t('Remove')}</button>
        </div>
      {:else}
        <p class="muted">{t('None scheduled.')}</p>
      {/each}
      <div class="row">
        <button type="button" class="sm" onclick={() => schedules.push({ every: '1h', keep: 24 })} disabled={!!why || schedules.length >= 8}>
          {t('Add a schedule')}
        </button>
        <button
          type="button"
          class="sm"
          onclick={() => (schedules = [{ every: '1h', keep: 24 }, { every: '1d', keep: 30 }])}
          disabled={!!why}>{t('Hourly for a day, daily for a month')}</button
        >
      </div>
    </fieldset>

    <fieldset>
      <legend>{t('Backups')}</legend>
      <label class="check"><input type="checkbox" bind:checked={backupOn} disabled={!!why} /> {t('Back it up to a target')}</label>
      {#if backupOn}
        {#if targets.length}
          <label>
            {t('Target')}
            <select bind:value={target} disabled={!!why}>
              <option value="" disabled>{t('Pick one')}</option>
              {#each targets as name (name)}<option value={name}>{name}</option>{/each}
            </select>
          </label>
        {:else}
          <p class="muted">{t('No backup target yet.')} <a href="/backup-targets">{t('Add one')}</a></p>
        {/if}
        <div class="row">
          <label>{t('Every')} <input bind:value={backupEvery} placeholder="1d" size="6" disabled={!!why} /></label>
          <label>{t('Keep')} <input type="number" min="1" bind:value={backupKeep} disabled={!!why} /></label>
        </div>
      {/if}
    </fieldset>

    <fieldset>
      <legend>{t('Size')}</legend>
      <label>{t('Size')} <input bind:value={size} placeholder="20Gi" size="8" disabled={!!why} /></label>
    </fieldset>

    {#if why}
      <p class="muted">{why}</p>
    {:else}
      <SpecChange object={volume} {spec} {notes} disabled={everyBad || keepBad} />
    {/if}
  </form>
{/if}

<style>
  .protection fieldset {
    border: 1px solid var(--line);
    border-radius: 8px;
    padding: 0.75rem 1rem;
    margin: 0 0 1rem;
    display: grid;
    gap: 0.5rem;
    max-width: 44rem;
  }
  .protection legend {
    font-weight: 600;
    padding: 0 0.25rem;
  }
  .protection label {
    display: grid;
    gap: 0.25rem;
  }
  .protection label.check {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }
  .protection .row {
    display: flex;
    gap: 0.75rem;
    align-items: end;
    flex-wrap: wrap;
  }
  .callout {
    border-left: 3px solid var(--primary);
    padding-left: 0.75rem;
  }
</style>

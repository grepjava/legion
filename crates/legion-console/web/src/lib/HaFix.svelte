<script lang="ts">
  // How to put one single point of failure right (`legion ha`), from the
  // Availability page: a Volume's on its Protection tab, filled in for the
  // fix; a Service's in a small form here, reviewed before it is saved
  // (SpecChange); the cluster's as the commands that do it.
  import { api, type HaFinding, type LObject } from '#lib/api.ts';
  import { cannotWrite } from '#lib/session.svelte.ts';
  import { t } from '#lib/i18n.svelte.ts';
  import SpecChange from '#lib/SpecChange.svelte';

  let { finding, onsaved = () => {} }: { finding: HaFinding; onsaved?: () => void } = $props();

  const parts = $derived(/^(\w+) ([^/]+)\/(.+)$/.exec(finding.object ?? ''));
  const kind = $derived(parts?.[1] ?? '');
  const namespace = $derived(parts?.[2] ?? '');
  const name = $derived(parts?.[3] ?? '');
  const why = $derived(cannotWrite(finding.namespace ?? ''));
  const volumeHref = (fix: string) =>
    `/k/Volume/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}?tab=protection&fix=${fix}`;

  // A Service's fix: read as it is, to change from there.
  let service: LObject | null = $state(null);
  let error = $state('');
  $effect(() => {
    if (kind !== 'Service') return;
    api
      .get('Service', namespace, name)
      .then((s) => (service = s))
      .catch((e) => (error = e.message));
  });

  let replicas = $state(2);
  let required = $state(false);
  let address = $state('');
  let vhid = $state(1);

  const spec = $derived.by(() => {
    if (!service) return null;
    const s = { ...$state.snapshot(service.spec) } as Record<string, any>;
    if (finding.reason === 'one-replica') {
      if (s.autoscale) s.autoscale = { ...s.autoscale, min: Math.max(replicas, 2) };
      else s.replicas = replicas;
    } else if (finding.reason === 'one-node') {
      const placement = { ...(s.placement ?? {}) };
      const anti = ((placement.antiAffinity ?? []) as { service: string; required?: boolean }[]).filter(
        (a) => a.service !== name
      );
      placement.antiAffinity = [...anti, { service: name, ...(required ? { required: true } : {}) }];
      s.placement = placement;
    } else if (finding.reason === 'published') {
      s.endpoint = { ...(s.endpoint ?? {}), loadBalancer: { address: address.trim(), vhid } };
    }
    return s;
  });
  const addressBad = $derived(
    finding.reason === 'published' && !/^(\d{1,3}\.){3}\d{1,3}$/.test(address.trim())
  );
  const notes = $derived.by(() => {
    switch (finding.reason) {
      case 'one-replica':
        return [t('More tasks start beside the one there is; each needs its requests met on some node.')];
      case 'one-node':
        return required
          ? [t('At most one task per node: tasks beyond the nodes there are wait.')]
          : [t('Preferred, not required: new tasks go to other nodes where they can, and running ones stay.')];
      case 'published':
        return [
          t('The address is held by one ready node at a time, moved by CARP if it is lost; it must be free on the nodes’ network.')
        ];
      default:
        return [];
    }
  });
</script>

<div class="ha-fix">
  {#if kind === 'Volume'}
    {#if finding.reason === 'manual-failover'}
      <a class="button sm" href={volumeHref('automatic')}>{t('Fail over on its own')}</a>
    {:else}
      <a class="button sm" href={volumeHref('replica')}>{t('Add a replica')}</a>
    {/if}
  {:else if kind === 'Service'}
    {#if why}
      <p class="muted">{why}</p>
    {:else if error}
      <p class="error">{error}</p>
    {:else if service && spec}
      {#if finding.reason === 'one-replica'}
        {#if service.spec?.mode === 'stateful'}
          <p class="muted">
            {t('Each replica of a stateful service has its own data: a second one helps only if the application replicates between them.')}
          </p>
        {/if}
        <label>{t('Replicas')} <input type="number" min="2" bind:value={replicas} /></label>
      {:else if finding.reason === 'one-node'}
        <label class="check">
          <input type="checkbox" bind:checked={required} />
          {t('Never two on one node (otherwise only preferred)')}
        </label>
      {:else if finding.reason === 'published'}
        <div class="row">
          <label>{t('Load balancer address')} <input bind:value={address} placeholder="192.168.1.250" aria-invalid={addressBad} /></label>
          <label>{t('CARP vhid (1 to 255)')} <input type="number" min="1" max="255" bind:value={vhid} /></label>
        </div>
      {/if}
      <SpecChange object={service} {spec} {notes} disabled={addressBad || replicas < 2} {onsaved} />
    {:else}
      <p class="muted" aria-busy="true">{t('Reading it…')}</p>
    {/if}
  {:else if finding.reason === 'managers' || finding.reason === 'leaders'}
    <p>{t('On a manager, make a token; then join a third manager, or a witness on any small FreeBSD machine both reach:')}</p>
    <pre class="mono">legion join-token manager
legion join --witness --manager MANAGER:7470 TOKEN   # {t('on the witness')}
service legion start</pre>
    <p>{t('Or make a worker a full manager in place, its tasks running on:')}</p>
    <pre class="mono">legion node promote NODE</pre>
  {:else if finding.reason === 'nodes'}
    <p>{t('Join another worker, or make a drained or paused node active again:')}</p>
    <pre class="mono">legion join-token worker
legion join --manager MANAGER:7470 TOKEN   # {t('on the new node')}</pre>
    <a class="button sm" href="/k/Node">{t('Nodes')}</a>
  {/if}
</div>

<style>
  .ha-fix {
    display: grid;
    gap: 0.5rem;
    max-width: 44rem;
  }
  .ha-fix label {
    display: grid;
    gap: 0.25rem;
  }
  .ha-fix label.check {
    display: flex;
    gap: 0.5rem;
    align-items: center;
  }
  .ha-fix .row {
    display: flex;
    gap: 0.75rem;
    flex-wrap: wrap;
  }
</style>

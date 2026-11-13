<script lang="ts">
  // Namespaces' quotas and what each uses of them now (`legion quota`):
  // a bar per resource the quota limits, with the numbers beside it, so
  // the bar is never read alone. Fetched again as tasks change.
  import { follow, quotas, type NamespaceQuota } from '#lib/api.ts';

  let { namespace = '' }: { namespace?: string } = $props();
  let list: NamespaceQuota[] = $state([]);
  let error = $state('');
  let loaded = $state(false);

  $effect(() => {
    const load = () =>
      quotas(namespace)
        .then((r) => {
          list = r.namespaces;
          error = '';
        })
        .catch((e) => (error = e.message))
        .finally(() => (loaded = true));
    // What counts changes as tasks are placed and end, and Volumes come
    // and go: loaded again after each such change.
    let pending: ReturnType<typeof setTimeout> | null = null;
    const soon = () => {
      if (pending) clearTimeout(pending);
      pending = setTimeout(load, 500);
    };
    const stops = [follow('Task', namespace, soon, () => {}), follow('Volume', namespace, soon, () => {})];
    load();
    return () => {
      if (pending) clearTimeout(pending);
      stops.forEach((s) => s());
    };
  });

  const gi = 1024 ** 3;
  const show = (r: string, v: number) =>
    r === 'memory' || r === 'storage'
      ? `${(v / gi).toFixed(v / gi < 10 ? 1 : 0)} Gi`
      : r === 'cpu'
        ? `${v.toFixed(v < 10 ? 2 : 1)} cores`
        : `${v} ${r}`;
  const share = (q: { limit: number; used: number }) => (q.limit > 0 ? q.used / q.limit : 0);
  const tone = (f: number) => (f >= 0.9 ? 'bad' : f >= 0.75 ? 'wait' : '');
</script>

{#if error}<p class="error">{error}</p>{/if}
{#if loaded && !list.length}
  <p class="muted">{namespace ? `${namespace} has no quota.` : 'No namespace you can see has a quota.'}</p>
{/if}
{#each list as n (n.namespace)}
  <div class="quota">
    {#if !namespace}<h3><a href="/k/Namespace/_/{n.namespace}">{n.namespace}</a></h3>{/if}
    {#each n.resources as q (q.resource)}
      {@const f = share(q)}
      <div class="row">
        <span class="name">{q.resource}</span>
        <span class="bar" role="meter" aria-valuemin="0" aria-valuemax={q.limit} aria-valuenow={q.used}
          aria-label="{n.namespace} {q.resource}"
          ><span class="fill {tone(f)}" style="width: {Math.min(100, f * 100)}%"></span></span
        >
        <span class="nums">{show(q.resource, q.used)} of {show(q.resource, q.limit)} ({Math.round(f * 100)}%)</span>
      </div>
    {/each}
  </div>
{/each}

<style>
  .quota {
    margin-bottom: 1rem;
  }
  h3 {
    margin: 0 0 0.35rem;
    font-size: 1rem;
  }
  .row {
    display: grid;
    grid-template-columns: 6rem minmax(8rem, 1fr) auto;
    gap: 0.75rem;
    align-items: center;
    margin: 0.2rem 0;
  }
  .name {
    color: var(--muted);
  }
  .bar {
    height: 8px;
    border-radius: 4px;
    background: var(--side);
    overflow: hidden;
  }
  .fill {
    display: block;
    height: 100%;
    border-radius: 4px;
    background: var(--accent);
  }
  .fill.wait {
    background: var(--wait);
  }
  .fill.bad {
    background: var(--bad);
  }
  .nums {
    font-variant-numeric: tabular-nums;
  }
</style>

<script lang="ts" module>
  /** Every task counts. */
  const EVERY = () => true;
</script>

<script lang="ts">
  // What a task, a service's tasks or a node's tasks used (`legion usage`):
  // CPU, memory, and (when node telemetry is in scope) ZFS pool space and
  // network throughput. The last hour as the leader sampled it, or up to
  // 30 days from what the managers keep; a strip under the charts to look
  // closer at part of it.
  import { usageHistory, type NodeSeries, type TaskSeries } from '#lib/api.ts';
  import Chart from '#lib/Chart.svelte';
  import RangeStrip from '#lib/RangeStrip.svelte';
  import { sumHeld } from '#lib/series.ts';
  import { RANGES, duration } from '#lib/span.ts';
  import { t, tn } from '#lib/i18n.svelte.ts';

  let {
    namespace,
    task = '',
    nodeFilter = '',
    pick = EVERY,
    compact = false
  }: {
    /** Empty: every namespace the user may read. */
    namespace: string;
    task?: string;
    /** Optional single node name for ZFS/network telemetry filtering. */
    nodeFilter?: string;
    /** Which tasks count. */
    pick?: (t: TaskSeries) => boolean;
    compact?: boolean;
  } = $props();

  let series: TaskSeries[] = $state([]);
  let nodeSeries: NodeSeries[] = $state([]);
  let step = $state(15_000);
  let error = $state('');
  let loaded = $state(false);
  // How far back, and the window on it the charts show (null: all of it).
  let range = $state(RANGES[0]);
  let view: [number, number] | null = $state(null);

  $effect(() => {
    let stopped = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    const back = range;
    view = null;
    const load = async () => {
      try {
        const r = await usageHistory(namespace, task, back === RANGES[0] ? 0 : back);
        if (stopped) return;
        series = r.tasks;
        nodeSeries = r.nodes ?? [];
        step = r.step_ms || 15_000;
        error = '';
      } catch (e) {
        error = (e as Error).message;
      } finally {
        loaded = true;
        // As often as samples come, and at least each minute.
        if (!stopped) timer = setTimeout(load, Math.min(Math.max(step, 15_000), 60_000));
      }
    };
    load();
    return () => {
      stopped = true;
      if (timer) clearTimeout(timer);
    };
  });

  // Summed per step, each task and node counted once in every step it was
  // sampled in, though their samples fall apart (lib/series.ts).
  const summed = $derived.by(() => {
    const picked = series.filter(pick);
    const nodesIn = task ? [] : nodeSeries.filter((ns) => !nodeFilter || ns.node === nodeFilter);
    return {
      cpu: sumHeld(picked.map((ts) => ts.samples.map(([at, c]) => [at, c] as [number, number])), step),
      mem: sumHeld(picked.map((ts) => ts.samples.map(([at, , m]) => [at, m] as [number, number])), step),
      zfs: sumHeld(nodesIn.map((ns) => ns.samples.map(([at, used]) => [at, used] as [number, number])), step),
      net: sumHeld(
        nodesIn.map((ns) => ns.samples.map(([at, , , rx, tx]) => [at, rx + tx] as [number, number])),
        step
      ),
      tasks: picked.length
    };
  });
  // What the window on the strip covers.
  const shown = $derived.by(() => {
    const w = view;
    const within = (p: [number, number][]) => (w ? p.filter(([at]) => at >= w[0] && at <= w[1]) : p);
    return { cpu: within(summed.cpu), mem: within(summed.mem), zfs: within(summed.zfs), net: within(summed.net) };
  });

  // Requests and limits of the tasks there now, those with a recent sample;
  // a limit only when every one of them has one.
  const marks = $derived.by(() => {
    const newest = Math.max(0, ...series.flatMap((t) => t.samples.map((s) => s[0])));
    const now = series
      .filter(pick)
      .filter((t) => t.samples.length && t.samples[t.samples.length - 1][0] >= newest - 2 * step);
    const total = (f: (t: TaskSeries) => number) => now.reduce((a, t) => a + f(t), 0);
    const every = (f: (t: TaskSeries) => number) => now.length > 0 && now.every((t) => f(t) > 0);
    const of = (req: (t: TaskSeries) => number, lim: (t: TaskSeries) => number) => [
      ...(total(req) > 0 ? [{ label: t('requested'), value: total(req) }] : []),
      ...(every(lim) ? [{ label: t('limit'), value: total(lim) }] : [])
    ];
    return {
      cpu: of((t) => t.cpu_request_percent, (t) => t.cpu_limit_percent),
      mem: of((t) => t.memory_request_bytes, (t) => t.memory_limit_bytes)
    };
  });

  const percent = (v: number) => `${v.toFixed(v < 10 ? 1 : 0)}%`;
  const bytes = (v: number) => {
    const units = ['B', 'Ki', 'Mi', 'Gi', 'Ti'];
    let i = 0;
    while (v >= 1024 && i < units.length - 1) {
      v /= 1024;
      i++;
    }
    return `${v.toFixed(v < 10 && i > 0 ? 1 : 0)}${units[i]}`;
  };
  const bps = (v: number) => `${bytes(v)}/s`;
  const totalMem = $derived(summed.mem.at(-1)?.[1] ?? 0);
  const totalCpu = $derived(summed.cpu.at(-1)?.[1] ?? 0);
  // The agent's metrics (legion-agent's metrics.rs), one series per
  // container, labelled namespace, task and container.
  const selector = $derived.by(() => {
    const m = [
      ...(namespace ? [`namespace="${namespace}"`] : []),
      ...(task ? [`task="${task}"`] : [])
    ];
    return m.length ? `{${m.join(',')}}` : '';
  });
  // Whether the selector picks the tasks the charts sum: a service's or a
  // node's are not told apart by the metrics' labels.
  const exact = $derived(pick === EVERY);
</script>

{#if error}<p class="error" role="alert">{error}</p>{/if}
{#if !compact}
  <div class="ranges" role="group" aria-label={t('How far back')}>
    {#each RANGES as r (r)}
      <button
        type="button"
        class="sm"
        class:primary={range === r}
        aria-pressed={range === r}
        data-range={r}
        onclick={() => (range = r)}>{duration(r)}</button
      >
    {/each}
  </div>
{/if}
{#if loaded && !summed.tasks && !summed.zfs.length}
  <p class="muted">
    {range === RANGES[0]
      ? t('No usage heard in the last hour: nothing ran, or the leader started since.')
      : t('No usage kept for the last {span}.', { span: duration(range) })}
  </p>
{:else if loaded}
  {#if !compact}
    <p class="muted">
      {summed.tasks > 1
        ? tn(
            'The last {span}, a sample every {every}, summed over {n} tasks. CPU is in percent of one CPU.',
            'The last {span}, a sample every {every}, summed over {n} tasks. CPU is in percent of one CPU.',
            summed.tasks,
            { span: duration(range), every: duration(step) }
          )
        : t('The last {span}, a sample every {every}. CPU is in percent of one CPU.', {
            span: duration(range),
            every: duration(step)
          })}
    </p>
    <RangeStrip points={summed.cpu.length ? summed.cpu : summed.zfs} bind:view />
  {/if}
  <div class={compact ? 'grid-2' : ''}>
    <div class={compact ? 'card' : ''}>
      <Chart
        title={t('CPU')}
        points={shown.cpu}
        format={percent}
        {step}
        marks={marks.cpu}
        tone="accent"
        {compact}
      />
    </div>
    <div class={compact ? 'card' : ''}>
      <Chart
        title={t('Memory')}
        points={shown.mem}
        format={bytes}
        {step}
        marks={marks.mem}
        tone="good"
        {compact}
      />
    </div>
    {#if summed.zfs.length}
      <div class={compact ? 'card' : ''}>
        <Chart
          title={t('ZFS pool used')}
          points={shown.zfs}
          format={bytes}
          {step}
          tone="zfs"
          {compact}
        />
      </div>
    {/if}
    {#if summed.net.length}
      <div class={compact ? 'card' : ''}>
        <Chart
          title={t('Network throughput')}
          points={shown.net}
          format={bps}
          {step}
          tone="wait"
          {compact}
        />
      </div>
    {/if}
  </div>
  {#if !compact && summed.cpu.length}
    <details class="card" style="margin-top: 0.75rem">
      <summary>{t('Prometheus telemetry & PromQL')}</summary>
      {#if exact}
        <p class="muted">
          {t('Exposed by each node daemon on --metrics-listen (/metrics). Live values from current samples:')}
        </p>
        <pre>{`sum(legion_container_cpu_percent${selector}) ${totalCpu.toFixed(1)}
sum(legion_container_memory_bytes${selector}) ${totalMem}`}</pre>
      {:else}
        <p class="muted">{t('Exposed by each node daemon on --metrics-listen (/metrics).')}</p>
      {/if}
      <p class="muted">{t('Example PromQL:')}</p>
      <pre>{`sum by (task) (rate(legion_container_cpu_seconds_total${selector}[5m]))
sum by (task) (legion_container_memory_bytes${selector})`}</pre>
    </details>
  {/if}
{/if}

<style>
  .ranges {
    display: inline-flex;
    margin-bottom: 0.5rem;
  }
  .ranges button + button {
    margin-left: -1px;
  }
</style>

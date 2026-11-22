<script lang="ts" module>
  /** Every task counts. */
  const EVERY = () => true;
</script>

<script lang="ts">
  // What a task, a service's tasks or a node's tasks used (`legion usage`):
  // CPU, memory, and (when node telemetry is in scope) ZFS pool space, pool
  // I/O and network throughput. The last hour as the leader sampled it, or up to
  // 30 days from what the managers keep; a strip under the charts to look
  // closer at part of it.
  import { followHistory, type NodeSeries, type TaskSeries } from '#lib/api.ts';
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
    compact = false,
    heading = ''
  }: {
    /** Empty: every namespace the user may read. */
    namespace: string;
    task?: string;
    /** Optional single node name for ZFS/network telemetry filtering. */
    nodeFilter?: string;
    /** Which tasks count. */
    pick?: (t: TaskSeries) => boolean;
    compact?: boolean;
    /** A title over the charts, the range picker beside it. */
    heading?: string;
  } = $props();

  let series: TaskSeries[] = $state([]);
  let nodeSeries: NodeSeries[] = $state([]);
  let step = $state(15_000);
  let error = $state('');
  let loaded = $state(false);
  // How far back, and the window on it the charts show (null: all of it).
  let range = $state(RANGES[0]);
  let view: [number, number] | null = $state(null);

  // Followed: the manager sends it again each time a sample is added.
  $effect(() => {
    const back = range;
    view = null;
    return followHistory(
      namespace,
      task,
      back === RANGES[0] ? 0 : back,
      (r) => {
        series = r.tasks;
        nodeSeries = r.nodes ?? [];
        step = r.step_ms || 15_000;
        error = '';
        loaded = true;
      },
      (why) => {
        error = why;
        if (why) loaded = true;
      }
    );
  });

  // Summed per step, each task and node counted once in every step it was
  // sampled in, though their samples fall apart (lib/series.ts).
  const summed = $derived.by(() => {
    const picked = series.filter(pick);
    // The nodes' own figures: every node's for the whole cluster, one node's
    // on its page, none for a task or a service, whose they are not.
    const nodesIn = task || (pick !== EVERY && !nodeFilter) ? [] : nodeSeries.filter((ns) => !nodeFilter || ns.node === nodeFilter);
    // Pool I/O only from samples that have it: a manager from before it was
    // kept sends five values.
    const io = (i: 5 | 6) =>
      sumHeld(
        nodesIn.map((ns) => ns.samples.filter((s) => s.length > i).map((s) => [s[0], s[i] ?? 0] as [number, number])),
        step
      );
    return {
      cpu: sumHeld(picked.map((ts) => ts.samples.map(([at, c]) => [at, c] as [number, number])), step),
      mem: sumHeld(picked.map((ts) => ts.samples.map(([at, , m]) => [at, m] as [number, number])), step),
      zfs: sumHeld(nodesIn.map((ns) => ns.samples.map(([at, used]) => [at, used] as [number, number])), step),
      rx: sumHeld(nodesIn.map((ns) => ns.samples.map(([at, , , rx]) => [at, rx] as [number, number])), step),
      tx: sumHeld(nodesIn.map((ns) => ns.samples.map(([at, , , , tx]) => [at, tx] as [number, number])), step),
      read: io(5),
      written: io(6),
      tasks: picked.length
    };
  });
  // What the window on the strip covers.
  const shown = $derived.by(() => {
    const w = view;
    const within = (p: [number, number][]) => (w ? p.filter(([at]) => at >= w[0] && at <= w[1]) : p);
    return {
      cpu: within(summed.cpu),
      mem: within(summed.mem),
      zfs: within(summed.zfs),
      rx: within(summed.rx),
      tx: within(summed.tx),
      read: within(summed.read),
      written: within(summed.written)
    };
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

{#snippet ranges()}
  <div class="segmented" role="group" aria-label={t('How far back')}>
    {#each RANGES as r (r)}
      <button type="button" aria-pressed={range === r} data-range={r} onclick={() => (range = r)}
        >{duration(r)}</button
      >
    {/each}
  </div>
{/snippet}
{#if heading}
  <div class="section-head">
    <h2>{heading}</h2>
    {@render ranges()}
  </div>
{:else}
  <div class="ranges">{@render ranges()}</div>
{/if}
{#if error}<p class="error" role="alert">{error}</p>{/if}
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
  <div class={compact ? 'grid-2 chart-cards' : ''}>
    {#if !summed.tasks && !task && pick === EVERY && !nodeFilter}
      <!-- A namespace's or the cluster's: CPU and memory are what tasks
           use, so with none say so, rather than draw two empty charts
           waiting for samples that will not come. One task's or service's
           still waits for its first samples. -->
      <div class={compact ? 'card' : ''}>
        <p class="muted">
          {namespace
            ? t('No tasks ran in {namespace} in the last {span}, so there is no CPU or memory to show.', {
                namespace,
                span: duration(range)
              })
            : t('No tasks ran in the last {span}, so there is no CPU or memory to show.', {
                span: duration(range)
              })}
        </p>
      </div>
    {:else}
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
    {/if}
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
    {#if summed.rx.length || summed.tx.length}
      <div class={compact ? 'card' : ''}>
        <Chart
          title={t('Network throughput')}
          label={t('received')}
          points={shown.rx}
          also={{ label: t('sent'), points: shown.tx }}
          format={bps}
          {step}
          tone="wait"
          {compact}
        />
      </div>
    {/if}
    {#if summed.read.length || summed.written.length}
      <div class={compact ? 'card' : ''}>
        <Chart
          title={t('Pool I/O')}
          label={t('read')}
          points={shown.read}
          also={{ label: t('written'), points: shown.written }}
          format={bps}
          {step}
          tone="zfs"
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
  .chart-cards > .card {
    display: flex;
    flex-direction: column;
  }
  .ranges {
    margin-bottom: 0.5rem;
  }
</style>

<script lang="ts">
  // What a task, a service's tasks or a node's tasks used over the last
  // hour (`legion usage`), as the leader sampled it: CPU, memory, and
  // (when node telemetry is in scope) ZFS pool space and network throughput.
  import { usageHistory, type NodeSeries, type TaskSeries } from '#lib/api.ts';
  import Chart from '#lib/Chart.svelte';
  import { t, tn } from '#lib/i18n.svelte.ts';

  let {
    namespace,
    task = '',
    nodeFilter = '',
    pick = () => true,
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

  $effect(() => {
    let stopped = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    const load = async () => {
      try {
        const r = await usageHistory(namespace, task);
        if (stopped) return;
        series = r.tasks;
        nodeSeries = r.nodes ?? [];
        step = r.step_ms || 15_000;
        error = '';
      } catch (e) {
        error = (e as Error).message;
      } finally {
        loaded = true;
        if (!stopped) timer = setTimeout(load, step);
      }
    };
    load();
    return () => {
      stopped = true;
      if (timer) clearTimeout(timer);
    };
  });

  // Summed per sample time, rounded to the step: tasks' samples fall
  // within a step of each other.
  const summed = $derived.by(() => {
    const cpu = new Map<number, number>();
    const mem = new Map<number, number>();
    const zfs = new Map<number, number>();
    const net = new Map<number, number>();
    for (const ts of series.filter(pick)) {
      for (const [at, c, m] of ts.samples) {
        const k = Math.round(at / step) * step;
        cpu.set(k, (cpu.get(k) ?? 0) + c);
        mem.set(k, (mem.get(k) ?? 0) + m);
      }
    }
    if (!task) {
      for (const ns of nodeSeries) {
        if (nodeFilter && ns.node !== nodeFilter) continue;
        for (const [at, used, , rx, tx] of ns.samples) {
          const k = Math.round(at / step) * step;
          zfs.set(k, (zfs.get(k) ?? 0) + used);
          net.set(k, (net.get(k) ?? 0) + rx + tx);
        }
      }
    }
    const sorted = (m: Map<number, number>) =>
      [...m.entries()].sort((a, b) => a[0] - b[0]) as [number, number][];
    return {
      cpu: sorted(cpu),
      mem: sorted(mem),
      zfs: sorted(zfs),
      net: sorted(net),
      tasks: series.filter(pick).length
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
</script>

{#if error}<p class="error" role="alert">{error}</p>{/if}
{#if loaded && !summed.tasks && !summed.zfs.length}
  <p class="muted">
    {t('No usage heard in the last hour: nothing ran, or the leader started since.')}
  </p>
{:else if loaded}
  {#if !compact}
    <p class="muted">
      {summed.tasks > 1
        ? tn(
            'The last hour, a sample every {seconds} s, summed over {n} tasks. CPU is in percent of one CPU.',
            'The last hour, a sample every {seconds} s, summed over {n} tasks. CPU is in percent of one CPU.',
            summed.tasks,
            { seconds: Math.round(step / 1000) }
          )
        : t('The last hour, a sample every {seconds} s. CPU is in percent of one CPU.', {
            seconds: Math.round(step / 1000)
          })}
    </p>
  {/if}
  <div class={compact ? 'grid-2' : ''}>
    <div class={compact ? 'card' : ''}>
      <Chart
        title={t('CPU')}
        points={summed.cpu}
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
        points={summed.mem}
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
          points={summed.zfs}
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
          points={summed.net}
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
      <p class="muted">
        {t('Exposed by each node daemon on --metrics-listen (/metrics). Live values from current samples:')}
      </p>
      <pre>{`legion_task_cpu_cores{namespace="${namespace || '*'}"${task ? `,task="${task}"` : ''}} ${totalCpu.toFixed(4)}
legion_task_memory_bytes{namespace="${namespace || '*'}"${task ? `,task="${task}"` : ''}} ${totalMem}`}</pre>
      <p class="muted">{t('Example PromQL:')}</p>
      <pre>{`rate(legion_task_cpu_seconds_total{${task ? `task="${task}"` : `namespace="${namespace || 'default'}"`}}[5m])
legion_task_memory_bytes{${task ? `task="${task}"` : `namespace="${namespace || 'default'}"`}}`}</pre>
    </details>
  {/if}
{/if}

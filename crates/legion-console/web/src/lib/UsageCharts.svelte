<script lang="ts">
  // What a task, a service's tasks or a node's tasks used over the last
  // hour (`legion usage`), as the leader sampled it: CPU and memory, each
  // its own chart, summed over the tasks at each sample. Fetched again as
  // each new sample is due.
  import { usageHistory, type TaskSeries } from '#lib/api.ts';
  import Chart from '#lib/Chart.svelte';

  let {
    namespace,
    task = '',
    pick = () => true
  }: {
    /** Empty: every namespace the user may read. */
    namespace: string;
    task?: string;
    /** Which tasks count. */
    pick?: (t: TaskSeries) => boolean;
  } = $props();

  let series: TaskSeries[] = $state([]);
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
    for (const t of series.filter(pick)) {
      for (const [at, c, m] of t.samples) {
        const k = Math.round(at / step) * step;
        cpu.set(k, (cpu.get(k) ?? 0) + c);
        mem.set(k, (mem.get(k) ?? 0) + m);
      }
    }
    const sorted = (m: Map<number, number>) =>
      [...m.entries()].sort((a, b) => a[0] - b[0]) as [number, number][];
    return { cpu: sorted(cpu), mem: sorted(mem), tasks: series.filter(pick).length };
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
      ...(total(req) > 0 ? [{ label: 'requested', value: total(req) }] : []),
      ...(every(lim) ? [{ label: 'limit', value: total(lim) }] : [])
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
</script>

{#if error}<p class="error">{error}</p>{/if}
{#if loaded && !summed.tasks}
  <p class="muted">No usage heard in the last hour: nothing ran, or the leader started since.</p>
{:else if loaded}
  <p class="muted">
    The last hour, a sample every {Math.round(step / 1000)} s{summed.tasks > 1
      ? `, summed over ${summed.tasks} tasks`
      : ''}. CPU is in percent of one CPU.
  </p>
  <Chart title="CPU" points={summed.cpu} format={percent} {step} marks={marks.cpu} />
  <Chart title="Memory" points={summed.mem} format={bytes} {step} marks={marks.mem} />
{/if}

<script lang="ts">
  // One series over time, on one axis: a 2px line, a faint grid, the
  // title naming it (no legend for one series), a crosshair and tooltip on
  // hover, and the latest, average and highest values as text beside it.
  let {
    title,
    points,
    format,
    step,
    marks = []
  }: {
    title: string;
    /** [milliseconds since the epoch, value], oldest first. */
    points: [number, number][];
    format: (v: number) => string;
    /** The time between samples: a longer gap breaks the line. */
    step: number;
    /** Levels to compare with, drawn dashed and named: a request, a limit. */
    marks?: { label: string; value: number }[];
  } = $props();

  let width = $state(600);
  const height = 150;
  const pad = { top: 10, right: 12, bottom: 22, left: 64 };
  const plotW = $derived(Math.max(10, width - pad.left - pad.right));
  const plotH = height - pad.top - pad.bottom;

  const t0 = $derived(points.length ? points[0][0] : 0);
  const t1 = $derived(points.length ? points[points.length - 1][0] : 1);
  const top = $derived.by(() => {
    const max = Math.max(0, ...points.map((p) => p[1]), ...marks.map((m) => m.value));
    if (max <= 0) return 1;
    // A round top: 1, 2 or 5 times a power of ten.
    const e = Math.pow(10, Math.floor(Math.log10(max)));
    return [1, 2, 5, 10].map((m) => m * e).find((v) => v >= max) ?? max;
  });
  const x = (t: number) => pad.left + (t1 === t0 ? plotW : ((t - t0) / (t1 - t0)) * plotW);
  const y = (v: number) => pad.top + plotH - (v / top) * plotH;
  const path = $derived.by(() => {
    let d = '';
    let prev: number | null = null;
    for (const [t, v] of points) {
      const gap = prev !== null && t - prev > step * 2.5;
      d += `${d && !gap ? 'L' : 'M'}${x(t).toFixed(1)},${y(v).toFixed(1)}`;
      prev = t;
    }
    return d;
  });
  const values = $derived(points.map((p) => p[1]));
  const last = $derived(values.length ? values[values.length - 1] : 0);
  const avg = $derived(values.length ? values.reduce((a, b) => a + b, 0) / values.length : 0);
  const max = $derived(values.length ? Math.max(...values) : 0);
  const clock = (t: number) =>
    new Date(t).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

  let hover: number | null = $state(null);
  function move(e: PointerEvent) {
    const r = (e.currentTarget as SVGElement).getBoundingClientRect();
    const px = e.clientX - r.left;
    if (!points.length) return;
    let best = 0;
    for (let i = 1; i < points.length; i++) {
      if (Math.abs(x(points[i][0]) - px) < Math.abs(x(points[best][0]) - px)) best = i;
    }
    hover = best;
  }
</script>

<figure class="chart" bind:clientWidth={width}>
  <figcaption>
    <strong>{title}</strong>
    {#if points.length}
      <span class="muted"
        >now {format(last)} · average {format(avg)} · highest {format(max)}{#each marks as m (m.label)}
          · {m.label} {format(m.value)}{/each}</span
      >
    {/if}
  </figcaption>
  {#if points.length < 2}
    <p class="muted">Not enough samples yet: one comes every {Math.round(step / 1000)} s.</p>
  {:else}
    <svg
      {width}
      {height}
      role="img"
      aria-label="{title}: now {format(last)}, average {format(avg)}, highest {format(max)}"
      onpointermove={move}
      onpointerleave={() => (hover = null)}
    >
      {#each [0, 0.5, 1] as f (f)}
        <line class="grid" x1={pad.left} x2={pad.left + plotW} y1={y(top * f)} y2={y(top * f)} />
        <text class="axis" x={pad.left - 6} y={y(top * f) + 4} text-anchor="end">{format(top * f)}</text>
      {/each}
      <text class="axis" x={pad.left} y={height - 6}>{clock(t0)}</text>
      <text class="axis" x={pad.left + plotW} y={height - 6} text-anchor="end">{clock(t1)}</text>
      {#each marks as m (m.label)}
        <line class="mark" x1={pad.left} x2={pad.left + plotW} y1={y(m.value)} y2={y(m.value)} />
        <text class="axis" x={pad.left + plotW - 4} y={y(m.value) - 4} text-anchor="end"
          >{m.label} {format(m.value)}</text
        >
      {/each}
      <path class="line" d={path} />
      {#if hover !== null}
        {@const [ht, hv] = points[hover]}
        <line class="cross" x1={x(ht)} x2={x(ht)} y1={pad.top} y2={pad.top + plotH} />
        <circle class="dot" cx={x(ht)} cy={y(hv)} r="4" />
        <g transform="translate({Math.min(x(ht) + 8, pad.left + plotW - 120)},{pad.top + 4})">
          <rect class="tip" width="116" height="36" rx="4" />
          <text class="tiptext" x="8" y="15">{clock(ht)}</text>
          <text class="tiptext strong" x="8" y="29">{format(hv)}</text>
        </g>
      {/if}
    </svg>
  {/if}
</figure>

<style>
  .chart {
    margin: 0 0 1rem;
  }
  figcaption {
    display: flex;
    gap: 0.75rem;
    align-items: baseline;
    flex-wrap: wrap;
    margin-bottom: 0.25rem;
  }
  svg {
    display: block;
    touch-action: none;
  }
  .grid {
    stroke: var(--line);
    stroke-width: 1;
  }
  .axis {
    fill: var(--muted);
    font-size: 11px;
  }
  .line {
    fill: none;
    stroke: var(--accent);
    stroke-width: 2;
    stroke-linejoin: round;
    stroke-linecap: round;
  }
  .mark {
    stroke: var(--muted);
    stroke-width: 1;
    stroke-dasharray: 6 4;
  }
  .cross {
    stroke: var(--muted);
    stroke-width: 1;
    stroke-dasharray: 3 3;
  }
  .dot {
    fill: var(--accent);
    stroke: var(--panel);
    stroke-width: 2;
  }
  .tip {
    fill: var(--panel);
    stroke: var(--line);
  }
  .tiptext {
    fill: var(--muted);
    font-size: 11px;
  }
  .tiptext.strong {
    fill: var(--text);
    font-weight: 600;
  }
</style>

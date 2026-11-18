<script lang="ts">
  // One series over time, on one axis: a 2px line with a soft area fill,
  // a faint grid, the title naming it, a crosshair and tooltip on hover,
  // and the latest, average and highest values as text beside it; every
  // sample as a table too, behind a disclosure, for whoever can't see the
  // line or wants the numbers (WCAG 1.1.1, 1.3.1).
  import { t } from '#lib/i18n.svelte.ts';
  import Ico from '#lib/Ico.svelte';
  import { Download } from '@lucide/svelte';

  let {
    title,
    points,
    format,
    step,
    marks = [],
    tone = 'accent',
    compact = false
  }: {
    title: string;
    /** [milliseconds since the epoch, value], oldest first. */
    points: [number, number][];
    format: (v: number) => string;
    /** The time between samples: a longer gap breaks the line. */
    step: number;
    /** Levels to compare with, drawn dashed and named: a request, a limit. */
    marks?: { label: string; value: number }[];
    tone?: 'accent' | 'good' | 'zfs' | 'wait';
    compact?: boolean;
  } = $props();

  let width = $state(600);
  const height = $derived(compact ? 120 : 150);
  const pad = { top: 10, right: 12, bottom: 22, left: 64 };
  const plotW = $derived(Math.max(10, width - pad.left - pad.right));
  const plotH = $derived(height - pad.top - pad.bottom);
  const gradId = `grad-${Math.random().toString(36).slice(2, 9)}`;

  const strokeVar = $derived(
    tone === 'good'
      ? 'var(--good)'
      : tone === 'zfs'
        ? 'var(--zfs)'
        : tone === 'wait'
          ? 'var(--wait)'
          : 'var(--accent)'
  );

  const t0 = $derived(points.length ? points[0][0] : 0);
  const t1 = $derived(points.length ? points[points.length - 1][0] : 1);
  const top = $derived.by(() => {
    const max = Math.max(0, ...points.map((p) => p[1]), ...marks.map((m) => m.value));
    if (max <= 0) return 1;
    // A round top: 1, 2 or 5 times a power of ten.
    const e = Math.pow(10, Math.floor(Math.log10(max)));
    return [1, 2, 5, 10].map((m) => m * e).find((v) => v >= max) ?? max;
  });
  const x = (tm: number) => pad.left + (t1 === t0 ? plotW : ((tm - t0) / (t1 - t0)) * plotW);
  const y = (v: number) => pad.top + plotH - (v / top) * plotH;

  const segments = $derived.by(() => {
    const segs: [number, number][][] = [];
    let cur: [number, number][] = [];
    let prev: number | null = null;
    for (const [tm, v] of points) {
      if (prev !== null && tm - prev > step * 2.5 && cur.length) {
        segs.push(cur);
        cur = [];
      }
      cur.push([tm, v]);
      prev = tm;
    }
    if (cur.length) segs.push(cur);
    return segs;
  });

  const path = $derived(
    segments
      .map((seg) =>
        seg.map(([tm, v], i) => `${i === 0 ? 'M' : 'L'}${x(tm).toFixed(1)},${y(v).toFixed(1)}`).join('')
      )
      .join('')
  );

  const area = $derived(
    segments
      .filter((seg) => seg.length > 1)
      .map((seg) => {
        const line = seg
          .map(([tm, v], i) => `${i === 0 ? 'M' : 'L'}${x(tm).toFixed(1)},${y(v).toFixed(1)}`)
          .join('');
        const base = pad.top + plotH;
        const firstX = x(seg[0][0]).toFixed(1);
        const lastX = x(seg[seg.length - 1][0]).toFixed(1);
        return `${line}L${lastX},${base}L${firstX},${base}Z`;
      })
      .join('')
  );

  const values = $derived(points.map((p) => p[1]));
  const last = $derived(values.length ? values[values.length - 1] : 0);
  const avg = $derived(values.length ? values.reduce((a, b) => a + b, 0) / values.length : 0);
  const max = $derived(values.length ? Math.max(...values) : 0);
  const clock = (tm: number) =>
    new Date(tm).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

  // The samples as CSV, for a spreadsheet: the time in ISO 8601, the value.
  function downloadCsv() {
    const rows = [`time,${JSON.stringify(title)}`, ...points.map(([tm, v]) => `${new Date(tm).toISOString()},${v}`)];
    const a = document.createElement('a');
    a.href = URL.createObjectURL(new Blob([rows.join('\n') + '\n'], { type: 'text/csv' }));
    a.download = `${title.replace(/[^\w.-]+/g, '-').toLowerCase()}.csv`;
    a.click();
    setTimeout(() => URL.revokeObjectURL(a.href), 1000);
  }
  const spanMin = $derived(Math.max(1, Math.round((t1 - t0) / 60000)));

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

<figure class="chart" bind:clientWidth={width} style:--chart-color={strokeVar}>
  <figcaption>
    <strong>{title}</strong>
    {#if points.length}
      <span class="muted"
        >{t('now {now} · average {average} · highest {highest}', {
          now: format(last),
          average: format(avg),
          highest: format(max)
        })}{#each marks as m (m.label)}
          · {m.label} {format(m.value)}{/each}</span
      >
      <button
        type="button"
        class="sm chart-tool"
        title={t('Download the samples as CSV')}
        aria-label={t('Download {title} as CSV', { title })}
        onclick={downloadCsv}><Ico icon={Download} size={14} />CSV</button
      >
    {/if}
  </figcaption>
  {#if points.length < 2}
    <p class="muted">
      {t('Not enough samples yet: one comes every {seconds} s.', {
        seconds: Math.round(step / 1000)
      })}
    </p>
  {:else}
    <svg
      {width}
      {height}
      role="img"
      aria-label={t('{title}: now {now}, average {average}, highest {highest}', {
        title,
        now: format(last),
        average: format(avg),
        highest: format(max)
      })}
      onpointermove={move}
      onpointerleave={() => (hover = null)}
    >
      <defs>
        <linearGradient id={gradId} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color={strokeVar} stop-opacity="0.28" />
          <stop offset="100%" stop-color={strokeVar} stop-opacity="0.0" />
        </linearGradient>
      </defs>
      {#each [0, 0.5, 1] as f (f)}
        <line class="grid" x1={pad.left} x2={pad.left + plotW} y1={y(top * f)} y2={y(top * f)} />
        <text class="axis" x={pad.left - 6} y={y(top * f) + 4} text-anchor="end"
          >{format(top * f)}</text
        >
      {/each}
      <text class="axis" x={pad.left} y={height - 6}>{clock(t0)}</text>
      <text class="axis" x={pad.left + plotW} y={height - 6} text-anchor="end">{clock(t1)}</text>
      {#each marks as m (m.label)}
        <line class="mark" x1={pad.left} x2={pad.left + plotW} y1={y(m.value)} y2={y(m.value)} />
        <text class="axis" x={pad.left + plotW - 4} y={y(m.value) - 4} text-anchor="end"
          >{m.label} {format(m.value)}</text
        >
      {/each}
      {#if area}
        <path d={area} fill="url(#{gradId})" />
      {/if}
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
    <p class="chart-range muted">
      {t('last {minutes} min · a sample every {seconds} s', { minutes: spanMin, seconds: Math.round(step / 1000) })}
    </p>
    <details class="values">
      <summary>{t('{title}: values as a table', { title })}</summary>
      <!-- Scrolled, and focusable so the keyboard scrolls it too (WCAG 2.1.1,
           axe's scrollable-region-focusable), which Svelte's rule doesn't know. -->
      <!-- svelte-ignore a11y_no_noninteractive_tabindex -->
      <div class="scroll" tabindex="0" role="region" aria-label={t('{title} over time', { title })}>
        <table>
          <caption class="sr-only">{t('{title} over time', { title })}</caption>
          <thead>
            <tr>
              <th scope="col">{t('Time')}</th>
              <th scope="col">{title}</th>
            </tr>
          </thead>
          <tbody>
            {#each [...points].reverse() as [pt, pv] (pt)}
              <tr>
                <td>{new Date(pt).toLocaleTimeString()}</td>
                <td>{format(pv)}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    </details>
  {/if}
</figure>

<style>
  .chart-tool {
    margin-left: auto;
  }
  .chart-range {
    margin: 0.1rem 0 0.2rem;
    font-size: 0.78rem;
  }
  .chart {
    margin: 0 0 0.75rem;
  }
  figcaption {
    display: flex;
    gap: 0.65rem;
    align-items: baseline;
    flex-wrap: wrap;
    margin-bottom: 0.2rem;
    font-size: 0.85rem;
  }
  svg {
    display: block;
    touch-action: pan-y;
  }
  .values {
    margin-top: 0.2rem;
    font-size: 0.8rem;
  }
  .values .scroll {
    max-height: 14rem;
    overflow-y: auto;
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
    stroke: var(--chart-color, var(--accent));
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
    fill: var(--chart-color, var(--accent));
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

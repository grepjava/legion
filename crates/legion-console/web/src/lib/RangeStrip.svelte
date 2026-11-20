<script lang="ts">
  // The whole range in a thin strip under the charts, a window on it to
  // look closer: dragged across with a pointer, or moved to where a click
  // falls (WCAG 2.5.7: no dragging needed), or moved with the arrow keys
  // and made wider or narrower with Shift and the arrows (or + and -). The
  // charts show what the window covers; "Whole range" shows it all again.
  import { t } from '#lib/i18n.svelte.ts';
  import { stamp } from '#lib/span.ts';

  let {
    points,
    view = $bindable(null)
  }: {
    /** [milliseconds since the epoch, value] over the whole range, oldest first. */
    points: [number, number][];
    /** The window shown, [from, to]; null for all of it. */
    view?: [number, number] | null;
  } = $props();

  let width = $state(600);
  const height = 40;
  const t0 = $derived(points.length ? points[0][0] : 0);
  const t1 = $derived(points.length ? points[points.length - 1][0] : 1);
  const span = $derived(Math.max(1, t1 - t0));
  const top = $derived(Math.max(1e-9, ...points.map((p) => p[1])));
  const x = (tm: number) => ((tm - t0) / span) * width;
  const at = (px: number) => t0 + Math.min(1, Math.max(0, px / width)) * span;
  const line = $derived(
    points.map(([tm, v], i) => `${i ? 'L' : 'M'}${x(tm).toFixed(1)},${(height - 2 - (v / top) * (height - 6)).toFixed(1)}`).join('')
  );
  const shown = $derived(view ?? [t0, t1]);

  // A window keeps within the range and is at least a fiftieth of it.
  function set(from: number, to: number) {
    const least = span / 50;
    if (to - from < least) {
      const mid = (from + to) / 2;
      from = mid - least / 2;
      to = mid + least / 2;
    }
    if (from < t0) [from, to] = [t0, to + (t0 - from)];
    if (to > t1) [from, to] = [from - (to - t1), t1];
    from = Math.max(t0, from);
    to = Math.min(t1, to);
    view = from <= t0 && to >= t1 ? null : [from, to];
  }

  let start: number | null = null;
  let moved = false;
  function down(e: PointerEvent) {
    (e.currentTarget as Element).setPointerCapture(e.pointerId);
    start = e.offsetX;
    moved = false;
  }
  function move(e: PointerEvent) {
    if (start === null) return;
    if (Math.abs(e.offsetX - start) > 3) moved = true;
    if (moved) set(at(Math.min(start, e.offsetX)), at(Math.max(start, e.offsetX)));
  }
  function up(e: PointerEvent) {
    if (start === null) return;
    if (!moved) {
      // A click: the window, as wide as it is (a quarter of the range at
      // first), moved to centre where it fell.
      const wide = view ? view[1] - view[0] : span / 4;
      const mid = at(e.offsetX);
      set(mid - wide / 2, mid + wide / 2);
    }
    start = null;
  }
  function key(e: KeyboardEvent) {
    const [from, to] = shown;
    const wide = to - from;
    const by = span / 20;
    const grow = (f: number) => {
      const mid = (from + to) / 2;
      set(mid - (wide * f) / 2, mid + (wide * f) / 2);
    };
    if (e.key === 'ArrowLeft' && e.shiftKey) grow(0.8);
    else if (e.key === 'ArrowRight' && e.shiftKey) grow(1.25);
    else if (e.key === 'ArrowLeft') set(from - by, to - by);
    else if (e.key === 'ArrowRight') set(from + by, to + by);
    else if (e.key === '-') grow(1.25);
    else if (e.key === '+' || e.key === '=') grow(0.8);
    else if (e.key === 'Home') set(t0, t0 + wide);
    else if (e.key === 'End') set(t1 - wide, t1);
    else if (e.key === 'Escape' && view) view = null;
    else return;
    e.preventDefault();
  }
  const said = $derived(
    t('From {from} to {to}', { from: stamp(shown[0], span), to: stamp(shown[1], span) })
  );
</script>

{#if points.length > 1}
  <div class="strip" bind:clientWidth={width}>
    <svg
      {width}
      {height}
      role="slider"
      tabindex="0"
      aria-label={t('Window on the range: drag or click to move it; arrows move it, Shift and arrows make it wider or narrower')}
      aria-valuemin={0}
      aria-valuemax={100}
      aria-valuenow={Math.round((((shown[0] + shown[1]) / 2 - t0) / span) * 100)}
      aria-valuetext={said}
      data-view={view ? `${Math.round(view[0])}-${Math.round(view[1])}` : 'all'}
      onpointerdown={down}
      onpointermove={move}
      onpointerup={up}
      onpointercancel={() => (start = null)}
      onkeydown={key}
    >
      <path class="spark" d={line} />
      {#if view}
        <rect class="out" x="0" y="0" width={Math.max(0, x(view[0]))} {height} />
        <rect class="out" x={x(view[1])} y="0" width={Math.max(0, width - x(view[1]))} {height} />
        <rect class="window" x={x(view[0])} y="0.5" width={Math.max(2, x(view[1]) - x(view[0]))} height={height - 1} />
      {/if}
    </svg>
    <p class="muted said">
      <span>{said}</span>
      {#if view}
        <button type="button" class="sm" onclick={() => (view = null)}>{t('Whole range')}</button>
      {/if}
    </p>
  </div>
{/if}

<style>
  .strip {
    margin: 0.25rem 0 0.75rem;
  }
  svg {
    display: block;
    cursor: crosshair;
    touch-action: pan-y;
    border: 1px solid var(--line);
    border-radius: 4px;
  }
  svg:focus-visible {
    outline: 2px solid var(--accent);
    outline-offset: 2px;
  }
  .spark {
    fill: none;
    stroke: var(--muted);
    stroke-width: 1;
  }
  .out {
    fill: var(--bg, #000);
    opacity: 0.45;
  }
  .window {
    fill: none;
    stroke: var(--accent);
    stroke-width: 1.5;
  }
  .said {
    display: flex;
    gap: 0.5rem;
    align-items: center;
    margin: 0.2rem 0 0;
    font-size: 0.8rem;
  }
</style>

<script lang="ts">
  // A status card: what it is about above (with its icon), the figure to
  // read at a glance, a line on what the figure means, a meter where it is
  // a share of something, and a link to where more is shown. The same on
  // every page that sums something up (the Overview, Volumes, a node).
  import type { Snippet } from 'svelte';
  import Ico from '#lib/Ico.svelte';
  import type { IconComponent } from '#lib/icons.ts';

  let {
    label,
    icon,
    value,
    note = '',
    tone = '',
    pct,
    meterLabel = '',
    href = '',
    more = '',
    children
  }: {
    label: string;
    icon?: IconComponent;
    /** The figure. */
    value: string;
    /** What the figure means. */
    note?: string;
    /** good, wait, bad or zfs: the figure's colour and the meter's. */
    tone?: string;
    /** A share, 0 to 100, drawn as a meter. */
    pct?: number;
    /** The meter's name, said for those who cannot see it. */
    meterLabel?: string;
    /** Where more is shown, and the link's words. */
    href?: string;
    more?: string;
    /** Anything else, between the note and the link. */
    children?: Snippet;
  } = $props();

  const fill = $derived(pct === undefined ? 0 : Math.max(0, Math.min(100, pct)));
  // A meter's colour: the tile's, else by how full it is.
  const meterTone = $derived(tone || (fill >= 90 ? 'bad' : fill >= 75 ? 'wait' : 'good'));
</script>

<section class="tile" aria-label={label}>
  <div class="tile-head">
    <h3 class="tile-label">{label}</h3>
    {#if icon}<Ico {icon} size={16} class={tone} />{/if}
  </div>
  <p class="tile-value {tone}">{value}</p>
  {#if note}<p class="tile-note">{note}</p>{/if}
  {#if pct !== undefined}
    <div
      class="meter"
      role="meter"
      aria-valuemin={0}
      aria-valuemax={100}
      aria-valuenow={Math.round(fill)}
      aria-label={meterLabel || label}
    >
      <div class="meter-fill {meterTone}" style:width="{fill}%"></div>
    </div>
  {/if}
  {@render children?.()}
  {#if href && more}
    <p class="tile-foot"><a {href}>{more}</a></p>
  {/if}
</section>

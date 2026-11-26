<svelte:options namespace="svg" />

<script lang="ts">
  // What a Service runs, drawn in its ring on the Topology page: a bhyve
  // VM's monitor, its product's mark, a database's cylinder, or a jail's
  // cube.
  import type { LObject } from './api.ts';
  import { logoFor } from './logos.ts';
  import { isDb, isVm } from './topology.ts';

  let { service: s, x, y }: { service: LObject; x: number; y: number } = $props();

  const logo = $derived(logoFor(`${s.metadata.name} ${s.spec?.pod?.containers?.[0]?.image ?? ''}`));
</script>

{#if isVm(s)}
  <rect x={x - 10} y={y - 9} width="20" height="14" rx="2" class="icon-stroke" />
  <line x1={x - 5} y1={y + 9} x2={x + 5} y2={y + 9} class="icon-stroke" />
  <line x1={x} y1={y + 5} x2={x} y2={y + 9} class="icon-stroke" />
{:else if logo}
  <path
    class="logo"
    style:fill="light-dark({logo.light}, {logo.dark})"
    d={logo.path}
    transform="translate({x - 11} {y - 11}) scale({22 / 24})"
    aria-hidden="true"
  ></path>
{:else if isDb(s)}
  <ellipse cx={x} cy={y - 7} rx="9" ry="3.5" class="icon-stroke" />
  <path d="M {x - 9} {y - 7} v 14 c 0 2 4 3.5 9 3.5 s 9 -1.5 9 -3.5 v -14" class="icon-stroke" />
  <path d="M {x - 9} {y} c 0 2 4 3.5 9 3.5 s 9 -1.5 9 -3.5" class="icon-stroke" />
{:else}
  <polygon
    points="{x},{y - 11} {x + 10},{y - 5} {x + 10},{y + 6} {x},{y + 12} {x - 10},{y + 6} {x - 10},{y - 5}"
    class="icon-stroke"
  />
  <polyline points="{x - 10},{y - 5} {x},{y + 1} {x + 10},{y - 5}" class="icon-stroke" />
  <line x1={x} y1={y + 1} x2={x} y2={y + 12} class="icon-stroke" />
{/if}

<style>
  .icon-stroke {
    fill: none;
    stroke: var(--text);
    stroke-width: 1.5;
    stroke-linecap: round;
    stroke-linejoin: round;
  }
  .logo {
    fill: var(--text);
  }
</style>

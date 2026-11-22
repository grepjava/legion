<script lang="ts">
  // A row's less used actions behind one "More" button: a disclosure, so it
  // needs no menu roles and works from the keyboard as any button does. It
  // closes once one of its actions is chosen, on Escape, and on a click
  // anywhere else.
  import type { Snippet } from 'svelte';
  import { MoreHorizontal } from '@lucide/svelte';
  import Ico from './Ico.svelte';
  import { t } from '#lib/i18n.svelte.ts';

  let { label, children }: { label?: string; children: Snippet } = $props();
  let el: HTMLDetailsElement | undefined = $state();
  // Placed on the page, not in the row, so a table's scrolling box does not
  // clip it: under the button, its right edges lined up.
  let at = $state({ top: 0, right: 0 });

  function placed() {
    if (!el?.open) return;
    const r = el.querySelector('summary')!.getBoundingClientRect();
    at = { top: r.bottom + 4, right: Math.max(8, window.innerWidth - r.right) };
  }

  function close() {
    if (el) el.open = false;
  }
  function onWindowClick(e: MouseEvent) {
    if (el?.open && !el.contains(e.target as Node)) close();
  }
  function onKey(e: KeyboardEvent) {
    if (e.key === 'Escape' && el?.open) {
      close();
      el.querySelector('summary')?.focus();
    }
  }
</script>

<svelte:window onclick={onWindowClick} onkeydown={onKey} onresize={close} onscrollcapture={close} />

<details class="row-menu" bind:this={el} ontoggle={placed}>
  <summary class="button sm" aria-label={label ?? t('More actions')}
    ><Ico icon={MoreHorizontal} size={15} /><span aria-hidden="true">{t('More')}</span></summary
  >
  <!-- An action chosen closes the menu; what it opens takes the focus. -->
  <!-- svelte-ignore a11y_click_events_have_key_events, a11y_no_static_element_interactions -->
  <div class="row-menu-items" style:top="{at.top}px" style:right="{at.right}px" onclick={(e) => (e.target as HTMLElement).closest('button, a') && close()}>
    {@render children()}
  </div>
</details>

<style>
  .row-menu {
    position: relative;
    display: inline-block;
  }
  /* Drawn as the row's other small buttons are. */
  summary {
    list-style: none;
    display: inline-flex;
    align-items: center;
    gap: 0.3rem;
    cursor: pointer;
    font-size: 0.78rem;
    padding: 0.16rem 0.45rem;
    min-height: 24px;
    border: 1px solid var(--control);
    border-radius: 7px;
    background: var(--panel);
  }
  summary:hover,
  .row-menu[open] > summary {
    background: var(--surface2);
  }
  summary::-webkit-details-marker {
    display: none;
  }
  .row-menu-items {
    position: fixed;
    z-index: 40;
    display: flex;
    flex-direction: column;
    min-width: 11rem;
    padding: 0.3rem;
    background: var(--panel);
    border: 1px solid var(--line);
    border-radius: 10px;
    box-shadow: 0 10px 24px rgb(0 0 0 / 0.25);
  }
  .row-menu-items :global(button),
  .row-menu-items :global(a.button) {
    justify-content: flex-start;
    width: 100%;
    margin: 0;
    border-color: transparent;
    background: transparent;
    text-align: left;
  }
  .row-menu-items :global(button:hover:not(:disabled)),
  .row-menu-items :global(a.button:hover) {
    background: var(--surface2);
  }
</style>

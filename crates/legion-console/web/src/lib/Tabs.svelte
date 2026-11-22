<script lang="ts" generics="Id extends string">
  // A row of tabs (WAI-ARIA's tabs pattern): one in the Tab order, the
  // arrow keys, Home and End to move between them. A tab can carry a mark
  // saying something was filled in on it. The panel is the caller's, with
  // the id `panelId` and labelled by the tab `{idPrefix}-{tab}`.
  type Tab = { id: Id; label: string; marked?: boolean };
  let {
    tabs,
    tab = $bindable(),
    label,
    idPrefix = 'tab',
    panelId = 'panel',
    markLabel = ''
  }: {
    tabs: Tab[];
    tab: Id;
    label: string;
    idPrefix?: string;
    panelId?: string;
    /** What a tab's mark says to a screen reader. */
    markLabel?: string;
  } = $props();

  let list: HTMLElement | undefined = $state();
  function onKey(e: KeyboardEvent) {
    const i = tabs.findIndex((x) => x.id === tab);
    const to =
      e.key === 'ArrowRight'
        ? (i + 1) % tabs.length
        : e.key === 'ArrowLeft'
          ? (i - 1 + tabs.length) % tabs.length
          : e.key === 'Home'
            ? 0
            : e.key === 'End'
              ? tabs.length - 1
              : -1;
    if (to < 0) return;
    e.preventDefault();
    tab = tabs[to].id;
    list?.querySelectorAll<HTMLElement>('[role="tab"]')[to]?.focus();
  }
</script>

<div class="tabs" role="tablist" aria-label={label} bind:this={list}>
  {#each tabs as x (x.id)}
    <button
      type="button"
      role="tab"
      id="{idPrefix}-{x.id}"
      aria-selected={tab === x.id}
      aria-controls={panelId}
      tabindex={tab === x.id ? 0 : -1}
      class:here={tab === x.id}
      onclick={() => (tab = x.id)}
      onkeydown={onKey}
      aria-describedby={x.marked && markLabel ? `${idPrefix}-mark` : undefined}
      >{x.label}{#if x.marked}<span class="tab-mark" aria-hidden="true"></span>{/if}</button
    >
  {/each}
</div>
<!-- Said with a marked tab, kept out of its name so the name stays its label. -->
<span id="{idPrefix}-mark" hidden>{markLabel}</span>

<style>
  .tab-mark {
    display: inline-block;
    width: 0.4rem;
    height: 0.4rem;
    margin-left: 0.35rem;
    border-radius: 50%;
    background: var(--link);
    vertical-align: 0.15em;
  }
  @media (forced-colors: active) {
    .tab-mark {
      background: CanvasText;
    }
  }
</style>

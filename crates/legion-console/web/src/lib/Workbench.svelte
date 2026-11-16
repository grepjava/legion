<script lang="ts">
  // Persistent Multi-Tab Bottom Workbench Drawer: keeps live TaskLogs,
  // interactive xterm.js jail shells, Build logs, and Watch Events open
  // across page navigations.
  import { onMount } from 'svelte';
  import BuildLog from './BuildLog.svelte';
  import Events from './Events.svelte';
  import TaskLogs from './TaskLogs.svelte';
  import { t } from './i18n.svelte.ts';
  import { closeWorkbench, session } from './session.svelte.ts';

  const STORAGE_KEY = 'legion.workbenchHeight';
  const MIN_HEIGHT = 140;
  const DEFAULT_HEIGHT = 320;

  let tablist: HTMLElement | undefined = $state();
  let dockHeight = $state(DEFAULT_HEIGHT);
  let windowHeight = $state(0);
  // The separator's range (WAI-ARIA): 0 when minimized, else the dock's
  // height, at most what the window leaves.
  const maxHeight = $derived(windowHeight ? Math.max(260, windowHeight - 140) : 720);
  let resizing = $state(false);
  let dragStartY = 0;
  let dragStartHeight = DEFAULT_HEIGHT;

  const active = $derived(
    session.workbench.find((x) => x.id === session.activeTab) ?? session.workbench[0] ?? null
  );

  function clampHeight(px: number): number {
    const max = typeof window !== 'undefined' ? Math.max(260, window.innerHeight - 140) : maxHeight;
    return Math.round(Math.min(max, Math.max(MIN_HEIGHT, px)));
  }

  function persistHeight(px: number) {
    try {
      localStorage.setItem(STORAGE_KEY, String(px));
    } catch {
      /* ignore storage errors */
    }
  }

  onMount(() => {
    try {
      const saved = Number(localStorage.getItem(STORAGE_KEY));
      if (Number.isFinite(saved) && saved >= MIN_HEIGHT) {
        dockHeight = clampHeight(saved);
      }
    } catch {
      /* ignore */
    }
  });

  function onResizeDown(e: PointerEvent) {
    if (e.button !== 0) return;
    e.preventDefault();
    const target = e.currentTarget as HTMLElement;
    target.setPointerCapture(e.pointerId);
    resizing = true;
    dragStartY = e.clientY;
    dragStartHeight = session.workbenchMinimized ? MIN_HEIGHT : dockHeight;
    if (session.workbenchMinimized) {
      session.workbenchMinimized = false;
    }
  }

  function onResizeMove(e: PointerEvent) {
    if (!resizing) return;
    const delta = dragStartY - e.clientY;
    dockHeight = clampHeight(dragStartHeight + delta);
  }

  function onResizeUp(e: PointerEvent) {
    if (!resizing) return;
    resizing = false;
    try {
      (e.currentTarget as HTMLElement).releasePointerCapture(e.pointerId);
    } catch {
      /* ignore */
    }
    persistHeight(dockHeight);
  }

  function onResizeKey(e: KeyboardEvent) {
    const step = e.shiftKey ? 64 : 24;
    if (e.key === 'ArrowUp') {
      e.preventDefault();
      session.workbenchMinimized = false;
      dockHeight = clampHeight(dockHeight + step);
      persistHeight(dockHeight);
    } else if (e.key === 'ArrowDown') {
      e.preventDefault();
      dockHeight = clampHeight(dockHeight - step);
      persistHeight(dockHeight);
    }
  }

  function onTabKey(e: KeyboardEvent) {
    const tabs = session.workbench;
    if (!tabs.length || !active) return;
    const i = tabs.findIndex((x) => x.id === active.id);
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
    session.activeTab = tabs[to].id;
    session.workbenchMinimized = false;
    tablist?.querySelectorAll<HTMLElement>('[role="tab"]')[to]?.focus();
  }
</script>

<svelte:window bind:innerHeight={windowHeight} />

{#if session.workbench.length}
  <section class="workbench" class:resizing aria-label={t('Operator workbench')}>
    <!-- svelte-ignore a11y_no_noninteractive_tabindex -->
    <!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
    <div
      class="workbench-resizer"
      role="separator"
      aria-orientation="horizontal"
      aria-label={t('Resize workbench')}
      aria-valuemin={0}
      aria-valuemax={maxHeight}
      aria-valuenow={session.workbenchMinimized ? 0 : Math.min(dockHeight, maxHeight)}
      tabindex="0"
      onpointerdown={onResizeDown}
      onpointermove={onResizeMove}
      onpointerup={onResizeUp}
      onpointercancel={onResizeUp}
      onkeydown={onResizeKey}
    >
      <span class="workbench-grip" aria-hidden="true"></span>
    </div>
    <div class="workbench-bar">
      <div
        class="tabs wb-tabs"
        role="tablist"
        aria-label={t('Workbench tabs')}
        bind:this={tablist}
      >
        {#each session.workbench as item (item.id)}
          <button
            type="button"
            role="tab"
            id="wb-tab-{encodeURIComponent(item.id)}"
            aria-selected={active?.id === item.id}
            tabindex={active?.id === item.id ? 0 : -1}
            onclick={() => {
              session.activeTab = item.id;
              session.workbenchMinimized = false;
            }}
            onkeydown={onTabKey}
          >
            {item.title}
          </button>
        {/each}
      </div>
      <div class="wb-actions">
        {#if active}
          <button
            type="button"
            class="sm"
            onclick={() => closeWorkbench(active.id)}
            aria-label={t('Close {title}', { title: active.title })}
          >
            {t('Close tab')}
          </button>
        {/if}
        <button
          type="button"
          class="sm"
          aria-expanded={!session.workbenchMinimized}
          onclick={() => (session.workbenchMinimized = !session.workbenchMinimized)}
        >
          {session.workbenchMinimized ? t('Expand') : t('Minimize')}
        </button>
        <button
          type="button"
          class="sm"
          onclick={() => {
            session.workbench = [];
            session.activeTab = '';
          }}
        >
          {t('Close all')}
        </button>
      </div>
    </div>

    {#if active && !session.workbenchMinimized}
      <div
        class="workbench-body"
        style:height="{dockHeight}px"
        role="tabpanel"
        aria-labelledby="wb-tab-{encodeURIComponent(active.id)}"
      >
        {#key active.id}
          {#if active.kind === 'logs' && active.object}
            <TaskLogs task={active.object} />
          {:else if active.kind === 'terminal' && active.object}
            {#await import('#lib/Terminal.svelte') then Terminal}
              <Terminal.default task={active.object} />
            {/await}
          {:else if active.kind === 'build' && active.object}
            <BuildLog build={active.object} />
          {:else if active.kind === 'events'}
            <Events
              kind={active.targetKind ?? ''}
              namespace={active.namespace}
              name={active.name}
            />
          {/if}
        {/key}
      </div>
    {/if}
  </section>
{/if}

<style>
  .wb-tabs {
    margin: 0;
    border-bottom: none;
    flex: 1;
  }
  .wb-actions {
    display: flex;
    gap: 0.35rem;
    align-items: center;
  }
</style>

<script lang="ts">
  // Persistent Multi-Tab Bottom Workbench Drawer: keeps live TaskLogs,
  // interactive xterm.js jail shells, Build logs, and Watch Events open
  // across page navigations. Under it, always there, a thin bar (CONSOLE.md
  // phase 10): what runs now, else the last thing done and how it ended,
  // opening into the Activity tab.
  import { onMount } from 'svelte';
  import Activity from './Activity.svelte';
  import { activity, running, sent } from './activity.svelte.ts';
  import BuildLog from './BuildLog.svelte';
  import Events from './Events.svelte';
  import TaskLogs from './TaskLogs.svelte';
  import { t } from './i18n.svelte.ts';
  import { closeWorkbench, moveWorkbench, openWorkbench, session } from './session.svelte.ts';
  import { sortable } from './drop.svelte.ts';
  import Ico from './Ico.svelte';
  import { toolIcon } from './icons.ts';

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

  // The bar: what runs, else the newest of what ended and what was done here.
  let now = $state(Date.now());
  $effect(() => {
    const tick = setInterval(() => (now = Date.now()), 5000);
    return () => clearInterval(tick);
  });
  const runs = $derived(running(activity.ops));
  const newest = $derived(runs[runs.length - 1]);
  const last = $derived.by(() => {
    const ended = activity.ops
      .filter((o) => o.endedMs)
      .map((o) => ({
        at: o.endedMs ?? 0,
        text: o.message ? `${o.what}: ${o.state}, ${o.message}` : `${o.what}: ${o.state}`,
        tone: o.state === 'succeeded' ? 'good' : o.state === 'failed' ? 'bad' : ''
      }));
    const done = activity.done.map((d) => ({
      at: d.atMs,
      text: d.text,
      tone: d.error ? 'bad' : 'good'
    }));
    return [...ended, ...done].sort((a, b) => b.at - a.at)[0] ?? null;
  });
  const ago = (ms: number) => {
    const s = Math.max(0, Math.round((now - ms) / 1000));
    if (s < 60) return t('just now');
    if (s < 3600) return t('{n} min ago', { n: Math.round(s / 60) });
    return t('{n} h ago', { n: Math.round(s / 3600) });
  };
  const activityOpen = $derived(
    session.activeTab === 'activity' &&
      !session.workbenchMinimized &&
      session.workbench.some((x) => x.id === 'activity')
  );
  function toggleActivity() {
    if (activityOpen) {
      session.workbenchMinimized = true;
      return;
    }
    openWorkbench({ id: 'activity', kind: 'activity', title: t('Activity'), namespace: '', name: '' });
  }

  function onTabKey(e: KeyboardEvent) {
    // Alt with an arrow moves the tab instead (sortable).
    if (e.altKey) return;
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

<section class="workbench" class:resizing aria-label={t('Operator workbench')}>
  {#if session.workbench.length}
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
        {#each session.workbench as item, i (item.id)}
          <button
            type="button"
            role="tab"
            use:sortable={{ group: 'workbench', index: i, count: session.workbench.length, move: moveWorkbench }}
            aria-keyshortcuts="Alt+ArrowLeft Alt+ArrowRight"
            id="wb-tab-{encodeURIComponent(item.id)}"
            aria-selected={active?.id === item.id}
            tabindex={active?.id === item.id ? 0 : -1}
            onclick={() => {
              session.activeTab = item.id;
              session.workbenchMinimized = false;
            }}
            onkeydown={onTabKey}
          >
            <Ico icon={toolIcon(item.kind)} size={14} />
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
          {:else if active.kind === 'activity'}
            <Activity />
          {/if}
        {/key}
      </div>
    {/if}
  {/if}
  <div class="wb-status">
    <button
      type="button"
      class="wb-status-open"
      aria-expanded={activityOpen}
      onclick={toggleActivity}
      title={t('Activity: what runs and what was done')}
    >
      <Ico icon={toolIcon('activity')} size={14} />
      {#if newest}
        <span class="dot wait wb-pulse" aria-hidden="true"></span>
        <span class="wb-what">
          {runs.length === 1
            ? t('{what}: {stage}', { what: newest.what, stage: newest.stage })
            : t('{n} running; {what}: {stage}', {
                n: runs.length,
                what: newest.what,
                stage: newest.stage
              })}
          {#if newest.bytes}
            <span class="muted">({sent(newest.bytes)})</span>
          {/if}
        </span>
      {:else if last}
        <span class="dot {last.tone}" aria-hidden="true"></span>
        <span class="wb-what">{last.text}</span>
        <span class="muted">· {ago(last.at)}</span>
      {:else}
        <span class="muted">{t('Nothing has run lately')}</span>
      {/if}
    </button>
    {#if activity.error}
      <span class="error wb-error" title={activity.error}>{t('activity not current')}</span>
    {/if}
  </div>
</section>

<style>
  .wb-status {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    min-height: 26px;
    padding: 0 0.6rem;
    border-top: 1px solid var(--line);
    background: var(--side);
    font-size: 0.78rem;
  }
  .wb-status-open {
    display: flex;
    align-items: center;
    gap: 0.45rem;
    min-width: 0;
    flex: 1;
    background: none;
    border: none;
    padding: 0.15rem 0;
    color: var(--text);
    font: inherit;
    text-align: left;
    cursor: pointer;
    min-height: 24px;
  }
  .wb-status-open:hover .wb-what {
    text-decoration: underline;
  }
  .wb-what {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .wb-error {
    white-space: nowrap;
  }
  @media (prefers-reduced-motion: no-preference) {
    .wb-pulse {
      animation: wb-pulse 1.6s ease-in-out infinite;
    }
  }
  @keyframes wb-pulse {
    50% {
      opacity: 0.35;
    }
  }
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

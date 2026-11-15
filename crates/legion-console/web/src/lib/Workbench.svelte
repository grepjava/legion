<script lang="ts">
  // Persistent Multi-Tab Bottom Workbench Drawer: keeps live TaskLogs,
  // interactive xterm.js jail shells, Build logs, and Watch Events open
  // across page navigations.
  import BuildLog from './BuildLog.svelte';
  import Events from './Events.svelte';
  import TaskLogs from './TaskLogs.svelte';
  import { t } from './i18n.svelte.ts';
  import { closeWorkbench, session } from './session.svelte.ts';

  let tablist: HTMLElement | undefined = $state();
  const active = $derived(
    session.workbench.find((x) => x.id === session.activeTab) ?? session.workbench[0] ?? null
  );

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

{#if session.workbench.length}
  <section class="workbench" aria-label={t('Operator workbench')}>
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

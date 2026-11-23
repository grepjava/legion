<script lang="ts" module>
  import type { IconComponent } from '#lib/icons.ts';

  /** One entry of a context menu: an action, a submenu, or a line. */
  export interface MenuItem {
    label: string;
    icon?: IconComponent;
    /** What it does; the menu closes first. */
    run?: () => void;
    /** Why it can't be chosen now, shown as its title; absent if it can. */
    disabled?: string;
    /** Drawn as danger (stop, delete). */
    danger?: boolean;
    /** A submenu, opened beside it (a node to move to, say). */
    children?: MenuItem[];
    /** A line above it. */
    separated?: boolean;
  }
</script>

<script lang="ts">
  // A context menu at a point (right-click, or the menu key on what has
  // focus): a list of actions, a submenu opening beside its entry. Arrow
  // keys move, Right opens a submenu, Left or Escape goes back, Enter
  // chooses; a click outside, scrolling or Escape closes it, focus going
  // back to what it was opened on (WAI-ARIA menu pattern).
  import { tick } from 'svelte';
  import Ico from '#lib/Ico.svelte';
  import { ChevronRight } from '@lucide/svelte';

  let {
    x,
    y,
    items,
    label,
    onclose
  }: { x: number; y: number; items: MenuItem[]; label: string; onclose: () => void } = $props();

  let menu: HTMLUListElement | undefined = $state();
  let open = $state(-1);
  let left = $state(0);
  let top = $state(0);
  const returnTo = document.activeElement as HTMLElement | null;

  // Kept on screen, then the first entry focused.
  $effect(() => {
    void [x, y];
    tick().then(() => {
      if (!menu) return;
      const r = menu.getBoundingClientRect();
      left = Math.max(4, Math.min(x, window.innerWidth - r.width - 4));
      top = Math.max(4, Math.min(y, window.innerHeight - r.height - 4));
      entries(menu)[0]?.focus();
    });
  });

  const entries = (ul: HTMLElement) =>
    [...ul.querySelectorAll<HTMLElement>(':scope > li > [role=menuitem]')].filter(
      (e) => e.getAttribute('aria-disabled') !== 'true'
    );

  function close() {
    onclose();
    returnTo?.focus?.();
  }

  function choose(item: MenuItem) {
    if (item.disabled || !item.run) return;
    close();
    item.run();
  }

  function key(e: KeyboardEvent, list: HTMLElement, sub: boolean) {
    const all = entries(list);
    const at = all.indexOf(document.activeElement as HTMLElement);
    const move = (to: number) => all[(to + all.length) % all.length]?.focus();
    switch (e.key) {
      case 'ArrowDown':
        move(at + 1);
        break;
      case 'ArrowUp':
        move(at - 1);
        break;
      case 'Home':
        move(0);
        break;
      case 'End':
        move(all.length - 1);
        break;
      case 'ArrowRight': {
        const i = Number((document.activeElement as HTMLElement)?.dataset.index ?? -1);
        if (!sub && items[i]?.children?.length) {
          open = i;
          tick().then(() => {
            const ul = menu?.querySelector<HTMLElement>(`[data-sub="${i}"]`);
            if (ul) entries(ul)[0]?.focus();
          });
        }
        break;
      }
      case 'ArrowLeft':
        if (sub) {
          const i = open;
          open = -1;
          tick().then(() => menu?.querySelector<HTMLElement>(`[data-index="${i}"]`)?.focus());
        }
        break;
      case 'Escape':
        if (sub) {
          const i = open;
          open = -1;
          tick().then(() => menu?.querySelector<HTMLElement>(`[data-index="${i}"]`)?.focus());
        } else close();
        break;
      case 'Tab':
        close();
        break;
      default:
        return;
    }
    e.preventDefault();
    e.stopPropagation();
  }
</script>

<svelte:window
  onpointerdown={(e) => {
    if (menu && !menu.contains(e.target as Node)) close();
  }}
  onscroll={close}
  onresize={close}
/>

<ul
  class="context-menu card"
  role="menu"
  aria-label={label}
  bind:this={menu}
  style:left="{left}px"
  style:top="{top}px"
  onkeydown={(e) => key(e, menu!, false)}
  oncontextmenu={(e) => e.preventDefault()}
>
  {#each items as item, i (item.label)}
    <li role="none" class:separated={item.separated}>
      <button
        type="button"
        role="menuitem"
        tabindex="-1"
        data-index={i}
        class:danger={item.danger}
        aria-disabled={item.disabled ? 'true' : undefined}
        aria-haspopup={item.children?.length ? 'menu' : undefined}
        aria-expanded={item.children?.length ? open === i : undefined}
        title={item.disabled ?? ''}
        onclick={() => (item.children?.length ? (open = open === i ? -1 : i) : choose(item))}
        onpointerenter={() => (open = item.children?.length ? i : -1)}
      >
        {#if item.icon}<Ico icon={item.icon} size={14} />{:else}<span class="no-icon"></span>{/if}
        <span class="label">{item.label}</span>
        {#if item.children?.length}<Ico icon={ChevronRight} size={14} />{/if}
      </button>
      {#if item.children?.length && open === i}
        <ul
          class="context-menu sub card"
          role="menu"
          aria-label={item.label}
          data-sub={i}
          onkeydown={(e) => key(e, e.currentTarget, true)}
        >
          {#each item.children as child (child.label)}
            <li role="none">
              <button
                type="button"
                role="menuitem"
                tabindex="-1"
                class:danger={child.danger}
                aria-disabled={child.disabled ? 'true' : undefined}
                title={child.disabled ?? ''}
                onclick={() => choose(child)}
              >
                {#if child.icon}<Ico icon={child.icon} size={14} />{:else}<span class="no-icon"></span>{/if}
                <span class="label">{child.label}</span>
              </button>
            </li>
          {/each}
        </ul>
      {/if}
    </li>
  {/each}
</ul>

<style>
  .context-menu {
    position: fixed;
    z-index: 50;
    min-width: 13rem;
    margin: 0;
    padding: 0.3rem;
    list-style: none;
    box-shadow: 0 8px 24px rgb(0 0 0 / 0.35);
  }
  .context-menu li {
    position: relative;
  }
  .context-menu li.separated {
    border-top: 1px solid var(--line);
    margin-top: 0.25rem;
    padding-top: 0.25rem;
  }
  .context-menu button {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    width: 100%;
    padding: 0.4rem 0.6rem;
    border: 0;
    border-radius: 6px;
    background: none;
    color: var(--text);
    text-align: left;
    font: inherit;
    cursor: pointer;
  }
  .context-menu button:hover,
  .context-menu button:focus-visible {
    background: color-mix(in oklab, var(--primary) 18%, transparent);
    outline: none;
  }
  .context-menu button[aria-disabled='true'] {
    color: var(--muted);
    cursor: not-allowed;
  }
  .context-menu button.danger:not([aria-disabled='true']) {
    color: var(--bad);
  }
  .context-menu .label {
    flex: 1;
  }
  .context-menu .no-icon {
    width: 14px;
  }
  .sub {
    position: absolute;
    left: 100%;
    top: -0.3rem;
  }
</style>

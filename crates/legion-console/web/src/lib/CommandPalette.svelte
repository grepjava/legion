<script lang="ts">
  // Global ⌘K / Ctrl+K Command Palette: jumps to any studio, resource kind,
  // live cluster object, or Docks live logs / shell into the Workbench.
  import { goto } from '$app/navigation';
  import { api, type LObject } from './api.ts';
  import { t } from './i18n.svelte.ts';
  import { plural } from './kinds.ts';
  import { cannotWrite, openWorkbench, session, setDensity, setTheme } from './session.svelte.ts';
  import { page } from '$app/state';
  import { prefs } from './prefs.svelte.ts';
  import Ico from './Ico.svelte';
  import { actionIcon, kindIcon, pageIcon, type IconComponent } from './icons.ts';

  interface Item {
    id: string;
    group: string;
    label: string;
    detail: string;
    run: () => void;
  }

  /** Each item's icon: the page's or kind's it leads to, else its action's. */
  function iconOf(item: Item): IconComponent {
    const [what, rest] = [item.id.slice(0, item.id.indexOf(':')), item.id.slice(item.id.indexOf(':') + 1)];
    if (what === 'nav') return pageIcon(rest);
    if (what === 'pin' || what === 'recent') return pageIcon(item.detail);
    if (what === 'kind' || what === 'obj') return kindIcon(rest.split(':')[0]);
    return actionIcon(item.id);
  }

  let dialog: HTMLDialogElement | undefined = $state();
  let input: HTMLInputElement | undefined = $state();
  let query = $state('');
  let index = $state(0);
  let objects: LObject[] = $state([]);
  let list: HTMLUListElement | undefined = $state();

  // Light-dismiss fallback for browsers without native <dialog closedby="any">
  function onDialogClick(event: MouseEvent) {
    if (!dialog || 'closedBy' in HTMLDialogElement.prototype) return;
    if (event.target !== dialog) return;
    const rect = dialog.getBoundingClientRect();
    const inside =
      rect.top <= event.clientY &&
      event.clientY <= rect.top + rect.height &&
      rect.left <= event.clientX &&
      event.clientX <= rect.left + rect.width;
    if (!inside) dialog.close();
  }

  $effect(() => {
    if (!dialog) return;
    if (session.paletteOpen) {
      if (!dialog.open) {
        query = '';
        index = 0;
        dialog.showModal();
        queueMicrotask(() => input?.focus());
        loadObjects();
      }
    } else if (dialog.open) {
      dialog.close();
    }
  });

  async function loadObjects() {
    if (!session.me) return;
    const ns = session.namespace;
    const kinds = ['Service', 'Task', 'Volume', 'Build', ...(session.me.all ? ['Node'] : [])];
    const settled = await Promise.allSettled(
      kinds.map((k) => api.list(k, k === 'Node' ? '' : ns))
    );
    const all: LObject[] = [];
    for (const r of settled) {
      if (r.status === 'fulfilled') all.push(...r.value.objects);
    }
    objects = all;
  }

  function pick(fn: () => void) {
    session.paletteOpen = false;
    fn();
  }

  const items: Item[] = $derived.by(() => {
    const navGroup = t('Navigation');
    const objGroup = t('Cluster objects');
    const actGroup = t('Quick actions');

    // The pages the user pinned, then those they visited last (not the
    // one shown, nor one already pinned).
    const here = page.url.pathname + page.url.search;
    const pins = prefs.pins.map((p) => ({
      id: `pin:${p.href}`,
      group: t('Pinned'),
      label: p.title,
      detail: p.href,
      run: () => goto(p.href)
    }));
    const recent = prefs.recent
      .filter((r) => r.href !== here && !prefs.pins.some((p) => p.href === r.href))
      .map((r) => ({
        id: `recent:${r.href}`,
        group: t('Recent'),
        label: r.title,
        detail: r.href,
        run: () => goto(r.href)
      }));
    const base: Item[] = [
      ...pins,
      ...recent,
      { id: 'nav:/', group: navGroup, label: t('Overview'), detail: 'g o', run: () => goto('/') },
      { id: 'nav:/stacks', group: navGroup, label: t('Stacks'), detail: 'compose apps: stop / start / remove together', run: () => goto('/stacks') },
      { id: 'nav:/diagnostics', group: navGroup, label: t('Diagnostics'), detail: 'legion doctor on every node / support bundle', run: () => goto('/diagnostics') },
      { id: 'nav:/disk', group: navGroup, label: t('Disk usage'), detail: 'images / tasks / volumes / builds', run: () => goto('/disk') },
      { id: 'nav:/volumes', group: navGroup, label: t('Volumes'), detail: 'g v · ZFS / snapshots / migrate', run: () => goto('/volumes') },
      { id: 'nav:/images', group: navGroup, label: t('Images'), detail: 'g i · OCI / search / build / push', run: () => goto('/images') },
      { id: 'nav:/topology', group: navGroup, label: t('Topology'), detail: 'g t · pf reachability & VIPs', run: () => goto('/topology') },
      { id: 'nav:/apply', group: navGroup, label: t('Apply YAML'), detail: 'g y · live dry-run diff', run: () => goto('/apply') },
      { id: 'nav:/deploy', group: navGroup, label: t('Deploy'), detail: '/deploy', run: () => goto('/deploy') },
      { id: 'nav:/templates', group: navGroup, label: t('Templates'), detail: '/templates', run: () => goto('/templates') },
      { id: 'nav:/import', group: navGroup, label: t('Import compose'), detail: '/import', run: () => goto('/import') },
      { id: 'nav:/network', group: navGroup, label: t('Network'), detail: '/network', run: () => goto('/network') },
      { id: 'nav:/alerts', group: navGroup, label: t('Alerts'), detail: 'g a', run: () => goto('/alerts') }
    ];
    if (session.me?.all) {
      base.push(
        { id: 'nav:/cluster', group: navGroup, label: t('Operations'), detail: '/cluster', run: () => goto('/cluster') },
        { id: 'nav:/audit', group: navGroup, label: t('Audit'), detail: '/audit', run: () => goto('/audit') },
        { id: 'nav:/settings', group: navGroup, label: t('Console settings'), detail: '/settings', run: () => goto('/settings') }
      );
    }
    for (const k of session.kinds) {
      if (k.clusterScoped && !session.me?.all) continue;
      base.push({
        id: `kind:${k.kind}`,
        group: navGroup,
        label: plural(k.kind),
        detail: `/k/${k.kind}`,
        run: () => goto(`/k/${k.kind}`)
      });
    }
    for (const o of objects) {
      const ns = o.metadata.namespace ?? '';
      const path = `/k/${encodeURIComponent(o.kind)}/${encodeURIComponent(ns || '_')}/${encodeURIComponent(o.metadata.name)}`;
      base.push({
        id: `obj:${o.kind}:${ns}:${o.metadata.name}`,
        group: objGroup,
        label: `${o.kind} ${ns ? `${ns}/` : ''}${o.metadata.name}`,
        detail: String(o.status?.state ?? ''),
        run: () => goto(path)
      });
      if (o.kind === 'Task') {
        base.push({
          id: `logs:${ns}:${o.metadata.name}`,
          group: actGroup,
          label: t('Keep logs open: {name}', { name: `${ns}/${o.metadata.name}` }),
          detail: t('Workbench'),
          run: () =>
            openWorkbench({
              id: `logs:${ns}/${o.metadata.name}`,
              kind: 'logs',
              title: `${o.metadata.name} · ${t('Logs')}`,
              namespace: ns,
              name: o.metadata.name,
              object: o
            })
        });
        if (o.status?.state === 'running' && !cannotWrite(ns)) {
          base.push({
            id: `term:${ns}:${o.metadata.name}`,
            group: actGroup,
            label: t('Open jail shell: {name}', { name: `${ns}/${o.metadata.name}` }),
            detail: t('Workbench'),
            run: () =>
              openWorkbench({
                id: `term:${ns}/${o.metadata.name}`,
                kind: 'terminal',
                title: `${o.metadata.name} · ${t('Terminal')}`,
                namespace: ns,
                name: o.metadata.name,
                object: o
              })
          });
        }
      }
    }
    base.push(
      { id: 'theme:dark', group: actGroup, label: t('Switch to dark theme'), detail: t('Theme'), run: () => setTheme('dark') },
      { id: 'theme:light', group: actGroup, label: t('Switch to light theme'), detail: t('Theme'), run: () => setTheme('light') },
      { id: 'theme:system', group: actGroup, label: t('Follow system theme'), detail: t('Theme'), run: () => setTheme('system') },
      {
        id: 'density:toggle',
        group: actGroup,
        label: session.density === 'compact' ? t('Switch to comfortable density') : t('Switch to compact density'),
        detail: t('Density'),
        run: () => setDensity(session.density === 'compact' ? 'comfortable' : 'compact')
      }
    );

    // Every word typed, somewhere in the item; those whose name starts
    // with the first word first.
    const words = query.trim().toLowerCase().split(/\s+/).filter(Boolean);
    if (!words.length) return base.slice(0, 40);
    const text = (x: Item) => `${x.label} ${x.detail} ${x.group}`.toLowerCase();
    const found = base.filter((x) => words.every((w) => text(x).includes(w)));
    const first = (x: Item) => (x.label.toLowerCase().startsWith(words[0]) ? 0 : 1);
    return found
      .map((x, i) => ({ x, i }))
      .sort((a, b) => first(a.x) - first(b.x) || a.i - b.i)
      .map(({ x }) => x)
      .slice(0, 40);
  });

  $effect(() => {
    if (index >= items.length) index = 0;
  });
  // The chosen item kept in view as the arrows move through a long list.
  $effect(() => {
    list?.children[index]?.scrollIntoView({ block: 'nearest' });
  });

  function onKey(e: KeyboardEvent) {
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      index = items.length ? (index + 1) % items.length : 0;
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      index = items.length ? (index - 1 + items.length) % items.length : 0;
    } else if (e.key === 'Enter' && items[index]) {
      e.preventDefault();
      pick(items[index].run);
    }
  }
</script>

<dialog
  bind:this={dialog}
  closedby="any"
  aria-labelledby="cmd-palette-title"
  onclose={() => (session.paletteOpen = false)}
  onclick={onDialogClick}
  class="palette"
>
  <header class="palette-head">
    <h2 id="cmd-palette-title">{t('Command palette')}</h2>
    <button type="button" class="sm" onclick={() => dialog?.close()}>{t('Close')}</button>
  </header>
  <input
    bind:this={input}
    type="search"
    bind:value={query}
    onkeydown={onKey}
    placeholder={t('Jump to a service, task, volume, studio, or action…')}
    aria-label={t('Filter commands')}
    class="palette-input"
  />
  {#if items.length}
    <ul class="palette-list" role="list" bind:this={list}>
      {#each items as item, i (item.id)}
        <li>
          <button
            type="button"
            class="palette-item"
            class:selected={i === index}
            onpointerenter={() => (index = i)}
            onclick={() => pick(item.run)}
          >
            <span class="palette-label">
              <Ico icon={iconOf(item)} />
              <span class="muted group-tag">{item.group}</span>
              <strong>{item.label}</strong>
            </span>
            {#if item.detail}<span class="muted mono">{item.detail}</span>{/if}
          </button>
        </li>
      {/each}
    </ul>
  {:else}
    <p class="muted">{t('No matching commands or objects.')}</p>
  {/if}
</dialog>

<style>
  .palette-label {
    display: inline-flex;
    align-items: center;
    gap: 0.45rem;
    min-width: 0;
  }
  .palette-item.selected :global(.ico),
  .palette-item:hover :global(.ico) {
    color: var(--text);
  }
  .palette {
    padding: 0.9rem;
    max-width: min(38rem, 94vw);
  }
  .palette-head {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 0.5rem;
  }
  .palette-head h2 {
    margin: 0;
    font-size: 0.95rem;
  }
  .palette-input {
    width: 100%;
    padding: 0.5rem 0.65rem;
    margin-bottom: 0.5rem;
  }
  .palette-list {
    list-style: none;
    margin: 0;
    padding: 0;
    max-height: 22rem;
    overflow-y: auto;
    display: flex;
    flex-direction: column;
    gap: 0.15rem;
  }
  .palette-item {
    width: 100%;
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 0.75rem;
    border: 1px solid transparent;
    background: transparent;
    padding: 0.4rem 0.55rem;
    border-radius: 6px;
    text-align: left;
  }
  .palette-item.selected,
  .palette-item:hover {
    background: var(--surface2);
    border-color: var(--control);
  }
  .group-tag {
    font-size: 0.72rem;
    margin-right: 0.45rem;
    text-transform: uppercase;
  }
</style>

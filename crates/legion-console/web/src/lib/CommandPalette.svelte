<script lang="ts">
  // Global ⌘K / Ctrl+K Command Palette: jumps to any studio, resource kind,
  // live cluster object, or Docks live logs / shell into the Workbench.
  import { goto } from '$app/navigation';
  import { api, type LObject } from './api.ts';
  import { t } from './i18n.svelte.ts';
  import { plural } from './kinds.ts';
  import { openWorkbench, session, setDensity, setTheme } from './session.svelte.ts';

  interface Item {
    id: string;
    group: string;
    label: string;
    detail: string;
    run: () => void;
  }

  let dialog: HTMLDialogElement | undefined = $state();
  let input: HTMLInputElement | undefined = $state();
  let query = $state('');
  let index = $state(0);
  let objects: LObject[] = $state([]);

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

    const base: Item[] = [
      { id: 'nav:/', group: navGroup, label: t('Overview'), detail: 'g o', run: () => goto('/') },
      { id: 'nav:/stacks', group: navGroup, label: t('Stacks'), detail: 'compose apps: stop / start / remove together', run: () => goto('/stacks') },
      { id: 'nav:/diagnostics', group: navGroup, label: t('Diagnostics'), detail: 'legion doctor on every node / support bundle', run: () => goto('/diagnostics') },
      { id: 'nav:/disk', group: navGroup, label: t('Disk usage'), detail: 'images / tasks / volumes / builds', run: () => goto('/disk') },
      { id: 'nav:/volumes', group: navGroup, label: t('Volumes Studio'), detail: 'g v · ZFS / snapshots / migrate', run: () => goto('/volumes') },
      { id: 'nav:/images', group: navGroup, label: t('Images & GHCR Hub'), detail: 'g i · OCI / search / build / push', run: () => goto('/images') },
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
          label: t('Dock logs: {name}', { name: `${ns}/${o.metadata.name}` }),
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
        if (o.status?.state === 'running') {
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

    const q = query.trim().toLowerCase();
    const filtered = q
      ? base.filter(
          (x) =>
            x.label.toLowerCase().includes(q) ||
            x.detail.toLowerCase().includes(q) ||
            x.group.toLowerCase().includes(q)
        )
      : base;
    return filtered.slice(0, 40);
  });

  $effect(() => {
    if (index >= items.length) index = 0;
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
    <ul class="palette-list" role="list">
      {#each items as item, i (item.id)}
        <li>
          <button
            type="button"
            class="palette-item"
            class:selected={i === index}
            onpointerenter={() => (index = i)}
            onclick={() => pick(item.run)}
          >
            <span>
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

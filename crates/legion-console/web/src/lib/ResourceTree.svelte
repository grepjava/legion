<script lang="ts">
  // The side bar as a tree of the cluster's objects (CONSOLE.md, the
  // console's feel, 6): by namespace, each with its services, their tasks
  // and its Volumes; or by node, each with the tasks and Volumes on it.
  // Each has its kind's icon and a dot for its state, and leads to its page.
  // Fed by watches of its own; a WAI-ARIA tree, walked with the arrow keys.
  import { page } from '$app/state';
  import { goto } from '$app/navigation';
  import { follow, type LObject } from './api.ts';
  import { tone } from './columns.ts';
  import { t } from './i18n.svelte.ts';
  import Ico from './Ico.svelte';
  import { kindIcon } from './icons.ts';
  import { prefs, setBranchOpen } from './prefs.svelte.ts';
  import { session } from './session.svelte.ts';

  let { mode, filter = '' }: { mode: 'namespace' | 'node'; filter?: string } = $props();

  interface Item {
    key: string;
    label: string;
    href: string;
    kind: string;
    vm?: boolean;
    state?: string;
    detail?: string;
    children: Item[];
  }

  let services: LObject[] = $state([]);
  let tasks: LObject[] = $state([]);
  let volumes: LObject[] = $state([]);
  let nodes: LObject[] = $state([]);
  let error = $state('');

  $effect(() => {
    const ns = session.namespace;
    const fail = (e: Error) => (error = e.message);
    const stops = [
      follow('Service', ns, (o) => (services = o), fail),
      follow('Task', ns, (o) => (tasks = o), fail),
      follow('Volume', ns, (o) => (volumes = o), fail)
    ];
    if (session.me?.all) stops.push(follow('Node', '', (o) => (nodes = o), fail));
    return () => stops.forEach((s) => s());
  });

  const path = (o: LObject) =>
    `/k/${o.kind}/${encodeURIComponent(o.metadata.namespace || '_')}/${encodeURIComponent(o.metadata.name)}`;
  const isVm = (o: LObject) =>
    (o.spec?.pod?.containers ?? []).some((c: { vm?: unknown }) => c.vm);
  // The tasks worth showing: those meant to run, not the ended ones kept.
  const live = $derived(tasks.filter((x) => x.spec?.desiredState === 'running'));
  const taskItem = (x: LObject): Item => ({
    key: `Task/${x.metadata.namespace}/${x.metadata.name}`,
    label: x.metadata.name,
    href: path(x),
    kind: 'Task',
    vm: isVm(x),
    state: String(x.status?.state ?? ''),
    children: []
  });
  const volumeItem = (v: LObject): Item => ({
    key: `Volume/${v.metadata.namespace}/${v.metadata.name}`,
    label: v.metadata.name,
    href: path(v),
    kind: 'Volume',
    state: v.status?.lost ? 'failed' : v.status?.replica?.state === 'stale' ? 'stale' : '',
    children: []
  });
  // A service is ready when all it asks for are; waiting otherwise.
  const serviceState = (s: LObject) => {
    const want = Number(s.status?.replicas ?? s.spec?.replicas ?? 0);
    const ready = Number(s.status?.ready ?? 0);
    if (s.spec?.stopped) return 'paused';
    return want && ready >= want ? 'ready' : want ? 'pending' : '';
  };

  const roots = $derived.by((): Item[] => {
    if (mode === 'node') {
      return [...nodes]
        .sort((a, b) => a.metadata.name.localeCompare(b.metadata.name))
        .map((n) => ({
          key: `Node/${n.metadata.name}`,
          label: n.metadata.name,
          href: path(n),
          kind: 'Node',
          state: String(n.status?.state ?? ''),
          detail: n.spec?.role === 'manager' ? t('manager') : '',
          children: [
            ...live.filter((x) => x.spec?.node === n.metadata.name).map(taskItem),
            ...volumes.filter((v) => v.status?.node === n.metadata.name).map(volumeItem)
          ]
        }));
    }
    const names = [
      ...new Set([...services, ...live, ...volumes].map((o) => o.metadata.namespace ?? ''))
    ].sort();
    return names.map((ns) => ({
      key: `Namespace/${ns}`,
      label: ns,
      href: `/k/Namespace/_/${encodeURIComponent(ns)}`,
      kind: 'Namespace',
      children: [
        ...services
          .filter((s) => s.metadata.namespace === ns)
          .map((s) => ({
            key: `Service/${ns}/${s.metadata.name}`,
            label: s.metadata.name,
            href: path(s),
            kind: 'Service',
            state: serviceState(s),
            children: live
              .filter((x) => x.metadata.namespace === ns && x.spec?.service === s.metadata.name)
              .map(taskItem)
          })),
        ...live
          .filter((x) => x.metadata.namespace === ns && !x.spec?.service)
          .map(taskItem),
        ...volumes.filter((v) => v.metadata.namespace === ns).map(volumeItem)
      ]
    }));
  });

  // With a filter, what matches and the branches leading to it, opened.
  const needle = $derived(filter.trim().toLowerCase());
  function prune(items: Item[]): Item[] {
    if (!needle) return items;
    const out: Item[] = [];
    for (const i of items) {
      const kids = prune(i.children);
      if (kids.length || i.label.toLowerCase().includes(needle)) out.push({ ...i, children: kids });
    }
    return out;
  }
  const shown = $derived(prune(roots));
  const isOpen = (i: Item) => !!needle || prefs.treeOpen.includes(`${mode}:${i.key}`);

  // The rows as the keyboard walks them: every item whose branch is open.
  const rows = $derived.by(() => {
    const out: { item: Item; level: number; parent: string | null }[] = [];
    const walk = (items: Item[], level: number, parent: string | null) => {
      for (const i of items) {
        out.push({ item: i, level, parent });
        if (i.children.length && isOpen(i)) walk(i.children, level + 1, i.key);
      }
    };
    walk(shown, 1, null);
    return out;
  });
  let focused = $state('');
  const current = $derived(rows.find((r) => page.url.pathname === r.item.href)?.item.key ?? '');
  const tabStop = $derived(focused || current || rows[0]?.item.key || '');
  let tree: HTMLElement | undefined = $state();

  function focusRow(key: string) {
    focused = key;
    queueMicrotask(() =>
      tree?.querySelector<HTMLElement>(`[data-key="${CSS.escape(key)}"]`)?.focus()
    );
  }
  function toggle(i: Item, open = !isOpen(i)) {
    setBranchOpen(`${mode}:${i.key}`, open);
  }
  function onKey(e: KeyboardEvent, i: Item) {
    const at = rows.findIndex((r) => r.item.key === i.key);
    const row = rows[at];
    switch (e.key) {
      case 'ArrowDown':
        if (rows[at + 1]) focusRow(rows[at + 1].item.key);
        break;
      case 'ArrowUp':
        if (at > 0) focusRow(rows[at - 1].item.key);
        break;
      case 'ArrowRight':
        if (i.children.length && !isOpen(i)) toggle(i, true);
        else if (i.children.length) focusRow(i.children[0].key);
        break;
      case 'ArrowLeft':
        if (i.children.length && isOpen(i)) toggle(i, false);
        else if (row?.parent) focusRow(row.parent);
        break;
      case 'Home':
        if (rows[0]) focusRow(rows[0].item.key);
        break;
      case 'End':
        if (rows.length) focusRow(rows[rows.length - 1].item.key);
        break;
      case 'Enter':
        goto(i.href);
        break;
      default:
        return;
    }
    e.preventDefault();
  }
</script>

{#snippet branch(items: Item[], level: number)}
  {#each items as i (i.key)}
    {@const open = isOpen(i)}
    <li
      role="treeitem"
      aria-level={level}
      aria-expanded={i.children.length ? open : undefined}
      aria-selected={current === i.key}
      aria-current={current === i.key ? 'page' : undefined}
      tabindex={tabStop === i.key ? 0 : -1}
      data-key={i.key}
      onkeydown={(e) => {
        if (e.target === e.currentTarget) onKey(e, i);
      }}
      onfocus={(e) => {
        if (e.target === e.currentTarget) focused = i.key;
      }}
    >
      <div class="row" class:here={current === i.key} style:--level={level}>
        {#if i.children.length}
          <button
            type="button"
            class="twist"
            class:open
            tabindex="-1"
            aria-hidden="true"
            onclick={() => toggle(i)}
          ></button>
        {:else}
          <span class="twist none" aria-hidden="true"></span>
        {/if}
        <a href={i.href} tabindex="-1">
          <Ico icon={kindIcon(i.kind, i.vm)} size={15} />
          {#if i.state}<span class="dot {tone(i.state)}" title={i.state} aria-hidden="true"></span>{/if}
          <span class="label">{i.label}</span>
          {#if i.state}<span class="sr-only">, {i.state}</span>{/if}
          {#if i.detail}<span class="detail">{i.detail}</span>{/if}
          {#if i.children.length && !open}<span class="n">{i.children.length}</span>{/if}
        </a>
      </div>
      {#if i.children.length && open}
        <ul role="group">{@render branch(i.children, level + 1)}</ul>
      {/if}
    </li>
  {/each}
{/snippet}

{#if error}
  <p class="error small-error" role="alert">{error}</p>
{/if}
{#if shown.length}
  <ul
    class="tree"
    role="tree"
    bind:this={tree}
    aria-label={mode === 'node' ? t('Objects by node') : t('Objects by namespace')}
  >
    {@render branch(shown, 1)}
  </ul>
{:else if needle}
  <p class="muted tree-empty">{t('Nothing matches.')}</p>
{:else}
  <p class="muted tree-empty">{mode === 'node' ? t('No nodes to show.') : t('No objects to show yet.')}</p>
{/if}

<style>
  .tree,
  .tree ul {
    list-style: none;
    margin: 0;
    padding: 0;
  }
  .tree [role='treeitem'] {
    outline: none;
  }
  .row {
    display: flex;
    align-items: center;
    gap: 0.15rem;
    padding-left: calc((var(--level) - 1) * 0.9rem);
    border-radius: 6px;
    min-height: 30px;
  }
  .row:hover,
  .row.here {
    background: var(--surface2);
  }
  .row.here {
    box-shadow: inset 3px 0 0 var(--accent);
  }
  [role='treeitem']:focus-visible > .row {
    outline: 2px solid var(--link);
    outline-offset: -2px;
  }
  .row a {
    flex: 1;
    min-width: 0;
    display: flex;
    align-items: center;
    gap: 0.4rem;
    color: var(--text);
    font-size: 0.9rem;
    padding: 0.2rem 0.35rem 0.2rem 0;
    text-decoration: none;
  }
  .row a:hover :global(.ico),
  .row.here :global(.ico) {
    color: var(--text);
  }
  .label {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .detail,
  .n {
    margin-left: auto;
    font-size: 0.72rem;
    color: var(--muted);
  }
  .twist {
    flex: none;
    width: 1.1rem;
    height: 1.1rem;
    padding: 0;
    border: none;
    background: none;
    min-height: 0;
    cursor: pointer;
    position: relative;
  }
  .twist::before {
    content: '';
    position: absolute;
    left: 0.35rem;
    top: 0.32rem;
    width: 0.36rem;
    height: 0.36rem;
    border-right: 1.5px solid var(--muted);
    border-bottom: 1.5px solid var(--muted);
    transform: rotate(-45deg);
    transition: transform 140ms ease;
  }
  .twist.open::before {
    transform: rotate(45deg);
  }
  .twist.none::before {
    display: none;
  }
  .tree-empty {
    padding: 0.5rem;
    font-size: 0.88rem;
  }
  .small-error {
    font-size: 0.85rem;
  }
  @media (prefers-reduced-motion: reduce) {
    .twist::before {
      transition: none;
    }
  }
</style>

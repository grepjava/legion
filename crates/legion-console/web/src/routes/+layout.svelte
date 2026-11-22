<script lang="ts">
  import '../app.css';
  import { untrack } from 'svelte';
  import { afterNavigate, beforeNavigate, goto, onNavigate } from '$app/navigation';
  import { page } from '$app/state';
  import { fromAddress, keepInAddress } from '#lib/address.svelte.ts';
  import {
    api,
    consoleSettings,
    followActivity,
    followAlerts,
    followStatus,
    plugins,
    setToken,
    whenSignedOut,
    type Status
  } from '#lib/api.ts';
  import CommandPalette from '#lib/CommandPalette.svelte';
  import Confirm from '#lib/Confirm.svelte';
  import Toasts from '#lib/Toasts.svelte';
  import FreeBsdLogo from '#lib/FreeBsdLogo.svelte';
  import Shortcuts, { CHORDS } from '#lib/Shortcuts.svelte';
  import { routeDrop, sortable } from '#lib/drop.svelte.ts';
  import {
    movePin,
    prefs,
    setSectionOpen,
    setSidebarHidden,
    setTree,
    togglePin,
    unpin,
    visited,
    type TreeMode
  } from '#lib/prefs.svelte.ts';
  import Workbench from '#lib/Workbench.svelte';
  import { activity } from '#lib/activity.svelte.ts';
  import { plural } from '#lib/kinds.ts';
  import Ico from '#lib/Ico.svelte';
  import { drafts } from '#lib/drafts.svelte.ts';
  import ResourceTree from '#lib/ResourceTree.svelte';
  import { kindIcon, pageIcon, type IconComponent } from '#lib/icons.ts';
  import {
    CircleQuestionMark,
    GripVertical,
    MonitorCog,
    Moon,
    PanelLeft,
    Search,
    Star,
    Sun,
    X
  } from '@lucide/svelte';
  import {
    namespaces,
    session,
    setTheme,
    type ThemeMode
  } from '#lib/session.svelte.ts';
  import { i18n, languageName, languages, setLanguage, t, tn } from '#lib/i18n.svelte.ts';
  import { live } from '#lib/live.svelte.ts';

  let { children } = $props();
  let adminNamespaces: string[] = $state([]);
  let ready = $state(false);
  let raftStatus = $state(null as Status | null);

  whenSignedOut(() => {
    session.me = null;
    if (page.url.pathname !== '/login') goto('/login');
  });

  $effect(() => {
    if (page.url.pathname === '/login') {
      ready = true;
      return;
    }
    if (session.me) return;
    (async () => {
      try {
        session.me = await api.me();
        setToken(session.me.csrf);
        session.kinds = await api.kinds();
        plugins()
          .then((p) => (session.plugins = p.plugins))
          .catch(() => (session.plugins = []));
        if (session.me.all) {
          const ns = await api.list('Namespace');
          adminNamespaces = ns.objects.map((o) => o.metadata.name).sort();
        }
      } catch {
        // Not signed in: whenSignedOut took the user to sign in.
      } finally {
        ready = true;
      }
    })();
  });

  // Raft's leader, term and store version for the top bar, as they
  // change; the last known kept while the stream is back.
  $effect(() => {
    if (!session.me) {
      raftStatus = null;
      return;
    }
    return followStatus((s) => (raftStatus = s));
  });

  // Global keyboard shortcuts: Ctrl+K for the command palette, `?` for the
  // list of shortcuts, `/` for the page's filter, `[` for the side bar, and
  // `g` then a key for a page (CHORDS).
  let chordAt = 0;
  let helpOpen = $state(false);
  function onGlobalKey(e: KeyboardEvent) {
    if (!session.me || page.url.pathname === '/login') return;
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
      e.preventDefault();
      session.paletteOpen = !session.paletteOpen;
      return;
    }
    if (session.paletteOpen || helpOpen || e.metaKey || e.ctrlKey || e.altKey) return;
    const target = e.target;
    if (
      target instanceof Element &&
      (target.matches('input, textarea, select, [contenteditable="true"]') ||
        target.closest('.terminal, dialog'))
    ) {
      return;
    }
    const now = Date.now();
    if (now - chordAt < 900) {
      chordAt = 0;
      const to = CHORDS.find(
        (c) => c.key === e.key.toLowerCase() && (!c.admin || session.me?.all)
      )?.to;
      if (to) {
        e.preventDefault();
        goto(to);
      }
      return;
    }
    if (e.key === 'g') {
      chordAt = now;
    } else if (e.key === '?') {
      e.preventDefault();
      helpOpen = true;
    } else if (e.key === '[') {
      e.preventDefault();
      toggleSidebar();
    } else if (e.key === '/') {
      // The page's own filter or search, else the menu's.
      const field =
        [...document.querySelectorAll<HTMLInputElement>('main input[type="search"]')].find(
          (f) => f.offsetWidth || f.offsetHeight
        ) ?? (sideShown ? navSearch : undefined);
      if (field) {
        e.preventDefault();
        field.focus();
        field.select();
      }
    }
  }

  // The side bar: hidden at the user's choice; on a narrow window, shown
  // only when asked for, and put away again on going to a page.
  let narrow = $state(false);
  let narrowOpen = $state(false);
  let navSearch: HTMLInputElement | undefined = $state();
  let sideToggle: HTMLButtonElement | undefined = $state();
  $effect(() => {
    const q = matchMedia('(max-width: 48rem)');
    const set = () => (narrow = q.matches);
    set();
    q.addEventListener('change', set);
    return () => q.removeEventListener('change', set);
  });
  const sideShown = $derived(narrow ? narrowOpen : !prefs.sidebarHidden);
  function toggleSidebar() {
    if (sideShown && document.activeElement?.closest('aside')) sideToggle?.focus();
    if (narrow) narrowOpen = !narrowOpen;
    else setSidebarHidden(!prefs.sidebarHidden);
  }

  // The page's scroll: the page itself (main) scrolls, not the window, so
  // SvelteKit's own reset does not reach it. A new page starts at the top;
  // Back and Forward return to where the page was left.
  let mainCol: HTMLElement | undefined = $state();
  const scrolls = new Map<string, number>();
  beforeNavigate(({ from }) => {
    if (from && mainCol) scrolls.set(from.url.href, mainCol.scrollTop);
  });
  afterNavigate(({ type, to }) => {
    narrowOpen = false;
    const top = (type === 'popstate' && to && scrolls.get(to.url.href)) || 0;
    // Once the page has drawn, so that it is as long as it was.
    requestAnimationFrame(() => mainCol?.scrollTo({ top }));
  });

  // A short cross-fade between pages, where the browser has view
  // transitions and the user has not asked for less motion.
  onNavigate((navigation) => {
    if (
      !document.startViewTransition ||
      matchMedia('(prefers-reduced-motion: reduce)').matches ||
      navigation.from?.url.pathname === navigation.to?.url.pathname
    ) {
      return;
    }
    return new Promise((resolve) => {
      document.startViewTransition(async () => {
        resolve();
        await navigation.complete;
      });
    });
  });

  // A file dropped where no field takes it: a compose file opens in Import
  // compose, other YAML or JSON in Apply YAML. Anything else is refused
  // here, rather than the browser leaving the console to show the file.
  let dragDepth = $state(0);
  let dropNote = $state('');
  let noteTimer: ReturnType<typeof setTimeout> | undefined;
  const carriesFiles = (e: DragEvent) =>
    !!e.dataTransfer && [...e.dataTransfer.types].includes('Files');
  function note(text: string) {
    dropNote = text;
    clearTimeout(noteTimer);
    noteTimer = setTimeout(() => (dropNote = ''), 6000);
  }
  function onWindowDragEnter(e: DragEvent) {
    if (carriesFiles(e)) dragDepth++;
  }
  function onWindowDragLeave(e: DragEvent) {
    if (carriesFiles(e)) dragDepth = Math.max(0, dragDepth - 1);
  }
  function onWindowDragOver(e: DragEvent) {
    if (!carriesFiles(e)) return;
    e.preventDefault();
    if (e.dataTransfer) e.dataTransfer.dropEffect = 'copy';
  }
  async function onWindowDrop(e: DragEvent) {
    dragDepth = 0;
    if (!carriesFiles(e)) return;
    e.preventDefault();
    const file = e.dataTransfer?.files[0];
    if (!file || !session.me || page.url.pathname === '/login') return;
    const to = await routeDrop(file);
    if (to) {
      note(t('Opened {name}.', { name: file.name }));
      if (page.url.pathname !== to) goto(to);
    } else {
      note(
        t('{name}: drop it on a field that takes it. Elsewhere, only compose files and YAML open.', {
          name: file.name
        })
      );
    }
  }

  // The user's alerts, followed for as long as they are signed in.
  $effect(() => {
    if (!session.me) {
      session.alerts = [];
      return;
    }
    return followAlerts(
      (a) => (session.alerts = a),
      (why) => (session.alertsError = why)
    );
  });
  // The cluster's long operations, for the workbench's bar and its tab.
  $effect(() => {
    if (!session.me) {
      activity.ops = [];
      return;
    }
    return followActivity(
      (ops) => (activity.ops = ops),
      (why) => (activity.error = why)
    );
  });
  const critical = $derived(session.alerts.some((a) => a.severity === 'critical'));
  // Why a live view stopped moving, if one has: the first, as they share a
  // cause (the gateway, the network).
  const stale = $derived(Object.values(live.down)[0] ?? '');
  // The count said when it changes, as the badge only shows it (WCAG
  // 4.1.3); signed in with alerts already, said once.
  let said = $state('');
  let before = 0;
  $effect(() => {
    const n = session.alerts.length;
    if (n === before) return;
    before = n;
    said =
      n === 0
        ? t('No alerts now.')
        : critical
          ? tn('1 alert now, critical.', '{n} alerts now, some critical.', n)
          : tn('1 alert now.', '{n} alerts now.', n);
  });

  async function signOut() {
    await api.logout().catch(() => {});
    session.me = null;
    goto('/login');
  }

  // The settings: the banner before signing in, all of them after; the
  // namespace chosen once a sign-in: the one the address names, if the
  // user may read it, else the console's default, if they may read that.
  const ALL_NAMESPACES = '*';
  const askedNamespace = fromAddress('ns');
  let defaulted = $state(false);
  $effect(() => {
    const me = session.me;
    if (!me) defaulted = false;
    consoleSettings()
      .then((s) => {
        session.settings = s;
        if (me && !untrack(() => defaulted)) {
          const readable = (ns: string) => ns === '' || me.all || ns in me.namespaces;
          const asked = askedNamespace === ALL_NAMESPACES ? '' : askedNamespace;
          const ns = s.defaultNamespace;
          if (asked !== null && readable(asked)) {
            session.namespace = asked;
          } else if (ns && !session.namespace && readable(ns)) {
            session.namespace = ns;
          }
          defaulted = true;
        }
      })
      .catch(() => {});
  });
  // The namespace looked at, in the address (`?ns=jax2`, `*` for all), so
  // a reload or a shared link shows the same; as asked until chosen.
  keepInAddress('ns', () =>
    defaulted ? session.namespace || ALL_NAMESPACES : (askedNamespace ?? '')
  );

  /** The same page on another cluster's console. */
  function there(url: string): string {
    try {
      return new URL(page.url.pathname + page.url.search, new URL(url).origin).href;
    } catch {
      return url;
    }
  }

  /** The side bar's link to the page shown (WCAG 1.3.1). */
  const at = (here: boolean) => (here ? ('page' as const) : undefined);

  // Each page's title (WCAG 2.4.2), which SvelteKit also announces on
  // every navigation.
  const PAGES: Record<string, string> = $derived({
    '/': t('Overview'),
    '/login': t('Sign in'),
    '/alerts': t('Alerts'),
    '/volumes': t('Volumes'),
    '/images': t('Images'),
    '/deploy': t('Deploy'),
    '/templates': t('Templates'),
    '/apps': t('Apps'),
    '/import': t('Import compose'),
    '/stacks': t('Stacks'),
    '/apply': t('Apply YAML'),
    '/topology': t('Topology'),
    '/network': t('Network'),
    '/disk': t('Disk usage'),
    '/diagnostics': t('Diagnostics'),
    '/cluster': t('Operations'),
    '/audit': t('Audit'),
    '/settings': t('Console settings'),
    '/clusters': t('Clusters'),
    '/build': t('Builder')
  });
  const pageTitle = $derived.by(() => {
    const path = page.url.pathname;
    const [, k, kind, ns, name] = path.split('/').map(decodeURIComponent);
    const here =
      PAGES[path] ??
      (k === 'plugins' && ns
        ? t('{name} (plugin in {namespace})', { name: ns, namespace: kind })
        : undefined) ??
      (k === 'k' && kind
        ? name
          ? ns && ns !== '_'
            ? t('{name} ({kind} in {namespace})', { name, kind, namespace: ns })
            : t('{name} ({kind})', { name, kind })
          : plural(kind)
        : 'Legion');
    return here;
  });
  const title = $derived(
    [pageTitle, session.settings.name, 'Legion']
      .filter((x, i) => x && (i < 2 || x !== pageTitle))
      .join(' · ')
  );

  // Each page visited, for the palette's recent pages; the page shown, to pin.
  const here = $derived({ href: page.url.pathname + page.url.search, title: pageTitle });
  $effect(() => {
    const ref = { ...here };
    if (session.me && page.url.pathname !== '/login') untrack(() => visited(ref));
  });
  const pinned = $derived(prefs.pins.some((p) => p.href === here.href));

  /** A side bar section's `open`: as the user left it, but open while the
   * menu is filtered or the page shown is in it. */
  const sectionOpen = (key: string, items: NavItem[] = []) =>
    !!navFilter || !prefs.closed.includes(key) || items.some(isHere);
  function sectionToggled(key: string, e: Event) {
    if (navFilter) return;
    const open = (e.currentTarget as HTMLDetailsElement).open;
    if (open === prefs.closed.includes(key)) setSectionOpen(key, open);
  }
  function pinMoved(from: number, to: number) {
    const name = prefs.pins[from]?.title ?? '';
    movePin(from, to);
    said = t('{name} moved to {n} of {total}.', { name, n: to + 1, total: prefs.pins.length });
  }
  const filteredPins = $derived(
    prefs.pins.map((p, i) => ({ ...p, i })).filter((p) => matchesNav(p.title))
  );

  const choices = $derived(namespaces(adminNamespaces));
  const shownPlugins = $derived(
    session.plugins.filter((p) => !session.namespace || p.namespace === session.namespace)
  );
  const nsKinds = $derived(session.kinds.filter((k) => !k.clusterScoped));
  const clusterKinds = $derived(
    session.me?.all ? session.kinds.filter((k) => k.clusterScoped) : []
  );
  // The theme button goes round dark, light and the system's.
  const nextTheme = (m: ThemeMode): ThemeMode => (m === 'dark' ? 'light' : m === 'light' ? 'system' : 'dark');
  const themeName = (m: ThemeMode) => (m === 'dark' ? t('Dark') : m === 'light' ? t('Light') : t('System'));
  const voters = $derived(raftStatus ? raftStatus.members.filter((m) => m.voter).length : 0);
  let navQuery = $state('');
  const navFilter = $derived(navQuery.trim().toLowerCase());
  const matchesNav = (label: string) => !navFilter || label.toLowerCase().includes(navFilter);

  // The side bar's groups, as a tree: what to look at, the applications,
  // ways to make them, storage, the network, configuration, the cluster.
  // A kind goes in the group KIND_GROUP gives it, any other with
  // configuration; Volumes are listed on the Volumes page, not apart.
  type NavItem = { href: string; label: string; icon: IconComponent; alerts?: boolean; prefix?: boolean };
  type NavGroup = { key: string; label: string; items: NavItem[] };
  const pageItem = (href: string, label: string, alerts = false): NavItem => ({
    href,
    label,
    icon: pageIcon(href),
    alerts
  });
  const kindItem = (kind: string): NavItem => ({
    href: `/k/${kind}`,
    label: plural(kind),
    icon: kindIcon(kind),
    prefix: true
  });
  const KIND_GROUP: Record<string, string> = {
    Service: 'apps',
    Job: 'apps',
    CronJob: 'apps',
    Task: 'apps',
    Build: 'storage',
    BackupTarget: 'storage',
    Route: 'network',
    NetworkPolicy: 'network'
  };
  const LISTED_ELSEWHERE = new Set(['Volume']);
  const shown = (items: NavItem[]) => items.filter((i) => matchesNav(i.label));
  const navGroups = $derived.by((): NavGroup[] => {
    const kindsIn = (group: string) => [
      ...nsKinds
        .filter((k) => !LISTED_ELSEWHERE.has(k.kind) && (KIND_GROUP[k.kind] ?? 'config') === group)
        .map((k) => kindItem(k.kind)),
      ...clusterKinds.filter((k) => KIND_GROUP[k.kind] === group).map((k) => kindItem(k.kind))
    ];
    return [
      {
        key: 'observe',
        label: t('Observe'),
        items: [
          pageItem('/', t('Overview')),
          pageItem('/alerts', t('Alerts'), true),
          pageItem('/topology', t('Topology')),
          pageItem('/diagnostics', t('Diagnostics'))
        ]
      },
      { key: 'apps', label: t('Applications'), items: [...kindsIn('apps'), pageItem('/stacks', t('Stacks'))] },
      {
        key: 'create',
        label: t('Create'),
        items: [
          pageItem('/deploy', t('Deploy')),
          pageItem('/apps', t('Apps')),
          pageItem('/templates', t('Templates')),
          pageItem('/import', t('Import compose')),
          pageItem('/apply', t('Apply YAML')),
          pageItem('/build', t('Builder'))
        ]
      },
      {
        key: 'storage',
        label: t('Storage and images'),
        items: [
          pageItem('/volumes', t('Volumes')),
          pageItem('/disk', t('Disk usage')),
          pageItem('/images', t('Images')),
          ...kindsIn('storage')
        ]
      },
      { key: 'network', label: t('Network'), items: [pageItem('/network', t('Network')), ...kindsIn('network')] },
      { key: 'config', label: t('Configuration and access'), items: kindsIn('config') }
    ]
      .map((g) => ({ ...g, items: shown(g.items) }))
      .filter((g) => g.items.length);
  });
  // The cluster's own: for those who may see all of it; a kind with a
  // group of its own (BackupTarget, with storage) goes there instead.
  const clusterItems = $derived(
    clusterKinds.length
      ? shown([
          ...clusterKinds.filter((k) => !KIND_GROUP[k.kind]).map((k) => kindItem(k.kind)),
          pageItem('/cluster', t('Operations')),
          pageItem('/audit', t('Audit')),
          pageItem('/settings', t('Console settings'))
        ])
      : []
  );
  const isHere = (item: NavItem) =>
    item.prefix ? page.url.pathname.startsWith(item.href) : page.url.pathname === item.href;

  // A section's heading only while some of its entries pass the filter.
  const showAllClusters = $derived(matchesNav(t('All clusters')));
  const filteredClusters = $derived(
    (session.settings.clusters ?? []).filter((c) => matchesNav(c.name))
  );
  const filteredPlugins = $derived(shownPlugins.filter((p) => matchesNav(p.name)));
  const filteredLinks = $derived((session.settings.links ?? []).filter((l) => matchesNav(l.label)));
</script>

{#snippet navLink(item: NavItem)}
  <a href={item.href} class:here={isHere(item)} aria-current={at(isHere(item))}
    ><Ico icon={item.icon} /><span>{item.label}</span
    >{#if item.alerts && session.alerts.length}<span class="count {critical ? 'bad' : 'wait'}"
        >{session.alerts.length}</span
      >{/if}{#if drafts[item.href.slice(1)]}<span class="draft-dot" title={t('A draft is kept here')}
        ><span class="sr-only">{t('(draft)')}</span></span
      >{/if}</a
  >
{/snippet}

<svelte:window
  onkeydown={onGlobalKey}
  ondragenter={onWindowDragEnter}
  ondragleave={onWindowDragLeave}
  ondragover={onWindowDragOver}
  ondrop={onWindowDrop}
/>
<svelte:head>
  <title>{title}</title>
</svelte:head>
{#if session.settings.banner}
  <div class="banner {session.settings.banner.level ?? 'info'}" role="note">
    {session.settings.banner.text}
  </div>
{/if}
{#if page.url.pathname === '/login'}
  {@render children()}
{:else if session.me}
  {#if stale}
    <div class="banner warning" role="status">
      {t('Not current: {why}. What you see may be out of date; reconnecting…', { why: stale })}
    </div>
  {/if}
  <a class="skip" href="#main">{t('Skip to the page')}</a>
  <p class="sr-only" aria-live="polite">{said}</p>
  <CommandPalette />
  <Confirm />
  <Toasts />
  <Shortcuts bind:open={helpOpen} />
  {#if dropNote}
    <p class="toast" role="status">{dropNote}</p>
  {/if}
  {#if dragDepth > 0}
    <p class="drop-hint" aria-hidden="true">
      {t('Drop a compose file to import it, or YAML to apply it; or drop on a field to choose the file there.')}
    </p>
  {/if}
  <div class="app-frame">
    <header class="topbar">
      <button
        type="button"
        class="sm side-toggle"
        bind:this={sideToggle}
        aria-controls="side"
        aria-expanded={sideShown}
        aria-keyshortcuts="["
        title={sideShown ? t('Hide the side bar ([)') : t('Show the side bar ([)')}
        onclick={toggleSidebar}
      >
        <Ico icon={PanelLeft} size={18} />
        <span class="sr-only">{t('Side bar')}</span>
      </button>
      <a class="topbar-brand" href="/">
        <FreeBsdLogo size={24} />
        <span class="brand-title"><strong>Legion</strong></span>
        {#if session.settings.name}
          <span class="topbar-cluster">· {session.settings.name}</span>
        {/if}
      </a>

      <label class="ns-picker">
        <Ico icon={kindIcon('Namespace')} size={15} />
        <span class="sr-only">{t('Namespace')}</span>
        <select bind:value={session.namespace} title={t('Namespace')}>
          <option value="">{t('All I can see')}</option>
          {#each choices as ns (ns)}
            <option value={ns}>{ns}</option>
          {/each}
        </select>
      </label>

      {#if raftStatus}
        <span
          class="telemetry-pill"
          title={t('{voters} voting managers, term {term}, store version {version}', {
            voters,
            term: raftStatus.term,
            version: raftStatus.version
          })}
        >
          <span class={raftStatus.leader ? 'dot-live' : 'dot-bad'} aria-hidden="true"></span>
          <span>{raftStatus.leader ? t('quorum') : t('no leader')}</span>
        </span>
      {/if}

      {#if session.alertsError}
        <span class="telemetry-pill" title={session.alertsError}>
          <span class="dot-bad" aria-hidden="true"></span>
          <span>{t('watch degraded')}</span>
        </span>
      {/if}

      <div class="topbar-spacer"></div>

      <button
        type="button"
        class="sm pin-toggle"
        aria-pressed={pinned}
        title={pinned ? t('Unpin this page from the side bar') : t('Pin this page to the side bar')}
        onclick={() => togglePin({ ...here })}
      >
        <Ico icon={Star} fill={pinned} class={pinned ? 'on' : ''} />
        {t('Pin')}
      </button>

      <button
        type="button"
        class="cmd-trigger"
        onclick={() => (session.paletteOpen = true)}
        aria-keyshortcuts="Control+K"
      >
        <Ico icon={Search} />
        <span>{t('Jump or run…')}</span>
        <kbd>Ctrl+K</kbd>
      </button>

      <button
        type="button"
        class="sm"
        title={t('Theme: {now} (click for {next})', {
          now: themeName(session.theme),
          next: themeName(nextTheme(session.theme))
        })}
        onclick={() => setTheme(nextTheme(session.theme))}
      >
        <Ico icon={session.theme === 'dark' ? Moon : session.theme === 'light' ? Sun : MonitorCog} size={18} />
        <span class="sr-only">{t('Color theme: {now}', { now: themeName(session.theme) })}</span>
      </button>

      <button
        type="button"
        class="sm"
        aria-keyshortcuts="Shift+?"
        title={t('Keyboard shortcuts (?)')}
        onclick={() => (helpOpen = true)}
      >
        <Ico icon={CircleQuestionMark} size={18} />
        <span class="sr-only">{t('Keyboard shortcuts')}</span>
      </button>
    </header>

    <div class="shell" class:side-hidden={!sideShown} class:narrow-open={narrow && sideShown}>
      <aside id="side" hidden={!sideShown}>
        <input
          bind:this={navSearch}
          class="nav-search"
          type="search"
          bind:value={navQuery}
          placeholder={t('Filter menu…')}
          aria-label={t('Filter menu')}
        />
        <div class="tree-switch" role="radiogroup" aria-label={t('The side bar shows')}>
          {#each [{ id: 'pages', label: t('Pages') }, { id: 'namespace', label: t('Namespaces') }, ...(session.me.all ? [{ id: 'node', label: t('Nodes') }] : [])] as m (m.id)}
            <button
              type="button"
              role="radio"
              aria-checked={prefs.tree === m.id}
              class:on={prefs.tree === m.id}
              onclick={() => setTree(m.id as TreeMode)}>{m.label}</button
            >
          {/each}
        </div>
        {#if prefs.tree !== 'pages' && (prefs.tree !== 'node' || session.me.all)}
          <ResourceTree mode={prefs.tree === 'node' ? 'node' : 'namespace'} filter={navQuery} />
        {:else}
        <nav aria-label={t('Console')}>
          {#if filteredPins.length}
            <details class="nav-sec" open={sectionOpen('pinned')} ontoggle={(e) => sectionToggled('pinned', e)}>
              <summary class="nav-group">{t('Pinned')}</summary>
              <ul class="pins" aria-label={t('Pinned pages: drag, or Alt with an arrow key, to reorder')}>
                {#each filteredPins as p (p.href)}
                  <li class="pin-row">
                    <a
                      href={p.href}
                      class:here={here.href === p.href}
                      aria-current={at(here.href === p.href)}
                      use:sortable={{ group: 'pins', index: p.i, count: prefs.pins.length, move: pinMoved }}
                      aria-keyshortcuts="Alt+ArrowUp Alt+ArrowDown"
                      ><span class="grip" aria-hidden="true"><Ico icon={GripVertical} size={14} /></span><Ico
                        icon={pageIcon(p.href)}
                      /><span class="pin-title">{p.title}</span></a
                    >
                    <button
                      type="button"
                      class="link unpin"
                      title={t('Unpin {name}', { name: p.title })}
                      aria-label={t('Unpin {name}', { name: p.title })}
                      onclick={() => unpin(p.href)}><Ico icon={X} size={14} /></button
                    >
                  </li>
                {/each}
              </ul>
            </details>
          {/if}

          {#each navGroups as g (g.key)}
            <details class="nav-sec" open={sectionOpen(g.key, g.items)} ontoggle={(e) => sectionToggled(g.key, e)}>
              <summary class="nav-group">{g.label}</summary>
              {#each g.items as item (item.href)}{@render navLink(item)}{/each}
            </details>
          {/each}

          {#if session.settings.clusters?.length && (showAllClusters || filteredClusters.length)}
            <details class="nav-sec" open={sectionOpen('clusters')} ontoggle={(e) => sectionToggled('clusters', e)}>
              <summary class="nav-group">{t('Clusters')}</summary>
              {#if showAllClusters}
                <a
                  href="/clusters"
                  class:here={page.url.pathname === '/clusters'}
                  aria-current={at(page.url.pathname === '/clusters')}
                  ><Ico icon={pageIcon('/clusters')} /><span>{t('All clusters')}</span></a
                >
              {/if}
              {#each filteredClusters as c, i (i)}
                <a
                  href={there(c.url)}
                  title={t('This page on {name}, signed into on its own', { name: c.name })}
                  ><Ico icon={pageIcon('/clusters')} /><span>{c.name}</span></a
                >
              {/each}
            </details>
          {/if}
          {#if filteredPlugins.length}
            <details class="nav-sec" open={sectionOpen('plugins')} ontoggle={(e) => sectionToggled('plugins', e)}>
              <summary class="nav-group">{t('Plugins')}</summary>
              {#each filteredPlugins as p (`${p.namespace}/${p.name}`)}
                {@const href = `/plugins/${p.namespace}/${p.name}`}
                <a {href} class:here={page.url.pathname === href} aria-current={at(page.url.pathname === href)}
                  ><Ico icon={pageIcon(href)} /><span
                    >{p.name}{#if !session.namespace}<span class="muted"> ({p.namespace})</span>{/if}</span
                  ></a
                >
              {/each}
            </details>
          {/if}
          {#if filteredLinks.length}
            <details class="nav-sec" open={sectionOpen('links')} ontoggle={(e) => sectionToggled('links', e)}>
              <summary class="nav-group">{t('Links')}</summary>
              {#each filteredLinks as l, i (i)}
                <a href={l.url} target="_blank" rel="noopener noreferrer"
                  ><Ico icon={pageIcon(l.url)} /><span>{l.label}</span></a
                >
              {/each}
            </details>
          {/if}
          {#if clusterItems.length}
            <details class="nav-sec" open={sectionOpen('cluster', clusterItems)} ontoggle={(e) => sectionToggled('cluster', e)}>
              <summary class="nav-group">{t('Cluster')}</summary>
              {#each clusterItems as item (item.href)}{@render navLink(item)}{/each}
            </details>
          {/if}
        </nav>
        {/if}
        <div class="who">
          <span title={session.me.role}>{session.me.name}</span>
          {#if languages.length > 1}
            <select
              aria-label={t('Language')}
              value={i18n.language}
              onchange={(e) => setLanguage(e.currentTarget.value)}
            >
              {#each languages as code (code)}
                <option value={code}>{languageName(code)}</option>
              {/each}
            </select>
          {/if}
          <button class="link" onclick={signOut}>{t('Sign out')}</button>
        </div>
      </aside>
      <div class="main-col">
        <main id="main" tabindex="-1" bind:this={mainCol}>
          {@render children()}
        </main>
        <Workbench />
      </div>
    </div>
  </div>
{:else if ready}
  <p class="center">{t('Signing in…')}</p>
{/if}

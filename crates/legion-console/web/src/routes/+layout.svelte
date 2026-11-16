<script lang="ts">
  import '../app.css';
  import { untrack } from 'svelte';
  import { afterNavigate, beforeNavigate, goto, onNavigate } from '$app/navigation';
  import { page } from '$app/state';
  import {
    api,
    consoleSettings,
    followAlerts,
    plugins,
    setToken,
    whenSignedOut,
    type Status
  } from '#lib/api.ts';
  import CommandPalette from '#lib/CommandPalette.svelte';
  import FreeBsdLogo from '#lib/FreeBsdLogo.svelte';
  import Shortcuts, { CHORDS } from '#lib/Shortcuts.svelte';
  import { routeDrop, sortable } from '#lib/drop.svelte.ts';
  import {
    movePin,
    prefs,
    setSectionOpen,
    setSidebarHidden,
    togglePin,
    unpin,
    visited
  } from '#lib/prefs.svelte.ts';
  import Workbench from '#lib/Workbench.svelte';
  import { plural } from '#lib/kinds.ts';
  import {
    namespaces,
    session,
    setTheme,
    type ThemeMode
  } from '#lib/session.svelte.ts';
  import { i18n, languageName, languages, setLanguage, t } from '#lib/i18n.svelte.ts';
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

  // Poll Raft quorum / term / store version for the Top Telemetry Strip.
  function setRaftStatus(s: Status | null) {
    raftStatus = s;
  }
  $effect(() => {
    if (!session.me) {
      setRaftStatus(null);
      return;
    }
    let stopped = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    const poll = async () => {
      try {
        const s = await api.status();
        if (!stopped) setRaftStatus(s);
      } catch {
        // Keep last known status.
      } finally {
        if (!stopped) timer = setTimeout(poll, 15_000);
      }
    };
    poll();
    return () => {
      stopped = true;
      if (timer) clearTimeout(timer);
    };
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

  // The page's scroll: the main column scrolls, not the window, so
  // SvelteKit's own reset does not reach it. A new page starts at the top;
  // Back and Forward return to where the page was left.
  let mainCol: HTMLDivElement | undefined = $state();
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
          ? t('{n} alerts now, some critical.', { n })
          : t('{n} alerts now.', { n });
  });

  async function signOut() {
    await api.logout().catch(() => {});
    session.me = null;
    goto('/login');
  }

  // The settings: the banner before signing in, all of them after; the
  // default namespace chosen once a sign-in, if the user may read it.
  let defaulted = false;
  $effect(() => {
    const me = session.me;
    if (!me) defaulted = false;
    consoleSettings()
      .then((s) => {
        session.settings = s;
        const ns = s.defaultNamespace;
        if (me && ns && !defaulted && !session.namespace && (me.all || ns in me.namespaces)) {
          session.namespace = ns;
        }
        if (me) defaulted = true;
      })
      .catch(() => {});
  });

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
    '/volumes': t('Volumes Studio'),
    '/images': t('Images & GHCR Hub'),
    '/deploy': t('Deploy'),
    '/templates': t('Templates'),
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

  /** A side bar section's `open`: as the user left it, but open while the menu is filtered. */
  const sectionOpen = (key: string) => !!navFilter || !prefs.closed.includes(key);
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
  const voters = $derived(raftStatus ? raftStatus.members.filter((m) => m.voter).length : 0);
  let navQuery = $state('');
  const navFilter = $derived(navQuery.trim().toLowerCase());
  const matchesNav = (label: string) => !navFilter || label.toLowerCase().includes(navFilter);

  const observeItems = $derived(
    [
      { href: '/', label: t('Overview') },
      { href: '/alerts', label: t('Alerts'), alerts: true },
      { href: '/topology', label: t('Topology') },
      { href: '/network', label: t('Network') },
      { href: '/disk', label: t('Disk usage') },
      { href: '/diagnostics', label: t('Diagnostics') }
    ].filter((i) => matchesNav(i.label))
  );

  const studioItems = $derived(
    [
      { href: '/volumes', label: t('Volumes Studio') },
      { href: '/images', label: t('Images & GHCR Hub') },
      { href: '/build', label: t('Builder') },
      { href: '/stacks', label: t('Stacks') },
      { href: '/deploy', label: t('Deploy') },
      { href: '/templates', label: t('Templates') },
      { href: '/import', label: t('Import compose') },
      { href: '/apply', label: t('Apply YAML') }
    ].filter((i) => matchesNav(i.label))
  );

  // A section's heading only while some of its entries pass the filter.
  const showAllClusters = $derived(matchesNav(t('All clusters')));
  const filteredClusters = $derived(
    (session.settings.clusters ?? []).filter((c) => matchesNav(c.name))
  );
  const filteredPlugins = $derived(shownPlugins.filter((p) => matchesNav(p.name)));
  const filteredLinks = $derived((session.settings.links ?? []).filter((l) => matchesNav(l.label)));
  const filteredNsKinds = $derived(nsKinds.filter((k) => matchesNav(plural(k.kind))));
  const filteredClusterKinds = $derived(clusterKinds.filter((k) => matchesNav(plural(k.kind))));
  const clusterOpsItems = $derived(
    [
      { href: '/cluster', label: t('Operations') },
      { href: '/audit', label: t('Audit') },
      { href: '/settings', label: t('Console settings') }
    ].filter((i) => matchesNav(i.label))
  );
</script>

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
        <span aria-hidden="true">☰</span>
        <span class="sr-only">{t('Side bar')}</span>
      </button>
      <a class="topbar-brand" href="/">
        <FreeBsdLogo size={24} />
        <span class="brand-title">FreeBSD <strong>Legion</strong></span>
        {#if session.settings.name}
          <span class="topbar-cluster">· {session.settings.name}</span>
        {/if}
      </a>

      {#if raftStatus}
        <span class="telemetry-pill">
          <span class={raftStatus.leader ? 'dot-live' : 'dot-bad'} aria-hidden="true"></span>
          <strong>{raftStatus.leader ? t('quorum ok') : t('no leader')}</strong>
          <span class="mono">
            {t('{voters}v · t{term} · r{version}', {
              voters,
              term: raftStatus.term,
              version: raftStatus.version
            })}
          </span>
        </span>
      {/if}

      <span class="telemetry-pill">
        <span class={session.alertsError ? 'dot-bad' : 'dot-live'} aria-hidden="true"></span>
        <span>{session.alertsError ? t('watch degraded') : t('watch live')}</span>
      </span>

      <div class="topbar-spacer"></div>

      <button
        type="button"
        class="sm pin-toggle"
        aria-pressed={pinned}
        title={pinned ? t('Unpin this page from the side bar') : t('Pin this page to the side bar')}
        onclick={() => togglePin({ ...here })}
      >
        <span aria-hidden="true">{pinned ? '★' : '☆'}</span>
        {t('Pin')}
      </button>

      <button
        type="button"
        class="cmd-trigger"
        onclick={() => (session.paletteOpen = true)}
        aria-keyshortcuts="Control+K"
      >
        <span>{t('Jump or run…')}</span>
        <kbd>Ctrl+K</kbd>
      </button>

      <select
        aria-label={t('Color theme')}
        value={session.theme}
        onchange={(e) => setTheme(e.currentTarget.value as ThemeMode)}
      >
        <option value="dark">{t('Dark')}</option>
        <option value="light">{t('Light')}</option>
        <option value="system">{t('System')}</option>
      </select>

      <button
        type="button"
        class="sm"
        aria-keyshortcuts="Shift+?"
        title={t('Keyboard shortcuts (?)')}
        onclick={() => (helpOpen = true)}
      >
        <span aria-hidden="true">?</span>
        <span class="sr-only">{t('Keyboard shortcuts')}</span>
      </button>
    </header>

    <div class="shell" class:side-hidden={!sideShown} class:narrow-open={narrow && sideShown}>
      <aside id="side" hidden={!sideShown}>
        <label class="ns">
          {t('Namespace')}
          <select bind:value={session.namespace}>
            <option value="">{t('All I can see')}</option>
            {#each choices as ns (ns)}
              <option value={ns}>{ns}</option>
            {/each}
          </select>
        </label>
        <input
          bind:this={navSearch}
          class="nav-search"
          type="search"
          bind:value={navQuery}
          placeholder={t('Filter menu…')}
          aria-label={t('Filter menu')}
        />
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
                      ><span class="grip" aria-hidden="true">⠿</span><span class="pin-title">{p.title}</span></a
                    >
                    <button
                      type="button"
                      class="link unpin"
                      title={t('Unpin {name}', { name: p.title })}
                      aria-label={t('Unpin {name}', { name: p.title })}
                      onclick={() => unpin(p.href)}>×</button
                    >
                  </li>
                {/each}
              </ul>
            </details>
          {/if}

          {#if observeItems.length}
            <details class="nav-sec" open={sectionOpen('observe')} ontoggle={(e) => sectionToggled('observe', e)}>
              <summary class="nav-group">{t('Observe')}</summary>
              {#each observeItems as item (item.href)}
                <a
                  href={item.href}
                  class:here={page.url.pathname === item.href}
                  aria-current={at(page.url.pathname === item.href)}
                >
                  <span>{item.label}</span>
                  {#if item.alerts && session.alerts.length}
                    <span class="count {critical ? 'bad' : 'wait'}">{session.alerts.length}</span>
                  {/if}
                </a>
              {/each}
            </details>
          {/if}

          {#if studioItems.length}
            <details class="nav-sec" open={sectionOpen('studios')} ontoggle={(e) => sectionToggled('studios', e)}>
              <summary class="nav-group">{t('Studios')}</summary>
              {#each studioItems as item (item.href)}
                <a
                  href={item.href}
                  class:here={page.url.pathname === item.href}
                  aria-current={at(page.url.pathname === item.href)}
                  >{item.label}</a
                >
              {/each}
            </details>
          {/if}

          {#if session.settings.clusters?.length && (showAllClusters || filteredClusters.length)}
            <details class="nav-sec" open={sectionOpen('clusters')} ontoggle={(e) => sectionToggled('clusters', e)}>
              <summary class="nav-group">{t('Clusters')}</summary>
              {#if showAllClusters}
                <a
                  href="/clusters"
                  class:here={page.url.pathname === '/clusters'}
                  aria-current={at(page.url.pathname === '/clusters')}>{t('All clusters')}</a
                >
              {/if}
              {#each filteredClusters as c, i (i)}
                <a
                  href={there(c.url)}
                  title={t('This page on {name}, signed into on its own', { name: c.name })}
                  >{c.name}</a
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
                  >{p.name}{#if !session.namespace}<span class="muted"> ({p.namespace})</span>{/if}</a
                >
              {/each}
            </details>
          {/if}
          {#if filteredLinks.length}
            <details class="nav-sec" open={sectionOpen('links')} ontoggle={(e) => sectionToggled('links', e)}>
              <summary class="nav-group">{t('Links')}</summary>
              {#each filteredLinks as l, i (i)}
                <a href={l.url} target="_blank" rel="noopener noreferrer">{l.label}</a>
              {/each}
            </details>
          {/if}
          {#if filteredNsKinds.length}
            <details class="nav-sec" open={sectionOpen('workloads')} ontoggle={(e) => sectionToggled('workloads', e)}>
              <summary class="nav-group">{t('Workloads and config')}</summary>
              {#each filteredNsKinds as k (k.kind)}
                <a
                  href="/k/{k.kind}"
                  class:here={page.url.pathname.startsWith(`/k/${k.kind}`)}
                  aria-current={at(page.url.pathname.startsWith(`/k/${k.kind}`))}
                  >{plural(k.kind)}</a
                >
              {/each}
            </details>
          {/if}
          {#if clusterKinds.length && (clusterOpsItems.length || filteredClusterKinds.length)}
            <details class="nav-sec" open={sectionOpen('cluster')} ontoggle={(e) => sectionToggled('cluster', e)}>
              <summary class="nav-group">{t('Cluster')}</summary>
              {#each clusterOpsItems as item (item.href)}
                <a
                  href={item.href}
                  class:here={page.url.pathname === item.href}
                  aria-current={at(page.url.pathname === item.href)}
                  >{item.label}</a
                >
              {/each}
              {#each filteredClusterKinds as k (k.kind)}
                <a
                  href="/k/{k.kind}"
                  class:here={page.url.pathname.startsWith(`/k/${k.kind}`)}
                  aria-current={at(page.url.pathname.startsWith(`/k/${k.kind}`))}
                  >{plural(k.kind)}</a
                >
              {/each}
            </details>
          {/if}
        </nav>
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
      <div class="main-col" bind:this={mainCol}>
        <main id="main" tabindex="-1">
          {@render children()}
        </main>
        <Workbench />
      </div>
    </div>
  </div>
{:else if ready}
  <p class="center">{t('Signing in…')}</p>
{/if}

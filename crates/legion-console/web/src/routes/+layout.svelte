<script lang="ts">
  import '../app.css';
  import { goto } from '$app/navigation';
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
  import Workbench from '#lib/Workbench.svelte';
  import { plural } from '#lib/kinds.ts';
  import {
    namespaces,
    session,
    setDensity,
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

  // Global keyboard shortcuts: ⌘K / Ctrl+K for Command Palette, and g + key chords.
  let chordAt = 0;
  function onGlobalKey(e: KeyboardEvent) {
    if (!session.me || page.url.pathname === '/login') return;
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
      e.preventDefault();
      session.paletteOpen = !session.paletteOpen;
      return;
    }
    if (session.paletteOpen || e.metaKey || e.ctrlKey || e.altKey) return;
    const target = e.target as HTMLElement | null;
    if (
      target &&
      (target.matches('input, textarea, select, [contenteditable="true"]') ||
        target.closest('.terminal, dialog'))
    ) {
      return;
    }
    const now = Date.now();
    if (e.key === 'g') {
      chordAt = now;
      return;
    }
    if (now - chordAt < 900) {
      chordAt = 0;
      const dest: Record<string, string> = {
        o: '/',
        v: '/volumes',
        i: '/images',
        t: '/topology',
        y: '/apply',
        a: '/alerts'
      };
      const to = dest[e.key.toLowerCase()];
      if (to) {
        e.preventDefault();
        goto(to);
      }
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
    '/build': t('Build a new image')
  });
  const title = $derived.by(() => {
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
    return [here, session.settings.name, 'Legion'].filter(Boolean).join(' · ');
  });

  const choices = $derived(namespaces(adminNamespaces));
  const shownPlugins = $derived(
    session.plugins.filter((p) => !session.namespace || p.namespace === session.namespace)
  );
  const nsKinds = $derived(session.kinds.filter((k) => !k.clusterScoped));
  const clusterKinds = $derived(
    session.me?.all ? session.kinds.filter((k) => k.clusterScoped) : []
  );
  const voters = $derived(raftStatus ? raftStatus.members.filter((m) => m.voter).length : 0);
</script>

<svelte:window onkeydown={onGlobalKey} />
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
  <div class="app-frame">
    <header class="topbar">
      <a class="topbar-brand" href="/">
        <span class="mark" aria-hidden="true">L</span>
        <span>Legion</span>
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
        aria-pressed={session.density === 'compact'}
        onclick={() => setDensity(session.density === 'compact' ? 'comfortable' : 'compact')}
      >
        {t('Compact')}
      </button>
    </header>

    <div class="shell">
      <aside>
        <label class="ns">
          {t('Namespace')}
          <select bind:value={session.namespace}>
            <option value="">{t('All I can see')}</option>
            {#each choices as ns (ns)}
              <option value={ns}>{ns}</option>
            {/each}
          </select>
        </label>
        <nav aria-label={t('Console')}>
          <a href="/" class:here={page.url.pathname === '/'} aria-current={at(page.url.pathname === '/')}
            >{t('Overview')}</a
          >
          <a
            href="/alerts"
            class:here={page.url.pathname === '/alerts'}
            aria-current={at(page.url.pathname === '/alerts')}
            >{t('Alerts')}{#if session.alerts.length}
              <span class="count {critical ? 'bad' : 'wait'}">{session.alerts.length}</span>{/if}</a
          >
          <a
            href="/topology"
            class:here={page.url.pathname === '/topology'}
            aria-current={at(page.url.pathname === '/topology')}>{t('Topology')}</a
          >
          <a
            href="/network"
            class:here={page.url.pathname === '/network'}
            aria-current={at(page.url.pathname === '/network')}>{t('Network')}</a
          >
          <a
            href="/disk"
            class:here={page.url.pathname === '/disk'}
            aria-current={at(page.url.pathname === '/disk')}>{t('Disk usage')}</a
          >
          <a
            href="/diagnostics"
            class:here={page.url.pathname === '/diagnostics'}
            aria-current={at(page.url.pathname === '/diagnostics')}>{t('Diagnostics')}</a
          >

          <h2>{t('Studios')}</h2>
          <a
            href="/volumes"
            class:here={page.url.pathname === '/volumes'}
            aria-current={at(page.url.pathname === '/volumes')}>{t('Volumes Studio')}</a
          >
          <a
            href="/images"
            class:here={page.url.pathname === '/images'}
            aria-current={at(page.url.pathname === '/images')}>{t('Images & GHCR Hub')}</a
          >
          <a
            href="/stacks"
            class:here={page.url.pathname === '/stacks'}
            aria-current={at(page.url.pathname === '/stacks')}>{t('Stacks')}</a
          >
          <a
            href="/deploy"
            class:here={page.url.pathname === '/deploy'}
            aria-current={at(page.url.pathname === '/deploy')}>{t('Deploy')}</a
          >
          <a
            href="/templates"
            class:here={page.url.pathname === '/templates'}
            aria-current={at(page.url.pathname === '/templates')}>{t('Templates')}</a
          >
          <a
            href="/import"
            class:here={page.url.pathname === '/import'}
            aria-current={at(page.url.pathname === '/import')}>{t('Import compose')}</a
          >
          <a
            href="/apply"
            class:here={page.url.pathname === '/apply'}
            aria-current={at(page.url.pathname === '/apply')}>{t('Apply YAML')}</a
          >

          {#if session.settings.clusters?.length}
            <h2>{t('Clusters')}</h2>
            <a
              href="/clusters"
              class:here={page.url.pathname === '/clusters'}
              aria-current={at(page.url.pathname === '/clusters')}>{t('All clusters')}</a
            >
            {#each session.settings.clusters as c, i (i)}
              <a
                href={there(c.url)}
                title={t('This page on {name}, signed into on its own', { name: c.name })}
                >{c.name}</a
              >
            {/each}
          {/if}
          {#if shownPlugins.length}
            <h2>{t('Plugins')}</h2>
            {#each shownPlugins as p (`${p.namespace}/${p.name}`)}
              {@const href = `/plugins/${p.namespace}/${p.name}`}
              <a {href} class:here={page.url.pathname === href} aria-current={at(page.url.pathname === href)}
                >{p.name}{#if !session.namespace}<span class="muted"> ({p.namespace})</span>{/if}</a
              >
            {/each}
          {/if}
          {#if session.settings.links?.length}
            <h2>{t('Links')}</h2>
            {#each session.settings.links as l, i (i)}
              <a href={l.url} target="_blank" rel="noopener noreferrer">{l.label}</a>
            {/each}
          {/if}
          <h2>{t('Workloads and config')}</h2>
          {#each nsKinds as k (k.kind)}
            <a
              href="/k/{k.kind}"
              class:here={page.url.pathname.startsWith(`/k/${k.kind}`)}
              aria-current={at(page.url.pathname.startsWith(`/k/${k.kind}`))}
              >{plural(k.kind)}</a
            >
          {/each}
          {#if clusterKinds.length}
            <h2>{t('Cluster')}</h2>
            <a
              href="/cluster"
              class:here={page.url.pathname === '/cluster'}
              aria-current={at(page.url.pathname === '/cluster')}>{t('Operations')}</a
            >
            <a
              href="/audit"
              class:here={page.url.pathname === '/audit'}
              aria-current={at(page.url.pathname === '/audit')}>{t('Audit')}</a
            >
            <a
              href="/settings"
              class:here={page.url.pathname === '/settings'}
              aria-current={at(page.url.pathname === '/settings')}>{t('Console settings')}</a
            >
            {#each clusterKinds as k (k.kind)}
              <a
                href="/k/{k.kind}"
                class:here={page.url.pathname.startsWith(`/k/${k.kind}`)}
                aria-current={at(page.url.pathname.startsWith(`/k/${k.kind}`))}
                >{plural(k.kind)}</a
              >
            {/each}
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
      <div class="main-col">
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

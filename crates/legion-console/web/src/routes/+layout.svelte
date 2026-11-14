<script lang="ts">
  import '../app.css';
  import { goto } from '$app/navigation';
  import { page } from '$app/state';
  import { api, consoleSettings, followAlerts, plugins, setToken, whenSignedOut } from '#lib/api.ts';
  import { plural } from '#lib/kinds.ts';
  import { namespaces, session } from '#lib/session.svelte.ts';
  import { i18n, languageName, languages, setLanguage, t } from '#lib/i18n.svelte.ts';

  let { children } = $props();
  let adminNamespaces: string[] = $state([]);
  let ready = $state(false);

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
    '/deploy': t('Deploy'),
    '/templates': t('Templates'),
    '/import': t('Import compose'),
    '/apply': t('Apply YAML'),
    '/topology': t('Topology'),
    '/network': t('Network'),
    '/cluster': t('Operations'),
    '/audit': t('Audit'),
    '/settings': t('Console settings'),
    '/clusters': t('Clusters')
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
</script>

<svelte:head>
  <title>{title}</title>
</svelte:head>
{#if session.settings.banner}
  <div class="banner {session.settings.banner.level ?? 'info'}" role="note">{session.settings.banner.text}</div>
{/if}
{#if page.url.pathname === '/login'}
  {@render children()}
{:else if session.me}
  <a class="skip" href="#main">{t('Skip to the page')}</a>
  <div class="shell">
    <aside>
      <a class="brand" href="/"
        >Legion{#if session.settings.name}<span class="cluster">{session.settings.name}</span
          >{/if}</a
      >
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
        <a href="/" class:here={page.url.pathname === '/'} aria-current={at(page.url.pathname === '/')}>{t('Overview')}</a>
        <a href="/alerts" class:here={page.url.pathname === '/alerts'} aria-current={at(page.url.pathname === '/alerts')}
          >{t('Alerts')}{#if session.alerts.length}
            <span class="count {critical ? 'bad' : 'wait'}">{session.alerts.length}</span>{/if}</a
        >
        <a href="/deploy" class:here={page.url.pathname === '/deploy'} aria-current={at(page.url.pathname === '/deploy')}>{t('Deploy')}</a>
        <a href="/templates" class:here={page.url.pathname === '/templates'} aria-current={at(page.url.pathname === '/templates')}>{t('Templates')}</a>
        <a href="/import" class:here={page.url.pathname === '/import'} aria-current={at(page.url.pathname === '/import')}>{t('Import compose')}</a>
        <a href="/apply" class:here={page.url.pathname === '/apply'} aria-current={at(page.url.pathname === '/apply')}>{t('Apply YAML')}</a>
        <a href="/topology" class:here={page.url.pathname === '/topology'} aria-current={at(page.url.pathname === '/topology')}>{t('Topology')}</a>
        <a href="/network" class:here={page.url.pathname === '/network'} aria-current={at(page.url.pathname === '/network')}>{t('Network')}</a>
        {#if session.settings.clusters?.length}
          <h2>{t('Clusters')}</h2>
          <a href="/clusters" class:here={page.url.pathname === '/clusters'} aria-current={at(page.url.pathname === '/clusters')}>{t('All clusters')}</a>
          {#each session.settings.clusters as c, i (i)}
            <a href={there(c.url)} title={t('This page on {name}, signed into on its own', { name: c.name })}
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
          <a href="/k/{k.kind}" class:here={page.url.pathname.startsWith(`/k/${k.kind}`)} aria-current={at(page.url.pathname.startsWith(`/k/${k.kind}`))}
            >{plural(k.kind)}</a
          >
        {/each}
        {#if clusterKinds.length}
          <h2>{t('Cluster')}</h2>
          <a href="/cluster" class:here={page.url.pathname === '/cluster'} aria-current={at(page.url.pathname === '/cluster')}>{t('Operations')}</a>
          <a href="/audit" class:here={page.url.pathname === '/audit'} aria-current={at(page.url.pathname === '/audit')}>{t('Audit')}</a>
          <a href="/settings" class:here={page.url.pathname === '/settings'} aria-current={at(page.url.pathname === '/settings')}>{t('Console settings')}</a>
          {#each clusterKinds as k (k.kind)}
            <a href="/k/{k.kind}" class:here={page.url.pathname.startsWith(`/k/${k.kind}`)} aria-current={at(page.url.pathname.startsWith(`/k/${k.kind}`))}
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
    <main id="main" tabindex="-1">
      {@render children()}
    </main>
  </div>
{:else if ready}
  <p class="center">{t('Signing in…')}</p>
{/if}

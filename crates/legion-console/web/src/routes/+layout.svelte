<script lang="ts">
  import '../app.css';
  import { goto } from '$app/navigation';
  import { page } from '$app/state';
  import { api, consoleSettings, followAlerts, setToken, whenSignedOut } from '#lib/api.ts';
  import { plural } from '#lib/kinds.ts';
  import { namespaces, session } from '#lib/session.svelte.ts';

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
  const PAGES: Record<string, string> = {
    '/': 'Overview',
    '/login': 'Sign in',
    '/alerts': 'Alerts',
    '/deploy': 'Deploy',
    '/templates': 'Templates',
    '/import': 'Import compose',
    '/apply': 'Apply YAML',
    '/topology': 'Topology',
    '/network': 'Network',
    '/cluster': 'Operations',
    '/audit': 'Audit',
    '/settings': 'Console settings',
    '/clusters': 'Clusters'
  };
  const title = $derived.by(() => {
    const path = page.url.pathname;
    const [, k, kind, ns, name] = path.split('/').map(decodeURIComponent);
    const here =
      PAGES[path] ??
      (k === 'k' && kind
        ? name
          ? `${name} (${kind}${ns && ns !== '_' ? ` in ${ns}` : ''})`
          : plural(kind)
        : 'Legion');
    return [here, session.settings.name, 'Legion'].filter(Boolean).join(' · ');
  });

  const choices = $derived(namespaces(adminNamespaces));
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
  <a class="skip" href="#main">Skip to the page</a>
  <div class="shell">
    <aside>
      <a class="brand" href="/"
        >Legion{#if session.settings.name}<span class="cluster">{session.settings.name}</span
          >{/if}</a
      >
      <label class="ns">
        Namespace
        <select bind:value={session.namespace}>
          <option value="">All I can see</option>
          {#each choices as ns (ns)}
            <option value={ns}>{ns}</option>
          {/each}
        </select>
      </label>
      <nav aria-label="Console">
        <a href="/" class:here={page.url.pathname === '/'} aria-current={at(page.url.pathname === '/')}>Overview</a>
        <a href="/alerts" class:here={page.url.pathname === '/alerts'} aria-current={at(page.url.pathname === '/alerts')}
          >Alerts{#if session.alerts.length}
            <span class="count {critical ? 'bad' : 'wait'}">{session.alerts.length}</span>{/if}</a
        >
        <a href="/deploy" class:here={page.url.pathname === '/deploy'} aria-current={at(page.url.pathname === '/deploy')}>Deploy</a>
        <a href="/templates" class:here={page.url.pathname === '/templates'} aria-current={at(page.url.pathname === '/templates')}>Templates</a>
        <a href="/import" class:here={page.url.pathname === '/import'} aria-current={at(page.url.pathname === '/import')}>Import compose</a>
        <a href="/apply" class:here={page.url.pathname === '/apply'} aria-current={at(page.url.pathname === '/apply')}>Apply YAML</a>
        <a href="/topology" class:here={page.url.pathname === '/topology'} aria-current={at(page.url.pathname === '/topology')}>Topology</a>
        <a href="/network" class:here={page.url.pathname === '/network'} aria-current={at(page.url.pathname === '/network')}>Network</a>
        {#if session.settings.clusters?.length}
          <h2>Clusters</h2>
          <a href="/clusters" class:here={page.url.pathname === '/clusters'} aria-current={at(page.url.pathname === '/clusters')}>All clusters</a>
          {#each session.settings.clusters as c, i (i)}
            <a href={there(c.url)} title="This page on {c.name}, signed into on its own"
              >{c.name}</a
            >
          {/each}
        {/if}
        {#if session.settings.links?.length}
          <h2>Links</h2>
          {#each session.settings.links as l, i (i)}
            <a href={l.url} target="_blank" rel="noopener noreferrer">{l.label}</a>
          {/each}
        {/if}
        <h2>Workloads and config</h2>
        {#each nsKinds as k (k.kind)}
          <a href="/k/{k.kind}" class:here={page.url.pathname.startsWith(`/k/${k.kind}`)} aria-current={at(page.url.pathname.startsWith(`/k/${k.kind}`))}
            >{plural(k.kind)}</a
          >
        {/each}
        {#if clusterKinds.length}
          <h2>Cluster</h2>
          <a href="/cluster" class:here={page.url.pathname === '/cluster'} aria-current={at(page.url.pathname === '/cluster')}>Operations</a>
          <a href="/audit" class:here={page.url.pathname === '/audit'} aria-current={at(page.url.pathname === '/audit')}>Audit</a>
          <a href="/settings" class:here={page.url.pathname === '/settings'} aria-current={at(page.url.pathname === '/settings')}>Console settings</a>
          {#each clusterKinds as k (k.kind)}
            <a href="/k/{k.kind}" class:here={page.url.pathname.startsWith(`/k/${k.kind}`)} aria-current={at(page.url.pathname.startsWith(`/k/${k.kind}`))}
              >{plural(k.kind)}</a
            >
          {/each}
        {/if}
      </nav>
      <div class="who">
        <span title={session.me.role}>{session.me.name}</span>
        <button class="link" onclick={signOut}>Sign out</button>
      </div>
    </aside>
    <main id="main" tabindex="-1">
      {@render children()}
    </main>
  </div>
{:else if ready}
  <p class="center">Signing in…</p>
{/if}

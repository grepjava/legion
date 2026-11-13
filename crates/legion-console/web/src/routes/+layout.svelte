<script lang="ts">
  import '../app.css';
  import { goto } from '$app/navigation';
  import { page } from '$app/state';
  import { api, setToken, whenSignedOut } from '#lib/api.ts';
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

  async function signOut() {
    await api.logout().catch(() => {});
    session.me = null;
    goto('/login');
  }

  const choices = $derived(namespaces(adminNamespaces));
  const nsKinds = $derived(session.kinds.filter((k) => !k.clusterScoped));
  const clusterKinds = $derived(
    session.me?.all ? session.kinds.filter((k) => k.clusterScoped) : []
  );
</script>

{#if page.url.pathname === '/login'}
  {@render children()}
{:else if session.me}
  <div class="shell">
    <aside>
      <a class="brand" href="/">Legion</a>
      <label class="ns">
        Namespace
        <select bind:value={session.namespace}>
          <option value="">All I can see</option>
          {#each choices as ns (ns)}
            <option value={ns}>{ns}</option>
          {/each}
        </select>
      </label>
      <nav>
        <a href="/" class:here={page.url.pathname === '/'}>Overview</a>
        <a href="/apply" class:here={page.url.pathname === '/apply'}>Apply YAML</a>
        <h2>Workloads and config</h2>
        {#each nsKinds as k (k.kind)}
          <a href="/k/{k.kind}" class:here={page.url.pathname.startsWith(`/k/${k.kind}`)}
            >{plural(k.kind)}</a
          >
        {/each}
        {#if clusterKinds.length}
          <h2>Cluster</h2>
          <a href="/cluster" class:here={page.url.pathname === '/cluster'}>Operations</a>
          <a href="/audit" class:here={page.url.pathname === '/audit'}>Audit</a>
          {#each clusterKinds as k (k.kind)}
            <a href="/k/{k.kind}" class:here={page.url.pathname.startsWith(`/k/${k.kind}`)}
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
    <main>
      {@render children()}
    </main>
  </div>
{:else if ready}
  <p class="center">Signing in…</p>
{/if}

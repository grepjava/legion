<script lang="ts">
  import { goto } from '$app/navigation';
  import { api } from '#lib/api.ts';
  import { session } from '#lib/session.svelte.ts';

  let error = $state('');
  let busy = $state(false);

  // The code comes in the URL's fragment, which the browser never sends to
  // a server; it is taken out of the address bar at once.
  $effect(() => {
    const code = new URLSearchParams(location.hash.slice(1)).get('code');
    if (!code) return;
    history.replaceState(null, '', '/login');
    busy = true;
    api
      .login(code)
      .then(() => {
        session.me = null;
        goto('/');
      })
      .catch((e) => {
        error = e.message;
        busy = false;
      });
  });
</script>

<div class="login">
  <h1>Legion</h1>
  {#if busy}
    <p>Signing in…</p>
  {:else}
    {#if error}
      <p class="error">{error}</p>
    {/if}
    <p>Sign in from the command line, with the identity you use there:</p>
    <pre>legion console login</pre>
    <p>It prints a link that signs you in here. Open it within a minute; it works once.</p>
  {/if}
</div>

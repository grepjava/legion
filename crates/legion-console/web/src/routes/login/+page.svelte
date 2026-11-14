<script lang="ts">
  import { goto } from '$app/navigation';
  import { api } from '#lib/api.ts';
  import { session } from '#lib/session.svelte.ts';
  import { t } from '#lib/i18n.svelte.ts';

  let error = $state('');
  let busy = $state(false);

  // The code comes in the URL's fragment, which the browser never sends to
  // a server; it is taken out of the address bar at once. So does why a
  // single sign-on failed.
  $effect(() => {
    const fragment = new URLSearchParams(location.hash.slice(1));
    const failed = fragment.get('error');
    if (failed) {
      history.replaceState(null, '', '/login');
      error = t('Single sign-on failed: {why}', { why: failed });
      return;
    }
    const code = fragment.get('code');
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
    <p>{t('Signing in…')}</p>
  {:else}
    {#if error}
      <p class="error" role="alert">{error}</p>
    {/if}
    {#if session.settings.sso}
      <p><a class="button primary" href="/api/oidc/start" data-sveltekit-reload>{session.settings.sso.label}</a></p>
      <p class="muted">{t('Or sign in from the command line, with the identity you use there:')}</p>
    {:else}
      <p>{t('Sign in from the command line, with the identity you use there:')}</p>
    {/if}
    <pre>legion console login</pre>
    <p>{t('It prints a link that signs you in here. Open it within a minute; it works once.')}</p>
  {/if}
</div>

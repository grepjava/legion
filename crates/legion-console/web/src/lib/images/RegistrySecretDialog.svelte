<script lang="ts">
  // A registry login saved as a Docker config Secret in the namespace, for
  // pushing or pulling private images; chosen in every form once saved.
  import { api } from '../api.ts';
  import { lightDismiss } from '../dialog.ts';
  import { t } from '../i18n.svelte.ts';
  import type { Studio } from './studio.svelte.ts';

  let { studio }: { studio: Studio } = $props();

  let dialog: HTMLDialogElement | undefined = $state();
  let name = $state('ghcr-cred');
  let server = $state('ghcr.io');
  let user = $state('');
  let token = $state('');

  export function show() {
    dialog?.showModal();
  }

  async function submit(e: SubmitEvent) {
    e.preventDefault();
    const made = name.trim();
    const namespace = studio.namespace;
    await studio.act(async () => {
      const auth = btoa(`${user.trim()}:${token}`);
      const config = JSON.stringify({
        auths: { [server.trim()]: { username: user.trim(), password: token, auth } }
      });
      await api.apply({
        object: {
          apiVersion: 'legion/v1',
          kind: 'Secret',
          metadata: { name: made, namespace },
          spec: { text: config }
        }
      });
      studio.saved(made);
      token = '';
      dialog?.close();
      return t('Saved registry Secret {namespace}/{name}', { namespace, name: made });
    });
  }
</script>

<dialog
  bind:this={dialog}
  closedby="any"
  aria-labelledby="ghcr-sec-title"
  onclick={(e) => lightDismiss(e, dialog)}
>
  <header class="bar">
    <h2 id="ghcr-sec-title">{t('Save GHCR / Registry Credentials Secret')}</h2>
    <button type="button" class="sm" onclick={() => dialog?.close()}>{t('Close')}</button>
  </header>
  <form class="stacked-form" onsubmit={submit}>
    <p class="muted">
      {t(
        'Creates a Docker config Secret in {namespace} for pushing or pulling private images (for GitHub Container Registry, use ghcr.io and a Personal Access Token with write:packages).',
        { namespace: studio.namespace }
      )}
    </p>
    <label>
      {t('Secret name')}
      <input bind:value={name} placeholder="ghcr-cred" required />
    </label>
    <label>
      {t('Registry server')}
      <input bind:value={server} placeholder="ghcr.io" required />
    </label>
    <label>
      {t('Username')}
      <input bind:value={user} autocomplete="username" placeholder="github-handle" required />
    </label>
    <label>
      {t('Token or password')}
      <input
        type="password"
        bind:value={token}
        autocomplete="current-password"
        placeholder="ghp_… or github_pat_…"
        required
      />
    </label>
    <div class="controls">
      <button class="primary" disabled={!!studio.writeWhy || studio.busy || !name.trim() || !user.trim() || !token}>
        {t('Save Secret')}
      </button>
      <button type="button" onclick={() => dialog?.close()}>{t('Cancel')}</button>
    </div>
  </form>
</dialog>

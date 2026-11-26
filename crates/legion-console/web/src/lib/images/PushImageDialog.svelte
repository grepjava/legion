<script lang="ts">
  // A node's cached image pushed, under a new reference, to GHCR or
  // another registry.
  import { api, type CachedNodeImage } from '../api.ts';
  import { lightDismiss } from '../dialog.ts';
  import { t } from '../i18n.svelte.ts';
  import type { Studio } from './studio.svelte.ts';

  let { studio, onnewsecret }: { studio: Studio; onnewsecret: () => void } = $props();

  let dialog: HTMLDialogElement | undefined = $state();
  let node = $state('');
  let source = $state('');
  let target = $state('');

  export function show(img: CachedNodeImage) {
    node = img.node;
    source = img.reference;
    const leaf = img.reference.split('/').pop() ?? 'jail-app:latest';
    target = img.reference.startsWith('ghcr.io/') ? img.reference : `ghcr.io/owner/${leaf}`;
    studio.pushSecret = studio.secrets[0] ?? '';
    dialog?.showModal();
  }

  async function submit(e: SubmitEvent) {
    e.preventDefault();
    const to = target.trim();
    if (!to) return;
    await studio.act(async () => {
      const r = await api.nodeImageOp({
        op: 'push',
        node,
        image: source,
        target: to,
        namespace: studio.namespace,
        secret: studio.pushSecret
      });
      dialog?.close();
      return t('Pushed {target} ({digest})', { target: to, digest: r.digest ? r.digest.slice(0, 19) : 'ok' });
    });
  }
</script>

<dialog
  bind:this={dialog}
  closedby="any"
  aria-labelledby="push-ghcr-title"
  onclick={(e) => lightDismiss(e, dialog)}
>
  <header class="bar">
    <h2 id="push-ghcr-title">{t('Push cached image to GHCR / Registry')}</h2>
    <button type="button" class="sm" onclick={() => dialog?.close()}>{t('Close')}</button>
  </header>
  <form class="stacked-form" onsubmit={submit}>
    <p class="muted mono">{node} · {source}</p>
    <label>
      {t('Destination reference')}
      <input bind:value={target} placeholder="ghcr.io/owner/repo:tag" required />
    </label>
    <div class="controls">
      <label style="flex: 1">
        {t('Registry Secret in {namespace}', { namespace: studio.namespace })}
        <select bind:value={studio.pushSecret}>
          <option value="">{t('none')}</option>
          {#each studio.secrets as s (s)}<option value={s}>{s}</option>{/each}
        </select>
      </label>
      <button type="button" class="sm" onclick={onnewsecret}>
        {t('New GHCR Secret…')}
      </button>
    </div>
    <div class="controls">
      <button class="primary" disabled={studio.busy || !target.trim()}>{t('Push')}</button>
      <button type="button" onclick={() => dialog?.close()}>{t('Cancel')}</button>
    </div>
  </form>
</dialog>

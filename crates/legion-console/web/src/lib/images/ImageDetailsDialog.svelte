<script lang="ts">
  // A node's cached image, read from its store: how it was built.
  import { api, type CachedNodeImage, type RegistryImageInspect } from '../api.ts';
  import { lightDismiss } from '../dialog.ts';
  import { t } from '../i18n.svelte.ts';
  import ImageDetails from '../ImageDetails.svelte';

  let dialog: HTMLDialogElement | undefined = $state();
  let details: RegistryImageInspect | null = $state(null);
  let of = $state('');
  let error = $state('');

  export async function show(img: CachedNodeImage) {
    of = `${img.node} · ${img.reference}`;
    details = null;
    error = '';
    dialog?.showModal();
    try {
      details = await api.registryInspect(img.reference, '', '', img.node);
    } catch (err) {
      error = (err as Error).message;
    }
  }
</script>

<dialog
  bind:this={dialog}
  closedby="any"
  aria-labelledby="image-details-title"
  onclick={(e) => lightDismiss(e, dialog)}
>
  <header class="bar">
    <h2 id="image-details-title">{t('Image details')}</h2>
    <button type="button" class="sm" onclick={() => dialog?.close()}>{t('Close')}</button>
  </header>
  <p class="muted mono">{of}</p>
  {#if error}
    <p class="error" role="alert">{error}</p>
  {:else if details}
    <ImageDetails d={details} />
  {:else}
    <p class="muted">{t('Reading the node’s copy…')}</p>
  {/if}
</dialog>

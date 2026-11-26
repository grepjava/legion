<script lang="ts">
  // A node's cached image audited against FreeBSD's VuXML.
  import { api, type CachedNodeImage, type ImageScan } from '../api.ts';
  import { lightDismiss } from '../dialog.ts';
  import { t, tn } from '../i18n.svelte.ts';

  let dialog: HTMLDialogElement | undefined = $state();
  let scan: ImageScan | null = $state(null);
  let of = $state('');
  let error = $state('');

  export async function show(img: CachedNodeImage) {
    of = `${img.node} · ${img.reference}`;
    scan = null;
    error = '';
    dialog?.showModal();
    try {
      scan = await api.scanImage(img.node, img.reference);
    } catch (err) {
      error = (err as Error).message;
    }
  }
</script>

<dialog
  bind:this={dialog}
  closedby="any"
  aria-labelledby="image-scan-title"
  onclick={(e) => lightDismiss(e, dialog)}
>
  <header class="bar">
    <h2 id="image-scan-title">{t('Vulnerabilities')}</h2>
    <button type="button" class="sm" onclick={() => dialog?.close()}>{t('Close')}</button>
  </header>
  <p class="muted mono">{of}</p>
  {#if error}
    <p class="error" role="alert">{error}</p>
  {:else if !scan}
    <p class="muted">{t('Auditing its packages…')}</p>
  {:else if !scan.packages.length}
    <p>{t('No FreeBSD package database in this image: nothing to audit.')}</p>
  {:else}
    <p>
      {#if scan.vulnerable.length}
        <span class="state bad">{t('{n} vulnerable', { n: scan.vulnerable.length })}</span>
      {:else}
        <span class="state good">{t('no known vulnerability')}</span>
      {/if}
      {tn('1 package checked against FreeBSD’s VuXML', '{n} packages checked against FreeBSD’s VuXML', scan.packages.length)}
    </p>
    {#if scan.used_by.length}
      <p>{t('Used by: {who}', { who: scan.used_by.join(', ') })}</p>
    {/if}
    {#each scan.vulnerable as p (p.name)}
      <h3 class="mono">{p.name}-{p.version}</h3>
      <ul>
        {#each p.issues as i (i.url || i.description)}
          <li>
            {#if i.url}<a href={i.url} target="_blank" rel="noopener noreferrer">{i.description}</a>
            {:else}{i.description}{/if}
            {#if i.cves.length}<span class="mono muted">{i.cves.join(' ')}</span>{/if}
            {#if i.affected.length}<span class="muted">{t('affects {versions}', { versions: i.affected.join(', ') })}</span>{/if}
          </li>
        {/each}
      </ul>
    {/each}
    <details>
      <summary>{t('Every package ({n})', { n: scan.packages.length })}</summary>
      <ul class="mono">
        {#each scan.packages as p (p.name)}<li>{p.name}-{p.version}</li>{/each}
      </ul>
    </details>
  {/if}
</dialog>

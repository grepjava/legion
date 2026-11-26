<script lang="ts" module>
  /** The `g` chords: the key after `g`, and where it goes. */
  export const CHORDS: { key: string; to: string; admin?: boolean }[] = [
    { key: 'o', to: '/' },
    { key: 'a', to: '/alerts' },
    { key: 't', to: '/topology' },
    { key: 'n', to: '/network' },
    { key: 'v', to: '/volumes' },
    { key: 'i', to: '/images' },
    { key: 'b', to: '/build' },
    { key: 's', to: '/stacks' },
    { key: 'd', to: '/deploy' },
    { key: 'y', to: '/apply' },
    { key: 'c', to: '/cluster', admin: true }
  ];
</script>

<script lang="ts">
  // The keyboard shortcuts, shown by `?` or the side bar's link.
  import { t } from './i18n.svelte.ts';
  import { lightDismiss } from './dialog.ts';
  import { session } from './session.svelte.ts';

  let { open = $bindable(false) } = $props();
  let dialog: HTMLDialogElement | undefined = $state();

  $effect(() => {
    if (!dialog) return;
    if (open && !dialog.open) dialog.showModal();
    else if (!open && dialog.open) dialog.close();
  });

  const where = $derived<Record<string, string>>({
    '/': t('Overview'),
    '/alerts': t('Alerts'),
    '/topology': t('Topology'),
    '/network': t('Network'),
    '/volumes': t('Volumes'),
    '/images': t('Images'),
    '/build': t('Builder'),
    '/stacks': t('Stacks'),
    '/deploy': t('Deploy'),
    '/apply': t('Apply YAML'),
    '/cluster': t('Operations')
  });
</script>

<dialog
  bind:this={dialog}
  class="shortcuts"
  aria-labelledby="shortcuts-title"
  closedby="any"
  onclick={(e) => lightDismiss(e, dialog)}
  onclose={() => (open = false)}
>
  <header class="bar">
    <h2 id="shortcuts-title">{t('Keyboard shortcuts')}</h2>
    <button type="button" class="sm" onclick={() => (open = false)}>{t('Close')}</button>
  </header>
  <div class="grid-2">
    <dl class="keys">
      <dt><kbd>Ctrl</kbd> <kbd>K</kbd></dt>
      <dd>{t('Jump to a page or object, or run an action')}</dd>
      <dt><kbd>?</kbd></dt>
      <dd>{t('These shortcuts')}</dd>
      <dt><kbd>/</kbd></dt>
      <dd>{t("The page's filter or search")}</dd>
      <dt><kbd>[</kbd></dt>
      <dd>{t('Hide or show the side bar')}</dd>
      <dt><kbd>Alt</kbd> <kbd>↑</kbd> <kbd>↓</kbd></dt>
      <dd>{t('Move a pinned page or a workbench tab')}</dd>
    </dl>
    <dl class="keys">
      {#each CHORDS.filter((c) => !c.admin || session.me?.all) as c (c.key)}
        <dt><kbd>g</kbd> <kbd>{c.key}</kbd></dt>
        <dd>{where[c.to]}</dd>
      {/each}
    </dl>
  </div>
  <p class="muted">
    {t('Drop a compose file anywhere to import it, or other YAML to apply it; drop an archive or backup on its field to choose it.')}
  </p>
</dialog>

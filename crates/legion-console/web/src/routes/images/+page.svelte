<script lang="ts">
  // Images, in three tabs:
  // 1) Cluster Node Images: the OCI images each node holds (jails and bhyve
  //    VMs): .tar export and upload, push to ghcr.io, pre-pull, scan,
  //    remove, prune (lib/images/NodeImages.svelte).
  // 2) Search & OCI Inspector: the FreeBSD catalog, Docker Hub and ghcr.io
  //    searched; any image's platforms, tags, digest and cosign signature
  //    (lib/images/SearchImages.svelte).
  // 3) Package & Push: a Build from a Containerfile or a Git repository,
  //    pushed to ghcr.io; the Build runs, their logs in the Workbench
  //    (lib/images/BuildImage.svelte).
  // What they share (the namespace, its Secrets, what happened) is a
  // Studio (lib/images/studio.svelte.ts), which also keeps each tab's
  // draft while another is open.
  import Tabs from '#lib/Tabs.svelte';
  import { fromAddress, keepInAddress } from '#lib/address.svelte.ts';
  import { api } from '#lib/api.ts';
  import { t } from '#lib/i18n.svelte.ts';
  import { cannotWrite, namespaces, session } from '#lib/session.svelte.ts';
  import BuildImage from '#lib/images/BuildImage.svelte';
  import ImageDetailsDialog from '#lib/images/ImageDetailsDialog.svelte';
  import ImageScanDialog from '#lib/images/ImageScanDialog.svelte';
  import NodeImages from '#lib/images/NodeImages.svelte';
  import PushImageDialog from '#lib/images/PushImageDialog.svelte';
  import RegistrySecretDialog from '#lib/images/RegistrySecretDialog.svelte';
  import SearchImages from '#lib/images/SearchImages.svelte';
  import { fromPackages, Studio } from '#lib/images/studio.svelte.ts';

  type TabId = 'nodes' | 'search' | 'package';
  const TABS: TabId[] = ['nodes', 'search', 'package'];
  // The tab the address names, else the one for the user; the tab open is
  // kept in the address, so a reload or a link keeps it. Cluster Node
  // Images is only for those who see the whole cluster.
  const firstTab: TabId = session.me?.all ? 'nodes' : 'search';
  const askedTab = fromAddress('tab');
  let tab: TabId = $state(TABS.find((x) => x === askedTab && (x !== 'nodes' || session.me?.all)) ?? firstTab);
  keepInAddress('tab', () => tab, firstTab);

  let adminNamespaces: string[] = $state([]);
  const nsChoices = $derived(namespaces(adminNamespaces));
  const workNs = $derived(session.namespace || nsChoices[0] || 'default');
  const studio = new Studio(
    () => workNs,
    () => cannotWrite(workNs)
  );

  $effect(() => {
    if (session.me?.all) {
      api
        .list('Namespace')
        .then((r) => (adminNamespaces = r.objects.map((o) => o.metadata.name).sort()))
        .catch(() => {});
      api
        .list('Node')
        .then((r) => (studio.nodes = r.objects))
        .catch(() => {});
    }
  });

  $effect(() => {
    const ns = workNs;
    if (!ns) return;
    api
      .list('Secret', ns)
      .then((r) => (studio.secrets = r.objects.map((o) => o.metadata.name).sort()))
      .catch(() => (studio.secrets = []));
  });

  const tabs = $derived([
    ...(session.me?.all ? [{ id: 'nodes' as TabId, label: t('Cluster Node Images') }] : []),
    { id: 'search' as TabId, label: t('Search & OCI Inspector') },
    { id: 'package' as TabId, label: t('Package & Push to GHCR') }
  ]);

  let details: ImageDetailsDialog | undefined = $state();
  let scan: ImageScanDialog | undefined = $state();
  let push: PushImageDialog | undefined = $state();
  let secret: RegistrySecretDialog | undefined = $state();

  // No FreeBSD image found: a Build that installs FreeBSD's package.
  function buildFromPackages(name: string) {
    studio.build.from = 'containerfile';
    studio.build.containerfile = fromPackages(name);
    tab = 'package';
  }
</script>

<header class="bar">
  <h1>{t('Images')}</h1>
  <div class="controls">
    <button type="button" class="sm" onclick={() => secret?.show()}>
      {t('Add GHCR / Registry Login')}
    </button>
  </div>
</header>
<p class="muted">
  {t(
    'Search FreeBSD jail & OCI images, inspect multi-arch manifests and cosign signatures, package custom jails from Containerfiles or Git, push directly to GitHub Container Registry (ghcr.io), or export and load offline .tar image archives.'
  )}
</p>

{#if studio.error}<p class="error" role="alert">{studio.error}</p>{/if}
{#if studio.message}<p class="ok" role="status">{studio.message}</p>{/if}

<Tabs {tabs} bind:tab label={t('Image studio views')} idPrefix="img-tab" panelId="img-panel" />

<div role="tabpanel" id="img-panel" aria-labelledby="img-tab-{tab}">
  {#if tab === 'nodes'}
    <NodeImages
      {studio}
      ondetails={(img) => details?.show(img)}
      onscan={(img) => scan?.show(img)}
      onpush={(img) => push?.show(img)}
    />
  {:else if tab === 'search'}
    <SearchImages {studio} onbuild={buildFromPackages} />
  {:else}
    <BuildImage {studio} onnewsecret={() => secret?.show()} />
  {/if}
</div>

<ImageDetailsDialog bind:this={details} />
<ImageScanDialog bind:this={scan} />
<PushImageDialog bind:this={push} {studio} onnewsecret={() => secret?.show()} />
<RegistrySecretDialog bind:this={secret} {studio} />

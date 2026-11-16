<script lang="ts">
  // Docker-Desktop Jail Images & GHCR Packaging Hub:
  // 1) Cluster Node Images: cached OCI jail images across nodes, .tar export,
  //    .tar upload/load, re-tag & push to ghcr.io, pre-pull, remove, prune.
  // 2) Registry Search & OCI Inspector: FreeBSD jail catalog + Docker Hub
  //    search, plus ghcr.io/OCI multi-arch, tag, digest & cosign inspector.
  // 3) Jail Image Packaging & GHCR Push Studio: Containerfile templates or
  //    Git repo -> jailer build -> push to ghcr.io + inline GHCR PAT Secret
  //    creator + live Build runs table & Workbench log docking.
  import { droppable } from '#lib/drop.svelte.ts';
  import { goto } from '$app/navigation';
  import {
    api,
    downloads,
    follow,
    type CachedNodeImage,
    type ImageScan,
    type LObject,
    type RegistryImageInspect,
    type RegistrySearchHit
  } from '#lib/api.ts';
  import { tone } from '#lib/columns.ts';
  import ImageDetails from '#lib/ImageDetails.svelte';
  import { t, tn } from '#lib/i18n.svelte.ts';
  import { cannotWrite, namespaces, openWorkbench, session } from '#lib/session.svelte.ts';

  type TabId = 'nodes' | 'search' | 'package';
  let tab: TabId = $state(session.me?.all ? 'nodes' : 'search');
  let tablist: HTMLElement | undefined = $state();

  let adminNamespaces: string[] = $state([]);
  let nodes: LObject[] = $state([]);
  let builds: LObject[] = $state([]);
  let services: LObject[] = $state([]);
  let secrets: string[] = $state([]);

  let error = $state('');
  let message = $state('');
  let busy = $state(false);

  const nsChoices = $derived(namespaces(adminNamespaces));
  const workNs = $derived(session.namespace || nsChoices[0] || 'default');
  const writeWhy = $derived(cannotWrite(workNs));

  $effect(() => {
    if (session.me?.all) {
      api
        .list('Namespace')
        .then((r) => (adminNamespaces = r.objects.map((o) => o.metadata.name).sort()))
        .catch(() => {});
      api
        .list('Node')
        .then((r) => (nodes = r.objects))
        .catch(() => {});
    }
  });

  $effect(() => {
    const ns = workNs;
    if (!ns) return;
    api
      .list('Secret', ns)
      .then((r) => (secrets = r.objects.map((o) => o.metadata.name).sort()))
      .catch(() => (secrets = []));
    const stops = [
      follow('Build', ns, (o) => (builds = o.slice().reverse()), () => {}),
      follow('Service', ns, (o) => (services = o), () => {})
    ];
    return () => stops.forEach((s) => s());
  });

  const tabs = $derived(
    [
      ...(session.me?.all ? [{ id: 'nodes' as TabId, label: t('Cluster Node Images') }] : []),
      { id: 'search' as TabId, label: t('Search & OCI Inspector') },
      { id: 'package' as TabId, label: t('Package & Push to GHCR') }
    ]
  );

  function tabKey(e: KeyboardEvent) {
    const i = tabs.findIndex((x) => x.id === tab);
    const to =
      e.key === 'ArrowRight'
        ? (i + 1) % tabs.length
        : e.key === 'ArrowLeft'
          ? (i - 1 + tabs.length) % tabs.length
          : e.key === 'Home'
            ? 0
            : e.key === 'End'
              ? tabs.length - 1
              : -1;
    if (to < 0) return;
    e.preventDefault();
    tab = tabs[to].id;
    tablist?.querySelectorAll<HTMLElement>('[role="tab"]')[to]?.focus();
  }

  function lightDismiss(e: MouseEvent, dlg: HTMLDialogElement | undefined) {
    if (!dlg || 'closedBy' in HTMLDialogElement.prototype) return;
    if (e.target !== dlg) return;
    const r = dlg.getBoundingClientRect();
    const inside =
      r.top <= e.clientY &&
      e.clientY <= r.top + r.height &&
      r.left <= e.clientX &&
      e.clientX <= r.left + r.width;
    if (!inside) dlg.close();
  }

  // --- Tab 1: Cluster Node Images ---
  let cached: CachedNodeImage[] = $state([]);
  let loadingImages = $state(false);
  let nodeFilter = $state('');
  let imageFilter = $state('');
  let pullRef = $state('');
  let pullNode = $state('');
  let pullSecret = $state('');
  let loadFiles: FileList | undefined = $state();
  let loadNode = $state('');
  let loadRef = $state('');

  async function refreshNodeImages() {
    if (!session.me?.all) return;
    loadingImages = true;
    try {
      const r = await api.nodeImages(nodeFilter);
      cached = r.images;
    } catch (e) {
      error = (e as Error).message;
    } finally {
      loadingImages = false;
    }
  }

  $effect(() => {
    if (tab === 'nodes' && session.me?.all) {
      void nodeFilter;
      refreshNodeImages();
    }
  });

  // Jails, bhyve VMs (`legion.vm.image`), or both, as the node says.
  let kindFilter: 'all' | 'jail' | 'vm' = $state('all');

  const shownImages = $derived.by(() => {
    const q = imageFilter.trim().toLowerCase();
    return cached.filter(
      (i) =>
        (kindFilter === 'all' || (kindFilter === 'vm') === !!i.vm) &&
        (!q ||
          i.reference.toLowerCase().includes(q) ||
          i.node.toLowerCase().includes(q) ||
          i.digest.toLowerCase().includes(q))
    );
  });

  async function pullImage(e: SubmitEvent) {
    e.preventDefault();
    if (!pullRef.trim()) return;
    busy = true;
    error = '';
    message = '';
    try {
      const r = await api.nodeImageOp({
        op: 'pull',
        node: pullNode,
        image: pullRef.trim(),
        namespace: workNs,
        secret: pullSecret
      });
      message = t('Pulled {image} ({digest})', {
        image: pullRef.trim(),
        digest: r.digest ? r.digest.slice(0, 19) : 'ok'
      });
      pullRef = '';
      await refreshNodeImages();
    } catch (err) {
      error = (err as Error).message;
    } finally {
      busy = false;
    }
  }

  async function uploadTar(e: SubmitEvent) {
    e.preventDefault();
    const f = loadFiles?.[0];
    if (!f || !loadNode.trim()) return;
    busy = true;
    error = '';
    message = '';
    try {
      const r = await api.loadImage(f, {
        node: loadNode.trim(),
        reference: loadRef.trim() || undefined
      });
      message = tn(
        'Loaded archive onto {node} ({n} byte)',
        'Loaded archive onto {node} ({n} bytes)',
        r.bytes,
        { node: r.node }
      );
      await refreshNodeImages();
    } catch (err) {
      error = (err as Error).message;
    } finally {
      busy = false;
    }
  }

  async function pruneNodes() {
    if (!confirm(t('Prune unreferenced image layers on {target}?', { target: nodeFilter || t('all ready nodes') }))) {
      return;
    }
    busy = true;
    error = '';
    message = '';
    try {
      const r = await api.nodeImageOp({ op: 'prune', node: nodeFilter });
      message = tn('Pruned {n} unused layer/image', 'Pruned {n} unused layers/images', r.removed);
      await refreshNodeImages();
    } catch (err) {
      error = (err as Error).message;
    } finally {
      busy = false;
    }
  }

  // Push cached node image to GHCR modal
  let pushDialog: HTMLDialogElement | undefined = $state();
  let pushSourceNode = $state('');
  let pushSourceRef = $state('');
  let pushTargetRef = $state('');
  let pushSecretName = $state('');

  // A node's cached image, read from its store: how it was built.
  let detailsDialog: HTMLDialogElement | undefined = $state();
  let details: RegistryImageInspect | null = $state(null);
  let detailsOf = $state('');
  let detailsError = $state('');

  async function showDetails(img: CachedNodeImage) {
    detailsOf = `${img.node} · ${img.reference}`;
    details = null;
    detailsError = '';
    detailsDialog?.showModal();
    try {
      details = await api.registryInspect(img.reference, '', '', img.node);
    } catch (err) {
      detailsError = (err as Error).message;
    }
  }

  // A node's cached image audited against FreeBSD's VuXML.
  let scanDialog: HTMLDialogElement | undefined = $state();
  let scan: ImageScan | null = $state(null);
  let scanError = $state('');

  async function scanImage(img: CachedNodeImage) {
    detailsOf = `${img.node} · ${img.reference}`;
    scan = null;
    scanError = '';
    scanDialog?.showModal();
    try {
      scan = await api.scanImage(img.node, img.reference);
    } catch (err) {
      scanError = (err as Error).message;
    }
  }

  function openPushModal(img: CachedNodeImage) {
    pushSourceNode = img.node;
    pushSourceRef = img.reference;
    const leaf = img.reference.split('/').pop() ?? 'jail-app:latest';
    pushTargetRef = img.reference.startsWith('ghcr.io/') ? img.reference : `ghcr.io/owner/${leaf}`;
    pushSecretName = secrets[0] ?? '';
    pushDialog?.showModal();
  }

  async function submitPushCached(e: SubmitEvent) {
    e.preventDefault();
    if (!pushTargetRef.trim()) return;
    busy = true;
    error = '';
    message = '';
    try {
      const r = await api.nodeImageOp({
        op: 'push',
        node: pushSourceNode,
        image: pushSourceRef,
        target: pushTargetRef.trim(),
        namespace: workNs,
        secret: pushSecretName
      });
      pushDialog?.close();
      message = t('Pushed {target} ({digest})', {
        target: pushTargetRef.trim(),
        digest: r.digest ? r.digest.slice(0, 19) : 'ok'
      });
    } catch (err) {
      error = (err as Error).message;
    } finally {
      busy = false;
    }
  }

  // --- Tab 2: Registry Search & OCI Inspector ---
  let searchQuery = $state('');
  let searchSecret = $state('');
  let sourceFilter: 'all' | 'freebsd' | 'ghcr' | 'docker' = $state('all');
  let hits: RegistrySearchHit[] = $state([]);
  let searching = $state(false);
  let inspectInput = $state('');
  let inspectSecret = $state('');
  let inspected: RegistryImageInspect | null = $state(null);
  let inspecting = $state(false);

  const filteredHits = $derived(
    hits.filter((h) => {
      const isGhcr = h.source === 'ghcr.io' || h.name.startsWith('ghcr.io/');
      if (sourceFilter === 'freebsd' && h.source !== 'freebsd-catalog') return false;
      if (sourceFilter === 'ghcr' && !isGhcr) return false;
      if (sourceFilter === 'docker' && h.source !== 'docker-hub') return false;
      // Whether a hit is a VM image is in its config's labels
      // (legion.vm.image, as `legion build vm` sets it), which a search
      // does not fetch: the inspector shows it.
      return true;
    })
  );

  async function runSearch(e?: Event) {
    e?.preventDefault();
    searching = true;
    error = '';
    try {
      const r = await api.registrySearch(searchQuery.trim(), 24, workNs, searchSecret);
      hits = r.results;
    } catch (err) {
      error = (err as Error).message;
    } finally {
      searching = false;
    }
  }

  $effect(() => {
    if (tab === 'search' && !hits.length && !searching) {
      runSearch();
    }
  });

  async function runInspect(refToInspect?: string) {
    const target = (refToInspect ?? inspectInput).trim();
    if (!target) return;
    inspectInput = target;
    inspecting = true;
    error = '';
    try {
      inspected = await api.registryInspect(target, workNs, inspectSecret);
    } catch (err) {
      inspected = null;
      error = (err as Error).message;
    } finally {
      inspecting = false;
    }
  }

  // --- Tab 3: Jail Image Packaging & GHCR Push Studio ---
  const TEMPLATES: Record<string, string> = {
    minimal:
      'FROM ghcr.io/freebsd/freebsd-runtime:14.2\nCOPY --chmod=0755 <<EOF /usr/local/bin/entrypoint.sh\n#!/bin/sh\necho "FreeBSD jail ready"\nsleep 3600\nEOF\nCMD ["/usr/local/bin/entrypoint.sh"]\n',
    pkg:
      'FROM dougrabson/freebsd:14.2\nRUN ASSUME_ALWAYS_YES=yes pkg install -y nginx ca_root_nss\nEXPOSE 80\nCMD ["/usr/local/sbin/nginx", "-g", "daemon off;"]\n',
    daemon:
      'FROM dougrabson/freebsd-minimal:14.2\nWORKDIR /app\nENV APP_ENV=production\nEXPOSE 8080\nCMD ["/bin/sh", "-c", "echo listening on 8080 && sleep 3600"]\n'
  };

  let pkgFrom: 'containerfile' | 'git' = $state('containerfile');
  let pkgContainerfile = $state(TEMPLATES.minimal);
  let pkgGitUrl = $state('');
  let pkgGitRef = $state('');
  let pkgGitPath = $state('');
  let pkgImage = $state('ghcr.io/your-org/freebsd-app:v1');
  let pkgSecret = $state('');
  let pkgRolloutService = $state('');

  async function startPackageBuild(e: SubmitEvent) {
    e.preventDefault();
    error = '';
    message = '';
    const baseName =
      pkgImage
        .split('@')[0]
        .split(':')[0]
        .split('/')
        .pop()
        ?.toLowerCase()
        .replace(/[^a-z0-9-]/g, '-')
        .replace(/^-+|-+$/g, '') || 'image';
    const name = `${baseName}-${Date.now().toString(36)}`.slice(0, 63);
    const source =
      pkgFrom === 'git'
        ? {
            git: {
              url: pkgGitUrl.trim(),
              ...(pkgGitRef.trim() ? { ref: pkgGitRef.trim() } : {}),
              ...(pkgGitPath.trim() ? { path: pkgGitPath.trim() } : {})
            }
          }
        : { containerfile: pkgContainerfile };
    const spec: Record<string, unknown> = {
      source,
      image: pkgImage.trim(),
      ...(pkgSecret ? { pushSecret: pkgSecret } : {}),
      ...(pkgRolloutService ? { rollout: { service: pkgRolloutService } } : {})
    };
    const obj = {
      apiVersion: 'legion/v1',
      kind: 'Build',
      metadata: { name, namespace: workNs },
      spec
    };
    busy = true;
    try {
      await api.apply({ object: obj });
      message = t('Started Build {namespace}/{name} → {image}', {
        namespace: workNs,
        name,
        image: pkgImage.trim()
      });
      const created = await api.get('Build', workNs, name).catch(() => null);
      if (created) {
        openWorkbench({
          id: `build:${workNs}/${name}`,
          kind: 'build',
          title: `${name} · ${t('Build')}`,
          namespace: workNs,
          name,
          object: created
        });
      }
    } catch (err) {
      error = (err as Error).message;
    } finally {
      busy = false;
    }
  }

  // Inline GHCR / Registry Docker Config Secret Creator
  let secretDialog: HTMLDialogElement | undefined = $state();
  let secName = $state('ghcr-cred');
  let secServer = $state('ghcr.io');
  let secUser = $state('');
  let secToken = $state('');

  async function createRegistrySecret(e: SubmitEvent) {
    e.preventDefault();
    busy = true;
    error = '';
    try {
      const auth = btoa(`${secUser.trim()}:${secToken}`);
      const dockerCfg = JSON.stringify({
        auths: {
          [secServer.trim()]: {
            username: secUser.trim(),
            password: secToken,
            auth
          }
        }
      });
      await api.apply({
        object: {
          apiVersion: 'legion/v1',
          kind: 'Secret',
          metadata: { name: secName.trim(), namespace: workNs },
          spec: { text: dockerCfg }
        }
      });
      const nameMade = secName.trim();
      secrets = [...new Set([...secrets, nameMade])].sort();
      pkgSecret = nameMade;
      pushSecretName = nameMade;
      pullSecret = nameMade;
      secToken = '';
      secretDialog?.close();
      message = t('Saved registry Secret {namespace}/{name}', {
        namespace: workNs,
        name: nameMade
      });
    } catch (err) {
      error = (err as Error).message;
    } finally {
      busy = false;
    }
  }
</script>

<header class="bar">
  <h1>{t('Images & GHCR Hub')}</h1>
  <div class="controls">
    <button type="button" class="sm" onclick={() => secretDialog?.showModal()}>
      {t('Add GHCR / Registry Login')}
    </button>
  </div>
</header>
<p class="muted">
  {t(
    'Search FreeBSD jail & OCI images, inspect multi-arch manifests and cosign signatures, package custom jails from Containerfiles or Git, push directly to GitHub Container Registry (ghcr.io), or export and load offline .tar image archives.'
  )}
</p>

{#if error}<p class="error" role="alert">{error}</p>{/if}
{#if message}<p class="ok" role="status">{message}</p>{/if}

<div class="tabs" role="tablist" aria-label={t('Image studio views')} bind:this={tablist}>
  {#each tabs as item (item.id)}
    <button
      type="button"
      role="tab"
      id="img-tab-{item.id}"
      aria-selected={tab === item.id}
      tabindex={tab === item.id ? 0 : -1}
      onclick={() => (tab = item.id)}
      onkeydown={tabKey}
    >
      {item.label}
    </button>
  {/each}
</div>

<div role="tabpanel" aria-labelledby="img-tab-{tab}">
  {#if tab === 'nodes'}
    <section class="grid-2 studio-top">
      <div class="card">
        <h2>{t('Pre-pull image to cluster')}</h2>
        <form class="controls" onsubmit={pullImage}>
          <input
            bind:value={pullRef}
            placeholder="ghcr.io/freebsd/freebsd-runtime:14.2"
            aria-label={t('Image reference')}
            required
          />
          <select bind:value={pullNode} aria-label={t('Target node')}>
            <option value="">{t('All ready nodes')}</option>
            {#each nodes as n (n.metadata.name)}
              <option value={n.metadata.name}>{n.metadata.name}</option>
            {/each}
          </select>
          <select bind:value={pullSecret} aria-label={t('Registry login Secret')}>
            <option value="">{t('Public (no login)')}</option>
            {#each secrets as s (s)}<option value={s}>{s}</option>{/each}
          </select>
          <button class="primary" disabled={busy || !pullRef.trim()}>{t('Pull')}</button>
        </form>
      </div>

      <div class="card">
        <h2>{t('Load OCI / Docker .tar archive')}</h2>
        <form class="controls" onsubmit={uploadTar}>
          <span class="dropzone" use:droppable><input type="file" accept=".tar,application/x-tar" bind:files={loadFiles} aria-label={t('Image archive')} /><span class="hint" aria-hidden="true">{t('or drop it here')}</span></span>
          <select bind:value={loadNode} aria-label={t('Node')} required>
            <option value="">{t('Choose node…')}</option>
            {#each nodes as n (n.metadata.name)}
              <option value={n.metadata.name}>{n.metadata.name}</option>
            {/each}
          </select>
          <input
            bind:value={loadRef}
            placeholder={t('tag override (optional)')}
            aria-label={t('Tag override')}
          />
          <button class="primary" disabled={busy || !loadFiles?.length || !loadNode}>
            {t('Upload .tar')}
          </button>
        </form>
      </div>
    </section>

    <div class="bar">
      <h2>{t('Cached node images ({n})', { n: shownImages.length })}</h2>
      <div class="controls">
        <input
          type="search"
          bind:value={imageFilter}
          placeholder={t('Filter reference, digest, node…')}
          aria-label={t('Filter cached images')}
        />
        <div class="pill-row" role="group" aria-label={t('Filter by workload')}>
          {#each [{ id: 'all', label: t('Jails & VMs') }, { id: 'jail', label: t('Jails') }, { id: 'vm', label: t('bhyve VMs') }] as const as f (f.id)}
            <button
              type="button"
              class="pill"
              class:active={kindFilter === f.id}
              aria-pressed={kindFilter === f.id}
              onclick={() => (kindFilter = f.id)}>{f.label}</button
            >
          {/each}
        </div>
        <select bind:value={nodeFilter} aria-label={t('Filter by node')}>
          <option value="">{t('All nodes')}</option>
          {#each nodes as n (n.metadata.name)}
            <option value={n.metadata.name}>{n.metadata.name}</option>
          {/each}
        </select>
        <button type="button" class="sm" onclick={refreshNodeImages} disabled={loadingImages}>
          {t('Refresh')}
        </button>
        <button type="button" class="sm danger" onclick={pruneNodes} disabled={busy}>
          {t('Prune unused')}
        </button>
      </div>
    </div>

    {#if shownImages.length}
      <!-- svelte-ignore a11y_no_noninteractive_tabindex -->
      <div class="table-scroll" tabindex="0" role="region" aria-label={t('Cached node images')}>
        <table>
          <caption class="sr-only">{t('Cached node images')}</caption>
          <thead>
            <tr>
              <th scope="col">{t('Reference')}</th>
              <th scope="col">{t('Node')}</th>
              <th scope="col">{t('Platform')}</th>
              <th scope="col">{t('Layers')}</th>
              <th scope="col">{t('Used by')}</th>
              <th scope="col">{t('Digest')}</th>
              <th scope="col">{t('Actions')}</th>
            </tr>
          </thead>
          <tbody>
            {#each shownImages as img (`${img.node}:${img.reference}`)}
              <tr>
                <td class="mono">
                  <strong>{img.reference}</strong>
                  {#if img.vm}<span class="tag">{t('bhyve VM')}</span>{/if}
                </td>
                <td class="mono">{img.node}</td>
                <td class="mono">{img.os}/{img.arch}</td>
                <td class="mono">{img.layers}</td>
                <td>
                  {#each img.used_by as who (who)}
                    <div class="mono">{who}</div>
                  {:else}
                    <span class="muted">{t('nothing')}</span>
                  {/each}
                </td>
                <td class="mono muted" title={img.digest}>
                  {img.digest ? img.digest.slice(0, 19) : '—'}
                </td>
                <td class="controls">
                  <button type="button" class="sm" onclick={() => showDetails(img)}>
                    {t('Details')}
                  </button>
                  <button type="button" class="sm" onclick={() => scanImage(img)}>
                    {t('Scan')}
                  </button>
                  <a
                    class="button sm"
                    href={downloads.image(img.node, img.reference)}
                    download
                  >
                    {t('Download .tar')}
                  </a>
                  <button type="button" class="sm" onclick={() => openPushModal(img)}>
                    {t('Push to GHCR')}
                  </button>
                  <button
                    type="button"
                    class="sm"
                    onclick={() =>
                      goto(
                        `/deploy?namespace=${encodeURIComponent(workNs)}&image=${encodeURIComponent(img.reference)}`
                      )}
                  >
                    {t('Deploy')}
                  </button>
                  <button
                    type="button"
                    class="sm danger"
                    disabled={busy}
                    onclick={async () => {
                      if (!confirm(t('Remove cached image {image} from {node}?', { image: img.reference, node: img.node }))) return;
                      busy = true;
                      try {
                        await api.nodeImageOp({ op: 'remove', node: img.node, image: img.reference });
                        await refreshNodeImages();
                      } catch (err) {
                        error = (err as Error).message;
                      } finally {
                        busy = false;
                      }
                    }}
                  >
                    {t('Remove')}
                  </button>
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {:else if !loadingImages}
      <p class="muted">{t('No cached images reported on the selected nodes.')}</p>
    {/if}
  {:else if tab === 'search'}
    <div class="split-pane">
      <section class="card">
        <h2>{t('Search FreeBSD catalog, Docker Hub & ghcr.io')}</h2>
        <form class="controls" onsubmit={runSearch}>
          <input
            type="search"
            bind:value={searchQuery}
            placeholder={t('freebsd, postgres, or ghcr.io/owner/repo…')}
            aria-label={t('Search images')}
          />
          <select bind:value={searchSecret} aria-label={t('GHCR login Secret')}>
            <option value="">{t('No GHCR Secret')}</option>
            {#each secrets as s (s)}
              <option value={s}>{s}</option>
            {/each}
          </select>
          <button class="primary" disabled={searching}>{t('Search')}</button>
        </form>

        <div class="pill-row" role="group" aria-label={t('Filter by source')}>
          {#each [{ id: 'all', label: t('All sources') }, { id: 'freebsd', label: t('FreeBSD catalog') }, { id: 'ghcr', label: 'ghcr.io' }, { id: 'docker', label: 'Docker Hub' }] as const as f (f.id)}
            <button
              type="button"
              class="pill"
              class:active={sourceFilter === f.id}
              aria-pressed={sourceFilter === f.id}
              onclick={() => (sourceFilter = f.id)}>{f.label}</button
            >
          {/each}
        </div>

        {#if filteredHits.length}
          <ul class="hit-list" role="list">
            {#each filteredHits as h (h.name)}
              {@const fullRef = h.suggested_tag ? `${h.name}:${h.suggested_tag}` : h.name}
              <li class="hit-card">
                <div class="hit-head">
                  <strong class="mono">{h.name}</strong>
                  <span>
                    <span class="tag"
                      >{h.source === 'freebsd-catalog'
                        ? t('FreeBSD jail')
                        : h.source === 'ghcr.io'
                          ? 'ghcr.io'
                          : 'Docker Hub'}</span
                    >
                    {#if h.official}<span class="state good">{t('official')}</span>{/if}
                    {#if h.stars > 0}<span class="tag">★ {h.stars}</span>{/if}
                  </span>
                </div>
                <p class="muted hit-desc">{h.description || '—'}</p>
                <div class="controls">
                  <span class="mono muted">{h.suggested_tag ? `:${h.suggested_tag}` : t('tag not known')}</span>
                  <button
                    type="button"
                    class="sm"
                    onclick={() => {
                      if (searchSecret && !inspectSecret) inspectSecret = searchSecret;
                      runInspect(fullRef);
                    }}
                  >
                    {t('Inspect OCI')}
                  </button>
                  {#if session.me?.all}
                    <button
                      type="button"
                      class="sm"
                      disabled={busy}
                      onclick={async () => {
                        busy = true;
                        try {
                          await api.nodeImageOp({ op: 'pull', image: fullRef });
                          message = t('Pulled {image} onto ready nodes', { image: fullRef });
                        } catch (err) {
                          error = (err as Error).message;
                        } finally {
                          busy = false;
                        }
                      }}
                    >
                      {t('Pre-pull')}
                    </button>
                  {/if}
                  <button
                    type="button"
                    class="sm primary"
                    onclick={() =>
                      goto(
                        `/deploy?namespace=${encodeURIComponent(workNs)}&image=${encodeURIComponent(fullRef)}`
                      )}
                  >
                    {t('Deploy')}
                  </button>
                </div>
              </li>
            {/each}
          </ul>
        {/if}
      </section>

      <section class="card">
        <h2>{t('OCI / GHCR Image Inspector')}</h2>
        <form
          class="controls"
          onsubmit={(e) => {
            e.preventDefault();
            runInspect();
          }}
        >
          <input
            bind:value={inspectInput}
            placeholder="ghcr.io/freebsd/freebsd-runtime:14.2"
            aria-label={t('Image to inspect')}
            required
          />
          <select bind:value={inspectSecret} aria-label={t('Registry login Secret')}>
            <option value="">{t('Public')}</option>
            {#each secrets as s (s)}<option value={s}>{s}</option>{/each}
          </select>
          <button class="primary" disabled={inspecting || !inspectInput.trim()}>
            {t('Inspect')}
          </button>
        </form>

        {#if inspecting}
          <p class="muted">{t('Querying registry manifest & tags…')}</p>
        {:else if inspected}
          <dl class="facts">
            <dt>{t('Image')}</dt>
            <dd class="mono">{inspected.image}</dd>
            <dt>{t('Digest')}</dt>
            <dd class="mono">{inspected.digest}</dd>
            <dt>{t('Cosign policy')}</dt>
            <dd>
              <span class="state {inspected.signed ? 'good' : 'wait'}">
                {inspected.signed ? t('verified signature') : t('unsigned / no matching ImagePolicy')}
              </span>
            </dd>
            <dt>{t('Workload')}</dt>
            <dd>
              {inspected.run?.labels?.['legion.vm.image'] === 'true' ? t('bhyve VM') : t('FreeBSD Jail')}
            </dd>
            <dt>{t('Platforms')}</dt>
            <dd>
              {#if inspected.platforms.length}
                {#each inspected.platforms as p (`${p.os}/${p.arch}:${p.digest}`)}
                  <span class="tag mono">{p.os}/{p.arch}</span>
                {/each}
              {:else}
                <span class="muted">{t('single-manifest image')}</span>
              {/if}
            </dd>
          </dl>
          <ImageDetails d={inspected} />

          <div class="controls" style="margin-top: 0.75rem">
            <button
              type="button"
              class="primary sm"
              onclick={() =>
                goto(
                  `/deploy?namespace=${encodeURIComponent(workNs)}&image=${encodeURIComponent(inspected!.image)}`
                )}
            >
              {t('Deploy this image')}
            </button>
          </div>

          {#if inspected.tags.length}
            <h3>{t('Available tags ({n})', { n: inspected.tags.length })}</h3>
            <div class="tag-cloud">
              {#each inspected.tags.slice(0, 60) as tg (tg)}
                {@const repo = inspected.image.split('@')[0].replace(/:[^:/]*$/, '')}
                <button
                  type="button"
                  class="sm mono"
                  onclick={() => runInspect(`${repo}:${tg}`)}
                >
                  {tg}
                </button>
              {/each}
            </div>
          {/if}
        {:else}
          <p class="muted">
            {t(
              'Enter any ghcr.io, Docker Hub, or private OCI reference to inspect its manifest digest, FreeBSD/Linux architectures, cosign verification, and available tags.'
            )}
          </p>
        {/if}
      </section>
    </div>
  {:else}
    <div class="split-pane">
      <section class="card">
        <h2>{t('Package jail image & push to GHCR')}</h2>
        <form class="pkg-form" onsubmit={startPackageBuild}>
          <div class="controls">
            <label class="choice">
              <input type="radio" bind:group={pkgFrom} value="containerfile" />
              {t('Containerfile')}
            </label>
            <label class="choice">
              <input type="radio" bind:group={pkgFrom} value="git" />
              {t('Git repository')}
            </label>
            {#if pkgFrom === 'containerfile'}
              <span class="muted">{t('Starter:')}</span>
              <button type="button" class="sm" onclick={() => (pkgContainerfile = TEMPLATES.minimal)}>
                {t('FreeBSD minimal')}
              </button>
              <button type="button" class="sm" onclick={() => (pkgContainerfile = TEMPLATES.pkg)}>
                {t('FreeBSD nginx pkg')}
              </button>
              <button type="button" class="sm" onclick={() => (pkgContainerfile = TEMPLATES.daemon)}>
                {t('App daemon')}
              </button>
            {/if}
          </div>

          {#if pkgFrom === 'containerfile'}
            <label>
              {t('Containerfile')}
              <textarea
                class="editor"
                use:droppable
                rows="10"
                bind:value={pkgContainerfile}
                spellcheck="false"
              ></textarea>
            </label>
          {:else}
            <label>
              {t('Git URL')}
              <input bind:value={pkgGitUrl} placeholder="https://github.com/org/repo.git" required />
            </label>
            <div class="grid-2">
              <label>
                {t('Branch or tag')}
                <input bind:value={pkgGitRef} placeholder="main" />
              </label>
              <label>
                {t('Subdirectory')}
                <input bind:value={pkgGitPath} placeholder="." />
              </label>
            </div>
          {/if}

          <label>
            {t('Push destination (GHCR or OCI registry)')}
            <input
              bind:value={pkgImage}
              placeholder="ghcr.io/your-org/jail-service:v1"
              required
            />
          </label>

          <div class="controls">
            <label style="flex: 1">
              {t('Registry credentials Secret')}
              <select bind:value={pkgSecret}>
                <option value="">{t('none')}</option>
                {#each secrets as s (s)}<option value={s}>{s}</option>{/each}
              </select>
            </label>
            <button type="button" class="sm" onclick={() => secretDialog?.showModal()}>
              {t('New GHCR Secret…')}
            </button>
          </div>

          <label>
            {t('Auto-rollout Service after push')} <span class="muted">{t('(optional)')}</span>
            <select bind:value={pkgRolloutService}>
              <option value="">{t('Do not roll out a Service (package & push only)')}</option>
              {#each services as s (s.metadata.name)}
                <option value={s.metadata.name}>{s.metadata.name}</option>
              {/each}
            </select>
          </label>

          <div class="controls">
            <button
              class="primary"
              disabled={!!writeWhy || busy || !pkgImage.trim()}
              title={writeWhy ?? ''}
            >
              {t('Build & Push Image')}
            </button>
            {#if writeWhy}<span class="muted">{writeWhy}</span>{/if}
          </div>
        </form>
      </section>

      <section class="card">
        <h2>{t('Recent Build runs in {namespace}', { namespace: workNs })}</h2>
        {#if builds.length}
          <table>
            <caption class="sr-only">{t('Recent Build runs')}</caption>
            <thead>
              <tr>
                <th scope="col">{t('Build')}</th>
                <th scope="col">{t('Target image')}</th>
                <th scope="col">{t('Phase')}</th>
                <th scope="col">{t('Workbench')}</th>
              </tr>
            </thead>
            <tbody>
              {#each builds.slice(0, 15) as b (b.metadata.uid)}
                <tr>
                  <td>
                    <a href="/k/Build/{workNs}/{b.metadata.name}?tab=buildlog">{b.metadata.name}</a>
                  </td>
                  <td class="mono">{b.spec?.image ?? '—'}</td>
                  <td>
                    <span class="state {tone(b.status?.phase?.toLowerCase?.() ?? '')}">
                      {b.status?.phase ?? 'Pending'}
                    </span>
                  </td>
                  <td>
                    <button
                      type="button"
                      class="sm"
                      data-look
                      onclick={() =>
                        openWorkbench({
                          id: `build:${workNs}/${b.metadata.name}`,
                          kind: 'build',
                          title: `${b.metadata.name} · ${t('Build')}`,
                          namespace: workNs,
                          name: b.metadata.name,
                          object: b
                        })}
                    >
                      {t('Dock log')}
                    </button>
                  </td>
                </tr>
              {/each}
            </tbody>
          </table>
        {:else}
          <p class="muted">
            {t(
              'No Builds in this namespace yet. Label at least one FreeBSD node with legion.builder=true to execute jailer image builds.'
            )}
          </p>
        {/if}
      </section>
    </div>
  {/if}
</div>

<dialog
  bind:this={detailsDialog}
  closedby="any"
  aria-labelledby="image-details-title"
  onclick={(e) => lightDismiss(e, detailsDialog)}
>
  <header class="bar">
    <h2 id="image-details-title">{t('Image details')}</h2>
    <button type="button" class="sm" onclick={() => detailsDialog?.close()}>{t('Close')}</button>
  </header>
  <p class="muted mono">{detailsOf}</p>
  {#if detailsError}
    <p class="error" role="alert">{detailsError}</p>
  {:else if details}
    <ImageDetails d={details} />
  {:else}
    <p class="muted">{t('Reading the node’s copy…')}</p>
  {/if}
</dialog>

<dialog
  bind:this={scanDialog}
  closedby="any"
  aria-labelledby="image-scan-title"
  onclick={(e) => lightDismiss(e, scanDialog)}
>
  <header class="bar">
    <h2 id="image-scan-title">{t('Vulnerabilities')}</h2>
    <button type="button" class="sm" onclick={() => scanDialog?.close()}>{t('Close')}</button>
  </header>
  <p class="muted mono">{detailsOf}</p>
  {#if scanError}
    <p class="error" role="alert">{scanError}</p>
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
      {t('{n} packages checked against FreeBSD’s VuXML', { n: scan.packages.length })}
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

<dialog
  bind:this={pushDialog}
  closedby="any"
  aria-labelledby="push-ghcr-title"
  onclick={(e) => lightDismiss(e, pushDialog)}
>
  <header class="bar">
    <h2 id="push-ghcr-title">{t('Push cached image to GHCR / Registry')}</h2>
    <button type="button" class="sm" onclick={() => pushDialog?.close()}>{t('Close')}</button>
  </header>
  <form class="pkg-form" onsubmit={submitPushCached}>
    <p class="muted mono">{pushSourceNode} · {pushSourceRef}</p>
    <label>
      {t('Destination reference')}
      <input bind:value={pushTargetRef} placeholder="ghcr.io/owner/repo:tag" required />
    </label>
    <div class="controls">
      <label style="flex: 1">
        {t('Registry Secret in {namespace}', { namespace: workNs })}
        <select bind:value={pushSecretName}>
          <option value="">{t('none')}</option>
          {#each secrets as s (s)}<option value={s}>{s}</option>{/each}
        </select>
      </label>
      <button type="button" class="sm" onclick={() => secretDialog?.showModal()}>
        {t('New GHCR Secret…')}
      </button>
    </div>
    <div class="controls">
      <button class="primary" disabled={busy || !pushTargetRef.trim()}>{t('Push')}</button>
      <button type="button" onclick={() => pushDialog?.close()}>{t('Cancel')}</button>
    </div>
  </form>
</dialog>

<dialog
  bind:this={secretDialog}
  closedby="any"
  aria-labelledby="ghcr-sec-title"
  onclick={(e) => lightDismiss(e, secretDialog)}
>
  <header class="bar">
    <h2 id="ghcr-sec-title">{t('Save GHCR / Registry Credentials Secret')}</h2>
    <button type="button" class="sm" onclick={() => secretDialog?.close()}>{t('Close')}</button>
  </header>
  <form class="pkg-form" onsubmit={createRegistrySecret}>
    <p class="muted">
      {t(
        'Creates a Docker config Secret in {namespace} for pushing or pulling private images (for GitHub Container Registry, use ghcr.io and a Personal Access Token with write:packages).',
        { namespace: workNs }
      )}
    </p>
    <label>
      {t('Secret name')}
      <input bind:value={secName} placeholder="ghcr-cred" required />
    </label>
    <label>
      {t('Registry server')}
      <input bind:value={secServer} placeholder="ghcr.io" required />
    </label>
    <label>
      {t('Username')}
      <input bind:value={secUser} autocomplete="username" placeholder="github-handle" required />
    </label>
    <label>
      {t('Token or password')}
      <input
        type="password"
        bind:value={secToken}
        autocomplete="current-password"
        placeholder="ghp_… or github_pat_…"
        required
      />
    </label>
    <div class="controls">
      <button class="primary" disabled={!!writeWhy || busy || !secName.trim() || !secUser.trim() || !secToken}>
        {t('Save Secret')}
      </button>
      <button type="button" onclick={() => secretDialog?.close()}>{t('Cancel')}</button>
    </div>
  </form>
</dialog>

<style>
  .studio-top {
    margin-bottom: 1rem;
  }
  .hit-list {
    list-style: none;
    padding: 0;
    margin: 0.75rem 0 0;
    display: flex;
    flex-direction: column;
    gap: 0.55rem;
    max-height: 62vh;
    overflow-y: auto;
  }
  .hit-card {
    border: 1px solid var(--line);
    border-radius: 6px;
    padding: 0.65rem 0.75rem;
    background: var(--bg);
  }
  .hit-head {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 0.5rem;
    flex-wrap: wrap;
  }
  .hit-desc {
    margin: 0.25rem 0 0.45rem;
    font-size: 0.82rem;
  }
  .tag-cloud {
    display: flex;
    flex-wrap: wrap;
    gap: 0.3rem;
    max-height: 14rem;
    overflow-y: auto;
  }
  .pkg-form {
    display: flex;
    flex-direction: column;
    gap: 0.65rem;
  }
  .pkg-form label {
    display: flex;
    flex-direction: column;
    gap: 0.2rem;
  }
  .pkg-form label.choice {
    flex-direction: row;
    align-items: center;
    gap: 0.35rem;
  }
</style>

<script lang="ts">
  // The Images page's search tab: the FreeBSD catalog, Docker Hub and
  // ghcr.io searched, any image's manifest inspected (platforms, tags,
  // cosign signature, whether Legion can run it), and an offer to build
  // from FreeBSD's packages when no FreeBSD image is found.
  import { untrack } from 'svelte';
  import { goto } from '$app/navigation';
  import { api } from '../api.ts';
  import { t } from '../i18n.svelte.ts';
  import ImageDetails from '../ImageDetails.svelte';
  import { session } from '../session.svelte.ts';
  import { pkgName, type Studio } from './studio.svelte.ts';

  let { studio, onbuild }: { studio: Studio; onbuild: (name: string) => void } = $props();

  const s = $derived(studio.search);
  let searching = $state(false);
  let inspecting = $state(false);

  const filteredHits = $derived(
    s.hits.filter((h) => {
      const isGhcr = h.source === 'ghcr.io' || h.name.startsWith('ghcr.io/');
      if (s.source === 'freebsd' && h.source !== 'freebsd-catalog') return false;
      if (s.source === 'ghcr' && !isGhcr) return false;
      if (s.source === 'docker' && h.source !== 'docker-hub') return false;
      // Whether a hit is a VM image is in its config's labels
      // (legion.vm.image, as `legion build vm` sets it), which a search
      // does not fetch: the inspector shows it.
      return true;
    })
  );
  // Whether the search found a FreeBSD image: if not, the offer to build
  // one from FreeBSD's packages, which have far more software than any
  // registry has FreeBSD images of.
  const freebsdFound = $derived(s.hits.some((h) => h.os === 'freebsd'));

  async function search(e?: Event) {
    e?.preventDefault();
    searching = true;
    studio.error = '';
    try {
      const q = s.query.trim();
      const r = await api.registrySearch(q, 24, studio.namespace, s.secret);
      s.hits = r.results;
      s.searched = q;
    } catch (err) {
      studio.error = (err as Error).message;
    } finally {
      searching = false;
    }
  }

  // The first visit searches with nothing typed: what is popular. Once
  // each time the tab opens, not again on every change: a search that
  // found nothing, or failed, used to be sent again at once, endlessly.
  $effect(() => {
    untrack(() => {
      if (!s.hits.length && !searching) search();
    });
  });

  async function inspect(ref?: string) {
    const target = (ref ?? s.inspectInput).trim();
    if (!target) return;
    s.inspectInput = target;
    inspecting = true;
    studio.error = '';
    try {
      s.inspected = await api.registryInspect(target, studio.namespace, s.inspectSecret);
    } catch (err) {
      s.inspected = null;
      studio.error = (err as Error).message;
    } finally {
      inspecting = false;
    }
  }

  async function prePull(image: string) {
    await studio.act(async () => {
      await api.nodeImageOp({ op: 'pull', image });
      return t('Pulled {image} onto ready nodes', { image });
    });
  }

  const deploy = (image: string) =>
    goto(`/deploy?namespace=${encodeURIComponent(studio.namespace)}&image=${encodeURIComponent(image)}`);
</script>

<div class="split-pane">
  <section class="card">
    <h2>{t('Search FreeBSD catalog, Docker Hub & ghcr.io')}</h2>
    <form class="controls" onsubmit={search}>
      <input
        type="search"
        bind:value={s.query}
        placeholder={t('freebsd, postgres, or ghcr.io/owner/repo…')}
        aria-label={t('Search images')}
      />
      <select bind:value={s.secret} aria-label={t('GHCR login Secret')}>
        <option value="">{t('No GHCR Secret')}</option>
        {#each studio.secrets as x (x)}
          <option value={x}>{x}</option>
        {/each}
      </select>
      <button class="primary" disabled={searching}>{t('Search')}</button>
    </form>

    <div class="pill-row" role="group" aria-label={t('Filter by source')}>
      {#each [{ id: 'all', label: t('All sources') }, { id: 'freebsd', label: t('FreeBSD catalog') }, { id: 'ghcr', label: 'ghcr.io' }, { id: 'docker', label: 'Docker Hub' }] as const as f (f.id)}
        <button
          type="button"
          class="pill"
          class:active={s.source === f.id}
          aria-pressed={s.source === f.id}
          onclick={() => (s.source = f.id)}>{f.label}</button
        >
      {/each}
    </div>

    {#if s.searched && !searching && !freebsdFound && pkgName(s.searched) && !s.searched.startsWith('ghcr.io/')}
      {@const name = pkgName(s.searched)}
      <div class="build-offer" role="status">
        <p>
          {s.hits.length
            ? t('No FreeBSD image of “{query}” was found: those listed are for Linux, or their OS is not known.', {
                query: s.searched
              })
            : t('No FreeBSD image of “{query}” was found.', { query: s.searched })}
          {t('FreeBSD’s packages have far more software than registries have FreeBSD images of: build an image that installs one.')}
        </p>
        <div class="controls">
          <button type="button" class="sm primary" data-look onclick={() => onbuild(name)}
            >{t('Build from FreeBSD packages')}</button
          >
          <a
            class="button sm"
            href="https://www.freshports.org/search.php?query={encodeURIComponent(name)}&search=go&stype=name&method=match"
            target="_blank"
            rel="noopener noreferrer">{t('Find the package’s name on FreshPorts')}</a
          >
        </div>
      </div>
    {/if}
    {#if filteredHits.length}
      <ul class="hit-list" role="list">
        {#each filteredHits as h (h.name)}
          {@const fullRef = h.suggested_tag ? `${h.name}:${h.suggested_tag}` : h.name}
          <li class="hit-card">
            <div class="hit-head">
              <strong class="mono">{h.name}</strong>
              <span>
                <span class="tag"
                  >{h.source === 'freebsd-catalog' ? t('catalog') : h.source === 'ghcr.io' ? 'ghcr.io' : 'Docker Hub'}</span
                >
                {#if h.os === 'freebsd'}
                  <span class="state good">{t('FreeBSD jail')}</span>
                {:else if h.os === 'linux'}
                  <span
                    class="state wait"
                    title={t('Runs in a jail through the Linux ABI, on nodes started with --linux; not every Linux image works (docs/LINUX.md)')}
                    >{t('Linux: --linux nodes')}</span
                  >
                {:else if h.source === 'docker-hub'}
                  <span
                    class="state wait"
                    title={t('Most Docker Hub images are built for Linux: Inspect OCI shows which OS this one is for')}
                    >{t('likely Linux')}</span
                  >
                {:else}
                  <span class="state" title={t('Inspect OCI shows which OS it is for')}>{t('OS not known')}</span>
                {/if}
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
                  if (s.secret && !s.inspectSecret) s.inspectSecret = s.secret;
                  inspect(fullRef);
                }}
              >
                {t('Inspect OCI')}
              </button>
              {#if session.me?.all}
                <button type="button" class="sm" disabled={studio.busy} onclick={() => prePull(fullRef)}>
                  {t('Pre-pull')}
                </button>
              {/if}
              <button type="button" class="sm primary" onclick={() => deploy(fullRef)}>
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
        inspect();
      }}
    >
      <input
        bind:value={s.inspectInput}
        placeholder="ghcr.io/freebsd/freebsd-runtime:15.1"
        aria-label={t('Image to inspect')}
        required
      />
      <select bind:value={s.inspectSecret} aria-label={t('Registry login Secret')}>
        <option value="">{t('Public')}</option>
        {#each studio.secrets as x (x)}<option value={x}>{x}</option>{/each}
      </select>
      <button class="primary" disabled={inspecting || !s.inspectInput.trim()}>
        {t('Inspect')}
      </button>
    </form>

    {#if inspecting}
      <p class="muted">{t('Querying registry manifest & tags…')}</p>
    {:else if s.inspected}
      {@const inspected = s.inspected}
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
          {#if inspected.run?.labels?.['legion.vm.image'] === 'true'}
            {t('bhyve VM')}
          {:else if inspected.os === 'freebsd'}
            {t('FreeBSD Jail')}
          {:else if inspected.os === 'linux'}
            <span class="state wait">{t('Linux image')}</span>
            {t('runs in a jail through the Linux ABI, only on nodes started with --linux, and not every Linux image works there')}
          {:else}
            <span class="state bad">{t('Not runnable')}</span>
            {t('built for {os}: Legion runs FreeBSD images, and Linux ones on --linux nodes', {
              os: inspected.os || t('an OS it does not say')
            })}
          {/if}
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
        <button type="button" class="primary sm" onclick={() => deploy(inspected.image)}>
          {t('Deploy this image')}
        </button>
      </div>

      {#if inspected.tags.length}
        <h3>{t('Available tags ({n})', { n: inspected.tags.length })}</h3>
        <div class="tag-cloud">
          {#each inspected.tags.slice(0, 60) as tg (tg)}
            {@const repo = inspected.image.split('@')[0].replace(/:[^:/]*$/, '')}
            <button type="button" class="sm mono" onclick={() => inspect(`${repo}:${tg}`)}>
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

<style>
  .build-offer {
    border: 1px dashed var(--control);
    border-radius: 8px;
    padding: 0.6rem 0.8rem;
    margin: 0.6rem 0;
  }
  .build-offer p {
    margin: 0 0 0.5rem;
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
</style>

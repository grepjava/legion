<script lang="ts">
  // A new image for a Service (CONSOLE.md phase 5): a Build from a Git
  // repository or a Containerfile, built with jailer on a builder node,
  // pushed where given, then the Service rolled out to it. Opens the
  // Build's log once made.
  import { goto } from '$app/navigation';
  import { page } from '$app/state';
  import { api, type LObject } from '#lib/api.ts';
  import { t } from '#lib/i18n.svelte.ts';
  import { cannotWrite } from '#lib/session.svelte.ts';

  const namespace = $derived(page.url.searchParams.get('namespace') ?? '');
  const serviceName = $derived(page.url.searchParams.get('service') ?? '');
  let service = $state<LObject | null>(null);
  let secrets: string[] = $state([]);
  let from: 'git' | 'containerfile' = $state('git');
  let url = $state('');
  let ref = $state('');
  let path = $state('');
  let containerfile = $state('FROM docker.io/freebsd/freebsd-runtime:15.1\n');
  let image = $state('');
  let pushSecret = $state('');
  let container = $state('');
  let error = $state('');
  let busy = $state(false);
  const why = $derived(cannotWrite(namespace));
  const containers: string[] = $derived(
    (service?.spec?.pod?.containers ?? []).map((c: { name: string }) => c.name)
  );

  $effect(() => {
    if (!namespace || !serviceName) return;
    api
      .get('Service', namespace, serviceName)
      .then((s) => {
        service = s;
        const names = (s.spec?.pod?.containers ?? []).map((c: { name: string }) => c.name);
        if (names.length > 1 && !container) container = names[0];
        // Its image's repository, for a new tag to be given.
        const now: string = s.spec?.pod?.containers?.[0]?.image ?? '';
        const repo = now.split('@')[0].replace(/:[^:/]*$/, '');
        if (!image && repo.includes('/')) image = `${repo}:`;
      })
      .catch((e) => (error = e.message));
    api
      .list('Secret', namespace)
      .then((l) => (secrets = l.objects.map((o) => o.metadata.name).sort()))
      .catch(() => (secrets = []));
  });

  async function build(e: Event) {
    e.preventDefault();
    error = '';
    const name = `${serviceName}-${Date.now().toString(36)}`.slice(0, 63);
    const source =
      from === 'git'
        ? {
            git: {
              url: url.trim(),
              ...(ref.trim() ? { ref: ref.trim() } : {}),
              ...(path.trim() ? { path: path.trim() } : {})
            }
          }
        : { containerfile };
    const object = {
      apiVersion: 'legion/v1',
      kind: 'Build',
      metadata: { name, namespace },
      spec: {
        source,
        image: image.trim(),
        ...(pushSecret ? { pushSecret } : {}),
        rollout: { service: serviceName, ...(container ? { container } : {}) }
      }
    };
    busy = true;
    try {
      // JSON is YAML.
      await api.apply({ yaml: JSON.stringify(object) });
      goto(`/k/Build/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}?tab=buildlog`);
    } catch (err) {
      error = (err as Error).message;
    } finally {
      busy = false;
    }
  }
</script>

<header class="bar">
  <h1>{t('Build a new image for {service}', { service: serviceName })}</h1>
</header>
<p class="muted">
  {t(
    'Built with jailer on a builder node, pushed where you say, then the service rolled out to it. Its log opens once it starts.'
  )}
</p>

<form class="build" onsubmit={build}>
  <fieldset>
    <legend>{t('Source')}</legend>
    <label class="choice"
      ><input type="radio" bind:group={from} value="git" /> {t('A Git repository')}</label
    >
    <label class="choice"
      ><input type="radio" bind:group={from} value="containerfile" /> {t('A Containerfile')}</label
    >
    {#if from === 'git'}
      <label
        >{t('Repository')}
        <input bind:value={url} aria-label={t('Repository')} placeholder="https://git.example/team/app" required />
      </label>
      <label
        >{t('Branch or tag')} <span class="muted">{t('(empty: its default branch)')}</span>
        <input bind:value={ref} aria-label={t('Branch or tag')} />
      </label>
      <label
        >{t('Directory')} <span class="muted">{t('(empty: the top, its Containerfile there)')}</span>
        <input bind:value={path} aria-label={t('Directory')} />
      </label>
    {:else}
      <label
        >{t('Containerfile')} <span class="muted">{t('(built with nothing beside it)')}</span>
        <textarea class="editor" rows="10" bind:value={containerfile} aria-label={t('Containerfile')}></textarea>
      </label>
    {/if}
  </fieldset>
  <label
    >{t('Push to')} <span class="muted">{t('(registry/repository:tag)')}</span>
    <input bind:value={image} aria-label={t('Push to')} placeholder="registry.example/team/app:2" required />
  </label>
  <label
    >{t("The registry's login")} <span class="muted">{t('(a Secret holding a Docker config)')}</span>
    <select bind:value={pushSecret} aria-label={t("The registry's login")}>
      <option value="">{t('none')}</option>
      {#each secrets as s (s)}<option value={s}>{s}</option>{/each}
    </select>
  </label>
  {#if containers.length > 1}
    <label
      >{t('Container')}
      <select bind:value={container} aria-label={t('Container')} required>
        {#each containers as c (c)}<option value={c}>{c}</option>{/each}
      </select>
    </label>
  {/if}
  <div class="controls">
    <button class="primary" disabled={!!why || busy || !service} title={why ?? ''}>{t('Build')}</button>
    {#if why}<span class="muted">{why}.</span>{/if}
  </div>
  {#if error}<p class="error" role="alert">{error}</p>{/if}
</form>

<style>
  .build {
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
    max-width: 44rem;
  }
  .build label {
    display: flex;
    flex-direction: column;
    gap: 0.2rem;
  }
  .build label.choice {
    flex-direction: row;
    align-items: center;
    gap: 0.4rem;
  }
  fieldset {
    border: 1px solid var(--line);
    border-radius: 6px;
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }
</style>

<script lang="ts">
  // Guided Builder Wizard for FreeBSD Jails & bhyve VMs:
  // Step 1: Target (FreeBSD Jail | bhyve VM)
  // Step 2: Source (Git source | Containerfile | Deploy VM Service)
  // Step 3: Destination, Credentials & Signing (or VM hardware specs)
  // Step 4: Review & Launch
  import { goto } from '$app/navigation';
  import { page } from '$app/state';
  import { api, type LObject } from '#lib/api.ts';
  import { t } from '#lib/i18n.svelte.ts';
  import { cannotWrite, namespaces, session } from '#lib/session.svelte.ts';

  const paramNamespace = $derived(page.url.searchParams.get('namespace') ?? '');
  const paramService = $derived(page.url.searchParams.get('service') ?? '');

  let step = $state<1 | 2 | 3 | 4>(1);
  let target = $state<'jail' | 'vm'>('jail');
  let vmAction = $state<'build' | 'deploy'>('build');

  let selectedNamespace = $state('');
  let selectedService = $state('');
  let allNamespaces: string[] = $state([]);
  let services: LObject[] = $state([]);
  let secrets: string[] = $state([]);

  // Build fields
  let from: 'git' | 'containerfile' = $state('git');
  let url = $state('');
  let ref = $state('');
  let path = $state('');
  let containerfile = $state('FROM docker.io/freebsd/freebsd-runtime:15.1\n');
  let image = $state('');
  let pushSecret = $state('');
  let signingKeySecret = $state('');
  let container = $state('');

  // bhyve VM Service fields
  let vmServiceName = $state('');
  let vmImage = $state('');
  let vmCpus = $state(1);
  let vmMemory = $state('512Mi');
  let vmBoot: 'bhyveload' | 'uefi' = $state('bhyveload');

  let error = $state('');
  let busy = $state(false);

  const nsChoices = $derived(namespaces(allNamespaces));
  const namespace = $derived(paramNamespace || selectedNamespace || session.namespace || nsChoices[0] || '');
  const activeServiceName = $derived(paramService || selectedService);
  const service = $derived(services.find((s) => s.metadata.name === activeServiceName) ?? null);
  const why = $derived(namespace ? cannotWrite(namespace) : t('Choose a namespace'));
  const containers: string[] = $derived(
    (service?.spec?.pod?.containers ?? []).map((c: { name: string }) => c.name)
  );

  $effect(() => {
    if (!session.me?.all) return;
    api
      .list('Namespace')
      .then((l) => (allNamespaces = l.objects.map((o) => o.metadata.name).sort()))
      .catch(() => {});
  });

  $effect(() => {
    if (!namespace) return;
    api
      .list('Service', namespace)
      .then((l) => (services = l.objects.sort((a, b) => a.metadata.name.localeCompare(b.metadata.name))))
      .catch(() => (services = []));
    api
      .list('Secret', namespace)
      .then((l) => (secrets = l.objects.map((o) => o.metadata.name).sort()))
      .catch(() => (secrets = []));
  });

  $effect(() => {
    if (!service) return;
    const names = (service.spec?.pod?.containers ?? []).map((c: { name: string }) => c.name);
    if (names.length > 1 && !container) container = names[0];
    const now: string = service.spec?.pod?.containers?.[0]?.image ?? '';
    const repo = now.split('@')[0].replace(/:[^:/]*$/, '');
    if (!image && repo.includes('/')) image = `${repo}:`;
  });

  function selectTarget(next: 'jail' | 'vm') {
    target = next;
    error = '';
    if (next === 'vm' && containerfile === 'FROM docker.io/freebsd/freebsd-runtime:15.1\n') {
      containerfile = 'FROM scratch\nCOPY disk.img /disk.img\n';
    } else if (next === 'jail' && containerfile === 'FROM scratch\nCOPY disk.img /disk.img\n') {
      containerfile = 'FROM docker.io/freebsd/freebsd-runtime:15.1\n';
    }
  }

  function applyTemplate(kind: 'runtime' | 'pkg' | 'rust' | 'vm') {
    from = 'containerfile';
    if (kind === 'runtime') {
      containerfile = 'FROM docker.io/freebsd/freebsd-runtime:15.1\nCOPY app /usr/local/bin/app\nCMD ["/usr/local/bin/app"]\n';
    } else if (kind === 'pkg') {
      containerfile = 'FROM docker.io/freebsd/freebsd-notoolchain:15.1\nRUN env ASSUME_ALWAYS_YES=yes pkg install -y nginx && pkg clean -ay\nCMD ["/usr/local/sbin/nginx", "-g", "daemon off;"]\n';
    } else if (kind === 'rust') {
      containerfile = 'FROM docker.io/freebsd/freebsd-toolchain:15.1 AS build\nWORKDIR /src\nCOPY . .\nRUN cargo build --release\n\nFROM docker.io/freebsd/freebsd-runtime:15.1\nCOPY --from=build /src/target/release/app /usr/local/bin/app\nCMD ["/usr/local/bin/app"]\n';
    } else {
      containerfile = 'FROM scratch\nCOPY disk.img /disk.img\n';
    }
  }

  const step2Valid = $derived(
    target === 'vm' && vmAction === 'deploy'
      ? Boolean(vmServiceName.trim() && vmImage.trim())
      : from === 'git'
        ? Boolean(url.trim())
        : Boolean(containerfile.trim())
  );

  const step3Valid = $derived(
    target === 'vm' && vmAction === 'deploy'
      ? Boolean(namespace && vmMemory.trim())
      : Boolean(namespace && image.trim())
  );

  async function submitBuild() {
    error = '';
    const baseName = activeServiceName || (target === 'vm' ? 'vm-build' : 'build');
    const name = `${baseName}-${Date.now().toString(36)}`.slice(0, 63);
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
        ...(signingKeySecret ? { signingKeySecret } : {}),
        ...(activeServiceName
          ? { rollout: { service: activeServiceName, ...(container ? { container } : {}) } }
          : {})
      }
    };
    busy = true;
    try {
      await api.apply({ yaml: JSON.stringify(object) });
      goto(`/k/Build/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}?tab=buildlog`);
    } catch (err) {
      error = (err as Error).message;
    } finally {
      busy = false;
    }
  }

  async function deployVm() {
    error = '';
    const name = vmServiceName.trim();
    const object = {
      apiVersion: 'legion/v1',
      kind: 'Service',
      metadata: { name, namespace },
      spec: {
        replicas: 1,
        pod: {
          containers: [
            {
              name: 'vm',
              image: vmImage.trim(),
              vm: {
                cpus: Number(vmCpus) || 1,
                memory: vmMemory.trim(),
                boot: vmBoot
              }
            }
          ]
        }
      }
    };
    busy = true;
    try {
      await api.apply({ yaml: JSON.stringify(object) });
      goto(`/k/Service/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}`);
    } catch (err) {
      error = (err as Error).message;
    } finally {
      busy = false;
    }
  }
</script>

<header class="bar">
  <h1>
    {paramService
      ? t('Build a new image for {service}', { service: paramService })
      : t('Builder Wizard')}
  </h1>
</header>

<nav class="wizard-steps" aria-label={t('Wizard steps')}>
  <button type="button" class="wizard-step" class:active={step === 1} class:done={step > 1} onclick={() => (step = 1)}>
    <span class="wizard-num">1</span>
    <span>{t('Workload type')}</span>
  </button>
  <button type="button" class="wizard-step" class:active={step === 2} class:done={step > 2} onclick={() => (step = 2)}>
    <span class="wizard-num">2</span>
    <span>{t('Source')}</span>
  </button>
  <button
    type="button"
    class="wizard-step"
    class:active={step === 3}
    class:done={step > 3}
    disabled={!step2Valid}
    onclick={() => (step = 3)}
  >
    <span class="wizard-num">3</span>
    <span>{target === 'vm' && vmAction === 'deploy' ? t('VM resources') : t('Destination & signing')}</span>
  </button>
  <button
    type="button"
    class="wizard-step"
    class:active={step === 4}
    disabled={!step2Valid || !step3Valid}
    onclick={() => (step = 4)}
  >
    <span class="wizard-num">4</span>
    <span>{t('Review & launch')}</span>
  </button>
</nav>

<section class="card wizard-body">
  {#if step === 1}
    <h2>{t('1. Choose what to build or launch')}</h2>
    <div class="choice-grid">
      <button
        type="button"
        class="choice-card"
        class:selected={target === 'jail'}
        onclick={() => selectTarget('jail')}
      >
        <strong>{t('FreeBSD Jail Image')}</strong>
        <span class="muted">
          {t('Build an OCI jail image from Git source or a Containerfile on a builder node, push to a registry, and optionally roll out a Service.')}
        </span>
      </button>
      <button
        type="button"
        class="choice-card"
        class:selected={target === 'vm'}
        onclick={() => selectTarget('vm')}
      >
        <strong>{t('bhyve Virtual Machine')}</strong>
        <span class="muted">
          {t('Build a VM OCI image (/disk.img) from Git source or Containerfile, package local disks with `legion build vm`, or deploy a bhyve VM Service.')}
        </span>
      </button>
    </div>
    <div class="wizard-actions">
      <button type="button" class="primary" onclick={() => (step = 2)}>{t('Next: Source')}</button>
    </div>
  {:else if step === 2}
    <h2>
      {target === 'jail'
        ? t('2. Choose FreeBSD Jail source')
        : t('2. Choose bhyve VM source or image')}
    </h2>

    {#if target === 'vm'}
      <div class="choice-grid" style="margin-bottom: 0.9rem">
        <button
          type="button"
          class="choice-card"
          class:selected={vmAction === 'build'}
          onclick={() => (vmAction = 'build')}
        >
          <strong>{t('Build VM OCI image')}</strong>
          <span class="muted">{t('Build from Git source or a Containerfile producing /disk.img.')}</span>
        </button>
        <button
          type="button"
          class="choice-card"
          class:selected={vmAction === 'deploy'}
          onclick={() => (vmAction = 'deploy')}
        >
          <strong>{t('Deploy existing VM image')}</strong>
          <span class="muted">{t('Launch a bhyve VM Service from an existing /disk.img OCI image.')}</span>
        </button>
      </div>
    {/if}

    {#if target === 'vm' && vmAction === 'deploy'}
      <div class="build">
        <label>
          {t('Service name')}
          <input bind:value={vmServiceName} placeholder="freebsd-vm" required />
        </label>
        <label>
          {t('VM OCI image')} <span class="muted">{t('(containing /disk.img)')}</span>
          <input bind:value={vmImage} placeholder="ghcr.io/owner/myvm:1" required />
        </label>
        <p class="muted">
          {t('To package a local raw disk or rootfs directory into an OCI image from the CLI:')}
        </p>
        <pre>legion build vm -t localhost/myvm:1 --rootfs ./guest-root --size 512m</pre>
      </div>
    {:else}
      <div class="build">
        <div class="pill-row" role="group" aria-label={t('Source type')}>
          <button type="button" class="pill" class:active={from === 'git'} onclick={() => (from = 'git')}>
            {t('Build from source (Git)')}
          </button>
          <button
            type="button"
            class="pill"
            class:active={from === 'containerfile'}
            onclick={() => (from = 'containerfile')}
          >
            {t('Containerfile')}
          </button>
        </div>

        {#if from === 'git'}
          <label>
            {t('Git repository URL')}
            <input bind:value={url} placeholder="https://github.com/org/app" required />
          </label>
          <label>
            {t('Branch, tag, or commit')} <span class="muted">{t('(empty: default branch)')}</span>
            <input bind:value={ref} placeholder="main" />
          </label>
          <label>
            {t('Subdirectory')} <span class="muted">{t('(empty: repository root)')}</span>
            <input bind:value={path} />
          </label>
        {:else}
          <div class="pill-row">
            <span class="muted">{t('Templates:')}</span>
            {#if target === 'jail'}
              <button type="button" class="sm" onclick={() => applyTemplate('runtime')}>{t('Minimal runtime')}</button>
              <button type="button" class="sm" onclick={() => applyTemplate('pkg')}>{t('FreeBSD pkg')}</button>
              <button type="button" class="sm" onclick={() => applyTemplate('rust')}>{t('Rust multi-stage')}</button>
            {:else}
              <button type="button" class="sm" onclick={() => applyTemplate('vm')}>{t('VM disk.img')}</button>
            {/if}
          </div>
          <label>
            {t('Containerfile')}
            <textarea class="editor" rows="10" bind:value={containerfile}></textarea>
          </label>
        {/if}
      </div>
    {/if}

    <div class="wizard-actions">
      <button type="button" onclick={() => (step = 1)}>{t('Back')}</button>
      <button type="button" class="primary" disabled={!step2Valid} onclick={() => (step = 3)}>
        {t('Next')}
      </button>
    </div>
  {:else if step === 3}
    <h2>
      {target === 'vm' && vmAction === 'deploy'
        ? t('3. Namespace & VM resources')
        : t('3. Registry destination, signing & rollout')}
    </h2>
    <div class="build">
      {#if !paramNamespace}
        <label>
          {t('Namespace')}
          <select value={namespace} onchange={(e) => (selectedNamespace = e.currentTarget.value)} required>
            {#each nsChoices as ns (ns)}<option value={ns}>{ns}</option>{/each}
          </select>
        </label>
      {/if}

      {#if target === 'vm' && vmAction === 'deploy'}
        <div class="row">
          <label>
            {t('vCPUs')}
            <input type="number" min="1" max="64" bind:value={vmCpus} required />
          </label>
          <label>
            {t('Memory')}
            <input bind:value={vmMemory} placeholder="512Mi" required />
          </label>
          <label>
            {t('Boot loader')}
            <select bind:value={vmBoot}>
              <option value="bhyveload">bhyveload</option>
              <option value="uefi">uefi</option>
            </select>
          </label>
        </div>
      {:else}
        <label>
          {t('Push to')} <span class="muted">{t('(registry/repository:tag)')}</span>
          <input bind:value={image} placeholder="ghcr.io/owner/app:1" required />
        </label>
        <label>
          {t("The registry's login")} <span class="muted">{t('(Secret with Docker config / GHCR PAT)')}</span>
          <select bind:value={pushSecret}>
            <option value="">{t('none')}</option>
            {#each secrets as s (s)}<option value={s}>{s}</option>{/each}
          </select>
        </label>
        <label>
          {t('Signing key')}
          <span class="muted">{t('(Secret with P-256 key for cosign signature, SLSA provenance & SBOM)')}</span>
          <select bind:value={signingKeySecret}>
            <option value="">{t('none')}</option>
            {#each secrets as s (s)}<option value={s}>{s}</option>{/each}
          </select>
        </label>
        {#if !paramService}
          <label>
            {t('Roll out to Service')} <span class="muted">{t('(optional)')}</span>
            <select bind:value={selectedService}>
              <option value="">{t('none (build and push only)')}</option>
              {#each services as s (s.metadata.name)}
                <option value={s.metadata.name}>{s.metadata.name}</option>
              {/each}
            </select>
          </label>
        {/if}
        {#if containers.length > 1}
          <label>
            {t('Container')}
            <select bind:value={container} required>
              {#each containers as c (c)}<option value={c}>{c}</option>{/each}
            </select>
          </label>
        {/if}
      {/if}
    </div>

    <div class="wizard-actions">
      <button type="button" onclick={() => (step = 2)}>{t('Back')}</button>
      <button type="button" class="primary" disabled={!step3Valid} onclick={() => (step = 4)}>
        {t('Next: Review')}
      </button>
    </div>
  {:else}
    <h2>{t('4. Review & launch')}</h2>
    <dl class="kv">
      <dt>{t('Workload')}</dt>
      <dd>{target === 'jail' ? t('FreeBSD Jail') : t('bhyve VM')}</dd>
      <dt>{t('Namespace')}</dt>
      <dd class="mono">{namespace}</dd>
      {#if target === 'vm' && vmAction === 'deploy'}
        <dt>{t('Service')}</dt>
        <dd class="mono">{vmServiceName}</dd>
        <dt>{t('Image')}</dt>
        <dd class="mono">{vmImage}</dd>
        <dt>{t('Resources')}</dt>
        <dd class="mono">{vmCpus} vCPU · {vmMemory} · {vmBoot}</dd>
      {:else}
        <dt>{t('Source')}</dt>
        <dd class="mono">{from === 'git' ? `${url}${ref ? `@${ref}` : ''}` : t('Inline Containerfile')}</dd>
        <dt>{t('Push to')}</dt>
        <dd class="mono">{image}</dd>
        {#if activeServiceName}
          <dt>{t('Rollout Service')}</dt>
          <dd class="mono">{activeServiceName}</dd>
        {/if}
      {/if}
    </dl>

    <div class="wizard-actions">
      <button type="button" onclick={() => (step = 3)}>{t('Back')}</button>
      {#if target === 'vm' && vmAction === 'deploy'}
        <button type="button" class="primary" disabled={!!why || busy} onclick={deployVm}>
          {busy ? t('Deploying…') : t('Deploy VM')}
        </button>
      {:else}
        <button type="button" class="primary" disabled={!!why || busy} onclick={submitBuild}>
          {busy ? t('Starting build…') : t('Start Build')}
        </button>
      {/if}
      {#if why}<span class="muted">{why}.</span>{/if}
    </div>
    {#if error}<p class="error" role="alert">{error}</p>{/if}
  {/if}
</section>

<style>
  .wizard-body {
    max-width: 48rem;
  }
  .build {
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
  }
  .build label {
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
  }
  .row {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 0.75rem;
  }
  .wizard-actions {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    margin-top: 1.1rem;
    padding-top: 0.85rem;
    border-top: 1px solid var(--line);
  }
</style>

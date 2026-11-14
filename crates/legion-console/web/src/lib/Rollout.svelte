<script lang="ts">
  // A Service's tasks against what it asks for, its latest update as the
  // controller reports it, and a new image for a container: applied over
  // the version shown, so a change meanwhile is never overwritten.
  import { api, ApiError, type LObject } from './api.ts';
  import { tone } from './columns.ts';
  import { cannotWrite } from './session.svelte.ts';
  import { t } from './i18n.svelte.ts';

  let { service }: { service: LObject } = $props();
  const ns = $derived(service.metadata.namespace ?? '');
  const why = $derived(cannotWrite(ns));
  const status = $derived(service.status ?? {});
  const update = $derived(status.update);
  const current = $derived(status.observedGeneration === service.metadata.generation);
  const containers: { name: string; image: string }[] = $derived(service.spec?.pod?.containers ?? []);
  let images: Record<string, string> = $state({});
  let message = $state('');
  let error = $state('');

  async function rollOut(container: string) {
    const image = images[container]?.trim();
    if (!image) return;
    error = '';
    message = '';
    const spec = structuredClone($state.snapshot(service.spec));
    const c = spec.pod.containers.find((c: { name: string }) => c.name === container);
    if (c.image === image) {
      message = t('{container} already runs {image}', { container, image });
      return;
    }
    c.image = image;
    try {
      await api.apply({
        object: {
          apiVersion: service.apiVersion,
          kind: service.kind,
          metadata: {
            name: service.metadata.name,
            namespace: ns,
            labels: service.metadata.labels ?? {}
          },
          spec
        },
        expectedVersion: service.metadata.version
      });
      images[container] = '';
      message = t('{container} rolling out {image}', { container, image });
    } catch (e) {
      error =
        e instanceof ApiError && e.status === 409
          ? t('The Service changed meanwhile; look at it again and roll out once more.')
          : (e as Error).message;
    }
  }
</script>

<div class="cards">
  <div class="card">
    <h2>{t('Tasks')}</h2>
    <p class="big">{status.ready ?? 0} / {status.desired ?? service.spec?.replicas ?? 0}</p>
    <p class="muted">{t('ready; {n} running', { n: status.running ?? 0 })}</p>
  </div>
  <div class="card">
    <h2>{t('Latest update')}</h2>
    {#if update}
      <p class="big"><span class="state {tone(update.state)}">{update.state}</span></p>
      <p class="muted"
        >{update.message
          ? t('from generation {generation}: {message}', { generation: update.generation, message: update.message })
          : t('from generation {generation}', { generation: update.generation })}</p
      >
    {:else}
      <p class="muted">{t('None yet.')}</p>
    {/if}
  </div>
  <div class="card">
    <h2>{t('Spec')}</h2>
    <p class="big">{t('generation {generation}', { generation: service.metadata.generation })}</p>
    <p class="muted">{current ? t('seen by the controller') : t('not yet taken up')}</p>
  </div>
</div>

<table>
  <thead><tr><th>{t('Container')}</th><th>{t('Image')}</th><th>{t('New image')}</th></tr></thead>
  <tbody>
    {#each containers as c (c.name)}
      <tr>
        <td>{c.name}</td>
        <td><code>{c.image}</code></td>
        <td>
          <form
            class="inline"
            onsubmit={(e) => {
              e.preventDefault();
              rollOut(c.name);
            }}
          >
            <input
              placeholder={t('registry/name:tag')}
              bind:value={images[c.name]}
              disabled={!!why}
              aria-label={t('New image for {container}', { container: c.name })}
            />
            <button disabled={!!why || !images[c.name]} title={why ?? ''}>{t('Roll out')}</button>
          </form>
        </td>
      </tr>
    {/each}
  </tbody>
</table>
{#if error}<p class="error" role="alert">{error}</p>{/if}
{#if message}<p class="ok" role="status">{message}</p>{/if}

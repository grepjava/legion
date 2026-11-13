<script lang="ts">
  // A Service's tasks against what it asks for, its latest update as the
  // controller reports it, and a new image for a container: applied over
  // the version shown, so a change meanwhile is never overwritten.
  import { api, ApiError, type LObject } from './api.ts';
  import { tone } from './columns.ts';
  import { cannotWrite } from './session.svelte.ts';

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
      message = `${container} already runs ${image}`;
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
      message = `${container} rolling out ${image}`;
    } catch (e) {
      error =
        e instanceof ApiError && e.status === 409
          ? 'The Service changed meanwhile; look at it again and roll out once more.'
          : (e as Error).message;
    }
  }
</script>

<div class="cards">
  <div class="card">
    <h2>Tasks</h2>
    <p class="big">{status.ready ?? 0} / {status.desired ?? service.spec?.replicas ?? 0}</p>
    <p class="muted">ready; {status.running ?? 0} running</p>
  </div>
  <div class="card">
    <h2>Latest update</h2>
    {#if update}
      <p class="big"><span class="state {tone(update.state)}">{update.state}</span></p>
      <p class="muted">from generation {update.generation}{update.message ? `: ${update.message}` : ''}</p>
    {:else}
      <p class="muted">None yet.</p>
    {/if}
  </div>
  <div class="card">
    <h2>Spec</h2>
    <p class="big">generation {service.metadata.generation}</p>
    <p class="muted">{current ? 'seen by the controller' : 'not yet taken up'}</p>
  </div>
</div>

<table>
  <thead><tr><th>Container</th><th>Image</th><th>New image</th></tr></thead>
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
              placeholder="registry/name:tag"
              bind:value={images[c.name]}
              disabled={!!why}
              aria-label="New image for {c.name}"
            />
            <button disabled={!!why || !images[c.name]} title={why ?? ''}>Roll out</button>
          </form>
        </td>
      </tr>
    {/each}
  </tbody>
</table>
{#if error}<p class="error">{error}</p>{/if}
{#if message}<p class="ok">{message}</p>{/if}

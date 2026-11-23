<script lang="ts">
  // A change to an object's spec made in a form, shown before it is made:
  // Review asks the API what it would store (a dry run, its diff as
  // `legion diff` prints it, checked by the same rules as `legion apply`),
  // with what the change would do said under it; Save writes it only over
  // the version the form was filled from, so nobody's change in between is
  // lost. The forms of a Volume's protection, of the Availability page's
  // fixes and of backup targets use it.
  import { api, ApiError, type LObject } from '#lib/api.ts';
  import { t } from '#lib/i18n.svelte.ts';
  import Diff from '#lib/Diff.svelte';

  let {
    object,
    spec,
    notes = [],
    save = '',
    disabled = false,
    onsaved = () => {}
  }: {
    /** The object as read: its kind, name, labels and version. */
    object: LObject;
    /** Its spec as the form would have it. */
    spec: unknown;
    /** What the change would do, said under its diff. */
    notes?: string[];
    /** The save button's words. */
    save?: string;
    /** The form is not ready (a field refused). */
    disabled?: boolean;
    onsaved?: () => void;
  } = $props();

  let diff: string | null = $state(null);
  let error = $state('');
  let busy = $state(false);
  let done = $state('');

  // The form changed since its review: reviewed again before saving.
  const written = $derived(JSON.stringify(spec));
  let reviewed = $state('');
  $effect(() => {
    if (written !== reviewed) diff = null;
  });

  const body = () => ({
    object: {
      apiVersion: object.apiVersion ?? 'legion/v1',
      kind: object.kind,
      metadata: {
        name: object.metadata.name,
        ...(object.metadata.namespace ? { namespace: object.metadata.namespace } : {}),
        labels: object.metadata.labels ?? {}
      },
      spec: $state.snapshot(spec)
    },
    expectedVersion: object.metadata.version || undefined
  });

  async function review() {
    busy = true;
    error = '';
    done = '';
    try {
      const r = await api.apply({ ...body(), dryRun: true });
      diff = r.results[0]?.diff ?? '';
      reviewed = written;
    } catch (e) {
      error = (e as Error).message;
    } finally {
      busy = false;
    }
  }

  async function write() {
    busy = true;
    error = '';
    try {
      await api.apply(body());
      diff = null;
      done = t('Saved.');
      onsaved();
    } catch (e) {
      error =
        e instanceof ApiError && e.status === 409
          ? t('It changed meanwhile: review the change again.')
          : (e as Error).message;
      diff = null;
    } finally {
      busy = false;
    }
  }
</script>

<div class="spec-change">
  {#if diff === null}
    <button type="button" onclick={review} disabled={disabled || busy} aria-busy={busy}>
      {t('Review the change')}
    </button>
  {:else if diff.trim() === ''}
    <p class="muted">{t('Nothing would change.')}</p>
  {:else}
    <h3>{t('What would change')}</h3>
    <Diff text={diff} />
    {#if notes.length}
      <ul class="notes">
        {#each notes as note (note)}<li>{note}</li>{/each}
      </ul>
    {/if}
    <div class="row">
      <button type="button" class="primary" onclick={write} disabled={busy} aria-busy={busy}>
        {save || t('Save')}
      </button>
      <button type="button" onclick={() => (diff = null)} disabled={busy}>{t('Change it')}</button>
    </div>
  {/if}
  {#if error}<p class="error" role="alert">{error}</p>{/if}
  {#if done}<p class="muted" role="status">{done}</p>{/if}
</div>

<style>
  .spec-change {
    margin-top: 1rem;
  }
  .notes {
    margin: 0.75rem 0;
  }
  .row {
    display: flex;
    gap: 0.5rem;
  }
</style>

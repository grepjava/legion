<script lang="ts" module>
  /** The label that makes a Config a note on what it names: `<Kind>/<name>`. */
  export const NOTE = 'legion.note';
</script>

<script lang="ts">
  // Notes on an object: Configs labelled `legion.note: <Kind>/<name>`, in
  // its namespace (a node's in any the user may read), shown as Markdown.
  // Whoever may write Configs there adds one or changes it here; a note is
  // a Config like any other, so `legion apply` writes them too.
  import { api, type LObject } from '#lib/api.ts';
  import { t } from '#lib/i18n.svelte.ts';
  import Markdown from '#lib/Markdown.svelte';
  import Ico from '#lib/Ico.svelte';
  import { cannotWrite } from '#lib/session.svelte.ts';
  import { NotebookPen } from '@lucide/svelte';

  let { kind, namespace, name }: { kind: string; namespace: string; name: string } = $props();

  const names = $derived(`${kind}/${name}`);
  let notes: LObject[] = $state([]);
  let error = $state('');
  // The note being written: its Config's name and version (none: a new one).
  let editing: { name: string; namespace: string; version?: number; text: string } | null =
    $state(null);
  let saving = $state(false);
  // Where a new note goes: the object's namespace, or for one in none
  // (a node), `default`.
  const home = $derived(namespace || 'default');
  const refused = $derived(cannotWrite(home));

  async function load() {
    try {
      const listed = await api.list('Config', namespace);
      notes = listed.objects
        .filter((c) => c.metadata.labels?.[NOTE] === names)
        .sort((a, b) => a.metadata.name.localeCompare(b.metadata.name));
      error = '';
    } catch (e) {
      // A node's notes are looked for in every namespace, which not every
      // user may list: none shown, rather than an error on every node.
      notes = [];
      error = namespace ? (e as Error).message : '';
    }
  }
  $effect(() => {
    void names;
    void namespace;
    editing = null;
    load();
  });

  const text = (c: LObject) => (c.spec?.text as string | undefined) ?? '';
  // A new note's name: `note-<kind>-<name>`, as a DNS label.
  const fresh = $derived(
    `note-${kind}-${name}`
      .toLowerCase()
      .replace(/[^a-z0-9-]+/g, '-')
      .slice(0, 63)
      .replace(/-+$/, '')
  );

  async function save() {
    if (!editing || saving) return;
    saving = true;
    try {
      await api.apply({
        object: {
          apiVersion: 'legion/v1',
          kind: 'Config',
          metadata: { name: editing.name, namespace: editing.namespace, labels: { [NOTE]: names } },
          spec: { text: editing.text }
        },
        expectedVersion: editing.version
      });
      editing = null;
      await load();
    } catch (e) {
      error = (e as Error).message;
    } finally {
      saving = false;
    }
  }
</script>

<section class="notes" aria-labelledby="notes-title">
  <h2 id="notes-title" class="sr-only">{t('Notes')}</h2>
  {#if error}<p class="error" role="alert">{error}</p>{/if}
  {#each notes as n (n.metadata.namespace + '/' + n.metadata.name)}
    <article class="card note" data-note={n.metadata.name}>
      {#if editing?.name === n.metadata.name && editing.namespace === n.metadata.namespace}
        {@render editor()}
      {:else}
        <Markdown source={text(n)} />
        <p class="muted by">
          <a href="/k/Config/{n.metadata.namespace}/{n.metadata.name}">{n.metadata.namespace}/{n.metadata.name}</a>
          <button
            type="button"
            class="sm"
            disabled={!!cannotWrite(n.metadata.namespace ?? '')}
            title={cannotWrite(n.metadata.namespace ?? '') || undefined}
            onclick={() =>
              (editing = {
                name: n.metadata.name,
                namespace: n.metadata.namespace ?? '',
                version: n.metadata.version,
                text: text(n)
              })}>{t('Edit note')}</button
          >
        </p>
      {/if}
    </article>
  {/each}
  {#if editing && !notes.some((n) => n.metadata.name === editing?.name && n.metadata.namespace === editing?.namespace)}
    <article class="card note">{@render editor()}</article>
  {:else if !editing}
    <p class="add">
      <button
        type="button"
        class="sm"
        disabled={!!refused}
        title={refused || undefined}
        onclick={() => (editing = { name: fresh, namespace: home, text: '' })}
        ><Ico icon={NotebookPen} size={14} />{notes.length ? t('Add another note') : t('Add a note')}</button
      >
    </p>
  {/if}
</section>

{#snippet editor()}
  {#if editing}
    <label class="field">
      <span>{t('Note on {what}, in Markdown', { what: names })}</span>
      <textarea rows="6" bind:value={editing.text}></textarea>
    </label>
    {#if editing.text.trim()}
      <div class="preview" aria-label={t('Preview')}>
        <Markdown source={editing.text} />
      </div>
    {/if}
    <p class="by">
      <button type="button" class="sm primary" disabled={saving || !editing.text.trim()} onclick={save}
        >{t('Save note')}</button
      >
      <button type="button" class="sm" onclick={() => (editing = null)}>{t('Cancel')}</button>
      <span class="muted">{t('Kept as the Config {namespace}/{name}', { namespace: editing.namespace, name: editing.name })}</span>
    </p>
  {/if}
{/snippet}

<style>
  .notes {
    margin: 0 0 1rem;
  }
  .note {
    margin-bottom: 0.5rem;
  }
  .by {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
    align-items: center;
    margin: 0.5rem 0 0;
    font-size: 0.8rem;
  }
  .add {
    margin: 0;
  }
  .field {
    display: grid;
    gap: 0.25rem;
  }
  textarea {
    width: 100%;
    font-family: var(--mono, monospace);
  }
  .preview {
    margin-top: 0.5rem;
    padding: 0.5rem;
    border: 1px dashed var(--line);
    border-radius: 4px;
  }
</style>

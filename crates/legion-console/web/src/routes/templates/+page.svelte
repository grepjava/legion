<script lang="ts">
  // Templates (CONSOLE.md phase 5): the examples every console has and the
  // compose files saved as Configs labelled `legion.template`, each opened
  // in the compose import with its variables as parameters.
  import { templates, type Template } from '#lib/api.ts';

  let list: Template[] = $state([]);
  let error = $state('');
  $effect(() => {
    templates()
      .then((r) => (list = r.templates))
      .catch((e) => (error = e.message));
  });
  const use = (t: Template) =>
    `/import?template=${encodeURIComponent(t.name)}` +
    (t.namespace ? `&from=${encodeURIComponent(t.namespace)}` : '');
</script>

<header class="bar"><h1>Templates</h1></header>
<p class="muted">
  Compose files to deploy from: the examples, and those saved from <a href="/import">Import compose</a> as
  Configs labelled <code>legion.template</code>.
</p>
{#if error}<p class="error" role="alert">{error}</p>{/if}
<ul class="cards">
  {#each list as t (`${t.namespace}/${t.name}`)}
    <li class="card">
      <h2><a href={use(t)}>{t.name}</a></h2>
      <p class="muted">{t.namespace ? `saved in ${t.namespace}` : 'example'}</p>
      <p>{t.about}</p>
      <a class="button" href={use(t)} aria-label="Use {t.name}">Use</a>
    </li>
  {/each}
</ul>

<style>
  .cards {
    list-style: none;
    padding: 0;
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(18rem, 1fr));
    gap: 1rem;
  }
  .card {
    border: 1px solid var(--line);
    border-radius: 6px;
    padding: 0.75rem 1rem;
    background: var(--panel);
  }
  .card h2 {
    margin: 0;
  }
</style>

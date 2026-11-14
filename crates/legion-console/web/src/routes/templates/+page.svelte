<script lang="ts">
  // Templates (CONSOLE.md phase 5): the examples every console has and the
  // compose files saved as Configs labelled `legion.template`, each opened
  // in the compose import with its variables as parameters.
  import { templates, type Template } from '#lib/api.ts';
  import { t } from '#lib/i18n.svelte.ts';

  let list: Template[] = $state([]);
  let error = $state('');
  $effect(() => {
    templates()
      .then((r) => (list = r.templates))
      .catch((e) => (error = e.message));
  });
  const use = (tpl: Template) =>
    `/import?template=${encodeURIComponent(tpl.name)}` +
    (tpl.namespace ? `&from=${encodeURIComponent(tpl.namespace)}` : '');
</script>

<header class="bar"><h1>{t('Templates')}</h1></header>
<p class="muted">
  {t('Compose files to deploy from: the examples, and those saved from')}
  <a href="/import">{t('Import compose')}</a>
  {t('as Configs labelled')} <code>legion.template</code>.
</p>
{#if error}<p class="error" role="alert">{error}</p>{/if}
<ul class="cards">
  {#each list as tpl (`${tpl.namespace}/${tpl.name}`)}
    <li class="card">
      <h2><a href={use(tpl)}>{tpl.name}</a></h2>
      <p class="muted">{tpl.namespace ? t('saved in {namespace}', { namespace: tpl.namespace }) : t('example')}</p>
      <p>{tpl.about}</p>
      <a class="button" href={use(tpl)} aria-label={t('Use {name}', { name: tpl.name })}>{t('Use')}</a>
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

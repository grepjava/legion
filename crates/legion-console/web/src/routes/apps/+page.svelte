<script lang="ts">
  // Ready-to-deploy applications (TODO.MD item 5): the daemonless catalog
  // of FreeBSD images, read when the page opens, nothing of it kept. An app
  // opens on Import compose with its compose file and its variables'
  // defaults, to be looked over and deployed as a stack.
  import { api, type CatalogApp } from '#lib/api.ts';
  import { t } from '#lib/i18n.svelte.ts';
  import { Table } from '#lib/table.svelte.ts';
  import TableBar from '#lib/TableBar.svelte';
  import Pager from '#lib/Pager.svelte';

  let apps: CatalogApp[] = $state([]);
  let error = $state('');
  let loaded = $state(false);
  let category = $state('');

  $effect(() => {
    api
      .apps()
      .then((r) => (apps = r.apps))
      .catch((e) => (error = (e as Error).message))
      .finally(() => (loaded = true));
  });

  const categories = $derived([...new Set(apps.map((a) => a.category).filter(Boolean))].sort());
  const table = new Table(() => (category ? apps.filter((a) => a.category === category) : apps), {
    sorts: { name: (a) => a.name || a.id },
    sortBy: 'name',
    text: (a) => `${a.id} ${a.name} ${a.category} ${a.description}`,
    size: 24
  });
</script>

<header class="bar">
  <h1>{t('Apps')}</h1>
</header>
<p class="muted">
  {t(
    'FreeBSD applications from the daemonless catalog, ready to deploy as a stack. Each opens on Import compose with what it asks for filled in, to look over before it is deployed.'
  )}
</p>

{#if error}
  <p class="error" role="alert">{t('The catalog could not be read: {why}', { why: error })}</p>
{:else if !loaded}
  <p class="muted">{t('Reading the catalog…')}</p>
{:else}
  <TableBar {table} label={t('Search apps')} placeholder={t('Search by name or what it does…')}>
    <div class="pill-row" role="group" aria-label={t('Category')}>
      <button type="button" class="pill" class:active={!category} aria-pressed={!category} onclick={() => (category = '')}
        >{t('All')}</button
      >
      {#each categories as c (c)}
        <button
          type="button"
          class="pill"
          class:active={category === c}
          aria-pressed={category === c}
          onclick={() => {
            category = c;
            table.page = 0;
          }}>{c}</button
        >
      {/each}
    </div>
  </TableBar>
  <ul class="plain app-grid">
    {#each table.rows as a (a.id)}
      <li class="card app">
        <div class="app-head">
          <span class="badge" aria-hidden="true">{(a.name || a.id).slice(0, 1).toUpperCase()}</span>
          <div>
            <h2>{a.name || a.id}</h2>
            <p class="muted small">{a.category}{#if a.version} · {a.version}{/if}</p>
          </div>
        </div>
        <p class="about">{a.description}</p>
        <div class="app-foot">
          {#if a.web_url}<a href={a.web_url} target="_blank" rel="noopener noreferrer">{t('Its site')}</a>{/if}
          <a class="button primary" href="/import?app={encodeURIComponent(a.id)}"
            aria-label={t('Deploy {name}', { name: a.name || a.id })}>{t('Deploy')}</a
          >
        </div>
      </li>
    {:else}
      <li class="empty">{t('Nothing matches.')}</li>
    {/each}
  </ul>
  <Pager {table} label={t('Apps')} />
{/if}

<style>
  .app-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(17rem, 1fr));
    gap: 0.75rem;
  }
  .app {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }
  .app-head {
    display: flex;
    align-items: center;
    gap: 0.7rem;
  }
  .app-head h2 {
    margin: 0;
    font-size: 1rem;
  }
  .app-head p {
    margin: 0.1rem 0 0;
    font-size: 0.8rem;
  }
  .badge {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 2.2rem;
    height: 2.2rem;
    flex: none;
    border-radius: 8px;
    background: var(--surface2);
    color: var(--text);
    font-weight: 700;
  }
  .about {
    margin: 0;
    font-size: 0.88rem;
    color: var(--muted);
    flex: 1;
  }
  .app-foot {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.5rem;
    padding-top: 0.5rem;
    border-top: 1px solid var(--line);
  }
</style>

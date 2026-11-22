<script lang="ts">
  import { page } from '$app/state';
  import { fromAddress, keepInAddress } from '#lib/address.svelte.ts';
  import { api, follow, removal, type LObject } from '#lib/api.ts';
  import { ask } from '#lib/confirm.svelte.ts';
  import RowDetail from '#lib/RowDetail.svelte';
  import Pager from '#lib/Pager.svelte';
  import { Table } from '#lib/table.svelte.ts';
  import { busyWhile, toast } from '#lib/toast.svelte.ts';
  import { ChevronRight, RotateCw, Trash2, X } from '@lucide/svelte';
  import { columns, tone } from '#lib/columns.ts';
  import { matches, parse } from '#lib/filter.ts';
  import { plural } from '#lib/kinds.ts';
  import Ico from '#lib/Ico.svelte';
  import { kindIcon } from '#lib/icons.ts';
  import { t } from '#lib/i18n.svelte.ts';
  import { cannotWrite, session } from '#lib/session.svelte.ts';

  const kind = $derived(page.params.kind ?? '');
  const scoped = $derived(session.kinds.find((k) => k.kind === kind)?.clusterScoped ?? false);
  let objects: LObject[] = $state([]);
  let error = $state('');
  let filter = $state('');
  // Whether the watch has said what there is yet: until then, nothing
  // listed means nothing known, not none.
  let loaded = $state(false);
  // The column sorted on (-1 the name, -2 the namespace), and which way.
  let sortBy = $state(-1);
  let sortDir: 'ascending' | 'descending' = $state('ascending');
  function sortOn(col: number) {
    if (sortBy === col) sortDir = sortDir === 'ascending' ? 'descending' : 'ascending';
    else {
      sortBy = col;
      sortDir = 'ascending';
    }
  }
  // A link may bring a filter (a label on an object's page); another kind
  // starts unfiltered. Read as the kind opens, not followed: the filter
  // typed is kept in the address, so a reload or a link keeps it.
  $effect(() => {
    void kind;
    filter = fromAddress('filter') ?? '';
  });
  keepInAddress('filter', () => filter);

  $effect(() => {
    objects = [];
    error = '';
    loaded = false;
    return follow(
      kind,
      scoped ? '' : session.namespace,
      (o) => {
        objects = o;
        error = '';
        loaded = true;
      },
      (e) => {
        error = e.message;
        loaded = true;
      }
    );
  });

  const cols = $derived(columns(kind));
  const terms = $derived(parse(filter));
  // Kinds differ in their columns: a new kind is sorted by name.
  $effect(() => {
    void kind;
    sortBy = -1;
    sortDir = 'ascending';
  });
  const collator = new Intl.Collator(undefined, { numeric: true, sensitivity: 'base' });
  const cell = (o: LObject, col: number) =>
    col === -1 ? o.metadata.name : col === -2 ? (o.metadata.namespace ?? '') : (cols[col]?.value(o) ?? '');
  const shown = $derived.by(() => {
    const found = terms.length ? objects.filter((o) => matches(o, terms)) : objects;
    const sign = sortDir === 'ascending' ? 1 : -1;
    return [...found].sort(
      (a, b) =>
        sign * collator.compare(String(cell(a, sortBy)), String(cell(b, sortBy))) ||
        collator.compare(a.metadata.name, b.metadata.name)
    );
  });
  const sorted = (col: number) => (sortBy === col ? sortDir : undefined);
  // Paged as every list is (lib/table.svelte.ts); the filter and the sort
  // stay this page's own, as the filter takes labels and states.
  const paged = new Table(() => shown);
  $effect(() => {
    void filter;
    void kind;
    void sortBy;
    void sortDir;
    paged.page = 0;
  });

  // Rows chosen for the toolbar's actions, and rows opened on what they
  // hold; by uid, so a row keeps both as the list changes around it.
  let chosen: string[] = $state([]);
  let opened: string[] = $state([]);
  $effect(() => {
    void kind;
    void session.namespace;
    chosen = [];
    opened = [];
  });
  // What is chosen and still listed: a row gone is no longer chosen.
  const picked = $derived(shown.filter((o) => chosen.includes(o.metadata.uid)));
  const allPicked = $derived(shown.length > 0 && picked.length === shown.length);
  function choose(o: LObject, on: boolean) {
    chosen = on ? [...chosen, o.metadata.uid] : chosen.filter((u) => u !== o.metadata.uid);
  }
  function chooseAll(on: boolean) {
    chosen = on ? shown.map((o) => o.metadata.uid) : [];
  }
  function flip(o: LObject) {
    const u = o.metadata.uid;
    opened = opened.includes(u) ? opened.filter((x) => x !== u) : [...opened, u];
  }
  // Why the chosen can't be changed, if one of them can't.
  const why = $derived(
    picked.map((o) => cannotWrite(scoped ? '' : (o.metadata.namespace ?? ''))).find((w) => w) ?? null
  );
  let busy = $state(false);
  // Each chosen row in turn; what failed is said in one toast that stays.
  async function each(what: (o: LObject) => Promise<unknown>, done: string) {
    busy = true;
    const these = picked;
    const failed: string[] = [];
    try {
      await busyWhile(async () => {
        for (const o of these) {
          try {
            await what(o);
          } catch (e) {
            failed.push(`${o.metadata.name}: ${(e as Error).message}`);
          }
        }
        return '';
      });
      if (failed.length) toast(failed.join('; '), true);
      else toast(done);
      chosen = [];
    } finally {
      busy = false;
    }
  }
  async function removeChosen() {
    const n = picked.length;
    const names = picked.map((o) => o.metadata.name).join(', ');
    // What can't be come back from asks for the count typed.
    const lasting = ['Volume', 'Namespace', 'Node'].includes(kind);
    if (
      !(await ask(t('Delete {n} {kinds}: {names}?', { n, kinds: plural(kind), names }), {
        action: t('Delete'),
        danger: true,
        typed: lasting ? String(n) : undefined,
        check: picked.flatMap((o) => removal(kind, o.metadata.namespace ?? '', o.metadata.name) ?? [])
      }))
    )
      return;
    await each(
      (o) => api.remove(kind, o.metadata.namespace ?? '', o.metadata.name, o.metadata.version),
      t('{n} {kinds} deleted', { n, kinds: plural(kind) })
    );
  }
  async function restartChosen() {
    const n = picked.length;
    const names = picked.map((o) => o.metadata.name).join(', ');
    if (
      !(await ask(t('Restart {names}? Their tasks are replaced, as an update replaces them.', { names }), {
        action: t('Restart')
      }))
    )
      return;
    await each((o) => api.restart(o.metadata.namespace ?? '', o.metadata.name), t('{n} restarted', { n }));
  }
  const href = (o: LObject) => `/k/${kind}/${o.metadata.namespace || '_'}/${o.metadata.name}`;
</script>

<header class="bar">
  <h1 class="with-icon"><Ico icon={kindIcon(kind)} size={22} />{plural(kind)}</h1>
</header>

<!-- One toolbar: the filter and the count, or what the chosen rows can do. -->
<div class="toolbar" role="toolbar" aria-label={t('{kinds}: filter and actions', { kinds: plural(kind) })}>
  {#if picked.length}
    <span class="chosen">{t('{n} chosen', { n: picked.length })}</span>
    {#if kind === 'Service'}
      <button type="button" class="sm" disabled={!!why || busy} title={why ?? ''} onclick={restartChosen}
        ><Ico icon={RotateCw} size={14} />{t('Restart')}</button
      >
    {/if}
    <button type="button" class="sm danger" disabled={!!why || busy} title={why ?? ''} onclick={removeChosen}
      ><Ico icon={Trash2} size={14} />{t('Delete')}</button
    >
    <button type="button" class="sm" onclick={() => chooseAll(false)}><Ico icon={X} size={14} />{t('Clear')}</button>
    {#if why}<span class="muted small">{t('Read only: {why}.', { why })}</span>{/if}
  {:else}
    <input type="search" aria-label={t('Filter')} placeholder={t('Filter: name, app=web, !canary')}
      title={t('Words match names; key=value, key!=value and !key (no such label) match labels')} bind:value={filter} />
    <span class="muted count">{t('{shown} of {total}, live', { shown: shown.length, total: objects.length })}</span>
  {/if}
</div>

{#if error}
  <p class="error" role="alert">{error}</p>
{/if}

{#if shown.length}
  <table>
    <thead>
      <tr>
        <th scope="col" class="pick">
          <input type="checkbox" checked={allPicked} indeterminate={picked.length > 0 && !allPicked}
            aria-label={t('Choose every row shown')} onchange={(e) => chooseAll(e.currentTarget.checked)} />
        </th>
        <th scope="col" aria-sort={sorted(-1)}>
          <button type="button" class="sort-btn" data-look onclick={() => sortOn(-1)}>{t('Name')}</button>
        </th>
        {#if !scoped}
          <th scope="col" aria-sort={sorted(-2)}>
            <button type="button" class="sort-btn" data-look onclick={() => sortOn(-2)}>{t('Namespace')}</button>
          </th>
        {/if}
        {#each cols as c, i (c.label)}
          <th scope="col" aria-sort={sorted(i)}>
            <button type="button" class="sort-btn" data-look onclick={() => sortOn(i)}>{c.label}</button>
          </th>
        {/each}
      </tr>
    </thead>
    <tbody>
      {#each paged.rows as o (o.metadata.uid)}
        {@const open = opened.includes(o.metadata.uid)}
        <tr class:picked={chosen.includes(o.metadata.uid)}>
          <td class="pick">
            <input type="checkbox" checked={chosen.includes(o.metadata.uid)}
              aria-label={t('Choose {name}', { name: o.metadata.name })}
              onchange={(e) => choose(o, e.currentTarget.checked)} />
          </td>
          <td class="name-cell">
            <button type="button" class="open-row" data-look class:open aria-expanded={open}
              aria-controls="detail-{o.metadata.uid}"
              aria-label={t('What {name} holds', { name: o.metadata.name })}
              onclick={() => flip(o)}><Ico icon={ChevronRight} size={14} /></button>
            <a href={href(o)}>{o.metadata.name}</a>
          </td>
          {#if !scoped}<td>{o.metadata.namespace}</td>{/if}
          {#each cols as c (c.label)}
            <td>
              {#if c.state}
                <span class="state {tone(c.value(o))}">{c.value(o)}</span>
              {:else if c.href?.(o)}
                <a href={c.href(o)}>{c.value(o)}</a>
              {:else}
                {c.value(o)}
              {/if}
            </td>
          {/each}
        </tr>
        {#if open}
          <tr class="detail-row" id="detail-{o.metadata.uid}">
            <td colspan={cols.length + (scoped ? 2 : 3)}><RowDetail object={o} /></td>
          </tr>
        {/if}
      {/each}
    </tbody>
  </table>
  <Pager table={paged} label={plural(kind)} />
{:else if !loaded}
  <!-- Rows to come, in the table's shape, until the watch has answered. -->
  <div class="skeleton" role="status" aria-label={t('Loading…')}>
    {#each [0, 1, 2, 3, 4] as i (i)}<div class="skeleton-row" style:opacity={1 - i * 0.16}></div>{/each}
  </div>
{:else if !error}
  {#if terms.length && objects.length}
    <p class="empty">{t('None match the filter.')} <button type="button" class="link" onclick={() => (filter = '')}>{t('Clear the filter')}</button></p>
  {:else}
    <p class="empty">
      {t('No {kinds} here yet.', { kinds: plural(kind) })}
      {#if ['Service', 'Job', 'CronJob', 'Volume', 'Secret', 'Config', 'Route', 'NetworkPolicy'].includes(kind)}
        <a href={kind === 'Service' ? '/deploy' : '/apply'}
          >{kind === 'Service' ? t('Deploy one') : t('Write one')}</a
        >
      {/if}
    </p>
  {/if}
{/if}

<style>
  .toolbar {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    flex-wrap: wrap;
    min-height: 2.4rem;
    margin: 0 0 0.6rem;
    padding: 0.35rem 0.5rem;
    border: 1px solid var(--line);
    border-radius: 6px;
    background: var(--panel);
  }
  .toolbar input[type='search'] {
    min-width: 16rem;
  }
  .toolbar .count {
    margin-left: auto;
    font-size: 0.85rem;
  }
  .chosen {
    font-weight: 600;
    font-size: 0.88rem;
  }
  .small {
    font-size: 0.82rem;
  }
  .pick {
    width: 2rem;
    text-align: center;
  }
  tr.picked td {
    background: color-mix(in srgb, var(--link) 9%, transparent);
  }
  .name-cell {
    white-space: nowrap;
  }
  .open-row {
    min-height: 0;
    padding: 0.05rem;
    margin-right: 0.3rem;
    border: none;
    background: none;
    vertical-align: middle;
  }
  .open-row :global(.ico) {
    transition: transform 140ms ease;
  }
  .open-row.open :global(.ico) {
    transform: rotate(90deg);
  }
  tr.detail-row > td {
    background: var(--bg);
  }
  @media (prefers-reduced-motion: reduce) {
    .open-row :global(.ico) {
      transition: none;
    }
  }
</style>

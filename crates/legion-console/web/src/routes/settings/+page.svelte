<script lang="ts">
  // The console's settings for this cluster (CONSOLE.md phase 6, `legion
  // console configure`): its name, a banner every page shows, side-bar
  // links, the other clusters' consoles, the namespace users start in, how
  // long a session lasts. The admin's alone.
  import { consoleSettings, setConsoleSettings, type ConsoleSettings } from '#lib/api.ts';
  import { cannotWrite, session } from '#lib/session.svelte.ts';
  import { t } from '#lib/i18n.svelte.ts';

  let name = $state('');
  let banner = $state('');
  let level: 'info' | 'warning' | 'danger' = $state('info');
  let links = $state('');
  let clusters = $state('');
  let defaultNamespace = $state('');
  let length = $state('');
  let error = $state('');
  let saved = $state('');
  let busy = $state(false);
  // Shown only once read: what is typed before then would be overwritten.
  let loaded = $state(false);
  const why = $derived(cannotWrite(''));

  const lines = (ps: [string, string][]) => ps.map(([a, b]) => `${a} | ${b}`).join('\n');

  function show(s: ConsoleSettings) {
    loaded = true;
    name = s.name ?? '';
    banner = s.banner?.text ?? '';
    level = s.banner?.level ?? 'info';
    links = lines((s.links ?? []).map((l) => [l.label, l.url]));
    clusters = lines((s.clusters ?? []).map((c) => [c.name, c.url]));
    defaultNamespace = s.defaultNamespace ?? '';
    length = s.session ?? '';
  }
  $effect(() => {
    consoleSettings()
      .then(show)
      .catch((e) => (error = e.message));
  });

  /** `Name | https://address` lines, or why one is wrong. */
  function pairs(text: string, what: 'link' | 'cluster'): [string, string][] | string {
    const out: [string, string][] = [];
    for (const [i, raw] of text.split('\n').entries()) {
      const line = raw.trim();
      if (!line) continue;
      const bar = line.lastIndexOf('|');
      if (bar <= 0)
        return what === 'link'
          ? t('link line {line}: write Name | https://address', { line: i + 1 })
          : t('cluster line {line}: write Name | https://address', { line: i + 1 });
      out.push([line.slice(0, bar).trim(), line.slice(bar + 1).trim()]);
    }
    return out;
  }

  /** The form as settings, or why a line is wrong. */
  function read(): ConsoleSettings | string {
    const s: ConsoleSettings = {};
    if (name.trim()) s.name = name.trim();
    if (banner.trim()) s.banner = { text: banner.trim(), level };
    const ls = pairs(links, 'link');
    if (typeof ls === 'string') return ls;
    if (ls.length) s.links = ls.map(([label, url]) => ({ label, url }));
    const cs = pairs(clusters, 'cluster');
    if (typeof cs === 'string') return cs;
    if (cs.length) s.clusters = cs.map(([name, url]) => ({ name, url }));
    if (defaultNamespace.trim()) s.defaultNamespace = defaultNamespace.trim();
    if (length.trim()) s.session = length.trim();
    return s;
  }

  async function save(e: Event) {
    e.preventDefault();
    error = '';
    saved = '';
    const s = read();
    if (typeof s === 'string') {
      error = s;
      return;
    }
    busy = true;
    try {
      const set = await setConsoleSettings(s);
      show(set);
      session.settings = set;
      saved = t('Saved. New sessions last as long as set here; those open keep theirs.');
    } catch (err) {
      error = (err as Error).message;
    } finally {
      busy = false;
    }
  }
</script>

<header class="bar"><h1>{t('Console settings')}</h1></header>
<p class="muted">
  {t("For everyone using this cluster's console, as")} <code>legion console configure</code> {t('sets them.')}
</p>

<form class="settings" onsubmit={save}>
  <fieldset class="whole" disabled={!loaded}>
  <label
    >{t('Cluster name')} <span class="muted">{t('(at the top of the side bar and in the page title)')}</span>
    <input bind:value={name} aria-label={t('Cluster name')} maxlength="500" />
  </label>
  <fieldset>
    <legend>{t('Banner')}</legend>
    <label
      >{t('Text')} <span class="muted">{t('(shown above every page, sign-in included; empty for none)')}</span>
      <input bind:value={banner} aria-label={t('Banner text')} maxlength="500" />
    </label>
    <label
      >{t('Level')}
      <select bind:value={level} aria-label={t('Banner level')}>
        <option value="info">{t('info')}</option>
        <option value="warning">{t('warning')}</option>
        <option value="danger">{t('danger')}</option>
      </select>
    </label>
  </fieldset>
  <label
    >{t('Links')} <span class="muted">{t('(Label | https://address, one a line, in the side bar)')}</span>
    <textarea rows="4" bind:value={links} aria-label={t('Links')}></textarea>
  </label>
  <label
    >{t('Other clusters')} <span class="muted"
      >{t(
        '(Name | https://console-address, one a line; a cluster that lists this one back shows who you are there)'
      )}</span
    >
    <textarea rows="3" bind:value={clusters} aria-label={t('Other clusters')}></textarea>
  </label>
  <label
    >{t('Default namespace')} <span class="muted">{t('(where users start, if they may read it)')}</span>
    <input bind:value={defaultNamespace} aria-label={t('Default namespace')} />
  </label>
  <label
    >{t('Session length')} <span class="muted">{t('(5m to 168h; empty for 12h)')}</span>
    <input bind:value={length} aria-label={t('Session length')} placeholder="12h" />
  </label>
  <div class="controls">
    <button class="primary" disabled={!!why || busy} title={why ?? ''}>{t('Save')}</button>
    {#if why}<span class="muted">{why}.</span>{/if}
  </div>
  {#if error}<p class="error" role="alert">{error}</p>{/if}
  {#if saved}<p class="ok" role="status">{saved}</p>{/if}
  </fieldset>
</form>

<style>
  .settings {
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
    max-width: 40rem;
  }
  .settings label {
    display: flex;
    flex-direction: column;
    gap: 0.2rem;
  }
  fieldset.whole {
    display: contents;
  }
  fieldset:not(.whole) {
    border: 1px solid var(--line);
    border-radius: 6px;
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }
</style>

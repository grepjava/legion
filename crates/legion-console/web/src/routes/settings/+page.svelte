<script lang="ts">
  // The console's settings for this cluster (CONSOLE.md phase 6, `legion
  // console configure`): a banner every page shows, side-bar links, the
  // namespace users start in, how long a session lasts. The admin's alone.
  import { consoleSettings, setConsoleSettings, type ConsoleSettings } from '#lib/api.ts';
  import { cannotWrite, session } from '#lib/session.svelte.ts';

  let banner = $state('');
  let level: 'info' | 'warning' | 'danger' = $state('info');
  let links = $state('');
  let defaultNamespace = $state('');
  let length = $state('');
  let error = $state('');
  let saved = $state('');
  let busy = $state(false);
  const why = $derived(cannotWrite(''));

  function show(s: ConsoleSettings) {
    banner = s.banner?.text ?? '';
    level = s.banner?.level ?? 'info';
    links = (s.links ?? []).map((l) => `${l.label} | ${l.url}`).join('\n');
    defaultNamespace = s.defaultNamespace ?? '';
    length = s.session ?? '';
  }
  $effect(() => {
    consoleSettings()
      .then(show)
      .catch((e) => (error = e.message));
  });

  /** The form as settings, or why a link line is wrong. */
  function read(): ConsoleSettings | string {
    const s: ConsoleSettings = {};
    if (banner.trim()) s.banner = { text: banner.trim(), level };
    const ls = [];
    for (const [i, raw] of links.split('\n').entries()) {
      const line = raw.trim();
      if (!line) continue;
      const bar = line.lastIndexOf('|');
      if (bar <= 0) return `link line ${i + 1}: write Label | https://address`;
      ls.push({ label: line.slice(0, bar).trim(), url: line.slice(bar + 1).trim() });
    }
    if (ls.length) s.links = ls;
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
      saved = 'Saved. New sessions last as long as set here; those open keep theirs.';
    } catch (err) {
      error = (err as Error).message;
    } finally {
      busy = false;
    }
  }
</script>

<header class="bar"><h1>Console settings</h1></header>
<p class="muted">
  For everyone using this cluster's console, as <code>legion console configure</code> sets them.
</p>

<form class="settings" onsubmit={save}>
  <fieldset>
    <legend>Banner</legend>
    <label
      >Text <span class="muted">(shown above every page, sign-in included; empty for none)</span>
      <input bind:value={banner} aria-label="Banner text" maxlength="500" />
    </label>
    <label
      >Level
      <select bind:value={level} aria-label="Banner level">
        <option value="info">info</option>
        <option value="warning">warning</option>
        <option value="danger">danger</option>
      </select>
    </label>
  </fieldset>
  <label
    >Links <span class="muted">(Label | https://address, one a line, in the side bar)</span>
    <textarea rows="4" bind:value={links} aria-label="Links"></textarea>
  </label>
  <label
    >Default namespace <span class="muted">(where users start, if they may read it)</span>
    <input bind:value={defaultNamespace} aria-label="Default namespace" />
  </label>
  <label
    >Session length <span class="muted">(5m to 168h; empty for 12h)</span>
    <input bind:value={length} aria-label="Session length" placeholder="12h" />
  </label>
  <div class="controls">
    <button class="primary" disabled={!!why || busy} title={why ?? ''}>Save</button>
    {#if why}<span class="muted">{why}.</span>{/if}
  </div>
  {#if error}<p class="error">{error}</p>{/if}
  {#if saved}<p class="ok">{saved}</p>{/if}
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
  fieldset {
    border: 1px solid var(--line);
    border-radius: 6px;
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }
</style>

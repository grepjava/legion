<script lang="ts">
  // What an admin runs the cluster with, as the CLI does it: the CA and
  // its rotation, join tokens, autolock, the control state's backup and
  // users' credentials.
  import { api, downloads, type CaStatus } from '#lib/api.ts';
  import { session } from '#lib/session.svelte.ts';

  let ca = $state<CaStatus | null>(null);
  let error = $state('');
  let tokens: Record<string, string> = $state({});
  let lockKey = $state<string | null>(null);
  let user = $state({ name: '', role: '', namespace: '', days: 0 });
  let made = $state('');

  const fail = (e: unknown) => (error = (e as Error).message);

  // A rotation's progress is each holder moving to the new CA: asked for
  // again while one runs.
  $effect(() => {
    let stopped = false;
    let timer: ReturnType<typeof setTimeout>;
    async function poll() {
      try {
        ca = await api.ca();
      } catch (e) {
        fail(e);
      }
      if (!stopped) timer = setTimeout(poll, ca?.previous ? 3000 : 30000);
    }
    poll();
    return () => {
      stopped = true;
      clearTimeout(timer);
    };
  });

  const short = (d: string) => (d ? d.slice(0, 12) : 'not known');
  const moved = $derived(ca ? ca.holders.filter((h) => h.issuer === ca?.digest).length : 0);

  async function rotate() {
    if (
      !confirm(
        'Rotate the cluster CA? Every node moves to a new CA by itself, the old one trusted until all have. ' +
          "Then users' certificates and copies of admin.pem from before are refused: issue them again."
      )
    )
      return;
    error = '';
    try {
      ca = await api.rotateCa();
    } catch (e) {
      fail(e);
    }
  }

  async function token(role: 'manager' | 'worker', rotate: boolean) {
    if (rotate && !confirm(`Make a new ${role} join token? The one before stops working.`)) return;
    error = '';
    try {
      tokens[role] = (await api.joinToken(role, rotate)).token;
    } catch (e) {
      fail(e);
    }
  }

  async function lock(action: 'show' | 'enable' | 'disable' | 'rotate') {
    const warn = {
      enable: 'Turn autolock on? A manager restarted from now on starts only after `legion unlock` with the key shown next: keep it safe.',
      disable: 'Turn autolock off? Managers then start without the key.',
      rotate: 'Make a new autolock key? The one before stops working.',
      show: ''
    }[action];
    if (warn && !confirm(warn)) return;
    error = '';
    try {
      lockKey = (await api.autolock(action)).key;
    } catch (e) {
      fail(e);
    }
  }

  // The credentials are offered as a file, once; nothing keeps them.
  async function createUser(e: SubmitEvent) {
    e.preventDefault();
    error = '';
    made = '';
    try {
      const r = await api.createUser({
        name: user.name,
        role: user.role || undefined,
        namespace: user.namespace || undefined,
        days: user.days || undefined
      });
      const url = URL.createObjectURL(new Blob([r.pem], { type: 'application/x-pem-file' }));
      const a = document.createElement('a');
      a.href = url;
      a.download = `${user.name}.pem`;
      a.click();
      URL.revokeObjectURL(url);
      made = `${user.name}.pem saved: their credentials for ${r.days} days${r.binding ? `, RoleBinding ${r.binding}` : ''}. Hand it to them for --identity; it is not shown again.`;
      user = { name: '', role: '', namespace: '', days: 0 };
    } catch (e) {
      fail(e);
    }
  }
</script>

<header class="bar"><h1>Cluster</h1></header>
{#if !session.me?.all}
  <p class="muted">Only an admin runs the cluster.</p>
{/if}
{#if error}<p class="error">{error}</p>{/if}

<h2>Certificate authority</h2>
{#if ca}
  <p>
    Issuing: <code>{short(ca.digest)}</code>
    {#if ca.previous}
      <span class="state wait">rotating</span> from <code>{short(ca.previous)}</code> since
      {new Date(ca.since_ms).toLocaleString()}: {moved} of {ca.holders.length} moved
    {/if}
  </p>
  <table>
    <thead><tr><th>Holder</th><th>Kind</th><th>Its CA</th></tr></thead>
    <tbody>
      {#each ca.holders as h (h.kind + h.name)}
        <tr>
          <td>{h.name}</td>
          <td>{h.kind}</td>
          <td>
            <span class="state {h.issuer === ca.digest ? 'good' : h.issuer ? 'wait' : ''}"
              >{short(h.issuer)}</span
            >
          </td>
        </tr>
      {/each}
    </tbody>
  </table>
  <div class="controls">
    <button class="danger" disabled={!!ca.previous || !session.me?.all} onclick={rotate}
      >Rotate the CA</button
    >
  </div>
{/if}

<h2>Join tokens</h2>
<table>
  <tbody>
    {#each ['worker', 'manager'] as const as role (role)}
      <tr>
        <td>{role}</td>
        <td><code>{tokens[role] ?? '••••••••'}</code></td>
        <td>
          <button onclick={() => token(role, false)}>Show</button>
          <button onclick={() => token(role, true)}>Rotate</button>
        </td>
      </tr>
    {/each}
  </tbody>
</table>

<h2>Autolock</h2>
<p class="muted">
  With autolock on, a manager that restarts starts only once it is given the key with
  <code>legion unlock</code>.
</p>
{#if lockKey !== null}
  <p>{lockKey ? 'On; key:' : 'Off.'} {#if lockKey}<code>{lockKey}</code>{/if}</p>
{/if}
<div class="controls">
  <button onclick={() => lock('show')}>Show</button>
  <button onclick={() => lock('enable')}>Turn on</button>
  <button onclick={() => lock('rotate')}>New key</button>
  <button class="danger" onclick={() => lock('disable')}>Turn off</button>
</div>

<h2>Backup</h2>
<p class="muted">
  The control state, every object and the cluster's keys, as <code>legion restore</code> takes it.
  Keep it as safe as the CA's key.
</p>
<a class="button" href={downloads.backup} download>Download a backup</a>

<h2>Users</h2>
<form class="controls" onsubmit={createUser}>
  <input placeholder="name" bind:value={user.name} required aria-label="User name" />
  <select bind:value={user.role} aria-label="Role">
    <option value="">no role yet</option>
    <option value="view">view</option>
    <option value="edit">edit</option>
    <option value="admin">admin</option>
  </select>
  <input placeholder="in namespace" bind:value={user.namespace} aria-label="Namespace" />
  <input type="number" min="0" placeholder="days" bind:value={user.days} aria-label="Days valid" />
  <button class="primary" disabled={!user.name}>Create credentials</button>
</form>
{#if made}<p class="ok">{made}</p>{/if}

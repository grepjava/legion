<script lang="ts">
  // What an admin runs the cluster with, as the CLI does it: the CA and
  // its rotation, join tokens, autolock, the control state's backup and
  // users' credentials.
  import { api, downloads, type CaStatus } from '#lib/api.ts';
  import { session } from '#lib/session.svelte.ts';
  import { t, tn } from '#lib/i18n.svelte.ts';

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

  const short = (d: string) => (d ? d.slice(0, 12) : t('not known'));
  const moved = $derived(ca ? ca.holders.filter((h) => h.issuer === ca?.digest).length : 0);

  async function rotate() {
    if (
      !confirm(
        t(
          "Rotate the cluster CA? Every node moves to a new CA by itself, the old one trusted until all have. Then users' certificates and copies of admin.pem from before are refused: issue them again."
        )
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
    const ask =
      role === 'manager'
        ? t('Make a new manager join token? The one before stops working.')
        : t('Make a new worker join token? The one before stops working.');
    if (rotate && !confirm(ask)) return;
    error = '';
    try {
      tokens[role] = (await api.joinToken(role, rotate)).token;
    } catch (e) {
      fail(e);
    }
  }

  async function lock(action: 'show' | 'enable' | 'disable' | 'rotate') {
    const warn = {
      enable: t(
        'Turn autolock on? A manager restarted from now on starts only after `legion unlock` with the key shown next: keep it safe.'
      ),
      disable: t('Turn autolock off? Managers then start without the key.'),
      rotate: t('Make a new autolock key? The one before stops working.'),
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
      const file = `${user.name}.pem`;
      made = r.binding
        ? tn(
            '{file} saved: their credentials for {n} day, RoleBinding {binding}. Hand it to them for --identity; it is not shown again.',
            '{file} saved: their credentials for {n} days, RoleBinding {binding}. Hand it to them for --identity; it is not shown again.',
            r.days,
            { file, binding: r.binding }
          )
        : tn(
            '{file} saved: their credentials for {n} day. Hand it to them for --identity; it is not shown again.',
            '{file} saved: their credentials for {n} days. Hand it to them for --identity; it is not shown again.',
            r.days,
            { file }
          );
      user = { name: '', role: '', namespace: '', days: 0 };
    } catch (e) {
      fail(e);
    }
  }
</script>

<header class="bar"><h1>{t('Cluster')}</h1></header>
{#if !session.me?.all}
  <p class="muted">{t('Only an admin runs the cluster.')}</p>
{/if}
{#if error}<p class="error" role="alert">{error}</p>{/if}

<h2>{t('Certificate authority')}</h2>
{#if ca}
  <p>
    {t('Issuing:')} <code>{short(ca.digest)}</code>
    {#if ca.previous}
      <span class="state wait">{t('rotating')}</span> {t('from')} <code>{short(ca.previous)}</code>
      {t('since {when}: {moved} of {total} moved', {
        when: new Date(ca.since_ms).toLocaleString(),
        moved,
        total: ca.holders.length
      })}
    {/if}
  </p>
  <table>
    <thead><tr><th>{t('Holder')}</th><th>{t('Kind')}</th><th>{t('Its CA')}</th></tr></thead>
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
      >{t('Rotate the CA')}</button
    >
  </div>
{/if}

<h2>{t('Join tokens')}</h2>
<table>
  <tbody>
    {#each ['worker', 'manager'] as const as role (role)}
      <tr>
        <td>{role === 'manager' ? t('manager') : t('worker')}</td>
        <td><code>{tokens[role] ?? '••••••••'}</code></td>
        <td>
          <button onclick={() => token(role, false)}>{t('Show')}</button>
          <button onclick={() => token(role, true)}>{t('Rotate')}</button>
        </td>
      </tr>
    {/each}
  </tbody>
</table>

<h2>{t('Autolock')}</h2>
<p class="muted">
  {t('With autolock on, a manager that restarts starts only once it is given the key with')}
  <code>legion unlock</code>.
</p>
{#if lockKey !== null}
  <p>{lockKey ? t('On; key:') : t('Off.')} {#if lockKey}<code>{lockKey}</code>{/if}</p>
{/if}
<div class="controls">
  <button onclick={() => lock('show')}>{t('Show')}</button>
  <button onclick={() => lock('enable')}>{t('Turn on')}</button>
  <button onclick={() => lock('rotate')}>{t('New key')}</button>
  <button class="danger" onclick={() => lock('disable')}>{t('Turn off')}</button>
</div>

<h2>{t('Backup')}</h2>
<p class="muted">
  {t("The control state, every object and the cluster's keys, as")} <code>legion restore</code>
  {t("takes it. Keep it as safe as the CA's key.")}
</p>
<a class="button" href={downloads.backup} download>{t('Download a backup')}</a>

<h2>{t('Users')}</h2>
<form class="controls" onsubmit={createUser}>
  <input placeholder={t('name')} bind:value={user.name} required aria-label={t('User name')} />
  <select bind:value={user.role} aria-label={t('Role')}>
    <option value="">{t('no role yet')}</option>
    <option value="view">view</option>
    <option value="edit">edit</option>
    <option value="admin">admin</option>
  </select>
  <input placeholder={t('in namespace')} bind:value={user.namespace} aria-label={t('Namespace')} />
  <input type="number" min="0" placeholder={t('days')} bind:value={user.days} aria-label={t('Days valid')} />
  <button class="primary" disabled={!user.name}>{t('Create credentials')}</button>
</form>
{#if made}<p class="ok" role="status">{made}</p>{/if}

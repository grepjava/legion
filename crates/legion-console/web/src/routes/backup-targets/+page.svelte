<script lang="ts">
  // Where Volumes are backed up (BackupTarget, cluster-scoped): a ZFS host
  // reached over SSH, or a directory on the node holding the data. Listed,
  // added and changed here in a form, each change reviewed before it is
  // saved (SpecChange); a Volume's Protection tab picks one.
  import { api, follow, type LObject } from '#lib/api.ts';
  import { ask } from '#lib/confirm.svelte.ts';
  import { cannotWrite } from '#lib/session.svelte.ts';
  import { t } from '#lib/i18n.svelte.ts';
  import SpecChange from '#lib/SpecChange.svelte';

  let targets: LObject[] = $state([]);
  let error = $state('');
  $effect(() =>
    follow(
      'BackupTarget',
      '',
      (o) => (targets = o),
      (e) => (error = e.message)
    )
  );
  const why = $derived(cannotWrite(''));

  // The form: a target being added (editing null) or changed.
  let editing: LObject | null = $state(null);
  let adding = $state(false);
  let name = $state('');
  let kind = $state<'ssh' | 'directory'>('ssh');
  let host = $state('');
  let port = $state(22);
  let user = $state('root');
  let dataset = $state('');
  let keyNamespace = $state('default');
  let keyName = $state('');
  let hostKey = $state('');
  let path = $state('');

  function open(o: LObject | null) {
    editing = o;
    adding = !o;
    name = o?.metadata.name ?? '';
    const s = o?.spec ?? {};
    kind = s.directory ? 'directory' : 'ssh';
    host = s.ssh?.host ?? '';
    port = s.ssh?.port ?? 22;
    user = s.ssh?.user ?? 'root';
    dataset = s.ssh?.dataset ?? '';
    keyNamespace = s.ssh?.keySecret?.namespace ?? 'default';
    keyName = s.ssh?.keySecret?.name ?? '';
    hostKey = s.ssh?.hostKey ?? '';
    path = s.directory?.path ?? '';
  }

  const object = $derived<LObject>(
    editing ?? {
      apiVersion: 'legion/v1',
      kind: 'BackupTarget',
      metadata: { name: name.trim() } as LObject['metadata'],
      spec: {},
      status: {}
    }
  );
  const spec = $derived(
    kind === 'ssh'
      ? {
          ssh: {
            host: host.trim(),
            port,
            user: user.trim(),
            dataset: dataset.trim(),
            keySecret: { namespace: keyNamespace.trim(), name: keyName.trim() },
            hostKey: hostKey.trim()
          }
        }
      : { directory: { path: path.trim() } }
  );
  const ready = $derived(
    !!name.trim() &&
      (kind === 'ssh'
        ? !!(host.trim() && user.trim() && dataset.trim() && keyName.trim() && hostKey.trim())
        : path.trim().startsWith('/'))
  );

  function where(o: LObject): string {
    const s = o.spec ?? {};
    if (s.ssh) return `${s.ssh.user}@${s.ssh.host}:${s.ssh.port ?? 22} ${s.ssh.dataset}`;
    if (s.directory) return t('{path} on the node holding the data', { path: s.directory.path });
    return '';
  }

  async function remove(o: LObject) {
    const ok = await ask({
      message: t('Delete backup target {name}? Volumes backed up to it stop being backed up; what it holds stays.', {
        name: o.metadata.name
      }),
      typed: o.metadata.name
    });
    if (!ok) return;
    try {
      await api.remove('BackupTarget', '', o.metadata.name, o.metadata.version);
      if (editing?.metadata.name === o.metadata.name) editing = null;
    } catch (e) {
      error = (e as Error).message;
    }
  }
</script>

<header class="bar">
  <h1>{t('Backup targets')}</h1>
  {#if !why}<button class="sm" onclick={() => open(null)}>{t('Add a target')}</button>{/if}
</header>
<p class="muted">
  {t('Where Volumes are backed up: a ZFS host over SSH, or a directory on the node holding the data. A Volume picks one on its Protection tab.')}
</p>
{#if error}<p class="error" role="alert">{error}</p>{/if}

{#if targets.length}
  <table>
    <thead>
      <tr>
        <th scope="col">{t('Name')}</th>
        <th scope="col">{t('Kind')}</th>
        <th scope="col">{t('Where')}</th>
        <th scope="col"><span class="sr-only">{t('Actions')}</span></th>
      </tr>
    </thead>
    <tbody>
      {#each targets as o (o.metadata.name)}
        <tr>
          <td><a href={`/k/BackupTarget/_/${encodeURIComponent(o.metadata.name)}`}>{o.metadata.name}</a></td>
          <td>{o.spec?.ssh ? t('ZFS over SSH') : t('Directory')}</td>
          <td class="mono">{where(o)}</td>
          <td class="controls">
            {#if !why}
              <button class="sm" onclick={() => open(o)}>{t('Change')}</button>
              <button class="sm danger" onclick={() => remove(o)}>{t('Delete')}</button>
            {/if}
          </td>
        </tr>
      {/each}
    </tbody>
  </table>
{:else}
  <p class="empty">{t('No backup target yet.')}</p>
{/if}
{#if why}<p class="muted">{why}</p>{/if}

{#if (editing || adding) && !why}
  <form class="target card" onsubmit={(e) => e.preventDefault()}>
    <h2>{editing ? t('Change {name}', { name: editing.metadata.name }) : t('A new backup target')}</h2>
    <label>{t('Name')} <input bind:value={name} disabled={!!editing} placeholder="offsite" /></label>
    <fieldset class="kinds">
      <legend>{t('Kind')}</legend>
      <label class="check"><input type="radio" bind:group={kind} value="ssh" /> {t('A ZFS host over SSH')}</label>
      <label class="check"><input type="radio" bind:group={kind} value="directory" /> {t('A directory on the node holding the data')}</label>
    </fieldset>
    {#if kind === 'ssh'}
      <div class="row">
        <label>{t('Host')} <input bind:value={host} placeholder="backup.example.org" /></label>
        <label>{t('Port')} <input type="number" min="1" max="65535" bind:value={port} /></label>
        <label>{t('User')} <input bind:value={user} /></label>
      </div>
      <label>
        {t('Dataset')}
        <input bind:value={dataset} placeholder="tank/backups" />
        <span class="muted">{t('Each Volume is received into DATASET/NAMESPACE.VOLUME.')}</span>
      </label>
      <div class="row">
        <label>{t('Key Secret namespace')} <input bind:value={keyNamespace} /></label>
        <label>{t('Key Secret name')} <input bind:value={keyName} placeholder="backup-key" /></label>
      </div>
      <p class="muted">{t('The Secret holds the private key: legion secret create backup-key --from-file id_ed25519')}</p>
      <label>
        {t('Host key')}
        <textarea rows="2" bind:value={hostKey} placeholder="ssh-ed25519 AAAA…"></textarea>
        <span class="muted">{t('As ssh-keyscan prints it, without the host name: no other host is believed.')}</span>
      </label>
    {:else}
      <label>
        {t('Path')}
        <input bind:value={path} placeholder="/mnt/backups" />
        <span class="muted">{t('Mounted by you on every node that holds data (NFS, a second disk).')}</span>
      </label>
    {/if}
    <SpecChange
      {object}
      {spec}
      disabled={!ready}
      save={editing ? t('Save') : t('Add the target')}
      onsaved={() => {
        editing = null;
        adding = false;
      }}
    />
    <button type="button" class="sm" onclick={() => ((editing = null), (adding = false))}>{t('Cancel')}</button>
  </form>
{/if}

<style>
  .target {
    display: grid;
    gap: 0.6rem;
    max-width: 44rem;
    margin-top: 1rem;
    padding: 1rem;
  }
  .target label {
    display: grid;
    gap: 0.25rem;
  }
  .target label.check {
    display: flex;
    gap: 0.5rem;
    align-items: center;
  }
  .target .row {
    display: flex;
    gap: 0.75rem;
    flex-wrap: wrap;
  }
  .kinds {
    border: 0;
    padding: 0;
    margin: 0;
  }
</style>

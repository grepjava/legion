<script lang="ts">
  // Where Volumes are backed up (BackupTarget, cluster-scoped): a ZFS host
  // reached over SSH, a directory on the node holding the data, or an
  // S3-compatible bucket. Listed,
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
  let kind = $state<'ssh' | 'directory' | 's3'>('ssh');
  let host = $state('');
  let port = $state(22);
  let user = $state('root');
  let dataset = $state('');
  let keyNamespace = $state('default');
  let keyName = $state('');
  let hostKey = $state('');
  let path = $state('');
  let endpoint = $state('');
  let region = $state('us-east-1');
  let bucket = $state('');
  let prefix = $state('');
  let accessKeyId = $state('');
  let virtualHosted = $state(false);

  function open(o: LObject | null) {
    editing = o;
    adding = !o;
    name = o?.metadata.name ?? '';
    const s = o?.spec ?? {};
    kind = s.directory ? 'directory' : s.s3 ? 's3' : 'ssh';
    host = s.ssh?.host ?? '';
    port = s.ssh?.port ?? 22;
    user = s.ssh?.user ?? 'root';
    dataset = s.ssh?.dataset ?? '';
    const secret = s.ssh?.keySecret ?? s.s3?.secretKeySecret;
    keyNamespace = secret?.namespace ?? 'default';
    keyName = secret?.name ?? '';
    hostKey = s.ssh?.hostKey ?? '';
    path = s.directory?.path ?? '';
    endpoint = s.s3?.endpoint ?? '';
    region = s.s3?.region ?? 'us-east-1';
    bucket = s.s3?.bucket ?? '';
    prefix = s.s3?.prefix ?? '';
    accessKeyId = s.s3?.accessKeyId ?? '';
    virtualHosted = !!s.s3?.virtualHosted;
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
      : kind === 's3'
        ? {
            s3: {
              endpoint: endpoint.trim().replace(/\/+$/, ''),
              region: region.trim() || 'us-east-1',
              bucket: bucket.trim(),
              ...(prefix.trim() ? { prefix: prefix.trim().replace(/^\/+|\/+$/g, '') } : {}),
              accessKeyId: accessKeyId.trim(),
              secretKeySecret: { namespace: keyNamespace.trim(), name: keyName.trim() },
              ...(virtualHosted ? { virtualHosted: true } : {})
            }
          }
        : { directory: { path: path.trim() } }
  );
  const ready = $derived(
    !!name.trim() &&
      (kind === 'ssh'
        ? !!(host.trim() && user.trim() && dataset.trim() && keyName.trim() && hostKey.trim())
        : kind === 's3'
          ? /^https?:\/\/./.test(endpoint.trim()) && !!(bucket.trim() && accessKeyId.trim() && keyName.trim())
          : path.trim().startsWith('/'))
  );

  function where(o: LObject): string {
    const s = o.spec ?? {};
    if (s.ssh) return `${s.ssh.user}@${s.ssh.host}:${s.ssh.port ?? 22} ${s.ssh.dataset}`;
    if (s.directory) return t('{path} on the node holding the data', { path: s.directory.path });
    if (s.s3) return `s3://${s.s3.bucket}/${s.s3.prefix ? s.s3.prefix + '/' : ''} ${t('at')} ${s.s3.endpoint}`;
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
  {t('Where Volumes are backed up: a ZFS host over SSH, a directory on the node holding the data, or an S3-compatible bucket. A Volume picks one on its Protection tab.')}
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
          <td>{o.spec?.ssh ? t('ZFS over SSH') : o.spec?.s3 ? t('S3 bucket') : t('Directory')}</td>
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
      <label class="check"><input type="radio" bind:group={kind} value="s3" /> {t('An S3-compatible bucket')}</label>
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
    {:else if kind === 's3'}
      <div class="row">
        <label>
          {t('Endpoint')}
          <input bind:value={endpoint} placeholder="https://s3.eu-west-1.amazonaws.com" />
        </label>
        <label>{t('Region')} <input bind:value={region} /></label>
      </div>
      <div class="row">
        <label>{t('Bucket')} <input bind:value={bucket} placeholder="legion-backups" /></label>
        <label>{t('Prefix')} <input bind:value={prefix} placeholder="legion/prod" /></label>
      </div>
      <span class="muted">{t('Each backup is an object, whole: PREFIX/NAMESPACE.VOLUME/SNAPSHOT.zfs.')}</span>
      <label>{t('Access key ID')} <input bind:value={accessKeyId} /></label>
      <div class="row">
        <label>{t('Secret key Secret namespace')} <input bind:value={keyNamespace} /></label>
        <label>{t('Secret key Secret name')} <input bind:value={keyName} placeholder="s3-key" /></label>
      </div>
      <p class="muted">{t('The Secret holds the secret access key: legion secret create s3-key --from-file secret-key')}</p>
      <label class="check">
        <input type="checkbox" bind:checked={virtualHosted} />
        {t('Name the bucket in the host (bucket.endpoint), for services that take no other')}
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

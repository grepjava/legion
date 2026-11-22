<script lang="ts">
  // A node's host and its ZFS pools, as its agent last read them
  // (CONSOLE.md, phase 8): FreeBSD's release, the kernel, how and when it
  // booted; each pool's health, how full, its errors and its last scrub.
  // The errors can be taken as seen, and the scrubs scheduled, as
  // `legion node errors-seen` and `legion node scrub` do.
  import { api, bytes, loadText, type LObject } from './api.ts';
  import { ask } from './confirm.svelte.ts';
  import { t, tn } from './i18n.svelte.ts';
  import { cannotWrite } from './session.svelte.ts';
  import { busyWhile, toast } from './toast.svelte.ts';
  import Tile from './Tile.svelte';
  import { Activity, Database, Monitor, ShieldCheck } from '@lucide/svelte';

  let { node }: { node: LObject } = $props();
  const name = $derived(node.metadata.name);

  interface Scan {
    function: string;
    state: string;
    startMs?: number;
    endMs?: number;
    errors?: number;
  }
  interface Pool {
    name: string;
    state: string;
    sizeBytes: number;
    allocBytes: number;
    readErrors?: number;
    writeErrors?: number;
    checksumErrors?: number;
    scan?: Scan;
  }
  const host = $derived(node.status?.host as
    | { release: string; kernel: string; boot?: string; bootedMs?: number }
    | undefined);
  const pools = $derived((node.status?.pools ?? []) as Pool[]);
  const seen = $derived((node.spec?.poolErrorsSeen ?? {}) as Record<string, number>);
  const why = $derived(cannotWrite(''));

  // The load moves every second: asked for with the usage, not stored.
  let load = $state('');
  $effect(() => {
    const n = name;
    let stopped = false;
    let timer: ReturnType<typeof setTimeout>;
    async function poll() {
      try {
        const u = await api.usage();
        load = loadText(u.nodes.find((x) => x.node === n)?.load);
      } catch {
        load = '';
      }
      if (!stopped) timer = setTimeout(poll, 5000);
    }
    timer = setTimeout(poll, 0);
    return () => {
      stopped = true;
      clearTimeout(timer);
    };
  });

  const errors = (p: Pool) => (p.readErrors ?? 0) + (p.writeErrors ?? 0) + (p.checksumErrors ?? 0);
  const pct = (p: Pool) => (p.sizeBytes ? Math.round((p.allocBytes / p.sizeBytes) * 100) : 0);
  const fill = (n: number) => (n >= 90 ? 'bad' : n >= 80 ? 'wait' : 'good');
  const health = (s: string) => (s === 'ONLINE' ? 'good' : s === 'DEGRADED' ? 'wait' : 'bad');
  function ago(ms: number) {
    const s = Math.max(0, Math.round((Date.now() - ms) / 1000));
    if (s < 120) return t('{n} s ago', { n: s });
    if (s < 7200) return t('{n} min ago', { n: Math.round(s / 60) });
    if (s < 172_800) return t('{n} h ago', { n: Math.round(s / 3600) });
    return t('{n} days ago', { n: Math.round(s / 86_400) });
  }
  const scanned = (p: Pool) => {
    const s = p.scan;
    if (s?.state === 'scanning') return t('{what} running', { what: s.function });
    if (s?.function === 'scrub' && s.state === 'finished' && s.endMs) return ago(s.endMs);
    return t('never');
  };
  const unseen = $derived(pools.filter((p) => errors(p) > (seen[p.name] ?? 0)));

  let busy = $state(false);
  async function takeAsSeen() {
    const counts = Object.fromEntries(pools.filter((p) => errors(p) > 0).map((p) => [p.name, errors(p)]));
    if (
      !(await ask(t('Take the errors of {pools} on {node} as seen? Only more raise pool-errors again.', {
        pools: unseen.map((p) => p.name).join(', '),
        node: name
      }), { action: t('Take as seen') }))
    )
      return;
    busy = true;
    try {
      await busyWhile(() => api.set('Node', '', name, 'poolErrorsSeen', counts));
      toast(t('{node}: errors taken as seen', { node: name }));
    } catch (e) {
      toast((e as Error).message, true);
    } finally {
      busy = false;
    }
  }

  let every: number | null = $state(null);
  const stored = $derived((node.spec?.scrubEveryDays as number | undefined) ?? null);
  $effect(() => {
    every = stored;
  });
  async function schedule() {
    const value = every && every > 0 ? Math.round(every) : null;
    busy = true;
    try {
      const r = await busyWhile(() => api.set('Node', '', name, 'scrubEveryDays', value));
      toast(
        !r.changed
          ? t('{node} already so', { node: name })
          : value
            ? t('{node}: its pools scrubbed every {n} days', { node: name, n: value })
            : t('{node}: no scheduled scrubs', { node: name })
      );
    } catch (e) {
      toast((e as Error).message, true);
    } finally {
      busy = false;
    }
  }

  // The status cards over the details: the host, its pools' health, their
  // space, and when they were last verified (the stalest pool's scrub).
  const sizeAll = $derived(pools.reduce((a, p) => a + (p.sizeBytes ?? 0), 0));
  const usedAll = $derived(pools.reduce((a, p) => a + (p.allocBytes ?? 0), 0));
  const online = $derived(pools.filter((p) => p.state === 'ONLINE').length);
  const errorsAll = $derived(pools.reduce((a, p) => a + errors(p), 0));
  const scrubbing = $derived(pools.some((p) => p.scan?.state === 'scanning'));
  const lastScrubs = $derived(
    pools.map((p) => (p.scan?.function === 'scrub' && p.scan.state === 'finished' && p.scan.endMs ? p.scan.endMs : 0))
  );
  const never = $derived(lastScrubs.filter((x) => !x).length);
  // The latest scrub of those scrubbed least lately; pools never scrubbed
  // are counted apart.
  const scrubbed = $derived(lastScrubs.filter((x) => x));
  const stalest = $derived(scrubbed.length ? Math.min(...scrubbed) : 0);
  const scrubFound = $derived(pools.reduce((a, p) => a + (p.scan?.function === 'scrub' ? (p.scan.errors ?? 0) : 0), 0));
</script>

<div class="tiles host-tiles">
  {#if host}
    <Tile
      label={t('Host')}
      icon={Monitor}
      value={host.release}
      note={host.bootedMs ? t('Booted {when}', { when: ago(host.bootedMs) }) + (load ? ` · ${t('load {load}', { load })}` : '') : load ? t('load {load}', { load }) : ''}
    />
  {/if}
  {#if pools.length}
    <Tile
      label={t('Storage health')}
      icon={ShieldCheck}
      value={t('{n} of {total} online', { n: online, total: pools.length })}
      tone={online < pools.length ? 'bad' : errorsAll > Object.values(seen).reduce((a, n) => a + n, 0) ? 'wait' : 'good'}
      note={errorsAll ? t('{r} read, {w} write, {c} checksum', {
        r: pools.reduce((a, p) => a + (p.readErrors ?? 0), 0),
        w: pools.reduce((a, p) => a + (p.writeErrors ?? 0), 0),
        c: pools.reduce((a, p) => a + (p.checksumErrors ?? 0), 0)
      }) : t('No known pool or device errors')}
    />
    <Tile
      label={t('Capacity')}
      icon={Database}
      value={t('{pct}% used', { pct: sizeAll ? Math.round((usedAll / sizeAll) * 100) : 0 })}
      note={t('{used} of {total} · {free} free', { used: bytes(usedAll), total: bytes(sizeAll), free: bytes(sizeAll - usedAll) })}
      pct={sizeAll ? (usedAll / sizeAll) * 100 : 0}
      meterLabel={t('Pool space used')}
    />
    <Tile
      label={t('Pool verification')}
      icon={Activity}
      value={scrubbing
        ? t('Scrub running')
        : never
          ? tn('1 pool never scrubbed', '{n} pools never scrubbed', never)
          : scrubFound
            ? t('Last scrub found errors')
            : t('Last scrub clean')}
      tone={scrubFound ? 'bad' : never && !scrubbing ? 'wait' : 'good'}
      note={(stalest ? t('The stalest pool scrubbed {when}', { when: ago(stalest) }) : '') +
        (stored ? `${stalest ? ' · ' : ''}${t('every {n} days', { n: stored })}` : `${stalest ? ' · ' : ''}${t('none scheduled')}`)}
    />
  {/if}
</div>

<h2>{t('Host')}</h2>
{#if host}
  <dl class="facts">
    <dt>{t('FreeBSD')}</dt>
    <dd>{host.release}{#if host.kernel && host.kernel !== host.release}<span class="muted"> · {t('kernel {k}', { k: host.kernel })}</span>{/if}</dd>
    {#if host.boot}<dt>{t('Boot')}</dt><dd>{host.boot}</dd>{/if}
    {#if load}<dt>{t('Load')}</dt><dd title={t('Over 1, 5 and 15 minutes')}>{load}</dd>{/if}
    {#if host.bootedMs}
      <dt>{t('Up since')}</dt>
      <dd>{new Date(host.bootedMs).toLocaleString()} <span class="muted">({ago(host.bootedMs)})</span></dd>
    {/if}
  </dl>
{:else}
  <p class="empty">{t('Its agent has not said yet: it reads the host every 10 s.')}</p>
{/if}

<h2>{t('Pools')}</h2>
{#if pools.length}
  <table>
    <thead>
      <tr>
        <th scope="col">{t('Pool')}</th>
        <th scope="col">{t('Health')}</th>
        <th scope="col">{t('Used')}</th>
        <th scope="col">{t('Errors')}</th>
        <th scope="col">{t('Last scrub')}</th>
      </tr>
    </thead>
    <tbody>
      {#each pools as p (p.name)}
        {@const n = pct(p)}
        <tr>
          <td class="mono">{p.name}</td>
          <td><span class="state {health(p.state)}">{p.state}</span></td>
          <td>
            <div class="used">
              <div class="meter" role="img" aria-label={t('{pct}% full', { pct: n })}>
                <div class="meter-fill {fill(n)}" style:width="{n}%"></div>
              </div>
              <span>{n}% <span class="muted">· {t('{free} free', { free: bytes(p.sizeBytes - p.allocBytes) })}</span></span>
            </div>
          </td>
          <td>
            {#if errors(p)}
              <span class:bad={errors(p) > (seen[p.name] ?? 0)}
                >{t('{r} read, {w} write, {c} checksum', {
                  r: p.readErrors ?? 0,
                  w: p.writeErrors ?? 0,
                  c: p.checksumErrors ?? 0
                })}</span
              >
              {#if seen[p.name]}<span class="muted">{t(' ({n} taken as seen)', { n: seen[p.name] })}</span>{/if}
            {:else}
              <span class="muted">{t('none')}</span>
            {/if}
          </td>
          <td>{scanned(p)}</td>
        </tr>
      {/each}
    </tbody>
  </table>
  <div class="actions">
    <button type="button" disabled={!!why || busy || !unseen.length}
      title={why ?? (unseen.length ? t('Only more errors raise pool-errors again') : t('No errors beyond those taken as seen'))}
      onclick={takeAsSeen}>{t('Take errors as seen')}</button
    >
    <span class="group">
      <label class="inline-label"
        ><span class="muted">{t('Scrub every')}</span>
        <input type="number" min="1" bind:value={every} placeholder={t('never')} disabled={!!why} /></label
      >
      <span class="muted">{t('days')}</span>
      <button type="button" disabled={!!why || busy || every === stored} title={why ?? t('Empty for no scheduled scrubs')}
        onclick={schedule}>{t('Set')}</button
      >
    </span>
  </div>
{:else}
  <p class="empty">{t('No pools reported: the host has none, or its agent has not read them yet.')}</p>
{/if}

<style>
  .used {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    min-width: 12rem;
  }
  .used .meter {
    flex: 0 0 6rem;
  }
  .host-tiles {
    margin-top: 0.5rem;
  }
</style>

<script lang="ts">
  import { ask } from '#lib/confirm.svelte.ts';
  // Each node's disk, as its agent last reported it: what holds the space
  // under Legion's ZFS root (images, tasks' roots, Volumes, the rest) and
  // jailer's build store beside it, with what can be reclaimed.
  import { api, bytes, type NodeUsage } from '#lib/api.ts';
  import { t } from '#lib/i18n.svelte.ts';
  import { session } from '#lib/session.svelte.ts';

  let nodes: NodeUsage[] = $state([]);
  let error = $state('');
  let message = $state('');
  let busy = $state('');

  async function load() {
    try {
      nodes = (await api.usage()).nodes;
      error = '';
    } catch (err) {
      error = (err as Error).message;
    }
  }

  $effect(() => {
    load();
    const timer = setInterval(load, 10_000);
    return () => clearInterval(timer);
  });

  interface Part {
    label: string;
    used: number;
    /** Reclaims it: an image operation on the node. */
    reclaim?: { op: 'prune' | 'prune-builds'; label: string; ask: string };
    href?: string;
  }

  function parts(n: NodeUsage): Part[] {
    const volumes = (n.volumes ?? []).reduce((sum, v) => sum + v.used_bytes, 0);
    const images = n.images_bytes ?? 0;
    const jails = n.jails_bytes ?? 0;
    const other = Math.max(0, (n.zfs_used_bytes ?? 0) - images - jails - volumes);
    return [
      {
        label: t('Images'),
        used: images,
        reclaim: {
          op: 'prune',
          label: t('Prune unused images'),
          ask: t('Remove the layers no image on {node} uses?', { node: n.node })
        },
        href: '/images'
      },
      { label: t('Tasks’ roots'), used: jails },
      { label: t('Volumes'), used: volumes, href: '/volumes' },
      { label: t('Other'), used: other }
    ];
  }

  async function reclaim(node: string, r: NonNullable<Part['reclaim']>) {
    if (!(await ask(r.ask))) return;
    busy = `${node}:${r.op}`;
    message = '';
    try {
      await api.nodeImageOp({ op: r.op, node });
      message = t('{what} on {node}: done; the figures change with its next report.', {
        what: r.label,
        node
      });
    } catch (err) {
      error = (err as Error).message;
    } finally {
      busy = '';
    }
  }

  const reported = $derived(nodes.filter((n) => (n.zfs_used_bytes ?? 0) + (n.zfs_avail_bytes ?? 0) > 0));
  const pct = (part: number, whole: number) => (whole > 0 ? Math.round((100 * part) / whole) : 0);
</script>

<h1>{t('Disk usage')}</h1>
<p class="muted">
  {t('What takes the space on each node, as its agent last reported it (every few seconds).')}
</p>
{#if error}<p class="error" role="alert">{error}</p>{/if}
{#if message}<p class="notice" role="status">{message}</p>{/if}

{#if !session.me?.all}
  <p class="muted">{t('Only the cluster admin sees the nodes’ disks.')}</p>
{:else if !reported.length}
  <p class="muted">{t('No node has reported its disk yet, or none keeps its data in ZFS.')}</p>
{:else}
  {#each reported as n (n.node)}
    {@const used = n.zfs_used_bytes ?? 0}
    {@const total = used + (n.zfs_avail_bytes ?? 0)}
    <section class="card" aria-labelledby="disk-{n.node}">
      <h2 id="disk-{n.node}"><a href="/k/Node/_/{n.node}">{n.node}</a></h2>
      <p>
        {t('{used} of {total} used ({pct}%), {free} free', {
          used: bytes(used),
          total: bytes(total),
          pct: pct(used, total),
          free: bytes(n.zfs_avail_bytes ?? 0)
        })}
      </p>
      <meter min="0" max={total} value={used} high={total * 0.8} optimum={0} aria-label={t('{node}: used', { node: n.node })}></meter>
      <table>
        <caption class="sr-only">{t('What takes the space on {node}', { node: n.node })}</caption>
        <thead>
          <tr>
            <th scope="col">{t('Held by')}</th>
            <th scope="col">{t('Used')}</th>
            <th scope="col">{t('Share')}</th>
            <th scope="col"><span class="sr-only">{t('Actions')}</span></th>
          </tr>
        </thead>
        <tbody>
          {#each parts(n) as p (p.label)}
            <tr>
              <th scope="row">{#if p.href}<a href={p.href}>{p.label}</a>{:else}{p.label}{/if}</th>
              <td class="mono">{bytes(p.used)}</td>
              <td class="mono">{pct(p.used, used)}%</td>
              <td>
                {#if p.reclaim}
                  <button
                    type="button"
                    class="sm"
                    disabled={!!busy}
                    onclick={() => reclaim(n.node, p.reclaim!)}>{p.reclaim.label}</button
                  >
                {/if}
              </td>
            </tr>
          {/each}
          {#if n.build_bytes}
            <tr>
              <th scope="row">{t('Build store (jailer, beside Legion’s)')}</th>
              <td class="mono">{bytes(n.build_bytes)}</td>
              <td></td>
              <td>
                <button
                  type="button"
                  class="sm"
                  disabled={!!busy}
                  onclick={() =>
                    reclaim(n.node, {
                      op: 'prune-builds',
                      label: t('Prune the build cache'),
                      ask: t('Remove the layers and cache no image in jailer’s store on {node} uses?', {
                        node: n.node
                      })
                    })}>{t('Prune the build cache')}</button
                >
              </td>
            </tr>
          {/if}
        </tbody>
      </table>
      {#if n.volumes?.length}
        <details>
          <summary>{t('Volumes on {node} ({n})', { node: n.node, n: n.volumes.length })}</summary>
          <ul class="mono">
            {#each [...n.volumes].sort((a, b) => b.used_bytes - a.used_bytes) as v (`${v.namespace}/${v.volume}`)}
              <li>
                <a href="/k/Volume/{v.namespace}/{v.volume}">{v.namespace}/{v.volume}</a>
                {bytes(v.used_bytes)}
              </li>
            {/each}
          </ul>
        </details>
      {/if}
    </section>
  {/each}
{/if}

<style>
  meter {
    width: 100%;
    height: 0.9rem;
  }
  .card {
    margin-block: 1rem;
  }
</style>

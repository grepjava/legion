<script lang="ts">
  // The cluster's single points of failure (`legion ha`, docs/HA.md): what
  // would stop, or lose data, if one machine were lost, and how to put
  // each right. Worked out by the managers from what they hold; read when
  // the page opens and again on asking, as nothing announces it.
  import { api, type HaFinding, type HaReport } from '#lib/api.ts';
  import { session } from '#lib/session.svelte.ts';
  import { t, tn } from '#lib/i18n.svelte.ts';
  import Ico from '#lib/Ico.svelte';
  import Tile from '#lib/Tile.svelte';
  import { Crown, RefreshCw, Server, TriangleAlert, CircleAlert } from '@lucide/svelte';

  let report = $state<HaReport | null>(null);
  let error = $state('');
  let busy = $state(false);

  async function load(ns: string) {
    busy = true;
    try {
      report = await api.availability(ns);
      error = '';
    } catch (e) {
      error = (e as Error).message;
    } finally {
      busy = false;
    }
  }
  $effect(() => {
    void load(session.namespace);
  });

  const risks = $derived(report?.findings.filter((f) => f.severity === 'risk').length ?? 0);
  const warnings = $derived((report?.findings.length ?? 0) - risks);
  const voters = $derived(report?.managers.filter((m) => m.voter).length ?? 0);
  const witnesses = $derived(report?.managers.filter((m) => m.witness).length ?? 0);

  /** Where a finding's object is in the console. */
  function href(f: HaFinding): string | null {
    const m = /^(\w+) ([^/]+)\/(.+)$/.exec(f.object ?? '');
    if (!m) return null;
    return `/k/${encodeURIComponent(m[1])}/${encodeURIComponent(m[2])}/${encodeURIComponent(m[3])}`;
  }
</script>

<header class="bar">
  <h1>{t('Availability')}</h1>
  <button class="sm" onclick={() => load(session.namespace)} disabled={busy} aria-busy={busy}>
    <Ico icon={RefreshCw} />{t('Check again')}
  </button>
</header>
<p class="muted">
  {t(
    'What would stop, or lose data, if one machine were lost: each a single point of failure, with how to put it right.'
  )}
</p>
{#if error}<p class="error" role="alert">{error}</p>{/if}

{#if report}
  <div class="tiles">
    {#if report.managers.length}
      <Tile
        label={t('Managers')}
        icon={Crown}
        value={tn('{n} voting', '{n} voting', voters)}
        tone={voters >= 3 ? 'good' : 'bad'}
        note={witnesses ? tn('{n} of them a witness', '{n} of them witnesses', witnesses) : t('No witness')}
      />
      <Tile
        label={t('Nodes running tasks')}
        icon={Server}
        value={String(report.nodes)}
        tone={report.nodes >= 2 ? 'good' : 'bad'}
        href="/k/Node"
        more={t('All nodes')}
      />
    {/if}
    <Tile
      label={t('Risks')}
      icon={CircleAlert}
      value={String(risks)}
      tone={risks ? 'bad' : 'good'}
      note={t('Losing one machine stops something, or loses data.')}
    />
    <Tile
      label={t('Warnings')}
      icon={TriangleAlert}
      value={String(warnings)}
      tone={warnings ? 'wait' : 'good'}
      note={t('It carries on, but slowly, by hand, or only for some clients.')}
    />
  </div>

  {#if report.findings.length}
    <table>
      <thead>
        <tr>
          <th scope="col">{t('Severity')}</th>
          <th scope="col">{t('About')}</th>
          <th scope="col">{t('What would happen')}</th>
          <th scope="col">{t('How to put it right')}</th>
        </tr>
      </thead>
      <tbody>
        {#each report.findings as f (f.reason + (f.object ?? ''))}
          <tr>
            <td>
              <span class="state {f.severity === 'risk' ? 'bad' : 'wait'}">
                {f.severity === 'risk' ? t('risk') : t('warning')}
              </span>
            </td>
            <td>
              {#if href(f)}<a href={href(f)}>{f.object}</a>{:else}{f.object || t('the cluster')}{/if}
            </td>
            <td>{f.message}</td>
            <td>{f.fix}</td>
          </tr>
        {/each}
      </tbody>
    </table>
  {:else}
    <p class="empty">{t('No single point of failure found.')}</p>
  {/if}
  {#if report.hidden}
    <p class="muted">
      {tn(
        '{n} more in namespaces you may not read.',
        '{n} more in namespaces you may not read.',
        report.hidden
      )}
    </p>
  {/if}
{:else if !error}
  <p class="muted" aria-busy="true">{t('Checking…')}</p>
{/if}

<script lang="ts">
  // What `legion doctor` finds on every node, asked of their agents, and
  // the support bundle to send with a report of trouble.
  import { api, downloads, type NodeDiagnosis } from '#lib/api.ts';
  import { t } from '#lib/i18n.svelte.ts';
  import { session } from '#lib/session.svelte.ts';

  let nodes: NodeDiagnosis[] = $state([]);
  let asked = $state(false);
  let asking = $state(false);
  let error = $state('');

  async function ask() {
    asking = true;
    error = '';
    try {
      nodes = (await api.diagnose()).nodes;
      asked = true;
    } catch (err) {
      error = (err as Error).message;
    } finally {
      asking = false;
    }
  }

  $effect(() => {
    if (session.me?.all) ask();
  });

  // As `legion doctor` labels a finding.
  function label(f: NodeDiagnosis['findings'][number]): string {
    if (f.state === 'met') return t('ok');
    if (f.state === 'unknown') return t('unknown');
    if (f.need === 'required') return t('FAIL');
    return f.state === 'unsupported' ? t('unsupported') : t('warn');
  }
  const tone = (f: NodeDiagnosis['findings'][number]) =>
    f.state === 'met' ? 'good' : f.need === 'required' && f.state !== 'unknown' ? 'bad' : 'wait';
</script>

<h1>{t('Diagnostics')}</h1>
{#if !session.me?.all}
  <p class="muted">{t('Only the cluster admin diagnoses the nodes.')}</p>
{:else}
  <p class="muted">
    {t('What legion doctor finds on each node, asked of its agent. Nothing on the nodes is changed.')}
  </p>
  <div class="controls">
    <button type="button" disabled={asking} onclick={ask}>{asking ? t('Asking…') : t('Check again')}</button>
    <a class="button" href={downloads.supportBundle} download>{t('Download a support bundle')}</a>
  </div>
  <p class="muted">
    {t(
      'The bundle holds the cluster’s objects without its Secrets, with environment values and what else could be secret masked, its alerts, the last hour’s events and these findings. Read it before you send it.'
    )}
  </p>
  {#if error}<p class="error" role="alert">{error}</p>{/if}

  {#each nodes as n (n.node)}
    <section aria-labelledby="dx-{n.node}">
      <h2 id="dx-{n.node}">
        <a href="/k/Node/_/{n.node}">{n.node}</a>
        {#if n.error}
          <span class="state bad">{t('not asked')}</span>
        {:else if n.ready}
          <span class="state good">{t('ready')}</span>
        {:else}
          <span class="state bad">{t('not ready')}</span>
        {/if}
      </h2>
      {#if n.error}
        <p>{n.error}</p>
      {:else}
        <table>
          <caption class="sr-only">{t('What the doctor finds on {node}', { node: n.node })}</caption>
          <thead>
            <tr>
              <th scope="col">{t('Finding')}</th>
              <th scope="col">{t('Check')}</th>
              <th scope="col">{t('What was found')}</th>
              <th scope="col">{t('Fix')}</th>
            </tr>
          </thead>
          <tbody>
            {#each n.findings as f (f.id)}
              <tr>
                <td><span class="state {tone(f)}">{label(f)}</span></td>
                <td class="mono">{f.id}</td>
                <td>{f.detail}</td>
                <td class="mono">{f.remedy}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      {/if}
    </section>
  {:else}
    {#if asked}<p class="empty">{t('No nodes.')}</p>{/if}
  {/each}
{/if}

<script lang="ts">
  // How an image was built (its config's history, each step's layer) and
  // how it runs, and the services, Jobs and cron jobs that name it.
  import { bytes, type RegistryImageInspect } from '#lib/api.ts';
  import { t } from '#lib/i18n.svelte.ts';

  let { d }: { d: RegistryImageInspect } = $props();

  // `Service team-a/web` to its page.
  function href(who: string): string {
    const [kind, rest] = who.split(' ');
    const [ns, name] = rest.split('/');
    return `/k/${encodeURIComponent(kind)}/${encodeURIComponent(ns)}/${encodeURIComponent(name)}`;
  }

  const run = $derived(d.run);
  const labels = $derived(Object.entries(run?.labels ?? {}));
</script>

<dl class="facts">
  {#if d.os}
    <dt>{t('Runs on')}</dt>
    <dd class="mono">{d.os}/{d.arch}</dd>
  {/if}
  {#if d.created}
    <dt>{t('Created')}</dt>
    <dd class="mono">{d.created}</dd>
  {/if}
  {#if d.size}
    <dt>{t('Size')}</dt>
    <dd>{t('{size} compressed', { size: bytes(d.size) })}</dd>
  {/if}
  <dt>{t('Used by')}</dt>
  <dd>
    {#each d.used_by as who (who)}
      <a class="tag mono" href={href(who)}>{who}</a>
    {:else}
      <span class="muted">{t('nothing you can see')}</span>
    {/each}
  </dd>
  {#if run}
    {#if run.entrypoint.length}
      <dt>{t('Entrypoint')}</dt>
      <dd class="mono">{run.entrypoint.join(' ')}</dd>
    {/if}
    {#if run.cmd.length}
      <dt>{t('Command')}</dt>
      <dd class="mono">{run.cmd.join(' ')}</dd>
    {/if}
    {#if run.user}
      <dt>{t('User')}</dt>
      <dd class="mono">{run.user}</dd>
    {/if}
    {#if run.working_dir}
      <dt>{t('Directory')}</dt>
      <dd class="mono">{run.working_dir}</dd>
    {/if}
    {#if run.exposed_ports.length}
      <dt>{t('Ports')}</dt>
      <dd class="mono">{run.exposed_ports.join(', ')}</dd>
    {/if}
    {#if run.env.length}
      <dt>{t('Environment')}</dt>
      <dd class="mono">
        {#each run.env as e (e)}<div>{e}</div>{/each}
      </dd>
    {/if}
    {#if labels.length}
      <dt>{t('Labels')}</dt>
      <dd class="mono">
        {#each labels as [k, v] (k)}<div>{k}={v}</div>{/each}
      </dd>
    {/if}
  {/if}
</dl>

{#if d.history.length}
  <h3>{t('History')}</h3>
  <!-- svelte-ignore a11y_no_noninteractive_tabindex -->
  <div class="table-scroll" tabindex="0" role="region" aria-label={t('How the image was built')}>
    <table>
      <caption class="sr-only">{t('How the image was built, first step first')}</caption>
      <thead>
        <tr>
          <th scope="col">{t('Step')}</th>
          <th scope="col">{t('Layer')}</th>
          <th scope="col">{t('Created')}</th>
        </tr>
      </thead>
      <tbody>
        {#each d.history as s, i (i)}
          <tr>
            <td class="mono step">{s.created_by.trim() || '—'}</td>
            <td class="mono">
              {#if s.empty_layer}
                <span class="muted">{t('none')}</span>
              {:else if s.size}
                {bytes(s.size)}
              {:else}
                <span class="muted">?</span>
              {/if}
            </td>
            <td class="mono muted">{s.created}</td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
{/if}

<style>
  .step {
    white-space: pre-wrap;
    word-break: break-word;
    max-width: 60ch;
  }
</style>

<script lang="ts">
  // What `legion doctor --fix pf` would do to a node's /etc/pf.conf, shown
  // before anyone runs it, as Sylve shows its changes before its first
  // setup: the file as it would be, with what is added marked; the copy
  // of the old one kept, and the way back. The console only shows it: the
  // change is made on the node, as root, by the command given.
  import type { PfFix } from './api.ts';
  import { t } from './i18n.svelte.ts';

  let { node, fix }: { node: string; fix: PfFix } = $props();

  const command = 'legion doctor --fix pf';
  const backup = $derived(`${fix.path}.legion-<time>`);
  const undo = $derived(`cp ${backup} ${fix.path} && pfctl -f ${fix.path}`);
  // The file as it would be, each line marked where it is added.
  const lines = $derived.by(() => {
    const plan = fix.plan;
    if (!plan || plan.action !== 'change') return [];
    const added = new Set(plan.added);
    return plan.new
      .replace(/\n$/, '')
      .split('\n')
      .map((text) => ({
        text,
        added: added.has(text) || text.startsWith('# Added by legion doctor --fix pf') || text.startsWith('# Written by legion doctor --fix pf')
      }));
  });
  let copied = $state(false);
  async function copy() {
    await navigator.clipboard.writeText(command);
    copied = true;
    setTimeout(() => (copied = false), 1500);
  }
</script>

<section class="card review" aria-labelledby="pf-review-{node}">
  <h3 id="pf-review-{node}">{t('Review the fix for pf on {node}', { node })}</h3>
  {#if fix.error}
    <p class="error">{t('Its pf configuration could not be read: {why}', { why: fix.error })}</p>
  {:else if fix.plan?.action === 'nothing'}
    <p>
      {t('{path} already calls Legion’s anchors: pf has not loaded it yet. The fix loads it and has pf start at boot.', {
        path: fix.path
      })}
    </p>
  {:else if fix.plan?.action === 'refused'}
    <p>{t('{path} is left as it is: {why}.', { path: fix.path, why: fix.plan.why })}</p>
    <pre class="lines">{fix.plan.lines.join('\n')}</pre>
  {:else if fix.plan?.action === 'change'}
    <p>
      {t('Adds {n} lines to {path}, marked below; changes no line already there.', {
        n: fix.plan.added.length,
        path: fix.path
      })}
    </p>
    {#if fix.plan.note}<p class="wait">{t('Note: {note}.', { note: fix.plan.note })}</p>{/if}
    <pre class="file" aria-label={t('{path} as it would be', { path: fix.path })}>{#each lines as l, i (i)}<span
          class:added={l.added}
          ><span class="mark" aria-hidden="true">{l.added ? '+' : ' '}</span>{#if l.added}<span class="sr-only"
              >{t('added:')}
            </span>{/if}{l.text}
</span>{/each}</pre>
    <dl class="facts">
      <dt>{t('First')}</dt>
      <dd>{t('pf checks the new file (pfctl -nf); if it refuses it, nothing is written.')}</dd>
      <dt>{t('The old file')}</dt>
      <dd>{t('kept as {backup}', { backup })}</dd>
      <dt>{t('Then')}</dt>
      <dd>{t('pf is loaded, set to start at boot (pf_enable=YES) and given the new rules.')}</dd>
      <dt>{t('The way back')}</dt>
      <dd><code>{undo}</code></dd>
    </dl>
  {/if}
  {#if fix.plan && fix.plan.action !== 'refused' && !fix.error}
    <p class="run">
      {t('Run on {node}, as root:', { node })} <code>{command}</code>
      <button type="button" class="sm" data-look onclick={copy}>{copied ? t('Copied') : t('Copy')}</button>
    </p>
    <p class="muted">{t('It shows this again and asks before it changes anything.')}</p>
  {/if}
</section>

<style>
  .review {
    margin: 0.75rem 0 0;
    border-left: 4px solid var(--wait);
  }
  .review h3 {
    margin-top: 0;
  }
  pre.file,
  pre.lines {
    background: var(--bg);
    max-height: 22rem;
  }
  pre.file > span {
    display: block;
  }
  .mark {
    display: inline-block;
    width: 1.2rem;
    color: var(--muted);
  }
  .added {
    background: color-mix(in oklab, var(--good) 16%, transparent);
  }
  .added .mark {
    color: var(--good);
    font-weight: 700;
  }
  .run {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 0.5rem;
    margin-bottom: 0.2rem;
  }
  @media (forced-colors: active) {
    .added {
      outline: 1px solid CanvasText;
    }
  }
</style>

<script lang="ts">
  // The dialog `ask` (confirm.svelte.ts) opens: modal, its question as its
  // description, Cancel first in focus order but the action focused unless a
  // name is to be typed, in which case the field is. A change checked
  // first (`check`) shows what it would do, as `--dry-run` says it, and the
  // action waits for the check and stays off while anything blocks.
  import { t, tn } from './i18n.svelte.ts';
  import Ico from './Ico.svelte';
  import { confirming } from './confirm.svelte.ts';
  import { api, bytes, type DryRun } from './api.ts';
  import { TriangleExclamationPoint, CircleQuestionMark, OctagonX } from '@lucide/svelte';

  let dialog: HTMLDialogElement | undefined = $state();
  let typedNow = $state('');
  let go: HTMLButtonElement | undefined = $state();
  let field: HTMLInputElement | undefined = $state();

  const now = $derived(confirming.now);
  // The checks of the question open now (by its id: the question itself is
  // a reactive proxy, not to be told apart by identity): under way until
  // its own answer is in, done, or failed (a manager from before them,
  // say), which does not hold it.
  let checks: { for: number; runs: DryRun[] | null; error: string } = $state({ for: 0, runs: null, error: '' });
  const asks = $derived(!!now?.check?.length);
  const mine = $derived(!!now && checks.for === now.id);
  const checking = $derived(asks && (!mine || (!checks.runs && !checks.error)));
  const failed = $derived(asks && mine && !!checks.error);
  const runs = $derived(asks && mine ? (checks.runs ?? []) : []);
  const blocks = $derived(runs.some((r) => r.findings.some((f) => f.severity === 'blocking')));
  const findings = $derived(runs.flatMap((r) => r.findings));
  const moves = $derived(runs.flatMap((r) => r.moves));
  const stopped = $derived(runs.reduce((n, r) => n + r.stopped, 0));
  const hidden = $derived(runs.reduce((n, r) => n + (r.hidden ?? 0), 0));
  const blocked = $derived((!!now?.typed && typedNow.trim() !== now.typed) || checking || blocks);

  $effect(() => {
    const asked = now;
    if (!asked?.check?.length) return;
    const id = asked.id;
    checks = { for: id, runs: null, error: '' };
    Promise.all(asked.check.map((c) => api.dryRun(c))).then(
      (runs) => {
        if (checks.for === id) checks = { for: id, runs, error: '' };
      },
      (e) => {
        if (checks.for === id) checks = { for: id, runs: null, error: (e as Error).message };
      }
    );
  });
  // The question as the title, what follows it as the text: "Drain n2?" and
  // "Its tasks move to other nodes."
  const cut = $derived(now ? now.message.indexOf('?') : -1);
  const title = $derived(now ? (cut > 0 ? now.message.slice(0, cut + 1) : now.message) : '');
  const body = $derived(now && cut > 0 ? now.message.slice(cut + 1).trim() : '');

  $effect(() => {
    if (!dialog) return;
    if (now && !dialog.open) {
      typedNow = '';
      dialog.showModal();
      queueMicrotask(() => (now.typed ? field : go)?.focus());
    } else if (!now && dialog.open) {
      dialog.close();
    }
  });

  // Closed first, so focus is back on what asked before the answer goes:
  // the action that follows marks that button busy, not this dialog's.
  function answer(ok: boolean) {
    const yes = ok && !blocked;
    dialog?.close();
    confirming.now?.resolve(yes);
  }
</script>

<dialog
  bind:this={dialog}
  class="confirm"
  class:danger={now?.danger}
  aria-labelledby="confirm-title"
  aria-describedby="confirm-message"
  oncancel={(e) => {
    e.preventDefault();
    answer(false);
  }}
>
  {#if now}
    <form
      method="dialog"
      onsubmit={(e) => {
        e.preventDefault();
        answer(true);
      }}
    >
      <header>
        <Ico icon={now.danger ? TriangleExclamationPoint : CircleQuestionMark} size={20} class={now.danger ? 'bad' : ''} />
        <h2 id="confirm-title">{title}</h2>
      </header>
      {#if body}<p id="confirm-message">{body}</p>{/if}
      {#if now.detail?.length}
        <ul class="detail">
          {#each now.detail as d, i (i)}<li>{d}</li>{/each}
        </ul>
      {/if}
      {#if now.check?.length}
        <div class="check" aria-live="polite" data-check={checking ? 'checking' : failed ? 'error' : blocks ? 'blocks' : 'done'}>
          {#if checking}
            <p class="muted">{t('Checking what it would do…')}</p>
          {:else if failed}
            <p class="muted">{t('Not checked first: {why}', { why: checks.error })}</p>
          {:else}
            <ul class="found">
              {#if stopped}<li>{tn('{n} task stops where it runs', '{n} tasks stop where they run', stopped)}</li>{/if}
              {#each moves as m (`${m.namespace}/${m.volume}`)}
                <li>
                  {t('Volume {name} moves from {from} to {to} ({size})', {
                    name: `${m.namespace}/${m.volume}`,
                    from: m.from,
                    to: m.to,
                    size: m.bytes === undefined ? t('size not known') : bytes(m.bytes)
                  })}
                </li>
              {/each}
              {#each findings as f, i (i)}
                <li class={f.severity}>
                  {#if f.severity === 'blocking'}<Ico icon={OctagonX} size={14} class="bad" /><strong>{t('Blocks')}:</strong>
                  {:else}<Ico icon={TriangleExclamationPoint} size={14} />{/if}
                  {f.object ? `${f.object}: ` : ''}{f.message}
                </li>
              {/each}
              {#if hidden}<li class="muted">{tn('and {n} more in namespaces you may not read', 'and {n} more in namespaces you may not read', hidden)}</li>{/if}
              {#if !findings.length && !moves.length && !stopped}<li>{t('Nothing else changes.')}</li>{/if}
            </ul>
            <p class={blocks ? 'blocks' : 'muted'}>
              {blocks ? t('It would not complete: deal with what blocks it first.') : t('Nothing blocks it.')}
            </p>
          {/if}
        </div>
      {/if}
      {#if now.typed}
        <label class="typed">
          <span>{t('Type {name} to go on', { name: now.typed })}</span>
          <input
            bind:this={field}
            bind:value={typedNow}
            autocomplete="off"
            spellcheck="false"
            class="mono"
            aria-invalid={typedNow !== '' && blocked}
          />
        </label>
      {/if}
      <footer>
        <button type="button" onclick={() => answer(false)}>{t('Cancel')}</button>
        <button bind:this={go} type="submit" class={now.danger ? 'danger' : 'primary'} disabled={blocked}
          >{now.action}</button
        >
      </footer>
    </form>
  {/if}
</dialog>

<style>
  .confirm {
    max-width: min(30rem, 94vw);
    border-top: 3px solid var(--control);
  }
  .confirm.danger {
    border-top-color: var(--bad);
  }
  header {
    display: flex;
    align-items: center;
    gap: 0.55rem;
  }
  header :global(.ico.bad) {
    color: var(--bad);
  }
  h2 {
    margin: 0;
    font-size: 1.1rem;
  }
  p {
    margin: 0.7rem 0 0;
    line-height: 1.45;
  }
  .detail {
    margin: 0.6rem 0 0;
    padding-left: 1.1rem;
    color: var(--muted);
    font-size: 0.9rem;
  }
  .check {
    margin-top: 0.7rem;
    font-size: 0.9rem;
  }
  .check p {
    margin: 0.4rem 0 0;
  }
  .found {
    margin: 0;
    padding-left: 1.1rem;
    display: grid;
    gap: 0.2rem;
  }
  .found li.blocking {
    color: var(--text);
  }
  .found :global(.ico) {
    vertical-align: -2px;
    margin-right: 0.25rem;
  }
  .found :global(.ico.bad) {
    color: var(--bad);
  }
  .blocks {
    color: var(--bad);
    font-weight: 600;
  }
  .typed {
    display: grid;
    gap: 0.3rem;
    margin-top: 0.9rem;
    font-size: 0.9rem;
  }
  footer {
    display: flex;
    justify-content: flex-end;
    gap: 0.5rem;
    margin-top: 1.1rem;
  }
  @media (prefers-reduced-motion: no-preference) {
    .confirm[open] {
      animation: confirm-in 140ms ease-out;
    }
    @keyframes confirm-in {
      from {
        opacity: 0;
        transform: translateY(6px) scale(0.98);
      }
    }
  }
</style>

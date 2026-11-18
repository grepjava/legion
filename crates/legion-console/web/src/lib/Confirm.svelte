<script lang="ts">
  // The dialog `ask` (confirm.svelte.ts) opens: modal, its question as its
  // description, Cancel first in focus order but the action focused unless a
  // name is to be typed, in which case the field is.
  import { t } from './i18n.svelte.ts';
  import Ico from './Ico.svelte';
  import { confirming } from './confirm.svelte.ts';
  import { TriangleExclamationPoint, CircleQuestionMark } from '@lucide/svelte';

  let dialog: HTMLDialogElement | undefined = $state();
  let typedNow = $state('');
  let go: HTMLButtonElement | undefined = $state();
  let field: HTMLInputElement | undefined = $state();

  const now = $derived(confirming.now);
  const blocked = $derived(!!now?.typed && typedNow.trim() !== now.typed);
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

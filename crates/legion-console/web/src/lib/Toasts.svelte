<script lang="ts">
  // The toasts of toast.svelte.ts, newest at the foot: results announced
  // politely, errors as alerts and kept until closed.
  import { t } from './i18n.svelte.ts';
  import Ico from './Ico.svelte';
  import { dismiss, toasts } from './toast.svelte.ts';
  import { CircleCheck, CircleX, X } from '@lucide/svelte';
</script>

<div class="toasts" aria-live="polite">
  {#each toasts.list as item (item.id)}
    <div class="toast-item" class:error={item.error} role={item.error ? 'alert' : 'status'}>
      <Ico icon={item.error ? CircleX : CircleCheck} class={item.error ? 'bad' : 'good'} />
      <span>{item.text}</span>
      <button type="button" class="link" aria-label={t('Close')} onclick={() => dismiss(item.id)}
        ><Ico icon={X} size={14} /></button
      >
    </div>
  {/each}
</div>

<style>
  .toasts {
    position: fixed;
    right: 1rem;
    bottom: 1rem;
    z-index: 60;
    display: flex;
    flex-direction: column;
    gap: 0.45rem;
    max-width: min(26rem, 92vw);
    pointer-events: none;
  }
  .toast-item {
    pointer-events: auto;
    display: flex;
    align-items: flex-start;
    gap: 0.55rem;
    padding: 0.6rem 0.7rem 0.6rem 0.8rem;
    border-radius: 8px;
    background: var(--panel);
    color: var(--text);
    border: 1px solid var(--control);
    border-left: 3px solid var(--good);
    box-shadow: 0 10px 30px rgb(0 0 0 / 0.35);
    font-size: 0.9rem;
    line-height: 1.4;
  }
  .toast-item.error {
    border-left-color: var(--bad);
  }
  .toast-item > span {
    flex: 1;
    min-width: 0;
    overflow-wrap: anywhere;
  }
  .toast-item :global(.ico.good) {
    color: var(--good);
  }
  .toast-item :global(.ico.bad) {
    color: var(--bad);
  }
  @media (prefers-reduced-motion: no-preference) {
    .toast-item {
      animation: toast-slide 160ms ease-out;
    }
    @keyframes toast-slide {
      from {
        opacity: 0;
        transform: translateY(8px);
      }
    }
  }
</style>

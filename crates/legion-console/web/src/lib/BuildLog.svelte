<script lang="ts">
  // A Build's log (CONSOLE.md phase 5): jailer's output as the builder node
  // writes it, followed while the build runs; once it ended, the end of it
  // its status kept. Opened again as the build moves on.
  import { buildLog, type LObject } from './api.ts';
  import { t } from './i18n.svelte.ts';

  let { build }: { build: LObject } = $props();
  const ns = $derived(build.metadata.namespace ?? '');
  const name = $derived(build.metadata.name);
  // A string, so only a new phase opens the log again.
  const phase = $derived<string>(build.status?.phase ?? 'Pending');
  let text = $state('');
  let status = $state('');
  let box: HTMLPreElement | undefined = $state();
  let pinned = true;

  $effect(() => {
    const running = phase === 'Running';
    text = '';
    status = phase === 'Pending' ? t('Waiting for a builder node.') : t('reading…');
    if (phase === 'Pending') return;
    return buildLog(
      ns,
      name,
      running,
      (piece) => {
        text += piece;
        status = running ? t('following') : '';
        if (pinned) queueMicrotask(() => box?.scrollTo({ top: box.scrollHeight }));
      },
      (error) => (status = error ?? (running ? t('ended') : ''))
    );
  });
</script>

{#if status}<p class="muted" role="status">{status}</p>{/if}
<pre
  class="log"
  bind:this={box}
  onscroll={() => box && (pinned = box.scrollTop + box.clientHeight >= box.scrollHeight - 4)}
  aria-label={t('Build log')}>{text}</pre>

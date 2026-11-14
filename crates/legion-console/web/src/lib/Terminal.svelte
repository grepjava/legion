<script lang="ts">
  // A command in a task's container under a terminal, as `legion exec -it`
  // runs it: a shell unless told otherwise.
  import { onDestroy, onMount } from 'svelte';
  import { Terminal } from '@xterm/xterm';
  import { FitAddon } from '@xterm/addon-fit';
  import '@xterm/xterm/css/xterm.css';
  import { exec, type LObject } from './api.ts';
  import { cannotWrite } from './session.svelte.ts';

  let { task }: { task: LObject } = $props();
  const ns = $derived(task.metadata.namespace ?? '');
  const name = $derived(task.metadata.name);
  const containers: string[] = $derived(
    (task.spec?.pod?.containers ?? []).map((c: { name: string }) => c.name)
  );
  const why = $derived(cannotWrite(ns));
  const runningTask = $derived(task.status?.state === 'running');
  let container = $state('');
  let command = $state('/bin/sh');
  let open = $state(false);
  let status = $state('');
  let box: HTMLDivElement | undefined = $state();
  let closer: HTMLButtonElement | undefined = $state();
  let term: Terminal | null = null;
  let fit: FitAddon | null = null;
  let session: ReturnType<typeof exec> | null = null;

  function stop() {
    session?.close();
    session = null;
    term?.dispose();
    term = null;
    open = false;
  }

  function start() {
    stop();
    if (!box) return;
    const words = command.trim().split(/\s+/).filter(Boolean);
    term = new Terminal({
      cursorBlink: true,
      fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
      fontSize: 13
    });
    fit = new FitAddon();
    term.loadAddon(fit);
    term.open(box);
    fit.fit();
    open = true;
    status = 'connecting…';
    const t = term;
    session = exec(
      ns,
      name,
      { container: container || containers[0], command: words, cols: t.cols, rows: t.rows },
      (data) => {
        status = '';
        t.write(data);
      },
      (exit, error) => {
        open = false;
        status = error ?? `the command exited with ${exit}`;
        t.write(`\r\n\x1b[2m[${status}]\x1b[0m\r\n`);
      }
    );
    // Every key goes to the command but Shift+Esc, which leaves the
    // terminal for the page (WCAG 2.1.2; Esc alone still reaches it).
    t.attachCustomKeyEventHandler((e) => {
      if (e.type === 'keydown' && e.key === 'Escape' && e.shiftKey) {
        closer?.focus();
        return false;
      }
      return true;
    });
    t.onData((d) => session?.input(d));
    t.onResize(({ cols, rows }) => session?.resize(cols, rows));
    t.focus();
  }

  onMount(() => {
    const seen = new ResizeObserver(() => fit?.fit());
    if (box) seen.observe(box);
    return () => seen.disconnect();
  });
  onDestroy(stop);
</script>

<form
  class="controls"
  onsubmit={(e) => {
    e.preventDefault();
    start();
  }}
>
  {#if containers.length > 1}
    <label>
      Container
      <select bind:value={container}>
        {#each containers as c (c)}<option value={c}>{c}</option>{/each}
      </select>
    </label>
  {/if}
  <label>Command <input bind:value={command} aria-label="Command" /></label>
  <button class="primary" disabled={!!why || !runningTask} title={why ?? (runningTask ? '' : 'The task is not running')}
    >{open ? 'Start again' : 'Open'}</button
  >
  {#if open}<button type="button" onclick={stop} bind:this={closer}>Close</button>{/if}
  <span class="muted">{why ? `Read only: ${why}.` : status}</span>
</form>
{#if open}<p class="muted">Shift+Esc leaves the terminal for the page.</p>{/if}
<div class="terminal" bind:this={box} role="region" aria-label="Terminal"></div>

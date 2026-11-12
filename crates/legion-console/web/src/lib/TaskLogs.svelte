<script lang="ts">
  // A task's output, as `legion logs` prints it: the last lines, then
  // following it while the container runs.
  import { logs, type LObject } from './api.ts';

  let { task }: { task: LObject } = $props();
  const containers: string[] = $derived(
    (task.spec?.pod?.containers ?? []).map((c: { name: string }) => c.name)
  );
  let container = $state('');
  let tail = $state(500);
  let followOn = $state(true);
  let parts: { text: string; stderr: boolean }[] = $state([]);
  let status = $state('');
  let box: HTMLPreElement | undefined = $state();
  // Kept to the end while the reader is there.
  let pinned = true;

  // Keeps the output to about this much, dropping the oldest.
  const KEEP = 2_000_000;

  // Strings, so a new status of the task doesn't start the logs again.
  const ns = $derived(task.metadata.namespace ?? '');
  const name = $derived(task.metadata.name);
  const chosen = $derived(container || containers[0] || '');

  $effect(() => {
    const opts = { container: chosen, tail, follow: followOn };
    parts = [];
    status = 'reading…';
    let size = 0;
    return logs(
      ns,
      name,
      opts,
      (text, stderr) => {
        const last = parts[parts.length - 1];
        if (last && last.stderr === stderr) last.text += text;
        else parts.push({ text, stderr });
        size += text.length;
        while (size > KEEP && parts.length > 1) size -= parts.shift()!.text.length;
        status = opts.follow ? 'following' : '';
      },
      (error) => (status = error ?? 'ended')
    );
  });

  $effect(() => {
    void parts.length;
    void parts[parts.length - 1]?.text;
    if (box && pinned) box.scrollTop = box.scrollHeight;
  });

  function scrolled() {
    if (box) pinned = box.scrollTop + box.clientHeight >= box.scrollHeight - 8;
  }
</script>

<div class="controls">
  {#if containers.length > 1}
    <label>
      Container
      <select bind:value={container}>
        {#each containers as c (c)}<option value={c}>{c}</option>{/each}
      </select>
    </label>
  {/if}
  <label>
    Last
    <select bind:value={tail}>
      <option value={100}>100</option>
      <option value={500}>500</option>
      <option value={5000}>5000</option>
    </select>
    lines
  </label>
  <label><input type="checkbox" bind:checked={followOn} /> Follow</label>
  <span class="muted">{status}</span>
</div>

<pre class="log" bind:this={box} onscroll={scrolled}>{#each parts as p, i (i)}<span
      class:stderr={p.stderr}>{p.text}</span
    >{/each}{#if !parts.length}<span class="muted">No output.</span>{/if}</pre>

<script lang="ts">
  // A unified diff, its added and removed lines coloured.
  let { text }: { text: string } = $props();
  const lines = $derived(
    text.split('\n').map((l) => ({
      l,
      c: l.startsWith('+++') || l.startsWith('---') ? 'head' : l.startsWith('+') ? 'add' : l.startsWith('-') ? 'del' : l.startsWith('@@') ? 'hunk' : ''
    }))
  );
</script>

<pre class="diff">{#each lines as { l, c }, i (i)}<span class={c}>{l}</span>
{/each}</pre>

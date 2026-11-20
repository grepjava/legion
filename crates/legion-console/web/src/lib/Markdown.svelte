<script lang="ts">
  // A note's Markdown as elements (lib/markdown.ts): never as HTML. Its
  // headings sit under the page's own, from h3 down.
  import { parse, type Block, type Inline } from '#lib/markdown.ts';
  import Self from '#lib/Markdown.svelte';

  let { source = '', blocks }: { source?: string; blocks?: Block[] } = $props();
  const tree = $derived(blocks ?? parse(source));
  const outside = (href: string) => !href.startsWith('/') && !href.startsWith('#');
</script>

{#snippet spans(xs: Inline[])}{#each xs as x, i (i)}{#if x.t === 'text'}{x.text}{:else if x.t === 'code'}<code
        >{x.text}</code
      >{:else if x.t === 'strong'}<strong>{@render spans(x.children)}</strong>{:else if x.t === 'em'}<em
        >{@render spans(x.children)}</em
      >{:else if x.t === 'link'}<a
        href={x.href}
        rel={outside(x.href) ? 'noopener noreferrer' : undefined}
        target={outside(x.href) ? '_blank' : undefined}>{@render spans(x.children)}</a
      >{/if}{/each}{/snippet}

<div class="md">
  {#each tree as b, i (i)}
    {#if b.t === 'h'}
      {#if b.level === 1}<h3>{@render spans(b.children)}</h3>
      {:else if b.level === 2}<h4>{@render spans(b.children)}</h4>
      {:else if b.level === 3}<h5>{@render spans(b.children)}</h5>
      {:else}<h6>{@render spans(b.children)}</h6>{/if}
    {:else if b.t === 'p'}
      <p>{@render spans(b.children)}</p>
    {:else if b.t === 'quote'}
      <blockquote><Self blocks={b.children} /></blockquote>
    {:else if b.t === 'ul'}
      <ul>
        {#each b.items as it, j (j)}<li>{@render spans(it)}</li>{/each}
      </ul>
    {:else if b.t === 'ol'}
      <ol>
        {#each b.items as it, j (j)}<li>{@render spans(it)}</li>{/each}
      </ol>
    {:else if b.t === 'pre'}
      <pre><code>{b.text}</code></pre>
    {:else}
      <hr />
    {/if}
  {/each}
</div>

<style>
  .md :global(:first-child) {
    margin-top: 0;
  }
  .md :global(:last-child) {
    margin-bottom: 0;
  }
  .md h3,
  .md h4,
  .md h5,
  .md h6 {
    margin: 0.75rem 0 0.35rem;
    font-size: 0.95rem;
  }
  .md h3 {
    font-size: 1.05rem;
  }
  .md p,
  .md ul,
  .md ol,
  .md pre,
  .md blockquote {
    margin: 0.4rem 0;
  }
  .md blockquote {
    border-left: 3px solid var(--line);
    padding-left: 0.75rem;
    color: var(--muted);
  }
  .md pre {
    overflow-x: auto;
    padding: 0.5rem 0.65rem;
    border-radius: 4px;
    background: var(--bg);
  }
  .md a {
    text-decoration: underline;
  }
</style>

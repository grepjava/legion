// Markdown enough for a note: headings, paragraphs, lists, quotes, code
// blocks, and inline code, bold, italic and links. Parsed to a tree that
// Svelte renders as elements (Markdown.svelte): nothing written in a note
// becomes markup, so a note cannot put a script or a style on the page.
// Links go only to http, https and mailto, and to the console's own pages.

export type Inline =
  | { t: 'text'; text: string }
  | { t: 'code'; text: string }
  | { t: 'strong' | 'em'; children: Inline[] }
  | { t: 'link'; href: string; children: Inline[] };

export type Block =
  | { t: 'h'; level: 1 | 2 | 3 | 4; children: Inline[] }
  | { t: 'p'; children: Inline[] }
  | { t: 'quote'; children: Block[] }
  | { t: 'ul' | 'ol'; items: Inline[][] }
  | { t: 'pre'; text: string }
  | { t: 'hr' };

/** A link's target if it may be followed; null otherwise. */
export function safeHref(href: string): string | null {
  const h = href.trim();
  if (/^(https?:|mailto:)/i.test(h)) return h;
  if (h.startsWith('/') && !h.startsWith('//')) return h;
  if (h.startsWith('#')) return h;
  return null;
}

export function inline(text: string): Inline[] {
  const out: Inline[] = [];
  let plain = '';
  const flush = () => {
    if (plain) out.push({ t: 'text', text: plain });
    plain = '';
  };
  let i = 0;
  while (i < text.length) {
    const rest = text.slice(i);
    // A backslash keeps the next mark as it is.
    if (rest[0] === '\\' && rest.length > 1 && /[\\`*_[\]()#+\-.!>]/.test(rest[1])) {
      plain += rest[1];
      i += 2;
      continue;
    }
    let m = /^`([^`]+)`/.exec(rest);
    if (m) {
      flush();
      out.push({ t: 'code', text: m[1] });
      i += m[0].length;
      continue;
    }
    m = /^(\*\*|__)(?=\S)([\s\S]*?\S)\1/.exec(rest);
    if (m) {
      flush();
      out.push({ t: 'strong', children: inline(m[2]) });
      i += m[0].length;
      continue;
    }
    m = /^(\*|_)(?=\S)([\s\S]*?\S)\1/.exec(rest);
    if (m && !(m[1] === '_' && /\w/.test(text[i - 1] ?? ''))) {
      flush();
      out.push({ t: 'em', children: inline(m[2]) });
      i += m[0].length;
      continue;
    }
    m = /^\[([^\]]+)\]\(([^)\s]+)\)/.exec(rest);
    if (m) {
      const href = safeHref(m[2]);
      flush();
      if (href) out.push({ t: 'link', href, children: inline(m[1]) });
      else out.push(...inline(m[1]));
      i += m[0].length;
      continue;
    }
    m = /^https?:\/\/[^\s<>()]+[^\s<>().,;:!?'"]/.exec(rest);
    if (m && !/\w/.test(text[i - 1] ?? '')) {
      flush();
      out.push({ t: 'link', href: m[0], children: [{ t: 'text', text: m[0] }] });
      i += m[0].length;
      continue;
    }
    plain += rest[0];
    i += 1;
  }
  flush();
  return out;
}

export function parse(source: string): Block[] {
  const lines = source.replace(/\r\n?/g, '\n').split('\n');
  const blocks: Block[] = [];
  let i = 0;
  const para: string[] = [];
  const endPara = () => {
    if (para.length) blocks.push({ t: 'p', children: inline(para.join(' ')) });
    para.length = 0;
  };
  while (i < lines.length) {
    const line = lines[i];
    if (/^\s*$/.test(line)) {
      endPara();
      i++;
      continue;
    }
    const fence = /^\s*(```|~~~)/.exec(line);
    if (fence) {
      endPara();
      const code: string[] = [];
      i++;
      while (i < lines.length && !lines[i].trimStart().startsWith(fence[1])) code.push(lines[i++]);
      i++;
      blocks.push({ t: 'pre', text: code.join('\n') });
      continue;
    }
    const h = /^(#{1,6})\s+(.*?)\s*#*\s*$/.exec(line);
    if (h) {
      endPara();
      blocks.push({ t: 'h', level: Math.min(4, h[1].length) as 1 | 2 | 3 | 4, children: inline(h[2]) });
      i++;
      continue;
    }
    if (/^\s*([-*_])(\s*\1){2,}\s*$/.test(line)) {
      endPara();
      blocks.push({ t: 'hr' });
      i++;
      continue;
    }
    if (/^\s*>/.test(line)) {
      endPara();
      const quoted: string[] = [];
      while (i < lines.length && /^\s*>/.test(lines[i])) quoted.push(lines[i++].replace(/^\s*> ?/, ''));
      blocks.push({ t: 'quote', children: parse(quoted.join('\n')) });
      continue;
    }
    const item = /^\s*([-*+]|\d+[.)])\s+/.exec(line);
    if (item) {
      endPara();
      const ordered = /\d/.test(item[1]);
      const items: string[] = [];
      while (i < lines.length) {
        const m = /^\s*([-*+]|\d+[.)])\s+(.*)$/.exec(lines[i]);
        if (m && /\d/.test(m[1]) === ordered) items.push(m[2]);
        else if (items.length && /^\s{2,}\S/.test(lines[i])) items[items.length - 1] += ' ' + lines[i].trim();
        else break;
        i++;
      }
      blocks.push({ t: ordered ? 'ol' : 'ul', items: items.map(inline) });
      continue;
    }
    para.push(line.trim());
    i++;
  }
  endPara();
  return blocks;
}

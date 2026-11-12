// An object as YAML, the way `legion get -o yaml` reads: block style,
// strings quoted only when they would read as something else.

const RESERVED = /^(true|false|yes|no|on|off|null|~|y|n)$/i;
const NUMBER = /^[-+]?(\d[\d_]*(\.\d*)?|\.\d+)([eE][-+]?\d+)?$|^0x[0-9a-f]+$|^0o[0-7]+$|^[-+]?\.(inf|nan)$/i;
const PLAIN = /^[A-Za-z0-9_./][A-Za-z0-9 _./:@+=,()-]*$/;

function scalar(s: string): string {
  if (
    s === '' ||
    !PLAIN.test(s) ||
    RESERVED.test(s) ||
    NUMBER.test(s) ||
    s.endsWith(' ') ||
    s.includes(': ') ||
    s.endsWith(':') ||
    s.includes(' #')
  ) {
    // A JSON string is a valid YAML double-quoted scalar.
    return JSON.stringify(s);
  }
  return s;
}

function leaf(v: unknown): string | null {
  if (v === null || v === undefined) return 'null';
  if (typeof v === 'boolean' || typeof v === 'number') return String(v);
  if (typeof v === 'string') return scalar(v);
  if (Array.isArray(v) && v.length === 0) return '[]';
  if (typeof v === 'object' && Object.keys(v as object).length === 0) return '{}';
  return null;
}

function lines(v: unknown, indent: string, out: string[]) {
  if (Array.isArray(v)) {
    for (const item of v) {
      const l = leaf(item);
      if (l !== null) {
        out.push(`${indent}- ${l}`);
        continue;
      }
      // The item's first line goes after the dash, the rest under it.
      const inner: string[] = [];
      lines(item, indent + '  ', inner);
      out.push(`${indent}- ${inner[0].slice(indent.length + 2)}`, ...inner.slice(1));
    }
    return;
  }
  for (const [k, item] of Object.entries(v as Record<string, unknown>)) {
    if (item === undefined) continue;
    const l = leaf(item);
    if (l !== null) out.push(`${indent}${scalar(k)}: ${l}`);
    else {
      out.push(`${indent}${scalar(k)}:`);
      lines(item, Array.isArray(item) ? indent : indent + '  ', out);
    }
  }
}

export function toYaml(v: unknown): string {
  const l = leaf(v);
  if (l !== null) return l + '\n';
  const out: string[] = [];
  lines(v, '', out);
  return out.join('\n') + '\n';
}

import type { LObject } from './api.ts';

// The list filter: words match the name (or namespace/name); `key=value`,
// `key!=value` and `!key` (no such label) match labels. All terms must hold.

type Term =
  | { name: string }
  | { key: string; value: string; not: boolean }
  | { key: string; has: boolean };

export function parse(text: string): Term[] {
  return text
    .split(/[\s,]+/)
    .filter(Boolean)
    .map((t): Term => {
      const ne = t.indexOf('!=');
      if (ne > 0) return { key: t.slice(0, ne), value: t.slice(ne + 2), not: true };
      const eq = t.indexOf('=');
      if (eq > 0) return { key: t.slice(0, eq), value: t.slice(eq + 1).replace(/^=/, ''), not: false };
      if (t.startsWith('!') && t.length > 1) return { key: t.slice(1), has: false };
      // A word with a label-like prefix (`app.kubernetes.io/name`) is still
      // a name: only `=` makes a term a label.
      return { name: t };
    });
}

export function matches(o: LObject, terms: Term[]): boolean {
  const labels = o.metadata.labels ?? {};
  const full = `${o.metadata.namespace ?? ''}/${o.metadata.name}`;
  return terms.every((t) => {
    if ('name' in t) return full.includes(t.name);
    if ('has' in t) return t.key in labels === t.has;
    const v = labels[t.key];
    return t.not ? v !== t.value : v === t.value;
  });
}

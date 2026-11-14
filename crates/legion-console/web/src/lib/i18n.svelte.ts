// Translations (CONSOLE.md phase 6). The console is written in English and
// every string a person reads goes through `t()`, or `tn()` where it counts
// something. A language is a file `locales/<code>.json` (`fr`, `pt-BR`)
// mapping each English string to its translation, found when the console
// is built; a string missing there stays English. `npm run messages` lists
// every string to translate in `messages.json`.
//
// Strings are written whole, with `{name}` for what changes, never pieced
// together, so a translation may put the words in its own order. Names
// from the API (kinds such as Service, fields, states the API reports)
// and what users typed stay as they are.

/** A translation: text, or a counted one by plural category (`one`,
 * `few`, `many`, `other`, as `Intl.PluralRules` names them). */
type Entry = string | Partial<Record<Intl.LDMLPluralRule, string>>;
type Catalog = Record<string, Entry>;
type Vars = Record<string, string | number>;

const files = import.meta.glob<Catalog>('./locales/*.json', { eager: true, import: 'default' });
const catalogs: Record<string, Catalog> = Object.fromEntries(
  Object.entries(files).map(([path, c]) => [path.replace(/^.*\/([^/]+)\.json$/, '$1'), c])
);

/** English first, then each language there is a file for. */
export const languages: string[] = [
  'en',
  ...Object.keys(catalogs)
    .filter((l) => l !== 'en')
    .sort()
];

const KEY = 'legion.language';

/** The language chosen before, else the browser's first one there is a
 * file for (`pt-BR`, then `pt`), else English. */
function initial(): string {
  try {
    const saved = localStorage.getItem(KEY);
    if (saved && languages.includes(saved)) return saved;
  } catch {
    // No storage: the browser's choice.
  }
  for (const l of globalThis.navigator?.languages ?? []) {
    if (languages.includes(l)) return l;
    const base = l.split('-')[0];
    if (languages.includes(base)) return base;
  }
  return 'en';
}

export const i18n = $state({ language: initial() });

if (typeof document !== 'undefined') document.documentElement.lang = i18n.language;

export function setLanguage(language: string) {
  if (!languages.includes(language)) return;
  i18n.language = language;
  document.documentElement.lang = language;
  try {
    localStorage.setItem(KEY, language);
  } catch {
    // Kept for this page only.
  }
}

function fill(text: string, vars?: Vars): string {
  if (!vars) return text;
  return text.replace(/\{(\w+)\}/g, (whole, k: string) => (k in vars ? String(vars[k]) : whole));
}

/** `text` in the chosen language, its `{name}`s filled from `vars`. */
export function t(text: string, vars?: Vars): string {
  const entry = catalogs[i18n.language]?.[text];
  return fill(typeof entry === 'string' && entry ? entry : text, vars);
}

/** A count: English `one` for 1 and `other` for the rest, `{n}` the count.
 * A language's entry, under the English `other`, gives each plural form
 * it has. */
export function tn(one: string, other: string, n: number, vars?: Vars): string {
  const all = { ...vars, n };
  const entry = catalogs[i18n.language]?.[other];
  if (entry && typeof entry === 'object') {
    const form = new Intl.PluralRules(i18n.language).select(n);
    const text = entry[form] ?? entry.other;
    if (text) return fill(text, all);
  }
  return fill(n === 1 ? one : other, all);
}

/** A language's own name, for the menu. */
export function languageName(code: string): string {
  try {
    return new Intl.DisplayNames([code], { type: 'language' }).of(code) ?? code;
  } catch {
    return code;
  }
}

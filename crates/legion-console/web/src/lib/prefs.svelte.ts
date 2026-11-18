// What the console remembers for this browser: the pages pinned to the side
// bar (in the user's order), the pages visited last, whether the side bar is
// hidden and which of its sections are closed. Kept in localStorage, and
// followed across tabs.

export interface PageRef {
  /** The path and query, as the page was opened. */
  href: string;
  /** Its title, as the side bar and the palette show it. */
  title: string;
}

const KEYS = {
  pins: 'legion.pins',
  recent: 'legion.recent',
  sidebar: 'legion.sidebar',
  closed: 'legion.nav-closed',
  tree: 'legion.tree',
  treeOpen: 'legion.tree-open'
} as const;

/** What the side bar shows: its pages, or the cluster's objects as a tree
 * by namespace or by node. */
export type TreeMode = 'pages' | 'namespace' | 'node';

/** How many pages the recent list keeps. */
export const RECENT_MAX = 8;

function read<T>(key: string, fallback: T, valid: (v: unknown) => v is T): T {
  try {
    const raw = localStorage.getItem(key);
    if (raw === null) return fallback;
    const v: unknown = JSON.parse(raw);
    return valid(v) ? v : fallback;
  } catch {
    return fallback;
  }
}

function write(key: string, value: unknown) {
  try {
    localStorage.setItem(key, JSON.stringify(value));
  } catch {
    // Kept for this page only.
  }
}

const isRefs = (v: unknown): v is PageRef[] =>
  Array.isArray(v) &&
  v.every(
    (x) =>
      x && typeof x === 'object' && typeof x.href === 'string' && typeof x.title === 'string'
  );
const isStrings = (v: unknown): v is string[] =>
  Array.isArray(v) && v.every((x) => typeof x === 'string');
const isBool = (v: unknown): v is boolean => typeof v === 'boolean';
const isMode = (v: unknown): v is TreeMode => v === 'pages' || v === 'namespace' || v === 'node';

const hasStorage = typeof localStorage !== 'undefined';

export const prefs = $state({
  pins: hasStorage ? read(KEYS.pins, [] as PageRef[], isRefs) : [],
  recent: hasStorage ? read(KEYS.recent, [] as PageRef[], isRefs) : [],
  /** The side bar hidden, to give the page the whole width. */
  sidebarHidden: hasStorage ? read(KEYS.sidebar, false, isBool) : false,
  /** The side bar's sections the user closed, by their key. */
  closed: hasStorage ? read(KEYS.closed, [] as string[], isStrings) : [],
  tree: (hasStorage ? read(KEYS.tree, 'pages' as TreeMode, isMode) : 'pages') as TreeMode,
  /** The tree's branches opened, by their key. */
  treeOpen: hasStorage ? read(KEYS.treeOpen, [] as string[], isStrings) : []
});

if (typeof window !== 'undefined') {
  window.addEventListener('storage', (e) => {
    if (e.key === KEYS.pins) prefs.pins = read(KEYS.pins, [], isRefs);
    else if (e.key === KEYS.recent) prefs.recent = read(KEYS.recent, [], isRefs);
    else if (e.key === KEYS.sidebar) prefs.sidebarHidden = read(KEYS.sidebar, false, isBool);
    else if (e.key === KEYS.closed) prefs.closed = read(KEYS.closed, [], isStrings);
  });
}

export const isPinned = (href: string) => prefs.pins.some((p) => p.href === href);

export function togglePin(ref: PageRef) {
  prefs.pins = isPinned(ref.href)
    ? prefs.pins.filter((p) => p.href !== ref.href)
    : [...prefs.pins, ref];
  write(KEYS.pins, prefs.pins);
}

export function unpin(href: string) {
  prefs.pins = prefs.pins.filter((p) => p.href !== href);
  write(KEYS.pins, prefs.pins);
}

/** The pins in a new order: the one at `from` put at `to`. */
export function movePin(from: number, to: number) {
  prefs.pins = moved(prefs.pins, from, to);
  write(KEYS.pins, prefs.pins);
}

/** A page visited: first in the recent list, once. */
export function visited(ref: PageRef) {
  prefs.recent = [ref, ...prefs.recent.filter((p) => p.href !== ref.href)].slice(0, RECENT_MAX);
  write(KEYS.recent, prefs.recent);
}

export function setSidebarHidden(hidden: boolean) {
  prefs.sidebarHidden = hidden;
  write(KEYS.sidebar, hidden);
}

export function setTree(mode: TreeMode) {
  prefs.tree = mode;
  write(KEYS.tree, mode);
}

export function setBranchOpen(key: string, open: boolean) {
  const rest = prefs.treeOpen.filter((k) => k !== key);
  // The newest few hundred: a long session doesn't grow it without end.
  prefs.treeOpen = open ? [...rest, key].slice(-300) : rest;
  write(KEYS.treeOpen, prefs.treeOpen);
}

export function setSectionOpen(key: string, open: boolean) {
  const rest = prefs.closed.filter((k) => k !== key);
  prefs.closed = open ? rest : [...rest, key];
  write(KEYS.closed, prefs.closed);
}

/** `list` with the item at `from` moved to `to`; the same list if either is out of range. */
export function moved<T>(list: T[], from: number, to: number): T[] {
  if (from === to || from < 0 || to < 0 || from >= list.length || to >= list.length) return list;
  const next = [...list];
  const [item] = next.splice(from, 1);
  next.splice(to, 0, item);
  return next;
}

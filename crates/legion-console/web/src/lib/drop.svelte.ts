// Files dragged onto the console. `droppable` makes an element take them:
// a textarea reads a dropped text file into itself; anything else hands the
// files to the file input inside it, as if chosen there, so the page's own
// `bind:files` and checks see them. A file dropped anywhere else that the
// console can open goes to the page for it (`routeDrop`): a compose file to
// Import compose, other YAML or JSON to Apply YAML.
import { tick } from 'svelte';
import type { Action } from 'svelte/action';

/** The largest text file read into an editor. */
export const TEXT_MAX = 2 * 1024 * 1024;

/** Text dropped outside any zone, waiting for the page it was sent to. */
export const dropped = $state({ apply: '', compose: '' });

/** The text waiting for `page`, taken: it is opened once. */
export function takeDropped(page: 'apply' | 'compose'): string {
  const text = dropped[page];
  dropped[page] = '';
  return text;
}

const hasFiles = (e: DragEvent) => !!e.dataTransfer && [...e.dataTransfer.types].includes('Files');

/** Whether `file` is one `accept` (an input's attribute) allows. */
export function accepted(file: File, accept: string): boolean {
  const wanted = accept
    .split(',')
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);
  if (!wanted.length) return true;
  const name = file.name.toLowerCase();
  const type = file.type.toLowerCase();
  return wanted.some((w) =>
    w.startsWith('.')
      ? name.endsWith(w)
      : w.endsWith('/*')
        ? type.startsWith(w.slice(0, -1))
        : type === w
  );
}

/** Where a file dropped outside any zone opens, if anywhere. */
export function routeFor(name: string): 'compose' | 'apply' | null {
  const n = name.toLowerCase();
  if (/^(docker-)?compose(\.[\w-]+)?\.ya?ml$/.test(n)) return 'compose';
  if (/\.(ya?ml|json)$/.test(n)) return 'apply';
  return null;
}

function flash(node: HTMLElement, cls: string) {
  node.classList.add(cls);
  setTimeout(() => node.classList.remove(cls), 1200);
}

/**
 * Takes files dropped on `node`. While a file is over it, `node` has the
 * class `drop-over`; a file it cannot take makes it `drop-refused` a moment.
 */
export const droppable: Action<HTMLElement> = (node) => {
  let depth = 0;
  const over = (on: boolean) => node.classList.toggle('drop-over', on);

  function enter(e: DragEvent) {
    if (!hasFiles(e)) return;
    e.preventDefault();
    depth++;
    over(true);
  }
  function leave(e: DragEvent) {
    if (!hasFiles(e)) return;
    depth = Math.max(0, depth - 1);
    if (!depth) over(false);
  }
  function dragover(e: DragEvent) {
    if (!hasFiles(e)) return;
    e.preventDefault();
    e.stopPropagation();
    if (e.dataTransfer) e.dataTransfer.dropEffect = 'copy';
  }
  async function drop(e: DragEvent) {
    if (!hasFiles(e)) return;
    e.preventDefault();
    e.stopPropagation();
    depth = 0;
    over(false);
    const files = e.dataTransfer?.files;
    if (!files?.length) return;
    if (node instanceof HTMLTextAreaElement) {
      const file = files[0];
      if (node.disabled || node.readOnly || file.size > TEXT_MAX) return flash(node, 'drop-refused');
      node.value = await file.text();
      node.dispatchEvent(new Event('input', { bubbles: true }));
      node.focus();
      return;
    }
    const input = node.querySelector<HTMLInputElement>('input[type="file"]');
    if (!input || input.disabled) return flash(node, 'drop-refused');
    const take = input.multiple ? [...files] : [files[0]];
    if (!take.every((f) => accepted(f, input.accept))) return flash(node, 'drop-refused');
    const list = new DataTransfer();
    take.forEach((f) => list.items.add(f));
    input.files = list.files;
    input.dispatchEvent(new Event('input', { bubbles: true }));
    input.dispatchEvent(new Event('change', { bubbles: true }));
    flash(node, 'drop-taken');
  }

  node.addEventListener('dragenter', enter);
  node.addEventListener('dragleave', leave);
  node.addEventListener('dragover', dragover);
  node.addEventListener('drop', drop);
  return {
    destroy() {
      node.removeEventListener('dragenter', enter);
      node.removeEventListener('dragleave', leave);
      node.removeEventListener('dragover', dragover);
      node.removeEventListener('drop', drop);
    }
  };
};

/**
 * A file dropped outside any zone: its text kept for the page it opens on,
 * and that page's path; null for a file the console cannot open (the drop
 * is then left to the browser's refusal, as nothing takes it).
 */
export async function routeDrop(file: File): Promise<string | null> {
  const to = routeFor(file.name);
  if (!to || file.size > TEXT_MAX) return null;
  dropped[to] = await file.text();
  return to === 'compose' ? '/import' : '/apply';
}

export interface SortParams {
  /** Which list the item is in: items move only within their own. */
  group: string;
  /** The item's place in its list. */
  index: number;
  /** How many items the list has. */
  count: number;
  /** Puts the item at `from` at `to`. */
  move: (from: number, to: number) => void;
}

const SORT_TYPE = 'application/x-legion-sort';
/** The item being dragged, as `group:index`: a drag's data is unreadable before the drop. */
let dragging = '';

/**
 * An item of a list the user orders: dragged onto another item's place, or
 * moved one place by Alt+Up and Alt+Down (Left and Right too, for a row of
 * tabs). While an item is dragged over, it has the class `sort-over`; the
 * one dragged, `sort-dragging`.
 */
export const sortable: Action<HTMLElement, SortParams> = (node, initial) => {
  let p = initial;
  node.draggable = true;

  const mine = (e: DragEvent) =>
    !!e.dataTransfer && [...e.dataTransfer.types].includes(SORT_TYPE) && dragging.startsWith(`${p.group}:`);

  function start(e: DragEvent) {
    if (!e.dataTransfer) return;
    dragging = `${p.group}:${p.index}`;
    e.dataTransfer.effectAllowed = 'move';
    e.dataTransfer.setData(SORT_TYPE, dragging);
    node.classList.add('sort-dragging');
  }
  function end() {
    dragging = '';
    node.classList.remove('sort-dragging');
  }
  function over(e: DragEvent) {
    if (!mine(e)) return;
    e.preventDefault();
    if (e.dataTransfer) e.dataTransfer.dropEffect = 'move';
    node.classList.add('sort-over');
  }
  function leave() {
    node.classList.remove('sort-over');
  }
  function drop(e: DragEvent) {
    node.classList.remove('sort-over');
    if (!mine(e)) return;
    e.preventDefault();
    const from = Number(dragging.slice(p.group.length + 1));
    dragging = '';
    if (Number.isInteger(from) && from !== p.index) p.move(from, p.index);
  }
  function key(e: KeyboardEvent) {
    if (!e.altKey || e.ctrlKey || e.metaKey) return;
    const step =
      e.key === 'ArrowUp' || e.key === 'ArrowLeft' ? -1 : e.key === 'ArrowDown' || e.key === 'ArrowRight' ? 1 : 0;
    const to = p.index + step;
    if (!step || to < 0 || to >= p.count) return;
    e.preventDefault();
    e.stopPropagation();
    p.move(p.index, to);
    // Moved in the page, the item may lose the focus: given it back.
    tick().then(() => node.focus());
  }

  node.addEventListener('dragstart', start);
  node.addEventListener('dragend', end);
  node.addEventListener('dragover', over);
  node.addEventListener('dragleave', leave);
  node.addEventListener('drop', drop);
  node.addEventListener('keydown', key);
  return {
    update(next) {
      p = next;
    },
    destroy() {
      node.removeEventListener('dragstart', start);
      node.removeEventListener('dragend', end);
      node.removeEventListener('dragover', over);
      node.removeEventListener('dragleave', leave);
      node.removeEventListener('drop', drop);
      node.removeEventListener('keydown', key);
    }
  };
};

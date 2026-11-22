// Every list in the console sorted, searched and paged the same way
// (TODO.MD item 6, one component kit): a table's rows, the columns it can
// sort on, and the text a search looks in; the page's rows to draw. Drawn
// with SortTh (a header that sorts), TableBar (the search and the count)
// and Pager (the pages, past one).

const collator = new Intl.Collator(undefined, { numeric: true, sensitivity: 'base' });

export type Direction = 'ascending' | 'descending';
export type Key<T> = (row: T) => string | number | undefined | null;

export interface TableOptions<T> {
  /** The columns a header can sort on, by name. */
  sorts?: Record<string, Key<T>>;
  /** The column sorted on first, and which way; none keeps the rows' order. */
  sortBy?: string;
  dir?: Direction;
  /** The text a search looks in; none: the sort columns' values. */
  text?: (row: T) => string;
  /** Rows a page; 0 for one page however long. */
  size?: number;
}

export class Table<T> {
  query = $state('');
  sortBy = $state('');
  dir: Direction = $state('ascending');
  page = $state(0);
  /** Rows a page; 0 for one page. */
  size = $state(50);
  #rows: () => T[];
  #opts: TableOptions<T>;

  constructor(rows: () => T[], opts: TableOptions<T> = {}) {
    this.#rows = rows;
    this.#opts = opts;
    this.sortBy = opts.sortBy ?? '';
    this.dir = opts.dir ?? 'ascending';
    this.size = opts.size ?? 50;
  }

  #text(row: T): string {
    if (this.#opts.text) return this.#opts.text(row);
    return Object.values(this.#opts.sorts ?? {})
      .map((k) => String(k(row) ?? ''))
      .join(' ');
  }

  /** The rows the search keeps. */
  readonly found = $derived.by(() => {
    const rows = this.#rows();
    const words = this.query.trim().toLowerCase().split(/\s+/).filter(Boolean);
    if (!words.length) return rows;
    return rows.filter((r) => {
      const text = this.#text(r).toLowerCase();
      return words.every((w) => text.includes(w));
    });
  });

  /** Those rows in the order asked for. */
  readonly sorted = $derived.by(() => {
    const key = this.#opts.sorts?.[this.sortBy];
    if (!key) return this.found;
    const sign = this.dir === 'ascending' ? 1 : -1;
    return [...this.found].sort((a, b) => {
      const x = key(a);
      const y = key(b);
      if (typeof x === 'number' && typeof y === 'number') return sign * (x - y);
      return sign * collator.compare(String(x ?? ''), String(y ?? ''));
    });
  });

  readonly pages = $derived(this.size ? Math.max(1, Math.ceil(this.sorted.length / this.size)) : 1);

  /** The page's rows; a page past the last (the rows having gone) shows the last. */
  readonly rows = $derived.by(() => {
    if (!this.size) return this.sorted;
    const at = Math.min(this.page, this.pages - 1);
    return this.sorted.slice(at * this.size, (at + 1) * this.size);
  });

  /** The page shown, counted from 0. */
  readonly at = $derived(Math.min(this.page, this.pages - 1));

  /** How many there are, and how many the search keeps. */
  get total(): number {
    return this.#rows().length;
  }

  /** Sorts on `col`; again, the other way. */
  sort(col: string): void {
    if (this.sortBy === col) this.dir = this.dir === 'ascending' ? 'descending' : 'ascending';
    else {
      this.sortBy = col;
      this.dir = 'ascending';
    }
    this.page = 0;
  }

  /** A header's aria-sort. */
  sorting(col: string): Direction | undefined {
    return this.sortBy === col ? this.dir : undefined;
  }

  search(q: string): void {
    this.query = q;
    this.page = 0;
  }
}

// The lists' table (lib/table.svelte.ts): searched, sorted and paged as
// one, without a browser: `npm test`.
import { describe, expect, it } from 'vitest';
import { Table } from './table.svelte.ts';

type Row = { name: string; cpu: number };

const rows: Row[] = [
  { name: 'web-10', cpu: 5 },
  { name: 'web-2', cpu: 50 },
  { name: 'db', cpu: 20 },
  { name: 'cache', cpu: 1 }
];

function table(size = 50) {
  let data = $state(rows);
  const t = new Table(() => data, {
    sorts: { name: (r) => r.name, cpu: (r) => r.cpu },
    sortBy: 'name',
    size
  });
  return { t, set: (r: Row[]) => (data = r) };
}

describe('a table', () => {
  it('sorts names as people read numbers in them, and again the other way', () => {
    const { t } = table();
    expect(t.rows.map((r) => r.name)).toEqual(['cache', 'db', 'web-2', 'web-10']);
    t.sort('name');
    expect(t.rows.map((r) => r.name)).toEqual(['web-10', 'web-2', 'db', 'cache']);
    t.sort('cpu');
    expect(t.rows.map((r) => r.cpu)).toEqual([1, 5, 20, 50]);
    expect(t.sorting('cpu')).toBe('ascending');
    expect(t.sorting('name')).toBeUndefined();
  });

  it('searches the sort columns for every word', () => {
    const { t } = table();
    t.search('web');
    expect(t.rows.map((r) => r.name)).toEqual(['web-2', 'web-10']);
    t.search('web 50');
    expect(t.rows.map((r) => r.name)).toEqual(['web-2']);
    expect(t.total).toBe(4);
  });

  it('pages, a search going back to the first page', () => {
    const { t } = table(3);
    expect(t.pages).toBe(2);
    t.page = 1;
    expect(t.rows.map((r) => r.name)).toEqual(['web-10']);
    t.search('e');
    expect(t.page).toBe(0);
  });

  it('shows the last page when the rows it was on have gone', () => {
    const { t, set } = table(2);
    t.page = 1;
    set(rows.slice(0, 2));
    expect(t.at).toBe(0);
    expect(t.rows).toHaveLength(2);
  });
});

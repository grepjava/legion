// Choices a person makes on a page (the namespace looked at, a tab, a
// filter) kept in the address as `?key=value`, so a reload, a bookmark or
// a shared link opens the page as it was. A page reads its own back with
// `fromAddress` when it opens.

import { untrack } from 'svelte';
import { goto } from '$app/navigation';
import { page } from '$app/state';

// What is to change in the address, written at once: two choices made in
// the same moment must not each write the address as it was before the
// other.
const wanted = new Map<string, string | null>();
let chain: Promise<void> = Promise.resolve();

async function apply(): Promise<void> {
  if (wanted.size === 0) return;
  const url = new URL(location.href);
  for (const [key, value] of wanted) {
    if (value === null) url.searchParams.delete(key);
    else url.searchParams.set(key, value);
  }
  wanted.clear();
  if (url.href !== location.href) {
    // Shallow: the address changes, the page is not loaded again.
    await goto(url, { shallow: true, replace: true, state: page.state });
  }
}

/**
 * Keeps `value()` in the address as `?key=`, left out while it is `none`,
 * for as long as the calling component lives; again after each
 * navigation, which drops what the link did not carry.
 */
export function keepInAddress(key: string, value: () => string, none = ''): void {
  $effect(() => {
    const v = value();
    void page.url.pathname;
    untrack(() => {
      const have = new URL(location.href).searchParams.get(key) ?? none;
      if (have === v) return;
      wanted.set(key, v === none ? null : v);
      chain = chain.then(apply).catch(() => {});
    });
  });
}

/** What the address says for `key` now, without following it. */
export function fromAddress(key: string): string | null {
  return untrack(() => page.url.searchParams.get(key));
}

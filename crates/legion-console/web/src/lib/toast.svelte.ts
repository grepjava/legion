// What was done, said in a corner (Toasts.svelte, in the layout): a result
// goes after a few seconds, an error stays until it is closed. Kept short:
// the newest few, so a burst of actions does not cover the page.

export interface Toast {
  id: number;
  text: string;
  error: boolean;
}

import { noteDone } from './activity.svelte.ts';

export const toasts = $state<{ list: Toast[] }>({ list: [] });

const KEPT = 4;
const SHOWN_MS = 6000;
let next = 1;

export function toast(text: string, error = false): void {
  if (!text) return;
  // Kept for the workbench's bar and its Activity tab too.
  noteDone(text, error);
  const id = next++;
  toasts.list = [...toasts.list, { id, text, error }].slice(-KEPT);
  if (!error) setTimeout(() => dismiss(id), SHOWN_MS);
}

export function dismiss(id: number): void {
  toasts.list = toasts.list.filter((t) => t.id !== id);
}

/**
 * Runs `what` with the button that started it said busy (aria-busy, which
 * app.css draws as a spinner): the focused element when it is a button, as
 * it is after a click, and again once a confirmation has closed.
 */
export async function busyWhile<T>(what: () => Promise<T>): Promise<T> {
  const el = document.activeElement;
  const button = el instanceof HTMLButtonElement ? el : null;
  button?.setAttribute('aria-busy', 'true');
  try {
    return await what();
  } finally {
    button?.removeAttribute('aria-busy');
  }
}

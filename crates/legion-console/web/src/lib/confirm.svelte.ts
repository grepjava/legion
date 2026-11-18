// The console's own confirmation, in place of the browser's confirm(): one
// dialog (Confirm.svelte, in the layout) that says what will happen, on a
// button naming the action, and for what can't be undone asks for the
// object's name typed. `ask` resolves to whether it was confirmed.

export interface Ask {
  /** What will happen, as a question: "Drain n2? Its tasks move…". */
  message: string;
  /** The button's word; by default the message's first word ("Drain"). */
  action?: string;
  /** Drawn as danger; by default when the action destroys or stops. */
  danger?: boolean;
  /** A name to type before the button works, for what can't be undone. */
  typed?: string;
  /** Said under the message, e.g. a dry run's findings (phase 11). */
  detail?: string[];
}

interface Pending extends Ask {
  resolve: (ok: boolean) => void;
}

export const confirming = $state<{ now: Pending | null }>({ now: null });

const DANGER = /^(delete|remove|destroy|prune|drain|stop|pause|replace|rotate|roll|restore|evict|wipe)/i;

/** The action's word for its button, from the message's first word. */
export function actionOf(message: string): string {
  const first = /^[\p{L}-]+/u.exec(message.trim())?.[0] ?? '';
  return first || 'OK';
}

export function ask(what: string | Ask, more: Partial<Ask> = {}): Promise<boolean> {
  const a: Ask = typeof what === 'string' ? { message: what, ...more } : { ...what, ...more };
  const action = a.action ?? actionOf(a.message);
  // A second ask while one is open refuses the first: one question at a time.
  confirming.now?.resolve(false);
  return new Promise((resolve) => {
    confirming.now = {
      ...a,
      action,
      danger: a.danger ?? DANGER.test(action),
      resolve: (ok) => {
        confirming.now = null;
        resolve(ok);
      }
    };
  });
}

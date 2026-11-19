// What runs and what was done (CONSOLE.md phase 10): the cluster's long
// operations as the leader follows them, kept current while signed in,
// and what was done in this console, each action's answer as its toast
// said it. The workbench's bar says the newest of either; its Activity tab
// lists them all.
import type { Operation } from './api.ts';

export interface Done {
  id: number;
  atMs: number;
  text: string;
  error: boolean;
}

/** Actions kept of this console's, newest last. */
const KEPT = 50;

export const activity = $state({
  ops: [] as Operation[],
  /** Why the operations could not be followed, if they could not. */
  error: '',
  /** What was done in this console, oldest first. */
  done: [] as Done[]
});

let next = 1;

/** Notes an action's answer, as its toast says it. */
export function noteDone(text: string, error: boolean): void {
  activity.done = [...activity.done, { id: next++, atMs: Date.now(), text, error }].slice(-KEPT);
}

export function running(ops: Operation[]): Operation[] {
  return ops.filter((o) => o.state === 'running');
}

/** The stages a move goes through, in turn. */
export const MOVE_STAGES = ['preparing', 'full-copy', 'stopping', 'switch-over', 'cleanup'];

/** Bytes in binary units, as the CLI says them. */
export function sent(n: number): string {
  if (!n) return '';
  const units = ['KiB', 'MiB', 'GiB', 'TiB', 'PiB'];
  if (n < 1024) return `${n} B`;
  let v = n / 1024;
  let unit = units[0];
  for (const u of units.slice(1)) {
    if (v < 1024) break;
    v /= 1024;
    unit = u;
  }
  return `${v.toFixed(1)} ${unit}`;
}

/** How long, as `1h02m`, `3m05s` or `12s`. */
export function took(ms: number): string {
  const s = Math.max(0, Math.floor(ms / 1000));
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const r = s % 60;
  if (h) return `${h}h${String(m).padStart(2, '0')}m`;
  if (m) return `${m}m${String(r).padStart(2, '0')}s`;
  return `${r}s`;
}

/** The whole of what an operation said, as text to copy. */
export function outputText(op: Operation): string {
  const head = [
    op.what,
    `${op.state} · ${op.stage}`,
    op.by ? `by ${op.by}` : '',
    `began ${new Date(op.beganMs).toISOString()}`,
    op.endedMs ? `ended ${new Date(op.endedMs).toISOString()}` : ''
  ].filter(Boolean);
  const lines = (op.output ?? []).map((l) => `${new Date(l.atMs).toISOString()}  ${l.text}`);
  return [...head, '', ...lines].join('\n') + '\n';
}

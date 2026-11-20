// Lengths of time and moments, said as briefly as their size allows: the
// charts' spans and steps, and their times, with the day once a chart
// spans more than one.
import { t } from '#lib/i18n.svelte.ts';

const MIN = 60_000;
const HOUR = 60 * MIN;
const DAY = 24 * HOUR;

/** `15 s`, `45 min`, `1.5 h`, `7 days`. */
export function duration(ms: number): string {
  if (ms < MIN) return t('{n} s', { n: Math.round(ms / 1000) });
  if (ms < HOUR) return t('{n} min', { n: Math.round(ms / MIN) });
  if (ms < 2 * DAY) return t('{n} h', { n: Math.round((ms / HOUR) * 10) / 10 });
  return t('{n} days', { n: Math.round(ms / DAY) });
}

/** A moment on a chart spanning `span`: its time, and its day past a day. */
export function stamp(tm: number, span: number): string {
  const d = new Date(tm);
  return span > DAY
    ? d.toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
    : d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

/** The ranges a chart can show, in milliseconds. */
export const RANGES = [HOUR, 6 * HOUR, DAY, 7 * DAY, 30 * DAY];

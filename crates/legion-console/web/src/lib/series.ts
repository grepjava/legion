// Summing series sampled apart: tasks and nodes are sampled on their own
// clocks, so one's sample can fall in the step before or after another's.
// Rounding each to its step and adding what lands together counts some
// series twice in one step and not at all in the next: the sum dips and
// jumps. Instead, each step adds every series' latest sample at or before
// it, held for up to `hold` steps, so a series counts once in every step it
// was being sampled.

/** [milliseconds since the epoch, value], any order. */
export type Samples = [number, number][];

export function sumHeld(series: Samples[], step: number, hold = 2): [number, number][] {
  const sorted = series
    .map((s) => [...s].sort((a, b) => a[0] - b[0]))
    .filter((s) => s.length);
  const keys = [...new Set(sorted.flatMap((s) => s.map(([at]) => Math.round(at / step) * step)))].sort(
    (a, b) => a - b
  );
  const out: [number, number][] = keys.map((k) => [k, 0]);
  for (const s of sorted) {
    let i = 0;
    for (const row of out) {
      const k = row[0];
      // The latest sample that rounds to this step or an earlier one.
      while (i + 1 < s.length && Math.round(s[i + 1][0] / step) * step <= k) i++;
      const [at, v] = s[i];
      const ak = Math.round(at / step) * step;
      if (ak <= k && k - ak <= hold * step) row[1] += v;
    }
  }
  return out;
}

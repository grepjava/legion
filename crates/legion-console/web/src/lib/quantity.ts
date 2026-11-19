// Quantities as the API writes them (legion-api's quantity.rs): CPU as
// cores or millicores (`8`, `500m`), bytes with a binary or decimal suffix
// (`7683460Ki`, `1G`) or none.

const UNITS: Record<string, number> = {
  Ki: 2 ** 10,
  Mi: 2 ** 20,
  Gi: 2 ** 30,
  Ti: 2 ** 40,
  Pi: 2 ** 50,
  k: 1e3,
  K: 1e3,
  M: 1e6,
  G: 1e9,
  T: 1e12,
  P: 1e15
};

/** Cores in `q`; 0 for what isn't a CPU quantity. */
export function cores(q: unknown): number {
  const s = String(q ?? '').trim();
  const m = /^(\d+(?:\.\d+)?)(m?)$/.exec(s);
  if (!m) return 0;
  return m[2] ? Number(m[1]) / 1000 : Number(m[1]);
}

/** Bytes in `q`; 0 for what isn't a byte quantity. */
export function byteCount(q: unknown): number {
  const s = String(q ?? '').trim();
  const m = /^(\d+(?:\.\d+)?)([KMGTP]i|[kKMGTP])?$/.exec(s);
  if (!m) return 0;
  return Number(m[1]) * (m[2] ? UNITS[m[2]] : 1);
}

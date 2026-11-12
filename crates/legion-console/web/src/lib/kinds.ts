/** A kind's name for many of it: Services, NetworkPolicies. */
export function plural(kind: string): string {
  if (/[^aeiou]y$/.test(kind)) return kind.slice(0, -1) + 'ies';
  if (/(s|x|ch|sh)$/.test(kind)) return kind + 'es';
  return kind + 's';
}

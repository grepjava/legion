import type { KindInfo, Me } from './api';

/** Who is signed in, the kinds there are, and the namespace being looked at. */
export const session = $state({
  me: null as Me | null,
  kinds: [] as KindInfo[],
  /** Empty: every namespace the user may read. */
  namespace: ''
});

/** The namespaces the user has a role in; for the admin, those listed. */
export function namespaces(adminList: string[]): string[] {
  const me = session.me;
  if (!me) return [];
  return me.all ? adminList : Object.keys(me.namespaces).sort();
}

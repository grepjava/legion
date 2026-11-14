import type { Alert, ConsoleSettings, KindInfo, Me } from './api';

/** Who is signed in, the kinds there are, and the namespace being looked at. */
export const session = $state({
  me: null as Me | null,
  kinds: [] as KindInfo[],
  /** Empty: every namespace the user may read. */
  namespace: '',
  /** The user's alerts, kept current while signed in. */
  alerts: [] as Alert[],
  /** Why the alerts could not be followed, if they could not. */
  alertsError: '',
  /** The console's settings: the banner alone until signed in. */
  settings: {} as ConsoleSettings
});

/** The namespaces the user has a role in; for the admin, those listed. */
export function namespaces(adminList: string[]): string[] {
  const me = session.me;
  if (!me) return [];
  return me.all ? adminList : Object.keys(me.namespaces).sort();
}

/**
 * Why the user may not change objects in `namespace` (empty for the
 * cluster-wide kinds), or null when they may: a `view` user sees the
 * buttons, disabled with this as the reason. The API decides in the end.
 */
export function cannotWrite(namespace: string): string | null {
  const me = session.me;
  if (!me) return 'not signed in';
  if (me.all) return null;
  if (!namespace) return 'only an admin changes what is cluster-wide';
  const role = me.namespaces[namespace];
  if (role === 'edit' || role === 'admin') return null;
  return role ? `your role in ${namespace} is ${role}` : `you have no role in ${namespace}`;
}

/** Why the user may write in no namespace at all, as for a page whose
 * namespace is in what it sends (Apply YAML); null if they may somewhere. */
export function cannotWriteAnywhere(): string | null {
  const me = session.me;
  if (!me) return 'not signed in';
  if (me.all) return null;
  const roles = Object.values(me.namespaces);
  if (roles.some((r) => r === 'edit' || r === 'admin')) return null;
  return roles.length ? 'your roles let you look, not change' : 'you have no role in any namespace';
}

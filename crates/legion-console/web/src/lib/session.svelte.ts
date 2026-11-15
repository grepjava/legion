import type { Alert, ConsoleSettings, KindInfo, LObject, Me, Plugin } from './api';
import { t } from './i18n.svelte.ts';

export type ThemeMode = 'dark' | 'light' | 'system';
export type DensityMode = 'comfortable' | 'compact';

export interface WorkbenchTab {
  id: string;
  kind: 'logs' | 'terminal' | 'build' | 'events';
  title: string;
  namespace: string;
  name: string;
  targetKind?: string;
  object?: LObject;
}

function initialTheme(): ThemeMode {
  try {
    const saved = localStorage.getItem('color-scheme');
    if (saved === 'light' || saved === 'dark' || saved === 'system') return saved;
  } catch {
    // Storage unavailable.
  }
  return 'dark';
}

function initialDensity(): DensityMode {
  try {
    if (localStorage.getItem('legion.density') === 'compact') return 'compact';
  } catch {
    // Storage unavailable.
  }
  return 'comfortable';
}

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
  settings: {} as ConsoleSettings,
  /** Plugins' pages in the namespaces the user reads (CONSOLE.md phase 6). */
  plugins: [] as Plugin[],
  /** UI color scheme override (`dark`, `light`, or `system`). */
  theme: initialTheme(),
  /** UI layout density (`comfortable` or `compact`). */
  density: initialDensity(),
  /** Docked bottom workbench tabs (logs, terminal shells, builds, events). */
  workbench: [] as WorkbenchTab[],
  activeTab: '',
  workbenchMinimized: false,
  paletteOpen: false
});

function applyThemeToDom(mode: ThemeMode) {
  if (typeof document === 'undefined') return;
  const meta = document.querySelector<HTMLMetaElement>('meta[name="color-scheme"]');
  if (meta) meta.content = mode === 'system' ? 'light dark' : mode;
}

function applyDensityToDom(density: DensityMode) {
  if (typeof document === 'undefined') return;
  if (density === 'compact') document.documentElement.dataset.density = 'compact';
  else delete document.documentElement.dataset.density;
}

if (typeof window !== 'undefined') {
  applyThemeToDom(session.theme);
  applyDensityToDom(session.density);
  window.addEventListener('storage', (e) => {
    if (e.key === 'color-scheme') {
      const next: ThemeMode =
        e.newValue === 'light' || e.newValue === 'dark' || e.newValue === 'system'
          ? e.newValue
          : 'dark';
      session.theme = next;
      applyThemeToDom(next);
    } else if (e.key === 'legion.density') {
      const next: DensityMode = e.newValue === 'compact' ? 'compact' : 'comfortable';
      session.density = next;
      applyDensityToDom(next);
    }
  });
}

export function setTheme(mode: ThemeMode) {
  session.theme = mode;
  applyThemeToDom(mode);
  try {
    localStorage.setItem('color-scheme', mode);
  } catch {
    // Kept for this page only.
  }
}

export function setDensity(density: DensityMode) {
  session.density = density;
  applyDensityToDom(density);
  try {
    if (density === 'compact') localStorage.setItem('legion.density', 'compact');
    else localStorage.removeItem('legion.density');
  } catch {
    // Kept for this page only.
  }
}

export function openWorkbench(tab: WorkbenchTab) {
  const existing = session.workbench.find((x) => x.id === tab.id);
  if (!existing) {
    session.workbench = [...session.workbench, tab];
  } else if (tab.object) {
    existing.object = tab.object;
  }
  session.activeTab = tab.id;
  session.workbenchMinimized = false;
}

export function closeWorkbench(id: string) {
  const idx = session.workbench.findIndex((x) => x.id === id);
  if (idx < 0) return;
  const next = session.workbench.filter((x) => x.id !== id);
  session.workbench = next;
  if (session.activeTab === id) {
    session.activeTab = next[Math.min(idx, next.length - 1)]?.id ?? '';
  }
}

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
  if (!me) return t('not signed in');
  if (me.all) return null;
  if (!namespace) return t('only an admin changes what is cluster-wide');
  const role = me.namespaces[namespace];
  if (role === 'edit' || role === 'admin') return null;
  return role
    ? t('your role in {namespace} is {role}', { namespace, role })
    : t('you have no role in {namespace}', { namespace });
}

/** Why the user may write in no namespace at all, as for a page whose
 * namespace is in what it sends (Apply YAML); null if they may somewhere. */
export function cannotWriteAnywhere(): string | null {
  const me = session.me;
  if (!me) return t('not signed in');
  if (me.all) return null;
  const roles = Object.values(me.namespaces);
  if (roles.some((r) => r === 'edit' || r === 'admin')) return null;
  return roles.length ? t('your roles let you look, not change') : t('you have no role in any namespace');
}

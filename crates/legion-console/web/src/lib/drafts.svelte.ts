// Forms left half filled keep what was typed for this browser's session,
// so going to look at something else loses nothing; the side bar marks a
// page that holds one. Kept in sessionStorage: gone with the tab, never
// sent anywhere.

const KEY = 'legion-draft:';

/** The pages that hold a draft now, by name. */
export const drafts = $state<Record<string, boolean>>({});

for (let i = 0; typeof sessionStorage !== 'undefined' && i < sessionStorage.length; i++) {
  const k = sessionStorage.key(i);
  if (k?.startsWith(KEY)) drafts[k.slice(KEY.length)] = true;
}

export function loadDraft<T>(name: string): T | null {
  try {
    const raw = sessionStorage.getItem(KEY + name);
    return raw ? (JSON.parse(raw) as T) : null;
  } catch {
    return null;
  }
}

/** Keeps `value` as `name`'s draft, or forgets it when it is as `blank`. */
export function saveDraft(name: string, value: unknown, blank: unknown): void {
  const raw = JSON.stringify(value);
  try {
    if (raw === JSON.stringify(blank)) {
      sessionStorage.removeItem(KEY + name);
      drafts[name] = false;
    } else {
      sessionStorage.setItem(KEY + name, raw);
      drafts[name] = true;
    }
  } catch {
    // Storage refused (private mode, full): the form works without it.
  }
}

export function dropDraft(name: string): void {
  try {
    sessionStorage.removeItem(KEY + name);
  } catch {
    // As above.
  }
  drafts[name] = false;
}

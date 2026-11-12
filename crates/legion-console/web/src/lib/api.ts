// The gateway's API (DESIGN.md §3.9). Every call is made by the gateway as
// the signed-in user, so what comes back is what the CLI would show them.

export interface Meta {
  name: string;
  namespace?: string;
  uid: string;
  version: number;
  generation: number;
  labels?: Record<string, string>;
}

export interface LObject {
  apiVersion: string;
  kind: string;
  metadata: Meta;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  spec: any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  status: any;
}

export interface Me {
  name: string;
  role: 'admin' | 'user';
  all: boolean;
  namespaces: Record<string, string>;
}

export interface KindInfo {
  kind: string;
  clusterScoped: boolean;
}

export interface Status {
  node: string;
  name: string;
  leader: string | null;
  state: string;
  term: number;
  version: number;
  protocol: number;
  members: { id: string; addr: string; voter: boolean }[];
}

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string
  ) {
    super(message);
  }
}

/** Called when a request finds the session gone, to send the user to sign in. */
let onSignedOut: () => void = () => {};
export function whenSignedOut(f: () => void) {
  onSignedOut = f;
}

async function call<T>(method: string, path: string, body?: unknown): Promise<T> {
  const res = await fetch(path, {
    method,
    credentials: 'same-origin',
    headers: body === undefined ? {} : { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body)
  });
  if (res.status === 204) return undefined as T;
  const data = await res.json().catch(() => ({}));
  if (res.status === 401) onSignedOut();
  if (!res.ok) throw new ApiError(res.status, data.error ?? res.statusText);
  return data as T;
}

const q = (ns: string) => (ns ? `?namespace=${encodeURIComponent(ns)}` : '');

export const api = {
  login: (code: string) =>
    call<{ name: string; role: string; expires_ms: number }>('POST', '/api/login', { code }),
  logout: () => call<void>('POST', '/api/logout'),
  me: () => call<Me>('GET', '/api/me'),
  status: () => call<Status>('GET', '/api/status'),
  kinds: () => call<KindInfo[]>('GET', '/api/kinds'),
  list: (kind: string, namespace = '') =>
    call<{ objects: LObject[]; version: number }>(
      'GET',
      `/api/objects/${encodeURIComponent(kind)}${q(namespace)}`
    ),
  get: (kind: string, namespace: string, name: string) =>
    call<LObject>(
      'GET',
      `/api/objects/${encodeURIComponent(kind)}/${encodeURIComponent(namespace || '_')}/${encodeURIComponent(name)}`
    )
};

export function key(o: LObject): string {
  return `${o.metadata.namespace ?? ''}/${o.metadata.name}`;
}

/**
 * The objects of `kind` (in `namespace`, or every namespace the user may
 * read), kept current from the store's watch: `update` gets the whole list
 * after each change. Reconnects after a break. Returns a function that
 * stops it.
 */
export function follow(
  kind: string,
  namespace: string,
  update: (objects: LObject[]) => void,
  failed: (e: ApiError) => void
): () => void {
  let stopped = false;
  let socket: WebSocket | null = null;
  let retry: ReturnType<typeof setTimeout> | null = null;
  const objects = new Map<string, LObject>();
  const publish = () =>
    update(
      [...objects.values()].sort((a, b) => key(a).localeCompare(key(b)))
    );

  async function start() {
    if (stopped) return;
    try {
      const listed = await api.list(kind, namespace);
      objects.clear();
      for (const o of listed.objects) objects.set(key(o), o);
      publish();
      const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
      const params = new URLSearchParams({ from: String(listed.version) });
      if (namespace) params.set('namespace', namespace);
      socket = new WebSocket(
        `${proto}//${location.host}/api/watch/${encodeURIComponent(kind)}?${params}`
      );
      socket.onmessage = (m) => {
        const e = JSON.parse(m.data);
        if (e.error) return;
        if (e.object.kind !== kind) return;
        if (e.type === 'delete') objects.delete(key(e.object));
        else objects.set(key(e.object), e.object);
        publish();
      };
      socket.onclose = () => {
        if (!stopped) retry = setTimeout(start, 2000);
      };
    } catch (e) {
      if (e instanceof ApiError) {
        failed(e);
        if (e.status >= 500 && !stopped) retry = setTimeout(start, 3000);
      }
    }
  }
  start();
  return () => {
    stopped = true;
    if (retry) clearTimeout(retry);
    socket?.close();
  };
}

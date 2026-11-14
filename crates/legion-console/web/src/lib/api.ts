// The gateway's API (DESIGN.md §3.9). Every call is made by the gateway as
// the signed-in user, so what comes back is what the CLI would show them.
import { t } from '#lib/i18n.svelte.ts';

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
  /** The token every write carries (DESIGN.md §3.9); not sent to
   * another cluster's page. */
  csrf: string;
  /** This cluster's name, as its console settings give it. */
  cluster?: string;
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

/** The signed-in session's token for writes, from `/api/me`. */
let csrf = '';
export function setToken(token: string) {
  csrf = token;
}

async function call<T>(method: string, path: string, body?: unknown): Promise<T> {
  const headers: Record<string, string> = {};
  if (body !== undefined) headers['content-type'] = 'application/json';
  if (method !== 'GET' && csrf) headers['x-legion-csrf'] = csrf;
  const res = await fetch(path, {
    method,
    credentials: 'same-origin',
    headers,
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
    ),
  usage: (namespace = '') => call<{ nodes: NodeUsage[] }>('GET', `/api/usage${q(namespace)}`),
  /** What may reach what among a namespace's services, as `legion reach`. */
  reach: (namespace: string) =>
    call<{ services: string[]; edges: ReachEdge[] }>('GET', `/api/reach${q(namespace)}`),

  // Writes, as the user, each checked by the API as the CLI's are.
  manifest: (kind: string, namespace: string, name: string) =>
    call<{ yaml: string; version: number }>('GET', `/api/manifest/${path(kind, namespace, name)}`),
  apply: (body: {
    yaml?: string;
    object?: unknown;
    dryRun?: boolean;
    expectedVersion?: number;
  }) => call<{ results: ApplyResult[] }>('POST', '/api/apply', body),
  remove: (kind: string, namespace: string, name: string, version?: number) =>
    call<{ version: number }>(
      'DELETE',
      `/api/objects/${path(kind, namespace, name)}${version === undefined ? '' : `?version=${version}`}`
    ),
  set: (kind: string, namespace: string, name: string, field: string, value: unknown) =>
    call<{ key: string; changed: boolean }>('POST', '/api/set', {
      kind,
      namespace,
      name,
      field,
      value
    }),
  labels: (kind: string, namespace: string, name: string, changes: Record<string, string | null>) =>
    call<{ key: string; changed: boolean }>('POST', '/api/labels', {
      kind,
      namespace,
      name,
      changes
    }),
  rollback: (namespace: string, service: string) =>
    call<{ message: string }>('POST', '/api/rollback', { namespace, service }),
  nodeRole: (node: string, role: 'manager' | 'worker') =>
    call<void>('POST', '/api/node-role', { node, role }),

  // Cluster operations; the API lets only admins do most.
  audit: (filter: AuditFilter) => {
    const params = new URLSearchParams();
    for (const [k, v] of Object.entries(filter)) if (v !== undefined && v !== '') params.set(k, String(v));
    return call<{ events: AuditEvent[]; truncated: boolean }>('GET', `/api/audit?${params}`);
  },
  joinToken: (role: 'manager' | 'worker', rotate = false) =>
    call<{ token: string }>('POST', '/api/join-token', { role, rotate }),
  ca: () => call<CaStatus>('GET', '/api/ca'),
  rotateCa: () => call<CaStatus>('POST', '/api/ca'),
  autolock: (action: 'show' | 'enable' | 'disable' | 'rotate') =>
    call<{ key: string }>('POST', '/api/autolock', { action }),
  createUser: (user: { name: string; role?: string; namespace?: string; days?: number }) =>
    call<{ pem: string; binding: string | null; days: number }>('POST', '/api/users', user),
  /** Uploads a volume backup file to a Volume on `node`. */
  restoreVolume: async (
    file: File,
    to: { node: string; namespace?: string; name?: string; replace?: boolean }
  ) => {
    const params = new URLSearchParams({ node: to.node });
    if (to.namespace) params.set('namespace', to.namespace);
    if (to.name) params.set('name', to.name);
    if (to.replace) params.set('replace', 'true');
    const res = await fetch(`/api/volume-restore?${params}`, {
      method: 'POST',
      credentials: 'same-origin',
      headers: { 'x-legion-csrf': csrf, 'content-type': 'application/octet-stream' },
      body: file
    });
    const data = await res.json().catch(() => ({}));
    if (res.status === 401) onSignedOut();
    if (!res.ok) throw new ApiError(res.status, data.error ?? res.statusText);
    return data as { namespace: string; name: string; node: string; bytes: number; taken: number };
  }
};

/** Downloads, which the browser saves as files. */
export const downloads = {
  backup: '/api/backup',
  volume: (namespace: string, name: string) =>
    `/api/volume-backup/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}`
};

export interface AuditFilter {
  since_ms?: number;
  actor?: string;
  action?: string;
  namespace?: string;
  kind?: string;
  result?: '' | 'ok' | 'denied' | 'error';
  limit?: number;
}

export interface AuditEvent {
  timeMs: number;
  actor: { kind: string; name?: string };
  action: string;
  target?: { kind?: string; namespace?: string; name?: string };
  result: 'ok' | 'denied' | 'error';
  message?: string;
  manager: string;
}

export interface CaStatus {
  digest: string;
  /** While a rotation runs, the CA before it; empty after. */
  previous: string;
  since_ms: number;
  holders: { name: string; kind: string; issuer: string }[];
}

export interface ReachEdge {
  /** A service of the namespace; empty for other namespaces' tasks. */
  from: string;
  to: string;
  all_ports: boolean;
  ports: { protocol: string; port: number }[];
}

export interface ApplyResult {
  key: string;
  result: 'created' | 'updated' | 'unchanged' | 'applied';
  version: number;
  /** With a dry run: the stored object's manifest to what would be stored. */
  diff?: string;
}

function path(kind: string, namespace: string, name: string): string {
  return [kind, namespace || '_', name].map(encodeURIComponent).join('/');
}

/**
 * A command in a task's container under a terminal: `output` gets what it
 * writes, `ended` its exit code or why it failed. Returns what to send it.
 */
export function exec(
  namespace: string,
  task: string,
  opts: { container?: string; command?: string[]; cols: number; rows: number },
  output: (data: Uint8Array) => void,
  ended: (exit: number | null, error: string | null) => void
): { input: (data: string | Uint8Array) => void; resize: (cols: number, rows: number) => void; close: () => void } {
  const params = new URLSearchParams({ cols: String(opts.cols), rows: String(opts.rows), csrf });
  if (opts.container) params.set('container', opts.container);
  if (opts.command) params.set('command', JSON.stringify(opts.command));
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const socket = new WebSocket(
    `${proto}//${location.host}/api/exec/${encodeURIComponent(namespace)}/${encodeURIComponent(task)}?${params}`
  );
  socket.binaryType = 'arraybuffer';
  let done = false;
  const end = (exit: number | null, error: string | null) => {
    if (!done) ended(exit, error);
    done = true;
  };
  socket.onmessage = (m) => {
    if (m.data instanceof ArrayBuffer) return output(new Uint8Array(m.data));
    const e = JSON.parse(m.data);
    if (typeof e.exit === 'number') end(e.exit, null);
    else if (e.error) end(null, e.error);
  };
  socket.onclose = () => end(null, t('the terminal could not be opened, or its connection ended'));
  const encoder = new TextEncoder();
  const send = (data: string | ArrayBufferView) => {
    if (socket.readyState === WebSocket.OPEN) socket.send(data as never);
  };
  return {
    input: (data) => send(typeof data === 'string' ? encoder.encode(data) : data),
    resize: (cols, rows) => send(JSON.stringify({ resize: [cols, rows] })),
    close: () => {
      done = true;
      socket.close();
    }
  };
}

export interface NodeUsage {
  node: string;
  tasks: { namespace: string; task: string; cpu_percent: number; memory_bytes: number }[];
}

/**
 * A task's output as it comes: `out` gets each piece (and whether it was
 * stderr), `done` the end, with the reason when it failed. Returns a
 * function that stops it.
 */
export function logs(
  namespace: string,
  task: string,
  opts: { container?: string; tail?: number; follow?: boolean },
  out: (text: string, stderr: boolean) => void,
  done: (error: string | null) => void
): () => void {
  const params = new URLSearchParams();
  if (opts.container) params.set('container', opts.container);
  if (opts.tail !== undefined) params.set('tail', String(opts.tail));
  if (opts.follow) params.set('follow', 'true');
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const socket = new WebSocket(
    `${proto}//${location.host}/api/logs/${encodeURIComponent(namespace)}/${encodeURIComponent(task)}?${params}`
  );
  let ended = false;
  const end = (e: string | null) => {
    if (!ended) done(e);
    ended = true;
  };
  socket.onmessage = (m) => {
    const e = JSON.parse(m.data);
    if (e.out !== undefined) out(e.out, false);
    else if (e.err !== undefined) out(e.err, true);
    else if (e.error) end(e.error);
    else if (e.end) end(null);
  };
  // Refused before the upgrade (signed out, not allowed): no reason reaches
  // a WebSocket.
  socket.onclose = () => end(t('the logs could not be read'));
  return () => {
    ended = true;
    socket.close();
  };
}

/** A byte count as people read it: 1.5 GiB. */
export function bytes(n: number): string {
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let i = 0;
  while (n >= 1024 && i < units.length - 1) {
    n /= 1024;
    i++;
  }
  return `${i ? n.toFixed(1) : n} ${units[i]}`;
}

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

/** What an operator acts on now (`legion alerts`). */
export interface Alert {
  kind: string;
  severity: 'critical' | 'warning';
  namespace: string;
  /** `Kind/name`, or empty for the cluster itself. */
  object: string;
  message: string;
  since_ms: number;
}

/**
 * The user's alerts as they change: `update` gets the whole list each
 * time, `failed` why the stream broke. Reconnects after a break. Returns a
 * function that stops it.
 */
export function followAlerts(
  update: (alerts: Alert[]) => void,
  failed: (why: string) => void
): () => void {
  let stopped = false;
  let socket: WebSocket | null = null;
  let retry: ReturnType<typeof setTimeout> | null = null;
  function start() {
    if (stopped) return;
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    socket = new WebSocket(`${proto}//${location.host}/api/alerts`);
    socket.onmessage = (m) => {
      const e = JSON.parse(m.data);
      if (e.error) failed(e.error);
      else {
        failed('');
        update(e.alerts);
      }
    };
    socket.onclose = () => {
      if (!stopped) retry = setTimeout(start, 2000);
    };
  }
  start();
  return () => {
    stopped = true;
    if (retry) clearTimeout(retry);
    socket?.close();
  };
}

/** Where an alert's cause is in the console. */
export function alertHref(a: Alert): string {
  if (!a.object) return '/cluster';
  const [kind, name] = a.object.split('/', 2);
  return `/k/${encodeURIComponent(kind)}/${encodeURIComponent(a.namespace || '_')}/${encodeURIComponent(name)}`;
}

/** A task's samples over the last hour: [ms since the epoch, % of a CPU, bytes]. */
export interface TaskSeries {
  namespace: string;
  task: string;
  node: string;
  service: string;
  samples: [number, number, number][];
  /** As the store has it now; 0 for none, or a task since removed. */
  cpu_request_percent: number;
  memory_request_bytes: number;
  cpu_limit_percent: number;
  memory_limit_bytes: number;
}

/** `legion usage`: what tasks used over the last hour, as the leader sampled it. */
export function usageHistory(
  namespace: string,
  task = ''
): Promise<{ step_ms: number; tasks: TaskSeries[] }> {
  const params = new URLSearchParams();
  if (namespace) params.set('namespace', namespace);
  if (task) params.set('task', task);
  return call('GET', `/api/usage-history?${params}`);
}


/** A namespace's quota and what it uses of it: CPU in cores, memory and storage in bytes. */
export interface NamespaceQuota {
  namespace: string;
  resources: { resource: string; limit: number; used: number }[];
}

/** `legion quota`: the namespaces with a quota the user may read, or `namespace`'s. */
export function quotas(namespace = ''): Promise<{ namespaces: NamespaceQuota[] }> {
  const params = new URLSearchParams();
  if (namespace) params.set('namespace', namespace);
  return call('GET', `/api/quotas?${params}`);
}

/** A line `logSearch` found. */
export interface FoundLine {
  at_ms: number;
  task: string;
  container: string;
  stderr: boolean;
  text: string;
}

/** `legion logs-search`: a service's lines across its tasks, oldest first. */
export function logSearch(q: {
  namespace: string;
  service: string;
  since_ms: number;
  until_ms?: number;
  text?: string;
  limit?: number;
}): Promise<{ lines: FoundLine[]; truncated: boolean; unread: string[] }> {
  const params = new URLSearchParams({
    namespace: q.namespace,
    service: q.service,
    since_ms: String(q.since_ms)
  });
  if (q.until_ms !== undefined) params.set('until_ms', String(q.until_ms));
  if (q.text) params.set('text', q.text);
  if (q.limit) params.set('limit', String(q.limit));
  return call('GET', `/api/log-search?${params}`);
}

/** A variable a compose file reads, with its default. */
export interface ComposeVariable {
  name: string;
  default: string | null;
  required: boolean;
}

/** A compose file as Legion objects, as `legion stack convert` gives
 * them, nothing written; the namespace first when it is missing. */
export function compose(body: {
  compose: string;
  stack: string;
  namespace?: string;
  env: Record<string, string>;
}): Promise<{ objects: object[]; warnings: string[]; variables: ComposeVariable[] }> {
  return call('POST', '/api/compose', body);
}

/** A compose file to deploy from: an example (namespace empty), or one
 * saved as a Config labelled `legion.template`. */
export interface Template {
  name: string;
  namespace: string;
  about: string;
  compose: string;
}

export function templates(): Promise<{ templates: Template[] }> {
  return call('GET', '/api/templates');
}

/** The console's settings (`legion console settings`): before signing in,
 * the banner alone. */
/** Another cluster's console. */
export interface Peer {
  name: string;
  url: string;
}

export interface ConsoleSettings {
  /** This cluster's name. */
  name?: string;
  banner?: { text: string; level?: 'info' | 'warning' | 'danger' };
  links?: { label: string; url: string }[];
  /** The other clusters' consoles. */
  clusters?: Peer[];
  defaultNamespace?: string;
  /** Such as `8h`. */
  session?: string;
  /** Single sign-on, when it is on: what its button says. */
  sso?: { label: string };
}

export function consoleSettings(): Promise<ConsoleSettings> {
  return call('GET', '/api/console-settings');
}

/** `legion console configure`: the cluster admin only. */
export function setConsoleSettings(s: ConsoleSettings): Promise<ConsoleSettings> {
  return call('POST', '/api/console-settings', s);
}

/** Who the user is in another cluster, asked of its console, which answers
 * only if it lists this one back: signed in there, not, or no answer. */
export async function meIn(peer: Peer): Promise<Me | 'signed-out' | string> {
  try {
    const res = await fetch(new URL('/api/me', peer.url), { credentials: 'include' });
    if (res.status === 401) return 'signed-out';
    if (!res.ok) return t('it answered {status}', { status: res.status });
    return (await res.json()) as Me;
  } catch {
    return t('no answer it lets this console read; it may not list this cluster');
  }
}

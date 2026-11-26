// What the Topology page draws, apart from drawing it: the layout's
// measures, what each Service publishes, the connections seen summed
// between Services, and how each thing's health and second line read.
// Pure, so `npm test` checks it without a browser.
import type { ConnectionEdge, ConnectionEnd, LObject, ReachEdge } from './api.ts';
import { t, tn } from './i18n.svelte.ts';
import type { PublishedPort } from './topology-menu.svelte.ts';

// A row's least height; a service's row grows with its tasks, each
// TASK_STEP below the last, so they never reach into the next row.
export const ROW = 112;
export const TASK_STEP = 52;
export const HEADER_H = 34;
export const X_ROUTE = 24;
export const ROUTE_W = 168;
export const X_SVC_CX = 300;
export const SVC_R = 30;
export const X_TASK = 440;
export const TASK_W = 188;
export const TASK_H = 44;
export const X_VOL = 700;
export const VOL_W = 172;
export const BOX_H = 46;
// Node bands, in the pods' column.
export const BAND_HEAD = 28;
export const BAND_PAD = 10;
export const BAND_GAP = 14;
export const BAND_X = X_TASK - 12;
export const BAND_W = TASK_W + 24;
/** How many node colours there are before they repeat. */
export const NODE_HUES = 6;

export type Published = PublishedPort;
/** Connections seen from one end to a Service, summed over ports and ways. */
export type Seen = { from: string; to: string; n: number; what: string[] };
/** A node's band: its pods, where it starts and how tall it is. */
export type Band = { node: string; tasks: LObject[]; top: number; h: number };
/** A Service's mounts of a Volume. */
export type Use = { service: string; volume: string };

/** Each port a service publishes outside the cluster: through the routing
 * mesh on every node (`ingress`), or on the nodes running its tasks. */
export function publishedPorts(services: LObject[]): Published[] {
  return services.flatMap((s) => {
    const got: { port: number; protocol?: string; publishedPort: number }[] = s.status?.publishedPorts ?? [];
    return ((s.spec?.endpoint?.ports ?? []) as { port: number; protocol?: string; publish?: string }[])
      .filter((p) => p.publish === 'ingress' || p.publish === 'host')
      .map((p) => {
        const protocol = p.protocol ?? 'tcp';
        const pub = got.find((g) => g.port === p.port && (g.protocol ?? 'tcp') === protocol);
        return {
          service: s.metadata.name,
          what: `${p.port}/${protocol}`,
          at: pub?.publishedPort ?? null,
          host: p.publish === 'host'
        };
      });
  });
}

/** The Volumes each Service mounts. */
export function volumeUses(services: LObject[]): Use[] {
  return services.flatMap((s) =>
    ((s.spec?.pod?.volumes ?? []) as { volume?: { name: string } }[])
      .filter((v) => v.volume)
      .map((v) => ({ service: s.metadata.name, volume: v.volume!.name }))
  );
}

/** What the nodes saw: between two of `shown`'s Services, summed over
 * ports and ways (`between`); and into each from anything but another of
 * them (`into`). */
export function seenSummary(seen: ConnectionEdge[], shown: (service: string) => boolean) {
  const isSvc = (e: ConnectionEnd) => e.kind === 'service' && shown(e.name);
  const between = new Map<string, Seen>();
  const into = new Map<string, Seen>();
  for (const e of seen) {
    if (isSvc(e.from) && isSvc(e.to)) {
      if (e.from.name === e.to.name) continue;
      const key = `${e.from.name}>${e.to.name}`;
      const s = between.get(key) ?? { from: e.from.name, to: e.to.name, n: 0, what: [] };
      s.n += e.connections;
      s.what.push(`${e.port}/${e.protocol}`);
      between.set(key, s);
    } else if (isSvc(e.to)) {
      const s = into.get(e.to.name) ?? { from: '', to: e.to.name, n: 0, what: [] };
      s.n += e.connections;
      s.what.push(endName(e.from));
      into.set(e.to.name, s);
    }
  }
  return { between: [...between.values()], into: [...into.values()] };
}

/** A line's width for `n` connections: wider for more, up to about four
 * times as wide. */
export const width = (n: number) => 1.4 + Math.min(3.6, Math.log2(n + 1));
export const conns = (n: number) => tn('1 connection', '{n} connections', n);

/** One end of a connection, as a person reads it. */
export function endName(e: ConnectionEnd) {
  switch (e.kind) {
    case 'service':
      return e.name;
    case 'job':
      return t('Job {name}', { name: e.name });
    case 'node':
      return t('node {name}', { name: e.name });
    case 'namespace':
      return t('other namespaces');
    default:
      return t('outside');
  }
}

/** What a connection was made to. */
export const viaNote = (e: ConnectionEdge) =>
  e.via === 'service' ? t('to its service address') : e.via === 'published' ? t('to a published port') : t('to the task');

/** The ports a reach edge allows. */
export const ports = (e: ReachEdge) =>
  e.all_ports ? t('all ports') : e.ports.map((p) => `${p.port}/${p.protocol}`).join(', ');

export const isVm = (s: LObject) => Boolean((s.spec?.pod?.containers ?? []).some((c: { vm?: unknown }) => c.vm));
export const isDb = (s: LObject) =>
  /postgres|mysql|maria|redis|mongo|sqlite|db/i.test(`${s.metadata.name} ${s.spec?.pod?.containers?.[0]?.image ?? ''}`);

/** The container ports a Service's pod declares. */
export function containerPorts(s: LObject): number[] {
  const list: number[] = [];
  for (const c of s.spec?.pod?.containers ?? []) {
    for (const p of c.ports ?? []) {
      if (p.containerPort) list.push(p.containerPort);
    }
  }
  return list;
}

/** A Service's health: `idle` when it wants none, `good` when all it wants
 * are ready, `bad` when none are, `wait` between. */
export function serviceHealth(s: LObject) {
  const st = s.status;
  if (!st) return 'wait';
  if ((st.desired ?? 0) === 0) return 'idle';
  return (st.ready ?? 0) >= st.desired ? 'good' : (st.ready ?? 0) === 0 ? 'bad' : 'wait';
}

export function taskHealth(tk: LObject) {
  const st = tk.status?.state;
  if (st === 'running') return 'good';
  if (st === 'failed' || st === 'rejected') return 'bad';
  return 'wait';
}

export function nodeHealth(n: LObject | undefined) {
  const st = n?.status?.state;
  if (st === 'ready') return 'good';
  if (st === 'down') return 'bad';
  return st ? 'wait' : '';
}

/** A node's role and state, and its availability when not active. */
export function nodeNote(n: LObject | undefined) {
  if (!n) return '';
  const parts = [n.spec?.role ?? 'worker', n.status?.state ?? 'unknown'];
  const av = n.spec?.availability;
  if (av && av !== 'active') parts.push(av);
  return parts.join(' · ');
}

export const svcNote = (s: LObject) =>
  s.status
    ? t('{ready}/{desired} ready', { ready: s.status.ready ?? 0, desired: s.status.desired ?? 0 })
    : t('not yet placed');

// What a route and a published port show on their second line, and so
// what their links are named by first.
export const routeAddress = (r: LObject) =>
  `${r.spec?.host ?? ''}${r.spec?.path && r.spec.path !== '/' ? r.spec.path : ''}`;
export const portShown = (p: Published) => `${p.at === null ? p.what : `:${p.at}`} → ${p.what.split('/')[0]}`;
export const portWhere = (p: Published) =>
  p.at === null ? t('not published yet') : p.host ? t('on nodes running it') : t('on every node');

/** Each row's top, from the heights of those before it. */
export function tops(heights: number[]): number[] {
  const out: number[] = [];
  let top = HEADER_H;
  for (const h of heights) {
    out.push(top);
    top += h;
  }
  return out;
}

/** Node bands: each node's pods (in their services' order, then by name),
 * then those not placed on one yet. `order` gives a Service's row. */
export function nodeBands(tasks: LObject[], nodeNames: string[], order: Map<string, number>): Band[] {
  const shown = tasks
    .filter((tk) => order.has(tk.spec?.service))
    .sort(
      (a, b) =>
        order.get(a.spec.service)! - order.get(b.spec.service)! || a.metadata.name.localeCompare(b.metadata.name)
    );
  const groups = nodeNames.map((node) => ({ node, tasks: shown.filter((tk) => tk.spec?.node === node) }));
  const unplaced = shown.filter((tk) => !tk.spec?.node);
  if (unplaced.length) groups.push({ node: '', tasks: unplaced });
  let top = HEADER_H;
  return groups.map((g) => {
    const h = BAND_HEAD + Math.max(1, g.tasks.length) * TASK_STEP + BAND_PAD;
    const band = { ...g, top, h };
    top += h + BAND_GAP;
    return band;
  });
}

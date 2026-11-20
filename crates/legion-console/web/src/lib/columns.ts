import type { LObject } from './api';
import { t, tn } from '#lib/i18n.svelte.ts';

export interface Column {
  label: string;
  value: (o: LObject) => string;
  /** A state, shown with its colour. */
  state?: boolean;
  /** Where the value leads, when it names another object: a node. */
  href?: (o: LObject) => string;
}

/** A node's page, for a column naming one. */
const nodeHref = (name: unknown) => (name ? `/k/Node/_/${encodeURIComponent(String(name))}` : '');

const s = (v: unknown) => (v === undefined || v === null ? '' : String(v));

/** A Service's state, as it has none of its own: stopped or paused as its
 * spec says, else ready when all it wants are, pending while some are,
 * failed when none are. Empty when it wants none. */
export function serviceState(o: LObject): string {
  if (o.spec?.stopped) return 'stopped';
  if (o.spec?.paused) return 'paused';
  const want = Number(o.status?.desired ?? o.spec?.replicas ?? 0);
  const ready = Number(o.status?.ready ?? 0);
  return !want ? '' : ready >= want ? 'ready' : ready ? 'pending' : 'failed';
}

export interface HostLack {
  id: string;
  need: string;
  state: string;
  detail: string;
  remedy?: string;
}

/** What a node's host lacks that keeps new tasks off it: a required check
 * found lacking, or one its spec requires (NodeStatus::blocking). */
export function blocking(node: LObject): HostLack[] {
  const required: string[] = node.spec?.require ?? [];
  return ((node.status?.lacks ?? []) as HostLack[]).filter(
    (l) =>
      (l.state === 'unmet' || l.state === 'unsupported') &&
      (l.need === 'required' || required.includes(l.id))
  );
}
const labels = (m?: Record<string, string>) =>
  Object.entries(m ?? {})
    .map(([k, v]) => `${k}=${v}`)
    .join(', ');

/** The columns a list of `kind` shows besides name and namespace. */
export function columns(kind: string): Column[] {
  switch (kind) {
    case 'Node':
      return [
        { label: t('State'), value: (o) => s(o.status?.state), state: true },
        { label: t('Availability'), value: (o) => s(o.spec?.availability ?? 'active') },
        { label: t('Role'), value: (o) => s(o.spec?.role) },
        {
          label: t('Platform'),
          value: (o) =>
            o.status?.platform ? `${o.status.platform.os}/${o.status.platform.arch}` : ''
        },
        { label: t('Address'), value: (o) => s(o.status?.addr) },
        {
          label: t('Host'),
          value: (o) => {
            const ids = blocking(o).map((l) => l.id);
            return ids.length ? t('lacks {ids}', { ids: ids.join(', ') }) : '';
          }
        }
      ];
    case 'Task':
      return [
        {
          label: t('State'),
          value: (o) => (o.status?.paused ? 'paused' : s(o.status?.state)),
          state: true
        },
        { label: t('Ready'), value: (o) => (o.status?.ready ? t('yes') : '') },
        { label: t('Node'), value: (o) => s(o.spec?.node), href: (o) => nodeHref(o.spec?.node) },
        { label: t('IP'), value: (o) => s(o.status?.ip) },
        { label: t('Restarts'), value: (o) => s(o.status?.restarts ?? 0) },
        { label: t('Message'), value: (o) => s(o.status?.message) }
      ];
    case 'Service':
      return [
        { label: t('Mode'), value: (o) => s(o.spec?.mode ?? 'replicated') },
        { label: t('Replicas'), value: (o) => s(o.spec?.replicas) },
        {
          label: t('Image'),
          // A tag that names another image now than the one running says
          // so, and a Linux image (run through FreeBSD's Linux ABI) too.
          value: (o) =>
            s(
              o.spec?.pod?.containers
                ?.map((c: { image: string }) => {
                  let shown = c.image;
                  if (o.status?.linuxImages && c.image in o.status.linuxImages) {
                    shown = t('{image} (Linux)', { image: shown });
                  }
                  if (o.status?.images?.moved?.[c.image]) shown = t('{image} (tag moved)', { image: shown });
                  return shown;
                })
                .join(', ')
            )
        },
        {
          label: t('Ready'),
          value: (o) =>
            o.spec?.stopped
              ? t('stopped')
              : o.spec?.paused
                ? t('paused {n}/{of}', { n: o.status?.paused ?? 0, of: o.status?.running ?? 0 })
                : o.status
                ? `${o.status.ready ?? 0}/${o.status.desired ?? 0}`
                : ''
        },
        {
          label: t('Address'),
          value: (o) => {
            const lb = o.spec?.endpoint?.loadBalancer;
            return lb ? `${lb.address} (vhid ${lb.vhid})` : s(o.status?.virtualIp);
          }
        }
      ];
    case 'Job':
      return [
        { label: t('State'), value: (o) => s(o.status?.state), state: true },
        { label: t('Completions'), value: (o) => s(o.spec?.completions) }
      ];
    case 'CronJob':
      return [{ label: t('Schedule'), value: (o) => s(o.spec?.schedule) }];
    case 'Volume':
      return [
        { label: t('Driver'), value: (o) => s(o.spec?.driver ?? 'zfs') },
        { label: t('Node'), value: (o) => s(o.status?.node), href: (o) => nodeHref(o.status?.node) },
        { label: t('Size'), value: (o) => s(o.spec?.size) },
        { label: t('Lost'), value: (o) => s(o.status?.lost) },
        {
          label: t('Replica'),
          value: (o) => {
            const r = o.status?.replica;
            if (!r) return '';
            const at = r.takenMs ? new Date(r.takenMs).toLocaleString() : t('no copy yet');
            return [r.state ?? 'pending', r.node, at].filter((x) => x).join(' · ');
          }
        }
      ];
    case 'Route':
      return [
        { label: t('Host'), value: (o) => s(o.spec?.host) },
        { label: t('Service'), value: (o) => s(o.spec?.service) },
        { label: t('TLS'), value: (o) => (o.spec?.tls ? t('yes') : '') }
      ];
    case 'NetworkPolicy':
      return [
        { label: t('Services'), value: (o) => labels(o.spec?.services) || t('all') },
        { label: t('Ingress rules'), value: (o) => s(o.spec?.ingress?.length ?? 0) },
        {
          label: t('Egress'),
          value: (o) =>
            o.spec?.egress === undefined || o.spec?.egress === null
              ? t('not limited')
              : tn('{n} rule', '{n} rules', o.spec.egress.length)
        }
      ];
    case 'ImagePolicy':
      return [
        { label: t('Images'), value: (o) => s(o.spec?.images?.join(', ')) },
        { label: t('Keys'), value: (o) => s(o.spec?.keys?.length ?? 0) },
        { label: t('Mode'), value: (o) => s(o.spec?.mode ?? 'enforce') },
        {
          label: t('Attestations'),
          value: (o) =>
            ((o.spec?.attestations ?? []) as string[])
              .map((u) => u.replace(/^https:\/\//, ''))
              .join(', ')
        }
      ];
    case 'AlertReceiver':
      return [
        {
          label: t('Sends by'),
          value: (o) =>
            o.spec?.webhook ? t('webhook') : o.spec?.ntfy ? 'ntfy' : o.spec?.smtp ? t('mail') : ''
        },
        {
          label: t('Chooses'),
          value: (o) =>
            [
              o.spec?.severity === 'critical' ? t('critical only') : t('every severity'),
              o.spec?.namespaces?.length
                ? (o.spec.namespaces as string[]).map((n) => n || t('the cluster')).join(', ')
                : '',
              o.spec?.kinds?.length ? (o.spec.kinds as string[]).join(', ') : ''
            ]
              .filter(Boolean)
              .join(' · ')
        },
        {
          label: t('Delivered'),
          value: (o) =>
            o.status?.deliveredMs ? new Date(o.status.deliveredMs).toLocaleString() : t('not yet')
        },
        { label: t('State'), value: (o) => (o.status?.error ? 'failing' : 'ready'), state: true },
        { label: t('Why'), value: (o) => s(o.status?.error) }
      ];
    case 'Build':
      return [
        { label: t('Phase'), value: (o) => s(o.status?.phase ?? 'Pending'), state: true },
        { label: t('Image'), value: (o) => s(o.spec?.image) },
        {
          label: t('Source'),
          value: (o) => s(o.spec?.source?.git?.url ?? (o.spec?.source?.containerfile ? t('Containerfile') : ''))
        },
        { label: t('Node'), value: (o) => s(o.status?.node), href: (o) => nodeHref(o.status?.node) },
        { label: t('Pushed'), value: (o) => s(o.status?.pinned) },
        { label: t('Why'), value: (o) => s(o.status?.message) }
      ];
    case 'RoleBinding':
      return [
        { label: t('User'), value: (o) => s(o.spec?.user) },
        { label: t('Group'), value: (o) => s(o.spec?.group) },
        { label: t('Role'), value: (o) => s(o.spec?.role) }
      ];
    default:
      return [];
  }
}

/** A colour class for a state. */
export function tone(state: string): string {
  switch (state.toLowerCase()) {
    case 'succeeded':
    case 'ready':
    case 'running':
    case 'complete':
    case 'active':
      return 'good';
    case 'pending':
    case 'starting':
    case 'preparing':
    case 'assigned':
    case 'suspect':
    case 'draining':
    case 'paused':
    case 'stale':
      return 'wait';
    case 'failed':
    case 'failing':
    case 'rejected':
    case 'down':
    case 'orphaned':
      return 'bad';
    default:
      return '';
  }
}

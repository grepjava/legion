import type { LObject } from './api';
import { t, tn } from '#lib/i18n.svelte.ts';

export interface Column {
  label: string;
  value: (o: LObject) => string;
  /** A state, shown with its colour. */
  state?: boolean;
}

const s = (v: unknown) => (v === undefined || v === null ? '' : String(v));
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
        { label: t('Address'), value: (o) => s(o.status?.addr) }
      ];
    case 'Task':
      return [
        { label: t('State'), value: (o) => s(o.status?.state), state: true },
        { label: t('Ready'), value: (o) => (o.status?.ready ? t('yes') : '') },
        { label: t('Node'), value: (o) => s(o.spec?.node) },
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
          value: (o) => s(o.spec?.pod?.containers?.map((c: { image: string }) => c.image).join(', '))
        },
        {
          label: t('Ready'),
          value: (o) => (o.status ? `${o.status.ready ?? 0}/${o.status.desired ?? 0}` : '')
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
        { label: t('Node'), value: (o) => s(o.status?.node) },
        { label: t('Size'), value: (o) => s(o.spec?.size) },
        { label: t('Lost'), value: (o) => s(o.status?.lost) }
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
        { label: t('Mode'), value: (o) => s(o.spec?.mode ?? 'enforce') }
      ];
    case 'Build':
      return [
        { label: t('Phase'), value: (o) => s(o.status?.phase ?? 'Pending'), state: true },
        { label: t('Image'), value: (o) => s(o.spec?.image) },
        {
          label: t('Source'),
          value: (o) => s(o.spec?.source?.git?.url ?? (o.spec?.source?.containerfile ? t('Containerfile') : ''))
        },
        { label: t('Node'), value: (o) => s(o.status?.node) },
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
      return 'wait';
    case 'failed':
    case 'rejected':
    case 'down':
    case 'orphaned':
      return 'bad';
    default:
      return '';
  }
}

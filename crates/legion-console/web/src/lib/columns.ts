import type { LObject } from './api';

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
        { label: 'State', value: (o) => s(o.status?.state), state: true },
        { label: 'Availability', value: (o) => s(o.spec?.availability ?? 'active') },
        { label: 'Role', value: (o) => s(o.spec?.role) },
        {
          label: 'Platform',
          value: (o) =>
            o.status?.platform ? `${o.status.platform.os}/${o.status.platform.arch}` : ''
        },
        { label: 'Address', value: (o) => s(o.status?.addr) }
      ];
    case 'Task':
      return [
        { label: 'State', value: (o) => s(o.status?.state), state: true },
        { label: 'Ready', value: (o) => (o.status?.ready ? 'yes' : '') },
        { label: 'Node', value: (o) => s(o.spec?.node) },
        { label: 'IP', value: (o) => s(o.status?.ip) },
        { label: 'Restarts', value: (o) => s(o.status?.restarts ?? 0) },
        { label: 'Message', value: (o) => s(o.status?.message) }
      ];
    case 'Service':
      return [
        { label: 'Mode', value: (o) => s(o.spec?.mode ?? 'replicated') },
        { label: 'Replicas', value: (o) => s(o.spec?.replicas) },
        {
          label: 'Image',
          value: (o) => s(o.spec?.pod?.containers?.map((c: { image: string }) => c.image).join(', '))
        },
        {
          label: 'Ready',
          value: (o) => (o.status ? `${o.status.ready ?? 0}/${o.status.desired ?? 0}` : '')
        },
        {
          label: 'Address',
          value: (o) => {
            const lb = o.spec?.endpoint?.loadBalancer;
            return lb ? `${lb.address} (vhid ${lb.vhid})` : s(o.status?.virtualIp);
          }
        }
      ];
    case 'Job':
      return [
        { label: 'State', value: (o) => s(o.status?.state), state: true },
        { label: 'Completions', value: (o) => s(o.spec?.completions) }
      ];
    case 'CronJob':
      return [{ label: 'Schedule', value: (o) => s(o.spec?.schedule) }];
    case 'Volume':
      return [
        { label: 'Driver', value: (o) => s(o.spec?.driver ?? 'zfs') },
        { label: 'Node', value: (o) => s(o.status?.node) },
        { label: 'Size', value: (o) => s(o.spec?.size) },
        { label: 'Lost', value: (o) => s(o.status?.lost) }
      ];
    case 'Route':
      return [
        { label: 'Host', value: (o) => s(o.spec?.host) },
        { label: 'Service', value: (o) => s(o.spec?.service) },
        { label: 'TLS', value: (o) => (o.spec?.tls ? 'yes' : '') }
      ];
    case 'NetworkPolicy':
      return [
        { label: 'Services', value: (o) => labels(o.spec?.services) || 'all' },
        { label: 'Ingress rules', value: (o) => s(o.spec?.ingress?.length ?? 0) },
        {
          label: 'Egress',
          value: (o) =>
            o.spec?.egress === undefined || o.spec?.egress === null
              ? 'not limited'
              : `${o.spec.egress.length} rules`
        }
      ];
    case 'ImagePolicy':
      return [
        { label: 'Images', value: (o) => s(o.spec?.images?.join(', ')) },
        { label: 'Keys', value: (o) => s(o.spec?.keys?.length ?? 0) },
        { label: 'Mode', value: (o) => s(o.spec?.mode ?? 'enforce') }
      ];
    case 'RoleBinding':
      return [
        { label: 'User', value: (o) => s(o.spec?.user) },
        { label: 'Group', value: (o) => s(o.spec?.group) },
        { label: 'Role', value: (o) => s(o.spec?.role) }
      ];
    default:
      return [];
  }
}

/** A colour class for a state. */
export function tone(state: string): string {
  switch (state) {
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

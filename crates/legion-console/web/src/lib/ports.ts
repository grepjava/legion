// Where a service's ports can be opened from a browser: its published
// ports, its load balancer's address and the Routes to it.
import type { LObject } from '#lib/api.ts';
import { t } from '#lib/i18n.svelte.ts';

export interface Reach {
  /** What it is, e.g. `80/tcp`. */
  what: string;
  /** How it is reached: `ingress`, `host`, `load balancer`, `route`. */
  how: string;
  /** Links, each with the text shown; empty when none can be made. */
  links: { href: string; text: string }[];
  /** Why there is no link, or where it is reachable without one. */
  note: string;
}

/** `https` for the ports and names that speak it, `http` otherwise. */
export function scheme(port: number, name = ''): 'http' | 'https' {
  return port === 443 || port === 8443 || /https|tls|ssl/i.test(name) ? 'https' : 'http';
}

/** A host as it goes in a URL: IPv6 addresses bracketed. */
export function urlHost(host: string): string {
  return host.includes(':') && !host.startsWith('[') ? `[${host}]` : host;
}

/** The host of `addr`, `host:port` or `[v6]:port`. */
export function hostOf(addr: string): string {
  const v6 = /^\[([^\]]+)\](?::\d+)?$/.exec(addr);
  if (v6) return v6[1];
  const i = addr.lastIndexOf(':');
  return i > 0 && addr.indexOf(':') === i ? addr.slice(0, i) : addr;
}

function link(port: number, name: string, host: string, path = '') {
  const s = scheme(port, name);
  const shown = (s === 'http' && port === 80) || (s === 'https' && port === 443) ? '' : `:${port}`;
  const href = `${s}://${urlHost(host)}${shown}${path}`;
  return { href, text: href };
}

/**
 * Each way the service can be reached from outside the cluster.
 * `here` is the host the console was opened at, a node of the cluster, which
 * the routing mesh answers ingress ports on; `nodes` gives each node's
 * address, where known; `tasks` and `routes` are the namespace's.
 */
export function reach(
  service: LObject,
  here: string,
  tasks: LObject[],
  nodes: Map<string, string>,
  routes: LObject[]
): Reach[] {
  const out: Reach[] = [];
  const name = service.metadata.name;
  const ports: { name?: string; port: number; protocol?: string; publish?: string }[] =
    service.spec?.ports ?? [];
  const published: { port: number; protocol: string; publishedPort: number }[] =
    service.status?.publishedPorts ?? [];
  const running = tasks.filter(
    (t) => t.spec?.service === name && !t.spec?.job && t.status?.state === 'running'
  );
  for (const p of ports) {
    const protocol = p.protocol ?? 'tcp';
    const what = `${p.port}/${protocol}`;
    const tcp = protocol === 'tcp';
    const pub = published.find((x) => x.port === p.port && (x.protocol ?? 'tcp') === protocol);
    if (p.publish === 'ingress') {
      if (!pub) {
        out.push({ what, how: 'ingress', links: [], note: t('not published yet') });
      } else if (!tcp) {
        out.push({ what, how: 'ingress', links: [], note: t('port {port} on every node', { port: pub.publishedPort }) });
      } else {
        out.push({ what, how: 'ingress', links: [link(pub.publishedPort, p.name ?? '', here)], note: '' });
      }
    } else if (p.publish === 'host') {
      const at = pub?.publishedPort ?? p.port;
      const on = [...new Set(running.map((t) => t.spec?.node as string).filter(Boolean))].sort();
      const known = on.filter((n) => nodes.has(n));
      const links = tcp ? known.map((n) => link(at, p.name ?? '', nodes.get(n)!)) : [];
      const unknown = on.filter((n) => !nodes.has(n) || !tcp);
      const note = !on.length
        ? t('no task running')
        : unknown.length
          ? t('port {port} on {nodes}', { port: at, nodes: unknown.join(', ') })
          : '';
      out.push({ what, how: 'host', links, note });
    }
  }
  const lb = service.spec?.endpoint?.loadBalancer;
  if (lb?.address) {
    for (const p of ports) {
      const protocol = p.protocol ?? 'tcp';
      out.push({
        what: `${p.port}/${protocol}`,
        how: 'load balancer',
        links: protocol === 'tcp' ? [link(p.port, p.name ?? '', lb.address)] : [],
        note: protocol === 'tcp' ? '' : `${lb.address}:${p.port}`
      });
    }
  }
  for (const r of routes.filter((r) => r.spec?.service === name)) {
    const s = r.spec?.tls ? 'https' : 'http';
    const href = `${s}://${urlHost(r.spec.host)}${r.spec?.path ?? ''}`;
    out.push({ what: r.metadata.name, how: 'route', links: [{ href, text: href }], note: '' });
  }
  return out;
}

// What a right-click (or the menu key) offers on the Topology page, and
// what dragging does there: a Service started, stopped, scaled, moved to
// another node with its data, duplicated onto one with a copy of its data;
// a task's logs and console; a node paused, drained or made the leader; a
// Volume moved or protected. Each change asks first, the moves checked as
// a drain is (what they copy, what would block), and never gives two
// copies of a Service one Volume's data.
import { goto } from '$app/navigation';
import { api, type LObject } from './api.ts';
import { ask } from './confirm.svelte.ts';
import { busyWhile, toast } from './toast.svelte.ts';
import { cannotWrite } from './session.svelte.ts';
import { t } from './i18n.svelte.ts';
import type { MenuItem } from './ContextMenu.svelte';
import {
  ArrowRightLeft,
  CirclePause,
  CirclePlay,
  Copy,
  Crown,
  ExternalLink,
  FilePenLine,
  Clock,
  Layers,
  LifeBuoy,
  Play,
  RotateCw,
  ScrollText,
  Square,
  SquareTerminal,
  Trash2,
  Truck,
  Undo2,
  Unplug,
  Globe
} from '@lucide/svelte';

/** What the menus need of the page. */
export interface Scene {
  namespace: string;
  /** The nodes that may take tasks, by name. */
  nodes: string[];
  /** The manager that leads, by name; empty if not known. */
  leader: string;
  /** The managers, by name. */
  managers: string[];
  /** The namespace's Services, for those sharing a Volume. */
  services: LObject[];
}

/** `s` and every Service of `all` sharing a Volume with it, and with
 * those in turn: what must move together, as no Service can be left
 * holding data that moved away. */
export function movingWith(s: LObject, all: LObject[]): LObject[] {
  const group = new Map([[s.metadata.name, s]]);
  const vols = new Set(volumesOf(s));
  for (let grew = true; grew; ) {
    grew = false;
    for (const o of all) {
      if (group.has(o.metadata.name) || !volumesOf(o).some((v) => vols.has(v))) continue;
      group.set(o.metadata.name, o);
      for (const v of volumesOf(o)) vols.add(v);
      grew = true;
    }
  }
  return [...group.values()];
}

const page = (kind: string, ns: string, name: string, tab = '') =>
  `/k/${encodeURIComponent(kind)}/${encodeURIComponent(ns || '_')}/${encodeURIComponent(name)}${tab ? `?tab=${tab}` : ''}`;

/** The Volumes a Service's pod mounts as they are: its data, which more
 * than one copy must never share. */
export function volumesOf(s: LObject): string[] {
  return ((s.spec?.pod?.volumes ?? []) as { volume?: { name: string } }[])
    .map((v) => v.volume?.name)
    .filter((n): n is string => !!n);
}

/** The node a Service's placement pins it to, if any. */
function pinnedTo(s: LObject): string {
  const c = ((s.spec?.placement?.constraints ?? []) as string[]).find((c) => /^node\.name\s*==/.test(c));
  return c ? c.split('==')[1].trim() : '';
}

/** Its spec pinned to `node`, any other node pin replaced. */
function pinned(spec: Record<string, unknown>, node: string): Record<string, unknown> {
  const placement = { ...((spec.placement as Record<string, unknown>) ?? {}) };
  const constraints = ((placement.constraints as string[]) ?? []).filter((c) => !/^node\.name\s*==/.test(c));
  placement.constraints = [...constraints, `node.name == ${node}`];
  return { ...spec, placement };
}

async function done(what: () => Promise<string>) {
  try {
    toast(await busyWhile(what));
  } catch (e) {
    toast((e as Error).message, true);
  }
}

/** Moves Service `s` to `node`, its data with it, and with it every
 * Service sharing that data: checked first as one change. */
export async function moveService(s: LObject, node: string, scene: Scene) {
  const ns = scene.namespace;
  const name = s.metadata.name;
  const group = movingWith(s, scene.services);
  const names = group.map((g) => g.metadata.name);
  const vols = [...new Set(group.flatMap(volumesOf))];
  const others = names.filter((n) => n !== name);
  const ok = await ask({
    message: vols.length
      ? t(
          'Move {name} to {node}? Its data on Volume {volumes} is copied there first; it stops for the switch-over, then runs on {node}.',
          { name, node, volumes: vols.join(', ') }
        )
      : t('Move {name} to {node}? Its tasks start there and stop where they are.', { name, node }),
    detail: others.length
      ? [t('{services} use that data too, and move with it.', { services: others.join(', ') })]
      : undefined,
    action: t('Move'),
    check: [{ change: 'move-services', namespace: ns, services: names, node }]
  });
  if (!ok) return;
  await done(async () => {
    for (const v of vols) await api.set('Volume', ns, v, 'node', node);
    for (const n of names) {
      const fresh = await api.get('Service', ns, n);
      await api.apply({
        object: {
          apiVersion: fresh.apiVersion,
          kind: 'Service',
          metadata: { name: n, namespace: ns, labels: fresh.metadata.labels ?? {} },
          spec: pinned(fresh.spec, node)
        },
        expectedVersion: fresh.metadata.version
      });
    }
    return vols.length
      ? t('{services} moving to {node}, their data with them', { services: names.join(', '), node })
      : t('{name} moving to {node}', { name, node });
  });
}

/** A copy of Service `s` on `node`, on a copy of its data. */
export async function duplicateService(s: LObject, node: string, scene: Scene) {
  const ns = scene.namespace;
  const name = s.metadata.name;
  const copy = `${name}-${node}`.slice(0, 63).replace(/-+$/, '');
  const vols = volumesOf(s);
  const copies = vols.map((v) => `${v}-${node}`.slice(0, 63).replace(/-+$/, ''));
  const ok = await ask({
    message: vols.length
      ? t(
          'Duplicate {name} onto {node} as {copy}? Its Volume {volumes} is snapshotted and copied to {node} as {copies}, so the copy runs on its own data from now on. The copy publishes no ports and holds no load balancer address.',
          { name, node, copy, volumes: vols.join(', '), copies: copies.join(', ') }
        )
      : t(
          'Duplicate {name} onto {node} as {copy}? The copy publishes no ports and holds no load balancer address.',
          { name, node, copy }
        ),
    action: t('Duplicate')
  });
  if (!ok) return;
  await done(async () => {
    const snap = `dup-${Date.now()}`;
    for (const [i, v] of vols.entries()) {
      await api.volumeSnapshotOp(ns, v, 'create', snap);
      await api.volumeSnapshotOp(ns, v, 'clone', snap, copies[i]);
      await api.set('Volume', ns, copies[i], 'node', node);
    }
    // A plain copy: what the page holds is a reactive proxy, which
    // structuredClone refuses.
    const spec = $state.snapshot(s.spec) as Record<string, any>;
    // Its own data: each Volume it mounts, the copy made of it.
    for (const pv of (spec.pod?.volumes ?? []) as { volume?: { name: string } }[]) {
      const i = vols.indexOf(pv.volume?.name ?? '');
      if (pv.volume && i >= 0) pv.volume.name = copies[i];
    }
    // Reached in the namespace only: a published port or an address
    // would clash with the original's.
    if (spec.endpoint) {
      delete spec.endpoint.loadBalancer;
      spec.endpoint.ports = (spec.endpoint.ports ?? []).map((p: Record<string, unknown>) => {
        const { publish: _p, publishedPort: _pp, ...rest } = p;
        return rest;
      });
    }
    if (vols.length) spec.replicas = 1;
    await api.apply({
      object: {
        apiVersion: s.apiVersion,
        kind: 'Service',
        metadata: { name: copy, namespace: ns, labels: s.metadata.labels ?? {} },
        spec: pinned(spec, node)
      }
    });
    return vols.length
      ? t('{copy} made on {node}; its data is being copied there', { copy, node })
      : t('{copy} made on {node}', { copy, node });
  });
}

/** What a Service's menu offers. */
export function serviceMenu(s: LObject, scene: Scene): MenuItem[] {
  const ns = scene.namespace;
  const name = s.metadata.name;
  const why = cannotWrite(ns) ?? undefined;
  const vols = volumesOf(s);
  const stopped = !!s.spec?.stopped;
  const paused = !!s.spec?.paused;
  const replicas = Number(s.spec?.replicas ?? 1);
  const here = pinnedTo(s);
  const set = (field: string, value: unknown, said: string) => () =>
    done(async () => ((await api.set('Service', ns, name, field, value)).changed ? said : t('Nothing to change')));
  const elsewhere = scene.nodes.filter((n) => n !== here);
  return [
    { label: t('Open'), icon: ExternalLink, run: () => goto(page('Service', ns, name)) },
    stopped
      ? { label: t('Start'), icon: Play, disabled: why, run: set('stopped', false, t('{name} started', { name })) }
      : {
          label: t('Stop'),
          icon: Square,
          danger: true,
          disabled: why,
          run: async () => {
            if (await ask(t('Stop {name}? Its tasks go; its spec stays for Start.', { name }), { action: t('Stop') }))
              set('stopped', true, t('{name} stopped', { name }))();
          }
        },
    paused
      ? { label: t('Resume'), icon: CirclePlay, disabled: why, run: set('paused', false, t('{name} resumed', { name })) }
      : { label: t('Pause'), icon: CirclePause, disabled: why, run: set('paused', true, t('{name} paused', { name })) },
    {
      label: t('Restart'),
      icon: RotateCw,
      disabled: why,
      run: async () => {
        if (await ask(t('Restart {name}? Its tasks are replaced, as an update replaces them.', { name }), { action: t('Restart') }))
          done(async () => (await api.restart(ns, name)).message);
      }
    },
    {
      label: t('Scale'),
      icon: Layers,
      disabled: why,
      children: [0, 1, 2, 3, 4, 5].map((n) => ({
        label: n === replicas ? t('{n} (now)', { n }) : String(n),
        // Never two copies over one Volume's data.
        disabled:
          n === replicas
            ? t('it has {n}', { n })
            : n > 1 && vols.length
              ? t('its data is on Volume {volumes}: more replicas would share it', { volumes: vols.join(', ') })
              : undefined,
        run: set('replicas', n, t('{name} scaled to {n}', { name, n }))
      }))
    },
    {
      label: t('Move to'),
      icon: Truck,
      separated: true,
      disabled: why ?? (elsewhere.length ? undefined : t('no other node takes tasks')),
      children: elsewhere.map((n) => ({ label: n, run: () => moveService(s, n, scene) }))
    },
    {
      label: t('Duplicate onto'),
      icon: Copy,
      disabled: why ?? (scene.nodes.length ? undefined : t('no node takes tasks')),
      children: scene.nodes.map((n) => ({ label: n, run: () => duplicateService(s, n, scene) }))
    },
    ...(vols.length
      ? [
          {
            label: t('Protect its data'),
            icon: LifeBuoy,
            run: () => goto(`${page('Volume', ns, vols[0], 'protection')}&fix=replica`)
          }
        ]
      : []),
    {
      label: t('Roll back'),
      icon: Undo2,
      disabled: why ?? (s.status?.update ? undefined : t('No update to roll back')),
      run: async () => {
        if (await ask(t('Roll {name} back to the spec before its latest update?', { name }), { action: t('Roll back') }))
          done(async () => (await api.rollback(ns, name)).message);
      }
    },
    { label: t('Logs'), icon: ScrollText, separated: true, run: () => goto(page('Service', ns, name, 'search')) },
    { label: t('Events'), icon: Clock, run: () => goto(page('Service', ns, name, 'events')) },
    {
      label: t('Edit YAML'),
      icon: FilePenLine,
      disabled: why,
      run: () =>
        goto(`/apply?kind=Service&namespace=${encodeURIComponent(ns)}&name=${encodeURIComponent(name)}`)
    },
    {
      label: t('Delete'),
      icon: Trash2,
      danger: true,
      separated: true,
      disabled: why,
      run: async () => {
        const ok = await ask({
          message: t('Delete Service {name}? Its tasks are stopped and removed.', { name }),
          action: t('Delete'),
          typed: name,
          check: [{ change: 'delete-service', namespace: ns, service: name }]
        });
        if (ok) done(async () => (await api.remove('Service', ns, name), t('{name} deleted', { name })));
      }
    }
  ];
}

/** What a task's menu offers. */
export function taskMenu(task: LObject, scene: Scene): MenuItem[] {
  const ns = scene.namespace;
  const name = task.metadata.name;
  const service = task.spec?.service as string | undefined;
  return [
    { label: t('Open'), icon: ExternalLink, run: () => goto(page('Task', ns, name)) },
    { label: t('Logs'), icon: ScrollText, run: () => goto(page('Task', ns, name, 'logs')) },
    { label: t('Console'), icon: SquareTerminal, run: () => goto(page('Task', ns, name, 'terminal')) },
    { label: t('Events'), icon: Clock, run: () => goto(page('Task', ns, name, 'events')) },
    ...(service
      ? [{ label: t('Its Service'), icon: Layers, separated: true, run: () => goto(page('Service', ns, service)) }]
      : [])
  ];
}

/** What a node's menu offers. */
export function nodeMenu(node: LObject | undefined, name: string, scene: Scene): MenuItem[] {
  const why = cannotWrite('') ?? undefined;
  const now = (node?.spec?.availability as string) ?? 'active';
  const avail = (to: string, label: string, icon: MenuItem['icon'], said: string, question: string) => ({
    label,
    icon,
    disabled: why ?? (now === to ? t('it is {state} already', { state: to }) : undefined),
    run: async () => {
      if (
        await ask(question, {
          action: label,
          check: to === 'drain' ? [{ change: 'drain' as const, node: name }] : undefined
        })
      )
        done(async () => ((await api.set('Node', '', name, 'availability', to)).changed ? said : t('Nothing to change')));
    }
  });
  const items: MenuItem[] = [
    { label: t('Open'), icon: ExternalLink, run: () => goto(page('Node', '', name)) },
    avail('active', t('Activate'), Play, t('{name} active', { name }), t('Activate {name}? It takes new tasks again.', { name })),
    avail('pause', t('Pause'), CirclePause, t('{name} paused', { name }), t('Pause {name}? It takes no new tasks; its tasks stay.', { name })),
    avail('drain', t('Drain'), ArrowRightLeft, t('{name} draining', { name }), t('Drain {name}? Its tasks move to other nodes.', { name }))
  ];
  if (scene.managers.includes(name) && scene.leader && scene.leader !== name)
    items.push({
      label: t('Lead'),
      icon: Crown,
      separated: true,
      disabled: why,
      run: async () => {
        const ok = await ask(
          t(
            'Make {name} the leader? For about 10 s nothing is placed or changed while the leader hands over; no node goes down and running tasks carry on.',
            { name }
          ),
          { action: t('Lead') }
        );
        if (ok)
          done(async () => {
            const r = await api.lead(name);
            return t('{name} leads now (nothing written for {s} s)', { name: r.leader, s: (r.tookMs / 1000).toFixed(1) });
          });
      }
    });
  return items;
}

/** Moves Volume `v` to `node`, checked first. */
export async function moveVolume(v: string, node: string, scene: Scene) {
  const ns = scene.namespace;
  const ok = await ask({
    message: t('Move Volume {name} to {node}? Its data is copied there; what uses it stops for the switch-over.', {
      name: v,
      node
    }),
    action: t('Move'),
    check: [{ change: 'move-volume', namespace: ns, volume: v, node }]
  });
  if (ok)
    done(async () => ((await api.set('Volume', ns, v, 'node', node)).changed ? t('{name} moving to {node}', { name: v, node }) : t('Nothing to change')));
}

/** What a Volume's menu offers. */
export function volumeMenu(v: string, on: string, scene: Scene): MenuItem[] {
  const ns = scene.namespace;
  const why = cannotWrite(ns) ?? undefined;
  const elsewhere = scene.nodes.filter((n) => n !== on);
  return [
    { label: t('Open'), icon: ExternalLink, run: () => goto(page('Volume', ns, v)) },
    { label: t('Protection'), icon: LifeBuoy, run: () => goto(page('Volume', ns, v, 'protection')) },
    { label: t('Snapshots and backups'), icon: Clock, run: () => goto(page('Volume', ns, v, 'data')) },
    {
      label: t('Move to'),
      icon: Truck,
      separated: true,
      disabled: why ?? (elsewhere.length ? undefined : t('no other node')),
      children: elsewhere.map((n) => ({ label: n, run: () => moveVolume(v, n, scene) }))
    }
  ];
}

/** A port a Service publishes outside the cluster, as the page draws it. */
export interface PublishedPort {
  service: string;
  /** `PORT/PROTOCOL`, the Service's port. */
  what: string;
  /** The port it is published on; null until it is. */
  at: number | null;
  /** On the nodes running it (`host`) rather than every node (`ingress`). */
  host: boolean;
}

async function copied(text: string) {
  try {
    await navigator.clipboard.writeText(text);
    toast(t('Copied {text}', { text }));
  } catch (e) {
    toast((e as Error).message, true);
  }
}

/** What a published port's menu offers: opened or copied at this node's
 * address (any node answers an `ingress` port), its Service, its ports
 * edited, or no longer published. */
export function portMenu(p: PublishedPort, scene: Scene): MenuItem[] {
  const ns = scene.namespace;
  const service = p.service;
  const why = cannotWrite(ns) ?? undefined;
  const [port, protocol] = p.what.split('/');
  const address = p.at === null ? '' : `${location.hostname}:${p.at}`;
  const notYet = p.at === null ? t('not published yet') : undefined;
  return [
    {
      label: t('Open in browser'),
      icon: ExternalLink,
      disabled: notYet ?? (protocol === 'tcp' ? undefined : t('a {protocol} port', { protocol })),
      children: ['https', 'http'].map((scheme) => ({
        label: `${scheme}://${address}`,
        run: () => window.open(`${scheme}://${address}/`, '_blank', 'noopener')
      }))
    },
    { label: t('Copy address'), icon: Copy, disabled: notYet, run: () => copied(address) },
    { label: t('Its Service'), icon: Layers, separated: true, run: () => goto(page('Service', ns, service)) },
    {
      label: t('Edit ports'),
      icon: FilePenLine,
      disabled: why,
      run: () =>
        goto(`/apply?kind=Service&namespace=${encodeURIComponent(ns)}&name=${encodeURIComponent(service)}`)
    },
    {
      label: t('Stop publishing'),
      icon: Unplug,
      danger: true,
      separated: true,
      disabled: why,
      run: async () => {
        const ok = await ask(
          p.host
            ? t('Stop publishing port {port}/{protocol} of {service}? It is no longer answered on the nodes running it; inside the cluster it still is.', { port, protocol, service })
            : t('Stop publishing port {port}/{protocol} of {service}? No node answers it any more; inside the cluster it still is.', { port, protocol, service }),
          { action: t('Stop publishing') }
        );
        if (!ok) return;
        await done(async () => {
          const fresh = await api.get('Service', ns, service);
          const endpoint = { ...(fresh.spec?.endpoint ?? {}) };
          endpoint.ports = ((endpoint.ports ?? []) as Record<string, unknown>[]).map((q) => {
            if (String(q.port) !== port || ((q.protocol as string) ?? 'tcp') !== protocol) return q;
            const { publish: _, publishedPort: __, ...rest } = q;
            return rest;
          });
          await api.apply({
            object: {
              apiVersion: fresh.apiVersion,
              kind: 'Service',
              metadata: { name: service, namespace: ns, labels: fresh.metadata.labels ?? {} },
              spec: { ...fresh.spec, endpoint }
            },
            expectedVersion: fresh.metadata.version
          });
          return t('Port {port}/{protocol} of {service} no longer published', { port, protocol, service });
        });
      }
    }
  ];
}

/** What an ingress Route's menu offers. */
export function routeMenu(r: LObject, scene: Scene): MenuItem[] {
  const ns = scene.namespace;
  const name = r.metadata.name;
  const why = cannotWrite(ns) ?? undefined;
  const host = (r.spec?.host as string) ?? '';
  const url = `${r.spec?.tls ? 'https' : 'http'}://${host}${(r.spec?.path as string) ?? '/'}`;
  const wildcard = host.startsWith('*') ? t('its host is a wildcard') : undefined;
  return [
    { label: t('Open'), icon: ExternalLink, run: () => goto(page('Route', ns, name)) },
    {
      label: t('Open in browser'),
      icon: Globe,
      disabled: wildcard,
      run: () => window.open(url, '_blank', 'noopener')
    },
    { label: t('Copy address'), icon: Copy, run: () => copied(url) },
    ...(r.spec?.service
      ? [{ label: t('Its Service'), icon: Layers, separated: true, run: () => goto(page('Service', ns, r.spec.service)) }]
      : []),
    {
      label: t('Edit YAML'),
      icon: FilePenLine,
      disabled: why,
      run: () => goto(`/apply?kind=Route&namespace=${encodeURIComponent(ns)}&name=${encodeURIComponent(name)}`)
    },
    {
      label: t('Delete'),
      icon: Trash2,
      danger: true,
      separated: true,
      disabled: why,
      run: async () => {
        const ok = await ask({
          message: t('Delete Route {name}? {url} is no longer routed to {service}.', { name, url, service: r.spec?.service ?? '' }),
          action: t('Delete'),
          typed: name
        });
        if (ok) done(async () => (await api.remove('Route', ns, name), t('{name} deleted', { name })));
      }
    }
  ];
}

// The Topology page's helpers: `npm test`.
import { describe, expect, it } from 'vitest';
import type { ConnectionEdge, LObject } from './api.ts';
import {
  BAND_GAP,
  BAND_HEAD,
  BAND_PAD,
  HEADER_H,
  TASK_STEP,
  nodeBands,
  publishedPorts,
  seenSummary,
  serviceHealth,
  tops,
  volumeUses
} from './topology.ts';

const svc = (name: string, spec: object = {}, status?: object): LObject =>
  ({ kind: 'Service', metadata: { name, namespace: 'shop' }, spec, status }) as unknown as LObject;
const task = (name: string, service: string, node?: string): LObject =>
  ({ kind: 'Task', metadata: { name, namespace: 'shop' }, spec: { service, node } }) as unknown as LObject;
const edge = (
  from: ConnectionEdge['from'],
  to: ConnectionEdge['to'],
  port: number,
  connections: number
): ConnectionEdge =>
  ({ from, to, port, protocol: 'tcp', via: 'task', connections, clients: 1 }) as unknown as ConnectionEdge;

describe('the topology', () => {
  it('lists published ports with where they are answered, null until published', () => {
    const web = svc(
      'web',
      {
        endpoint: {
          ports: [
            { port: 80, publish: 'ingress' },
            { port: 53, protocol: 'udp', publish: 'host' },
            { port: 9000 }
          ]
        }
      },
      { publishedPorts: [{ port: 80, publishedPort: 30080 }] }
    );
    expect(publishedPorts([web])).toEqual([
      { service: 'web', what: '80/tcp', at: 30080, host: false },
      { service: 'web', what: '53/udp', at: null, host: true }
    ]);
  });

  it('lists the Volumes each Service mounts, not its other pod volumes', () => {
    const db = svc('db', { pod: { volumes: [{ volume: { name: 'pg' } }, { tmpfs: {} }] } });
    expect(volumeUses([db, svc('web')])).toEqual([{ service: 'db', volume: 'pg' }]);
  });

  it('sums connections seen between shown Services and into each from outside', () => {
    const s = (name: string) => ({ kind: 'service', name }) as ConnectionEdge['from'];
    const seen = [
      edge(s('web'), s('db'), 5432, 3),
      edge(s('web'), s('db'), 6432, 2),
      edge(s('db'), s('db'), 5432, 9),
      edge({ kind: 'outside', name: '' } as ConnectionEdge['from'], s('web'), 443, 4),
      edge(s('other'), s('web'), 80, 1),
      edge(s('web'), s('other'), 80, 7)
    ];
    const { between, into } = seenSummary(seen, (n) => n === 'web' || n === 'db');
    expect(between).toEqual([{ from: 'web', to: 'db', n: 5, what: ['5432/tcp', '6432/tcp'] }]);
    expect(into).toEqual([{ from: '', to: 'web', n: 5, what: ['outside', 'other'] }]);
  });

  it('says a Service is idle, good, waiting or bad by how many are ready', () => {
    expect(serviceHealth(svc('a'))).toBe('wait');
    expect(serviceHealth(svc('a', {}, { desired: 0 }))).toBe('idle');
    expect(serviceHealth(svc('a', {}, { desired: 2, ready: 2 }))).toBe('good');
    expect(serviceHealth(svc('a', {}, { desired: 2, ready: 1 }))).toBe('wait');
    expect(serviceHealth(svc('a', {}, { desired: 2 }))).toBe('bad');
  });

  it('starts each row where the one before ends', () => {
    expect(tops([112, 160, 112])).toEqual([HEADER_H, HEADER_H + 112, HEADER_H + 272]);
  });

  it('bands pods by node in their services order, the unplaced last', () => {
    const order = new Map([
      ['web', 0],
      ['db', 1]
    ]);
    const tasks = [
      task('db-1', 'db', 'n1'),
      task('web-2', 'web', 'n1'),
      task('web-1', 'web', 'n2'),
      task('web-3', 'web'),
      task('gone-1', 'gone', 'n1')
    ];
    const bands = nodeBands(tasks, ['n1', 'n2', 'n3'], order);
    expect(bands.map((b) => [b.node, b.tasks.map((t) => t.metadata.name)])).toEqual([
      ['n1', ['web-2', 'db-1']],
      ['n2', ['web-1']],
      ['n3', []],
      ['', ['web-3']]
    ]);
    const h = (n: number) => BAND_HEAD + Math.max(1, n) * TASK_STEP + BAND_PAD;
    expect(bands[0].top).toBe(HEADER_H);
    expect(bands[1].top).toBe(HEADER_H + h(2) + BAND_GAP);
    expect(bands[2].h).toBe(h(0));
  });
});

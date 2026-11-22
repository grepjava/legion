// The console's pure helpers, checked without a browser or a cluster:
// `npm test`.
import { describe, expect, it } from 'vitest';
import type { LObject } from './api.ts';
import { matches, parse } from './filter.ts';
import { byteCount, cores } from './quantity.ts';
import { sumHeld } from './series.ts';
import { toYaml } from './yaml.ts';

const obj = (name: string, namespace = 'shop', labels: Record<string, string> = {}): LObject =>
  ({ kind: 'Service', metadata: { name, namespace, labels }, spec: {} }) as unknown as LObject;

describe('the list filter', () => {
  it('reads words as names and key=value, key!=value and !key as labels', () => {
    expect(parse('web app=shop tier!=db !canary')).toEqual([
      { name: 'web' },
      { key: 'app', value: 'shop', not: false },
      { key: 'tier', value: 'db', not: true },
      { key: 'canary', has: false }
    ]);
    // `==` reads as `=`; a label-like word without `=` is a name.
    expect(parse('app==shop app.kubernetes.io/name')).toEqual([
      { key: 'app', value: 'shop', not: false },
      { name: 'app.kubernetes.io/name' }
    ]);
    expect(parse('  , ')).toEqual([]);
  });

  it('keeps what every term holds for', () => {
    const web = obj('web', 'shop', { app: 'shop', tier: 'front' });
    expect(matches(web, parse('web'))).toBe(true);
    expect(matches(web, parse('shop/web'))).toBe(true);
    expect(matches(web, parse('app=shop tier!=db'))).toBe(true);
    expect(matches(web, parse('app=bank'))).toBe(false);
    expect(matches(web, parse('!tier'))).toBe(false);
    expect(matches(web, parse('!canary web'))).toBe(true);
    expect(matches(web, parse('web db'))).toBe(false);
  });
});

describe('quantities', () => {
  it('reads cores and millicores', () => {
    expect(cores('8')).toBe(8);
    expect(cores('500m')).toBe(0.5);
    expect(cores('1.5')).toBe(1.5);
    expect(cores(2)).toBe(2);
    expect(cores('2Gi')).toBe(0);
    expect(cores(undefined)).toBe(0);
  });

  it('reads bytes with binary and decimal suffixes', () => {
    expect(byteCount('1024')).toBe(1024);
    expect(byteCount('64Mi')).toBe(64 * 2 ** 20);
    expect(byteCount('7683460Ki')).toBe(7683460 * 1024);
    expect(byteCount('1G')).toBe(1e9);
    expect(byteCount('1k')).toBe(1000);
    expect(byteCount('1.5Gi')).toBe(1.5 * 2 ** 30);
    expect(byteCount('500m')).toBe(0);
    expect(byteCount(null)).toBe(0);
  });
});

describe('summing series sampled apart', () => {
  it('counts each series once a step, its last sample held', () => {
    // Two tasks sampled every 15 s, 5 s apart: each step has both.
    const a: [number, number][] = [
      [0, 1],
      [15_000, 1],
      [30_000, 1]
    ];
    const b: [number, number][] = [
      [5_000, 2],
      [20_000, 2],
      [35_000, 2]
    ];
    const sum = sumHeld([a, b], 15_000);
    expect(sum.map(([, v]) => v)).toEqual([3, 3, 3]);
  });

  it('drops a series held longer than it may be', () => {
    const gone: [number, number][] = [[0, 5]];
    const on: [number, number][] = [
      [0, 1],
      [15_000, 1],
      [30_000, 1],
      [45_000, 1]
    ];
    // Held two steps after its last sample, then no more.
    expect(sumHeld([gone, on], 15_000, 2).map(([, v]) => v)).toEqual([6, 6, 6, 1]);
  });

  it('sorts samples given in any order, and has nothing of nothing', () => {
    expect(
      sumHeld(
        [
          [
            [15_000, 2],
            [0, 1]
          ]
        ],
        15_000
      )
    ).toEqual([
      [0, 1],
      [15_000, 2]
    ]);
    expect(sumHeld([[], []], 15_000)).toEqual([]);
  });
});

describe('YAML as legion get writes it', () => {
  it('writes block style, quoting only what would read as something else', () => {
    expect(
      toYaml({
        apiVersion: 'legion/v1',
        kind: 'Service',
        metadata: { name: 'web', labels: {} },
        spec: { replicas: 2, paused: false, image: 'nginx:1.27', ports: [8080], env: [{ name: 'MODE', value: 'yes' }] }
      })
    ).toBe(
      [
        'apiVersion: legion/v1',
        'kind: Service',
        'metadata:',
        '  name: web',
        '  labels: {}',
        'spec:',
        '  replicas: 2',
        '  paused: false',
        '  image: nginx:1.27',
        '  ports:',
        '  - 8080',
        '  env:',
        '  - name: MODE',
        '    value: "yes"',
        ''
      ].join('\n')
    );
  });

  it('quotes strings that would read as numbers, booleans, null or keys', () => {
    expect(toYaml(['1.0', 'true', 'null', '', 'a: b', 'end:', 'x #y', 'plain'])).toBe(
      ['- "1.0"', '- "true"', '- "null"', '- ""', '- "a: b"', '- "end:"', '- "x #y"', '- plain', ''].join('\n')
    );
  });

  it('writes a lone value on its own line', () => {
    expect(toYaml('text')).toBe('text\n');
    expect(toYaml([])).toBe('[]\n');
    expect(toYaml(null)).toBe('null\n');
  });
});

// The Images page's shared state: `npm test`.
import { describe, expect, it } from 'vitest';
import { fromPackages, pkgName, Studio } from './studio.svelte.ts';

describe('the image studio', () => {
  it('chooses a Secret just saved in every form, listed once', () => {
    const s = new Studio(
      () => 'shop',
      () => null
    );
    s.secrets = ['b', 'ghcr'];
    s.saved('ghcr');
    s.saved('a');
    expect(s.secrets).toEqual(['a', 'b', 'ghcr']);
    expect([s.pullSecret, s.pushSecret, s.buildSecret]).toEqual(['a', 'a', 'a']);
  });

  it('says what work returned, or the error it threw, busy meanwhile', async () => {
    const s = new Studio(
      () => 'shop',
      () => null
    );
    s.error = 'old';
    let during = false;
    expect(
      await s.act(async () => {
        during = s.busy;
        return 'done';
      })
    ).toBe(true);
    expect([during, s.busy, s.message, s.error]).toEqual([true, false, 'done', '']);
    expect(
      await s.act(async () => {
        throw new Error('refused');
      })
    ).toBe(false);
    expect([s.busy, s.message, s.error]).toEqual([false, '', 'refused']);
  });

  it('reads a search as a package name and builds an image installing it', () => {
    expect(pkgName('  PostgreSQL17-Server! ')).toBe('postgresql17-server');
    expect(pkgName('c++ py3.11')).toBe('c++py3.11');
    expect(fromPackages('nginx')).toContain('pkg install -y nginx && pkg clean -ay');
  });
});

<script lang="ts">
  import { ask } from '#lib/confirm.svelte.ts';
  // Stacks: the services a compose file became (`legion stack deploy`, the
  // Import page), labelled `legion.stack`, shown and stopped, started or
  // removed together. Removing one keeps its Volumes and their data, as
  // `legion stack rm` does.
  import { api, follow, type LObject } from '#lib/api.ts';
  import { t } from '#lib/i18n.svelte.ts';
  import { cannotWrite, session } from '#lib/session.svelte.ts';

  const STACK = 'legion.stack';

  let services: LObject[] = $state([]);
  let error = $state('');
  let message = $state('');
  let busy = $state('');

  $effect(() => {
    const ns = session.namespace;
    error = '';
    return follow('Service', ns, (o) => (services = o), (e) => (error = e.message));
  });

  interface Stack {
    namespace: string;
    name: string;
    services: LObject[];
    ready: number;
    desired: number;
    stopped: number;
  }

  const stacks: Stack[] = $derived.by(() => {
    const by = new Map<string, Stack>();
    for (const s of services) {
      const name = s.metadata.labels?.[STACK];
      if (!name) continue;
      const namespace = s.metadata.namespace ?? '';
      const k = `${namespace}/${name}`;
      const st = by.get(k) ?? { namespace, name, services: [], ready: 0, desired: 0, stopped: 0 };
      st.services.push(s);
      st.ready += s.status?.ready ?? 0;
      st.desired += s.status?.desired ?? 0;
      if (s.spec?.stopped) st.stopped += 1;
      by.set(k, st);
    }
    return [...by.values()].sort((a, b) => `${a.namespace}/${a.name}`.localeCompare(`${b.namespace}/${b.name}`));
  });

  function condition(st: Stack): string {
    if (st.stopped === 0) return t('running');
    return st.stopped === st.services.length ? t('stopped') : t('partly stopped');
  }

  async function each(st: Stack, what: string, run: (s: LObject) => Promise<unknown>) {
    busy = `${st.namespace}/${st.name}`;
    message = '';
    error = '';
    try {
      for (const s of st.services) await run(s);
      message = t('{stack}: {what}', { stack: st.name, what });
    } catch (err) {
      error = (err as Error).message;
    } finally {
      busy = '';
    }
  }

  const stopAll = (st: Stack, stop: boolean) =>
    each(st, stop ? t('stopped') : t('started'), (s) =>
      api.set('Service', st.namespace, s.metadata.name, 'stopped', stop)
    );

  async function remove(st: Stack) {
    if (!(await ask(t('Remove stack {stack}: its services, Secrets and Configs? Its Volumes and their data stay.', { stack: st.name }))))
      return;
    busy = `${st.namespace}/${st.name}`;
    error = '';
    try {
      for (const kind of ['Service', 'Secret', 'Config']) {
        const all = (await api.list(kind, st.namespace)).objects;
        for (const o of all.filter((o) => o.metadata.labels?.[STACK] === st.name)) {
          await api.remove(kind, st.namespace, o.metadata.name);
        }
      }
      message = t('{stack} removed; its Volumes stay', { stack: st.name });
    } catch (err) {
      error = (err as Error).message;
    } finally {
      busy = '';
    }
  }
</script>

<h1>{t('Stacks')}</h1>
<p class="muted">
  {t('The services a compose file became, from the Import page or legion stack deploy, together.')}
  <a href="/import">{t('Import a compose file')}</a>
</p>
{#if error}<p class="error" role="alert">{error}</p>{/if}
{#if message}<p class="notice" role="status">{message}</p>{/if}

{#each stacks as st (`${st.namespace}/${st.name}`)}
  {@const why = cannotWrite(st.namespace)}
  <section aria-labelledby="stack-{st.namespace}-{st.name}">
    <h2 id="stack-{st.namespace}-{st.name}">
      {st.name}
      <span class="muted">{st.namespace}</span>
      <span class="state {st.stopped ? 'wait' : st.ready >= st.desired ? 'good' : 'wait'}">{condition(st)}</span>
    </h2>
    <p>
      {t('{n} services, {ready} of {desired} tasks ready', {
        n: st.services.length,
        ready: st.ready,
        desired: st.desired
      })}
    </p>
    <ul class="mono">
      {#each st.services as s (s.metadata.name)}
        <li>
          <a href="/k/Service/{st.namespace}/{s.metadata.name}">{s.metadata.name}</a>
          {#if s.spec?.stopped}<span class="muted">{t('stopped')}</span>
          {:else}<span class="muted">{s.status?.ready ?? 0}/{s.status?.desired ?? 0}</span>{/if}
        </li>
      {/each}
    </ul>
    <div class="controls">
      <button
        type="button"
        disabled={!!why || !!busy || st.stopped === st.services.length}
        title={why ?? ''}
        onclick={async () =>
          (await ask(t('Stop every service of {stack}?', { stack: st.name }), { action: t('Stop all') })) &&
          stopAll(st, true)}
        >{t('Stop all')}</button
      >
      <button
        type="button"
        disabled={!!why || !!busy || st.stopped === 0}
        title={why ?? ''}
        onclick={() => stopAll(st, false)}>{t('Start all')}</button
      >
      <button type="button" class="danger" disabled={!!why || !!busy} title={why ?? ''} onclick={() => remove(st)}
        >{t('Remove')}</button
      >
    </div>
  </section>
{:else}
  <p class="muted">{t('No stacks here yet.')}</p>
{/each}

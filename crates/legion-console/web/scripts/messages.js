// Lists every string the console translates (`t('…')`, `tn('…', '…', n)`
// in src) into messages.json, the file a translator copies to
// src/lib/locales/<code>.json and fills in. With --check it changes
// nothing and fails if messages.json is not current, or if a call's text
// is not written out where the script can read it.
import { readFileSync, readdirSync, statSync, writeFileSync } from 'node:fs';
import { join, relative } from 'node:path';

const root = new URL('..', import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, '$1');
const src = join(root, 'src');
const out = join(root, 'messages.json');

function files(dir) {
  return readdirSync(dir).flatMap((name) => {
    const p = join(dir, name);
    if (statSync(p).isDirectory()) return files(p);
    return /\.(svelte|ts)$/.test(name) && !name.endsWith('.d.ts') ? [p] : [];
  });
}

const QUOTED = String.raw`(?:'((?:\\.|[^'\\])*)'|"((?:\\.|[^"\\])*)"|\`((?:\\.|[^\`\\$])*)\`)`;
const single = new RegExp(String.raw`(?<![\w.$])t\(\s*${QUOTED}`, 'g');
const counted = new RegExp(String.raw`(?<![\w.$])tn\(\s*${QUOTED}\s*,\s*${QUOTED}`, 'g');
const anyCall = /(?<![\w.$])tn?\(\s*(?!\))/g;

const unescape = (s) => s.replace(/\\(.)/g, (_, c) => ({ n: '\n', t: '\t' })[c] ?? c);
const pick = (m, at) => unescape(m[at] ?? m[at + 1] ?? m[at + 2]);

const messages = {};
const problems = [];
for (const file of files(src)) {
  if (file.endsWith('i18n.svelte.ts')) continue;
  const text = readFileSync(file, 'utf8');
  const found = new Set();
  for (const m of text.matchAll(single)) {
    messages[pick(m, 1)] = '';
    found.add(m.index);
  }
  for (const m of text.matchAll(counted)) {
    messages[pick(m, 4)] = { one: '', other: '' };
    found.add(m.index);
  }
  for (const m of text.matchAll(anyCall)) {
    if (!found.has(m.index)) {
      const line = text.slice(0, m.index).split('\n').length;
      problems.push(`${relative(root, file)}:${line}: the text is not written out`);
    }
  }
}
// Only the English singular of a count is never a key of its own.
const sorted = Object.fromEntries(Object.keys(messages).sort().map((k) => [k, messages[k]]));
const json = JSON.stringify(sorted, null, 2) + '\n';

if (problems.length) {
  console.error(problems.join('\n'));
  process.exit(1);
}
if (process.argv.includes('--check')) {
  let current = '';
  try {
    current = readFileSync(out, 'utf8');
  } catch {
    // Missing: not current.
  }
  if (current !== json) {
    console.error('messages.json is not current: run npm run messages');
    process.exit(1);
  }
  console.log(`${Object.keys(sorted).length} messages, current`);
} else {
  writeFileSync(out, json);
  console.log(`${Object.keys(sorted).length} messages written to messages.json`);
}

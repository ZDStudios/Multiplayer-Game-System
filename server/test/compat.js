// Backward-compatibility test: every SDK version that ever shipped (test/compat/sdk-*.cjs, copied from git history)
// must keep working against the current server, on its own and in the same room as the current SDK.
// Add a copy of client/multiplayer.js here whenever a release changes it.
import { spawn } from 'node:child_process';
import { createRequire } from 'node:module';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import assert from 'node:assert/strict';

const require = createRequire(import.meta.url);
const here = import.meta.dirname;
const SDKS = fs.readdirSync(path.join(here, 'compat')).filter((f) => /^sdk-.*\.cjs$/.test(f)).sort()
  .map((f) => [f.slice(4, -4), require(path.join(here, 'compat', f))]);
const Current = require('../../client/multiplayer.js');
const PORT = 21000 + Math.floor(Math.random() * 1000);
const BASE = `http://127.0.0.1:${PORT}`;
const ADMIN = 'compat_admin_token_1234567890';
const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mpc-'));
const proc = spawn(process.execPath, ['--disable-warning=ExperimentalWarning', 'src/index.js'], {
  cwd: path.resolve(here, '..'),
  env: { ...process.env, PORT, DATA_DIR: dir, ADMIN_TOKEN: ADMIN, RECONNECT_GRACE_MS: '800', UDP_PORT: String(PORT + 1000), RTC_PORT: String(PORT + 2000) },
  stdio: ['ignore', 'inherit', 'inherit'],
});
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const api = async (method, p, body) => {
  const r = await fetch(BASE + '/api/admin' + p, { method, headers: { authorization: 'Bearer ' + ADMIN, 'content-type': 'application/json' }, body: body && JSON.stringify(body) });
  return r.json();
};
const waitFor = async (fn, ms = 3000) => { const t = Date.now(); while (!(await fn())) { if (Date.now() - t > ms) throw new Error('waitFor timeout: ' + fn); await sleep(20); } };
let n = 0;
const ok = (msg) => console.log(`  ok ${++n}. ${msg}`);

async function scenario(label, SDK, game) {
  const mk = async (name) => { const x = new SDK({ url: BASE, key: game.key, name }); await x.connect(); return x; };
  const a = await mk('A_' + label), b = await mk('B_' + label);
  const room = await a.create({ maxPlayers: 4, meta: { mode: 'ffa' } });
  await b.join(room.id);
  assert.equal(b.players.size, 2);
  await a.setState('board.0', 'X');
  await waitFor(() => b.getState('board.0') === 'X');
  await b.batchState({ turn: 'a' });
  await waitFor(() => a.state.turn === 'a');
  b.setPlayer({ x: 1, y: 2 });
  await waitFor(() => a.players.get(b.playerId) && a.players.get(b.playerId).data.x === 1);
  const got = new Promise((r) => b.onMessage('chat', (d, from) => r([d, from])));
  a.send('chat', { t: 1 });
  assert.deepEqual(await got, [{ t: 1 }, a.playerId]);
  ok(`${label}: rooms, shared state, player data, messages`);

  const setRes = await a.storage.set('save', { lvl: 2 });
  if (label === 'v1-original') assert.equal(setRes, true); else assert.ok(setRes); // v1 gets exactly what it always got
  assert.deepEqual(await b.storage.get('save', { player: a.playerId }), { lvl: 2 });
  assert.equal(await a.storage.incr('plays_' + label, 1, { scope: 'game' }), 1);
  const list = await a.storage.list({ scope: 'player' });
  assert.ok(Array.isArray(list) && list.some((x) => x.key === 'save' && x.value.lvl === 2));
  ok(`${label}: storage get/set/incr/list`);

  await a.leaderboard.submit('lap_' + label, 50, { mode: 'min' });
  await b.leaderboard.submit('lap_' + label, 40, { mode: 'min' });
  await a.leaderboard.submit('lap_' + label, 60, { mode: 'min' });
  const top = await a.leaderboard.top('lap_' + label, { order: 'asc' });
  assert.deepEqual(top.entries.map((e) => e.score), [40, 50]);
  assert.equal(top.me.rank, 2); assert.equal(top.me.score, 50);
  await a.leaderboard.submit('hi_' + label, 10, { mode: 'set' });
  await a.leaderboard.submit('hi_' + label, 3, { mode: 'set' });
  assert.equal((await a.leaderboard.top('hi_' + label)).entries[0].score, 3);
  ok(`${label}: leaderboards (min + order:asc, set, rank)`);

  const user = ('u_' + label).toLowerCase().replace(/[^a-z0-9_]/g, '_').slice(0, 24);
  await a.auth.register(user, 'hunter22');
  const c = await mk('C_' + label);
  assert.equal((await c.auth.login(user, 'hunter22')).playerId, a.playerId);
  ok(`${label}: accounts register + login`);

  const rj = new Promise((r) => b.once('reconnected', r));
  b._ws.close();
  await rj;
  assert.equal(b.room && b.room.id, room.id); assert.equal(b.getState('board.0'), 'X');
  ok(`${label}: auto-reconnect rejoins with state`);

  const q1 = await c.quickJoin('arena_' + label, { maxPlayers: 2 });
  const d = await mk('D_' + label);
  assert.equal((await d.quickJoin('arena_' + label, { maxPlayers: 2 })).id, q1.id);
  assert.ok((await d.listRooms('arena_' + label)).length === 1);
  assert.equal(typeof (await d.ping()), 'number');
  ok(`${label}: quickJoin, listRooms, ping`);
  for (const x of [a, b, c, d]) x.disconnect();
}

try {
  for (let i = 0; ; i++) { try { await fetch(BASE + '/health'); break; } catch { if (i > 50) throw new Error('server did not start'); await sleep(100); } }
  const game = await api('POST', '/games', { name: 'Compat' });
  for (const [label, SDK] of [...SDKS, ['current', Current]]) await scenario(label, SDK, game);

  // Mixed room: every old SDK together with the current one.
  const cur = new Current({ url: BASE, key: game.key, name: 'Now' });
  await cur.connect({ room: 'MIX' });
  const olds = [];
  for (const [label, SDK] of SDKS) { const o = new SDK({ url: BASE, key: game.key, name: label }); await o.connect(); await o.join('MIX'); olds.push(o); }
  await cur.setState('round', 7);
  await waitFor(() => olds.every((o) => o.state.round === 7));
  cur.setPlayer({ x: 9 }, { unreliable: true });
  await waitFor(() => olds.every((o) => o.players.get(cur.playerId).data.x === 9));
  olds[0].setPlayer({ y: 5 });
  await waitFor(() => cur.players.get(olds[0].playerId).data.y === 5);
  const heard = new Promise((r) => cur.onMessage('hello', (d) => r(d)));
  olds[0].send('hello', 'from v1');
  assert.equal(await heard, 'from v1');
  ok(`mixed room: ${SDKS.map((s) => s[0]).join(', ')} + current share state, player data and messages`);
  for (const o of [cur, ...olds]) o.disconnect();
  console.log(`\nAll ${n} compatibility checks passed (${SDKS.length} old SDK versions + current).`);
} catch (err) {
  console.error('\nFAILED:', err);
  process.exitCode = 1;
} finally {
  proc.kill();
  setTimeout(() => process.exit(), 300);
}

// End-to-end smoke test: boots the server on a temp dir, drives it with the real SDK.
import { spawn } from 'node:child_process';
import { createRequire } from 'node:module';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import assert from 'node:assert/strict';

const require = createRequire(import.meta.url);
const Multiplayer = require('../../client/multiplayer.js');
const PORT = 18000 + Math.floor(Math.random() * 1000);
const BASE = `http://127.0.0.1:${PORT}`;
const ADMIN = 'test_admin_token_1234567890';
const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mp-'));
const proc = spawn(process.execPath, ['--disable-warning=ExperimentalWarning', 'src/index.js'], {
  cwd: path.resolve(import.meta.dirname, '..'),
  env: { ...process.env, PORT, DATA_DIR: dir, ADMIN_TOKEN: ADMIN, RECONNECT_GRACE_MS: '800' },
  stdio: ['ignore', 'inherit', 'inherit'],
});
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const api = async (method, p, body, token = ADMIN) => {
  const r = await fetch(BASE + '/api/admin' + p, { method, headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json' }, body: body && JSON.stringify(body) });
  return { status: r.status, body: await r.json() };
};
const waitFor = async (fn, ms = 2000) => { const t = Date.now(); while (!fn()) { if (Date.now() - t > ms) throw new Error('waitFor timeout: ' + fn); await sleep(20); } };
let n = 0;
const ok = (msg) => console.log(`  ok ${++n}. ${msg}`);

try {
  for (let i = 0; ; i++) { try { await fetch(BASE + '/health'); break; } catch { if (i > 50) throw new Error('server did not start'); await sleep(100); } }

  assert.equal((await api('GET', '/overview', null, 'wrong')).status, 401); ok('admin auth rejects bad token');
  const game = (await api('POST', '/games', { name: 'Smoke' })).body;
  assert.match(game.key, /^pk_/); ok('create game via admin API');

  const a = new Multiplayer({ url: BASE, key: game.key, name: 'Alice' });
  const b = new Multiplayer({ url: BASE, key: game.key, name: 'Bob' });
  await a.connect(); await b.connect();
  assert.ok(a.playerId && a.playerId !== b.playerId); ok('two clients connect with distinct ids');

  await assert.rejects(new Multiplayer({ url: BASE, key: 'pk_nope' }).connect()); ok('bad game key rejected');

  const room = await a.create({ maxPlayers: 4, meta: { mode: 'ffa' } });
  assert.equal(room.id.length, 6); assert.ok(a.isHost);
  const joinedEv = new Promise((r) => a.once('playerJoined', r));
  await b.join(room.id);
  assert.equal((await joinedEv).name, 'Bob'); assert.equal(b.players.size, 2); ok('create room code + join, playerJoined fires');

  await a.setState('board.cells.0', 'X');
  await waitFor(() => b.getState('board.cells.0') === 'X'); ok('shared state syncs (dot paths)');
  await b.batchState({ turn: 'a', score: { a: 1 } });
  await waitFor(() => a.state.turn === 'a' && a.state.score.a === 1); ok('batch state');

  b.setPlayer({ x: 10, y: 20 });
  await waitFor(() => a.players.get(b.playerId).data.x === 10); ok('per-player data broadcast');

  const got = new Promise((r) => b.onMessage('chat', (d, from) => r([d, from])));
  a.send('chat', { text: 'hi' });
  assert.deepEqual(await got, [{ text: 'hi' }, a.playerId]); ok('relay messages');
  const toHost = new Promise((r) => a.onMessage('input', r));
  b.send('input', { k: 'jump' }, 'host');
  assert.deepEqual(await toHost, { k: 'jump' }); ok('send to host');

  await a.updateRoom({ stateWrite: 'host' });
  await assert.rejects(b.setState('hack', 1), /not_host|host-only/i); ok('host-authoritative state lock');
  await a.updateRoom({ stateWrite: 'all' });

  await a.storage.set('profile', { level: 3 });
  assert.deepEqual(await b.storage.get('profile', { player: a.playerId }), { level: 3 }); ok('player storage readable by others');
  await a.storage.set('secret', 42, { scope: 'private' });
  assert.equal(await b.storage.get('secret', { scope: 'private' }), null); ok('private storage isolated');
  assert.equal(await a.storage.incr('plays', 1, { scope: 'game' }), 1);
  assert.equal(await b.storage.incr('plays', 2, { scope: 'game' }), 3); ok('atomic game counter');
  assert.equal((await a.storage.list({ scope: 'player' })).length, 1); ok('list keys');

  await a.leaderboard.submit('high', 100); await b.leaderboard.submit('high', 250); await a.leaderboard.submit('high', 50);
  const top = await a.leaderboard.top('high');
  assert.deepEqual(top.entries.map((e) => e.score), [250, 100]); assert.equal(top.me.rank, 2); ok('leaderboard (max mode, rank)');

  const reg = await a.auth.register('alice_1', 'hunter22');
  assert.equal(reg.playerId, a.playerId);
  await assert.rejects(b.auth.register('alice_1', 'whatever'), /taken/); ok('account register + unique username');
  const c = new Multiplayer({ url: BASE, key: game.key, name: 'Alice2' });
  await c.connect();
  await assert.rejects(c.auth.login('alice_1', 'wrongpw'));
  const li = await c.auth.login('alice_1', 'hunter22');
  assert.equal(li.playerId, reg.playerId); ok('login restores account identity (and kicks old session)');
  await waitFor(() => !a.connected);
  await waitFor(() => !b.players.has(a.playerId) || b.players.get(a.playerId).connected === false); ok('old session marked disconnected');
  await waitFor(() => b.isHost, 3000); ok('host migrates to remaining player');

  const ov = (await api('GET', '/overview')).body;
  assert.equal(ov.games[0].online, 2); ok('admin overview reports live counts');
  const detail = (await api('GET', `/games/${game.id}`)).body;
  assert.equal(detail.rooms.length, 1); ok('admin game detail lists rooms');
  const ann = new Promise((r) => b.onMessage('announcement', (d, from) => r(from)));
  await api('POST', `/games/${game.id}/broadcast`, { data: 'hello' });
  assert.equal(await ann, 'server'); ok('admin broadcast');

  const rj = new Promise((r) => b.once('reconnected', r));
  b._ws.close(); // simulate a network drop (not user-initiated)
  await rj;
  assert.equal(b.room && b.room.id, room.id); assert.equal(b.getState('board.cells.0'), 'X'); ok('auto-reconnect rejoins room with state');

  const q1 = await c.quickJoin('ranked', { maxPlayers: 2 });
  const d = new Multiplayer({ url: BASE, key: game.key }); await d.connect();
  const q2 = await d.quickJoin('ranked', { maxPlayers: 2 });
  assert.equal(q1.id, q2.id);
  const e = new Multiplayer({ url: BASE, key: game.key }); await e.connect();
  assert.notEqual((await e.quickJoin('ranked', { maxPlayers: 2 })).id, q1.id); ok('quickJoin matchmaking fills then spills');

  await api('PATCH', `/games/${game.id}`, { enabled: false });
  await waitFor(() => !e.connected); ok('disabling game disconnects clients');

  for (const x of [a, b, c, d, e]) x.disconnect();
  console.log(`\nAll ${n} checks passed.`);
} catch (err) {
  console.error('\nFAILED:', err);
  process.exitCode = 1;
} finally {
  proc.kill();
  setTimeout(() => process.exit(), 300);
}

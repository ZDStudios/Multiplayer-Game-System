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

  assert.ok((await api('GET', '/overview')).body.players.some((p) => p.id === d.playerId)); ok('overview lists online players');
  const dKicked = new Promise((r) => d.once('kicked', r));
  await api('POST', `/games/${game.id}/players/${d.playerId}/kick`, {});
  assert.equal(await dKicked, 'kicked');
  await waitFor(() => !c.players.has(d.playerId)); ok('admin kick disconnects player + removes from room');
  await d.connect(); ok('kicked (not banned) player can reconnect');
  await api('POST', `/games/${game.id}/players/${d.playerId}/kick`, { ban: true });
  await waitFor(() => !d.connected);
  await assert.rejects(d.connect()); ok('banned player cannot reconnect');
  await api('POST', `/games/${game.id}/players/${d.playerId}/unban`);
  await d.connect(); ok('unban restores access');

  // ---- latency features ----
  const o1 = new Multiplayer({ url: BASE, key: game.key });
  await o1.connect({ room: 'OPTI' }); // pipelined join
  assert.equal(o1.room && o1.room.id, 'OPTI'); ok('connect({room}) joins in the same round trip');
  const o2 = new Multiplayer({ url: BASE, key: game.key }); await o2.connect({ room: 'OPTI' });
  let evs = 0; o1.on('state', () => evs++);
  const p1 = o1.setState('hp', 50);
  assert.equal(o1.state.hp, 50); ok('optimistic setState applies locally before the server replies');
  await p1; await waitFor(() => o2.state.hp === 50); await sleep(50);
  assert.equal(evs, 1); ok('own echo is deduplicated (one state event)');
  await o1.updateRoom({ stateWrite: 'host' }).catch(() => {});
  if (!o2.isHost) { await assert.rejects(o2.setState('hp', 1)); assert.equal(o2.state.hp, 50); ok('rejected optimistic write rolls back'); }
  await o1.updateRoom({ stateWrite: 'all' }).catch(() => {});
  o2.setPlayer({ x: 0 }); await sleep(100); o2.setPlayer({ x: 10 });
  await waitFor(() => o1.players.get(o2.playerId).data.x === 10);
  assert.ok(o1.predict(o2.playerId, ['x']).x > 10); ok('predict() extrapolates remote motion');

  const script = `
    let loops = 0;
    module.exports = {
      authoritative: true, tickRate: 30,
      onCreate(room) { room.setState('score', {}); room.data.ticks = 0; },
      onJoin(room, p) { room.setState('score.' + p.id, 0); },
      onMessage(room, p, type, data) {
        if (type === 'hit') room.batchState([['score.' + p.id, room.getState('score.' + p.id) + data.dmg], ['last', p.id]]);
        if (type === 'boom') throw new Error('kaboom');
        if (type === 'hang') { while (true) loops++; }
        if (type === 'ask') room.send('answer', 42, p.id);
      },
      onTick(room) { room.data.ticks++; if (room.data.ticks === 3) room.setState('ticked', true); },
    };`;
  assert.equal((await api('PUT', `/games/${game.id}/script`, { code: script })).body.loaded, true); ok('upload server script via admin API');
  assert.equal((await api('PUT', `/games/${game.id}/script`, { code: 'module.exports = {' })).status, 400); ok('broken script rejected with 400');
  await api('PUT', `/games/${game.id}/script`, { code: script });
  const s1 = new Multiplayer({ url: BASE, key: game.key }); await s1.connect({ room: 'SRV' });
  const s2 = new Multiplayer({ url: BASE, key: game.key }); await s2.connect({ room: 'SRV' });
  await waitFor(() => s2.state.score && s2.state.score[s1.playerId] === 0 && s2.state.ticked === true); ok('onCreate/onJoin/onTick drive state');
  s1.send('hit', { dmg: 7 }, 'server');
  await waitFor(() => s2.state.score[s1.playerId] === 7 && s2.state.last === s1.playerId); ok('client -> server script -> everyone (1 round trip)');
  await assert.rejects(s1.setState('score', 999), /authoritative/); ok('authoritative script blocks direct client writes');
  const ans = new Promise((r) => s1.onMessage('answer', (d, from) => r([d, from])));
  s1.send('ask', null, 'server');
  assert.deepEqual(await ans, [42, 'server']); ok('script can message a player');
  s1.send('boom', null, 'server'); s1.send('hang', null, 'server');
  await sleep(300);
  assert.ok((await s2.ping()) < 1000);
  const info = (await api('GET', `/games/${game.id}/script`)).body;
  assert.ok(info.errors.some((e) => /kaboom/.test(e.message)) && info.errors.some((e) => /timed out/i.test(e.message))); ok('script errors + infinite loops are contained (server stays up)');
  await api('DELETE', `/games/${game.id}/script`);
  await s1.setState('free', 1); ok('removing script restores client writes');
  await sleep(5200);
  assert.ok((await api('GET', '/overview')).body.players.some((p) => typeof p.rtt === 'number')); ok('server measures per-player RTT');
  for (const x of [o1, o2, s1, s2]) x.disconnect();

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

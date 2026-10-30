// End-to-end smoke test: boots the server on a temp dir, drives it with the real SDK.
import { spawn } from 'node:child_process';
import { createRequire } from 'node:module';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import assert from 'node:assert/strict';
import { RTCPeerConnection } from 'node-datachannel/polyfill';

const require = createRequire(import.meta.url);
const Multiplayer = require('../../client/multiplayer.js');
const PORT = 18000 + Math.floor(Math.random() * 1000);
const UDP_PORT = PORT + 1000, RTC_PORT = PORT + 2000;
const BASE = `http://127.0.0.1:${PORT}`;
const ADMIN = 'test_admin_token_1234567890';
const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mp-'));
const proc = spawn(process.execPath, ['--disable-warning=ExperimentalWarning', 'src/index.js'], {
  cwd: path.resolve(import.meta.dirname, '..'),
  env: { ...process.env, PORT, DATA_DIR: dir, ADMIN_TOKEN: ADMIN, RECONNECT_GRACE_MS: '800', UDP_PORT, RTC_PORT },
  stdio: ['ignore', 'inherit', 'inherit'],
});
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const api = async (method, p, body, token = ADMIN) => {
  const r = await fetch(BASE + '/api/admin' + p, { method, headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json' }, body: body && JSON.stringify(body) });
  return { status: r.status, body: await r.json() };
};
const waitFor = async (fn, ms = 2000) => { const t = Date.now(); while (!(await fn())) { if (Date.now() - t > ms) throw new Error('waitFor timeout: ' + fn); await sleep(20); } };
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

  // ---- game services ----
  const mk = async (name, o = {}) => { const x = new Multiplayer({ url: BASE, key: game.key, name, transports: ['websocket'], ...o }); await x.connect(); return x; };
  const x1 = await mk('Xena'), x2 = await mk('Xavier'), x3 = await mk('Xolo');

  // storage: versions / compare-and-set
  assert.deepEqual(await x1.storage.set('deck', [1, 2]), { version: 1 });
  assert.deepEqual(await x1.storage.set('deck', [1, 2, 3], { ifVersion: 1 }), { version: 2 });
  await assert.rejects(x1.storage.set('deck', [], { ifVersion: 1 }), /version/);
  await assert.rejects(x1.storage.set('deck', [], { ifVersion: 0 }), /version/);
  assert.equal((await x1.storage.get('deck', { meta: true })).version, 2); ok('storage: versions + compare-and-set');
  const upd = await x1.storage.update('inv', [['incr', 'coins', 5], ['push', 'log', 'a', 2], ['push', 'log', 'b', 2], ['push', 'log', 'c', 2],
    ['addToSet', 'tags', 'x'], ['addToSet', 'tags', 'x'], ['merge', 'stats', { hp: 10 }], ['max', 'best', 7], ['max', 'best', 3], ['set', 'gone', 1], ['unset', 'gone']]);
  assert.deepEqual(upd.value, { coins: 5, log: ['b', 'c'], tags: ['x'], stats: { hp: 10 }, best: 7 });
  assert.equal(await x1.storage.incr('inv', 2, { path: 'coins' }), 7); ok('storage: atomic update ops (incr/push cap/addToSet/merge/max/unset)');
  await x1.storage.setMany({ a: 1, b: { c: 2 } }, { scope: 'private' });
  assert.deepEqual(await x1.storage.getMany(['a', 'b', 'zz'], { scope: 'private' }), { a: 1, b: { c: 2 }, zz: null }); ok('storage: setMany / getMany');
  await x1.storage.set('temp', 'x', { ttl: 1 });
  assert.equal(await x1.storage.get('temp'), 'x');
  await sleep(1100);
  assert.equal(await x1.storage.get('temp'), null);
  assert.deepEqual(await x1.storage.set('temp', 'y', { ifVersion: 0 }), { version: 1 }); ok('storage: TTL expiry (expired keys count as missing)');
  await x1.storage.set('profile', { level: 3, region: 'au' });
  await x2.storage.set('profile', { level: 7, region: 'au' });
  await x3.storage.set('profile', { level: 9, region: 'eu' });
  const found = await x1.storage.query({ key: 'profile', where: [['level', '>=', 5], ['region', '=', 'au']] });
  assert.deepEqual(found.map((r) => [r.player, r.name]), [[x2.playerId, 'Xavier']]);
  const byLevel = await x1.storage.query({ key: 'profile', orderBy: 'level', desc: true, limit: 2 });
  assert.deepEqual(byLevel.map((r) => r.value.level), [9, 7]);
  await assert.rejects(x1.storage.query({ scope: 'private' }), /scope/);
  await assert.rejects(x1.storage.query({ where: [["level') OR 1=1 --", '=', 1]] }), /path/); ok('storage: query across players (filters, order, no private, injection-safe paths)');
  await api('PATCH', `/games/${game.id}`, { settings: { playerDataWrite: 'none' } });
  await assert.rejects(x1.storage.set('cheat', 1), { code: 'forbidden' });
  await api('PATCH', `/games/${game.id}`, { settings: { playerDataWrite: 'all' } }); ok('storage: playerDataWrite=none locks client writes');

  // leaderboards
  const sub = await x1.leaderboard.submit('daily_runs', 120, { meta: { char: 'mage' }, config: { periods: ['day', 'week'] } });
  assert.equal(sub.rank, 1); assert.equal(sub.periods.day.rank, 1); assert.equal(sub.periods.week.score, 120);
  await x2.leaderboard.submit('daily_runs', 90);
  const dTop = await x2.leaderboard.top('daily_runs', { period: 'day' });
  assert.deepEqual(dTop.entries.map((e) => [e.name, e.score]), [['Xena', 120], ['Xavier', 90]]);
  assert.deepEqual(dTop.entries[0].meta, { char: 'mage' }); assert.equal(dTop.me.rank, 2); assert.ok(dTop.resetsAt > Date.now());
  assert.equal((await x2.leaderboard.top('daily_runs', { period: 'day', previous: true })).entries.length, 0);
  await assert.rejects(x1.leaderboard.top('daily_runs', { period: 'month' }), { code: 'bad_period' }); ok('leaderboards: periods (day/week), meta, resetsAt, previous period');
  await x1.leaderboard.submit('laps', 65.2, { config: { mode: 'min' } });
  await x2.leaderboard.submit('laps', 61.9);
  await x1.leaderboard.submit('laps', 70);
  assert.deepEqual((await x1.leaderboard.top('laps')).entries.map((e) => e.score), [61.9, 65.2]); ok('leaderboards: min mode sorts ascending, keeps best');
  for (let i = 0; i < 10; i++) await api('POST', `/games/${game.id}/leaderboards/speed/submit`, { player: 'p_bot' + i, score: i * 10 });
  await x3.leaderboard.submit('speed', 45);
  const ar = await x3.leaderboard.around('speed', { range: 2 });
  assert.equal(ar.me.rank, 6); assert.deepEqual(ar.entries.map((e) => e.score), [60, 50, 45, 40, 30]); ok('leaderboards: around-me window');
  await api('PUT', `/games/${game.id}/leaderboards/ranked/config`, { clientWrite: false, min: 0, max: 5000 });
  await assert.rejects(x1.leaderboard.submit('ranked', 10), { code: 'forbidden' });
  await assert.rejects(api('POST', `/games/${game.id}/leaderboards/ranked/submit`, { player: x1.playerId, score: 99999 }).then((r) => { if (r.status !== 200) throw new Error(r.body.error); }), /bad_score/);
  ok('leaderboards: server-only boards + score bounds');
  assert.ok((await x1.leaderboard.boards()).some((b) => b.id === 'daily_runs' && b.periods.includes('week'))); ok('leaderboards: list board configs');

  // social
  const inc = new Promise((r) => x2.once('friend', r));
  assert.equal((await x1.friends.request(x2.playerId)).state, 'outgoing');
  assert.equal((await inc).state, 'incoming');
  const acc = new Promise((r) => x1.once('friend', r));
  assert.equal((await x2.friends.accept(x1.playerId)).state, 'friend');
  assert.equal((await acc).state, 'friend');
  const fl = await x1.friends.list();
  assert.equal(fl[0].id, x2.playerId); assert.equal(fl[0].online, true); ok('friends: request -> incoming event -> accept');
  const pres = new Promise((r) => x1.once('presence', r));
  await x2.setPresence('in lobby', { map: 'arena' });
  assert.deepEqual(await pres, { t: 'presence', id: x2.playerId, online: true, status: 'in lobby', data: { map: 'arena' } }); ok('presence: friends see status changes');
  assert.equal((await x1.leaderboard.friends('daily_runs', { period: 'day' })).entries.length, 2); ok('leaderboards: friends view');
  const prof = await x1.profiles.get([x2.playerId, x3.playerId]);
  assert.equal(prof[0].status, 'in lobby'); assert.equal(prof[1].status, undefined); // presence is friends-only
  assert.equal((await x1.profiles.find({ name: 'Xol' }))[0].id, x3.playerId); ok('profiles: lookup + name search');
  await x3.friends.block(x1.playerId);
  await assert.rejects(x1.send('dm', 1, x3.playerId) && x1.sendReliable('dm', 1, x3.playerId), /blocked/);
  assert.deepEqual(await x1.inbox.send(x3.playerId, 'gift', 1), { id: null }); ok('block: DMs refused, inbox silently dropped');

  // inbox
  const mail = new Promise((r) => x2.once('inbox', r));
  await x1.inbox.send(x2.playerId, 'gift', { gold: 50 });
  assert.deepEqual((await mail).data, { gold: 50 });
  const box = await x2.inbox.list();
  assert.equal(box.unread, 1); assert.equal(box.items[0].fromName, 'Xena');
  assert.equal((await x2.inbox.markRead()).unread, 0); ok('inbox: live delivery, list, mark read');
  x2.disconnect();
  await waitFor(async () => (await x1.friends.list())[0].online === false); ok('presence: friends see you go offline');
  await x1.inbox.send(x2.playerId, 'invite', { room: 'ABC' });
  await x2.connect();
  assert.equal(x2.inboxUnread, 1); ok('inbox: messages wait for offline players');

  // chat channels
  await x1.chat.join('global'); await x2.chat.join('global');
  const said = new Promise((r) => x2.once('chat:global', r));
  await x1.chat.send('global', 'hello world');
  const cm = await said;
  assert.equal(cm.text, 'hello world'); assert.equal(cm.name, 'Xena');
  assert.equal((await x3.chat.join('global')).history.length, 0); // x3 blocked x1: x1's history is hidden
  assert.equal((await (await mk('Late')).chat.join('global')).history[0].text, 'hello world'); ok('chat: channels, history, block filter');

  // matchmaking
  const m1 = await mk('M1'), m2 = await mk('M2'), m3 = await mk('M3');
  const p3 = m3.matchmake('duel', { rating: 3000 });
  const [r1, r2] = await Promise.all([m1.matchmake('duel', { rating: 1000 }), m2.matchmake('duel', { rating: 1050 })]);
  assert.equal(r1.id, r2.id); assert.equal(m1.players.size, 2); assert.ok(r1.private);
  assert.equal((await m3.matchmakingStatus()).queue, 'duel');
  const cancelled = assert.rejects(p3, /cancelled/);
  await m3.cancelMatchmaking(); await cancelled; ok('matchmaking: close ratings match into one room, far rating waits, cancel');
  const t0 = Date.now();
  const [sq1, sq2] = await Promise.all([m1.matchmake('squad', { size: 4, minSize: 2, maxWaitMs: 300 }), m3.matchmake('squad', { size: 4, minSize: 2, maxWaitMs: 300 })]);
  assert.equal(sq1.id, sq2.id); assert.ok(Date.now() - t0 >= 250); ok('matchmaking: starts with minSize after maxWaitMs');

  // remote config
  const cfgEv = new Promise((r) => x1.once('config', r));
  await api('PATCH', `/games/${game.id}`, { settings: { config: { motd: 'Season 2!', maxSpeed: 5 } } });
  assert.equal((await cfgEv).motd, 'Season 2!');
  assert.equal((await mk('Cfg')).config.maxSpeed, 5); ok('remote config: pushed live + in welcome');

  // server script uses the services
  await api('PUT', `/games/${game.id}/script`, { code: `module.exports = { onMessage(room, p, type) {
    if (type !== 'win') return;
    const w = room.storage.update('stats', [['incr', 'wins', 1]], { player: p.id });
    room.leaderboard.submit(p.id, 'ranked', w.value.wins * 100);
    room.inbox.send(p.id, 'reward', { wins: w.value.wins });
  } };` });
  const sc = await mk('Scripted');
  await sc.join('SVC');
  const rew = new Promise((r) => sc.once('inbox', r));
  sc.send('win', null, 'server');
  assert.deepEqual((await rew).data, { wins: 1 });
  assert.deepEqual(await sc.storage.get('stats'), { wins: 1 });
  assert.equal((await sc.leaderboard.top('ranked')).me.score, 100); ok('server script: storage + server-only leaderboard + inbox');
  await api('DELETE', `/games/${game.id}/script`);
  for (const x of [x1, x2, x3, m1, m2, m3, sc]) x.disconnect();

  {
  // ---- public servers ----
  const pw = async (name) => { const x = new Multiplayer({ url: BASE, key: game.key, name, transports: ['websocket'] }); await x.connect(); return x; };
  const made = await api('POST', `/games/${game.id}/servers`, { id: 'eu-1', name: 'EU #1', region: 'eu', tags: ['pvp'], maxPlayers: 2, motd: 'Welcome' });
  assert.equal(made.status, 201);
  const v1 = await pw('S1'), v2 = await pw('S2'), v3 = await pw('S3');
  const list = await v1.servers.list({ region: 'eu' });
  assert.deepEqual(list.map((x) => [x.id, x.name, x.players, x.maxPlayers]), [['eu-1', 'EU #1', 0, 2]]); ok('servers: admin-defined server shows in the server list');
  await v1.joinServer('eu-1');
  await v1.setState('world.day', 3);
  await v1.leave();
  assert.equal((await v2.servers.get('eu-1')).players, 0);
  await v1.joinServer('eu-1');
  assert.equal(v1.state.world.day, 3); assert.equal(v1.room.server, 'EU #1'); ok('servers: stay open when empty and keep their state');
  await v2.joinServer('eu-1');
  await assert.rejects(v3.joinServer('eu-1', { queue: false }), { code: 'room_full' });
  const pos = new Promise((r) => v3.once('queue', r));
  const queued = v3.joinServer('eu-1');
  assert.equal((await pos).position, 1);
  assert.equal((await v1.servers.get('eu-1')).queue, 1);
  await v1.leave();
  assert.equal((await queued).id, 'eu-1'); assert.equal(v3.room.id, 'eu-1'); ok('servers: full server -> queue position -> auto-join when a slot frees');
  const q2 = v1.joinServer('eu-1');
  await waitFor(async () => (await v2.servers.get('eu-1')).queue === 1);
  await api('PATCH', `/games/${game.id}/servers/eu-1`, { maxPlayers: 3 });
  assert.equal((await q2).id, 'eu-1'); ok('servers: raising maxPlayers lets the queue in');
  const q3 = (await pw('S4')).joinServer('eu-1');
  const cancelled3 = assert.rejects(q3, { code: 'server_deleted' });
  await api('PATCH', `/games/${game.id}/servers/eu-1`, { visibility: 'unlisted', password: 'pw' });
  assert.equal((await v1.servers.list()).length, 0);
  await assert.rejects((await pw('S5')).joinServer('eu-1', { queue: false }), { code: 'bad_password' }); ok('servers: unlisted + password');
  const kicked = new Promise((r) => v1.once('left', (room, reason) => r(reason)));
  await api('DELETE', `/games/${game.id}/servers/eu-1`);
  assert.equal(await kicked, 'server_deleted'); await cancelled3; ok('servers: delete closes the server and its queue');
  for (const x of [v1, v2, v3]) x.disconnect();

  // ---- room types + per-game limits ----
  await api('PATCH', `/games/${game.id}`, { settings: { requireRoomType: true,
    roomTypes: { duel: { maxPlayers: 2, stateWrite: 'host', meta: { mode: 'duel' } }, world: { maxPlayers: 500, aoi: { radius: 100 } } } } });
  const rt = await pw('RT');
  await assert.rejects(rt.create({ maxPlayers: 50 }), { code: 'room_type_required' });
  await assert.rejects(rt.create({ type: 'nope' }), { code: 'bad_room_type' });
  const duel = await rt.create({ type: 'duel', maxPlayers: 99, meta: { map: 'pit' } });
  assert.deepEqual([duel.type, duel.maxPlayers, duel.stateWrite, duel.meta.mode, duel.meta.map], ['duel', 2, 'host', 'duel', 'pit']); ok('room types: templates win over client options; requireRoomType enforced');
  await api('PATCH', `/games/${game.id}`, { settings: { limits: { maxRoomPlayers: 3 } } });
  assert.equal((await rt.create({ type: 'world' })).maxPlayers, 3); ok('limits: per-game maxRoomPlayers caps every room');
  await api('PATCH', `/games/${game.id}`, { settings: { limits: { maxRoomPlayers: 2000 } } });
  assert.equal((await rt.create({ type: 'world' })).maxPlayers, 500);
  await api('PATCH', `/games/${game.id}`, { settings: { requireRoomType: null, roomTypes: null, limits: null } });
  rt.disconnect();

  // ---- open world: interest management ----
  const at = async (name, x, y) => { const p = await pw(name); await p.join('OPEN', { maxPlayers: 2000, aoi: { radius: 100 }, playerData: { x, y } }); return p; };
  const wa = await at('WA', 0, 0), wb = await at('WB', 50, 0), wc = await at('WC', 1000, 0);
  assert.deepEqual([...wb.players.keys()].sort(), [wa.playerId, wb.playerId].sort());
  assert.ok(!wa.players.has(wc.playerId)); assert.ok(wa.players.has(wb.playerId)); ok('aoi: snapshot only holds players in range');
  await waitFor(() => wa.playerCount === 3, 3000); ok('aoi: playerCount still counts everyone');
  const seen = [];
  wc.on('playerUpdated', (p) => seen.push(p.id));
  wa.setPlayer({ hp: 5 });
  await waitFor(() => wb.players.get(wa.playerId).data.hp === 5);
  await sleep(100);
  assert.ok(!seen.includes(wa.playerId)); ok('aoi: updates only reach players in range');
  const enter = new Promise((r) => wa.on('playerJoined', (p, info) => p.id === wc.playerId && r(info)));
  wc.setPlayer({ x: 60, y: 10 });
  assert.deepEqual(await enter, { inRange: true });
  await waitFor(() => wc.players.has(wa.playerId) && wc.players.has(wb.playerId)); ok('aoi: moving closer brings players into view (both ways)');
  const exit = new Promise((r) => wa.on('playerLeft', (p, reason) => p.id === wc.playerId && r(reason)));
  wc.setPlayer({ x: 5000, y: 0 });
  assert.equal(await exit, 'out_of_range');
  await waitFor(() => !wc.players.has(wa.playerId)); ok('aoi: moving away removes them (reason out_of_range)');
  const near = new Promise((r) => wb.onMessage('shout', r));
  let far = false; wc.onMessage('shout', () => { far = true; });
  wa.send('shout', 'hi', 'nearby');
  assert.equal(await near, 'hi'); await sleep(100); assert.equal(far, false); ok("aoi: send(..., 'nearby') reaches only players in range");
  const gone = new Promise((r) => wa.on('playerLeft', (p, reason) => p.id === wb.playerId && r(reason)));
  await wb.leave();
  assert.equal(await gone, 'left'); ok('aoi: leaving notifies only those who could see you');
  for (const x of [wa, wb, wc]) x.disconnect();

  // ---- leaderboard compatibility with older clients ----
  const oldc = await pw('Old');
  await oldc.leaderboard.submit('ow_set', 10, { mode: 'set' });
  await oldc.leaderboard.submit('ow_set', 3, { mode: 'set' });
  assert.equal((await oldc.leaderboard.top('ow_set')).entries[0].score, 3); ok("compat: mode 'set' still overwrites");
  await api('POST', `/games/${game.id}/leaderboards/ow_legacy/submit`, { player: 'p_old1', score: 50 });
  await api('POST', `/games/${game.id}/leaderboards/ow_legacy/submit`, { player: 'p_old2', score: 20 });
  assert.deepEqual((await oldc.leaderboard.top('ow_legacy', { order: 'asc' })).entries.map((e) => e.score), [20, 50]); ok("compat: top({order:'asc'}) still overrides the sort");
  oldc.disconnect();

  }
  // ---- transports ----
  const tw = new Multiplayer({ url: BASE, key: game.key, transports: ['websocket'] });
  await tw.connect({ room: 'NET' });
  assert.equal(tw.transport, 'websocket'); ok('transports: [websocket] stays on WebSocket');
  const tu = new Multiplayer({ url: `http://localhost:${PORT}`, key: game.key, transports: ['udp', 'websocket'] }); // localhost -> ::1 in Node: exercises dual-stack UDP
  await tu.connect({ room: 'NET' });
  await waitFor(() => tu.transport === 'udp', 4000); ok('Node client probes and moves to raw UDP');
  const tr = new Multiplayer({ url: BASE, key: game.key, transports: ['webrtc', 'websocket'], RTCPeerConnection });
  await tr.connect({ room: 'NET' });
  await waitFor(() => tr.transport === 'webrtc', 8000); ok('client probes and moves to WebRTC data channels');
  const st = tr.transportStats;
  assert.ok(st.webrtc.samples >= 3 && st.websocket.samples >= 1); ok('transportStats measures every link');
  const ov2 = (await api('GET', '/overview')).body;
  const kinds = new Set(ov2.players.filter((p) => [tu, tr].some((x) => x.playerId === p.id)).map((p) => p.transport));
  assert.deepEqual([...kinds].sort(), ['udp', 'webrtc']); ok('server sends on the link each client chose (admin overview)');

  await tu.setState('via', 'udp');
  await waitFor(() => tr.state.via === 'udp' && tw.state.via === 'udp');
  await tr.setState('via', 'rtc');
  await waitFor(() => tu.state.via === 'rtc' && tw.state.via === 'rtc'); ok('state syncs across udp / webrtc / websocket clients');
  const gotU = new Promise((r) => tu.onMessage('hey', r));
  tr.send('hey', 1, tu.playerId);
  assert.equal(await gotU, 1); ok('relay message webrtc -> udp');

  tr.setPlayer({ x: 1 }, { unreliable: true });
  for (let i = 2; i <= 30; i++) tr.setPlayer({ x: i }, { unreliable: true });
  await waitFor(() => tu.players.get(tr.playerId).data.x === 30 && tw.players.get(tr.playerId).data.x === 30); ok('unreliable setPlayer lands on the newest value');

  // 30% packet loss both ways on the UDP link: every reliable op must still arrive, in order.
  const ul = tu._links.get('udp'), sendU = ul.send, frameU = tu._onFrame.bind(tu);
  ul.send = (f, un) => (Math.random() < 0.3 ? undefined : sendU(f, un));
  tu._onFrame = (l, d) => (l === ul && Math.random() < 0.3 ? undefined : frameU(l, d));
  const order = [];
  tw.on('state', (s, ev) => { for (const [p, v] of ev.ops || []) if (p === 'seq') order.push(v); });
  for (let i = 1; i <= 60; i++) tu.setState('seq', i).catch(() => {});
  await waitFor(() => tw.state.seq === 60 && tu.state.seq === 60, 8000);
  assert.deepEqual(order, Array.from({ length: 60 }, (_, i) => i + 1)); ok('30% UDP loss: all 60 reliable writes delivered once, in order');
  ul.send = sendU; tu._onFrame = frameU;

  // Kill the UDP path entirely: the client must fall back to WebSocket without losing messages.
  const back = new Promise((r) => tu.on('transport', (k) => k === 'websocket' && r()));
  ul.send = () => {}; tu._onFrame = (l, d) => (l === ul ? undefined : frameU(l, d));
  for (let i = 61; i <= 70; i++) tu.setState('seq', i).catch(() => {});
  await back;
  await waitFor(() => tw.state.seq === 70 && tu.state.seq === 70, 6000); ok('dead UDP link -> automatic fallback to WebSocket, nothing lost');
  tu._onFrame = frameU;

  // v1 clients (raw JSON over WebSocket, no session frames) keep working.
  const legacy = await new Promise((resolve, reject) => {
    const ws = new WebSocket(BASE.replace('http', 'ws') + '/ws');
    ws.onopen = () => ws.send(JSON.stringify({ t: 'hello', v: 1, key: game.key }));
    ws.onmessage = (ev) => { resolve(JSON.parse(ev.data)); ws.close(); };
    ws.onerror = reject;
  });
  assert.equal(legacy.t, 'welcome'); assert.equal(legacy.transports, undefined); ok('legacy v1 clients still served over plain WebSocket');
  for (const x of [tw, tu, tr]) x.disconnect();

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

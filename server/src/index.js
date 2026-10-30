// Generic multiplayer server: rooms, realtime shared state, relay messaging, persistent
// storage, leaderboards, player accounts, admin API + dashboard. One process, SQLite on disk.
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';
import { WebSocketServer } from 'ws';
import { openDb } from './db.js';
import { createScripting } from './scripts.js';
import { createTransports } from './transports.js';
import { createServices } from './services.js';
import { createAoi, normalizeAoi } from './aoi.js';
import { createServers } from './servers.js';
import { createSetup } from './setup.js';

const scrypt = promisify(crypto.scrypt);
const here = path.dirname(fileURLToPath(import.meta.url));
const env = process.env;

const CFG = {
  port: +env.PORT || 8080,
  dataDir: env.DATA_DIR || path.resolve(here, '../data'),
  publicDir: env.PUBLIC_DIR || path.resolve(here, '../..'),
  openGames: env.OPEN_GAMES === 'true',
  trustProxy: env.TRUST_PROXY === 'true',
  maxMsg: +env.MAX_MSG_BYTES || 64 * 1024,
  // A 50 Hz game sends several messages per tick (player.set + state.set + send ...), so allow 200/s sustained
  // (= 50 ticks x 4 msgs) with a 500 burst. Override with RATE_LIMIT / RATE_BURST.
  rate: +env.RATE_LIMIT || 200,          // sustained msgs/sec per connection
  burst: +env.RATE_BURST || 500,         // bucket size
  grace: +env.RECONNECT_GRACE_MS || 15000,
  maxRoomPlayers: +env.MAX_ROOM_PLAYERS || 1000,      // default per-room cap; a game can raise it up to the hard cap
  hardMaxRoomPlayers: +env.HARD_MAX_ROOM_PLAYERS || 5000,
  maxRooms: +env.MAX_ROOMS_PER_GAME || 10000,
  maxConnsPerIp: +env.MAX_CONNS_PER_IP || 50,
  // Extra transports next to the WebSocket (see transports.js). UDP_PORT=0 / WEBRTC=false turn them off.
  udpPort: env.UDP_PORT === '0' ? 0 : +env.UDP_PORT || 8081,
  udpBind: env.UDP_BIND || '',
  rtc: env.WEBRTC !== 'false',
  rtcPort: +env.RTC_PORT || 0,          // one UDP port for every WebRTC peer (ICE UDP mux); 0 = random ports
  publicIp: env.PUBLIC_IP || '',        // advertised in WebRTC candidates (needed inside Docker)
  publicHost: env.UDP_HOST || '',       // host clients send UDP to; default = the host they connected to
  iceServers: (env.ICE_SERVERS || '').split(',').map((v) => v.trim()).filter(Boolean),
};

// ---------- storage / secrets ----------
const store = openDb(CFG.dataDir);
const { S } = store;
const rid = (n = 9) => crypto.randomBytes(n).toString('base64url');
const metaOr = (k, make) => {
  const row = S.metaGet.get(k);
  if (row) return row.v;
  const v = make();
  S.metaSet.run(k, v);
  return v;
};
const SECRET = env.TOKEN_SECRET || metaOr('secret', () => rid(32));
let ADMIN_TOKEN = env.ADMIN_TOKEN;
if (!ADMIN_TOKEN || ADMIN_TOKEN.length < 16) {
  ADMIN_TOKEN = metaOr('admin_token', () => 'adm_' + rid(24));
  console.log(`[mp] ADMIN_TOKEN not set (or <16 chars). Using stored token: ${ADMIN_TOKEN}`);
}

// ---------- games ----------
const games = new Map();      // id -> game
const gamesByKey = new Map(); // key -> game
const gameStats = new Map();  // id -> { msgs, rate, peak, lastActive }
function loadGame(row) {
  const g = { id: row.id, name: row.name, key: row.key, enabled: !!row.enabled, created: row.created,
    settings: JSON.parse(row.settings || '{}') };
  games.set(g.id, g);
  gamesByKey.set(g.key, g);
  if (!gameStats.has(g.id)) gameStats.set(g.id, { msgs: 0, rate: 0, peak: 0, lastActive: 0, minutes: [], acc: 0, accN: 0, accMax: 0 });
  if (!g.settings.setupKey) { g.settings.setupKey = 'sk_' + rid(18); saveGame(g); } // for the setup API (coding agents)
  return g;
}
for (const row of S.gamesAll.all()) loadGame(row);

function createGame(name, settings = {}, key) {
  const id = 'g_' + rid(6);
  key = key || 'pk_' + rid(18);
  S.gameInsert.run(id, String(name).slice(0, 64) || 'Untitled', key, JSON.stringify(settings), Date.now());
  log(id, 'game', `Game created: ${name}`);
  return loadGame({ id, name, key, enabled: 1, settings: JSON.stringify(settings), created: Date.now() });
}
function saveGame(g) {
  S.gameUpdate.run(g.name, g.key, g.enabled ? 1 : 0, JSON.stringify(g.settings), g.id);
}

// Per-game overrides of the server-wide limits (settings.limits, edited in the dashboard).
function limits(game) {
  const l = isObj(game && game.settings.limits) ? game.settings.limits : {};
  const pick = (k, def, lo, hi) => (Number.isFinite(+l[k]) && l[k] !== null && l[k] !== '' ? Math.max(lo, Math.min(hi, +l[k])) : def);
  return {
    maxRoomPlayers: pick('maxRoomPlayers', Math.min(CFG.maxRoomPlayers, CFG.hardMaxRoomPlayers), 1, CFG.hardMaxRoomPlayers),
    maxRooms: pick('maxRooms', CFG.maxRooms, 1, 1e6),
    rate: pick('rateLimit', CFG.rate, 1, 10000),
    burst: pick('rateBurst', CFG.burst, 1, 100000),
    grace: pick('reconnectGraceMs', CFG.grace, 0, 3600000),
    defaultMaxPlayers: pick('defaultMaxPlayers', 16, 1, CFG.hardMaxRoomPlayers),
  };
}

// ---------- telemetry ----------
const started = Date.now();
const stats = { in: 0, out: 0, bytesIn: 0, bytesOut: 0, history: [] };
const events = []; // ring buffer for dashboard
function log(game, type, text) {
  events.push({ ts: Date.now(), game, type, text });
  if (events.length > 300) events.shift();
}
let lastIn = 0, lastOut = 0;
setInterval(() => {
  const now = Date.now();
  stats.history.push({ t: now, in: stats.in - lastIn, out: stats.out - lastOut, players: online.size, rooms: rooms.size });
  if (stats.history.length > 180) stats.history.shift();
  lastIn = stats.in; lastOut = stats.out;
  const perGame = new Map();
  for (const c of online.values()) perGame.set(c.game.id, (perGame.get(c.game.id) || 0) + 1);
  for (const [id, s] of gameStats) {
    const n = perGame.get(id) || 0;
    s.rate = s.msgs; s.msgs = 0;
    s.peak = Math.max(s.peak, n);
    // Per-minute averages of players online, kept for 24 h (dashboard: expected vs measured).
    s.acc += n; s.accN++; s.accMax = Math.max(s.accMax, n);
    if (s.accN >= 60) {
      s.minutes.push({ t: now, avg: s.acc / s.accN, max: s.accMax });
      if (s.minutes.length > 1440) s.minutes.shift();
      s.acc = s.accN = s.accMax = 0;
    }
  }
}, 1000).unref();

// ---------- errors / validation ----------
class E extends Error { constructor(code, msg) { super(msg || code); this.code = code; } }
const ID_RE = /^[A-Za-z0-9_\-]{1,64}$/;
const KEY_RE = /^[^\u0000-\u001f]{1,128}$/;
const BAD_KEYS = new Set(['__proto__', 'prototype', 'constructor']);
const isObj = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);
const str = (v, max, def) => (typeof v === 'string' ? v.slice(0, max) : def);
const cleanName = (v) => str(v, 32, '').replace(/[\u0000-\u001f<>]/g, '').trim();
function safeMerge(target, src) {
  for (const k of Object.keys(src)) {
    if (BAD_KEYS.has(k)) continue;
    if (src[k] === null) delete target[k]; else target[k] = src[k];
  }
  return target;
}
function setPath(obj, p, value, del) {
  const parts = String(p).split('.');
  if (!p || parts.length > 32 || parts.some((s) => !s || BAD_KEYS.has(s))) throw new E('bad_path');
  let o = obj;
  for (let i = 0; i < parts.length - 1; i++) {
    const k = parts[i];
    if (o[k] === null || typeof o[k] !== 'object') { if (del) return; o[k] = {}; }
    o = o[k];
  }
  const last = parts[parts.length - 1];
  if (del) { if (Array.isArray(o) && /^\d+$/.test(last)) o.splice(+last, 1); else delete o[last]; }
  else o[last] = value;
}

// ---------- tokens ----------
const sign = (gid, pid) => crypto.createHmac('sha256', SECRET).update(gid + '.' + pid).digest('base64url').slice(0, 32);
const makeToken = (gid, pid) => `${pid}.${sign(gid, pid)}`;
function readToken(gid, token) {
  if (typeof token !== 'string') return null;
  const i = token.lastIndexOf('.');
  if (i < 1) return null;
  const pid = token.slice(0, i), sig = Buffer.from(token.slice(i + 1));
  const good = Buffer.from(sign(gid, pid));
  return sig.length === good.length && crypto.timingSafeEqual(sig, good) && ID_RE.test(pid) ? pid : null;
}

// ---------- realtime core ----------
const rooms = new Map();  // gid \0 roomId -> room
const online = new Map(); // gid \0 pid -> client
const conns = new Set();
const ipConns = new Map();
const rkey = (gid, id) => gid + '\0' + id;
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const newCode = (gid) => {
  for (;;) {
    let s = '';
    for (const b of crypto.randomBytes(6)) s += CODE_CHARS[b % CODE_CHARS.length];
    if (!rooms.has(rkey(gid, s))) return s;
  }
};

// unreliable = fast lane (may drop, never resent); only matters for v2 clients on a WebRTC/UDP link.
function out(c, s, unreliable) {
  if (c.session) c.session.send(s, unreliable); else c.ws.send(s);
  stats.out++; stats.bytesOut += s.length;
}
function send(c, obj, unreliable) {
  if (!c || c.ws.readyState !== 1) return;
  out(c, JSON.stringify(obj), unreliable);
}
function broadcast(room, obj, exceptPid, unreliable) {
  const s = JSON.stringify(obj);
  for (const m of room.players.values()) {
    if (m.id === exceptPid || !m.client || m.client.ws.readyState !== 1) continue;
    out(m.client, s, unreliable);
  }
}
const pubPlayer = (m) => ({ id: m.id, name: m.name, data: m.data, connected: m.connected, joined: m.joined, username: m.username || null });
const rttOf = (m) => (m.client ? m.client.rtt : null);
const roomInfo = (r) => ({ id: r.id, host: r.host, meta: r.meta, maxPlayers: r.maxPlayers, playerCount: r.players.size, locked: r.locked,
  private: r.private, tag: r.tag, type: r.type, stateWrite: r.stateWrite, hasPassword: !!r.password, created: r.created,
  server: r.server ? r.server.name : undefined, aoi: r.aoi ? { radius: r.aoi.radius, keys: r.aoi.keys } : undefined });
// With interest management a player's snapshot only holds themself and the players they can see.
const snapshot = (r, pid) => {
  const me = pid != null && r.aoi ? r.players.get(pid) : null;
  const list = me ? [me, ...aoi.watchers(r, me)] : [...r.players.values()];
  return { ...roomInfo(r), players: list.map(pubPlayer), state: r.state };
};
// Everyone who should hear about member `m` (all others, or only those who can see m in an AOI room).
function* audience(room, m) {
  if (room.aoi) yield* aoi.watchers(room, m);
  else for (const o of room.players.values()) if (o !== m) yield o;
}
function tell(room, m, obj, unreliable) {
  const s = JSON.stringify(obj);
  for (const o of audience(room, m)) if (o.client && o.client.ws.readyState === 1) out(o.client, s, unreliable);
}
const roomCounts = new Map(); // gid -> open rooms

// Room options: a named room type from the game settings (its fields win), else the game's roomDefaults.
// trusted = server-defined (public servers), which skips requireRoomType.
function roomOptions(game, o = {}, trusted) {
  const st = game.settings, types = isObj(st.roomTypes) ? st.roomTypes : {};
  if (o.type != null && o.type !== '') {
    const t = Object.hasOwn(types, String(o.type)) ? types[String(o.type)] : null;
    if (!isObj(t)) throw new E('bad_room_type', `Unknown room type: ${o.type}`);
    const base = t.clientOptions === false ? { password: o.password, playerData: o.playerData } : o;
    return { ...base, ...t, meta: { ...(isObj(base.meta) ? base.meta : {}), ...(isObj(t.meta) ? t.meta : {}) }, type: String(o.type) };
  }
  if (!trusted && st.requireRoomType) throw new E('room_type_required', 'This game only allows its defined room types');
  return isObj(st.roomDefaults) ? { ...st.roomDefaults, ...o } : o;
}

function createRoom(game, id, o = {}, trusted) {
  o = roomOptions(game, o, trusted);
  const lim = limits(game);
  if ((roomCounts.get(game.id) || 0) >= lim.maxRooms) throw new E('too_many_rooms');
  const aoiCfg = normalizeAoi(o.aoi);
  if (aoiCfg === undefined) throw new E('bad_aoi', "aoi: { radius: number > 0, keys: ['x', 'y'] or ['x', 'y', 'z'] }");
  const room = {
    id, game, players: new Map(), state: isObj(o.state) ? safeMerge({}, o.state) : {}, host: null,
    meta: isObj(o.meta) ? o.meta : {}, type: o.type != null ? String(o.type) : null,
    maxPlayers: Math.max(1, Math.min(lim.maxRoomPlayers, Math.floor(+o.maxPlayers) || lim.defaultMaxPlayers)),
    private: !!o.private, locked: false, tag: str(o.tag, 64, ''), persist: !!o.persist,
    stateWrite: o.stateWrite === 'host' ? 'host' : 'all', password: str(o.password, 64, '') || null,
    queueAllowed: o.queue !== false, waiting: [], whitelist: null, permanent: false,
    created: Date.now(), banned: new Set(),
  };
  if (aoiCfg) aoi.enable(room, aoiCfg);
  roomCounts.set(game.id, (roomCounts.get(game.id) || 0) + 1);
  if (room.persist) {
    const saved = S.kvGet.get(game.id, 'room', id, 'state');
    if (saved) room.state = JSON.parse(saved.v);
  }
  rooms.set(rkey(game.id, id), room);
  log(game.id, 'room', `Room ${id} created`);
  if (scripts.has(game.id)) { scripts.call(room, 'onCreate'); scripts.startTick(room); }
  return room;
}
function destroyRoom(room, reason = 'empty') {
  if (room.persist) S.kvSet.run(room.game.id, 'room', room.id, 'state', JSON.stringify(room.state), Date.now());
  for (const m of room.players.values()) {
    clearTimeout(m.graceTimer);
    if (m.client) { send(m.client, { t: 'left', room: room.id, reason }); m.client.room = null; }
  }
  room.players.clear();
  for (const w of room.waiting.splice(0)) if (w.c.waitingIn === room) { w.c.waitingIn = null; send(w.c, { t: 'queue.cancelled', room: room.id, reason }); }
  scripts.stopTick(room);
  aoi.disable(room);
  rooms.delete(rkey(room.game.id, room.id));
  roomCounts.set(room.game.id, Math.max(0, (roomCounts.get(room.game.id) || 1) - 1));
  log(room.game.id, 'room', `Room ${room.id} closed (${reason})`);
}
function pickHost(room) {
  const cur = room.players.get(room.host);
  if (cur && cur.connected) return;
  let best = null;
  for (const m of room.players.values()) if (m.connected && (!best || m.joined < best.joined)) best = m;
  if (!best && !cur) for (const m of room.players.values()) if (!best || m.joined < best.joined) best = m;
  const next = best ? best.id : cur ? cur.id : null;
  if (next !== room.host) { room.host = next; broadcast(room, { t: 'host', id: next }); }
}
function removeMember(room, pid, reason) {
  const m = room.players.get(pid);
  if (!m) return;
  clearTimeout(m.graceTimer);
  tell(room, m, { t: 'player.left', id: pid, reason });
  if (room.aoi) aoi.remove(room, m);
  room.players.delete(pid);
  if (m.client && m.client.room === room) m.client.room = null;
  scripts.call(room, 'onLeave', pubPlayer(m), reason);
  drainQueue(room);
  if (room.players.size === 0) return room.permanent ? undefined : destroyRoom(room);
  pickHost(room);
}

// ---------- join queue (full rooms, mostly public servers) ----------
function leaveQueue(c, reason) {
  const room = c.waitingIn;
  if (!room) return false;
  c.waitingIn = null;
  const i = room.waiting.findIndex((w) => w.c === c);
  if (i >= 0) { room.waiting.splice(i, 1); queuePositions(room, i); }
  if (reason) send(c, { t: 'queue.cancelled', room: room.id, reason });
  return true;
}
function queuePositions(room, from = 0) {
  for (let i = from; i < room.waiting.length; i++) send(room.waiting[i].c, { t: 'queue', room: room.id, position: i + 1 });
}
function drainQueue(room) {
  let moved = false;
  while (room.waiting.length && room.players.size < room.maxPlayers) {
    const w = room.waiting.shift();
    moved = true;
    if (w.c.waitingIn !== room || w.c.ws.readyState !== 1) continue;
    w.c.waitingIn = null;
    try { send(w.c, { t: 'queue.joined', room: joinRoom(w.c, room.id, { ...w.o, queue: false }) }); }
    catch (e) { send(w.c, { t: 'queue.cancelled', room: room.id, reason: e.code || 'join_failed' }); }
  }
  if (moved) queuePositions(room);
}
function joinRoom(c, id, o = {}) {
  if (!ID_RE.test(id)) throw new E('bad_room_id', 'Room id must match [A-Za-z0-9_-]{1,64}');
  if (c.waitingIn && c.waitingIn.id !== id) leaveQueue(c, 'joined_elsewhere');
  if (c.room && c.room.id === id && c.room.players.has(c.pid)) return snapshot(c.room, c.pid);
  let room = rooms.get(rkey(c.game.id, id));
  const rejoin = room && room.players.has(c.pid);
  if (room && !rejoin) { // checks before leaving the current room, so a refused join keeps you where you are
    if (room.banned.has(c.pid)) throw new E('banned');
    if (room.locked) throw new E('room_locked');
    if (room.whitelist && !room.whitelist.has(c.pid) && !(c.username && room.whitelist.has(c.username))) throw new E('not_whitelisted');
    if (room.password && o.password !== room.password) throw new E('bad_password');
    if (room.players.size >= room.maxPlayers) {
      if (!o.queue || !room.queueAllowed) throw new E('room_full');
      if (c.waitingIn !== room) {
        if (room.waiting.length >= 10000) throw new E('queue_full');
        room.waiting.push({ c, o: { password: o.password, playerData: o.playerData } });
        c.waitingIn = room;
      }
      return { queued: true, room: id, position: room.waiting.findIndex((w) => w.c === c) + 1 };
    }
  }
  if (c.room) removeMember(c.room, c.pid, 'left');
  if (!room) {
    if (o.mustExist) throw new E('room_not_found');
    room = createRoom(c.game, id, o);
  }
  let m = room.players.get(c.pid);
  if (m) { // rejoin during reconnect grace
    clearTimeout(m.graceTimer);
    if (m.client && m.client !== c) send(m.client, { t: 'left', room: id, reason: 'replaced' });
    Object.assign(m, { client: c, connected: true, name: c.name });
    tell(room, m, { t: 'player.connection', id: c.pid, connected: true });
  } else {
    m = { id: c.pid, name: c.name, username: c.username, data: isObj(o.playerData) ? safeMerge({}, o.playerData) : {},
      connected: true, joined: Date.now(), client: c, graceTimer: null };
    room.players.set(c.pid, m);
    if (room.aoi) { aoi.place(room, m); aoi.refresh(room, m, true); } // neighbours hear about us; we get them in the snapshot
    else broadcast(room, { t: 'player.joined', player: pubPlayer(m) }, c.pid);
    if (scripts.has(c.game.id)) { c.room = room; pickHost(room); scripts.call(room, 'onJoin', pubPlayer(m)); }
  }
  c.room = room;
  pickHost(room);
  return snapshot(room, c.pid);
}
const aoi = createAoi({ send, pubPlayer });
const services = createServices({ db: store.db, E, online, rkey, send, out, isObj, str, safeMerge, ID_RE, KEY_RE, BAD_KEYS, CFG,
  newCode, joinRoom, snapshot });
const servers = createServers({ db: store.db, E, rooms, rkey, createRoom, destroyRoom, drainQueue, isObj, str, log, games, limits });
const scripts = createScripting({ log, broadcast, send, setPath, removeMember, pubPlayer, E, services, aoi });
for (const g of games.values()) if (g.settings.script) scripts.load(g);
const setup = createSetup({ db: store.db, E, games, saveGame, isObj, str, safeMerge, services, servers, scripts, setScript, metrics, log, rooms, online });

function onDisconnect(c) {
  const room = c.room;
  if (!room) return;
  const m = room.players.get(c.pid);
  if (!m || m.client !== c) return;
  m.client = null; m.connected = false;
  tell(room, m, { t: 'player.connection', id: c.pid, connected: false });
  pickHost(room);
  m.graceTimer = setTimeout(() => removeMember(room, c.pid, 'disconnected'), limits(room.game).grace);
}
const needRoom = (c) => { if (!c.room) throw new E('not_in_room'); return c.room; };
const needHost = (c) => { const r = needRoom(c); if (r.host !== c.pid) throw new E('not_host'); return r; };
function publicRooms(game, tag) {
  const out = [];
  for (const r of rooms.values()) {
    if (r.game !== game || r.private || (tag != null && r.tag !== tag)) continue;
    out.push({ ...roomInfo(r), players: r.players.size });
  }
  return out;
}

// ---------- message handlers ----------
const H = {
  ping: () => ({ s: Date.now() }),

  name(c, m) {
    const name = cleanName(m.name);
    if (!name) throw new E('bad_name');
    c.name = name;
    S.playerTouch.run(c.game.id, c.pid, name, Date.now(), Date.now());
    const mem = c.room && c.room.players.get(c.pid);
    if (mem) { mem.name = name; tell(c.room, mem, { t: 'player', id: c.pid, name }); }
    return { name };
  },

  rooms: (c, m) => publicRooms(c.game, m.tag == null ? null : String(m.tag)),
  join: (c, m) => joinRoom(c, String(m.room ?? ''), isObj(m.options) ? m.options : {}),
  create: (c, m) => joinRoom(c, newCode(c.game.id), isObj(m.options) ? m.options : {}),
  quick(c, m) {
    const o = isObj(m.options) ? m.options : {};
    const tag = str(m.tag, 64, '');
    const type = o.type != null && o.type !== '' ? String(o.type) : null;
    let best = null;
    for (const r of rooms.values()) {
      if (r.game !== c.game || r.private || r.locked || r.password || r.whitelist || r.tag !== tag || r.type !== type) continue;
      if (r.players.size >= r.maxPlayers || r.banned.has(c.pid)) continue;
      if (!best || r.players.size > best.players.size) best = r;
    }
    return joinRoom(c, best ? best.id : newCode(c.game.id), { ...o, tag, queue: false });
  },
  leave(c) { if (c.room) removeMember(c.room, c.pid, 'left'); return true; },
  'queue.leave'(c) { return leaveQueue(c); },

  'room.update'(c, m) {
    const r = needHost(c);
    if (m.locked != null) r.locked = !!m.locked;
    if (m.private != null) r.private = !!m.private;
    if (m.maxPlayers != null && !r.server) r.maxPlayers = Math.max(1, Math.min(limits(c.game).maxRoomPlayers, Math.floor(+m.maxPlayers) || r.maxPlayers));
    if (m.stateWrite != null) r.stateWrite = m.stateWrite === 'host' ? 'host' : 'all';
    if (isObj(m.meta)) safeMerge(r.meta, m.meta);
    if (m.host != null) {
      const t = r.players.get(String(m.host));
      if (!t || !t.connected) throw new E('bad_player');
      r.host = t.id; broadcast(r, { t: 'host', id: t.id });
    }
    broadcast(r, { t: 'room', room: roomInfo(r) });
    drainQueue(r);
    return roomInfo(r);
  },
  kick(c, m) {
    const r = needHost(c);
    const id = String(m.id), t = r.players.get(id);
    if (!t || id === c.pid) throw new E('bad_player');
    if (m.ban) r.banned.add(id);
    if (t.client) send(t.client, { t: 'left', room: r.id, reason: 'kicked' });
    removeMember(r, id, 'kicked');
    return true;
  },

  'state.set'(c, m) {
    const r = needRoom(c);
    if (r.stateWrite === 'host' && r.host !== c.pid) throw new E('not_host', 'Room state is host-only');
    if (scripts.authoritative(c.game.id)) throw new E('forbidden', 'Room state is server-authoritative; send an action to the server instead');
    let ops = m.ops;
    if (!Array.isArray(ops)) ops = [[m.path, m.value, m.del ? 1 : 0]];
    if (ops.length > 256) throw new E('too_many_ops');
    const out = [];
    for (const op of ops) {
      if (!Array.isArray(op)) throw new E('bad_op');
      const [p, v, del] = op;
      const isDel = !!del || v === undefined;
      setPath(r.state, p, v, isDel);
      out.push(isDel ? [p, null, 1] : [p, v]);
    }
    broadcast(r, { t: 'state', ops: out, by: c.pid });
    return true;
  },
  'player.set'(c, m, unreliable) {
    const r = needRoom(c);
    const mem = r.players.get(c.pid);
    if (!isObj(m.data)) throw new E('bad_data');
    // Fast-lane snapshots carry a sequence number: a late or duplicate one must not roll the data back.
    const seq = m.s != null && Number.isFinite(+m.s) ? +m.s : null;
    if (seq != null) { if (seq <= (mem.pseq || 0)) return true; mem.pseq = seq; }
    if (m.replace) mem.data = safeMerge({}, m.data); else safeMerge(mem.data, m.data);
    const msg = { t: 'player', id: c.pid, data: m.data, replace: !!m.replace };
    if (seq != null) msg.s = seq;
    if (r.aoi) aoi.place(r, mem);
    tell(r, mem, msg, unreliable);
    return true;
  },
  send(c, m, unreliable) {
    const msg = { t: 'msg', type: str(m.type, 64, 'message'), data: m.data ?? null, from: c.pid };
    const to = m.to;
    if (!c.room) { // direct message to any online player of the game
      if (typeof to !== 'string') throw new E('not_in_room');
      const target = online.get(rkey(c.game.id, to));
      if (!target) throw new E('player_offline');
      if (target.blocked && target.blocked.has(c.pid)) throw new E('blocked');
      msg.direct = true;
      send(target, msg, unreliable);
      return true;
    }
    const r = c.room;
    if (to === 'server') {
      if (!scripts.has(c.game.id)) throw new E('no_server_script', 'This game has no server script');
      scripts.call(r, 'onMessage', pubPlayer(r.players.get(c.pid)), msg.type, msg.data);
      return true;
    }
    if (to == null || to === 'all') return broadcast(r, msg, m.self ? null : c.pid, unreliable), true;
    if (to === 'nearby') { // everyone who can see me (everyone else in a room without interest management)
      tell(r, r.players.get(c.pid), msg, unreliable);
      if (m.self) send(c, msg, unreliable);
      return true;
    }
    const ids = to === 'host' ? [r.host] : Array.isArray(to) ? to.slice(0, 256) : [to];
    for (const id of ids) {
      const t = r.players.get(String(id));
      if (t && t.client) send(t.client, msg, unreliable);
    }
    return true;
  },

  async 'auth.register'(c, m) {
    const u = str(m.username, 24, '').toLowerCase();
    const p = str(m.password, 128, '');
    if (!/^[a-z0-9_.\-]{3,24}$/.test(u)) throw new E('bad_username', 'Username: 3-24 chars, a-z 0-9 _ . -');
    if (p.length < 6) throw new E('bad_password', 'Password must be at least 6 characters');
    if (S.accByPlayer.get(c.game.id, c.pid)) throw new E('already_registered');
    if (S.accGet.get(c.game.id, u)) throw new E('username_taken');
    const salt = rid(12);
    const hash = salt + ':' + (await scrypt(p, salt, 32)).toString('hex');
    if (S.accGet.get(c.game.id, u)) throw new E('username_taken');
    S.accInsert.run(c.game.id, u, hash, c.pid, Date.now());
    c.username = u;
    log(c.game.id, 'auth', `Account registered: ${u}`);
    return { playerId: c.pid, token: makeToken(c.game.id, c.pid), username: u, name: c.name };
  },
  async 'auth.login'(c, m) {
    const u = str(m.username, 24, '').toLowerCase();
    const acc = S.accGet.get(c.game.id, u);
    const [salt, hex] = acc ? acc.hash.split(':') : ['x', '00'.repeat(32)];
    const got = await scrypt(str(m.password, 128, ''), salt, 32);
    if (!acc || !crypto.timingSafeEqual(got, Buffer.from(hex, 'hex'))) {
      if (++c.loginFails > 8) setTimeout(() => c.ws.close(4029, 'too_many_attempts'), 10);
      throw new E('bad_credentials');
    }
    if (acc.player !== c.pid) {
      if (c.room) removeMember(c.room, c.pid, 'left');
      online.delete(rkey(c.game.id, c.pid));
      services.onOffline(c);
      claimIdentity(c, acc.player);
      const row = S.playerGet.get(c.game.id, acc.player);
      if (row && row.name) c.name = row.name;
      services.onOnline(c);
    }
    c.username = u;
    return { playerId: c.pid, token: makeToken(c.game.id, c.pid), username: u, name: c.name };
  },
};
Object.assign(H, services.handlers, servers.handlers);

function claimIdentity(c, pid) {
  const k = rkey(c.game.id, pid);
  const prev = online.get(k);
  if (prev && prev !== c) { send(prev, { t: 'error', code: 'replaced', message: 'Signed in elsewhere' }); prev.ws.close(4001, 'replaced'); }
  c.pid = pid;
  online.set(k, c);
}

function hello(c, m) {
  let game = gamesByKey.get(m.key);
  if (!game && CFG.openGames && typeof m.key === 'string' && m.key.length >= 8 && m.key.length <= 128) {
    game = createGame(str(m.game, 64, '') || 'Auto ' + m.key.slice(0, 6), {}, m.key);
  }
  if (!game) return c.ws.close(4003, 'bad_key');
  if (!game.enabled) return c.ws.close(4004, 'game_disabled');
  const origins = game.settings.allowedOrigins;
  if (Array.isArray(origins) && origins.length && !origins.includes(c.origin)) return c.ws.close(4003, 'origin_not_allowed');
  c.game = game;
  const pid = readToken(game.id, m.token) || 'p_' + rid(8);
  if ((game.settings.banned || []).includes(pid)) return c.ws.close(4005, 'banned');
  claimIdentity(c, pid);
  const row = S.playerGet.get(game.id, pid);
  c.name = cleanName(m.name) || (row && row.name) || 'Player-' + pid.slice(2, 6);
  c.username = (S.accByPlayer.get(game.id, pid) || {}).username || null;
  S.playerTouch.run(game.id, pid, c.name, Date.now(), Date.now());
  gameStats.get(game.id).lastActive = Date.now();
  send(c, { t: 'welcome', playerId: pid, token: makeToken(game.id, pid), name: c.name, username: c.username,
    serverTime: Date.now(), game: { id: game.id, name: game.name }, returning: !!row,
    transports: c.session ? net.offer(c) : undefined, ...services.welcomeExtras(c) });
  services.onOnline(c);
  if (!row) log(game.id, 'player', `New player ${c.name}`);
}

// v1 (legacy) clients: one bare JSON message per WebSocket frame.
function onLegacyMessage(c, raw) {
  let m;
  try { m = JSON.parse(raw); } catch { return send(c, { t: 'error', code: 'bad_json' }); }
  onMessage(c, m, raw.length, false);
}
// One application message, from any transport. unreliable = it came on the fast lane.
async function onMessage(c, m, size, unreliable) {
  // token bucket
  const now = Date.now();
  const lim = c.game ? limits(c.game) : CFG;
  c.tokens = Math.min(lim.burst, c.tokens + ((now - c.lastRefill) / 1000) * lim.rate);
  c.lastRefill = now;
  if (c.tokens < 1) {
    if (++c.dropped > 2000) return c.ws.close(4029, 'rate_limited');
    if (now - c.lastWarn > 1000) { c.lastWarn = now; send(c, { t: 'error', code: 'rate_limited' }); }
    return;
  }
  c.tokens--;
  stats.in++; stats.bytesIn += size;
  if (!isObj(m)) return;
  if (!c.game) {
    if (m.t !== 'hello') return c.ws.close(4000, 'hello_required');
    clearTimeout(c.helloTimer);
    return hello(c, m);
  }
  const gs = gameStats.get(c.game.id);
  if (gs) { gs.msgs++; gs.lastActive = now; }
  const fn = Object.hasOwn(H, m.t) ? H[m.t] : null;
  try {
    if (!fn) throw new E('unknown_type', `Unknown message type: ${m.t}`);
    const result = await fn(c, m, unreliable);
    if (m.rid != null) send(c, { t: 'res', rid: m.rid, ok: true, d: result === undefined ? null : result });
  } catch (e) {
    if (!(e instanceof E)) console.error('[mp] handler error', m.t, e);
    const err = { code: e.code || 'internal', message: e instanceof E ? e.message : 'Internal error' };
    if (m.rid != null) send(c, { t: 'res', rid: m.rid, ok: false, err });
    else send(c, { t: 'error', ...err, for: m.t });
  }
}

// ---------- HTTP: admin API + static ----------
const adminFails = new Map();
function clientIp(req) {
  if (CFG.trustProxy && req.headers['x-forwarded-for']) return String(req.headers['x-forwarded-for']).split(',')[0].trim();
  return req.socket.remoteAddress;
}
function json(res, code, body) {
  const s = JSON.stringify(body);
  res.writeHead(code, { 'content-type': 'application/json', 'cache-control': 'no-store' });
  res.end(s);
}
function readBody(req) {
  return new Promise((resolve, reject) => {
    let size = 0; const chunks = [];
    req.on('data', (d) => { size += d.length; if (size > 1 << 20) { reject(new E('too_large')); req.destroy(); } else chunks.push(d); });
    req.on('end', () => { try { resolve(chunks.length ? JSON.parse(Buffer.concat(chunks)) : {}); } catch { reject(new E('bad_json')); } });
    req.on('error', reject);
  });
}
function isAdmin(req, ip) {
  const f = adminFails.get(ip);
  if (f && f.n >= 20 && Date.now() - f.t < 600000) throw new E('rate_limited');
  const h = String(req.headers.authorization || '');
  const tok = Buffer.from(h.startsWith('Bearer ') ? h.slice(7) : '');
  const good = Buffer.from(ADMIN_TOKEN);
  if (tok.length === good.length && crypto.timingSafeEqual(tok, good)) return true;
  adminFails.set(ip, { n: (f && Date.now() - f.t < 600000 ? f.n : 0) + 1, t: Date.now() });
  return false;
}
// Measured numbers for one game: players online (now / averages / peaks), traffic per player, ping, room sizes.
function metrics(g) {
  const st = gameStats.get(g.id), now = Date.now();
  const window = (ms) => {
    const m = st.minutes.filter((x) => now - x.t <= ms);
    if (st.accN) m.push({ avg: st.acc / st.accN, max: st.accMax });
    return m.length ? { avg: Math.round((m.reduce((a, x) => a + x.avg, 0) / m.length) * 10) / 10, peak: Math.max(...m.map((x) => x.max)) } : { avg: null, peak: null };
  };
  let on = 0, pingSum = 0, pings = 0;
  for (const c of online.values()) if (c.game === g) { on++; if (typeof c.rtt === 'number') { pingSum += c.rtt; pings++; } }
  const sizes = [...rooms.values()].filter((r) => r.game === g && r.players.size).map((r) => r.players.size); // empty public servers don't count
  const h1 = window(3600000), h24 = window(86400000);
  return {
    online: on, avg1h: h1.avg, avg24h: h24.avg, peak1h: h1.peak, peak24h: h24.peak, peakSinceStart: st.peak,
    msgsPerSecond: st.rate, msgsPerPlayer: on ? Math.round((st.rate / on) * 10) / 10 : 0,
    avgPing: pings ? Math.round(pingSum / pings) : null,
    rooms: sizes.length, largestRoom: sizes.length ? Math.max(...sizes) : 0,
    avgRoomSize: sizes.length ? Math.round((sizes.reduce((a, b) => a + b, 0) / sizes.length) * 10) / 10 : 0,
    sampledMinutes: st.minutes.length,
  };
}
// Install / replace / remove a game's server script (admin API and setup API).
function setScript(g, code) {
  if (code.length > 512 * 1024) throw new E('too_large');
  g.settings.script = code || undefined;
  saveGame(g);
  for (const r of rooms.values()) if (r.game === g) { scripts.stopTick(r); r.api = null; }
  const s = code ? scripts.load(g) : (scripts.unload(g.id), null);
  if (s && !s.failed) for (const r of rooms.values()) if (r.game === g) scripts.startTick(r);
  log(g.id, 'admin', code ? 'Server script updated' : 'Server script removed');
  return scripts.info(g.id);
}
// Setup API auth: a game's setup key, with the same brute-force throttle as the admin token.
function setupAuth(req, ip) {
  const f = adminFails.get(ip);
  if (f && f.n >= 20 && Date.now() - f.t < 600000) throw new E('rate_limited');
  const h = String(req.headers.authorization || '');
  const g = setup.gameFor(h.startsWith('Bearer ') ? h.slice(7) : '');
  if (g) return g;
  adminFails.set(ip, { n: (f && Date.now() - f.t < 600000 ? f.n : 0) + 1, t: Date.now() });
  return null;
}
// asAdmin: the dashboard (admin token) — never locked out, and its chat messages come from the user.
async function setupRoute(g, req, sub, url, asAdmin, res) {
  const m = req.method;
  if (!asAdmin) {
    if (g.settings.setupDisabled) throw new E('setup_closed', 'Setup is finished and the setup API is closed. Game settings can only be changed in the dashboard (it can re-open setup).');
    setup.touch(g);
  }
  if (sub === 'messages') {
    if (m === 'POST') { const b = await readBody(req); return setup.post(g, asAdmin ? 'admin' : 'agent', b.text); }
    const after = Math.max(0, Math.floor(+url.searchParams.get('after') || 0));
    const wait = asAdmin ? 0 : +url.searchParams.get('wait') || 0;
    return { status: setup.status(g), messages: await setup.read(g, after, wait, (fn) => res.on('close', fn)) };
  }
  if (asAdmin && sub === 'reopen' && m === 'POST') return setup.reopen(g, 'sk_' + rid(18));
  if (sub === 'done' && m === 'POST') { const b = await readBody(req); return setup.finish(g, b.summary); }
  if (!sub && m === 'GET') return setup.view(g);
  if (sub === 'recommend' && m === 'POST') { const b = await readBody(req); return setup.recommend(isObj(b.profile) ? b.profile : b); }
  if (!sub && (m === 'PUT' || m === 'POST')) {
    const b = await readBody(req);
    const dry = url.searchParams.get('dryRun') === '1' || url.searchParams.get('dryRun') === 'true' || b.dryRun === true;
    return setup.apply(g, b, dry);
  }
  throw new E('not_found', 'Setup API: GET /api/setup · POST /api/setup/recommend · PUT /api/setup[?dryRun=1] · GET|POST /api/setup/messages · POST /api/setup/done');
}

const gameSummary = (g, counts) => {
  let on = 0, rc = 0;
  for (const c of online.values()) if (c.game === g) on++;
  for (const r of rooms.values()) if (r.game === g) rc++;
  const st = gameStats.get(g.id);
  const { script, ...settings } = g.settings;
  return { id: g.id, name: g.name, key: g.key, enabled: g.enabled, created: g.created, settings, hasScript: !!script, scriptOk: scripts.has(g.id),
    online: on, rooms: rc, totalPlayers: counts.get(g.id) || 0, msgRate: st.rate, peak: st.peak, lastActive: st.lastActive,
    avgPlayers1h: metrics(g).avg1h, expected: g.settings.profile ? g.settings.profile.players : null };
};
const trunc = (v, n = 20000) => { const s = JSON.stringify(v); return s.length > n ? s.slice(0, n) + '…(truncated)' : s; };

async function admin(req, res, parts, url) {
  const [, , , gid, sub, sid, act] = parts; // /api/admin/games/:gid/:sub/:sid/:act
  const method = req.method;
  if (parts[2] === 'overview' || (parts[2] === undefined && method === 'GET')) {
    const counts = new Map(S.playerCounts.all().map((r) => [r.game, r.n]));
    const mem = process.memoryUsage();
    return json(res, 200, {
      server: { uptime: Date.now() - started, started, node: process.version, rss: mem.rss, heap: mem.heapUsed,
        connections: conns.size, online: online.size, rooms: rooms.size, msgIn: stats.in, msgOut: stats.out,
        bytesIn: stats.bytesIn, bytesOut: stats.bytesOut, openGames: CFG.openGames, transports: net.info(), ...services.stats() },
      players: [...online.values()].slice(0, 500).map((c) => ({ game: c.game.id, gameName: c.game.name, id: c.pid, name: c.name,
        username: c.username, room: c.room ? c.room.id : null, since: c.since, rtt: c.rtt, transport: net.active(c) })),
      history: stats.history, games: [...games.values()].map((g) => gameSummary(g, counts)), events: events.slice(-100).reverse(),
    });
  }
  if (parts[2] !== 'games') throw new E('not_found');
  if (!gid) {
    if (method === 'POST') { const b = await readBody(req); return json(res, 201, createGame(str(b.name, 64, 'Untitled'), isObj(b.settings) ? b.settings : {})); }
    const counts = new Map(S.playerCounts.all().map((r) => [r.game, r.n]));
    return json(res, 200, [...games.values()].map((g) => gameSummary(g, counts)));
  }
  const g = games.get(gid);
  if (!g) throw new E('not_found');
  const roomOf = (id) => { const r = rooms.get(rkey(g.id, id)); if (!r) throw new E('not_found'); return r; };

  if (!sub) {
    if (method === 'DELETE') {
      for (const r of [...rooms.values()]) if (r.game === g) destroyRoom(r, 'game_deleted');
      for (const c of [...online.values()]) if (c.game === g) c.ws.close(4004, 'game_deleted');
      store.purgeGame(g.id); games.delete(g.id); gamesByKey.delete(g.key); gameStats.delete(g.id);
      log(g.id, 'game', `Game deleted: ${g.name}`);
      return json(res, 200, { ok: true });
    }
    if (method === 'PATCH') {
      const b = await readBody(req);
      if (b.name) g.name = str(b.name, 64, g.name);
      if (b.enabled != null) {
        g.enabled = !!b.enabled;
        if (!g.enabled) for (const c of [...online.values()]) if (c.game === g) c.ws.close(4004, 'game_disabled');
      }
      if (isObj(b.settings)) {
        if (b.settings.config !== undefined && b.settings.config !== null && !isObj(b.settings.config)) throw new E('bad_data', 'settings.config must be an object');
        g.settings = safeMerge(g.settings, b.settings);
        if ('config' in b.settings) services.pushConfig(g, online.values()); // remote config goes live at once
      }
      if (b.rotateKey) { gamesByKey.delete(g.key); g.key = 'pk_' + rid(18); gamesByKey.set(g.key, g); }
      if (b.rotateSetupKey) g.settings.setupKey = 'sk_' + rid(18);
      saveGame(g);
      log(g.id, 'game', `Game updated: ${g.name}`);
      return json(res, 200, gameSummary(g, new Map()));
    }
    const counts = new Map(S.playerCounts.all().map((r) => [r.game, r.n]));
    return json(res, 200, {
      ...gameSummary(g, counts), accounts: S.accCount.get(g.id).n, kvEntries: S.kvCount.get(g.id).n,
      metrics: metrics(g),
      rooms: [...rooms.values()].filter((r) => r.game === g).map((r) => ({ ...roomInfo(r), persist: r.persist, permanent: r.permanent, queue: r.waiting.length,
        players: [...r.players.values()].map((m) => ({ ...pubPlayer(m), rtt: rttOf(m), transport: m.client ? net.active(m.client) : null, data: trunc(m.data, 2000) })), state: trunc(r.state) })),
      recentPlayers: S.playersRecent.all(g.id, 50).map((p) => ({ ...p, online: online.has(rkey(g.id, p.id)) })),
      leaderboards: services.summary(g.id), ...services.stats(g.id), servers: servers.summary(g), limits: limits(g),
    });
  }
  if (sub === 'rooms' && sid && method === 'POST') {
    const r = roomOf(sid);
    if (act === 'close') { destroyRoom(r, 'closed_by_admin'); return json(res, 200, { ok: true }); }
    if (act === 'kick') {
      const b = await readBody(req); const t = r.players.get(String(b.player));
      if (!t) throw new E('not_found');
      if (b.ban) r.banned.add(t.id);
      if (t.client) send(t.client, { t: 'left', room: r.id, reason: 'kicked' });
      removeMember(r, t.id, 'kicked');
      return json(res, 200, { ok: true });
    }
    if (act === 'state') { // admin overwrite of room state
      const b = await readBody(req);
      if (!isObj(b.state)) throw new E('bad_data');
      r.state = safeMerge({}, b.state);
      broadcast(r, { t: 'state', ops: [], full: r.state, by: 'server' });
      return json(res, 200, { ok: true });
    }
  }
  if (sub === 'script') {
    if (method === 'GET') return json(res, 200, { code: g.settings.script || '', ...scripts.info(g.id) });
    if (method === 'PUT' || method === 'DELETE') {
      const b = method === 'PUT' ? await readBody(req) : {};
      const info = setScript(g, method === 'PUT' ? String(b.code ?? '') : '');
      return json(res, info.failed ? 400 : 200, info);
    }
  }
  if (sub === 'setup') { // the admin can use the setup API for any game: GET, PUT (?dryRun=1), POST setup/recommend
    return json(res, 200, await setupRoute(g, req, sid, url, true, res));
  }
  if (sub === 'players' && sid && method === 'POST') {
    const b = act === 'kick' ? await readBody(req) : {};
    const banned = new Set(g.settings.banned || []);
    if (act === 'unban') { banned.delete(sid); g.settings.banned = [...banned]; saveGame(g); return json(res, 200, { ok: true }); }
    if (act !== 'kick') throw new E('not_found');
    if (b.ban) { banned.add(sid); g.settings.banned = [...banned]; saveGame(g); }
    for (const r of [...rooms.values()]) if (r.game === g && r.players.has(sid)) removeMember(r, sid, 'kicked');
    const c = online.get(rkey(g.id, sid));
    if (c) { send(c, { t: 'error', code: 'kicked', message: str(b.reason, 200, 'Kicked by admin') }); c.ws.close(4005, b.ban ? 'banned' : 'kicked'); }
    log(g.id, 'admin', `${b.ban ? 'Banned' : 'Kicked'} player ${c ? c.name : sid}`);
    return json(res, 200, { ok: true, wasOnline: !!c });
  }
  if (sub === 'broadcast' && method === 'POST') {
    const b = await readBody(req);
    const msg = { t: 'msg', type: str(b.type, 64, 'announcement'), data: b.data ?? null, from: 'server' };
    let n = 0;
    for (const r of rooms.values()) if (r.game === g && (!b.room || r.id === b.room)) { broadcast(r, msg); n += r.players.size; }
    if (!b.room) for (const c of online.values()) if (c.game === g && !c.room) { send(c, msg); n++; }
    log(g.id, 'admin', `Broadcast "${msg.type}" to ${n} players`);
    return json(res, 200, { ok: true, delivered: n });
  }
  if (sub === 'kv') {
    if (method === 'GET') {
      if (url.searchParams.get('key') != null) {
        const row = S.kvGet.get(g.id, url.searchParams.get('scope') || 'game', url.searchParams.get('owner') || '', url.searchParams.get('key'));
        return json(res, 200, row ? { value: JSON.parse(row.v), updated: row.updated } : null);
      }
      const scope = url.searchParams.get('scope') || '', prefix = url.searchParams.get('prefix') || '';
      return json(res, 200, S.kvAdminList.all(g.id, scope, scope, prefix, prefix + '￿', Math.min(+url.searchParams.get('limit') || 200, 1000)));
    }
    const b = await readBody(req);
    const scope = str(b.scope, 16, 'game'), owner = str(b.owner, 64, ''), key = str(b.key, 128, '');
    if (!key) throw new E('bad_key');
    if (method === 'PUT') { services.storage.put(g.id, scope, owner, key, b.value); return json(res, 200, { ok: true }); }
    if (method === 'DELETE') { S.kvDel.run(g.id, scope, owner, key); return json(res, 200, { ok: true }); }
  }
  if (sub === 'leaderboards') {
    const b = method === 'PUT' || method === 'POST' ? await readBody(req) : {};
    return json(res, 200, services.adminLeaderboards(g, method, sid, act, parts[7], b, url));
  }
  if (sub === 'servers') {
    const b = method === 'POST' || method === 'PATCH' ? await readBody(req) : {};
    return json(res, method === 'POST' && !sid ? 201 : 200, servers.admin(g, method, sid, b));
  }
  if (sub === 'inbox' && method === 'POST') { // { to: playerId | [ids], type, data }
    const b = await readBody(req);
    const to = Array.isArray(b.to) ? b.to.slice(0, 1000) : [b.to];
    let n = 0;
    for (const id of to) { try { services.inbox.send(g.id, id, 'server', 'server', b.type, b.data); n++; } catch (e) { if (to.length === 1) throw e; } }
    log(g.id, 'admin', `Inbox "${str(b.type, 64, 'message')}" sent to ${n} player(s)`);
    return json(res, 200, { ok: true, delivered: n });
  }
  throw new E('not_found');
}

const STATIC = {
  '/': ['docs/index.html', 'text/html; charset=utf-8'],
  '/index.html': ['docs/index.html', 'text/html; charset=utf-8'],
  '/sdk.js': ['client/multiplayer.js', 'application/javascript; charset=utf-8'],
  '/multiplayer.js': ['client/multiplayer.js', 'application/javascript; charset=utf-8'],
  '/client/multiplayer.js': ['client/multiplayer.js', 'application/javascript; charset=utf-8'],
  '/demo': ['examples/demo.html', 'text/html; charset=utf-8'],
  '/MULTIPLAYER.md': ['MULTIPLAYER.md', 'text/markdown; charset=utf-8'],
};

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  res.setHeader('access-control-allow-origin', '*');
  res.setHeader('access-control-allow-headers', 'authorization, content-type');
  res.setHeader('access-control-allow-methods', 'GET, POST, PUT, PATCH, DELETE, OPTIONS');
  res.setHeader('x-content-type-options', 'nosniff');
  if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }
  const parts = url.pathname.split('/').filter(Boolean).map(decodeURIComponent);
  try {
    if (url.pathname === '/health') return json(res, 200, { ok: true, uptime: Date.now() - started, online: online.size, rooms: rooms.size });
    if (parts[0] === 'api' && parts[1] === 'setup') {
      const g = setupAuth(req, clientIp(req));
      if (!g) return json(res, 401, { error: 'unauthorized', message: 'Send Authorization: Bearer <setup key> (dashboard -> game -> Setup)' });
      return json(res, 200, await setupRoute(g, req, parts[2], url, false, res));
    }
    if (parts[0] === 'api' && parts[1] === 'admin') {
      if (!isAdmin(req, clientIp(req))) return json(res, 401, { error: 'unauthorized' });
      return await admin(req, res, parts, url);
    }
    const st = STATIC[url.pathname];
    if (st && req.method === 'GET') {
      const file = path.join(CFG.publicDir, st[0]);
      if (fs.existsSync(file)) {
        res.writeHead(200, { 'content-type': st[1], 'cache-control': 'no-cache' });
        return fs.createReadStream(file).pipe(res);
      }
    }
    json(res, 404, { error: 'not_found' });
  } catch (e) {
    const code = e.code === 'not_found' ? 404 : e.code === 'rate_limited' ? 429 : e.code === 'setup_closed' ? 403 : e instanceof E ? 400 : 500;
    if (code === 500) console.error('[mp] http error', e);
    json(res, code, { error: e.code || 'internal', message: e instanceof E ? e.message : 'Internal error' });
  }
});

const net = await createTransports(CFG, { deliver: (c, m, unreliable) => onMessage(c, m, 0, unreliable), log, onBytes: (n) => { stats.bytesIn += n; } });

// perMessageDeflate off: compressing many tiny frames costs CPU + latency and saves almost nothing.
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: CFG.maxMsg * 2, perMessageDeflate: false });
wss.on('connection', (ws, req) => {
  req.socket.setNoDelay(true);
  const ip = clientIp(req);
  const n = (ipConns.get(ip) || 0) + 1;
  if (n > CFG.maxConnsPerIp) return ws.close(4029, 'too_many_connections');
  ipConns.set(ip, n);
  const c = { ws, ip, origin: req.headers.origin || '', game: null, pid: null, name: '', username: null, room: null,
    tokens: CFG.burst, lastRefill: Date.now(), dropped: 0, lastWarn: 0, loginFails: 0, alive: true, missed: 0, rtt: null, pingAt: 0, since: Date.now() };
  c.helloTimer = setTimeout(() => !c.game && ws.close(4008, 'hello_timeout'), 10000);
  conns.add(c);
  ws.on('pong', () => { c.alive = true; if (c.pingAt) { c.rtt = Date.now() - c.pingAt; c.pingAt = 0; } });
  ws.on('message', (raw, bin) => {
    if (bin || raw.length > CFG.maxMsg) return send(c, { t: 'error', code: 'too_large' });
    const s = raw.toString();
    // v2 clients wrap everything in session frames (the first is always {"q":1,...}); v1 sends bare messages.
    if (!c.session && !c.legacy) { if (s.startsWith('{"q":')) net.attach(c); else c.legacy = true; }
    if (c.session) net.onFrame(c, c.links.websocket, s); else onLegacyMessage(c, s);
  });
  ws.on('close', () => {
    clearTimeout(c.helloTimer);
    conns.delete(c);
    net.detach(c);
    const left = (ipConns.get(ip) || 1) - 1;
    if (left) ipConns.set(ip, left); else ipConns.delete(ip);
    if (!c.game) return;
    if (online.get(rkey(c.game.id, c.pid)) === c) online.delete(rkey(c.game.id, c.pid));
    services.onOffline(c);
    leaveQueue(c);
    onDisconnect(c);
  });
  ws.on('error', () => {});
});
setInterval(() => {
  for (const c of conns) {
    if (!c.alive && ++c.missed >= 5) { c.ws.terminate(); continue; }
    if (c.alive) c.missed = 0;
    c.alive = false; c.pingAt = Date.now(); c.ws.ping();
  }
}, 5000).unref(); // also measures each client's RTT (shown in the dashboard)

// Push player-count changes (at most every 2 s): in AOI rooms clients can't count the players themselves.
setInterval(() => {
  for (const r of rooms.values()) if (r.lastCount !== r.players.size) { r.lastCount = r.players.size; broadcast(r, { t: 'room', room: { playerCount: r.players.size } }); }
}, 2000).unref();

servers.boot();
server.listen(CFG.port, () => {
  const t = net.info();
  console.log(`[mp] listening on :${CFG.port}  (ws path /ws, dashboard /, sdk /sdk.js, data ${CFG.dataDir})`);
  console.log(`[mp] transports: websocket, webrtc ${t.webrtc ? 'on' + (t.rtcPort ? ' (udp ' + t.rtcPort + ')' : '') : 'off'}, udp ${t.udp ? 'on :' + t.udp : 'off'}`);
  log(null, 'server', 'Server started');
});

function shutdown() {
  console.log('[mp] shutting down');
  for (const r of [...rooms.values()]) if (r.persist) S.kvSet.run(r.game.id, 'room', r.id, 'state', JSON.stringify(r.state), Date.now());
  for (const c of conns) c.ws.close(1012, 'server_restart');
  net.close();
  server.close();
  store.close();
  setTimeout(() => process.exit(0), 300);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

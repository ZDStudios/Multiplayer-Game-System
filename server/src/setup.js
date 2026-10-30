// Setup API: lets a coding agent (e.g. Claude adding multiplayer to a game) configure the game's server side
// itself, with that game's setup key (sk_...) instead of the admin token. The agent describes the game in a
// profile (players, update rate, latency target, data model...), asks for a recommendation, adjusts it, and
// applies it. The profile is stored and shown in the dashboard next to the measured numbers.
//   GET  /api/setup             current setup, live metrics, and the schema of everything that can be set
//   POST /api/setup/recommend   { profile } -> { setup, estimates, notes }
//   PUT  /api/setup             apply a setup document; ?dryRun=1 only validates -> report
import crypto from 'node:crypto';
import vm from 'node:vm';
import { normalizeAoi } from './aoi.js';

const TARGET_VISIBLE = 30;     // players one client should see at once in a big world
const BYTES_PER_UPDATE = 80;   // typical small JSON position update incl. framing
const SESSION_TYPES = ['match', 'lobby', 'persistent', 'open-world'];

export const SCHEMA = {
  profile: {
    genre: 'string, e.g. "top-down shooter", "open-world survival", "card game"',
    description: 'string: one paragraph about the game',
    sessionType: SESSION_TYPES.join(' | ') + ' (match = short games in rooms, lobby = hang-out rooms, persistent/open-world = always-on servers)',
    players: '{ average: concurrent players online on a normal day, peak: busiest moment, perRoom: players sharing one room/world, minPerRoom }',
    sendRate: 'Hz: how often each client sends its player data (positions) while active, 1-60',
    tickRate: 'Hz: server script onTick rate, 0-60 (0 = no server tick)',
    latencyTarget: 'ms: round trip the game feels good at (fighting ~60, shooters ~100, MMO ~200, turn-based ~1000)',
    movement: "'none' | '2d' | '3d'",
    positionKeys: "player-data fields holding the position, e.g. ['x','y'] or ['x','y','z']",
    worldSize: 'side length of the playable world in game units (used to size the area of interest)',
    viewDistance: 'how far a player can see in game units (becomes aoi.radius)',
    competitive: 'boolean: cheating matters (server-authoritative data and leaderboards)',
    regions: "array of region names for public servers, e.g. ['au', 'us']",
    dataModel: '{ player: {field: type}, state: {...}, storage: {...}, messages: {...} }: documentation of the data the game syncs',
    notes: 'string',
  },
  limits: '{ maxRoomPlayers, maxRooms, defaultMaxPlayers, rateLimit, rateBurst, reconnectGraceMs }',
  roomTypes: '{ name: room options (maxPlayers, aoi, stateWrite, private, persist, meta, tag, clientOptions:false) }',
  roomDefaults: 'room options for rooms created without a type',
  requireRoomType: 'boolean',
  storage: "{ playerDataWrite: 'all' | 'none', gameDataWrite: 'all' | 'none' }",
  leaderboards: '{ boardId: { mode: max|min|sum|last, sort: asc|desc, periods: [day, week, month], clientWrite, min, max } }',
  lockLeaderboards: 'boolean: clients cannot create new boards',
  inboxClientSend: 'boolean: players may send inbox messages to each other',
  servers: '[{ id, name, description, region, tags, maxPlayers, password, visibility, whitelist, queue, motd, type, options, enabled }]',
  replaceServers: 'boolean: delete servers that are not in the list',
  config: 'remote config object, readable by clients as mp.config',
  script: 'server script source (CommonJS, see MULTIPLAYER.md §5b); "" removes it',
};

export function createSetup(d) {
  const { E, games, saveGame, isObj, str, safeMerge, services, servers, scripts, setScript, metrics, log, rooms } = d;
  const num = (v, lo, hi) => (v == null || v === '' || !Number.isFinite(+v) ? null : Math.max(lo, Math.min(hi, +v)));
  const strs = (v, n, len) => (Array.isArray(v) ? v.slice(0, n).map((x) => str(String(x), len, '')).filter(Boolean) : []);

  function normProfile(p = {}) {
    if (!isObj(p)) throw new E('bad_profile', 'profile must be an object');
    const pl = isObj(p.players) ? p.players : {};
    const dm = p.dataModel === undefined ? null : p.dataModel;
    if (dm != null && JSON.stringify(dm).length > 8192) throw new E('too_large', 'profile.dataModel must be under 8 KB');
    const keys = Array.isArray(p.positionKeys) ? strs(p.positionKeys, 3, 32) : null;
    return {
      genre: str(p.genre, 64, ''), description: str(p.description, 500, ''),
      sessionType: SESSION_TYPES.includes(p.sessionType) ? p.sessionType : 'match',
      players: { average: num(pl.average, 0, 1e7), peak: num(pl.peak, 0, 1e7), perRoom: num(pl.perRoom, 1, 5000), minPerRoom: num(pl.minPerRoom, 1, 5000) },
      sendRate: num(p.sendRate, 0, 60), tickRate: num(p.tickRate, 0, 60), latencyTarget: num(p.latencyTarget, 10, 10000),
      movement: ['none', '2d', '3d'].includes(p.movement) ? p.movement : 'none',
      positionKeys: keys && keys.length >= 2 ? keys : null,
      worldSize: num(p.worldSize, 0, 1e9), viewDistance: num(p.viewDistance, 0, 1e9),
      competitive: !!p.competitive, regions: strs(p.regions, 20, 32),
      dataModel: dm, notes: str(p.notes, 2000, ''),
    };
  }

  // Traffic a room setup implies, and whether it fits one VPS.
  function estimate(p, perRoom, aoi) {
    const per = perRoom || p.players.perRoom || 8, rate = p.sendRate || 10;
    const visible = Math.max(0, aoi ? Math.min(per - 1, TARGET_VISIBLE) : per - 1);
    const peak = p.players.peak || p.players.average || per;
    const total = Math.round(peak * rate * visible);
    return {
      playersSeenPerClient: visible,
      updatesPerClientPerSecond: Math.round(rate * visible),
      downloadPerClientKBs: Math.round((rate * visible * BYTES_PER_UPDATE) / 102.4) / 10,
      sendsPerRoomPerSecond: Math.round(per * rate * visible),
      serverSendsPerSecondAtPeak: total,
      capacity: total < 50000 ? 'fits a 2 vCPU VPS' : total < 150000 ? 'needs about 4 vCPU' : 'too much for one server: lower sendRate, use aoi, or split rooms',
    };
  }

  function recommend(profile) {
    const p = normProfile(profile), notes = [];
    const per = p.players.perRoom || (p.sessionType === 'open-world' ? 200 : p.sessionType === 'persistent' ? 50 : 8);
    const persistent = p.sessionType === 'persistent' || p.sessionType === 'open-world';
    const sendRate = p.sendRate || (p.movement === 'none' ? 2 : 20);
    if (!p.players.perRoom) notes.push(`players.perRoom not given: assumed ${per}.`);
    let aoi = null;
    if (p.movement !== 'none' && (per > 32 || p.sessionType === 'open-world')) {
      const radius = p.viewDistance || (p.worldSize ? Math.round(p.worldSize * Math.sqrt(TARGET_VISIBLE / (Math.PI * per))) : null);
      aoi = { radius: radius || 100, keys: p.positionKeys || (p.movement === '3d' ? ['x', 'y', 'z'] : ['x', 'y']) };
      if (!radius) notes.push('aoi.radius is a placeholder (100): set it to the view distance in world units, or pass viewDistance / worldSize.');
      notes.push(`Interest management on: each player receives ~${TARGET_VISIBLE} nearby players instead of all ${per}.`);
    } else if (per > 32) notes.push(`${per} players per room without positions: every update reaches everyone. Keep per-player updates rare (sendRate ≤ 5) or add positions + aoi.`);
    const typeName = persistent ? 'world' : p.sessionType === 'lobby' ? 'lobby' : 'match';
    const main = { maxPlayers: per };
    if (aoi) main.aoi = aoi;
    if (persistent) main.persist = true;
    if (p.competitive) main.stateWrite = 'host';
    if (p.sessionType === 'match') main.private = false;
    const roomTypes = { [typeName]: main };
    if (p.sessionType === 'match' || p.sessionType === 'lobby') roomTypes.private = { ...main, private: true };
    const rateLimit = Math.max(60, Math.ceil(sendRate * 2 + 20 + (p.tickRate ? 10 : 0)));
    const setup = {
      profile: { ...p, sendRate, players: { ...p.players, perRoom: per } },
      limits: { maxRoomPlayers: Math.max(per, 16), defaultMaxPlayers: per, rateLimit, rateBurst: rateLimit * 3,
        reconnectGraceMs: persistent ? 60000 : p.sessionType === 'match' ? 20000 : 15000 },
      roomTypes, requireRoomType: true,
      storage: { playerDataWrite: p.competitive ? 'none' : 'all', gameDataWrite: 'none' },
      lockLeaderboards: p.competitive,
    };
    if (persistent) {
      const peak = p.players.peak || p.players.average || per;
      const regions = p.regions.length ? p.regions : ['main'];
      const each = Math.max(1, Math.ceil(peak / per / regions.length));
      setup.servers = [];
      for (const r of regions) for (let i = 1; i <= Math.min(each, 10); i++) {
        setup.servers.push({ id: `${r}-${i}`.toLowerCase().replace(/[^a-z0-9_-]/g, '-'), name: `${r.toUpperCase()} #${i}`, region: r, maxPlayers: per, type: typeName, queue: true });
      }
      notes.push(`${setup.servers.length} public server(s) for a peak of ${peak} players at ${per} per server.`);
    }
    if (p.competitive) notes.push('Competitive: players can read but not write their own storage (playerDataWrite none) and boards should use clientWrite:false. Put rules, scoring and rewards in the server script.');
    if (p.latencyTarget && p.latencyTarget < 80) notes.push(`Latency target ${p.latencyTarget} ms: host the server in the players' region, send movement with setPlayer(..., {unreliable: true}), and use prediction (mp.predict).`);
    if (sendRate > 30) notes.push(`sendRate ${sendRate} Hz is high; 20-30 Hz with mp.predict() usually looks the same and halves traffic.`);
    if (p.tickRate) notes.push(`Server script tickRate: set module.exports.tickRate = ${p.tickRate} in the script.`);
    return { setup, estimates: estimate({ ...p, sendRate }, per, aoi), notes };
  }

  // ---- validation of a setup document (nothing is changed until everything is valid) ----
  function validateRoomOptions(o, where) {
    if (!isObj(o)) throw new E('bad_setup', `${where} must be an object`);
    if (o.aoi !== undefined && normalizeAoi(o.aoi) === undefined) throw new E('bad_setup', `${where}.aoi: { radius > 0, keys: 2-3 field names }`);
    if (o.maxPlayers !== undefined && !(+o.maxPlayers >= 1)) throw new E('bad_setup', `${where}.maxPlayers must be ≥ 1`);
  }
  function validate(g, doc) {
    if (!isObj(doc)) throw new E('bad_setup', 'Send a JSON object (see GET /api/setup -> schema)');
    const unknown = Object.keys(doc).filter((k) => !Object.hasOwn(SCHEMA, k) && k !== 'dryRun');
    if (unknown.length) throw new E('bad_setup', `Unknown field(s): ${unknown.join(', ')}`);
    const out = { settings: {} };
    if (doc.profile !== undefined) out.profile = normProfile(doc.profile);
    if (doc.limits !== undefined) {
      if (!isObj(doc.limits)) throw new E('bad_setup', 'limits must be an object');
      const ok = ['maxRoomPlayers', 'maxRooms', 'defaultMaxPlayers', 'rateLimit', 'rateBurst', 'reconnectGraceMs'];
      for (const [k, v] of Object.entries(doc.limits)) if (!ok.includes(k) || !Number.isFinite(+v)) throw new E('bad_setup', `limits.${k}: one of ${ok.join(', ')}, as a number`);
      out.settings.limits = doc.limits;
    }
    if (doc.roomTypes !== undefined) {
      if (!isObj(doc.roomTypes)) throw new E('bad_setup', 'roomTypes must be an object of name -> room options');
      for (const [k, v] of Object.entries(doc.roomTypes)) { if (!/^[A-Za-z0-9_-]{1,64}$/.test(k)) throw new E('bad_setup', `roomTypes: bad name "${k}"`); validateRoomOptions(v, 'roomTypes.' + k); }
      out.settings.roomTypes = doc.roomTypes;
    }
    if (doc.roomDefaults !== undefined) { validateRoomOptions(doc.roomDefaults, 'roomDefaults'); out.settings.roomDefaults = doc.roomDefaults; }
    if (doc.requireRoomType !== undefined) out.settings.requireRoomType = !!doc.requireRoomType || null;
    if (doc.storage !== undefined) {
      if (!isObj(doc.storage)) throw new E('bad_setup', 'storage must be an object');
      for (const k of ['playerDataWrite', 'gameDataWrite']) if (doc.storage[k] !== undefined) {
        if (!['all', 'none'].includes(doc.storage[k])) throw new E('bad_setup', `storage.${k}: 'all' | 'none'`);
        out.settings[k] = doc.storage[k];
      }
    }
    for (const k of ['lockLeaderboards', 'inboxClientSend']) if (doc[k] !== undefined) out.settings[k] = !!doc[k];
    if (doc.config !== undefined) { if (!isObj(doc.config)) throw new E('bad_setup', 'config must be an object'); out.settings.config = doc.config; }
    if (doc.leaderboards !== undefined) {
      if (!isObj(doc.leaderboards)) throw new E('bad_setup', 'leaderboards must be an object of boardId -> config');
      out.leaderboards = Object.entries(doc.leaderboards).map(([id, c]) => {
        if (!/^[A-Za-z0-9_\-.:]{1,64}$/.test(id) || !isObj(c)) throw new E('bad_setup', `leaderboards.${id}: bad id or config`);
        return [id, services.leaderboard.normalize(c)];
      });
    }
    // Room types the servers point at must exist after this update.
    const types = { ...(isObj(g.settings.roomTypes) ? g.settings.roomTypes : {}), ...(out.settings.roomTypes || {}) };
    if (doc.servers !== undefined) {
      if (!Array.isArray(doc.servers) || doc.servers.length > 200) throw new E('bad_setup', 'servers must be an array (max 200)');
      out.servers = doc.servers.map((s, i) => {
        if (!isObj(s) || typeof s.id !== 'string' || !/^[A-Za-z0-9_-]{1,64}$/.test(s.id)) throw new E('bad_setup', `servers[${i}].id: [A-Za-z0-9_-]{1,64} required`);
        if (s.type && !Object.hasOwn(types, s.type)) throw new E('bad_setup', `servers[${i}].type "${s.type}" is not a room type`);
        if (s.options !== undefined) validateRoomOptions(s.options, `servers[${i}].options`);
        servers.validate(g, s, i);
        return s;
      });
      out.replaceServers = !!doc.replaceServers;
    }
    if (doc.script !== undefined) {
      const code = String(doc.script ?? '');
      if (code.length > 512 * 1024) throw new E('too_large', 'script is over 512 KB');
      if (code) try { new vm.Script(code, { filename: 'setup-script.js' }); } catch (e) { throw new E('bad_script', 'script does not compile: ' + e.message); }
      out.script = code;
    }
    return out;
  }

  function warningsFor(g, v) {
    const p = v.profile || g.settings.profile, w = [];
    if (!p) { w.push('No profile: describe the game (players, sendRate, latencyTarget) so the dashboard can compare expected vs measured.'); return w; }
    const types = isObj(g.settings.roomTypes) ? g.settings.roomTypes : {};
    for (const [k, t] of Object.entries(types)) if ((+t.maxPlayers || 0) > 32 && !t.aoi && p.movement !== 'none') w.push(`roomTypes.${k}: ${t.maxPlayers} players without aoi; every position update reaches everyone.`);
    const per = p.players && p.players.perRoom;
    const lim = g.settings.limits || {};
    if (per && lim.maxRoomPlayers && per > lim.maxRoomPlayers) w.push(`profile.players.perRoom (${per}) is above limits.maxRoomPlayers (${lim.maxRoomPlayers}).`);
    if (p.sendRate && lim.rateLimit && p.sendRate * 1.5 > lim.rateLimit) w.push(`limits.rateLimit (${lim.rateLimit}/s) is close to sendRate (${p.sendRate} Hz); messages may be dropped.`);
    return w;
  }

  function apply(g, doc, dryRun) {
    const v = validate(g, doc);
    const report = { dryRun: !!dryRun, applied: [], servers: { created: [], updated: [], deleted: [] }, leaderboards: [] };
    if (dryRun) {
      report.applied = [...(v.profile ? ['profile'] : []), ...Object.keys(v.settings), ...(v.leaderboards ? ['leaderboards'] : []), ...(v.servers ? ['servers'] : []), ...(v.script !== undefined ? ['script'] : [])];
      report.warnings = warningsFor({ settings: { ...g.settings, ...v.settings } }, v);
      return report;
    }
    if (v.profile) { g.settings.profile = { ...v.profile, updated: Date.now() }; report.applied.push('profile'); }
    if (Object.keys(v.settings).length) {
      const hadConfig = 'config' in v.settings;
      g.settings = safeMerge(g.settings, v.settings);
      report.applied.push(...Object.keys(v.settings));
      if (hadConfig) services.pushConfig(g, d.online.values());
    }
    saveGame(g);
    for (const [id, c] of v.leaderboards || []) { services.adminLeaderboards(g, 'PUT', id, 'config', null, c, null); report.leaderboards.push(id); }
    if (v.servers) {
      const existing = new Set(servers.admin(g, 'GET', null, {}).map((s) => s.id));
      for (const s of v.servers) {
        if (existing.has(s.id)) { servers.admin(g, 'PATCH', s.id, s); report.servers.updated.push(s.id); }
        else { servers.admin(g, 'POST', null, s); report.servers.created.push(s.id); }
      }
      if (v.replaceServers) for (const id of existing) if (!v.servers.some((s) => s.id === id)) { servers.admin(g, 'DELETE', id, {}); report.servers.deleted.push(id); }
    }
    if (v.script !== undefined) { report.script = setScript(g, v.script); report.applied.push('script'); }
    g.settings.setupApplied = Date.now(); saveGame(g);
    report.warnings = warningsFor(g, v);
    log(g.id, 'setup', `Setup applied: ${report.applied.concat(v.servers ? ['servers'] : [], v.leaderboards ? ['leaderboards'] : []).join(', ')}`);
    return report;
  }

  function view(g) {
    const st = g.settings;
    const pick = ['limits', 'roomTypes', 'roomDefaults', 'requireRoomType', 'lockLeaderboards', 'inboxClientSend', 'config'];
    return {
      game: { id: g.id, name: g.name, key: g.key },
      profile: st.profile || null,
      setup: { ...Object.fromEntries(pick.map((k) => [k, st[k] ?? null])), storage: { playerDataWrite: st.playerDataWrite || 'all', gameDataWrite: st.gameDataWrite || 'all' } },
      servers: servers.admin(g, 'GET', null, {}),
      leaderboards: services.leaderboard.boards(g.id),
      script: scripts.info(g.id),
      metrics: metrics(g),
      estimates: st.profile ? estimate(st.profile, st.profile.players.perRoom, Object.values(st.roomTypes || {}).some((t) => t && t.aoi)) : null,
      warnings: warningsFor(g, {}),
      setupApplied: st.setupApplied || null,
      status: status(g), setupCompleted: st.setupCompleted || null,
      chat: chat(g, 0, 100),
      schema: SCHEMA,
      endpoints: { view: 'GET /api/setup', recommend: 'POST /api/setup/recommend {profile}', apply: 'PUT /api/setup {..setup}', dryRun: 'PUT /api/setup?dryRun=1',
        say: 'POST /api/setup/messages {text}', read: 'GET /api/setup/messages?after=<id>&wait=<seconds>', done: 'POST /api/setup/done {summary}' },
    };
  }

  // Find the game a setup key belongs to (keys live in game settings; there are few games, so a scan is fine).
  // A finished setup still matches, so the agent gets a clear "setup_closed" instead of "unauthorized".
  function gameFor(key) {
    if (typeof key !== 'string' || !key.startsWith('sk_')) return null;
    const k = Buffer.from(key);
    for (const g of games.values()) {
      const s = Buffer.from(String(g.settings.setupKey || ''));
      if (s.length === k.length && crypto.timingSafeEqual(s, k)) return g;
    }
    return null;
  }

  // ---- setup chat: the agent explains what it's doing and asks questions; the user answers in the dashboard ----
  const M = {
    add: d.db.prepare('INSERT INTO setup_messages (game, sender, text, created) VALUES (?, ?, ?, ?) RETURNING id'),
    after: d.db.prepare('SELECT id, sender, text, created FROM setup_messages WHERE game = ? AND id > ? ORDER BY id LIMIT ?'),
    last: d.db.prepare('SELECT id, sender, text, created FROM setup_messages WHERE game = ? ORDER BY id DESC LIMIT ?'),
    trim: d.db.prepare('DELETE FROM setup_messages WHERE game = ? AND id <= (SELECT id FROM setup_messages WHERE game = ? ORDER BY id DESC LIMIT 1 OFFSET 500)'),
  };
  const waiters = new Map(); // gid -> Set<resolve>, agents long-polling for the user's reply
  const status = (g) => (g.settings.setupDisabled ? 'done' : g.settings.setupStatus || 'waiting');
  function post(g, sender, text) {
    text = str(text, 4000, '').trim();
    if (!text) throw new E('bad_message', 'text is required (up to 4000 characters)');
    const id = M.add.get(g.id, sender, text, Date.now()).id;
    M.trim.run(g.id, g.id);
    if (sender === 'agent') log(g.id, 'setup', 'Agent: ' + (text.length > 140 ? text.slice(0, 140) + '…' : text));
    if (sender === 'admin') for (const w of waiters.get(g.id) || []) w();
    return { id };
  }
  const chat = (g, after = 0, limit = 200) => (after > 0 ? M.after.all(g.id, after, limit) : M.last.all(g.id, limit).reverse());
  // Messages after `after`; with wait (seconds, max 60) it holds the request until the user replies.
  async function read(g, after, wait, onAbort) {
    const fresh = () => M.after.all(g.id, after, 200);
    let out = fresh();
    if (out.some((x) => x.sender === 'admin') || !(wait > 0)) return out;
    await new Promise((resolve) => {
      const set = waiters.get(g.id) || new Set();
      waiters.set(g.id, set);
      const done = () => { clearTimeout(t); set.delete(done); if (!set.size) waiters.delete(g.id); resolve(); };
      const t = setTimeout(done, Math.min(60, wait) * 1000);
      set.add(done);
      onAbort(done);
    });
    return fresh();
  }
  function touch(g) { // first agent call moves the setup from "waiting" to "in progress"
    if (status(g) === 'waiting') { g.settings.setupStatus = 'in_progress'; saveGame(g); }
  }
  // The agent is finished: keep its summary, close the setup API. From now on only the dashboard changes settings.
  function finish(g, summary) {
    if (summary) post(g, 'agent', summary);
    g.settings.setupStatus = 'done';
    g.settings.setupDisabled = true;
    g.settings.setupCompleted = Date.now();
    saveGame(g);
    for (const w of waiters.get(g.id) || []) w();
    log(g.id, 'setup', 'Setup finished by the agent; setup API closed');
    return { done: true, status: 'done', message: 'Setup API closed. Game settings can now only be changed in the dashboard.' };
  }
  // Dashboard: let an agent in again, with a fresh key (the old one stays dead).
  function reopen(g, newKey) {
    g.settings.setupDisabled = false;
    g.settings.setupStatus = 'waiting';
    g.settings.setupKey = newKey;
    saveGame(g);
    log(g.id, 'setup', 'Setup re-opened with a new key');
    return { status: 'waiting', setupKey: newKey };
  }

  return { recommend, apply, view, gameFor, normProfile, estimate, post, chat, read, touch, finish, reopen, status };
}

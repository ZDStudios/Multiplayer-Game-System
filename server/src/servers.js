// Public servers: permanent, listed rooms the admin defines, like the server list of an open-world or survival
// game. Each one is a room that never closes when empty, saves its state, and shows up in mp.servers.list()
// with its player count, region and tags. Players join with mp.joinServer(id), and wait in a queue when it's full.
const ID_RE = /^[A-Za-z0-9_-]{1,64}$/;
const SAVE_EVERY_MS = 30000;

export function createServers(d) {
  const { db, E, rooms, rkey, createRoom, destroyRoom, drainQueue, isObj, str, log, games, limits } = d;
  const q = (sql) => db.prepare(sql);
  const S = {
    all: q('SELECT game, id, config, created FROM servers ORDER BY created'),
    put: q('INSERT INTO servers (game, id, config, created) VALUES (?, ?, ?, ?) ON CONFLICT (game, id) DO UPDATE SET config = excluded.config'),
    del: q('DELETE FROM servers WHERE game = ? AND id = ?'),
    saveState: q(`INSERT INTO kv (game, scope, owner, k, v, updated) VALUES (?, 'room', ?, 'state', ?, ?)
                  ON CONFLICT (game, scope, owner, k) DO UPDATE SET v = excluded.v, updated = excluded.updated`),
  };
  const strs = (v, n, len) => (Array.isArray(v) ? v.slice(0, n).map((x) => str(String(x), len, '')).filter(Boolean) : []);

  function normalize(game, b, cur = {}) {
    const has = (k) => b[k] !== undefined;
    const c = {
      name: has('name') ? str(b.name, 64, '') : cur.name,
      description: has('description') ? str(b.description, 500, '') : cur.description || '',
      region: has('region') ? str(b.region, 32, '') : cur.region || '',
      tags: has('tags') ? strs(b.tags, 16, 32) : cur.tags || [],
      maxPlayers: has('maxPlayers') ? Math.floor(+b.maxPlayers) : cur.maxPlayers || 100,
      password: has('password') ? str(b.password, 64, '') || null : cur.password || null,
      visibility: has('visibility') ? (b.visibility === 'unlisted' ? 'unlisted' : 'public') : cur.visibility || 'public',
      whitelist: has('whitelist') ? strs(b.whitelist, 5000, 64) : cur.whitelist || [],
      queue: has('queue') ? b.queue !== false : cur.queue !== false,  // wait in line when full
      motd: has('motd') ? str(b.motd, 500, '') : cur.motd || '',
      type: has('type') ? (b.type ? str(b.type, 64, '') : null) : cur.type || null,   // a room type from the game settings
      options: has('options') ? (isObj(b.options) ? b.options : {}) : cur.options || {}, // any room option: aoi, stateWrite, meta, persist...
      enabled: has('enabled') ? b.enabled !== false : cur.enabled !== false,
    };
    if (!c.name) throw new E('bad_name', 'A server needs a name');
    if (!(c.maxPlayers >= 1 && c.maxPlayers <= limits(game).maxRoomPlayers)) throw new E('bad_value', `maxPlayers must be 1-${limits(game).maxRoomPlayers}`);
    return c;
  }

  // Room settings that come from the server config always win over the room type and options.
  function apply(room, cfg) {
    room.server = cfg;
    room.permanent = true;
    room.private = true; // not in the ad-hoc room list; servers have their own list
    room.maxPlayers = cfg.maxPlayers;
    room.password = cfg.password;
    room.whitelist = cfg.whitelist.length ? new Set(cfg.whitelist) : null;
    room.queueAllowed = cfg.queue;
    room.meta = { ...room.meta, server: { name: cfg.name, region: cfg.region, motd: cfg.motd } };
    drainQueue(room);
  }
  function start(game, id, cfg) {
    let room = rooms.get(rkey(game.id, id));
    if (!room) {
      const o = { ...cfg.options, type: cfg.type || undefined, persist: cfg.options.persist !== false };
      room = createRoom(game, id, o, true);
    }
    apply(room, cfg);
    return room;
  }
  function stop(room, reason) { room.permanent = false; destroyRoom(room, reason); }

  const info = (room, admin) => {
    const c = room.server;
    return {
      id: room.id, name: c.name, description: c.description, region: c.region, tags: c.tags, motd: c.motd, type: c.type,
      players: room.players.size, maxPlayers: room.maxPlayers, full: room.players.size >= room.maxPlayers,
      queue: room.waiting ? room.waiting.length : 0, hasPassword: !!c.password, whitelisted: c.whitelist.length > 0,
      visibility: c.visibility, meta: room.meta, aoi: room.aoi ? { radius: room.aoi.radius, keys: room.aoi.keys } : null,
      ...(admin ? { config: c, created: room.created } : {}),
    };
  };
  const serversOf = (game) => [...rooms.values()].filter((r) => r.game === game && r.server);

  // Boot: start every enabled server; save permanent state on a timer (and on shutdown via room.persist).
  function boot() {
    for (const r of S.all.all()) {
      const g = games.get(r.game);
      const cfg = JSON.parse(r.config);
      if (g && cfg.enabled !== false) try { start(g, r.id, { ...cfg, tags: cfg.tags || [], whitelist: cfg.whitelist || [], options: cfg.options || {} }); }
      catch (e) { console.error('[mp] server', r.id, 'failed to start:', e.message); }
    }
  }
  setInterval(() => {
    for (const room of rooms.values()) {
      if (!room.permanent || !room.persist) continue;
      const s = JSON.stringify(room.state);
      if (s !== room.savedState) { S.saveState.run(room.game.id, room.id, s, Date.now()); room.savedState = s; }
    }
  }, SAVE_EVERY_MS).unref();

  const handlers = {
    'servers.list'(c, m) {
      const tag = m.tag != null ? String(m.tag) : null, region = m.region != null ? String(m.region) : null;
      return serversOf(c.game).filter((r) => r.server.visibility === 'public' && (!tag || r.server.tags.includes(tag)) && (!region || r.server.region === region))
        .map((r) => info(r)).sort((a, b) => b.players - a.players);
    },
    'servers.get'(c, m) {
      const r = rooms.get(rkey(c.game.id, String(m.id ?? '')));
      if (!r || !r.server) throw new E('server_not_found');
      return info(r);
    },
  };

  // /api/admin/games/:gid/servers[/:sid]
  function admin(g, method, sid, body) {
    if (!sid) {
      if (method === 'GET') return [...S.all.all()].filter((r) => r.game === g.id).map((r) => {
        const live = rooms.get(rkey(g.id, r.id));
        return live && live.server ? info(live, true) : { id: r.id, ...JSON.parse(r.config), running: false };
      });
      if (method !== 'POST') throw new E('not_found');
      const id = body.id ? String(body.id) : 'srv_' + Math.random().toString(36).slice(2, 8);
      if (!ID_RE.test(id)) throw new E('bad_room_id', 'Server ids: [A-Za-z0-9_-]{1,64}');
      if (rooms.get(rkey(g.id, id)) && !rooms.get(rkey(g.id, id)).server) throw new E('room_exists', 'A normal room with this id is open');
      const cfg = normalize(g, body);
      S.put.run(g.id, id, JSON.stringify(cfg), Date.now());
      if (cfg.enabled) start(g, id, cfg);
      log(g.id, 'admin', `Server "${cfg.name}" created`);
      return { id, ...cfg };
    }
    const row = S.all.all().find((r) => r.game === g.id && r.id === sid);
    if (!row) throw new E('not_found');
    const live = rooms.get(rkey(g.id, sid));
    if (method === 'DELETE') {
      S.del.run(g.id, sid);
      if (live && live.server) stop(live, 'server_deleted');
      log(g.id, 'admin', `Server ${sid} deleted`);
      return { ok: true };
    }
    if (method === 'PATCH') {
      const cfg = normalize(g, body, JSON.parse(row.config));
      S.put.run(g.id, sid, JSON.stringify(cfg), row.created);
      if (!cfg.enabled) { if (live && live.server) stop(live, 'server_stopped'); }
      else if (live && live.server) apply(live, cfg);
      else start(g, sid, cfg);
      return live && live.server && cfg.enabled ? info(rooms.get(rkey(g.id, sid)), true) : { id: sid, ...cfg };
    }
    if (method === 'GET') return live && live.server ? info(live, true) : { id: sid, ...JSON.parse(row.config), running: false };
    throw new E('not_found');
  }

  // Check a server config (new, or a partial update of an existing one) without changing anything.
  function validate(g, b, i) {
    const row = S.all.all().find((r) => r.game === g.id && r.id === b.id);
    try { normalize(g, b, row ? JSON.parse(row.config) : {}); }
    catch (e) { throw new E(e.code || 'bad_setup', `servers[${i}]: ${e.message}`); }
  }

  return { boot, handlers, admin, validate, summary: (g) => serversOf(g).map((r) => info(r)) };
}

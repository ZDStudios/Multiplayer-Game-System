// Game services every game can opt into. Each one is generic: the game decides what keys, boards, queues
// and channels mean.
//   storage       JSON key/value per player / private / game: versions (compare-and-set), atomic update ops,
//                 batches, TTL, queries across all players, quotas
//   leaderboards  per-board config (sort, mode, daily/weekly/monthly periods, bounds, server-only), metadata,
//                 around-me and friends views
//   social        friends (request / accept / remove / block), presence, player lookup
//   inbox         persistent messages that reach players even when they're offline
//   matchmaking   rating-based queues that put matched players into a fresh room
//   chat          game-wide channels with recent history
//   config        remote config edited in the dashboard, pushed live to clients
// Clients reach these through message handlers. Server scripts and the admin API call the same core functions,
// so an authoritative game can lock client writes and do them from its script instead.
export function createServices(d) {
  const { db, E, online, rkey, send, isObj, str, safeMerge, ID_RE, KEY_RE, BAD_KEYS, CFG } = d;
  const q = (sql) => db.prepare(sql);
  const now = () => Date.now();
  const clamp = (v, lo, hi, def) => (v == null || !Number.isFinite(+v) ? def : Math.max(lo, Math.min(hi, Math.floor(+v))));
  const ids = (v, max = 100) => { if (!Array.isArray(v)) throw new E('bad_players', 'Expected an array of player ids'); return [...new Set(v.slice(0, max).map(String))].filter((x) => ID_RE.test(x)); };
  const json = (v, max, code = 'too_large') => { const s = JSON.stringify(v ?? null); if (s.length > max) throw new E(code); return s; };
  const clientOf = (gid, pid) => online.get(rkey(gid, pid)) || null;
  const nameOf = q('SELECT name FROM players WHERE game = ? AND id = ?');
  const exists = (gid, pid) => !!nameOf.get(gid, pid);
  const tx = (fn) => { db.exec('BEGIN'); try { const r = fn(); db.exec('COMMIT'); return r; } catch (e) { db.exec('ROLLBACK'); throw e; } };

  // ===================================================== storage =====================================================
  const SCOPES = new Set(['player', 'private', 'game']);
  const MAX_KEYS = +process.env.STORAGE_MAX_KEYS || 1000; // per player (player + private each); game scope gets 10x
  const K = {
    get: q('SELECT v, ver, updated, expires FROM kv WHERE game = ? AND scope = ? AND owner = ? AND k = ? AND (expires IS NULL OR expires > ?)'),
    put: q(`INSERT INTO kv (game, scope, owner, k, v, updated, ver, expires) VALUES (?, ?, ?, ?, ?, ?, 1, ?)
            ON CONFLICT (game, scope, owner, k) DO UPDATE SET v = excluded.v, updated = excluded.updated, expires = excluded.expires,
              ver = CASE WHEN kv.expires IS NOT NULL AND kv.expires <= excluded.updated THEN 1 ELSE kv.ver + 1 END
            RETURNING ver`),
    del: q('DELETE FROM kv WHERE game = ? AND scope = ? AND owner = ? AND k = ?'),
    count: q('SELECT COUNT(*) AS n FROM kv WHERE game = ? AND scope = ? AND owner = ? AND (expires IS NULL OR expires > ?)'),
    list: q(`SELECT k, v, ver, updated, expires FROM kv WHERE game = ? AND scope = ? AND owner = ? AND k >= ? AND k < ? AND k > ?
             AND (expires IS NULL OR expires > ?) ORDER BY k LIMIT ?`),
    purge: q('DELETE FROM kv WHERE expires IS NOT NULL AND expires <= ?'),
  };
  setInterval(() => K.purge.run(now()), 60000).unref();

  const checkKey = (k) => { if (typeof k !== 'string' || !KEY_RE.test(k)) throw new E('bad_key', 'Keys: 1-128 chars, no control characters'); return k; };
  const row = (r) => (r ? { value: JSON.parse(r.v), version: r.ver, updated: r.updated, expires: r.expires } : null);
  const sGet = (gid, scope, owner, key) => row(K.get.get(gid, scope, owner, checkKey(key), now()));
  function sPut(gid, scope, owner, key, value, o = {}) {
    checkKey(key);
    if (value === undefined) throw new E('bad_value');
    const s = json(value, CFG.maxMsg);
    const t = now(), cur = K.get.get(gid, scope, owner, key, t);
    if (o.ifVersion != null && (cur ? cur.ver : 0) !== +o.ifVersion) throw new E('version_conflict', `Expected version ${o.ifVersion}, found ${cur ? cur.ver : 0}`);
    if (!cur && K.count.get(gid, scope, owner, t).n >= (scope === 'game' ? MAX_KEYS * 10 : MAX_KEYS)) throw new E('quota_exceeded', 'Too many keys');
    const expires = o.ttl != null ? t + Math.max(1, +o.ttl || 1) * 1000 : o.keepTtl && cur ? cur.expires : null;
    return K.put.get(gid, scope, owner, key, s, t, expires).ver;
  }
  function sDel(gid, scope, owner, key, o = {}) {
    checkKey(key);
    if (o.ifVersion != null) { const cur = K.get.get(gid, scope, owner, key, now()); if ((cur ? cur.ver : 0) !== +o.ifVersion) throw new E('version_conflict'); }
    K.del.run(gid, scope, owner, key);
    return true;
  }

  // Atomic in-place edits: [[op, path, value, extra]]. path '' = the whole value.
  const OPS = new Set(['set', 'unset', 'incr', 'min', 'max', 'push', 'addToSet', 'pull', 'merge']);
  const same = (a, b) => a === b || JSON.stringify(a) === JSON.stringify(b);
  function applyOps(value, ops) {
    if (!Array.isArray(ops) || !ops.length || ops.length > 64) throw new E('bad_op', 'ops: 1-64 entries of [op, path, value]');
    const root = { v: value };
    for (const op of ops) {
      if (!Array.isArray(op) || !OPS.has(op[0])) throw new E('bad_op', 'op must be set | unset | incr | min | max | push | addToSet | pull | merge');
      const [kind, p, arg, extra] = op;
      const parts = p ? String(p).split('.') : [];
      if (parts.length > 32 || parts.some((s) => !s || BAD_KEYS.has(s))) throw new E('bad_path');
      let parent = root, k = 'v';
      for (const s of parts) {
        if (parent[k] === null || typeof parent[k] !== 'object') { if (kind === 'unset') { parent = null; break; } parent[k] = {}; }
        parent = parent[k]; k = s;
      }
      if (!parent) continue;
      const cur = parent[k];
      if (kind === 'set') parent[k] = arg;
      else if (kind === 'unset') { if (Array.isArray(parent) && /^\d+$/.test(k)) parent.splice(+k, 1); else delete parent[k]; }
      else if (kind === 'incr' || kind === 'min' || kind === 'max') {
        const n = +arg;
        if (!Number.isFinite(n)) throw new E('bad_value', kind + ' needs a number');
        const c = typeof cur === 'number' ? cur : kind === 'incr' ? 0 : n;
        parent[k] = kind === 'incr' ? c + n : kind === 'min' ? Math.min(c, n) : Math.max(c, n);
      } else if (kind === 'push' || kind === 'addToSet') {
        const arr = Array.isArray(cur) ? cur : (parent[k] = []);
        if (kind === 'push' || !arr.some((x) => same(x, arg))) arr.push(arg);
        if (+extra > 0 && arr.length > +extra) arr.splice(0, arr.length - +extra); // capped list: keep the newest N
      } else if (kind === 'pull') { if (Array.isArray(cur)) parent[k] = cur.filter((x) => !same(x, arg)); }
      else if (kind === 'merge') { if (!isObj(arg)) throw new E('bad_value', 'merge needs an object'); parent[k] = safeMerge(isObj(cur) ? cur : {}, arg); }
    }
    return root.v === undefined ? null : root.v;
  }
  function sUpdate(gid, scope, owner, key, ops, o = {}) {
    const cur = sGet(gid, scope, owner, key);
    if (o.ifVersion != null && (cur ? cur.version : 0) !== +o.ifVersion) throw new E('version_conflict');
    const value = applyOps(cur ? cur.value : null, ops);
    const version = sPut(gid, scope, owner, key, value, { ttl: o.ttl, keepTtl: o.ttl == null });
    return { value, version };
  }
  // Returns the new number (at `path` inside the value, or the value itself).
  function sIncr(gid, scope, owner, key, by, path) {
    const v = sUpdate(gid, scope, owner, key, [['incr', path || '', by]]).value;
    return path ? String(path).split('.').reduce((x, k) => (x == null ? x : x[k]), v) : v;
  }
  function sList(gid, scope, owner, o = {}) {
    const prefix = str(o.prefix, 128, '');
    return K.list.all(gid, scope, owner, prefix, prefix + '￿', str(o.after, 128, ''), now(), clamp(o.limit, 1, 500, 100))
      .map((r) => ({ key: r.k, ...row(r) }));
  }

  // Queries over JSON values with SQLite's json_extract. Paths are restricted to [A-Za-z0-9_.] so they can be inlined.
  const PATH_RE = /^[A-Za-z0-9_]{1,64}(\.[A-Za-z0-9_]{1,64}){0,7}$/;
  const CMP = { '=': '=', '==': '=', '!=': '!=', '>': '>', '>=': '>=', '<': '<', '<=': '<=' };
  const sqlVal = (v) => (typeof v === 'boolean' ? (v ? 1 : 0) : typeof v === 'number' || typeof v === 'string' || v === null ? v : JSON.stringify(v));
  const stmtCache = new Map();
  function sQuery(gid, o = {}) {
    const scope = o.scope || 'player';
    if (scope !== 'player' && scope !== 'game') throw new E('bad_scope', 'query works on player (every player\'s public data) or game scope');
    const jp = (p) => { if (!PATH_RE.test(String(p))) throw new E('bad_path', 'Query paths: a.b.c with [A-Za-z0-9_]'); return `json_extract(kv.v, '$.${p}')`; };
    const args = [gid, scope, now()];
    let sql = `SELECT kv.owner, kv.k, kv.v, kv.ver, kv.updated, p.name FROM kv LEFT JOIN players p ON p.game = kv.game AND p.id = kv.owner
      WHERE kv.game = ? AND kv.scope = ? AND (kv.expires IS NULL OR kv.expires > ?)`;
    if (o.key != null) { sql += ' AND kv.k = ?'; args.push(checkKey(o.key)); }
    else if (o.prefix) { sql += ' AND kv.k >= ? AND kv.k < ?'; args.push(String(o.prefix), String(o.prefix) + '￿'); }
    for (const w of (Array.isArray(o.where) ? o.where : []).slice(0, 8)) {
      if (!Array.isArray(w)) throw new E('bad_query');
      const [p, op, val] = w;
      if (op === 'in') {
        if (!Array.isArray(val) || !val.length) throw new E('bad_query', "'in' needs a non-empty array");
        const vs = val.slice(0, 50);
        sql += ` AND ${jp(p)} IN (${vs.map(() => '?').join(',')})`; args.push(...vs.map(sqlVal));
      } else if (op === 'exists') sql += ` AND ${jp(p)} IS ${val === false ? '' : 'NOT '}NULL`;
      else if (CMP[op]) { sql += ` AND ${jp(p)} ${CMP[op]} ?`; args.push(sqlVal(val)); }
      else throw new E('bad_query', 'where: [[path, op, value]], op = = != > >= < <= in exists');
    }
    if (o.orderBy) sql += ` ORDER BY ${o.orderBy === 'updated' ? 'kv.updated' : jp(o.orderBy)} ${o.desc ? 'DESC' : 'ASC'}`;
    sql += ' LIMIT ? OFFSET ?';
    args.push(clamp(o.limit, 1, 100, 20), clamp(o.offset, 0, 100000, 0));
    let st = stmtCache.get(sql);
    if (!st) { if (stmtCache.size > 200) stmtCache.clear(); stmtCache.set(sql, (st = db.prepare(sql))); }
    return st.all(...args).map((r) => ({ player: scope === 'player' ? r.owner : undefined, name: scope === 'player' ? r.name : undefined,
      key: r.k, value: JSON.parse(r.v), version: r.ver, updated: r.updated }));
  }

  // Who a client may touch. Server scripts and the admin bypass this.
  function target(c, m, write) {
    const scope = m.scope || 'player';
    if (!SCOPES.has(scope)) throw new E('bad_scope', 'scope must be player | private | game');
    const st = c.game.settings;
    if (scope === 'game') {
      if (write && st.gameDataWrite === 'none') throw new E('forbidden', 'Game data is read-only for clients');
      return ['game', ''];
    }
    if (write && st.playerDataWrite === 'none') throw new E('forbidden', 'Player data is written by the server script for this game');
    if (scope === 'player' && !write && m.player != null) {
      if (!ID_RE.test(String(m.player))) throw new E('bad_player');
      return ['player', String(m.player)];
    }
    return [scope, c.pid];
  }
  const storage = { get: sGet, put: sPut, del: sDel, update: sUpdate, incr: sIncr, list: sList, query: sQuery, applyOps };

  // ==================================================== leaderboards ====================================================
  const BOARD_RE = /^[A-Za-z0-9_\-.:]{1,64}$/;
  const PERIODS = ['all', 'day', 'week', 'month'];
  const B = {
    get: q('SELECT config FROM boards WHERE game = ? AND id = ?'),
    put: q('INSERT INTO boards (game, id, config, created) VALUES (?, ?, ?, ?) ON CONFLICT (game, id) DO UPDATE SET config = excluded.config'),
    list: q('SELECT id, config, created FROM boards WHERE game = ? ORDER BY id'),
    del: q('DELETE FROM boards WHERE game = ? AND id = ?'),
    score: q('SELECT score, meta, updated FROM scores WHERE game = ? AND board = ? AND player = ?'),
    set: q(`INSERT INTO scores (game, board, player, name, score, updated, meta) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (game, board, player) DO UPDATE SET name = excluded.name, score = excluded.score, updated = excluded.updated, meta = excluded.meta`),
    rename: q('UPDATE scores SET name = ? WHERE game = ? AND board = ? AND player = ?'),
    topDesc: q('SELECT player AS id, name, score, meta, updated FROM scores WHERE game = ? AND board = ? ORDER BY score DESC, updated ASC LIMIT ? OFFSET ?'),
    topAsc: q('SELECT player AS id, name, score, meta, updated FROM scores WHERE game = ? AND board = ? ORDER BY score ASC, updated ASC LIMIT ? OFFSET ?'),
    betterDesc: q('SELECT COUNT(*) AS n FROM scores WHERE game = ? AND board = ? AND (score > ? OR (score = ? AND updated < ?))'),
    betterAsc: q('SELECT COUNT(*) AS n FROM scores WHERE game = ? AND board = ? AND (score < ? OR (score = ? AND updated < ?))'),
    count: q('SELECT COUNT(*) AS n FROM scores WHERE game = ? AND board = ?'),
    delPlayer: q("DELETE FROM scores WHERE game = ? AND (board = ? OR board LIKE ? || '#%') AND player = ?"),
    delBoard: q("DELETE FROM scores WHERE game = ? AND (board = ? OR board LIKE ? || '#%')"),
    summary: q("SELECT board, COUNT(*) AS entries, MAX(updated) AS updated FROM scores WHERE game = ? AND board NOT LIKE '%#%' GROUP BY board"),
  };
  const numOr = (v) => (v == null || v === '' || !Number.isFinite(+v) ? null : +v);
  function normBoard(o = {}) {
    const mode = o.mode === 'set' ? 'last' : ['max', 'min', 'sum', 'last'].includes(o.mode) ? o.mode : 'max'; // 'set' = old name of 'last'
    return {
      mode,                                                    // how a new score combines with the stored one
      sort: o.sort === 'asc' || o.sort === 'desc' ? o.sort : mode === 'min' ? 'asc' : 'desc',
      periods: ['all', ...PERIODS.slice(1).filter((p) => Array.isArray(o.periods) && o.periods.includes(p))],
      clientWrite: o.clientWrite !== false,                   // false = only the server script / admin may submit
      min: numOr(o.min), max: numOr(o.max),                   // reject scores outside these bounds
    };
  }
  function boardCfg(gid, id, create, hint) {
    if (typeof id !== 'string' || !BOARD_RE.test(id)) throw new E('bad_board', 'Board ids: 1-64 chars of A-Z a-z 0-9 _ - . :');
    const r = B.get.get(gid, id);
    if (r) return JSON.parse(r.config);
    // Boards from before configs existed: read with the defaults, but let the first submit's mode decide the config.
    if (!create) return B.count.get(gid, id).n ? normBoard({}) : null;
    const cfg = normBoard(hint);
    B.put.run(gid, id, JSON.stringify(cfg), now());
    return cfg;
  }
  // Periods are UTC: days from 00:00, weeks from Monday 00:00, months from the 1st.
  function periodStart(p, t, back = 0) {
    const d = new Date(t); d.setUTCHours(0, 0, 0, 0);
    if (p === 'day') d.setUTCDate(d.getUTCDate() - back);
    else if (p === 'week') d.setUTCDate(d.getUTCDate() - ((d.getUTCDay() + 6) % 7) - 7 * back);
    else if (p === 'month') { d.setUTCDate(1); d.setUTCMonth(d.getUTCMonth() - back); }
    return d;
  }
  const rowBoard = (id, p, back = 0) => (p === 'all' ? id : `${id}#${p[0]}:${periodStart(p, now(), back).toISOString().slice(0, p === 'month' ? 7 : 10)}`);
  const resetsAt = (p) => (p === 'all' ? null : periodStart(p, now(), -1).getTime());
  const entry = (r, rank) => ({ rank, id: r.id, name: r.name, score: r.score, meta: r.meta ? JSON.parse(r.meta) : null, updated: r.updated });
  const rankOf = (gid, b, cfg, score, updated) => (cfg.sort === 'asc' ? B.betterAsc : B.betterDesc).get(gid, b, score, score, updated).n + 1;
  function needBoard(gid, id) { const cfg = boardCfg(gid, id, false); if (!cfg) throw new E('board_not_found'); return cfg; }
  // Older clients pass order: 'asc' | 'desc' per call; it still overrides the board's sort for that read.
  const view = (cfg, o) => (o.order === 'asc' || o.order === 'desc' ? { ...cfg, sort: o.order } : cfg);
  function pickPeriod(cfg, p) { p = p || 'all'; if (!cfg.periods.includes(p)) throw new E('bad_period', `This board keeps: ${cfg.periods.join(', ')}`); return p; }

  function lbSubmit(gid, board, pid, name, score, meta, o = {}) {
    score = +score;
    if (!Number.isFinite(score)) throw new E('bad_score');
    const cfg = boardCfg(gid, board, !o.client || !o.locked, o.hint);
    if (!cfg) throw new E('board_not_found', 'Leaderboards for this game are created in the dashboard');
    if (o.client && !cfg.clientWrite) throw new E('forbidden', 'This leaderboard only accepts scores from the server');
    if ((cfg.min != null && score < cfg.min) || (cfg.max != null && score > cfg.max)) throw new E('bad_score', 'Score out of range');
    const metaS = meta === undefined ? undefined : json(meta, 1024);
    const t = now(), periods = {};
    for (const p of cfg.periods) {
      const b = rowBoard(board, p), cur = B.score.get(gid, b, pid);
      let next = score;
      if (cur) next = cfg.mode === 'max' ? Math.max(cur.score, score) : cfg.mode === 'min' ? Math.min(cur.score, score) : cfg.mode === 'sum' ? cur.score + score : score;
      const improved = !cur || next !== cur.score || cfg.mode === 'last';
      const updated = improved ? t : cur.updated;
      if (improved) B.set.run(gid, b, pid, name, next, updated, metaS !== undefined ? metaS : cur ? cur.meta : null);
      else B.rename.run(name, gid, b, pid);
      periods[p] = { score: next, rank: rankOf(gid, b, cfg, next, updated), improved };
    }
    return { board, ...periods.all, periods };
  }
  function lbTop(gid, board, o = {}) {
    const cfg = view(needBoard(gid, board), o), p = pickPeriod(cfg, o.period), b = rowBoard(board, p, o.previous ? 1 : 0);
    const limit = clamp(o.limit, 1, 100, 10), offset = clamp(o.offset, 0, 1e7, 0);
    const entries = (cfg.sort === 'asc' ? B.topAsc : B.topDesc).all(gid, b, limit, offset).map((r, i) => entry(r, offset + i + 1));
    return { board, period: p, previous: !!o.previous, entries, total: B.count.get(gid, b).n, resetsAt: o.previous ? null : resetsAt(p) };
  }
  function lbEntry(gid, board, cfg, b, pid) {
    const r = B.score.get(gid, b, pid);
    return r ? entry({ id: pid, name: (nameOf.get(gid, pid) || {}).name, ...r }, rankOf(gid, b, cfg, r.score, r.updated)) : null;
  }
  function lbAround(gid, board, pid, o = {}) {
    const cfg = view(needBoard(gid, board), o), p = pickPeriod(cfg, o.period), b = rowBoard(board, p, o.previous ? 1 : 0);
    const me = lbEntry(gid, board, cfg, b, pid);
    if (!me) return { board, period: p, entries: [], me: null };
    const range = clamp(o.range, 1, 50, 5), offset = Math.max(0, me.rank - 1 - range);
    const entries = (cfg.sort === 'asc' ? B.topAsc : B.topDesc).all(gid, b, range * 2 + 1, offset).map((r, i) => entry(r, offset + i + 1));
    return { board, period: p, entries, me };
  }
  function lbPlayers(gid, board, list, o = {}) {
    const cfg = view(needBoard(gid, board), o), p = pickPeriod(cfg, o.period), b = rowBoard(board, p, o.previous ? 1 : 0);
    const entries = list.map((pid) => lbEntry(gid, board, cfg, b, pid)).filter(Boolean).sort((x, y) => x.rank - y.rank);
    return { board, period: p, entries };
  }
  const lbBoards = (gid) => B.list.all(gid).map((r) => ({ id: r.id, ...JSON.parse(r.config) }));
  const leaderboard = { submit: lbSubmit, top: lbTop, around: lbAround, players: lbPlayers, boards: lbBoards, config: boardCfg, normalize: normBoard };

  // ====================================================== social ======================================================
  const F = {
    get: q('SELECT state FROM friends WHERE game = ? AND player = ? AND other = ?'),
    set: q('INSERT INTO friends (game, player, other, state, created) VALUES (?, ?, ?, ?, ?) ON CONFLICT (game, player, other) DO UPDATE SET state = excluded.state'),
    del: q('DELETE FROM friends WHERE game = ? AND player = ? AND other = ?'),
    list: q(`SELECT f.other AS id, f.state, f.created, p.name, p.last_seen, a.username FROM friends f
             LEFT JOIN players p ON p.game = f.game AND p.id = f.other LEFT JOIN accounts a ON a.game = f.game AND a.player = f.other
             WHERE f.game = ? AND f.player = ? ORDER BY f.created`),
    ofState: q('SELECT other FROM friends WHERE game = ? AND player = ? AND state = ?'),
    count: q('SELECT COUNT(*) AS n FROM friends WHERE game = ? AND player = ?'),
    profile: q(`SELECT p.id, p.name, p.last_seen, a.username FROM players p LEFT JOIN accounts a ON a.game = p.game AND a.player = p.id
                WHERE p.game = ? AND p.id = ?`),
    byUser: q('SELECT player FROM accounts WHERE game = ? AND username = ?'),
    search: q(`SELECT id FROM players WHERE game = ? AND name LIKE ? ESCAPE '\\' ORDER BY last_seen DESC LIMIT ?`),
  };
  const MAX_FRIENDS = 500;
  const state = (gid, a, b) => (F.get.get(gid, a, b) || {}).state || null;
  const friendIds = (gid, pid) => F.ofState.all(gid, pid, 'friend').map((r) => r.other);
  const presenceOf = (gid, pid) => { const c = clientOf(gid, pid); return c ? { online: true, status: c.presence ? c.presence.status : null, data: c.presence ? c.presence.data : null } : { online: false, status: null, data: null }; };
  function notifyFriend(gid, to, from, st) {
    const c = clientOf(gid, to);
    if (c) send(c, { t: 'friend', id: from, state: st, name: (nameOf.get(gid, from) || {}).name || null, ...(st === 'friend' ? presenceOf(gid, from) : {}) });
  }
  function setBlocked(gid, pid) { const c = clientOf(gid, pid); if (c) c.blocked = new Set(F.ofState.all(gid, pid, 'blocked').map((r) => r.other)); }
  function friendTarget(c, m) {
    let id = m.player != null ? String(m.player) : null;
    if (id == null && m.username != null) { const a = F.byUser.get(c.game.id, String(m.username).toLowerCase()); if (!a) throw new E('player_not_found'); id = a.player; }
    if (!id || !ID_RE.test(id) || id === c.pid) throw new E('bad_player');
    if (!exists(c.game.id, id)) throw new E('player_not_found');
    return id;
  }
  function presenceBroadcast(c, isOnline) {
    const msg = { t: 'presence', id: c.pid, online: isOnline, status: isOnline && c.presence ? c.presence.status : null, data: isOnline && c.presence ? c.presence.data : null };
    for (const f of friendIds(c.game.id, c.pid)) { const fc = clientOf(c.game.id, f); if (fc) send(fc, msg); }
  }
  function profiles(c, list) {
    const gid = c.game.id, mine = new Set(friendIds(gid, c.pid));
    return list.map((id) => {
      const p = F.profile.get(gid, id);
      if (!p) return null;
      const pr = presenceOf(gid, id);
      return { id, name: p.name, username: p.username || null, online: pr.online, lastSeen: p.last_seen, friend: mine.has(id),
        ...(mine.has(id) ? { status: pr.status, data: pr.data } : {}) };
    }).filter(Boolean);
  }

  // ======================================================= inbox =======================================================
  const INBOX_MAX = 200;
  const I = {
    add: q('INSERT INTO inbox (game, player, sender, sender_name, type, data, created) VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING id'),
    list: q(`SELECT id, sender, sender_name, type, data, created, read FROM inbox WHERE game = ? AND player = ? AND id < ?
             AND (? = 0 OR read = 0) ORDER BY id DESC LIMIT ?`),
    counts: q('SELECT COUNT(*) AS total, COALESCE(SUM(read = 0), 0) AS unread FROM inbox WHERE game = ? AND player = ?'),
    trim: q('DELETE FROM inbox WHERE game = ? AND player = ? AND id <= (SELECT id FROM inbox WHERE game = ? AND player = ? ORDER BY id DESC LIMIT 1 OFFSET ?)'),
    read: q('UPDATE inbox SET read = 1 WHERE game = ? AND player = ? AND id = ?'),
    readAll: q('UPDATE inbox SET read = 1 WHERE game = ? AND player = ?'),
    del: q('DELETE FROM inbox WHERE game = ? AND player = ? AND id = ?'),
    delAll: q('DELETE FROM inbox WHERE game = ? AND player = ?'),
  };
  const item = (r) => ({ id: r.id, from: r.sender, fromName: r.sender_name, type: r.type, data: JSON.parse(r.data), created: r.created, read: !!r.read });
  function inboxSend(gid, to, from, fromName, type, data) {
    to = String(to);
    if (!ID_RE.test(to) || !exists(gid, to)) throw new E('player_not_found');
    const s = json(data, 8192);
    type = str(type, 64, 'message') || 'message';
    if (from !== 'server' && state(gid, to, from) === 'blocked') return { id: null }; // silently dropped
    const t = now(), id = I.add.get(gid, to, from, fromName, type, s, t).id;
    I.trim.run(gid, to, gid, to, INBOX_MAX);
    const c = clientOf(gid, to);
    if (c) send(c, { t: 'inbox', item: { id, from, fromName, type, data: JSON.parse(s), created: t, read: false } });
    return { id };
  }
  const idList = (v) => (v === 'all' ? 'all' : (Array.isArray(v) ? v : [v]).slice(0, 500).map((x) => Math.floor(+x)).filter(Number.isFinite));
  const inbox = { send: inboxSend };

  // ==================================================== matchmaking ====================================================
  // Tickets wait in a queue (name + match size). Every 250 ms each queue is scanned in rating order and the first
  // run of `size` tickets whose rating spread fits every member's tolerance becomes a match. Tolerance starts at
  // `spread` and grows by `widen` per second of waiting; after `maxWaitMs` a match may start with `minSize` players.
  const queues = new Map(); // gid \0 queue \0 size -> [ticket]
  let ticketSeq = 0;
  function mmLeave(c, reason) {
    const t = c.ticket;
    if (!t) return false;
    const list = queues.get(t.qkey);
    if (list) { const i = list.indexOf(t); if (i >= 0) list.splice(i, 1); if (!list.length) queues.delete(t.qkey); }
    c.ticket = null;
    if (reason) send(c, { t: 'mm.cancelled', queue: t.queue, reason });
    return true;
  }
  function mmJoin(c, m) {
    const queue = str(m.queue, 64, '');
    if (!BOARD_RE.test(queue)) throw new E('bad_queue', 'Queue names: 1-64 chars of A-Z a-z 0-9 _ - . :');
    const size = clamp(m.size, 2, 64, 2);
    const rating = Number.isFinite(+m.rating) ? +m.rating : 1000;
    mmLeave(c);
    const t = { id: ++ticketSeq, c, queue, size, minSize: clamp(m.minSize, 2, size, size), rating, joined: now(),
      spread: Number.isFinite(+m.spread) ? Math.max(0, +m.spread) : 100, widen: Number.isFinite(+m.widen) ? Math.max(0, +m.widen) : 25,
      maxWait: clamp(m.maxWaitMs, 0, 600000, 30000), options: isObj(m.options) ? m.options : {}, qkey: c.game.id + '\0' + queue + '\0' + size };
    if (!queues.has(t.qkey)) queues.set(t.qkey, []);
    queues.get(t.qkey).push(t);
    c.ticket = t;
    return { ticket: t.id, queue, size, waiting: queues.get(t.qkey).length };
  }
  function mmTick() {
    const t = now();
    for (const [qkey, list] of queues) {
      list.sort((a, b) => a.rating - b.rating);
      const tol = (x) => x.spread + (x.widen * (t - x.joined)) / 1000;
      for (let i = 0; i < list.length; i++) {
        const group = [list[i]];
        let limit = tol(list[i]);
        for (let j = i + 1; j < list.length && group.length < list[i].size; j++) {
          const lim = Math.min(limit, tol(list[j]));
          if (list[j].rating - list[i].rating > lim) break;
          group.push(list[j]); limit = lim;
        }
        const full = group.length === list[i].size;
        const early = group.length >= list[i].minSize && group.every((x) => t - x.joined >= x.maxWait);
        if (!full && !early) continue;
        for (const x of group) list.splice(list.indexOf(x), 1);
        i--;
        match(group);
      }
      if (!list.length) queues.delete(qkey);
    }
  }
  setInterval(mmTick, 250).unref();
  function match(group) {
    const first = group[0], gid = first.c.game.id;
    const o = { ...first.options, private: first.options.private !== false, maxPlayers: Math.max(first.size, +first.options.maxPlayers || 0),
      meta: { ...(isObj(first.options.meta) ? first.options.meta : {}), match: { queue: first.queue, players: group.map((x) => x.c.pid) } } };
    const roomId = d.newCode(gid);
    const joined = [];
    for (const x of group) {
      x.c.ticket = null;
      try { d.joinRoom(x.c, roomId, o); joined.push(x.c); } catch (e) { send(x.c, { t: 'mm.cancelled', queue: x.queue, reason: e.code || 'join_failed' }); }
    }
    const room = joined.length && joined[0].room;
    for (const c of joined) if (c.room === room) send(c, { t: 'matched', queue: first.queue, room: d.snapshot(room) });
  }

  // ======================================================== chat ========================================================
  const channels = new Map(); // gid \0 name -> { members: Set<client>, history: [] }
  const HISTORY = 50;
  function chatLeave(c, name) {
    const key = c.game.id + '\0' + name, ch = channels.get(key);
    if (ch) { ch.members.delete(c); if (!ch.members.size && !ch.history.length) channels.delete(key); }
    if (c.channels) c.channels.delete(name);
  }
  const cleanText = (v) => str(v, 500, '').replace(/[\u0000-\u0008\u000b-\u001f\u007f]/g, '').trim();

  // ==================================================== lifecycle ====================================================
  function onOnline(c) {
    setBlocked(c.game.id, c.pid);
    presenceBroadcast(c, true);
  }
  function onOffline(c) {
    mmLeave(c);
    for (const name of [...(c.channels || [])]) chatLeave(c, name);
    if (c.game && c.pid && !clientOf(c.game.id, c.pid)) presenceBroadcast(c, false);
  }
  const welcomeExtras = (c) => ({ config: c.game.settings.config || {}, inbox: { unread: I.counts.get(c.game.id, c.pid).unread } });
  function pushConfig(game, clients) { for (const c of clients) if (c.game === game) send(c, { t: 'config', config: game.settings.config || {} }); }

  // ================================================= client handlers =================================================
  const handlers = {
    'kv.get'(c, m) { const [s, o] = target(c, m, false); const r = sGet(c.game.id, s, o, m.key); return m.meta ? r : r ? r.value : null; },
    'kv.getMany'(c, m) {
      const [s, o] = target(c, m, false), out = {};
      if (!Array.isArray(m.keys) || m.keys.length > 100) throw new E('bad_key', 'keys: up to 100');
      for (const k of m.keys) { const r = sGet(c.game.id, s, o, k); out[k] = m.meta ? r : r ? r.value : null; }
      return out;
    },
    'kv.set'(c, m) { const [s, o] = target(c, m, true); return { version: sPut(c.game.id, s, o, m.key, m.value, { ifVersion: m.ifVersion, ttl: m.ttl }) }; },
    'kv.setMany'(c, m) {
      const [s, o] = target(c, m, true);
      if (!isObj(m.entries) || Object.keys(m.entries).length > 100) throw new E('bad_value', 'entries: an object of up to 100 keys');
      return tx(() => { const out = {}; for (const [k, v] of Object.entries(m.entries)) out[k] = sPut(c.game.id, s, o, k, v, { ttl: m.ttl }); return out; });
    },
    'kv.del'(c, m) { const [s, o] = target(c, m, true); return sDel(c.game.id, s, o, m.key, { ifVersion: m.ifVersion }); },
    'kv.incr'(c, m) {
      const [s, o] = target(c, m, true);
      return sIncr(c.game.id, s, o, m.key, m.by == null ? 1 : m.by, m.path);
    },
    'kv.update'(c, m) { const [s, o] = target(c, m, true); return sUpdate(c.game.id, s, o, m.key, m.ops, { ifVersion: m.ifVersion, ttl: m.ttl }); },
    'kv.list'(c, m) { const [s, o] = target(c, m, false); return sList(c.game.id, s, o, m); },
    'kv.query'(c, m) { return sQuery(c.game.id, m); },

    'lb.submit'(c, m) {
      return lbSubmit(c.game.id, m.board, c.pid, c.name, m.score, m.meta,
        { client: true, locked: c.game.settings.lockLeaderboards === true, hint: isObj(m.config) ? m.config : { mode: m.mode } });
    },
    'lb.top'(c, m) {
      const r = lbTop(c.game.id, m.board, m);
      const cfg = view(needBoard(c.game.id, m.board), m);
      return { ...r, me: lbEntry(c.game.id, m.board, cfg, rowBoard(m.board, r.period, m.previous ? 1 : 0), c.pid) };
    },
    'lb.around'(c, m) { return lbAround(c.game.id, m.board, m.player != null ? String(m.player) : c.pid, m); },
    'lb.players'(c, m) { return lbPlayers(c.game.id, m.board, m.friends ? [c.pid, ...friendIds(c.game.id, c.pid)] : ids(m.players), m); },
    'lb.boards'(c) { return lbBoards(c.game.id).map((b) => ({ ...b, resetsAt: Object.fromEntries(b.periods.map((p) => [p, resetsAt(p)])) })); },

    'friends.list'(c) {
      const gid = c.game.id;
      return F.list.all(gid, c.pid).map((r) => ({ id: r.id, name: r.name, username: r.username || null, state: r.state, since: r.created,
        lastSeen: r.last_seen, ...(r.state === 'friend' ? presenceOf(gid, r.id) : {}) }));
    },
    'friends.request'(c, m) {
      const gid = c.game.id, other = friendTarget(c, m), mine = state(gid, c.pid, other), theirs = state(gid, other, c.pid), t = now();
      if (mine === 'blocked') throw new E('blocked', 'Unblock this player first');
      if (mine === 'friend') return { state: 'friend' };
      if (mine === 'incoming') { // they asked first: accept
        tx(() => { F.set.run(gid, c.pid, other, 'friend', t); F.set.run(gid, other, c.pid, 'friend', t); });
        notifyFriend(gid, other, c.pid, 'friend');
        return { state: 'friend', ...presenceOf(gid, other) };
      }
      if (F.count.get(gid, c.pid).n >= MAX_FRIENDS) throw new E('quota_exceeded', 'Friend list is full');
      F.set.run(gid, c.pid, other, 'outgoing', t);
      if (theirs !== 'blocked') { F.set.run(gid, other, c.pid, 'incoming', t); notifyFriend(gid, other, c.pid, 'incoming'); }
      return { state: 'outgoing' };
    },
    'friends.accept'(c, m) {
      const gid = c.game.id, other = friendTarget(c, m);
      if (state(gid, c.pid, other) !== 'incoming') throw new E('no_request', 'No friend request from this player');
      const t = now();
      tx(() => { F.set.run(gid, c.pid, other, 'friend', t); F.set.run(gid, other, c.pid, 'friend', t); });
      notifyFriend(gid, other, c.pid, 'friend');
      return { state: 'friend', ...presenceOf(gid, other) };
    },
    'friends.remove'(c, m) { // unfriend, decline, cancel and unblock
      const gid = c.game.id, other = friendTarget(c, m), theirs = state(gid, other, c.pid);
      F.del.run(gid, c.pid, other);
      if (theirs && theirs !== 'blocked') { F.del.run(gid, other, c.pid); notifyFriend(gid, other, c.pid, null); }
      setBlocked(gid, c.pid);
      return { state: null };
    },
    'friends.block'(c, m) {
      const gid = c.game.id, other = friendTarget(c, m), theirs = state(gid, other, c.pid);
      F.set.run(gid, c.pid, other, 'blocked', now());
      if (theirs && theirs !== 'blocked') { F.del.run(gid, other, c.pid); notifyFriend(gid, other, c.pid, null); }
      setBlocked(gid, c.pid);
      return { state: 'blocked' };
    },
    'presence.set'(c, m) {
      c.presence = { status: str(m.status, 64, '') || null, data: m.data === undefined ? null : JSON.parse(json(m.data, 512)) };
      presenceBroadcast(c, true);
      return true;
    },
    'players.get'(c, m) { return profiles(c, ids(m.players)); },
    'players.find'(c, m) {
      if (m.username != null) { const a = F.byUser.get(c.game.id, String(m.username).toLowerCase()); return a ? profiles(c, [a.player]) : []; }
      const name = str(m.name, 32, '').replace(/[\\%_]/g, (x) => '\\' + x);
      if (!name) throw new E('bad_name');
      return profiles(c, F.search.all(c.game.id, name + '%', clamp(m.limit, 1, 50, 20)).map((r) => r.id));
    },

    'inbox.send'(c, m) {
      if (c.game.settings.inboxClientSend === false) throw new E('forbidden', 'Only the server can send inbox messages in this game');
      const t = now();
      c.inboxSent = (c.inboxSent || []).filter((x) => t - x < 60000);
      if (c.inboxSent.length >= 30) throw new E('rate_limited', 'At most 30 inbox messages per minute');
      c.inboxSent.push(t);
      return inboxSend(c.game.id, m.to, c.pid, c.name, m.type, m.data);
    },
    'inbox.list'(c, m) {
      const gid = c.game.id;
      const items = I.list.all(gid, c.pid, m.before != null ? Math.floor(+m.before) || 0 : Number.MAX_SAFE_INTEGER, m.unread ? 1 : 0, clamp(m.limit, 1, 100, 50)).map(item);
      return { items, ...I.counts.get(gid, c.pid) };
    },
    'inbox.read'(c, m) { const l = idList(m.ids); if (l === 'all') I.readAll.run(c.game.id, c.pid); else tx(() => l.forEach((id) => I.read.run(c.game.id, c.pid, id))); return I.counts.get(c.game.id, c.pid); },
    'inbox.delete'(c, m) { const l = idList(m.ids); if (l === 'all') I.delAll.run(c.game.id, c.pid); else tx(() => l.forEach((id) => I.del.run(c.game.id, c.pid, id))); return I.counts.get(c.game.id, c.pid); },

    'mm.join': mmJoin,
    'mm.leave'(c) { return mmLeave(c); },
    'mm.status'(c) { const t = c.ticket; return t ? { queue: t.queue, size: t.size, waitedMs: now() - t.joined, waiting: (queues.get(t.qkey) || []).length } : null; },

    'chat.join'(c, m) {
      const name = str(m.channel, 64, '');
      if (!BOARD_RE.test(name)) throw new E('bad_channel', 'Channel names: 1-64 chars of A-Z a-z 0-9 _ - . :');
      c.channels = c.channels || new Set();
      if (!c.channels.has(name) && c.channels.size >= 10) throw new E('too_many_channels', 'At most 10 channels at once');
      const key = c.game.id + '\0' + name;
      if (!channels.has(key)) channels.set(key, { members: new Set(), history: [] });
      const ch = channels.get(key);
      ch.members.add(c); c.channels.add(name);
      return { channel: name, members: ch.members.size, history: ch.history.filter((x) => !(c.blocked && c.blocked.has(x.from))) };
    },
    'chat.leave'(c, m) { chatLeave(c, str(m.channel, 64, '')); return true; },
    'chat.send'(c, m) {
      const name = str(m.channel, 64, ''), ch = channels.get(c.game.id + '\0' + name);
      if (!ch || !ch.members.has(c)) throw new E('not_in_channel');
      const text = cleanText(m.text);
      if (!text && m.data === undefined) throw new E('bad_message');
      const msg = { t: 'chat', channel: name, from: c.pid, name: c.name, text, data: m.data === undefined ? null : JSON.parse(json(m.data, 1024)), ts: now() };
      ch.history.push(msg); if (ch.history.length > HISTORY) ch.history.shift();
      const s = JSON.stringify(msg);
      for (const mc of ch.members) if (!(mc.blocked && mc.blocked.has(c.pid))) d.out(mc, s);
      return true;
    },

    'config.get'(c) { return c.game.settings.config || {}; },
  };

  // ==================================================== admin + stats ====================================================
  const summary = (gid) => {
    const counts = new Map(B.summary.all(gid).map((r) => [r.board, r]));
    const out = lbBoards(gid).map((b) => ({ board: b.id, config: b, entries: (counts.get(b.id) || {}).entries || 0, updated: (counts.get(b.id) || {}).updated || null }));
    for (const [id, r] of counts) if (!out.some((b) => b.board === id)) out.push({ board: id, config: null, entries: r.entries, updated: r.updated });
    return out;
  };
  function stats(gid) {
    let tickets = 0, chans = 0;
    for (const [k, l] of queues) if (!gid || k.startsWith(gid + '\0')) tickets += l.length;
    for (const k of channels.keys()) if (!gid || k.startsWith(gid + '\0')) chans++;
    return { matchmaking: tickets, channels: chans };
  }
  // /api/admin/games/:gid/leaderboards[/:board[/config|/players/:pid]]
  function adminLeaderboards(g, method, sid, act, extra, body, url) {
    if (!sid) {
      if (method === 'GET') return summary(g.id);
      throw new E('not_found');
    }
    if (act === 'config') {
      if (method === 'PUT') { const cfg = normBoard(body); B.put.run(g.id, sid, JSON.stringify(cfg), now()); return cfg; }
      return boardCfg(g.id, sid, false);
    }
    if (act === 'players' && extra && method === 'DELETE') { B.delPlayer.run(g.id, sid, sid, extra); return { ok: true }; }
    if (act === 'submit' && method === 'POST') return lbSubmit(g.id, sid, String(body.player), (nameOf.get(g.id, String(body.player)) || {}).name || null, body.score, body.meta);
    if (method === 'DELETE') { B.delBoard.run(g.id, sid, sid); if (url.searchParams.get('config') === 'delete') B.del.run(g.id, sid); return { ok: true }; }
    const cfg = boardCfg(g.id, sid, false);
    if (!cfg) return (B.topDesc.all(g.id, sid, 100, 0)).map((r, i) => entry(r, i + 1));
    return lbTop(g.id, sid, { limit: 100, period: url.searchParams.get('period') || 'all', previous: url.searchParams.get('previous') === '1' }).entries;
  }

  return { handlers, storage, leaderboard, inbox, onOnline, onOffline, welcomeExtras, pushConfig, adminLeaderboards, summary, stats, mmLeave };
}

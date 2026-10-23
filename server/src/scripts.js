// Server-side game logic ("server scripts"). One optional JS module per game, uploaded by the admin,
// run in a vm context on the server. It runs authoritative game rules next to the players' connections,
// so an action costs one round trip (client -> server -> everyone) instead of two via a host client.
//
// Script shape (CommonJS):
//   module.exports = {
//     authoritative: true,      // clients may not write room state directly (only the script can)
//     tickRate: 50,             // onTick calls per second, 1-60 (default 50 when onTick is defined)
//     onCreate(room) {}, onJoin(room, player) {}, onLeave(room, player, reason) {},
//     onMessage(room, player, type, data) {},   // client: mp.send(type, data, 'server')
//     onTick(room, dtSeconds) {},
//   };
import vm from 'node:vm';

const HOOK_TIMEOUT_MS = 50;   // a single hook call may not block the event loop longer than this
const LOAD_TIMEOUT_MS = 1000;
const MAX_TICK_RATE = 60;
const DEFAULT_TICK_RATE = Math.min(MAX_TICK_RATE, Math.max(1, +process.env.DEFAULT_TICK_RATE || 50));

export function createScripting(h) {
  const { log, broadcast, send, setPath, removeMember, pubPlayer, E } = h;
  const loaded = new Map(); // gameId -> { ctx, mod, errors: [], code }
  const dispatch = new vm.Script('__r = __f.apply(undefined, __a)');

  function load(game) {
    unload(game.id);
    const code = game.settings.script;
    if (!code) return null;
    const errors = [];
    const ctx = vm.createContext({
      console: {
        log: (...a) => log(game.id, 'script', a.map(fmt).join(' ')),
        error: (...a) => log(game.id, 'script', 'ERROR ' + a.map(fmt).join(' ')),
      },
      module: { exports: {} },
    }, { codeGeneration: { strings: false, wasm: false } });
    ctx.exports = ctx.module.exports;
    try {
      new vm.Script(code, { filename: `${game.id}.server.js` }).runInContext(ctx, { timeout: LOAD_TIMEOUT_MS });
    } catch (e) {
      const s = { ctx, mod: {}, errors: [{ ts: Date.now(), hook: 'load', message: String(e && e.stack || e) }], code, failed: true };
      loaded.set(game.id, s);
      log(game.id, 'script', `Script failed to load: ${e && e.message}`);
      return s;
    }
    const mod = ctx.module.exports || {};
    const s = { ctx, mod, errors, code, failed: false };
    loaded.set(game.id, s);
    log(game.id, 'script', 'Server script loaded');
    return s;
  }
  function unload(gid) { loaded.delete(gid); }
  const get = (gid) => { const s = loaded.get(gid); return s && !s.failed ? s : null; };
  const fmt = (v) => (typeof v === 'string' ? v : (() => { try { return JSON.stringify(v); } catch { return String(v); } })());

  // Call a hook with a hard CPU timeout; flush any state ops the hook produced as ONE broadcast.
  function call(room, hook, ...args) {
    const s = get(room.game.id);
    if (!s || typeof s.mod[hook] !== 'function') return undefined;
    s.ctx.__f = s.mod[hook]; s.ctx.__a = [api(room), ...args];
    let r;
    try { dispatch.runInContext(s.ctx, { timeout: HOOK_TIMEOUT_MS }); r = s.ctx.__r; }
    catch (e) {
      const msg = String(e && e.message || e);
      s.errors.push({ ts: Date.now(), hook, message: String(e && e.stack || e).slice(0, 2000) });
      if (s.errors.length > 50) s.errors.shift();
      log(room.game.id, 'script', `Error in ${hook}: ${msg}`);
    } finally { s.ctx.__f = s.ctx.__a = s.ctx.__r = undefined; flush(room); }
    return r;
  }
  function flush(room) {
    if (room.pendingOps && room.pendingOps.length) {
      broadcast(room, { t: 'state', ops: room.pendingOps, by: 'server' });
      room.pendingOps = null;
    }
  }

  // The object scripts see as `room`. Cached per room; read-through to live data.
  function api(room) {
    if (room.api) return room.api;
    const op = (p, v, del) => { setPath(room.state, p, v, del); (room.pendingOps ||= []).push(del ? [p, null, 1] : [p, v]); };
    const toIds = (to) => (to == null || to === 'all' ? null : Array.isArray(to) ? to : [to]);
    room.api = {
      get id() { return room.id; },
      get state() { return room.state; },          // read freely; WRITE via setState/batchState so clients get it
      get meta() { return room.meta; },
      get host() { return room.host; },
      get players() { return [...room.players.values()].map(pubPlayer); },
      data: {},                                    // server-only scratch space (never sent to clients)
      player: (id) => { const m = room.players.get(id); return m ? pubPlayer(m) : null; },
      getState: (p) => p.split('.').reduce((o, k) => (o == null ? o : o[k]), room.state),
      setState: (p, v) => op(p, v, v === undefined),
      deleteState: (p) => op(p, null, true),
      batchState: (ops) => {
        const list = Array.isArray(ops) ? ops : Object.entries(ops);
        for (const [p, v, del] of list) op(p, v, !!del || v === undefined);
      },
      send: (type, data, to) => {
        const msg = { t: 'msg', type: String(type), data: data ?? null, from: 'server' };
        const ids = toIds(to);
        if (!ids) return broadcast(room, msg);
        for (const id of ids) { const m = room.players.get(String(id)); if (m && m.client) send(m.client, msg); }
      },
      kick: (id, reason = 'kicked') => {
        const m = room.players.get(String(id));
        if (!m) return;
        if (m.client) send(m.client, { t: 'left', room: room.id, reason });
        setImmediate(() => removeMember(room, m.id, reason));
      },
      lock: (locked = true) => { room.locked = !!locked; },
      now: () => Date.now(),
    };
    return room.api;
  }

  function startTick(room) {
    stopTick(room);
    const s = get(room.game.id);
    const rate = s && typeof s.mod.onTick === 'function' ? Math.min(MAX_TICK_RATE, Math.max(1, +s.mod.tickRate || DEFAULT_TICK_RATE)) : 0;
    if (!rate) return;
    let last = performance.now();
    room.tickTimer = setInterval(() => {
      const now = performance.now();
      call(room, 'onTick', (now - last) / 1000);
      last = now;
    }, 1000 / rate);
  }
  function stopTick(room) { if (room.tickTimer) { clearInterval(room.tickTimer); room.tickTimer = null; } }

  return {
    load, unload, call, startTick, stopTick, flush,
    has: (gid) => !!get(gid),
    authoritative: (gid) => { const s = get(gid); return !!(s && s.mod.authoritative); },
    info: (gid) => {
      const s = loaded.get(gid);
      if (!s) return { loaded: false, errors: [] };
      const hooks = Object.keys(s.mod).filter((k) => typeof s.mod[k] === 'function');
      return { loaded: !s.failed, failed: !!s.failed, hooks, authoritative: !!s.mod.authoritative,
        tickRate: s.mod.tickRate || 0, errors: s.errors.slice(-20).reverse() };
    },
    E,
  };
}

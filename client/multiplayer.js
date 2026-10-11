/*!
 * Multiplayer SDK — client for the generic multiplayer server.
 * Works in browsers (global `Multiplayer`, or ESM/CommonJS import) and Node >= 22 (global WebSocket).
 *
 *   const mp = new Multiplayer({ url: 'wss://your-server', key: 'pk_...' });
 *   await mp.connect();
 *   await mp.quickJoin('lobby');
 *   mp.on('state', () => render(mp.state));
 */
(function (root, factory) {
  const M = factory();
  if (typeof module === 'object' && module.exports) module.exports = M;
  root.Multiplayer = M;
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  'use strict';

  const BAD = new Set(['__proto__', 'prototype', 'constructor']);
  const isObj = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);
  function setPath(obj, path, value, del) {
    const parts = String(path).split('.');
    let o = obj;
    for (let i = 0; i < parts.length - 1; i++) {
      const k = parts[i];
      if (BAD.has(k)) return;
      if (o[k] === null || typeof o[k] !== 'object') { if (del) return; o[k] = {}; }
      o = o[k];
    }
    const last = parts[parts.length - 1];
    if (BAD.has(last)) return;
    if (del) { if (Array.isArray(o) && /^\d+$/.test(last)) o.splice(+last, 1); else delete o[last]; }
    else o[last] = value;
  }
  function getPath(obj, path) {
    if (!path) return obj;
    let o = obj;
    for (const k of String(path).split('.')) { if (o == null) return undefined; o = o[k]; }
    return o;
  }
  function merge(t, s) { for (const k of Object.keys(s)) { if (BAD.has(k)) continue; if (s[k] === null) delete t[k]; else t[k] = s[k]; } return t; }

  function storageFor(kind) {
    try {
      const s = kind === 'device' ? globalThis.localStorage : kind === 'tab' ? globalThis.sessionStorage : null;
      if (s) { s.setItem('__mp', '1'); s.removeItem('__mp'); return s; }
    } catch (e) { /* private mode / node */ }
    const mem = {};
    return { getItem: (k) => mem[k] ?? null, setItem: (k, v) => { mem[k] = String(v); }, removeItem: (k) => { delete mem[k]; } };
  }

  class MultiplayerError extends Error {
    constructor(code, message) { super(message || code); this.code = code; this.name = 'MultiplayerError'; }
  }

  class Multiplayer {
    /**
     * @param {object} o
     * @param {string} o.url        Server base URL: https://host or wss://host (path /ws added automatically)
     * @param {string} o.key        Public game key (pk_...)
     * @param {string} [o.name]     Display name
     * @param {'tab'|'device'|'none'} [o.identity='tab']  Where the player token is kept. 'device' = same player across tabs/sessions.
     * @param {boolean} [o.autoReconnect=true]
     * @param {number} [o.timeout=10000] Request timeout ms
     * @param {Function} [o.WebSocket]   Custom WebSocket implementation (e.g. `ws` in older Node)
     */
    constructor(o = {}) {
      if (!o.url || !o.key) throw new Error('Multiplayer: { url, key } are required');
      let url = String(o.url).replace(/\/+$/, '').replace(/^http/, 'ws');
      if (!/\/ws$/.test(url)) url += '/ws';
      this.url = url;
      this.key = o.key;
      this.opts = Object.assign({ identity: 'tab', autoReconnect: true, timeout: 10000 }, o);
      this._WS = o.WebSocket || globalThis.WebSocket;
      this._store = storageFor(this.opts.identity);
      this._tokenKey = 'mp_token_' + this.key;
      this._handlers = {};
      this._pending = new Map();
      this._rid = 0;
      this._ws = null;
      this._closedByUser = false;
      this._retry = 0;
      this._clockOffset = 0;
      this.connected = false;
      this.playerId = null;
      this.username = null;
      this.name = o.name || null;
      this.game = null;
      this.room = null;      // { id, host, meta, maxPlayers, locked, private, tag, stateWrite, ... }
      this.state = {};       // shared room state (read-only locally; write via setState)
      this.players = new Map(); // id -> { id, name, data, connected, joined, username }
      this.latency = 0;
      this.storage = {
        get: (key, o = {}) => this.request('kv.get', { key, scope: o.scope, player: o.player }),
        set: (key, value, o = {}) => this.request('kv.set', { key, value, scope: o.scope }),
        delete: (key, o = {}) => this.request('kv.del', { key, scope: o.scope }),
        incr: (key, by = 1, o = {}) => this.request('kv.incr', { key, by, scope: o.scope }),
        list: (o = {}) => this.request('kv.list', { scope: o.scope, player: o.player, prefix: o.prefix, limit: o.limit }),
      };
      this.leaderboard = {
        submit: (board, score, o = {}) => this.request('lb.submit', { board, score, mode: o.mode }),
        top: (board, o = {}) => this.request('lb.top', { board, limit: o.limit, order: o.order }),
      };
      this.auth = {
        register: async (username, password) => this._setIdentity(await this.request('auth.register', { username, password })),
        login: async (username, password) => {
          const roomId = this.room && this.room.id;
          const r = this._setIdentity(await this.request('auth.login', { username, password }));
          if (roomId && r.playerId !== this._prevPid) this._resetRoom('left');
          return r;
        },
        logout: async () => { this._store.removeItem(this._tokenKey); this.username = null; this.disconnect(); return this.connect(); },
      };
    }

    // ---------- events ----------
    on(type, fn) { (this._handlers[type] = this._handlers[type] || new Set()).add(fn); return () => this.off(type, fn); }
    off(type, fn) { this._handlers[type] && this._handlers[type].delete(fn); }
    once(type, fn) { const off = this.on(type, (...a) => { off(); fn(...a); }); return off; }
    _emit(type, ...args) {
      const hs = this._handlers[type];
      if (hs) for (const fn of [...hs]) { try { fn(...args); } catch (e) { console.error('[mp] handler error for', type, e); } }
    }
    /** Listen for messages of a given type sent via send(). fn(data, fromPlayerId, raw) */
    onMessage(type, fn) { return this.on('msg:' + type, fn); }

    // ---------- connection ----------
    connect() {
      this._closedByUser = false;
      if (this._connecting) return this._connecting;
      this._connecting = new Promise((resolve, reject) => {
        let ws;
        try { ws = new this._WS(this.url); } catch (e) { this._connecting = null; return reject(e); }
        this._ws = ws;
        let welcomed = false;
        ws.onopen = () => {
          ws.send(JSON.stringify({ t: 'hello', v: 1, key: this.key, token: this._store.getItem(this._tokenKey) || undefined, name: this.name || undefined }));
        };
        ws.onmessage = (ev) => {
          let m; try { m = JSON.parse(ev.data); } catch (e) { return; }
          if (m.t === 'welcome') {
            welcomed = true;
            this._connecting = null;
            this._onWelcome(m).then(resolve, reject);
            return;
          }
          this._onMessage(m);
        };
        ws.onclose = (ev) => {
          const wasConnected = this.connected;
          this.connected = false;
          this._connecting = null;
          for (const p of this._pending.values()) { clearTimeout(p.timer); p.reject(new MultiplayerError('disconnected')); }
          this._pending.clear();
          clearInterval(this._pingTimer);
          const fatal = [4001, 4003, 4004].includes(ev.code);
          if (!welcomed) reject(new MultiplayerError(ev.reason || 'connect_failed', `Connection closed (${ev.code} ${ev.reason || ''})`));
          if (wasConnected) this._emit('disconnected', { code: ev.code, reason: ev.reason });
          if (fatal) { this._emit('error', new MultiplayerError(ev.reason || 'closed', `Closed by server: ${ev.reason}`)); this._resetRoom(ev.reason); return; }
          if (!this._closedByUser && this.opts.autoReconnect && (wasConnected || this._retry > 0)) this._scheduleReconnect();
        };
        ws.onerror = () => {};
      });
      return this._connecting;
    }

    async _onWelcome(m) {
      this._prevPid = this.playerId;
      this.playerId = m.playerId;
      this.name = m.name;
      this.username = m.username;
      this.game = m.game;
      this._store.setItem(this._tokenKey, m.token);
      this._clockOffset = m.serverTime - Date.now();
      this.connected = true;
      const reconnecting = this._retry > 0;
      this._retry = 0;
      this._pingTimer = setInterval(() => this.ping().catch(() => {}), 10000);
      this.ping().catch(() => {});
      if (reconnecting && this.room) {
        const id = this.room.id;
        try { this._applySnapshot(await this.request('join', { room: id, options: this._lastJoinOpts || {} })); }
        catch (e) { this._resetRoom('rejoin_failed'); }
        this._emit('reconnected', { room: this.room });
      }
      this._emit('connected', { playerId: this.playerId, name: this.name, username: this.username, returning: m.returning });
      return { playerId: this.playerId, name: this.name, username: this.username, returning: m.returning };
    }

    _scheduleReconnect() {
      const delay = Math.min(500 * 2 ** this._retry, 8000) * (0.75 + Math.random() * 0.5);
      this._retry++;
      this._emit('reconnecting', { attempt: this._retry, delay });
      clearTimeout(this._reconnTimer);
      this._reconnTimer = setTimeout(() => this.connect().catch(() => {}), delay);
    }

    disconnect() {
      this._closedByUser = true;
      clearTimeout(this._reconnTimer);
      clearInterval(this._pingTimer);
      if (this._ws) try { this._ws.close(1000, 'bye'); } catch (e) {}
      this._resetRoom('disconnected');
    }

    _setIdentity(r) {
      this._prevPid = this.playerId;
      this.playerId = r.playerId; this.username = r.username; if (r.name) this.name = r.name;
      this._store.setItem(this._tokenKey, r.token);
      return r;
    }

    // ---------- requests ----------
    /** Low-level: send a typed request, resolves with server data or rejects with MultiplayerError */
    request(t, payload = {}) {
      return new Promise((resolve, reject) => {
        if (!this._ws || this._ws.readyState !== 1 || !this.connected) return reject(new MultiplayerError('not_connected'));
        const rid = ++this._rid;
        const timer = setTimeout(() => { this._pending.delete(rid); reject(new MultiplayerError('timeout')); }, this.opts.timeout);
        this._pending.set(rid, { resolve, reject, timer });
        this._ws.send(JSON.stringify(Object.assign({}, payload, { t, rid })));
      });
    }
    /** Fire-and-forget (no ack). Returns false if not connected. */
    _fire(t, payload) {
      if (!this._ws || this._ws.readyState !== 1 || !this.connected) return false;
      this._ws.send(JSON.stringify(Object.assign({}, payload, { t })));
      return true;
    }

    _onMessage(m) {
      switch (m.t) {
        case 'res': {
          const p = this._pending.get(m.rid);
          if (!p) return;
          this._pending.delete(m.rid); clearTimeout(p.timer);
          return m.ok ? p.resolve(m.d) : p.reject(new MultiplayerError(m.err.code, m.err.message));
        }
        case 'state': {
          if (m.full) this.state = m.full;
          for (const [p, v, del] of m.ops) setPath(this.state, p, v, !!del);
          this._emit('state', this.state, { ops: m.ops, by: m.by });
          for (const [p, v] of m.ops) this._emit('state:' + p.split('.')[0], getPath(this.state, p.split('.')[0]), { path: p, value: v, by: m.by });
          return;
        }
        case 'player': {
          const pl = this.players.get(m.id);
          if (!pl) return;
          if (m.name) pl.name = m.name;
          if (m.data) { if (m.replace) pl.data = {}; merge(pl.data, m.data); }
          return this._emit('playerUpdated', pl, m.data || {});
        }
        case 'player.joined': {
          this.players.set(m.player.id, m.player);
          return this._emit('playerJoined', m.player);
        }
        case 'player.left': {
          const pl = this.players.get(m.id);
          this.players.delete(m.id);
          return this._emit('playerLeft', pl || { id: m.id }, m.reason);
        }
        case 'player.connection': {
          const pl = this.players.get(m.id);
          if (pl) { pl.connected = m.connected; this._emit('playerConnection', pl, m.connected); }
          return;
        }
        case 'host': {
          if (this.room) this.room.host = m.id;
          return this._emit('host', m.id, m.id === this.playerId);
        }
        case 'room': {
          if (this.room) Object.assign(this.room, m.room);
          return this._emit('roomUpdated', this.room);
        }
        case 'left': {
          if (this.room && this.room.id === m.room) {
            this._resetRoom(m.reason);
            if (m.reason === 'kicked') this._emit('kicked');
          }
          return;
        }
        case 'msg': {
          const from = m.from;
          this._emit('message', { type: m.type, data: m.data, from, direct: !!m.direct });
          return this._emit('msg:' + m.type, m.data, from, m);
        }
        case 'error': return this._emit('error', new MultiplayerError(m.code, m.message));
      }
    }

    // ---------- rooms ----------
    _applySnapshot(s) {
      const { players, state, ...info } = s;
      this.room = info;
      this.state = state || {};
      this.players = new Map(players.map((p) => [p.id, p]));
      this._emit('joined', this.room);
      this._emit('state', this.state, { ops: [], by: null, full: true });
      return this.room;
    }
    _resetRoom(reason) {
      if (!this.room) return;
      const old = this.room;
      this.room = null; this.state = {}; this.players = new Map();
      this._emit('left', old, reason);
    }
    /** Join room by id (created if it doesn't exist). options: {maxPlayers, private, password, meta, tag, persist, stateWrite:'all'|'host', state, playerData, mustExist} */
    async join(roomId, options = {}) { this._lastJoinOpts = { password: options.password }; return this._applySnapshot(await this.request('join', { room: roomId, options })); }
    /** Create a new room with a random 6-char code (great for "share this code" lobbies). */
    async create(options = {}) { this._lastJoinOpts = { password: options.password }; return this._applySnapshot(await this.request('create', { options })); }
    /** Matchmaking: join the fullest open public room with this tag, or create one. */
    async quickJoin(tag = '', options = {}) { this._lastJoinOpts = {}; return this._applySnapshot(await this.request('quick', { tag, options })); }
    async leave() { if (!this.room) return; await this.request('leave'); this._resetRoom('left'); }
    listRooms(tag) { return this.request('rooms', { tag }); }
    updateRoom(changes) { return this.request('room.update', changes); }
    kick(playerId, ban = false) { return this.request('kick', { id: playerId, ban }); }
    transferHost(playerId) { return this.request('room.update', { host: playerId }); }

    get isHost() { return !!this.room && this.room.host === this.playerId; }
    get me() { return this.players.get(this.playerId) || null; }
    get playerList() { return [...this.players.values()]; }

    // ---------- shared state ----------
    /** Set shared room state at a dot-path. Applied on all clients (including you) once the server confirms. */
    setState(path, value) { return this.request('state.set', { ops: [[path, value]] }); }
    deleteState(path) { return this.request('state.set', { ops: [[path, null, 1]] }); }
    /** Atomic batch: { 'a.b': 1, 'c': null-to-delete-use-deleteState } or array [[path, value, del?]] */
    batchState(ops) {
      const arr = Array.isArray(ops) ? ops : Object.entries(ops).map(([p, v]) => (v === undefined ? [p, null, 1] : [p, v]));
      return this.request('state.set', { ops: arr });
    }
    getState(path) { return getPath(this.state, path); }

    // ---------- per-player data ----------
    /** Merge into your own player data (positions, ready flags...). Applied locally immediately, broadcast to others. Fire-and-forget; null deletes a key. */
    setPlayer(data, { replace = false } = {}) {
      const me = this.me;
      if (me) { if (replace) me.data = {}; merge(me.data, data); this._emit('playerUpdated', me, data); }
      return this._fire('player.set', { data, replace });
    }

    // ---------- messaging ----------
    /** Send a message. to: undefined (everyone else) | 'all' | 'host' | playerId | [ids]. Outside a room, `to` must be a playerId (direct message). */
    send(type, data, to, { self = false } = {}) { return this._fire('send', { type, data, to, self }); }
    /** Same as send but resolves when the server accepted it (rejects on error). */
    sendReliable(type, data, to, { self = false } = {}) { return this.request('send', { type, data, to, self }); }

    // ---------- misc ----------
    async setName(name) { const r = await this.request('name', { name }); this.name = r.name; const me = this.me; if (me) me.name = r.name; return r.name; }
    async ping() {
      const t0 = Date.now();
      const r = await this.request('ping', { c: t0 });
      const t1 = Date.now();
      this.latency = t1 - t0;
      this._clockOffset = r.s + this.latency / 2 - t1;
      this._emit('ping', this.latency);
      return this.latency;
    }
    /** Server-synchronised clock (ms). Use for timers/countdowns all clients agree on. */
    serverTime() { return Date.now() + this._clockOffset; }
  }

  Multiplayer.MultiplayerError = MultiplayerError;
  Multiplayer.default = Multiplayer;
  return Multiplayer;
});

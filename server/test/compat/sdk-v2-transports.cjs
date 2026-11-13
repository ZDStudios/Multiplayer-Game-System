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

  // Node built-ins without an import statement, so this file still loads as a plain browser script.
  const nodeModule = (name) => {
    try { return typeof process !== 'undefined' && process.getBuiltinModule ? process.getBuiltinModule(name) : null; } catch (e) { return null; }
  };

  /**
   * Session layer, shared with the server. One ordered, reliable message stream that can ride on any
   * transport (link) and move between them mid-game without losing or reordering anything.
   *   reliable:   {"q":seq,"a":ack,"m":msg}   resent until acked, delivered in seq order, duplicates dropped
   *   unreliable: {"a":ack,"m":msg}            delivered on arrival, may be lost (latest-wins data)
   *   ack only:   {"a":ack}
   * A link is { kind, reliable, max, send(frame, unreliable), alive() }. `reliable` links (WebSocket, the
   * WebRTC ordered channel) never need a resend unless they die; UDP frames are resent after an RTO.
   * Frames bigger than link.max go over `fallback` (the WebSocket); seq numbers keep them in order.
   */
  class Session {
    constructor(fallback, deliver) {
      this.fallback = this.active = fallback;
      this.deliver = deliver;      // (msg, link, unreliable) => void
      this.rtt = 100;              // smoothed RTT of the active link (ms), drives the resend timeout
      this.out = 0;                // last reliable seq sent
      this.unacked = [];           // [{ q, s, via, at }] in seq order
      this.inNext = 1;             // next reliable seq expected
      this.buf = new Map();        // out-of-order arrivals
      this.ackOwed = false;
    }
    _link(s) { const t = this.active; return t.alive() && s.length <= t.max ? t : this.fallback; }
    _frame(q, s) {
      let f = '{';
      if (q) f += '"q":' + q + ',';
      if (this.ackOwed) { f += '"a":' + (this.inNext - 1) + ','; this.ackOwed = false; }
      return f + '"m":' + s + '}';
    }
    /** Send a serialized message. Returns the number of unacked reliable messages (for backpressure checks). */
    send(s, unreliable) {
      const t = this._link(s);
      if (unreliable) { t.send(this._frame(0, s), true); return this.unacked.length; }
      const q = ++this.out;
      this.unacked.push({ q, s, via: t, at: Date.now() });
      t.send(this._frame(q, s), false);
      return this.unacked.length;
    }
    recv(f, link) {
      if (typeof f.a === 'number') while (this.unacked.length && this.unacked[0].q <= f.a) this.unacked.shift();
      if (f.m === undefined) return;
      if (typeof f.q !== 'number') return this.deliver(f.m, link, true);
      this.ackOwed = true;
      if (f.q < this.inNext) return; // duplicate (resend that crossed our ack)
      if (f.q > this.inNext) { if (this.buf.size < 4096) this.buf.set(f.q, f.m); return; }
      this.inNext++;
      this.deliver(f.m, link, false);
      while (this.buf.has(this.inNext)) { const m = this.buf.get(this.inNext); this.buf.delete(this.inNext++); this.deliver(m, link, false); }
    }
    /** Call every ~25 ms: resends what was lost (or stranded on a dead link) and flushes a pending ack. */
    tick(now) {
      const rto = Math.min(1500, Math.max(100, this.rtt * 2 + 80));
      for (const u of this.unacked) {
        if (u.via.alive() && (u.via.reliable || now - u.at < rto)) continue;
        const t = this._link(u.s);
        u.via = t; u.at = now;
        t.send(this._frame(u.q, u.s), false);
      }
      if (this.ackOwed) { this.ackOwed = false; this._link('').send('{"a":' + (this.inNext - 1) + '}', true); }
    }
  }

  const TRANSPORTS = ['webrtc', 'udp', 'websocket'];
  // How much worse (ms) a link must score before we leave it: avoids flapping between near-equal links.
  const SWITCH_MARGIN = 2;
  // Tie-break: given equal numbers, prefer the UDP-based links (no TCP head-of-line blocking on loss).
  // The WebSocket handicap is bigger than SWITCH_MARGIN so equal links still move off TCP.
  const BIAS = { webrtc: 0, udp: 1, websocket: 6 };
  function newLink(kind, o) {
    return Object.assign({ kind, reliable: false, max: 1150, open: false, failed: false, srtt: 0, jitter: 0, samples: 0,
      hist: [], pend: new Map(), lastPong: 0, alive() { return this.open; }, close() {} }, o);
  }
  function linkStats(l) {
    const loss = l.hist.length ? l.hist.reduce((a, b) => a + b, 0) / l.hist.length : 0;
    return { open: l.open, rtt: Math.round(l.srtt * 10) / 10, jitter: Math.round(l.jitter * 10) / 10, loss: Math.round(loss * 100) / 100, samples: l.samples };
  }
  function linkScore(l, now) {
    if (!l.alive() || !l.samples || (l.kind !== 'websocket' && l.samples < 3)) return Infinity;
    for (const at of l.pend.values()) if (now - at > 2500) return Infinity; // stopped answering
    const s = linkStats(l);
    return l.srtt + 2 * l.jitter + 400 * s.loss + BIAS[l.kind];
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
     * @param {boolean} [o.optimistic=true] Apply your own setState writes locally at once (server order still wins)
     * @param {string[]} [o.transports=['webrtc','udp','websocket']] Links the SDK may use. It connects over WebSocket,
     *   measures every allowed link in the background and moves traffic to the fastest. ['websocket'] = WebSocket only.
     * @param {Function} [o.RTCPeerConnection] WebRTC implementation for Node (e.g. require('node-datachannel/polyfill').RTCPeerConnection)
     * @param {RTCIceServer[]} [o.iceServers=[]] STUN/TURN servers for the WebRTC link (rarely needed: the server has a public IP)
     * @param {number} [o.probeInterval=1000] ms between background link measurements once settled
     */
    constructor(o = {}) {
      if (!o.url || !o.key) throw new Error('Multiplayer: { url, key } are required');
      let url = String(o.url).replace(/\/+$/, '').replace(/^http/, 'ws');
      if (!/\/ws$/.test(url)) url += '/ws';
      this.url = url;
      this.key = o.key;
      this.opts = Object.assign({ identity: 'tab', autoReconnect: true, timeout: 10000, optimistic: true,
        transports: TRANSPORTS, iceServers: [], probeInterval: 1000 }, o);
      this.transport = null;     // link in use: 'webrtc' | 'udp' | 'websocket'
      this._links = new Map();   // kind -> link
      this._pseq = 0;
      this._optim = []; // my state batches applied locally, awaiting the server echo
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
      this._samples = new Map(); // playerId -> last two data samples (for predict())
      this._pseen = new Map();   // playerId -> newest fast-lane snapshot seq applied
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
    /**
     * Connect. Optionally join in the same round trip (saves ~1 RTT):
     *   await mp.connect({ room: 'ABC123', options })  |  { quickJoin: 'tag', options }  |  { create: true, options }
     */
    connect(o = {}) {
      this._closedByUser = false;
      if (this._connecting) return this._connecting;
      if (o.room != null) this._autoJoin = ['join', { room: o.room, options: o.options || {} }];
      else if (o.quickJoin != null) this._autoJoin = ['quick', { tag: o.quickJoin, options: o.options || {} }];
      else if (o.create) this._autoJoin = ['create', { options: o.options || {} }];
      if (this._autoJoin) this._lastJoinOpts = { password: (o.options || {}).password };
      this._connecting = new Promise((resolve, reject) => {
        let ws;
        try { ws = new this._WS(this.url); } catch (e) { this._connecting = null; return reject(e); }
        this._ws = ws;
        let welcomed = false;
        // The WebSocket opens first (instant, works everywhere) and stays up as the signalling channel and fallback.
        const wsLink = newLink('websocket', { reliable: true, max: Infinity, open: true,
          send: (f) => { if (ws.readyState === 1) ws.send(f); }, alive: () => ws.readyState === 1 });
        this._links = new Map([['websocket', wsLink]]);
        this.transport = 'websocket';
        this._sess = new Session(wsLink, (m) => {
          if (m.t !== 'welcome') return this._onMessage(m);
          welcomed = true;
          this._connecting = null;
          this._onWelcome(m).then(resolve, reject);
        });
        ws.onopen = () => {
          this._tickTimer = setInterval(() => this._sess.tick(Date.now()), 25);
          this._sess.send(JSON.stringify({ t: 'hello', v: 2, key: this.key, token: this._store.getItem(this._tokenKey) || undefined, name: this.name || undefined }));
          // Pipeline: send the (re)join right behind hello instead of waiting for welcome.
          const j = this.room && this._retry > 0 ? ['join', { room: this.room.id, options: this._lastJoinOpts || {} }] : this._autoJoin;
          this._autoJoin = null;
          this._joinP = j ? this._send(j[0], j[1]) : null;
          if (this._joinP) this._joinP.catch(() => {});
        };
        ws.onmessage = (ev) => this._onFrame(wsLink, ev.data);
        ws.onclose = (ev) => {
          const wasConnected = this.connected;
          this.connected = false;
          this._connecting = null;
          for (const p of this._pending.values()) { clearTimeout(p.timer); p.reject(new MultiplayerError('disconnected')); }
          this._pending.clear();
          clearInterval(this._pingTimer);
          this._closeLinks();
          const fatal = [4001, 4003, 4004, 4005].includes(ev.code);
          if (ev.code === 4005) this._emit('kicked', ev.reason);
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
      this._pingTimer = setInterval(() => this.ping().catch(() => {}), 5000);
      this.ping().catch(() => {});
      this._startLinks(m.transports || {});
      const joinP = this._joinP; this._joinP = null;
      if (reconnecting && this.room) {
        try { this._applySnapshot(await (joinP || this.request('join', { room: this.room.id, options: this._lastJoinOpts || {} }))); }
        catch (e) { this._resetRoom('rejoin_failed'); }
        this._emit('reconnected', { room: this.room });
      } else if (joinP) {
        this._applySnapshot(await joinP); // connect({room|quickJoin|create}) — rejects connect() if the join fails
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
      this._closeLinks();
      if (this._ws) try { this._ws.close(1000, 'bye'); } catch (e) {}
      this._resetRoom('disconnected');
    }

    // ---------- transports ----------
    // After welcome, open every other allowed link next to the WebSocket and probe them all: fast at first
    // (so the best link is in use within ~0.5 s), then every probeInterval for the rest of the session.
    // Traffic moves to the link with the best score (RTT + jitter + loss) and back if it degrades or dies.
    _startLinks(info) {
      this._linkInfo = info;
      this._retryAt = {};
      this._probeN = 0;
      this._probeSeq = 0;
      const loop = () => {
        this._probeTimer = setTimeout(loop, this._probeN < 15 ? 100 : this.opts.probeInterval);
        this._ensureLinks();
        this._probe();
        this._choose();
      };
      loop();
    }
    _ensureLinks() {
      const info = this._linkInfo, now = Date.now();
      for (const kind of ['webrtc', 'udp']) {
        if (!info[kind] || !this.opts.transports.includes(kind)) continue;
        const l = this._links.get(kind);
        if (l && !l.failed) continue;
        const r = this._retryAt[kind] || (this._retryAt[kind] = { at: 0, wait: 5000 });
        if (now < r.at) continue;
        if (l) { r.wait = Math.min(r.wait * 2, 60000); this._emit('transportFailed', kind); }
        r.at = now + r.wait;
        try { kind === 'udp' ? this._openUdp(info.udp) : this._openRtc(info.webrtc); } catch (e) { /* unsupported here */ }
      }
    }
    _probe() {
      const now = Date.now();
      this._probeN++;
      for (const l of this._links.values()) {
        for (const [id, at] of l.pend) if (now - at > 1000) { l.pend.delete(id); l.hist.push(1); if (l.hist.length > 20) l.hist.shift(); }
        if (!l.alive()) continue;
        const id = ++this._probeSeq;
        l.pend.set(id, now);
        l.send('{"pi":' + id + '}', true);
      }
    }
    _pong(l, id) {
      const at = l.pend.get(id);
      if (at == null) return;
      l.pend.delete(id);
      const r = Date.now() - at;
      l.jitter = l.samples ? l.jitter * 0.8 + Math.abs(r - l.srtt) * 0.2 : 0;
      l.srtt = l.samples ? l.srtt * 0.8 + r * 0.2 : r;
      l.samples++; l.lastPong = Date.now();
      l.hist.push(0); if (l.hist.length > 20) l.hist.shift();
    }
    _choose() {
      const now = Date.now(), cur = this._links.get(this.transport);
      let best = null, bestScore = Infinity;
      for (const l of this._links.values()) { const s = linkScore(l, now); if (s < bestScore) { best = l; bestScore = s; } }
      const curScore = cur ? linkScore(cur, now) : Infinity;
      if (!best) best = this._links.get('websocket');
      if (best !== cur && (curScore === Infinity || bestScore < curScore - Math.max(SWITCH_MARGIN, curScore * 0.1))) {
        if (curScore !== Infinity && ++this._better < 2) return; // must win twice in a row unless the current link is dead
        this._use(best);
      }
      this._better = 0;
      if (cur === best && this._probeN % 5 === 0) this._ctl({ use: best.kind, rtt: Math.round(best.srtt) }); // keep the server's resend timer tuned
    }
    _use(l) {
      const prev = this.transport;
      this.transport = l.kind;
      this._sess.active = l;
      this._sess.rtt = l.srtt || this._sess.rtt;
      this._ctl({ use: l.kind, rtt: Math.round(l.srtt) });
      this._emit('transport', l.kind, prev, this.transportStats);
    }
    /** Live measurements of every link: { webrtc: { open, rtt, jitter, loss, samples }, udp: {...}, websocket: {...} } */
    get transportStats() {
      const out = {};
      for (const [k, l] of this._links) out[k] = linkStats(l);
      return out;
    }
    _ctl(obj) { if (this._ws && this._ws.readyState === 1) this._ws.send(JSON.stringify(obj)); }
    _onFrame(l, data) {
      if (typeof data !== 'string') return;
      let f; try { f = JSON.parse(data); } catch (e) { return; }
      if (!isObj(f)) return;
      if (f.po != null) return this._pong(l, f.po);
      if (f.sig) return this._onSignal(f.sig);
      if (f.hk) { l.open = true; return; }
      if (this._sess) this._sess.recv(f, l);
    }
    _closeLinks() {
      clearTimeout(this._probeTimer);
      clearInterval(this._tickTimer);
      for (const l of this._links.values()) if (l.kind !== 'websocket') { l.open = false; l.failed = true; try { l.close(); } catch (e) {} }
    }

    // Raw UDP: Node (and other native runtimes) only, browsers can't open UDP sockets. Every datagram to the
    // server is prefixed with the session id from welcome; the server only answers the address that proved it.
    _openUdp(u) {
      const dgram = nodeModule('node:dgram'), dns = nodeModule('node:dns');
      if (!dgram || !dns || !u.sid) return;
      const old = this._links.get('udp'); if (old) old.close();
      const host = (u.host || new URL(this.url.replace(/^ws/, 'http')).hostname).replace(/^\[|\]$/g, '');
      const sess = this._sess;
      const l = newLink('udp', { reliable: false, max: 1150 });
      this._links.set('udp', l);
      dns.lookup(host, (err, addr, family) => {
        if (err || sess !== this._sess || l.failed) { l.failed = true; return; }
        const sock = dgram.createSocket(family === 6 ? 'udp6' : 'udp4');
        const prefix = u.sid + '|';
        sock.on('error', () => { l.open = false; l.failed = true; });
        sock.on('message', (buf, ri) => { if (ri.port === u.port) this._onFrame(l, buf.toString()); });
        l.send = (f) => { try { sock.send(prefix + f, u.port, addr); } catch (e) {} };
        l.close = () => { clearInterval(hs); try { sock.close(); } catch (e) {} };
        sock.unref();
        let tries = 0;
        const hs = setInterval(() => {
          if (l.open) return clearInterval(hs);
          if (++tries > 15) { clearInterval(hs); l.failed = true; return l.close(); }
          sock.send(prefix + '{"h":1}', u.port, addr);
        }, 200);
        if (hs.unref) hs.unref();
        sock.send(prefix + '{"h":1}', u.port, addr);
      });
    }

    // WebRTC data channels to the server: 'r' ordered+reliable, 'u' unordered with no retransmits (UDP-like).
    // Signalling (offer / answer / ICE candidates) goes over the WebSocket.
    async _openRtc() {
      const PC = this.opts.RTCPeerConnection || globalThis.RTCPeerConnection;
      if (!PC) return;
      const old = this._links.get('webrtc'); if (old) old.close();
      const sess = this._sess;
      const pc = new PC({ iceServers: this.opts.iceServers });
      const r = pc.createDataChannel('r', { ordered: true });
      const u = pc.createDataChannel('u', { ordered: false, maxRetransmits: 0 });
      const l = newLink('webrtc', { reliable: true, max: 60000, pc,
        send: (f, unrel) => { const ch = unrel && u.readyState === 'open' ? u : r; if (ch.readyState === 'open') try { ch.send(f); } catch (e) {} },
        alive: () => l.open && r.readyState === 'open',
        close: () => { clearTimeout(giveUp); try { pc.close(); } catch (e) {} } });
      this._links.set('webrtc', l);
      const onOpen = () => { if (r.readyState === 'open' && u.readyState === 'open') { l.open = true; clearTimeout(giveUp); } };
      const onDown = () => { l.open = false; l.failed = true; };
      r.onopen = u.onopen = onOpen;
      r.onclose = onDown;
      r.onmessage = u.onmessage = (ev) => this._onFrame(l, ev.data);
      pc.onicecandidate = (ev) => { if (ev.candidate && ev.candidate.candidate) this._ctl({ sig: { cand: ev.candidate.candidate, mid: ev.candidate.sdpMid } }); };
      pc.onconnectionstatechange = () => { if (pc.connectionState === 'failed' || pc.connectionState === 'closed') onDown(); };
      const giveUp = setTimeout(() => { if (!l.open) { onDown(); l.close(); } }, 8000);
      if (giveUp.unref) giveUp.unref();
      try {
        await pc.setLocalDescription(await pc.createOffer());
        if (sess !== this._sess) return l.close();
        this._ctl({ sig: { type: 'offer', sdp: pc.localDescription.sdp } });
      } catch (e) { onDown(); l.close(); }
    }
    _onSignal(sig) {
      const l = this._links.get('webrtc');
      if (!l || !l.pc || sig.type !== 'answer') return;
      l.pc.setRemoteDescription({ type: 'answer', sdp: sig.sdp }).catch(() => { l.failed = true; });
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
      if (!this.connected) return Promise.reject(new MultiplayerError('not_connected'));
      return this._send(t, payload);
    }
    _send(t, payload) {
      return new Promise((resolve, reject) => {
        if (!this._ws || this._ws.readyState !== 1) return reject(new MultiplayerError('not_connected'));
        const rid = ++this._rid;
        const timer = setTimeout(() => { this._pending.delete(rid); reject(new MultiplayerError('timeout')); }, this.opts.timeout);
        this._pending.set(rid, { resolve, reject, timer });
        this._sess.send(JSON.stringify(Object.assign({}, payload, { t, rid })));
      });
    }
    /** Fire-and-forget (no reply). unreliable = may be dropped, never resent (fast lane). Returns false if not connected. */
    _fire(t, payload, unreliable) {
      if (!this._ws || this._ws.readyState !== 1 || !this.connected) return false;
      this._sess.send(JSON.stringify(Object.assign({}, payload, { t })), unreliable);
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
          if (m.by === this.playerId && this._optim.length) {
            const mine = this._optim.shift();
            if (!mine.dirty) return; // already applied locally and nothing interleaved: skip the echo
          } else for (const b of this._optim) b.dirty = true; // someone else's write landed in between
          if (m.full) { this.state = m.full; this._optim = []; }
          for (const [p, v, del] of m.ops) setPath(this.state, p, v, !!del);
          this._emit('state', this.state, { ops: m.ops, by: m.by });
          for (const [p, v] of m.ops) this._emit('state:' + p.split('.')[0], getPath(this.state, p.split('.')[0]), { path: p, value: v, by: m.by });
          return;
        }
        case 'player': {
          const pl = this.players.get(m.id);
          if (!pl) return;
          if (m.s != null) { // fast-lane snapshot: drop anything older than what we already have
            if (m.s <= (this._pseen.get(m.id) || 0)) return;
            this._pseen.set(m.id, m.s);
          }
          if (m.name) pl.name = m.name;
          if (m.data) {
            const now = Date.now(), prev = this._samples.get(m.id);
            if (m.replace) pl.data = {};
            const before = prev ? prev.cur : null;
            merge(pl.data, m.data);
            this._samples.set(m.id, { prev: before, prevT: prev ? prev.t : 0, cur: Object.assign({}, pl.data), t: now });
          }
          return this._emit('playerUpdated', pl, m.data || {});
        }
        case 'player.joined': {
          this.players.set(m.player.id, m.player);
          return this._emit('playerJoined', m.player);
        }
        case 'player.left': {
          const pl = this.players.get(m.id);
          this.players.delete(m.id);
          this._samples.delete(m.id);
          this._pseen.delete(m.id);
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
      this._optim = [];
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
    setState(path, value) { return this._writeState([[path, value]]); }
    deleteState(path) { return this._writeState([[path, null, 1]]); }
    /** Atomic batch: { 'a.b': 1, 'c': null-to-delete-use-deleteState } or array [[path, value, del?]] */
    batchState(ops) {
      const arr = Array.isArray(ops) ? ops : Object.entries(ops).map(([p, v]) => (v === undefined ? [p, null, 1] : [p, v]));
      return this._writeState(arr);
    }
    _writeState(ops) {
      if (!this.opts.optimistic || !this.room || !this.connected) return this.request('state.set', { ops });
      // Client-side prediction: apply now (0 ms for you), reconcile with the server's ordering on echo.
      for (const [p, v, del] of ops) setPath(this.state, p, v, !!del || v === undefined);
      this._optim.push({ dirty: false });
      this._emit('state', this.state, { ops, by: this.playerId, optimistic: true });
      for (const [p, v] of ops) this._emit('state:' + String(p).split('.')[0], getPath(this.state, String(p).split('.')[0]), { path: p, value: v, by: this.playerId, optimistic: true });
      return this.request('state.set', { ops }).catch((e) => {
        this._optim.pop(); // rejected (e.g. not_host / server-authoritative): roll back to the server's truth
        return this.resync().then(() => { throw e; }, () => { throw e; });
      });
    }
    /** Re-fetch the authoritative room snapshot (used automatically after a rejected optimistic write). */
    async resync() { if (!this.room) return; return this._applySnapshot(await this.request('join', { room: this.room.id })); }
    getState(path) { return getPath(this.state, path); }

    // ---------- per-player data ----------
    /**
     * Merge into your own player data (positions, ready flags...). Applied locally immediately, broadcast to others. Fire-and-forget; null deletes a key.
     * { unreliable: true } = fast lane for high-rate data like movement: each update carries your whole player data
     * and a sequence number over the unreliable channel, so a lost or late packet is simply superseded by the next.
     * A reliable copy follows ~150 ms after the last update, so the final value always arrives.
     */
    setPlayer(data, { replace = false, unreliable = false } = {}) {
      const me = this.me;
      if (me) { if (replace) me.data = {}; merge(me.data, data); this._emit('playerUpdated', me, data); }
      if (!unreliable || !me) return this._fire('player.set', { data, replace });
      const snap = () => ({ data: me.data, replace: true, s: (this._pseq = Math.max(this._pseq + 1, Date.now())) });
      clearTimeout(this._settle);
      this._settle = setTimeout(() => this.me === me && this._fire('player.set', snap()), 150);
      return this._fire('player.set', snap(), true);
    }

    /**
     * Dead reckoning for remote players: where `keys` (default x,y) probably are *now*, extrapolated from the
     * velocity of their last two updates plus half your ping. Hides most of the network delay for smooth motion.
     * Returns a plain object { x, y, ... }. For your own player it returns your local data unchanged.
     */
    predict(playerId, keys = ['x', 'y'], { maxAheadMs = 250 } = {}) {
      const pl = this.players.get(playerId);
      if (!pl) return null;
      const out = {};
      const s = this._samples.get(playerId);
      for (const k of keys) out[k] = pl.data[k];
      if (playerId === this.playerId || !s || !s.prev || !s.prevT) return out;
      const dt = s.t - s.prevT;
      if (dt <= 0 || dt > 1000) return out;
      const ahead = Math.min(maxAheadMs, Date.now() - s.t + this.latency / 2);
      for (const k of keys) {
        const a = s.prev[k], b = s.cur[k];
        if (typeof a === 'number' && typeof b === 'number') out[k] = b + ((b - a) / dt) * ahead;
      }
      return out;
    }

    // ---------- messaging ----------
    /**
     * Send a message. to: undefined (everyone else) | 'all' | 'host' | 'server' (game's server script) | playerId | [ids]. Outside a room, `to` must be a playerId (direct message).
     * { unreliable: true } = fast lane: may be dropped or arrive out of order, never resent (inputs you send every frame anyway).
     */
    send(type, data, to, { self = false, unreliable = false } = {}) { return this._fire('send', { type, data, to, self }, unreliable); }
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
  Multiplayer.Session = Session; // the server uses the same session layer
  Multiplayer.default = Multiplayer;
  return Multiplayer;
});

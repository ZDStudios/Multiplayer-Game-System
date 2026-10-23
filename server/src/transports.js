// Transports. A v2 client keeps one session (ordered, reliable, acked: the Session class shared with the SDK)
// that can ride on any of three links, and the client decides which one carries the traffic:
//   websocket  always open first; signalling channel for the others and the fallback
//   webrtc     data channels 'r' (ordered, reliable) + 'u' (unordered, no retransmits), via node-datachannel
//   udp        raw datagrams for native / Node clients ("<sid>|<frame>"); browsers can't open UDP sockets
// The client probes every link ({"pi":n} -> {"po":n} on the same link) and tells us {"use":kind} over the
// WebSocket when it switches. Whatever was in flight on the old link is resent or reordered by the session.
import crypto from 'node:crypto';
import dgram from 'node:dgram';
import { createRequire } from 'node:module';

const { Session } = createRequire(import.meta.url)('../../client/multiplayer.js');
const isObj = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);
const MAX_UNACKED = 20000; // a client this far behind is stalled: drop it instead of buffering forever
const UDP_IDLE_MS = 6000;  // clients probe every second, so silence this long means the path is gone

// Dual-stack (IPv6 + IPv4-mapped) so "localhost" -> ::1 works too; IPv4-only when the host has no IPv6.
function bindUdp(cfg) {
  const tryBind = (type, addr) => new Promise((resolve, reject) => {
    const s = dgram.createSocket({ type, ipv6Only: false });
    s.once('error', reject);
    s.bind(cfg.udpPort, addr, () => { s.off('error', reject); resolve(s); });
  });
  if (cfg.udpBind) return tryBind(cfg.udpBind.includes(':') ? 'udp6' : 'udp4', cfg.udpBind);
  return tryBind('udp6', '::').catch(() => tryBind('udp4', '0.0.0.0'));
}

export async function createTransports(cfg, { deliver, log, onBytes }) {
  const sessions = new Set();
  const bySid = new Map();
  let rtc = null, udp = null;

  if (cfg.rtc) {
    try { rtc = await import('node-datachannel'); }
    catch (e) { console.log('[mp] WebRTC disabled: node-datachannel unavailable (' + e.message + ')'); }
  }

  if (cfg.udpPort) {
    udp = await bindUdp(cfg).catch((e) => { console.error('[mp] UDP disabled:', e.message); return null; });
  }
  if (udp) {
    udp.on('error', (e) => { console.error('[mp] UDP error', e.message); });
    udp.on('message', (buf, ri) => {
      if (buf.length > 1400) return;
      const s = buf.toString();
      const i = s.indexOf('|');
      if (i < 1 || i > 40) return;
      const c = bySid.get(s.slice(0, i));
      if (!c) return;
      const L = c.links.udp, body = s.slice(i + 1);
      if (body === '{"h":1}') { // (re)bind: holding the secret sid proves it's this client, even after a NAT rebind
        L.addr = ri.address; L.port = ri.port; L.seen = Date.now(); L.open = true;
        return L.send('{"hk":1}');
      }
      if (!L.open || ri.address !== L.addr || ri.port !== L.port) return;
      L.seen = Date.now();
      onFrame(c, L, body);
    });
  }

  const linkOf = (c, kind) => c.links[kind] || null;
  const sendCtl = (c, obj) => { if (c.ws.readyState === 1) c.ws.send(JSON.stringify(obj)); };

  /** Turn a WebSocket client into a v2 session client. */
  function attach(c) {
    const ws = { kind: 'websocket', reliable: true, max: Infinity, send: (f) => { if (c.ws.readyState === 1) c.ws.send(f); }, alive: () => c.ws.readyState === 1 };
    c.links = { websocket: ws, webrtc: null, udp: null };
    c.session = new Session(ws, (m, link, unreliable) => deliver(c, m, unreliable));
    sessions.add(c);
  }
  /** Called once the client said hello: what the client may open in addition to the WebSocket. */
  function offer(c) {
    const out = { webrtc: !!rtc };
    if (udp) {
      c.sid = crypto.randomBytes(15).toString('base64url');
      bySid.set(c.sid, c);
      const L = c.links.udp = { kind: 'udp', reliable: false, max: 1150, open: false, addr: '', port: 0, seen: 0,
        send: (f) => udp.send(f, L.port, L.addr, () => {}), alive: () => L.open && Date.now() - L.seen < UDP_IDLE_MS };
      out.udp = { port: cfg.udpPort, host: cfg.publicHost || undefined, sid: c.sid };
    }
    return out;
  }
  function detach(c) {
    sessions.delete(c);
    if (c.sid) bySid.delete(c.sid);
    if (c.links && c.links.webrtc) c.links.webrtc.close();
  }
  /** The link the server is currently sending on. */
  const active = (c) => (c.session ? c.session.active.kind : 'websocket');

  function onFrame(c, L, raw) {
    if (raw.length > cfg.maxMsg) return;
    onBytes(raw.length);
    let f;
    try { f = JSON.parse(raw); } catch { return; }
    if (!isObj(f)) return;
    if (f.pi != null) return L.send('{"po":' + (Math.floor(+f.pi) || 0) + '}', true);
    if (f.use != null) {
      if (!c.game) return;
      const t = linkOf(c, String(f.use));
      if (t && t.alive()) c.session.active = t;
      if (+f.rtt > 0) c.session.rtt = Math.min(2000, +f.rtt);
      return;
    }
    if (f.sig != null) return c.game && L.kind === 'websocket' && signal(c, f.sig);
    c.session.recv(f, L);
  }

  // ---- WebRTC: answer the client's offer; the client opens both data channels ----
  function signal(c, sig) {
    if (!rtc || !isObj(sig)) return;
    if (sig.type === 'offer' && typeof sig.sdp === 'string' && sig.sdp.length < 20000) {
      const now = Date.now();
      if (now - (c.rtcAt || 0) < 2000) return; // one offer per 2 s per client
      c.rtcAt = now;
      if (c.links.webrtc) c.links.webrtc.close();
      let pc;
      try {
        pc = new rtc.PeerConnection(c.pid || 'peer', {
          iceServers: cfg.iceServers, enableIceUdpMux: !!cfg.rtcPort,
          portRangeBegin: cfg.rtcPort || undefined, portRangeEnd: cfg.rtcPort || undefined, maxMessageSize: 65536,
        });
      } catch (e) { return log(c.game.id, 'net', 'WebRTC peer failed: ' + e.message); }
      const L = c.links.webrtc = { kind: 'webrtc', reliable: true, max: 60000, pc, r: null, u: null, closed: false,
        send(f, unrel) { const ch = unrel && L.u && L.u.isOpen() ? L.u : L.r; if (ch && ch.isOpen()) try { ch.sendMessage(f); } catch {} },
        alive: () => !L.closed && !!L.r && L.r.isOpen(),
        close() { if (L.closed) return; L.closed = true; try { pc.close(); } catch {} } };
      pc.onDataChannel((dc) => {
        const label = dc.getLabel();
        if ((label !== 'r' && label !== 'u') || L[label]) return dc.close();
        L[label] = dc;
        dc.onMessage((msg) => { if (typeof msg === 'string' && !L.closed) onFrame(c, L, msg); });
        dc.onClosed(() => { if (label === 'r') L.close(); });
      });
      pc.onStateChange((st) => { if (st === 'failed' || st === 'closed') L.close(); });
      let answered = false;
      pc.onGatheringStateChange((st) => {
        if (st !== 'complete' || answered || L.closed) return;
        answered = true;
        const d = pc.localDescription();
        if (d) sendCtl(c, { sig: { type: 'answer', sdp: publicSdp(d.sdp) } });
      });
      try { pc.setRemoteDescription(sig.sdp, 'offer'); } catch (e) { L.close(); }
    } else if (typeof sig.cand === 'string' && c.links.webrtc && !c.links.webrtc.closed) {
      try { c.links.webrtc.pc.addRemoteCandidate(sig.cand, String(sig.mid ?? '0')); } catch {}
    }
  }
  // Inside Docker the server only sees its container IP. With PUBLIC_IP set, advertise that instead.
  function publicSdp(sdp) {
    if (!cfg.publicIp) return sdp;
    return sdp.replace(/^(a=candidate:\S+ \d+ \S+ \d+ )(\d+\.\d+\.\d+\.\d+)( \d+ typ host)/gm,
      (all, a, ip, b) => (ip.startsWith('127.') ? all : a + cfg.publicIp + b));
  }

  // Resends + delayed acks for every session (cheap: almost always nothing to do).
  setInterval(() => {
    const now = Date.now();
    for (const c of sessions) {
      const s = c.session;
      if (s.unacked.length > MAX_UNACKED) { c.ws.close(4029, 'backpressure'); continue; }
      if (s.active !== s.fallback && !s.active.alive()) s.active = s.fallback; // the chosen link died: back to WebSocket
      s.tick(now);
    }
  }, 25).unref();

  return {
    attach, offer, detach, active, onFrame,
    info: () => ({ webrtc: !!rtc, udp: udp ? cfg.udpPort : null, rtcPort: cfg.rtcPort || null }),
    close() { try { udp && udp.close(); } catch {} for (const c of sessions) detach(c); },
  };
}

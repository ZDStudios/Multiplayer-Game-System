// Interest management ("area of interest") for big rooms such as open worlds. With the room option
//   aoi: { radius: 150, keys: ['x', 'y'] }      (2 or 3 numeric player-data fields, e.g. ['x', 'y', 'z'])
// each player only receives the players within `radius` of them. Others enter and leave their view as people
// move (player.joined with aoi:true / player.left with reason 'out_of_range'), and per-player updates go only to
// those who can see the sender. Without it, every position update in an N-player room costs N sends, so
// 1000 players moving at 20 Hz would need 20 million sends per second.
// Positions sit in a uniform grid (cell = exit radius), so a lookup only checks the 3x3 (or 3x3x3) cells around.
const KEY_RE = /^[A-Za-z0-9_]{1,32}$/;
const HYSTERESIS = 1.1; // leave view at 110% of the radius so players on the edge don't flicker in and out

export function normalizeAoi(o) {
  if (o == null || o === false) return null;
  const radius = +(o && o.radius);
  const keys = Array.isArray(o.keys) ? o.keys.slice(0, 3).map(String) : ['x', 'y'];
  if (!(radius > 0 && radius < 1e9) || keys.length < 2 || keys.some((k) => !KEY_RE.test(k))) return undefined;
  return { radius, keys };
}

export function createAoi({ send, pubPlayer }) {
  const active = new Set(); // rooms with interest management on

  function enable(room, cfg) {
    room.aoi = { ...cfg, cell: cfg.radius * HYSTERESIS, r2: cfg.radius ** 2, exit2: (cfg.radius * HYSTERESIS) ** 2, grid: new Map(), dirty: new Set() };
    active.add(room);
  }
  const disable = (room) => active.delete(room);

  function posOf(a, m) {
    const p = [];
    for (const k of a.keys) { const v = m.data[k]; if (typeof v !== 'number' || !Number.isFinite(v)) return null; p.push(v); }
    return p;
  }
  /** Re-read a member's position after their data changed. Visibility is recomputed on the next sweep (<=100 ms). */
  function place(room, m) {
    const a = room.aoi, p = posOf(a, m);
    m.pos = p;
    const key = p ? p.map((v) => Math.floor(v / a.cell)).join(',') : null;
    if (key !== m.cell) {
      if (m.cell != null) { const s = a.grid.get(m.cell); if (s) { s.delete(m); if (!s.size) a.grid.delete(m.cell); } }
      if (key != null) { let s = a.grid.get(key); if (!s) a.grid.set(key, (s = new Set())); s.add(m); }
      m.cell = key;
    }
    a.dirty.add(m);
  }
  function* near(a, m) {
    const base = m.cell.split(',').map(Number), dims = base.length;
    for (let i = 0; i < 3 ** dims; i++) {
      let n = i; const k = [];
      for (let j = 0; j < dims; j++) { k.push(base[j] + (n % 3) - 1); n = Math.floor(n / 3); }
      const s = a.grid.get(k.join(','));
      if (s) yield* s;
    }
  }
  const visOf = (m) => m.vis || (m.vis = new Set());
  /**
   * Recompute who `m` sees and tell both sides about changes (visibility is symmetric).
   * quietSelf: don't message `m` itself (it's joining and gets the visible players in its snapshot instead).
   */
  function refresh(room, m, quietSelf) {
    const a = room.aoi, vis = visOf(m), next = new Set();
    a.dirty.delete(m);
    if (m.pos) {
      for (const o of near(a, m)) {
        if (o === m || !o.pos) continue;
        let d2 = 0;
        for (let i = 0; i < m.pos.length; i++) d2 += (o.pos[i] - m.pos[i]) ** 2;
        if (d2 <= (vis.has(o.id) ? a.exit2 : a.r2)) next.add(o.id);
      }
    }
    for (const id of vis) {
      if (next.has(id)) continue;
      vis.delete(id);
      const o = room.players.get(id);
      if (!o) continue;
      visOf(o).delete(m.id);
      if (o.client) send(o.client, { t: 'player.left', id: m.id, reason: 'out_of_range' });
      if (m.client && !quietSelf) send(m.client, { t: 'player.left', id, reason: 'out_of_range' });
    }
    for (const id of next) {
      if (vis.has(id)) continue;
      const o = room.players.get(id);
      if (!o) continue;
      vis.add(id); visOf(o).add(m.id);
      if (o.client) send(o.client, { t: 'player.joined', player: pubPlayer(m), aoi: true });
      if (m.client && !quietSelf) send(m.client, { t: 'player.joined', player: pubPlayer(o), aoi: true });
    }
  }
  function remove(room, m) {
    const a = room.aoi;
    if (m.cell != null) { const s = a.grid.get(m.cell); if (s) { s.delete(m); if (!s.size) a.grid.delete(m.cell); } }
    m.cell = null; m.pos = null;
    a.dirty.delete(m);
    for (const id of visOf(m)) { const o = room.players.get(id); if (o) visOf(o).delete(m.id); }
  }
  /** Members who can currently see `m`. */
  function* watchers(room, m) { for (const id of visOf(m)) { const o = room.players.get(id); if (o) yield o; } }

  setInterval(() => { for (const room of active) for (const m of [...room.aoi.dirty]) refresh(room, m); }, 100).unref();
  return { enable, disable, place, refresh, remove, watchers };
}

# Multiplayer integration guide (for AI coding agents)

You are adding online multiplayer to a game using a **self-hosted, general-purpose multiplayer server**. The server handles connections, rooms, matchmaking, realtime sync, persistent storage, leaderboards and player accounts. **All game rules live in the game's own code.** The server is game-agnostic.

## 0. Configuration (ask the user if these are missing)

| Value | Example | Where it comes from |
|---|---|---|
| `SERVER_URL` | `https://mp.example.com` | The user's server. |
| `GAME_KEY` | `pk_3hF...` | Dashboard → Games → (game) → Game key. It's a public client key, so it can ship in client code. |

If you don't have both values, **ask the user for them**. Don't invent them. Keep them in one config constant at the top of the game, like `const MP = { url: '...', key: '...' }`.

## 1. Load the SDK

**Browser (any framework):**
```html
<script src="SERVER_URL/sdk.js"></script>   <!-- defines global `Multiplayer` -->
```
In a bundler or ES module project, either keep the `<script>` tag in `index.html`, or copy `SERVER_URL/sdk.js` into the project (for example `src/lib/multiplayer.js`) and `import './lib/multiplayer.js'`. The file sets `globalThis.Multiplayer` and also supports `module.exports`.

**Node (bots, tests, server-side tools):** Node ≥ 22 has a global `WebSocket`. Use `const Multiplayer = require('./multiplayer.js')`.

**Other engines (Unity, Godot, native):** see the wire protocol in §8. It's plain JSON over WebSocket.

## 2. Minimal working example

```js
const mp = new Multiplayer({ url: SERVER_URL, key: GAME_KEY, name: 'Player' });

mp.on('playerJoined', p => console.log(p.name, 'joined'));
mp.on('playerLeft',   p => console.log(p.name, 'left'));
mp.on('state',        state => render(state));           // shared room state changed
mp.onMessage('chat',  (data, fromId) => showChat(fromId, data.text));

await mp.connect();                       // → { playerId, name, username, returning }
await mp.quickJoin('default', { maxPlayers: 4 });   // matchmaking: join or create a room
// or: await mp.create({ maxPlayers: 4 })  → mp.room.id is a 6-char code to share
// or: await mp.join('ABC123')             → join (or create) a room by id

mp.setPlayer({ x: 10, y: 20 });           // my per-player data, broadcast to the room
await mp.setState('turn', mp.playerId);   // shared room state, ordered by the server
mp.send('chat', { text: 'gg' });          // one-off event to everyone else
```

## 3. Mental model

```
Game (one key) ─┬─ Rooms (id/code, up to maxPlayers, one host)
                │    ├─ room.state     shared JSON tree. Every write is ordered by the server and sent to all clients, the writer included.
                │    ├─ player.data    each player's own JSON. The owner writes it, it's applied locally right away and broadcast to others.
                │    └─ messages       transient events: to everyone, to the host, or to specific players.
                ├─ Storage (kv)      persistent: per-player, private, and game-wide
                ├─ Leaderboards      persistent scores with ranks
                └─ Accounts          optional username/password, to keep the same player across devices
```

- A client is in **at most one room** at a time. Joining another room leaves the current one.
- **Host:** the earliest-joined connected player. When the host leaves or drops, the server migrates the host automatically (`host` event). Use the host as the referee for authoritative logic.
- **Reconnects are automatic.** A player who drops keeps their slot for 15 s. Others see `playerConnection(p, false)`. The SDK reconnects, rejoins the room and gets a fresh snapshot (`reconnected` event).
- **Identity:** a signed token in `sessionStorage` (`identity: 'tab'`, the default, so each tab is a separate player and you can test with two tabs), or in `localStorage` (`identity: 'device'`, so the same player persists across sessions). Use `mp.auth` for cross-device identity.

## 4. Choose a sync architecture

Pick based on the game type and say which one you chose:

| Game type | Pattern |
|---|---|
| Turn-based, board, card, party, quiz, puzzle | **Shared state.** Put the whole game in `room.state` and write with `setState`/`batchState`. Optionally set `stateWrite: 'host'` and have clients *request* moves from the host. |
| Real-time action with rules that matter (scoring, damage, pickups, win conditions, competitive) | **Server script (preferred).** Put the rules in a server script (§5b). Clients send intents with `mp.send(type, data, 'server')`. The script validates them and writes `room.state`. That's **1 round trip**. Movement stays client-side in `setPlayer` and is rendered with `mp.predict()`. |
| Same, but you can't install a server script | **Host-authoritative.** Clients send intents with `mp.send(type, data, 'host')`, and the host validates them and writes `room.state` (`stateWrite: 'host'`). This costs **2 round trips** (client → server → host → server → everyone), so mask it with local prediction (§6a). |
| Casual real-time (cursors, movement, co-op sandbox) | **Client-authoritative.** Each client owns its avatar via `setPlayer({x,y,...})` at 10–20 Hz. Interpolate remote players. Use `send` for events. |

Authoritative logic runs either in a **server script** (§5b, fastest and cheat-resistant) or on the **host client**. With the host pattern: Host migration is automatic, so any client must be able to take over as host from `room.state` alone. **Keep all authoritative data in `room.state`, not in host-local variables.**

## 5. API reference

### Constructor
```js
new Multiplayer({
  url, key,                 // required
  name: 'Bob',              // display name (can change later with setName)
  identity: 'tab',          // 'tab' | 'device' | 'none'
  autoReconnect: true,
  timeout: 10000,           // request timeout (ms)
  WebSocket: undefined,     // custom implementation if needed
})
```

### Connection
| Call | Notes |
|---|---|
| `await mp.connect()` | Resolves `{playerId, name, username, returning}`. Rejects on a bad key, a disabled game or an origin that isn't allowed. |
| `await mp.connect({ quickJoin: 'tag', options })` / `{ room: 'ID', options }` / `{ create: true, options }` | Connects **and** joins in one round trip (the join is pipelined behind the handshake). Prefer this over `connect()` followed by `join()`. |
| `mp.disconnect()` | Intentional close, no auto-reconnect. |
| `mp.connected`, `mp.playerId`, `mp.name`, `mp.username`, `mp.latency` | |
| `mp.serverTime()` | Server-synced clock in ms. Use it for countdowns or round timers all clients agree on, e.g. store `endsAt: mp.serverTime() + 60000` in state. |
| `await mp.ping()` | Returns RTT in ms. Runs automatically every 10 s. |

### Rooms
| Call | Notes |
|---|---|
| `await mp.quickJoin(tag, options)` | Joins the fullest public, unlocked, non-full, password-less room with that `tag`, or creates one. |
| `await mp.create(options)` | New room with a random 6-char code (`mp.room.id`). |
| `await mp.join(roomId, options)` | Joins the room, or creates it with `options` if it doesn't exist. Pass `{mustExist: true}` to fail instead (error `room_not_found`). |
| `await mp.leave()` | |
| `await mp.listRooms(tag?)` | Public rooms: `[{id, players, maxPlayers, meta, locked, tag, hasPassword, ...}]`. |
| `await mp.updateRoom({locked, private, maxPlayers, meta, stateWrite})` | **Host only.** For example, `locked: true` when the match starts. |
| `await mp.kick(playerId, ban=false)` / `await mp.transferHost(playerId)` | Host only. |
| `mp.room` | `{id, host, meta, maxPlayers, locked, private, tag, stateWrite, hasPassword, created}` or `null`. |
| `mp.players` (Map id→player), `mp.playerList` (array), `mp.me`, `mp.isHost` | player = `{id, name, data, connected, joined, username}`. |

**Room options** (used when the room is created): `maxPlayers` (default 16), `private` (hidden from list and quickJoin), `password`, `tag`, `meta` (public object such as map or mode), `persist` (room state is saved when the room empties and restored when a room with the same id is created again, which suits persistent worlds), `stateWrite: 'all' | 'host'`, `state` (initial state), `playerData` (initial data for the joining player).

### Shared room state
| Call | Notes |
|---|---|
| `await mp.setState('a.b.c', value)` | Dot-path write. Creates intermediate objects. |
| `await mp.deleteState('a.b')` | |
| `await mp.batchState([[path, value], [path, null, 1 /*delete*/]])` or `batchState({path: value})` | Atomic. Everyone receives it as one update. |
| `mp.state`, `mp.getState('a.b')` | **Read-only locally.** Never mutate `mp.state` directly. Always write through the calls above. |

**Optimistic by default.** Your own writes apply to `mp.state` immediately (a `state` event fires with `{optimistic: true}`). Other clients get them about half an RTT later. The server still decides the order: if someone else's write lands first, your copy is reconciled automatically. If the write is rejected (`not_host`, or the room is server-authoritative), the promise rejects and `mp.state` rolls back to the server's version (`mp.resync()`). Turn it off with `new Multiplayer({ optimistic: false })`. For high-frequency data (60 Hz positions) use `setPlayer`, not `setState`.

### Per-player data
`mp.setPlayer({x, y, hp: null /*null deletes*/}, {replace=false})` is fire-and-forget. It's applied to `mp.me.data` immediately and merged into `player.data` on other clients. Send at 20–30 Hz (at most 60 messages/s total per client), and only changed fields.

`mp.predict(playerId, keys = ['x','y'], {maxAheadMs: 250})` does dead reckoning for **remote** players. It returns `{x, y}` extrapolated to "now" from the velocity between their last two updates plus half your ping. Render remote players at `predict()` (lerp toward it for smoothness), not at raw `p.data`. Raw data is always about one-way latency + send interval old.

### Messages
| Call | Notes |
|---|---|
| `mp.send(type, data, to?, {self})` | Fire-and-forget. `to`: omitted for everyone except me, `'all'`, `'host'`, `'server'` (the game's server script, §5b), a playerId, or an array of ids. Pass `{self:true}` with `'all'` to receive your own message too. |
| `await mp.sendReliable(...)` | Same arguments, but resolves when the server accepts it. |
| `mp.onMessage(type, (data, fromId, raw) => {})` | `fromId === 'server'` for admin broadcasts (type `announcement` by default). |
| Outside a room | `mp.send('invite', {room: 'ABC123'}, playerId)` sends a direct message to any online player of the game. |

### Persistent storage (key/value, JSON values ≤ 64 KB)
```js
await mp.storage.set('save', {level: 3, coins: 120});            // scope 'player' (default): I write, anyone can read
await mp.storage.get('save');                                    // mine
await mp.storage.get('save', {player: otherId});                 // someone else's public player data
await mp.storage.set('inventory', [...], {scope: 'private'});    // only I can read/write
await mp.storage.get('config', {scope: 'game'});                 // game-wide (admin can make it read-only in dashboard)
await mp.storage.incr('totalMatches', 1, {scope: 'game'});       // atomic counter → new value
await mp.storage.list({scope: 'player', prefix: 'slot_'});       // [{key, value, updated}]
await mp.storage.delete('save');
```
`get` returns `null` when a key is missing. Player storage is tied to the player identity, so use `identity: 'device'` or accounts if saves must survive a closed tab.

### Leaderboards
```js
await mp.leaderboard.submit('highscore', 1234);                  // mode 'max' (default) keeps the best
await mp.leaderboard.submit('fastest', 41.2, {mode: 'min'});     // 'min' | 'max' | 'sum' | 'set'
const {entries, me} = await mp.leaderboard.top('highscore', {limit: 10});  // order: 'desc' (default) | 'asc'
// entries: [{rank, id, name, score}], me: {rank, score} | null
```
Use `{order: 'asc'}` with `top()` for boards submitted with `mode: 'min'`.

### Accounts (optional)
```js
await mp.auth.register('alice', 'password123');  // attaches the current player (and its storage) to the account
await mp.auth.login('alice', 'password123');     // switches this connection to that account's playerId
await mp.auth.logout();                          // forgets token, reconnects as a new guest
mp.username                                      // null for guests
```
Logging in leaves the current room. Rejoin afterwards. A login kicks any other active session of the same account.

### Misc
`await mp.setName('Bob')` renames the player and broadcasts the change to the room.

### Events (`mp.on(event, fn)` returns an unsubscribe function, `mp.once`, `mp.off`)
| Event | Args |
|---|---|
| `connected` | `{playerId, name, username, returning}` |
| `disconnected` | `{code, reason}` |
| `reconnecting` | `{attempt, delay}` |
| `reconnected` | `{room}`. The room snapshot has already been re-applied. |
| `joined` | `room`. Fires after create, join, quickJoin and rejoin. |
| `left` | `(room, reason)`. reason: `left`, `kicked`, `closed_by_admin`, `replaced`, ... |
| `kicked` | Kicked from the room by the host (fires before `left`), or kicked/banned from the whole game by the admin (`reason` is `kicked` or `banned`; the connection closes and does not auto-reconnect). |
| `playerJoined` / `playerLeft` | `(player)` / `(player, reason)` |
| `playerUpdated` | `(player, changedData)`. Also fires for your own `setPlayer`. |
| `playerConnection` | `(player, connected)`. Temporary drop or return. |
| `host` | `(hostId, iAmHost)` |
| `roomUpdated` | `room` |
| `state` | `(state, {ops, by})`. Any state change, including the initial snapshot. |
| `state:<topKey>` | `(valueAtTopKey, {path, value, by})`, e.g. `mp.on('state:board', ...)` |
| `message` | `{type, data, from, direct}`. Every message. |
| `msg:<type>` | Same as `onMessage(type, ...)`. |
| `error` | `MultiplayerError {code, message}`. Async errors such as `rate_limited` or `replaced`. |
| `ping` | `latency` |

Request errors reject with `MultiplayerError` (`err.code`): `not_connected`, `timeout`, `not_in_room`, `not_host`, `room_full`, `room_locked`, `room_not_found`, `bad_password`, `banned`, `bad_path`, `bad_key`, `bad_scope`, `forbidden`, `too_large`, `username_taken`, `bad_credentials`, `already_registered`, `player_offline`, `no_server_script`.

## 5b. Server scripts (authoritative logic on the server)

Each game can have **one server script**: a CommonJS module that runs on the server for every room of that game. The user installs it in the dashboard under **Game → Server logic → Save & deploy**, or through the admin API: `PUT SERVER_URL/api/admin/games/GAME_ID/script` with `{"code": "..."}` and `Authorization: Bearer ADMIN_TOKEN`. The admin token is the user's secret. Never put it in client code. If you write a server script, **save it in the project as `server-script.js` and tell the user to paste it into Server logic**.

```js
module.exports = {
  authoritative: true,   // clients can't setState in this game's rooms; only the script writes state
  tickRate: 20,          // onTick calls per second (1–60). Omit for no tick loop.

  onCreate(room) {},                        // new room; initialize room.state here
  onJoin(room, player) {},                  // player = {id, name, data, connected, joined, username}
  onLeave(room, player, reason) {},
  onMessage(room, player, type, data) {},   // from mp.send(type, data, 'server')
  onTick(room, dt) {},                      // dt in seconds
};
```

| `room` API (inside the script) | |
|---|---|
| `room.id`, `room.meta`, `room.host`, `room.players` (array), `room.player(id)` | Read-only views. |
| `room.state` / `room.getState('a.b')` | Read. |
| `room.setState(path, value)`, `room.deleteState(path)`, `room.batchState([[path, value], [path, null, 1]])` | All writes in one hook call are sent to clients as **one** batched update. |
| `room.send(type, data, to?)` | `to`: omitted for everyone, a playerId, or an array. Clients receive it with `from === 'server'`. |
| `room.kick(id, reason?)`, `room.lock(true/false)`, `room.now()` | |
| `room.data` | Server-only scratch object, never sent to clients. Use it for secrets, RNG seeds and AI state. |
| `console.log(...)` | Shown in the dashboard activity feed. |

Rules: plain synchronous JS with no `require`, network, filesystem or timers (use `onTick`). Each hook call is limited to **50 ms of CPU**. A hook that throws or loops forever is stopped, logged under Server logic → errors, and the server keeps running. Validate everything in `onMessage`, because `data` comes from clients.

## 5c. Latency: what to expect and how to minimise it

Every message pays the network round trip between the player and the server (RTT, shown as `mp.latency`). The server adds about 1 ms. Design for this:

| Path | Cost |
|---|---|
| Your own `setPlayer` / `setState` (optimistic) | **0 ms** for you |
| You → other players (`setPlayer`, `send`, `setState`) | about **½ RTT** your side + ½ RTT theirs |
| Intent → server script → everyone sees the result | **1 RTT** |
| Intent → host client → everyone sees the result | **2 RTT**. Avoid it for fast-paced games. |

Techniques, all expected for action games:
1. **Client-side prediction.** Apply your own actions locally the moment the input happens (move, shoot animation, sound), then reconcile when the authoritative state arrives.
2. **Dead reckoning.** Render remote players at `mp.predict(id)`, not raw data.
3. **Server scripts** for authority instead of the host (1 RTT instead of 2).
4. **Send rate.** Use `setPlayer` at 20–30 Hz with small payloads (round numbers and short keys). Don't send unchanged data.
5. **One-RTT start.** Use `connect({ quickJoin | room | create })`.
6. **Shared clock.** Use `mp.serverTime()` for anything timed, such as countdowns, cooldowns and projectile spawn times, so clients agree regardless of latency.

## 6. Recipes

**Lobby with share codes + ready-up + synchronized start**
```js
const room = await mp.create({ maxPlayers: 4, meta: { mode: 'classic' } });
showCode(room.id);                                    // others: await mp.join(code, { mustExist: true })
readyBtn.onclick = () => mp.setPlayer({ ready: true });
mp.on('playerUpdated', () => {
  if (mp.isHost && mp.playerList.length > 1 && mp.playerList.every(p => p.data.ready))
    mp.updateRoom({ locked: true }).then(() => mp.setState('match', { phase: 'countdown', startAt: mp.serverTime() + 3000 }));
});
mp.on('state:match', m => m?.phase === 'countdown' && startAt(m.startAt));   // all clients use mp.serverTime()
```

**Turn-based (host validates moves)**
```js
await mp.join(code, { stateWrite: 'host', maxPlayers: 2 });
// everyone: request a move
mp.send('move', { cell: 4 }, 'host', { self: true });
// host: validate + apply
mp.onMessage('move', (mv, from) => {
  if (!mp.isHost) return;
  const s = mp.state;
  if (s.turn !== from || s.board[mv.cell]) return;               // reject illegal moves
  mp.batchState([[`board.${mv.cell}`, from], ['turn', nextPlayer(from)]]);
});
// host initializes when the room is new: if (mp.isHost && !mp.state.board) mp.setState('board', Array(9).fill(null));
```

**Real-time movement with smooth remote players**
```js
let last = 0;
function update(t) {
  moveLocalPlayer();                                              // client-side prediction: move immediately
  if (t - last > 50) { last = t; mp.setPlayer({ x: me.x, y: me.y, a: me.angle }); }  // 20 Hz
  for (const p of mp.playerList) if (p.id !== mp.playerId) {
    const v = view[p.id] ||= { x: p.data.x, y: p.data.y };
    v.x += (p.data.x - v.x) * 0.2; v.y += (p.data.y - v.y) * 0.2;  // interpolate toward the latest network position
  }
}
```

**Server-authoritative combat (fastest authoritative pattern)**
```js
// client: predict locally, ask the server to confirm
fireBtn.onclick = () => { playShotFx(); mp.send('shoot', { target: aimedId }, 'server'); };
mp.on('state:hp', hp => renderHealthBars(hp));
// server-script.js (user pastes it into Dashboard → Server logic)
module.exports = {
  authoritative: true,
  onJoin(room, p) { room.setState('hp.' + p.id, 100); },
  onMessage(room, p, type, d) {
    if (type !== 'shoot') return;
    const now = room.now(), last = room.data['cd_' + p.id] || 0;
    if (now - last < 250 || room.getState('hp.' + d.target) == null) return;   // cooldown + validation
    room.data['cd_' + p.id] = now;
    room.setState('hp.' + d.target, Math.max(0, room.getState('hp.' + d.target) - 10));
  },
};
```

**Host-authoritative world (no server script available)**: the host runs the simulation loop (`if (mp.isHost) tick()`) and writes compact results with `batchState` at 5–15 Hz. Clients send intents (`mp.send('shoot', {...}, 'host')`) and render `mp.state`. Because the whole world lives in `room.state`, a migrated host continues seamlessly (`mp.on('host', (_, me) => me && startSimLoop())`).

**Save/load progress**: `mp.storage.set('save', data)` on checkpoints and `const save = await mp.storage.get('save')` after `connect()`. Use `identity: 'device'` or accounts.

**Persistent shared world**: `mp.join('world-1', { persist: true, maxPlayers: 50 })`. State survives everyone leaving and server restarts.

**Spectators**: join with `playerData: { spectator: true }` and filter them out of gameplay.

## 7. Integration checklist (do all of these)

1. Put `SERVER_URL` and `GAME_KEY` in one config spot. Load the SDK.
2. Call `connect()` once at startup and show a "connecting…" state. Catch errors and show them.
3. Build a minimal lobby UI: **Quick play** (`quickJoin`), **Create room** (shows the code), **Join by code**. Show the player list with names and connection status.
4. Pick a sync pattern from §4 and say which one in a code comment.
5. Handle `playerJoined`, `playerLeft`, `playerConnection`, `host`, `reconnecting` / `reconnected`, `left` (kicked or closed) and `error`.
6. Throttle `setPlayer` to 20–30 Hz. Batch state writes. Never mutate `mp.state` directly. Use `connect({quickJoin|room|create})`.
6b. For action games: predict local actions, render remote players with `mp.predict()`, and put authoritative rules in a server script (`server-script.js` for the user to paste into the dashboard) instead of on the host.
7. Escape player-supplied text (names, chat) before inserting it as HTML.
8. Make sure single-player still works if the game had it, or that the game degrades gracefully when the server is unreachable.
9. Test with two browser tabs. The default `identity: 'tab'` makes them different players.

## 8. Wire protocol (for non-JS engines)

WebSocket `wss://HOST/ws`. Messages are text frames of JSON objects with a type field `t`.

1. On open, send `{"t":"hello","key":"GAME_KEY","token":"<saved token or omit>","name":"Bob"}`.
2. The server replies `{"t":"welcome","playerId","token","name","username","serverTime","game":{id,name},"returning"}`. **Persist `token`** and send it on the next hello to stay the same player.
3. Requests are `{"t":"<type>","rid":<int>, ...fields}`. You may send a `join`/`quick`/`create` right after `hello` without waiting for `welcome`. Messages are processed in order, which saves a round trip. The response is `{"t":"res","rid":<int>,"ok":true,"d":<data>}` or `{"t":"res","rid","ok":false,"err":{"code","message"}}`. Omit `rid` for fire-and-forget.

| Request `t` | Fields | Response `d` |
|---|---|---|
| `ping` | – | `{s: serverTimeMs}` |
| `name` | `name` | `{name}` |
| `rooms` | `tag?` | room list |
| `join` | `room`, `options?` | room snapshot `{id, host, meta, maxPlayers, locked, private, tag, stateWrite, hasPassword, created, players:[...], state}` |
| `create` | `options?` | snapshot |
| `quick` | `tag`, `options?` | snapshot |
| `leave` | – | `true` |
| `room.update` | `locked? private? maxPlayers? meta? stateWrite? host?` | room info |
| `kick` | `id, ban?` | `true` |
| `state.set` | `ops: [[path, value], [path, null, 1]]` | `true` |
| `player.set` | `data, replace?` | `true` |
| `send` | `type, data, to?, self?` (`to: "server"` delivers to the game's server script) | `true` |
| `kv.get` / `kv.set` / `kv.del` / `kv.incr` / `kv.list` | `key, value?, by?, scope?, player?, prefix?, limit?` | value / `true` / number / list |
| `lb.submit` | `board, score, mode?` | `{score, rank, improved}` |
| `lb.top` | `board, limit?, order?` | `{board, entries, me}` |
| `auth.register` / `auth.login` | `username, password` | `{playerId, token, username, name}` |

Server pushes:
- `{t:"state", ops, by, full?}`: apply ops in order. `[path, value]` sets and `[path, null, 1]` deletes. `full` replaces the whole state.
- `{t:"player", id, data?, name?, replace?}`: merge `data` into that player. A `null` value deletes the key.
- `{t:"player.joined", player}`, `{t:"player.left", id, reason}`, `{t:"player.connection", id, connected}`
- `{t:"host", id}`, `{t:"room", room}`, `{t:"left", room, reason}`
- `{t:"msg", type, data, from, direct?}`
- `{t:"error", code, message}`

Close codes: `4001` replaced by another session, `4003` bad key or origin, `4004` game disabled or deleted, `4005` kicked or banned by admin, `4008` hello timeout, `4029` rate limited. Don't auto-reconnect on 4001, 4003, 4004 or 4005. On other closes, reconnect with backoff, send hello with the saved token, then `join` the same room id within 15 s to keep the slot.

## 9. Limits

- 64 KB per message and per stored value. Up to 256 state ops per batch.
- About 60 messages/s sustained per connection (burst 200). Excess messages are dropped with an `error` `rate_limited` event.
- One room per connection. Rooms are in memory and disappear when empty unless `persist: true`.
- Without a server script the server trusts clients. For competitive integrity, use a server script with `authoritative: true` and validate in `onMessage`. Leaderboard submissions are client-reported.
- Server scripts get 50 ms of CPU per hook call and up to 60 Hz ticks.

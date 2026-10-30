# Multiplayer Game System

A self-hosted, game-agnostic multiplayer backend. One Docker stack on your VPS gives you:

- **Public servers and open worlds**: a server browser of permanent, admin-defined servers (name, region, tags, MOTD, password, whitelist, join queue, saved state), rooms of up to 5000 players, and interest management so each player only receives the players near them (1000 players in one room measured with a 0 ms median ping).
- **Setup API for coding agents**: each game has a secret setup key. The dashboard's integration prompt tells Claude to work out the game's needs, call `POST /api/setup/recommend` with a profile (players, update rate, latency target, data model) and apply the result with `PUT /api/setup`: room types, limits, servers, leaderboards, storage rules and the server script. The game's **Setup** tab then shows the expected numbers next to the measured ones (average and peak players, players per room, update rate, tick, ping, traffic).
- **Customisable per game**: limits (room size, room count, rate limits, reconnect grace), named room types that clients can't override, room defaults, and anything else via server scripts. All of it is edited in the dashboard.
- **Realtime rooms**: share codes, matchmaking (`quickJoin`), host election and migration, kick/ban, locking, passwords, reconnects that keep the player's slot.
- **Three sync channels**: ordered shared room state, per-player data, and relay messages (to everyone, the host, or specific players, plus DMs).
- **Server scripts**: optional authoritative game logic per game, edited in the dashboard. Each hook call has a CPU limit, and a script that crashes or loops forever is stopped without taking the server down. An action costs 1 round trip instead of 2 through a host client.
- **Best-link transports**: clients connect over WebSocket, then measure WebRTC data channels (browsers) and raw UDP (native/Node) in the background and move all traffic to the fastest link. If that link degrades or dies they switch again, with nothing lost or reordered. There's also an unreliable fast lane for movement.
- **Low latency**: optimistic state, dead-reckoning `predict()`, connect and join in one round trip, per-player ping in the dashboard, and kernel tuning (`tune-network.sh`).
- **Storage (SQLite)**: per-player, private and game-wide JSON storage with versions (compare-and-set), atomic update ops (incr / push / merge / max ...), batches, TTL expiry, queries across all players, quotas, and an optional "server script only" write lock. Persistent rooms too.
- **Leaderboards**: configurable boards (max / min / sum / last, asc / desc, score bounds), automatic daily / weekly / monthly resets with last period's results, per-score metadata, around-me and friends views, and server-only boards for cheat-proof rankings.
- **Social and meta-game**: friends (requests, blocking), live presence, player lookup, a persistent inbox for offline players (gifts, invites, rewards), rating-based matchmaking queues, game-wide chat channels with history, and remote config pushed live from the dashboard. Server scripts can use storage, leaderboards and the inbox directly.
- **Accounts**: optional username/password accounts that keep the same player across devices.
- **Admin**: one-click shut down / start per game, kick or ban any online player, plus a dashboard (static HTML that runs on GitHub Pages or from the server itself) with live traffic, games, rooms, players, storage browser, leaderboards and broadcasts.
- **Security**: per-game public keys, an admin bearer token, HMAC-signed player tokens, scrypt passwords, rate limits, message size caps, origin allow-lists, auto-HTTPS via Caddy.

![Dashboard overview: live players, rooms, traffic and games](docs/images/dashboard.jpg)

| Game detail: key, Claude prompt, live rooms | Room inspector: players, data, live state |
|---|---|
| ![Game detail](docs/images/game-detail.jpg) | ![Room inspector](docs/images/room-inspector.jpg) |

**Demo game** (`/demo`): five players in one room. The host spawns coins and validates every pickup.

![Coin Arena demo](docs/images/demo.jpg)

```
server/         Node 24 server (ws + node:sqlite), Dockerfile, smoke test
client/         multiplayer.js: browser/Node SDK (served at /sdk.js)
docs/           Dashboard (GitHub Pages root, also served at /)
examples/       demo.html: Coin Arena demo (served at /demo)
MULTIPLAYER.md  Integration guide to hand to any Claude chat
```

## Deploy on the Hostinger KVM 2

```bash
ssh root@YOUR_VPS_IP
git clone <your-repo-url> mp && cd mp       # or scp this folder up
sh deploy.sh
```

`deploy.sh` installs Docker if it's missing and creates `.env` with a random `ADMIN_TOKEN`. It sets `DOMAIN=<ip-with-dashes>.sslip.io`, a free hostname that resolves to your IP, so HTTPS works without buying a domain. It also records the VPS's public IP as `PUBLIC_IP` (WebRTC advertises it to browsers). It then opens ports 80, 443, 8081/udp and 8082/udp in ufw and runs `docker compose up -d --build`. To use your own domain, point an A record at the VPS, set `DOMAIN=` in `.env`, and run `docker compose up -d`.

Make sure ports 80 and 443 (TCP) and 8081 and 8082 (UDP) are open in the Hostinger panel firewall too. UDP goes straight to the container, not through Caddy/Traefik. If those UDP ports are blocked, everything still works over WebSocket.

| URL | What it is |
|---|---|
| `https://DOMAIN/` | Dashboard |
| `https://DOMAIN/demo` | Demo game. Open it from a game's page in the dashboard so the key is filled in. |
| `https://DOMAIN/sdk.js` | Client SDK |
| `https://DOMAIN/MULTIPLAYER.md` | Integration guide |
| `wss://DOMAIN/ws` | WebSocket endpoint (signalling + fallback) |
| `udp://DOMAIN:8081` | Raw UDP link for native/Node clients |
| `udp://DOMAIN:8082` | WebRTC data channels (all peers multiplexed on one port) |
| `https://DOMAIN/health` | Health check |

Operations:

```bash
docker compose logs -f mp                 # logs
docker compose up -d --build              # update after git pull
docker compose exec mp cat /data/mp.db > backup.db   # or back up the mp-data volume
```

## Moving to another server (e.g. closer to your players)

Latency is mostly distance plus routing, so host where your players are. Measured from Perth: Sydney ~51 ms, Melbourne ~59 ms, Singapore ~179 ms, Kuala Lumpur ~182 ms. For players all over Australia, **Sydney** is the best single location. Any Linux VPS with a public IPv4 works (x64 or arm64, 1 vCPU / 1 GB is plenty to start), with Ubuntu 22.04 or 24.04.

1. Create the VPS in the provider's panel. In its firewall, allow TCP 22, 80 and 443, and UDP 443, 8081 and 8082.
2. On the new server, let it SSH into the old one:
   `ssh-keygen -t ed25519 && ssh-copy-id root@OLD_IP`
3. On the new server, run:
   ```bash
   git clone <your-repo-url> mp && cd mp
   sh migrate.sh root@OLD_IP
   ```
   This installs Docker, then copies the database and the admin token. Game keys, player identities, accounts, storage and leaderboards all carry over. The old server is left stopped; the script prints a rollback command.
4. Point clients at the new server:
   - **Real domain:** change its A record to the new IP, set `DOMAIN=` in `.env`, then run `docker compose up -d`. Nothing changes for players.
   - **sslip.io name:** the URL changes with the IP. Update `SERVER_URL` in your games and in the dashboard login.
5. Run `sudo sh tune-network.sh`.

## Latency tuning

```bash
sudo sh tune-network.sh      # BBR + fq, low TCP send buffering, no slow-start after idle (persists across reboots)
```

The biggest factor is the network path between players and the VPS. Check the **Ping / link** column in the dashboard; it also shows which link each player ended up on. If players are far from the server or their ISP routes badly, put the domain behind **Cloudflare** (proxied DNS record; WebSockets work on the free plan). Cloudflare doesn't proxy raw UDP, so set `UDP_HOST` to a DNS-only record (or the IP) when you do this. Cloudflare's backbone often beats ISP routing. Measured from Perth to the Kuala Lumpur VPS: **314 ms direct vs 239 ms through Cloudflare**. Hosting the VPS in the same region as your players matters most of all.

## Dashboard on GitHub Pages

Push the repo, then go to **Settings → Pages → Deploy from branch → `main` / `/docs`**. Open the Pages URL and enter your server URL and admin token. They're stored only in that browser's localStorage. The same dashboard is also served from the VPS at `/`.

## Adding multiplayer to a game

1. In the dashboard, click **+ New game** to get a game key.
2. Click **Copy integration prompt** and paste it into any Claude chat or Claude Code session, along with `MULTIPLAYER.md`. Claude can also fetch the guide from `https://DOMAIN/MULTIPLAYER.md`.

## Local development

```bash
cd server && npm install && npm start     # http://localhost:8080, admin token printed on first run
npm test                                  # end-to-end smoke test (106 checks: rooms, setup API, public servers, open-world interest management, room types, storage, leaderboards, social, inbox, matchmaking, chat, UDP/WebRTC, packet loss, failover)
```

## Configuration (`.env`)

| Var | Default | |
|---|---|---|
| `DOMAIN` | – | Hostname for Caddy TLS |
| `ADMIN_TOKEN` | auto-generated, printed in logs | Dashboard and admin API bearer token (≥16 chars) |
| `OPEN_GAMES` | `false` | When `true`, any unknown key ≥8 chars auto-creates a game. Dev convenience only. |
| `RATE_LIMIT` / `RATE_BURST` | `60` / `200` | Messages per second per connection |
| `MAX_MSG_BYTES` | `65536` | Maximum size per message and per stored value |
| `RECONNECT_GRACE_MS` | `15000` | How long a dropped player keeps their room slot |
| `MAX_CONNS_PER_IP` | `50` | |
| `PUBLIC_IP` | – | Public IPv4 advertised in WebRTC candidates. Required in Docker for browsers to reach WebRTC. |
| `UDP_PORT` | `8081` | Raw UDP transport, `0` = off |
| `RTC_PORT` | `0` (random ports) | WebRTC on one multiplexed UDP port (compose sets `8082`) |
| `WEBRTC` | `true` | `false` turns off WebRTC |
| `UDP_HOST` | host the client connected to | Host clients send raw UDP to |
| `ICE_SERVERS` | – | Comma-separated STUN/TURN URLs for the server's WebRTC peer |
| `STORAGE_MAX_KEYS` | `1000` | Keys per player per scope (the game scope gets 10×) |
| `MAX_ROOM_PLAYERS` / `HARD_MAX_ROOM_PLAYERS` | `1000` / `5000` | Default room-size cap / the most any game may raise it to |
| `MAX_ROOMS_PER_GAME` | `10000` | Open rooms per game (games can lower it) |

## Setup API (per game, for coding agents)

`Authorization: Bearer SETUP_KEY` (dashboard → game → Setup). The key only reaches that game's configuration, never other games, players or the admin API. `GET /api/setup` · `POST /api/setup/recommend` (`{profile}`) · `PUT /api/setup` (`?dryRun=1`) · `GET|POST /api/setup/messages` (the setup chat with the dashboard; `?wait=60` long-polls for your reply) · `POST /api/setup/done` (`{summary}`: closes the setup API, after which settings change only in the dashboard). See MULTIPLAYER.md §0. Rotate the key in the dashboard to revoke it; **Re-open setup** issues a new key after done.

Admin equivalents: `GET|PUT /api/admin/games/:id/setup`, `GET|POST /api/admin/games/:id/setup/messages` (your replies), `POST /api/admin/games/:id/setup/reopen`.

## Admin REST API

All routes take `Authorization: Bearer ADMIN_TOKEN`.

`GET /api/admin/overview` (includes online players) · `POST /api/admin/games/:id/players/:pid/kick` (`{ban, reason}`) · `POST /api/admin/games/:id/players/:pid/unban` · `GET /api/admin/games/:id/leaderboards` · `GET|DELETE /api/admin/games/:id/leaderboards/:board` (`?period=day&previous=1`; DELETE resets every period, `?config=delete` also drops the config) · `GET|PUT /api/admin/games/:id/leaderboards/:board/config` (`{mode, sort, periods, clientWrite, min, max}`) · `POST /api/admin/games/:id/leaderboards/:board/submit` (`{player, score, meta}`) · `DELETE /api/admin/games/:id/leaderboards/:board/players/:pid` · `GET|PUT /api/admin/games/:id/setup` (`?dryRun=1`) · `POST /api/admin/games/:id/setup/recommend` · `GET|POST /api/admin/games/:id/servers` · `GET|PATCH|DELETE /api/admin/games/:id/servers/:sid` (`{name, description, region, tags, maxPlayers, password, visibility, whitelist, queue, motd, type, options, enabled}`) · `POST /api/admin/games/:id/inbox` (`{to: id | [ids], type, data}`) · `GET|POST /api/admin/games` · `GET|PATCH|DELETE /api/admin/games/:id` (PATCH accepts `{name, enabled, rotateKey, rotateSetupKey, settings:{allowedOrigins, gameDataWrite, playerDataWrite, lockLeaderboards, inboxClientSend, config, limits, roomTypes, roomDefaults, requireRoomType}}`; changing `config` pushes it to every connected client) · `POST /api/admin/games/:id/rooms/:room/close|kick|state` · `POST /api/admin/games/:id/broadcast` · `GET|PUT|DELETE /api/admin/games/:id/kv` · `GET|DELETE /api/admin/games/:id/leaderboards/:board`

#!/usr/bin/env sh
# Move a running install to THIS server (e.g. from Kuala Lumpur to Sydney). Run from the repo dir on the NEW server:
#
#   sh migrate.sh root@OLD_SERVER_IP [repo dir on the old server, default: mp]
#
# Copies the whole database (games + keys, players, accounts, storage, leaderboards, persistent rooms, server
# scripts) and the admin token, so game keys and saved player identities keep working. The old server's game
# service is left STOPPED so nothing is written there after the copy (restart: docker compose start mp).
# Needs SSH access from this server to the old one (ssh-copy-id root@OLD_SERVER_IP, or an agent-forwarded key).
set -e
OLD=$1; DIR=${2:-mp}
[ -n "$OLD" ] || { echo "usage: sh migrate.sh root@OLD_SERVER_IP [repo dir on old server, default: mp]"; exit 1; }
ssh "$OLD" "test -f $DIR/.env" || { echo "No $DIR/.env on $OLD: pass the repo dir as the 2nd argument."; exit 1; }

echo "== 1/4 Installing here (Docker, .env with this server's IP and a fresh domain)"
sh deploy.sh >/dev/null

echo "== 2/4 Carrying the admin token over"
for k in ADMIN_TOKEN TOKEN_SECRET; do
  v=$(ssh "$OLD" "grep '^$k=' $DIR/.env | cut -d= -f2-" || true)
  if [ -n "$v" ]; then sed -i "/^$k=/d" .env; echo "$k=$v" >> .env; fi
done

echo "== 3/4 Copying the database (old server goes offline now)"
ssh "$OLD" "cd $DIR && docker compose stop mp"
docker compose stop mp
# Stream /data through the mp image on both ends, so the files land owned by the container's user.
ssh "$OLD" "cd $DIR && docker compose run --rm --no-deps -T --entrypoint sh mp -c 'tar cf - -C /data .'" \
  | docker compose run --rm --no-deps -T --entrypoint sh mp -c 'rm -rf /data/* && tar xf - -C /data'

echo "== 4/4 Starting"
docker compose up -d
. ./.env
for i in 1 2 3 4 5 6 7 8 9 10; do docker compose exec -T mp wget -qO- http://127.0.0.1:8080/health >/dev/null 2>&1 && break; sleep 2; done
GAMES=$(docker compose exec -T mp wget -qO- --header "authorization: Bearer $ADMIN_TOKEN" http://127.0.0.1:8080/api/admin/games | grep -o '"id":"g_' | wc -l)
echo ""
echo "Moved: $GAMES game(s) now served from this server."
echo "Server:    https://$DOMAIN   (update SERVER_URL in your games and the dashboard if the domain changed)"
echo "Dashboard: https://$DOMAIN/   (admin token unchanged: $ADMIN_TOKEN)"
echo "Next:      sudo sh tune-network.sh    # BBR + low-latency TCP settings"
echo "Rollback:  ssh $OLD 'cd $DIR && docker compose start mp'"

#!/usr/bin/env sh
# One-shot install/update on a fresh Ubuntu/Debian VPS (Hostinger KVM etc). Run from the repo dir.
set -e
if ! command -v docker >/dev/null 2>&1; then curl -fsSL https://get.docker.com | sh; fi
if [ ! -f .env ]; then
  cp .env.example .env
  TOKEN=$(openssl rand -hex 32)
  IP=$(curl -fsS https://api.ipify.org || hostname -I | awk '{print $1}')
  sed -i "s/^ADMIN_TOKEN=.*/ADMIN_TOKEN=$TOKEN/; s/^DOMAIN=.*/DOMAIN=$(echo "$IP" | tr . -).sslip.io/" .env
  echo "Created .env (edit DOMAIN if you have a real domain)."
fi
command -v ufw >/dev/null 2>&1 && ufw status | grep -q active && { ufw allow 80/tcp; ufw allow 443/tcp; ufw allow 443/udp; } || true
docker compose up -d --build
. ./.env
echo ""
echo "Server:    https://$DOMAIN"
echo "Dashboard: https://$DOMAIN/   (admin token: $ADMIN_TOKEN)"
echo "Demo:      https://$DOMAIN/demo"

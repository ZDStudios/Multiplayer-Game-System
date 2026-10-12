#!/usr/bin/env sh
# Low-latency TCP tuning for a realtime WebSocket server (Ubuntu/Debian). Safe to re-run; persists across reboots.
#   - BBR congestion control + fq pacing: keeps queues short, much better on long/lossy routes than cubic
#   - tcp_notsent_lowat: stop the kernel from buffering MBs of unsent data per socket (head-of-line delay)
#   - no slow-start after idle: game sockets idle between bursts; don't throttle the next burst
#   - TCP Fast Open + MTU probing: faster (re)connects, avoids black-hole stalls on bad paths
set -e
[ "$(id -u)" = 0 ] || { echo "run as root (sudo sh tune-network.sh)"; exit 1; }

modprobe tcp_bbr 2>/dev/null || true
grep -qx tcp_bbr /etc/modules-load.d/bbr.conf 2>/dev/null || echo tcp_bbr > /etc/modules-load.d/bbr.conf

cat > /etc/sysctl.d/99-mp-latency.conf <<'EOF'
net.core.default_qdisc = fq
net.ipv4.tcp_congestion_control = bbr
net.ipv4.tcp_notsent_lowat = 16384
net.ipv4.tcp_slow_start_after_idle = 0
net.ipv4.tcp_fastopen = 3
net.ipv4.tcp_mtu_probing = 1
net.core.somaxconn = 4096
net.ipv4.tcp_max_syn_backlog = 4096
EOF
sysctl -q -p /etc/sysctl.d/99-mp-latency.conf

# default_qdisc only affects new interfaces: switch the live uplink to fq now
IF=$(ip route show default | awk '/default/ {print $5; exit}')
[ -n "$IF" ] && tc qdisc replace dev "$IF" root fq 2>/dev/null || true

echo "congestion control: $(sysctl -n net.ipv4.tcp_congestion_control)"
echo "qdisc on $IF:        $(tc qdisc show dev "$IF" | head -1)"
echo "notsent_lowat:      $(sysctl -n net.ipv4.tcp_notsent_lowat)"
echo "slow start idle:    $(sysctl -n net.ipv4.tcp_slow_start_after_idle)"

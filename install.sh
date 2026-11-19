#!/usr/bin/env bash
# Install fund-valuation on Debian 13 (run as root). Idempotent.
set -euo pipefail
APP_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$APP_DIR"

apt-get update -y
apt-get install -y python3 python3-venv python3-pip

if [ ! -d venv ]; then
  python3 -m venv venv
fi
./venv/bin/pip install --upgrade pip
./venv/bin/pip install -r requirements.txt

cat > /etc/systemd/system/fund-valuation.service <<EOF
[Unit]
Description=Fund Valuation MVP
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$APP_DIR
ExecStart=$APP_DIR/venv/bin/uvicorn app:app --host 127.0.0.1 --port 8000
Restart=on-failure
RestartSec=3
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable fund-valuation
systemctl restart fund-valuation
sleep 2
systemctl --no-pager status fund-valuation || true
echo
echo "Service listening on 127.0.0.1:8000. Put Nginx/Caddy in front for public HTTPS access."
echo "Quick check: curl -s http://127.0.0.1:8000/api/me"

#!/usr/bin/env bash
set -euo pipefail
mkdir -p "$HOME/.config/systemd/user"
cp /home/admin/.openclaw/workspace/systemd/factor-lab-web-ui.service "$HOME/.config/systemd/user/factor-lab-web-ui.service"
systemctl --user daemon-reload
systemctl --user enable --now factor-lab-web-ui.service
systemctl --user status --no-pager factor-lab-web-ui.service || true

#!/usr/bin/env bash
set -euo pipefail
mkdir -p "$HOME/.config/systemd/user"
cp /home/admin/.openclaw/workspace/systemd/factor-lab-research-daemon.service "$HOME/.config/systemd/user/factor-lab-research-daemon.service"
systemctl --user daemon-reload
systemctl --user enable --now factor-lab-research-daemon.service
systemctl --user status --no-pager factor-lab-research-daemon.service || true

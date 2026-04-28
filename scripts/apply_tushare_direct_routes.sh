#!/usr/bin/env bash
set -euo pipefail

sudo ip rule add to 60.205.198.20/32 lookup main pref 8990 2>/dev/null || true
sudo ip rule add to 8.140.225.26/32 lookup main pref 8991 2>/dev/null || true
sudo ip rule add to 47.94.110.54/32 lookup main pref 8992 2>/dev/null || true

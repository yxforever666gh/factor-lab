#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

SCRIPT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPT_ROOT / "src"))

from factor_lab.hermes_profiles import HERMES_PROFILE_SPECS


def desired_env_values() -> dict[str, str]:
    return {
        "FACTOR_LAB_AGENT_BACKEND": "hermes",
        "FACTOR_LAB_HERMES_MODE": "native",
        "FACTOR_LAB_HERMES_REQUIRE_PROFILE": "1",
        "FACTOR_LAB_HERMES_PROFILE_MAP_JSON": json.dumps({k: v.profile for k, v in HERMES_PROFILE_SPECS.items()}, separators=(",", ":")),
        "FACTOR_LAB_HERMES_SESSION_MODE": "resume",
        "FACTOR_LAB_HERMES_MODEL_SOURCE": "main",
        "FACTOR_LAB_HERMES_TIMEOUT_SECONDS": "300",
        "FACTOR_LAB_HERMES_ARTIFACT_DIR": "artifacts/hermes",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--bootstrap-profiles", action="store_true")
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    if shutil.which("hermes") is None:
        raise RuntimeError("hermes CLI is unavailable")
    values = desired_env_values()
    for k, v in values.items():
        print(f"{k}={v}")
    if args.write:
        path = Path(".env")
        existing = path.read_text(encoding="utf-8") if path.exists() else ""
        lines = [line for line in existing.splitlines() if not any(line.startswith(f"{k}=") for k in values)]
        lines.extend(f"{k}={v}" for k, v in values.items())
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

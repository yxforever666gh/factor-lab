#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Iterable

SCRIPT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPT_ROOT / "src"))

from factor_lab.hermes_profiles import HERMES_PROFILE_SPECS


def plan_profile_commands(existing_profiles: Iterable[str] = ()) -> list[list[str]]:
    existing = set(existing_profiles)
    commands: list[list[str]] = []
    for spec in HERMES_PROFILE_SPECS.values():
        if spec.profile not in existing:
            commands.append(["hermes", "profile", "create", spec.profile])
        commands.append(["hermes", "config", "set", f"profiles.{spec.profile}.terminal.cwd", "/home/admin/factor-lab"])
    return commands


def list_existing_profiles() -> set[str]:
    completed = subprocess.run(["hermes", "profile", "list"], capture_output=True, text=True, timeout=30)
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr or completed.stdout or "hermes profile list failed")
    return {line.strip().split()[0] for line in completed.stdout.splitlines() if line.strip()}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    existing = set() if args.dry_run else list_existing_profiles()
    commands = plan_profile_commands(existing)
    for command in commands:
        print(" ".join(command))
        if args.write:
            subprocess.run(command, check=True)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

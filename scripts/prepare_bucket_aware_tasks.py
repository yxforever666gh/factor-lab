#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path

from factor_lab.bucket_aware_task_preparer import prepare_bucket_aware_tasks
from factor_lab.research_os.legacy_entrypoint import retired_legacy_entrypoint

__all__ = ["prepare_bucket_aware_tasks"]


def _expand_config_globs(patterns: list[str]) -> list[Path]:
    paths: list[Path] = []
    for pattern in patterns:
        paths.extend(Path().glob(pattern))
    return sorted({path for path in paths})


def main() -> int:
    # The preparer remains importable for one-time legacy evidence migration;
    # executing it must not recreate the retired SQLite admission queue.
    return retired_legacy_entrypoint("scripts/prepare_bucket_aware_tasks.py")


if __name__ == "__main__":
    raise SystemExit(main())

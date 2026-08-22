"""Fail-closed compatibility responses for retired research entrypoints.

The implementation functions remain importable for numerical regression tests
and one-time evidence migration.  Executing their historical command-line
wrappers must not create a second candidate pipeline beside Research OS.
"""

from __future__ import annotations

import json
from typing import Any


def retired_legacy_entrypoint(
    entrypoint: str,
    *,
    next_command: str = "factor-lab research cycle",
) -> int:
    payload: dict[str, Any] = {
        "ok": False,
        "status": "retired_legacy_entrypoint",
        "entrypoint": str(entrypoint),
        "reason": "research_os_is_authoritative",
        "next": next_command,
        "candidate_written": False,
        "live_trading_enabled": False,
    }
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 2


__all__ = ["retired_legacy_entrypoint"]

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
HEARTBEAT_PATH = ROOT / "artifacts" / "system_heartbeat.jsonl"


def append_heartbeat(scope: str, status: str, **payload: Any) -> dict[str, Any]:
    HEARTBEAT_PATH.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": scope,
        "status": status,
        **payload,
    }
    with HEARTBEAT_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
    return row

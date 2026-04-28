from __future__ import annotations

import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "artifacts"


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def should_promote_research_paths() -> dict[str, Any]:
    research_flow_state = _read_json(ARTIFACTS / "research_flow_state.json", {})
    learning = _read_json(ARTIFACTS / "research_learning.json", {})
    recovery_state = research_flow_state.get("state")
    families = learning.get("families") or {}

    promotable = [
        family for family, row in families.items()
        if row.get("recommended_action") == "upweight" and not row.get("cooldown_active")
    ]
    should_promote = recovery_state in {"recovering", "recovered"} and bool(promotable)
    return {
        "should_promote": should_promote,
        "recovery_state": recovery_state,
        "promotable_families": promotable,
    }

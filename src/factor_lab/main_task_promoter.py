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


def should_promote_main_tasks() -> dict[str, Any]:
    main_task_state = _read_json(ARTIFACTS / "main_task_generation_state.json", {})
    learning = _read_json(ARTIFACTS / "main_task_learning.json", {})
    recovery_state = main_task_state.get("state")
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

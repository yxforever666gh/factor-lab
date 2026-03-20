from __future__ import annotations

import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "artifacts"
WINDOW = 12
NO_GAIN_COOLDOWN = 2


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _family_key_from_branch_id(branch_id: str | None) -> str | None:
    text = branch_id or ""
    if text.startswith("fallback_"):
        return None
    for key in [
        "stable_candidate_validation",
        "graveyard_diagnosis",
        "recent_window_validation",
        "window_expansion",
        "exploration",
    ]:
        if key in text:
            return key
    return None


def build_main_task_learning(memory_path: str | Path | None = None) -> dict[str, Any]:
    path = Path(memory_path) if memory_path else (ARTIFACTS / "research_memory.json")
    memory = _read_json(path, {})
    execution_feedback = list(memory.get("execution_feedback") or [])[-WINDOW:]

    families: dict[str, dict[str, Any]] = {}
    for row in execution_feedback:
        family = _family_key_from_branch_id(row.get("branch_id"))
        if not family:
            continue
        meta = families.setdefault(family, {
            "family": family,
            "recent_runs": 0,
            "recent_gain": 0,
            "recent_no_gain": 0,
            "consecutive_no_gain": 0,
            "cooldown_active": False,
            "recommended_action": "keep",
        })
        meta["recent_runs"] += 1
        if row.get("has_gain"):
            meta["recent_gain"] += 1
        else:
            meta["recent_no_gain"] += 1

    for family, meta in families.items():
        consecutive = 0
        for row in reversed(execution_feedback):
            row_family = _family_key_from_branch_id(row.get("branch_id"))
            if row_family != family:
                continue
            if row.get("has_gain"):
                break
            consecutive += 1
        meta["consecutive_no_gain"] = consecutive
        meta["cooldown_active"] = consecutive >= NO_GAIN_COOLDOWN
        if meta["cooldown_active"]:
            meta["recommended_action"] = "cooldown"
        elif meta["recent_gain"] >= 2 and meta["recent_gain"] >= meta["recent_no_gain"]:
            meta["recommended_action"] = "upweight"
        elif meta["recent_no_gain"] > meta["recent_gain"]:
            meta["recommended_action"] = "downweight"
        else:
            meta["recommended_action"] = "keep"

    payload = {
        "updated_at_utc": memory.get("updated_at_utc"),
        "families": families,
    }
    (ARTIFACTS / "main_task_learning.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload

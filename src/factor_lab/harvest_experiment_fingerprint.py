from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

HARVEST_ROOT = Path("artifacts/harvest_agent")


def canonical_plan(plan: dict[str, Any]) -> dict[str, Any]:
    return {
        "dataset_path": plan.get("dataset_path"),
        "actions": plan.get("actions") or [],
        "success_criteria": plan.get("success_criteria") or {},
    }


def fingerprint_plan(plan: dict[str, Any]) -> str:
    text = json.dumps(canonical_plan(plan), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:24]


def is_duplicate_fingerprint(root: str | Path, fingerprint: str) -> bool:
    base = Path(root) / HARVEST_ROOT
    for path in base.glob("cycle_*/experiment_fingerprint.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if payload.get("fingerprint") == fingerprint:
            return True
    return False


def write_fingerprint(cycle_dir: str | Path, plan: dict[str, Any]) -> Path:
    cycle_dir = Path(cycle_dir)
    cycle_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "fingerprint": fingerprint_plan(plan),
        "canonical_plan": canonical_plan(plan),
    }
    path = cycle_dir / "experiment_fingerprint.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path

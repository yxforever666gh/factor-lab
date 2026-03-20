from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "artifacts"
STORE_PATH = ARTIFACTS / "research_opportunity_store.json"
EXECUTION_PATH = ARTIFACTS / "opportunity_execution_plan.json"
OUTPUT_PATH = ARTIFACTS / "opportunity_archive_diagnostics.json"


def build_opportunity_archive_diagnostics(
    store_path: str | Path | None = None,
    execution_path: str | Path | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    spath = Path(store_path) if store_path else STORE_PATH
    epath = Path(execution_path) if execution_path else EXECUTION_PATH
    opath = Path(output_path) if output_path else OUTPUT_PATH

    store = json.loads(spath.read_text(encoding="utf-8")) if spath.exists() else {"opportunities": {}}
    execution = json.loads(epath.read_text(encoding="utf-8")) if epath.exists() else {"skipped": []}

    items = list((store.get("opportunities") or {}).values())
    skipped = list(execution.get("skipped") or [])
    skip_reason_by_id = {row.get("opportunity_id"): row.get("reason") for row in skipped if row.get("opportunity_id")}

    archive_counts: dict[str, int] = {}
    archive_samples: list[dict[str, Any]] = []
    funnel = {
        "proposed": 0,
        "scheduled": 0,
        "running": 0,
        "evaluated": 0,
        "promoted": 0,
        "rejected": 0,
        "archived": 0,
    }

    for row in items:
        state = row.get("state") or "proposed"
        if state in funnel:
            funnel[state] += 1
        if state != "archived":
            continue
        reason = None
        history = list(row.get("history") or [])
        if history:
            reason = history[-1].get("reason")
        reason = reason or skip_reason_by_id.get(row.get("opportunity_id")) or "unknown"
        archive_counts[reason] = archive_counts.get(reason, 0) + 1
        archive_samples.append({
            "opportunity_id": row.get("opportunity_id"),
            "type": row.get("opportunity_type"),
            "reason": reason,
            "priority": row.get("priority"),
            "novelty": row.get("novelty_score"),
            "confidence": row.get("confidence"),
        })

    payload = {
        "funnel": funnel,
        "archive_counts": dict(sorted(archive_counts.items(), key=lambda item: (-item[1], item[0]))),
        "archive_samples": archive_samples[:20],
        "skipped_count": len(skipped),
    }
    opath.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def build_llm_diagnostics(snapshot_path: str | Path, opportunities_path: str | Path, output_path: str | Path) -> dict[str, Any]:
    snapshot = json.loads(Path(snapshot_path).read_text(encoding="utf-8")) if Path(snapshot_path).exists() else {}
    opportunities = json.loads(Path(opportunities_path).read_text(encoding="utf-8")) if Path(opportunities_path).exists() else {}

    analyst = snapshot.get("analyst_signals") or {}
    feedback = snapshot.get("analyst_feedback_context") or {}
    flow_state = snapshot.get("research_flow_state") or {}
    learning = snapshot.get("research_learning") or {}
    opps = opportunities.get("opportunities") or []

    diagnostics = {
        "flow_state": flow_state,
        "opportunity_count": len(opps),
        "opportunity_types": sorted({row.get("opportunity_type") for row in opps if row.get("opportunity_type")}),
        "top_priority": max([float(row.get("priority") or 0.0) for row in opps] or [0.0]),
        "avg_confidence": round(sum(float(row.get("confidence") or 0.0) for row in opps) / max(len(opps), 1), 3) if opps else None,
        "avg_novelty": round(sum(float(row.get("novelty_score") or 0.0) for row in opps) / max(len(opps), 1), 3) if opps else None,
        "analyst_focus_count": len(analyst.get("focus_factors") or []),
        "analyst_recovery_trigger_count": ((feedback.get("analyst_learning_loop") or {}).get("recovery_trigger_count_last_5")) or 0,
        "learning_family_count": len((learning.get("families") or {})),
        "warnings": [],
    }

    if diagnostics["opportunity_count"] == 0:
        diagnostics["warnings"].append("no_opportunities_generated")
    if flow_state.get("state") == "recovering":
        diagnostics["warnings"].append("system_still_recovering")
    if diagnostics["avg_novelty"] is not None and diagnostics["avg_novelty"] < 0.45:
        diagnostics["warnings"].append("novelty_low")
    if diagnostics["avg_confidence"] is not None and diagnostics["avg_confidence"] < 0.55:
        diagnostics["warnings"].append("confidence_low")

    Path(output_path).write_text(json.dumps(diagnostics, ensure_ascii=False, indent=2), encoding="utf-8")
    return diagnostics

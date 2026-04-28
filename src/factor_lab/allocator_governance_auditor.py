from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.agent_schemas import ALLOCATOR_GOVERNANCE_AUDIT_SCHEMA_VERSION


_STATE_TO_ACTION = {
    "approved": "ok",
    "watchlist": "missing_followup",
    "shadow": "suspicious",
    "rejected": "suspicious",
}


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def build_allocator_governance_audit(*, approved_universe: dict[str, Any], current_portfolio: dict[str, Any] | None = None) -> dict[str, Any]:
    current_portfolio = current_portfolio or {}
    rows = list(approved_universe.get("rows") or [])
    selected_factors = {
        row.get("name")
        for row in (current_portfolio.get("selected_factors") or [])
        if row.get("name")
    }

    allocation_rows = []
    governance_rows = []
    bucket_alloc = ((approved_universe.get("budget_summary") or {}).get("bucket_allocations") or {})

    for row in rows:
        factor_name = row.get("factor_name")
        lifecycle_state = row.get("lifecycle_state") or row.get("universe_state") or "approved"
        governance_action = row.get("governance_action") or "keep"
        allocated_weight = float(row.get("allocated_weight") or row.get("portfolio_weight_hint") or 0.0)
        max_weight = float(row.get("max_weight") or 0.0)
        approval_tier = row.get("approval_tier") or "core"
        bucket = row.get("portfolio_bucket") or "unknown"

        if allocated_weight > max_weight > 0:
            allocation_audit = "inconsistent"
            budget_adjustment = f"cap_to_max_weight:{max_weight:.3f}"
        elif approval_tier == "bridge" and allocated_weight >= 0.3:
            allocation_audit = "concentrated"
            budget_adjustment = "trim_bridge_weight"
        elif allocated_weight <= 0:
            allocation_audit = "stale_governance"
            budget_adjustment = "missing_allocation_followup"
        else:
            allocation_audit = "ok"
            budget_adjustment = "keep"

        if lifecycle_state in {"shadow", "watchlist"} and governance_action == "keep" and factor_name in selected_factors:
            state_transition_audit = "suspicious"
        else:
            state_transition_audit = _STATE_TO_ACTION.get(lifecycle_state, "ok")
        if lifecycle_state == "approved" and governance_action in {"demote_candidate", "demote_bridge_candidate"}:
            state_transition_audit = "missing_followup"

        reasoning = [
            f"state={lifecycle_state}",
            f"governance_action={governance_action}",
            f"allocated={allocated_weight:.3f}",
            f"max={max_weight:.3f}",
            f"bucket={bucket}",
        ]
        allocation_rows.append({
            "factor_name": factor_name,
            "allocation_audit": allocation_audit,
            "budget_adjustment_suggestions": [budget_adjustment],
            "allocated_weight": allocated_weight,
            "max_weight": max_weight,
            "bucket": bucket,
            "reasoning_summary": "；".join(reasoning),
        })
        governance_rows.append({
            "factor_name": factor_name,
            "state_transition_audit": state_transition_audit,
            "demotion_review": governance_action if "demote" in governance_action else "none",
            "shadow_watchlist_review": lifecycle_state if lifecycle_state in {"shadow", "watchlist"} else "none",
            "reasoning_summary": "；".join(reasoning),
        })

    allocation_summary = {
        "schema_version": ALLOCATOR_GOVERNANCE_AUDIT_SCHEMA_VERSION,
        "count_by_audit": dict(Counter(row.get("allocation_audit") or "unknown" for row in allocation_rows)),
        "bucket_allocations": bucket_alloc,
    }
    governance_summary = {
        "schema_version": ALLOCATOR_GOVERNANCE_AUDIT_SCHEMA_VERSION,
        "count_by_audit": dict(Counter(row.get("state_transition_audit") or "unknown" for row in governance_rows)),
        "state_counts": (approved_universe.get("summary") or {}).get("state_counts") or {},
    }
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "schema_version": ALLOCATOR_GOVERNANCE_AUDIT_SCHEMA_VERSION,
        "allocation": {"rows": allocation_rows, "summary": allocation_summary},
        "governance": {"rows": governance_rows, "summary": governance_summary},
    }


def write_allocator_governance_audit(*, approved_universe: dict[str, Any], artifacts_dir: str | Path, current_portfolio: dict[str, Any] | None = None) -> dict[str, Any]:
    artifacts_dir = Path(artifacts_dir)
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    payload = build_allocator_governance_audit(approved_universe=approved_universe, current_portfolio=current_portfolio)
    (artifacts_dir / "approved_universe_allocation_audit.json").write_text(json.dumps(payload["allocation"], ensure_ascii=False, indent=2), encoding="utf-8")
    (artifacts_dir / "approved_universe_governance_audit.json").write_text(json.dumps(payload["governance"], ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def load_allocator_governance_audit(artifacts_dir: str | Path) -> dict[str, Any]:
    artifacts_dir = Path(artifacts_dir)
    return {
        "allocation": _load_json(artifacts_dir / "approved_universe_allocation_audit.json", {"rows": [], "summary": {}}),
        "governance": _load_json(artifacts_dir / "approved_universe_governance_audit.json", {"rows": [], "summary": {}}),
    }

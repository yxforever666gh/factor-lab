from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_POLICY_PATH = ROOT / "configs" / "research_runtime_takeover.json"


@dataclass
class RuntimeTakeoverPolicy:
    enabled: bool = True
    mode: str = "research_quality_takeover"
    allow_legacy_pending: bool = False
    allow_unmechanized_workflow: bool = False
    allow_recent_window_as_promotion_evidence: bool = False
    max_full_runs_per_cycle: int = 3
    allowed_value_routes: list[str] = field(default_factory=lambda: [
        "industry_relative_value",
        "value_quality_no_distress",
        "value_momentum_confirmation",
    ])
    allowed_controlled_routes: list[str] = field(default_factory=list)
    blocked_worker_note_patterns: list[str] = field(default_factory=lambda: [
        "candidate_earnings_yield_book_yield_recent",
        "rolling_30d_back",
        "rolling_60d_back",
    ])
    require_preflight_before_workflow: bool = True
    no_task_is_valid_research_outcome: bool = True

    def evaluate_task(self, task: dict[str, Any]) -> dict[str, Any]:
        if not self.enabled:
            return {"decision": "allow", "reasons": [], "mode": self.mode}

        reasons: list[str] = []
        task_type = str(task.get("task_type") or "")
        worker_note = str(task.get("worker_note") or "")
        payload = task.get("payload") or task.get("payload_json") or {}
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                payload = {}
        if not isinstance(payload, dict):
            payload = {}

        for pattern in self.blocked_worker_note_patterns:
            if pattern and pattern in worker_note:
                reasons.append("blocked_worker_note_pattern")
                break

        if task_type == "workflow":
            mechanism_id = payload.get("mechanism_id") or payload.get("mechanism")
            route_id = payload.get("route_id") or payload.get("value_route_id")
            is_baseline = worker_note.startswith("baseline") or payload.get("baseline_reason")
            if not mechanism_id and not self.allow_unmechanized_workflow and not is_baseline:
                reasons.append("missing_mechanism_id")
            allowed_routes = set(self.allowed_value_routes) | set(self.allowed_controlled_routes)
            if route_id and route_id not in allowed_routes:
                reasons.append("route_not_allowed_in_takeover")
            if "recent_" in worker_note and not self.allow_recent_window_as_promotion_evidence:
                reasons.append("recent_window_not_promotion_evidence")

        decision = "block" if reasons else "allow"
        return {
            "decision": decision,
            "reasons": sorted(set(reasons)),
            "mode": self.mode,
            "task_type": task_type,
            "route_id": payload.get("route_id") or payload.get("value_route_id"),
            "mechanism_id": payload.get("mechanism_id"),
        }


def load_runtime_takeover_policy(source: dict[str, Any] | str | Path | None = None) -> RuntimeTakeoverPolicy:
    if source is None:
        path = DEFAULT_POLICY_PATH
        data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    elif isinstance(source, (str, Path)):
        data = json.loads(Path(source).read_text(encoding="utf-8"))
    else:
        data = dict(source)
    allowed = data.get("allowed_value_routes")
    controlled_allowed = data.get("allowed_controlled_routes")
    blocked = data.get("blocked_worker_note_patterns")
    return RuntimeTakeoverPolicy(
        enabled=bool(data.get("enabled", True)),
        mode=str(data.get("mode") or "research_quality_takeover"),
        allow_legacy_pending=bool(data.get("allow_legacy_pending", False)),
        allow_unmechanized_workflow=bool(data.get("allow_unmechanized_workflow", False)),
        allow_recent_window_as_promotion_evidence=bool(data.get("allow_recent_window_as_promotion_evidence", False)),
        max_full_runs_per_cycle=int(data.get("max_full_runs_per_cycle") or 3),
        allowed_value_routes=list(allowed) if isinstance(allowed, list) else RuntimeTakeoverPolicy().allowed_value_routes,
        allowed_controlled_routes=list(controlled_allowed) if isinstance(controlled_allowed, list) else RuntimeTakeoverPolicy().allowed_controlled_routes,
        blocked_worker_note_patterns=list(blocked) if isinstance(blocked, list) else RuntimeTakeoverPolicy().blocked_worker_note_patterns,
        require_preflight_before_workflow=bool(data.get("require_preflight_before_workflow", True)),
        no_task_is_valid_research_outcome=bool(data.get("no_task_is_valid_research_outcome", True)),
    )

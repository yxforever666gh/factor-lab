from __future__ import annotations

import json
from pathlib import Path
from typing import Any

RANKS = {"promote": 0, "neutral": 1, "hold": 2, "demote": 3, "skip": 4}


def route_decision_rank(decision: str | None) -> int:
    return RANKS.get(str(decision or "neutral"), RANKS["neutral"])


def _decide(row: dict[str, Any]) -> tuple[str, str]:
    runs = int(row.get("run_count") or 0)
    passes = int(row.get("pass_gate_count") or 0)
    coverage = int(row.get("coverage_too_low_count") or 0)
    split = int(row.get("too_many_split_failures_count") or 0)
    bucket_oos_passes = int(row.get("bucket_aware_oos_pass_gate_count") or 0)
    try:
        bucket_pass_rate = float(row.get("bucket_aware_rolling_pass_rate_max") or 0)
        bucket_positive_ratio = float(row.get("bucket_aware_positive_spread_ratio_max") or 0)
    except (TypeError, ValueError):
        bucket_pass_rate = 0.0
        bucket_positive_ratio = 0.0
    if runs > 0 and coverage >= 2 and passes == 0:
        return "demote", "repeated_coverage_too_low"
    if bucket_oos_passes > 0 and coverage == 0 and bucket_pass_rate >= 0.6 and bucket_positive_ratio >= 0.7:
        return "promote", "bucket_aware_oos_stable"
    if runs > 0 and split >= 2:
        return "hold", "repeated_split_instability"
    if passes > 0 and coverage == 0 and split == 0:
        return "promote", "passes_gate_without_repeated_blockers"
    return "neutral", "insufficient_evidence"


def build_controlled_route_policy(summary: dict[str, Any] | None) -> dict[str, Any]:
    routes = {}
    for route, row in ((summary or {}).get("route_summary") or {}).items():
        decision, reason = _decide(row if isinstance(row, dict) else {})
        routes[str(route)] = {"decision": decision, "reason": reason, **(row if isinstance(row, dict) else {})}
    return {"schema_version": 1, "routes": routes}


def load_controlled_route_policy(path: str | Path = "artifacts/controlled_route_policy.json") -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {"schema_version": 1, "routes": {}}
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"schema_version": 1, "routes": {}}
    return payload if isinstance(payload, dict) else {"schema_version": 1, "routes": {}}


def write_controlled_route_policy(*, summary_path: str | Path = "artifacts/controlled_run_ledger_summary.json", output_path: str | Path = "artifacts/controlled_route_policy.json") -> dict[str, Any]:
    try:
        summary = json.loads(Path(summary_path).read_text(encoding="utf-8"))
    except Exception:
        summary = {}
    policy = build_controlled_route_policy(summary if isinstance(summary, dict) else {})
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(policy, ensure_ascii=False, indent=2), encoding="utf-8")
    return policy

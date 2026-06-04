#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SIM_DIR = Path("artifacts/small_institutional_simulation")
KNOWN_ARTIFACTS = [
    Path("artifacts/small_institutionalization/status.json"),
    SIM_DIR / "portfolio_construction_repair.json",
    SIM_DIR / "risk_reduction_plan.json",
    SIM_DIR / "risk_reduction_results.json",
    SIM_DIR / "risk_reduction_repair.json",
    Path("artifacts/controlled_restart_dry_run.json"),
    Path("artifacts/runtime_takeover_audit.json"),
]


def load_json(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def known_artifact_payloads(root: str | Path = ROOT) -> list[dict[str, Any]]:
    base = Path(root)
    artifacts: list[dict[str, Any]] = []
    for relative in KNOWN_ARTIFACTS:
        path = base / relative
        item: dict[str, Any] = {"path": str(relative), "present": path.exists()}
        if path.exists():
            item["payload"] = load_json(path)
        artifacts.append(item)
    return artifacts


def artifact_payload(source_artifacts: list[dict[str, Any]], suffix: str) -> dict[str, Any]:
    for item in source_artifacts:
        if str(item.get("path", "")).endswith(suffix):
            payload = item.get("payload")
            return payload if isinstance(payload, dict) else {}
    return {}


def artifact_present(source_artifacts: list[dict[str, Any]], suffix: str) -> bool:
    return any(str(item.get("path", "")).endswith(suffix) and bool(item.get("present")) for item in source_artifacts)


def repair_scoring_present(repair: dict[str, Any]) -> bool:
    markers = [
        "scored_source_artifact",
        "risk_reduction_scoring",
        "risk_reduction_results_scored_at_utc",
        "risk_reduction_results_repair_status",
    ]
    return any(repair.get(marker) for marker in markers)


def determine_correction_status(source_artifacts: list[dict[str, Any]]) -> dict[str, Any]:
    base_repair = artifact_payload(source_artifacts, "portfolio_construction_repair.json")
    risk_reduction_repair = artifact_payload(source_artifacts, "risk_reduction_repair.json")
    risk_reduction_repair_exists = artifact_present(source_artifacts, "risk_reduction_repair.json")
    repair = risk_reduction_repair if risk_reduction_repair_exists else base_repair
    repair_status = str(repair.get("repair_status") or "unknown")
    candidate_count = int(repair.get("candidate_count") or 0) if str(repair.get("candidate_count") or "0").isdigit() else 0
    recommended_candidate = repair.get("recommended_candidate")
    plan_exists = artifact_present(source_artifacts, "risk_reduction_plan.json")
    results_exists = artifact_present(source_artifacts, "risk_reduction_results.json")
    has_safe_candidate = bool(recommended_candidate) or candidate_count > 0 or repair_status in {
        "candidate_ready_for_manual_review",
        "safe_candidate_found",
        "repair_candidate_ready",
    }

    base: dict[str, Any] = {
        "failure_target": "portfolio_simulation_drawdown_blocker",
        "latest_agent_role": "implementer",
        "engineering_status": "artifact_backed_status_only",
        "runtime_safety": "safe_no_claimable_workflow",
        "repair_status": repair_status,
    }

    if has_safe_candidate:
        base.update(
            {
                "correction_status": "manual_review_required",
                "portfolio_status": "safe_candidate_found_pending_manual_review",
                "next_action": "manual_review_before_admission",
            }
        )
    elif repair_status == "blocked_no_drawdown_safe_candidate" and not plan_exists:
        base.update(
            {
                "correction_status": "needs_risk_reduction_plan",
                "portfolio_status": "blocked_no_drawdown_safe_candidate",
                "next_action": "write_risk_reduction_plan",
            }
        )
    elif repair_status == "blocked_no_drawdown_safe_candidate" and plan_exists and not results_exists:
        base.update(
            {
                "correction_status": "ready_for_executor_agent",
                "portfolio_status": "unproven_until_executor_runs",
                "next_action": "run_risk_reduction_controlled_executor",
            }
        )
    elif results_exists and not risk_reduction_repair_exists and not repair_scoring_present(base_repair):
        base.update(
            {
                "correction_status": "ready_for_verifier_agent",
                "portfolio_status": "risk_reduction_results_unscored",
                "next_action": "score_risk_reduction_results",
            }
        )
    elif repair_status == "blocked_no_drawdown_safe_candidate":
        base.update(
            {
                "correction_status": "blocked_after_scoring",
                "portfolio_status": "blocked_no_drawdown_safe_candidate_after_scoring",
                "next_action": "write_blocker_report_or_request_new_mechanism",
            }
        )
    else:
        base.update(
            {
                "correction_status": "needs_diagnostician_agent",
                "portfolio_status": "unknown",
                "next_action": "run_diagnostician_agent",
                "latest_agent_role": "diagnostician",
            }
        )
    return base


def inspect_hermes_correction_status(*, root: str | Path = ROOT) -> dict[str, Any]:
    return determine_correction_status(known_artifact_payloads(root))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Inspect deterministic Hermes correction status.")
    parser.add_argument("--root", default=str(ROOT))
    args = parser.parse_args(argv)
    print(json.dumps(inspect_hermes_correction_status(root=args.root), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

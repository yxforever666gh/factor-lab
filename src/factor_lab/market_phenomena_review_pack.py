from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

GATE_KEYS = ["production_execution_allowed", "live_trading_allowed", "queue_write_allowed", "timer_enable_allowed", "daemon_restore_allowed", "auto_promotion_allowed"]


def _closed_gates() -> dict[str, bool]:
    return {key: False for key in GATE_KEYS}


def _evidence_summary(deeper_oos_report: dict[str, Any], horizons: list[int]) -> list[dict[str, Any]]:
    wanted = set(horizons)
    out = []
    for item in deeper_oos_report.get("results") or []:
        if item.get("horizon") in wanted or item.get("split") == "oos":
            out.append(
                {
                    "horizon": item.get("horizon"),
                    "split": item.get("split"),
                    "status": item.get("status"),
                    "cost_adjusted_spread_vs_control": item.get("cost_adjusted_spread_vs_control"),
                    "spread_vs_control": item.get("spread_vs_control"),
                    "target_worst_forward_return": item.get("target_worst_forward_return"),
                    "usable_row_count": item.get("usable_row_count"),
                }
            )
    return out


def _review_questions(supported_horizons: list[int], rejected_horizons: list[dict[str, Any]]) -> list[str]:
    scope = ", ".join(f"{h}d" for h in supported_horizons) or "none"
    questions = [
        f"Do you accept limiting strategy design review to supported horizon(s): {scope}?",
        "Do you accept the remaining train tail risk, or should the drawdown guard be tightened before strategy design?",
        "Should liquidity_turnover_filter and drawdown_guard be mandatory in every prototype variant?",
        "Is the market-phenomenon mechanism still credible after rejected horizons are excluded?",
        "Should any rejected horizon be revisited with a different mechanism before strategy design?",
        "Do you approve moving to strategy design prototype review, with production/live trading still blocked?",
    ]
    if rejected_horizons:
        questions.append("Confirm rejected horizons remain excluded: " + ", ".join(f"{item.get('horizon')}d={item.get('reason')}" for item in rejected_horizons))
    return questions


def build_human_review_pack(*, run_id: str, iteration_plan: dict[str, Any], horizon_router: dict[str, Any], deeper_oos_report: dict[str, Any]) -> dict[str, Any]:
    gate = horizon_router.get("strategy_design_review_gate") or {}
    supported = list(horizon_router.get("supported_horizons") or gate.get("supported_horizons") or [])
    rejected = list(horizon_router.get("rejected_horizons") or gate.get("rejected_horizons") or [])
    review_allowed = bool(gate.get("strategy_design_review_allowed")) and bool(supported)
    evidence = _evidence_summary(deeper_oos_report, supported)
    spec = {
        "schema_version": 1,
        "run_id": run_id + "_strategy_design_spec",
        "mode": "strategy_design_spec",
        "phenomenon_id": iteration_plan.get("phenomenon_id") or horizon_router.get("phenomenon_id"),
        "title": iteration_plan.get("title"),
        "source_iteration_plan_run_id": iteration_plan.get("run_id"),
        "source_horizon_router_run_id": horizon_router.get("run_id"),
        "supported_horizons": supported,
        "review_scope": [f"{h}d" for h in supported],
        "rejected_horizons": rejected,
        "mechanism_hypothesis": iteration_plan.get("mechanism_hypothesis"),
        "participant_logic": iteration_plan.get("participant_logic"),
        "mandatory_constraints": iteration_plan.get("risk_cost_constraints") or {},
        "evidence_summary": evidence,
        "strategy_design_review_allowed": review_allowed,
        "strategy_generation_allowed": False,
        "human_approval_required": True,
        **_closed_gates(),
    }
    checklist = {
        "schema_version": 1,
        "run_id": run_id + "_strategy_design_review_checklist",
        "mode": "strategy_design_review_checklist",
        "phenomenon_id": spec.get("phenomenon_id"),
        "decision_required": "approve_or_reject_strategy_design_review" if review_allowed else "reject_or_request_more_research",
        "questions": _review_questions(supported, rejected),
        "must_confirm": [
            "No live trading",
            "No production queue writes",
            "No auto-promotion",
            "Only supported horizons are in scope",
            "Rejected horizons remain excluded unless a new mechanism is proposed",
        ],
        **_closed_gates(),
    }
    return {
        "schema_version": 1,
        "run_id": run_id,
        "mode": "human_review_pack",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "phenomenon_id": spec.get("phenomenon_id"),
        "title": spec.get("title"),
        "supported_horizons": supported,
        "rejected_horizons": rejected,
        "evidence_summary": evidence,
        "strategy_design_spec": spec,
        "strategy_design_review_checklist": checklist,
        **_closed_gates(),
    }


def validate_human_review_pack(pack: dict[str, Any]) -> dict[str, Any]:
    reason_codes: list[str] = []
    spec = pack.get("strategy_design_spec") or {}
    checklist = pack.get("strategy_design_review_checklist") or {}
    for key in GATE_KEYS:
        if pack.get(key) is not False:
            reason_codes.append(f"pack_gate_not_closed_{key}")
        if spec.get(key) is not False:
            reason_codes.append(f"spec_gate_not_closed_{key}")
        if checklist.get(key) is not False:
            reason_codes.append(f"checklist_gate_not_closed_{key}")
    if spec.get("strategy_generation_allowed") is not False:
        reason_codes.append("strategy_generation_not_closed")
    if spec.get("human_approval_required") is not True:
        reason_codes.append("human_approval_not_required")
    if spec.get("strategy_design_review_allowed") and not spec.get("supported_horizons"):
        reason_codes.append("review_allowed_without_supported_horizons")
    return {"decision": "keep" if not reason_codes else "reject", "reason_codes": reason_codes}


def human_review_pack_to_markdown(pack: dict[str, Any]) -> str:
    spec = pack.get("strategy_design_spec") or {}
    checklist = pack.get("strategy_design_review_checklist") or {}
    lines = [
        "# Human Review Pack",
        "",
        f"run_id: {pack.get('run_id')}",
        f"phenomenon_id: {pack.get('phenomenon_id')}",
        f"title: {pack.get('title')}",
        f"strategy_design_review_allowed: {spec.get('strategy_design_review_allowed')}",
        f"strategy_generation_allowed: {spec.get('strategy_generation_allowed')}",
        f"production_execution_allowed: {spec.get('production_execution_allowed')}",
        f"queue_write_allowed: {spec.get('queue_write_allowed')}",
        "",
        "## Supported horizons",
    ]
    lines.extend(f"- {h}d" for h in pack.get("supported_horizons") or [])
    lines.extend(["", "## Rejected horizons"])
    for item in pack.get("rejected_horizons") or []:
        lines.append(f"- {item.get('horizon')}d: {item.get('reason')}")
    lines.extend(["", "## Mandatory constraints"])
    for name in (spec.get("mandatory_constraints") or {}).keys():
        lines.append(f"- {name}")
    lines.extend(["", "## Evidence summary"])
    for item in pack.get("evidence_summary") or []:
        lines.append(f"- {item.get('split')} {item.get('horizon')}d: status={item.get('status')} cost_adj_spread={item.get('cost_adjusted_spread_vs_control')} worst={item.get('target_worst_forward_return')}")
    lines.extend(["", "## Review questions"])
    lines.extend(f"- {question}" for question in checklist.get("questions") or [])
    return "\n".join(lines).rstrip() + "\n"


def _checklist_to_markdown(checklist: dict[str, Any]) -> str:
    lines = [
        "# Strategy Design Review Checklist",
        "",
        f"run_id: {checklist.get('run_id')}",
        f"decision_required: {checklist.get('decision_required')}",
        "",
        "## Questions",
    ]
    lines.extend(f"- [ ] {question}" for question in checklist.get("questions") or [])
    lines.extend(["", "## Must confirm"])
    lines.extend(f"- [ ] {item}" for item in checklist.get("must_confirm") or [])
    return "\n".join(lines).rstrip() + "\n"


def write_human_review_pack(pack: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths = {
        "review_json": out / "human_review_pack.json",
        "review_markdown": out / "human_review_pack.md",
        "spec_json": out / "strategy_design_spec.json",
        "checklist_json": out / "strategy_design_review_checklist.json",
        "checklist_markdown": out / "strategy_design_review_checklist.md",
    }
    paths["review_json"].write_text(json.dumps(pack, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    paths["review_markdown"].write_text(human_review_pack_to_markdown(pack), encoding="utf-8")
    paths["spec_json"].write_text(json.dumps(pack.get("strategy_design_spec") or {}, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    paths["checklist_json"].write_text(json.dumps(pack.get("strategy_design_review_checklist") or {}, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    paths["checklist_markdown"].write_text(_checklist_to_markdown(pack.get("strategy_design_review_checklist") or {}), encoding="utf-8")
    return paths

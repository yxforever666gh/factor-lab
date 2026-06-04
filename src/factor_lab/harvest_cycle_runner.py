from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from factor_lab.harvest_backtest_runner import run_plan_backtest
from factor_lab.harvest_comparative_evaluator import compare_results
from factor_lab.harvest_data_request import build_harvest_data_request
from factor_lab.harvest_diagnostician import diagnose_analysis
from factor_lab.harvest_experiment_fingerprint import fingerprint_plan, write_fingerprint
from factor_lab.harvest_failure_attribution import attribute_harvest_failure
from factor_lab.harvest_oos_validator import validate_oos_robustness
from factor_lab.harvest_portfolio_branch_planner import build_portfolio_branch_plan
from factor_lab.harvest_real_execution_guard import validate_real_backtest_result
from factor_lab.harvest_research_decision import decide_next_research_branch
from factor_lab.harvest_result_analyzer import analyze_result_payload
from factor_lab.harvest_route_state import build_route_state
from factor_lab.harvest_semantic_duplicate import semantic_hash, write_semantic_signature
from factor_lab.harvest_v3_next_plan import build_v3_next_cycle_plan

HARVEST_ROOT = Path("artifacts/harvest_agent")
RUN_ID = "value_quality_cost_sensitivity_v1"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_md(path: Path, title: str, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"# {title}\n\n```json\n{json.dumps(payload, ensure_ascii=False, indent=2)}\n```\n", encoding="utf-8")


def _load_latest_result(base: Path, cycle_id: str | None) -> dict[str, Any] | None:
    if not cycle_id:
        return None
    matches = sorted((base / cycle_id).glob("runs/*/result.json"))
    if not matches:
        return None
    return json.loads(matches[0].read_text(encoding="utf-8"))


def run_harvest_cycle_from_plan(
    *,
    root: str | Path = ".",
    plan: dict[str, Any],
    previous_cycle_id: str | None = None,
    allow_controlled_execution: bool = False,
    update_latest_cycle: bool = True,
) -> dict[str, Any]:
    root = Path(root)
    base = root / HARVEST_ROOT
    base.mkdir(parents=True, exist_ok=True)
    cycle_id = str(plan.get("cycle_id"))
    cdir = base / cycle_id
    mechanism_route = plan.get("mechanism_route") or {}

    _write_json(cdir / "correction_plan.json", plan)
    _write_md(cdir / "correction_plan.md", "Correction Plan", plan)
    write_fingerprint(cdir, plan)
    write_semantic_signature(cdir, plan)
    _write_json(cdir / "mechanism_route.json", mechanism_route)
    _write_md(cdir / "mechanism_route.md", "Mechanism Route", mechanism_route)

    if allow_controlled_execution:
        result = run_plan_backtest(plan, output_dir=cdir / "runs" / RUN_ID, root=root)
        executed = int((result.get("execution") or {}).get("executed_count") or (result.get("summary") or {}).get("executed_count") or 0)
        real_execution = validate_real_backtest_result(result)
        if not real_execution.get("valid"):
            executed = 0
    else:
        result = {"status": "dry_run", "execution": {"executed_count": 0}, "best_result": {}}
        executed = 0
        real_execution = {"valid": False, "reason": "dry_run"}
        _write_json(cdir / "runs" / RUN_ID / "result.json", result)

    previous_result = _load_latest_result(base, previous_cycle_id)
    analysis = analyze_result_payload(result, cycle_id=cycle_id)
    analysis["source_result_path"] = str(cdir / "runs" / RUN_ID / "result.json")
    diagnosis = diagnose_analysis(analysis)
    comparison = compare_results(previous_result, result) if previous_result else {
        "schema_version": 1,
        "decision": "continue_same_mainline",
        "baseline_best_result": {},
        "candidate_best_result": result.get("best_result") or {},
        "deltas": {},
        "improvements": [],
        "regressions": [],
    }
    oos_validation = validate_oos_robustness(result)
    failure_attribution = attribute_harvest_failure(result, oos_validation)
    route_state = build_route_state(root, current_route=(mechanism_route or {}).get("mechanism_id"))
    data_request = build_harvest_data_request(mechanism_route)
    research_decision = decide_next_research_branch(
        diagnosis=diagnosis,
        oos_validation=oos_validation,
        failure_attribution=failure_attribution,
        route_state=route_state,
        mechanism_route=mechanism_route,
    )
    portfolio_branch_plan = build_portfolio_branch_plan(
        decision=research_decision,
        current_plan=plan,
        failure_attribution=failure_attribution,
        mechanism_route=mechanism_route,
    )
    v3_next_plan = build_v3_next_cycle_plan(
        current_cycle_id=cycle_id,
        diagnosis=diagnosis,
        oos_validation=oos_validation,
        failure_attribution=failure_attribution,
        route_state=route_state,
        research_decision=research_decision,
        portfolio_branch_plan=portfolio_branch_plan,
        data_request=data_request,
    )
    evidence = {
        "schema_version": 1,
        "cycle_id": cycle_id,
        "evidence": [{"experiment_id": RUN_ID, "status": result.get("status"), "metrics": analysis, "information_gain": "real_backtest_metrics" if executed else "dry_run"}],
        "summary": {"evidence_count": 1, "executed_count": 1 if executed else 0, "executed_backtest_count": executed},
    }
    verdict = {
        "schema_version": 1,
        "cycle_id": cycle_id,
        "decision": comparison.get("decision"),
        "reasoning": (diagnosis.get("failure_classes") or ["no_failure_detected"]) + [f"oos_class={oos_validation.get('oos_class')}"],
        "next_action": f"v3:{research_decision.get('decision')}",
        "manual_approval_required": bool(research_decision.get("manual_approval_required")),
    }
    try:
        next_id = f"cycle_{int(cycle_id.split('_')[-1]) + 1:04d}"
    except Exception:
        next_id = "cycle_next"
    next_plan = {"schema_version": 1, "cycle_id": next_id, "plan_status": v3_next_plan.get("plan_status"), "based_on_cycle": cycle_id, "next_action": verdict["next_action"], "v3_next_cycle_plan": "v3_next_cycle_plan.json"}

    for name, payload, title in [
        ("result_analysis", analysis, "Result Analysis"),
        ("diagnosis", diagnosis, "Diagnosis"),
        ("comparison", comparison, "Comparison"),
        ("oos_validation", oos_validation, "OOS Validation"),
        ("failure_attribution", failure_attribution, "Failure Attribution"),
        ("route_state", route_state, "Route State"),
        ("data_request", data_request, "Data Request"),
        ("research_decision", research_decision, "Research Decision"),
        ("portfolio_branch_plan", portfolio_branch_plan, "Portfolio Branch Plan"),
        ("v3_next_cycle_plan", v3_next_plan, "V3 Next Cycle Plan"),
        ("evidence_ledger", evidence, "Evidence Ledger"),
        ("verdict", verdict, "Verdict"),
        ("next_cycle_plan", next_plan, "Next Cycle Plan"),
    ]:
        _write_json(cdir / f"{name}.json", payload)
        _write_md(cdir / f"{name}.md", title, payload)

    latest = {"cycle_id": cycle_id, "cycle_status": "complete", "verdict": verdict["decision"], "next_action": verdict["next_action"], "manual_approval_required": verdict["manual_approval_required"]}
    if update_latest_cycle:
        _write_json(base / "latest_cycle.json", latest)
    return {
        **latest,
        "fingerprint": fingerprint_plan(plan),
        "semantic_hash": semantic_hash(plan),
        "mechanism_id": mechanism_route.get("mechanism_id"),
        "oos_class": oos_validation.get("oos_class"),
        "research_decision": research_decision.get("decision"),
        "executed_backtest_count": executed,
        "artifacts_dir": str(cdir),
        "best_result": result.get("best_result") or {},
        "failure_classes": diagnosis.get("failure_classes") or [],
        "real_execution": real_execution,
        "started_systemd_daemon": False,
        "scheduled_timer_enabled": False,
    }

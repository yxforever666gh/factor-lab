from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.harvest_backtest_runner import DEFAULT_DATASET_PATH, run_plan_backtest
from factor_lab.harvest_comparative_evaluator import compare_results
from factor_lab.harvest_data_request import build_harvest_data_request
from factor_lab.harvest_diagnostician import diagnose_analysis
from factor_lab.harvest_experiment_fingerprint import fingerprint_plan, is_duplicate_fingerprint, write_fingerprint
from factor_lab.harvest_failure_attribution import attribute_harvest_failure
from factor_lab.harvest_mechanism_routes import select_mechanism_route
from factor_lab.harvest_oos_validator import validate_oos_robustness
from factor_lab.harvest_portfolio_branch_planner import build_portfolio_branch_plan
from factor_lab.harvest_research_decision import decide_next_research_branch
from factor_lab.harvest_result_analyzer import analyze_result_payload
from factor_lab.harvest_route_state import build_route_state
from factor_lab.harvest_self_correction_planner import DEFAULT_WINDOWS, build_correction_plan
from factor_lab.harvest_semantic_duplicate import find_semantic_duplicate, semantic_hash, write_semantic_signature
from factor_lab.harvest_v3_next_plan import build_v3_next_cycle_plan

HARVEST_ROOT = Path("artifacts/harvest_agent")


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_md(path: Path, title: str, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"# {title}\n\n```json\n{json.dumps(payload, ensure_ascii=False, indent=2)}\n```\n", encoding="utf-8")


def _next_cycle_id(base: Path) -> str:
    latest = base / "latest_cycle.json"
    if latest.exists():
        try:
            n = int(str(json.loads(latest.read_text()).get("cycle_id", "cycle_0000")).split("_")[-1]) + 1
            return f"cycle_{n:04d}"
        except Exception:
            pass
    existing = sorted(base.glob("cycle_*"))
    if existing:
        return f"cycle_{int(existing[-1].name.split('_')[-1]) + 1:04d}"
    return "cycle_0001"


def _initial_plan(cycle_id: str, dataset_path: str) -> dict[str, Any]:
    route = select_mechanism_route({"failure_classes": []})
    return {
        "schema_version": 2,
        "cycle_id": cycle_id,
        "based_on_cycle": None,
        "plan_status": "planned",
        "objective": "baseline value-quality cost and OOS matrix",
        "dataset_path": dataset_path,
        "mechanism_route": route,
        "actions": [
            {"type": "set_signal_columns", "signal_columns": ["industry_relative_book_yield", "industry_relative_earnings_yield", "earnings_yield"]},
            {"type": "set_windows", "year_windows": DEFAULT_WINDOWS},
            {"type": "set_holding_counts", "holding_counts": [50, 75, 100]},
            {"type": "restrict_costs", "cost_bps_values": [0, 30, 60]},
        ],
        "success_criteria": {"max_drawdown_min": -0.35, "sharpe_min": 0.7, "positive_at_cost_bps": 30, "min_ok_windows": 2},
    }


def _load_latest_result(base: Path, cycle_id: str | None) -> dict[str, Any] | None:
    if not cycle_id:
        return None
    matches = sorted((base / cycle_id).glob("runs/*/result.json"))
    if not matches:
        return None
    return json.loads(matches[0].read_text(encoding="utf-8"))


def run_harvest_evolution_once(
    *,
    root: str | Path = ".",
    allow_controlled_execution: bool = False,
    previous_cycle_id: str | None = None,
    attempt_index: int = 0,
) -> dict[str, Any]:
    root = Path(root)
    base = root / HARVEST_ROOT
    base.mkdir(parents=True, exist_ok=True)
    cycle_id = _next_cycle_id(base)
    cdir = base / cycle_id
    dataset_path = str(Path(DEFAULT_DATASET_PATH))

    previous_result = _load_latest_result(base, previous_cycle_id)
    previous_analysis = analyze_result_payload(previous_result, cycle_id=previous_cycle_id) if previous_result else None
    previous_diagnosis = diagnose_analysis(previous_analysis) if previous_analysis else None

    if previous_analysis and previous_diagnosis:
        plan = build_correction_plan(previous_analysis, previous_diagnosis, next_cycle_id=cycle_id, attempt_index=attempt_index, dataset_path=dataset_path)
    else:
        plan = _initial_plan(cycle_id, dataset_path)

    fp = fingerprint_plan(plan)
    semantic_duplicate = find_semantic_duplicate(root, plan)
    while is_duplicate_fingerprint(root, fp) or semantic_duplicate is not None:
        attempt_index += 1
        if previous_analysis and previous_diagnosis:
            plan = build_correction_plan(previous_analysis, previous_diagnosis, next_cycle_id=cycle_id, attempt_index=attempt_index, dataset_path=dataset_path)
        else:
            plan = _initial_plan(cycle_id, dataset_path)
            plan["actions"].append({"type": "add_filter", "field": "volatility_20", "operator": "<=", "quantile": max(0.3, 0.7 - attempt_index * 0.1)})
        fp = fingerprint_plan(plan)
        semantic_duplicate = find_semantic_duplicate(root, plan)

    _write_json(cdir / "correction_plan.json", plan)
    _write_md(cdir / "correction_plan.md", "Correction Plan", plan)
    write_fingerprint(cdir, plan)
    write_semantic_signature(cdir, plan)
    mechanism_route = plan.get("mechanism_route") or {}
    _write_json(cdir / "mechanism_route.json", mechanism_route)
    _write_md(cdir / "mechanism_route.md", "Mechanism Route", mechanism_route)

    if allow_controlled_execution:
        result = run_plan_backtest(plan, output_dir=cdir / "runs/value_quality_cost_sensitivity_v1", root=root)
        executed = int((result.get("execution") or {}).get("executed_count") or 0)
    else:
        result = {"status": "dry_run", "execution": {"executed_count": 0}, "best_result": {}}
        executed = 0
        _write_json(cdir / "runs/value_quality_cost_sensitivity_v1/result.json", result)

    analysis = analyze_result_payload(result, cycle_id=cycle_id)
    analysis["source_result_path"] = str(cdir / "runs/value_quality_cost_sensitivity_v1/result.json")
    diagnosis = diagnose_analysis(analysis)
    comparison = compare_results(previous_result, result) if previous_result else {"schema_version": 1, "decision": "continue_same_mainline", "baseline_best_result": {}, "candidate_best_result": result.get("best_result") or {}, "deltas": {}, "improvements": [], "regressions": []}
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
        "evidence": [{"experiment_id": "value_quality_cost_sensitivity_v1", "status": result.get("status"), "metrics": analysis, "information_gain": "real_backtest_metrics" if executed else "dry_run"}],
        "summary": {"evidence_count": 1, "executed_count": 1 if executed else 0, "executed_backtest_count": executed},
    }
    verdict = {
        "schema_version": 1,
        "cycle_id": cycle_id,
        "decision": comparison.get("decision"),
        "reasoning": (diagnosis.get("failure_classes") or ["no_failure_detected"]) + [f"oos_class={oos_validation.get('oos_class')}"] ,
        "next_action": f"v3:{research_decision.get('decision')}",
        "manual_approval_required": bool(research_decision.get("manual_approval_required")),
    }
    next_plan = {"schema_version": 1, "cycle_id": f"cycle_{int(cycle_id.split('_')[-1]) + 1:04d}", "plan_status": v3_next_plan.get("plan_status"), "based_on_cycle": cycle_id, "next_action": verdict["next_action"], "v3_next_cycle_plan": "v3_next_cycle_plan.json"}

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
    _write_json(base / "latest_cycle.json", latest)
    return {**latest, "fingerprint": fp, "semantic_hash": semantic_hash(plan), "mechanism_id": (mechanism_route or {}).get("mechanism_id"), "oos_class": oos_validation.get("oos_class"), "research_decision": research_decision.get("decision"), "executed_backtest_count": executed, "artifacts_dir": str(cdir), "best_result": result.get("best_result") or {}, "failure_classes": diagnosis.get("failure_classes") or []}


def run_harvest_evolution_loop(*, root: str | Path = ".", cycles: int = 1, allow_controlled_execution: bool = False) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    previous: str | None = None
    for i in range(max(0, int(cycles))):
        result = run_harvest_evolution_once(root=root, allow_controlled_execution=allow_controlled_execution, previous_cycle_id=previous, attempt_index=i)
        results.append(result)
        previous = result["cycle_id"]
        if result.get("manual_approval_required"):
            break
    return {"loop_status": "complete", "cycles_requested": cycles, "cycles_run": len(results), "cycles": results, "latest_cycle_id": results[-1]["cycle_id"] if results else None, "started_systemd_daemon": False, "scheduled_timer_enabled": False}

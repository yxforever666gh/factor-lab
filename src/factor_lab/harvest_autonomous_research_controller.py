from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.harvest_backtest_runner import DEFAULT_DATASET_PATH
from factor_lab.harvest_controller_budget import budget_gate, estimate_backtest_count
from factor_lab.harvest_controller_ledger import append_controller_event, write_controller_summary
from factor_lab.harvest_controller_policy import HarvestControllerPolicy
from factor_lab.harvest_cycle_runner import run_harvest_cycle_from_plan
from factor_lab.harvest_v3_plan_loader import classify_v3_next_plan, load_latest_v3_next_plan
from factor_lab.harvest_strategy_governor import load_latest_strategy_plan
from factor_lab.harvest_v3_plan_materializer import materialize_v3_next_plan

HARVEST_ROOT = Path("artifacts/harvest_agent")
AUTONOMOUS_STRATEGY_LAB_ROOT = Path("artifacts/autonomous_strategy_lab")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _controller_run_id() -> str:
    return "controller_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _next_cycle_id(root: Path) -> str:
    base = root / HARVEST_ROOT
    candidates: list[int] = []
    latest = base / "latest_cycle.json"
    if latest.exists():
        try:
            cycle_id = json.loads(latest.read_text(encoding="utf-8")).get("cycle_id", "cycle_0000")
            candidates.append(int(str(cycle_id).split("_")[-1]))
        except Exception:
            pass
    for path in base.glob("cycle_*"):
        try:
            candidates.append(int(path.name.split("_")[-1]))
        except Exception:
            continue
    if candidates:
        return f"cycle_{max(candidates) + 1:04d}"
    return "cycle_0001"


def _load_mechanism_route(root: Path, cycle_id: str | None) -> dict[str, Any]:
    if not cycle_id:
        return {}
    path = root / HARVEST_ROOT / cycle_id / "mechanism_route.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _load_autonomous_strategy_lab_decision(root: Path) -> dict[str, Any] | None:
    latest = root / AUTONOMOUS_STRATEGY_LAB_ROOT / "latest_decision.json"
    controlled = root / AUTONOMOUS_STRATEGY_LAB_ROOT / "controlled_execution_decision.json"
    path = latest if latest.exists() else controlled
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    payload["_source_path"] = str(path)
    return payload


def _classify_autonomous_strategy_lab_decision(decision: dict[str, Any] | None) -> dict[str, Any]:
    if not decision:
        return {"action": "ignore", "reason": "missing_autonomous_strategy_lab_decision"}
    decision_name = decision.get("decision") or decision.get("execution_status") or decision.get("overall_status")
    reason_codes = set(decision.get("reason_codes") or [])
    if decision_name == "continue_route_with_constraints":
        return {"action": "continue", "reason": "autonomous_strategy_lab_continue_route_with_constraints"}
    if decision_name == "manual_review":
        return {"action": "stop", "reason": "autonomous_strategy_lab_manual_review"}
    if decision_name == "request_data" or "coverage_preflight_blocked" in reason_codes:
        return {"action": "stop", "reason": "autonomous_strategy_lab_request_data"}
    if decision_name in {"blocked", "stopped"}:
        return {"action": "stop", "reason": "autonomous_strategy_lab_blocked"}
    return {"action": "ignore", "reason": "autonomous_strategy_lab_no_stop_required"}


def _stop_event(run_id: str, index: int, reason: str, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "controller_run_id": run_id,
        "event_index": index,
        "event_type": "stop",
        "stop_reason": reason,
        "started_systemd_daemon": False,
        "scheduled_timer_enabled": False,
        **(extra or {}),
    }


def _stop_reason_from_classification(classification: dict[str, str]) -> str:
    reason = classification.get("reason") or classification.get("decision") or "not_executable"
    return reason


def run_harvest_autonomous_research_controller(
    *,
    root: str | Path = ".",
    policy: HarvestControllerPolicy | None = None,
    controller_run_id: str | None = None,
    use_latest_strategy_plan: bool = False,
    use_autonomous_strategy_lab_decision: bool = False,
) -> dict[str, Any]:
    root = Path(root)
    policy = policy or HarvestControllerPolicy()
    policy.validate()
    run_id = controller_run_id or _controller_run_id()
    run_dir = root / HARVEST_ROOT / "controller_runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    _write_json(run_dir / "controller_config.json", {
        **policy.__dict__,
        "use_latest_strategy_plan": use_latest_strategy_plan,
        "use_autonomous_strategy_lab_decision": use_autonomous_strategy_lab_decision,
    })

    autonomous_strategy_lab_decision = _load_autonomous_strategy_lab_decision(root) if use_autonomous_strategy_lab_decision else None
    autonomous_strategy_lab_classification = _classify_autonomous_strategy_lab_decision(autonomous_strategy_lab_decision)
    if use_autonomous_strategy_lab_decision:
        _write_json(run_dir / "autonomous_strategy_lab_state.json", {
            "schema_version": 1,
            "controller_run_id": run_id,
            "classification": autonomous_strategy_lab_classification,
            "latest_decision": autonomous_strategy_lab_decision,
        })
        if autonomous_strategy_lab_classification.get("action") == "stop":
            event = _stop_event(run_id, 1, str(autonomous_strategy_lab_classification.get("reason")), {
                "autonomous_strategy_lab_decision": autonomous_strategy_lab_decision,
                "manual_approval_required": autonomous_strategy_lab_classification.get("reason") == "autonomous_strategy_lab_manual_review",
            })
            append_controller_event(run_dir, event)
            _write_json(run_dir / "latest_decision.json", event)
            _write_json(run_dir / "stop_state.json", event)
            summary = write_controller_summary(run_dir, [event])
            summary.update({
                "cycles_requested": policy.max_cycles,
                "max_backtests": policy.max_backtests,
                "used_backtests": 0,
                "artifacts_dir": str(run_dir),
                "autonomous_strategy_lab_reason": autonomous_strategy_lab_classification.get("reason"),
            })
            _write_json(run_dir / "controller_summary.json", summary)
            _write_json(root / HARVEST_ROOT / "latest_controller_run.json", {"controller_run_id": run_id, "artifacts_dir": str(run_dir)})
            return summary

    strategy_plan = load_latest_strategy_plan(root) if use_latest_strategy_plan else None
    if strategy_plan and strategy_plan.get("plan_status") in {"blocked", "stopped"}:
        reason = "strategy_plan_blocked" if strategy_plan.get("plan_status") == "blocked" else "strategy_plan_stopped"
        event = _stop_event(run_id, 1, reason, {
            "strategy_plan_id": strategy_plan.get("strategy_run_id"),
            "strategy_decision": strategy_plan.get("strategy_decision"),
            "manual_approval_required": bool(strategy_plan.get("manual_approval_required")),
        })
        append_controller_event(run_dir, event)
        _write_json(run_dir / "latest_decision.json", event)
        _write_json(run_dir / "stop_state.json", event)
        summary = write_controller_summary(run_dir, [event])
        summary.update({
            "cycles_requested": policy.max_cycles,
            "max_backtests": policy.max_backtests,
            "used_backtests": 0,
            "artifacts_dir": str(run_dir),
            "strategy_plan_id": strategy_plan.get("strategy_run_id"),
            "strategy_decision": strategy_plan.get("strategy_decision"),
        })
        _write_json(run_dir / "controller_summary.json", summary)
        _write_json(root / HARVEST_ROOT / "latest_controller_run.json", {"controller_run_id": run_id, "artifacts_dir": str(run_dir)})
        return summary

    events: list[dict[str, Any]] = []
    used_backtests = 0
    previous_cycle_id: str | None = None

    for event_index in range(1, policy.max_cycles + 1):
        latest_plan = load_latest_v3_next_plan(root)
        if latest_plan is None:
            event = _stop_event(run_id, event_index, "missing_v3_next_cycle_plan")
            append_controller_event(run_dir, event)
            events.append(event)
            break

        source_cycle_id = latest_plan.get("_source_cycle_id")
        previous_cycle_id = str(source_cycle_id) if source_cycle_id else previous_cycle_id
        classification = classify_v3_next_plan(latest_plan)
        if classification.get("decision") != "executable":
            reason = _stop_reason_from_classification(classification)
            event = _stop_event(run_id, event_index, reason, {"based_on_cycle": previous_cycle_id})
            append_controller_event(run_dir, event)
            _write_json(run_dir / "latest_decision.json", event)
            _write_json(run_dir / "stop_state.json", event)
            events.append(event)
            break

        next_cycle_id = _next_cycle_id(root)
        mechanism_route = _load_mechanism_route(root, previous_cycle_id)
        materialized = materialize_v3_next_plan(
            latest_plan,
            next_cycle_id=next_cycle_id,
            dataset_path=str(DEFAULT_DATASET_PATH),
            mechanism_route=mechanism_route,
            strategy_plan=strategy_plan,
        )
        estimated = estimate_backtest_count(materialized)
        gate = budget_gate(estimated_backtests=estimated, used_backtests=used_backtests, max_backtests=policy.max_backtests)
        _write_json(run_dir / "budget_state.json", {
            "schema_version": 1,
            "controller_run_id": run_id,
            "event_index": event_index,
            "estimated_next_backtests": estimated,
            "used_backtests": used_backtests,
            "max_backtests": policy.max_backtests,
            "remaining_backtests": gate.get("remaining"),
            "decision": gate.get("decision"),
            "reason": gate.get("reason"),
        })
        if gate.get("decision") != "allow":
            event = _stop_event(run_id, event_index, str(gate.get("reason")), {
                "cycle_id": None,
                "based_on_cycle": previous_cycle_id,
                "estimated_backtest_count": estimated,
                "used_backtest_count": used_backtests,
                "budget_remaining_backtests": gate.get("remaining"),
            })
            append_controller_event(run_dir, event)
            _write_json(run_dir / "latest_decision.json", event)
            _write_json(run_dir / "stop_state.json", event)
            events.append(event)
            break

        cycle_result = run_harvest_cycle_from_plan(
            root=root,
            plan=materialized,
            previous_cycle_id=previous_cycle_id,
            allow_controlled_execution=policy.allow_controlled_execution,
            update_latest_cycle=policy.allow_controlled_execution,
        )
        executed = int(cycle_result.get("executed_backtest_count") or 0)
        used_backtests += executed
        event = {
            "schema_version": 1,
            "controller_run_id": run_id,
            "event_index": event_index,
            "event_type": "cycle",
            "cycle_id": cycle_result.get("cycle_id"),
            "based_on_cycle": previous_cycle_id,
            "branch": (materialized.get("research_decision") or {}).get("decision") or latest_plan.get("branch"),
            "source_v3_branch": latest_plan.get("branch"),
            "plan_status": latest_plan.get("plan_status"),
            "gate_decision": "allow_controlled_execution" if policy.allow_controlled_execution else "dry_run",
            "estimated_backtest_count": estimated,
            "executed_backtest_count": executed,
            "budget_remaining_backtests": max(0, policy.max_backtests - used_backtests),
            "oos_class": cycle_result.get("oos_class"),
            "research_decision": cycle_result.get("research_decision"),
            "manual_approval_required": bool(cycle_result.get("manual_approval_required")),
            "stop_reason": None,
            "artifact_dir": cycle_result.get("artifacts_dir"),
            "started_systemd_daemon": False,
            "scheduled_timer_enabled": False,
        }
        append_controller_event(run_dir, event)
        _write_json(run_dir / "latest_decision.json", event)
        events.append(event)
        previous_cycle_id = str(cycle_result.get("cycle_id"))

        if not policy.allow_controlled_execution:
            stop = _stop_event(run_id, event_index + 1, "dry_run_complete", {"based_on_cycle": previous_cycle_id})
            append_controller_event(run_dir, stop)
            _write_json(run_dir / "latest_decision.json", stop)
            _write_json(run_dir / "stop_state.json", stop)
            events.append(stop)
            break

        if policy.stop_on_manual_review and cycle_result.get("manual_approval_required"):
            stop = _stop_event(run_id, event_index + 1, "manual_approval_required", {"based_on_cycle": previous_cycle_id})
            append_controller_event(run_dir, stop)
            _write_json(run_dir / "latest_decision.json", stop)
            _write_json(run_dir / "stop_state.json", stop)
            events.append(stop)
            break
        if policy.stop_on_data_request and cycle_result.get("research_decision") == "data_request":
            stop = _stop_event(run_id, event_index + 1, "data_request", {"based_on_cycle": previous_cycle_id})
            append_controller_event(run_dir, stop)
            _write_json(run_dir / "latest_decision.json", stop)
            _write_json(run_dir / "stop_state.json", stop)
            events.append(stop)
            break
        if policy.stop_on_route_stop and cycle_result.get("research_decision") == "stop_route":
            stop = _stop_event(run_id, event_index + 1, "route_stop", {"based_on_cycle": previous_cycle_id})
            append_controller_event(run_dir, stop)
            _write_json(run_dir / "latest_decision.json", stop)
            _write_json(run_dir / "stop_state.json", stop)
            events.append(stop)
            break

    summary = write_controller_summary(run_dir, events)
    summary.update({
        "cycles_requested": policy.max_cycles,
        "max_backtests": policy.max_backtests,
        "used_backtests": used_backtests,
        "artifacts_dir": str(run_dir),
        "autonomous_strategy_lab_reason": autonomous_strategy_lab_classification.get("reason") if use_autonomous_strategy_lab_decision else None,
    })
    _write_json(run_dir / "controller_summary.json", summary)
    _write_json(root / HARVEST_ROOT / "latest_controller_run.json", {"controller_run_id": run_id, "artifacts_dir": str(run_dir)})
    return summary

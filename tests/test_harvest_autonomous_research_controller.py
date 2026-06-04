import json

import pandas as pd

from factor_lab.harvest_autonomous_research_controller import run_harvest_autonomous_research_controller
from factor_lab.harvest_controller_policy import HarvestControllerPolicy


def _write_dataset(root):
    dataset_dir = root / "artifacts/value_route_bucket_aware/runs/value_quality_no_distress_bucket_aware"
    dataset_dir.mkdir(parents=True)
    rows = []
    for date in ["2021-01-01", "2021-01-08", "2022-01-01", "2022-01-08"]:
        for i, ticker in enumerate(["a", "b", "c", "d", "e", "f"]):
            rows.append({
                "date": date,
                "ticker": ticker,
                "industry_relative_book_yield": 6 - i,
                "industry_relative_earnings_yield": i + 1,
                "earnings_yield": [3, 4, 5, 6, 1, 2][i],
                "forward_return_5d": [0.1, 0.08, 0.03, -0.02, -0.03, -0.04][i],
                "volatility_20": [0.1, 0.2, 0.3, 0.7, 0.8, 0.9][i],
                "turnover": [10, 9, 8, 3, 2, 1][i],
                "total_mv": [100, 90, 80, 70, 60, 50][i],
                "pb": [1, 1, 1, 2, 2, 2][i],
                "roe": [0.2, 0.19, 0.18, 0.1, 0.08, 0.05][i],
            })
    path = dataset_dir / "dataset.csv"
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def _write_latest_v3_plan(root, *, plan_status="planned", manual=False):
    base = root / "artifacts/harvest_agent"
    cdir = base / "cycle_0050"
    cdir.mkdir(parents=True)
    (base / "latest_cycle.json").write_text(json.dumps({"cycle_id": "cycle_0050"}))
    plan = {
        "schema_version": 1,
        "cycle_id": "cycle_0051",
        "based_on_cycle": "cycle_0050",
        "plan_status": plan_status,
        "branch": "cost_robustness_branch",
        "manual_approval_required": manual,
        "experiments": [
            {"type": "action", "action": {"type": "set_signal_columns", "signal_columns": ["industry_relative_book_yield"]}},
            {"type": "action", "action": {"type": "restrict_costs", "cost_bps_values": [30]}},
            {"type": "action", "action": {"type": "set_holding_counts", "holding_counts": [2]}},
            {"type": "action", "action": {"type": "set_windows", "year_windows": [{"label": "2021", "start_date": "2021-01-01", "end_date": "2021-12-31"}]}},
        ],
    }
    (cdir / "v3_next_cycle_plan.json").write_text(json.dumps(plan))
    return plan


def _write_strategy_plan(root, *, plan_status="planned", strategy_decision="shrink_search_space"):
    run_dir = root / "artifacts/harvest_agent/strategy_runs/strategy_test"
    run_dir.mkdir(parents=True)
    plan = {
        "schema_version": 1,
        "strategy_run_id": "strategy_test",
        "plan_status": plan_status,
        "strategy_decision": strategy_decision,
        "allowed_branches": ["risk_reduction_branch"],
        "blocked_branches": ["portfolio_construction_branch"],
        "experiment_constraints": {"max_next_backtests": 120, "allowed_cost_bps": [30], "allowed_holding_counts": [2]},
        "manual_approval_required": plan_status != "planned",
        "safety": {"no_timer": True, "no_daemon": True, "no_live_trading": True, "no_automatic_promotion": True},
    }
    (run_dir / "v5_strategy_plan.json").write_text(json.dumps(plan))
    (root / "artifacts/harvest_agent/latest_strategy_run.json").write_text(json.dumps({"strategy_run_id": "strategy_test", "artifacts_dir": "artifacts/harvest_agent/strategy_runs/strategy_test"}))
    return plan


def _write_autonomous_strategy_lab_decision(root, *, decision="request_data", reason_codes=None):
    run_dir = root / "artifacts/autonomous_strategy_lab"
    run_dir.mkdir(parents=True)
    payload = {
        "schema_version": 1,
        "run_id": "asl_test",
        "decision": decision,
        "route_id": "historical_relative_valuation_repair",
        "reason_codes": list(reason_codes or []),
        "controlled_execution_allowed": decision == "continue_route_with_constraints",
        "queue_write_allowed": False,
    }
    (run_dir / "latest_decision.json").write_text(json.dumps(payload))
    return payload


def test_controller_dry_run_materializes_cycle_and_writes_ledger(tmp_path):
    _write_dataset(tmp_path)
    _write_latest_v3_plan(tmp_path)

    summary = run_harvest_autonomous_research_controller(
        root=tmp_path,
        policy=HarvestControllerPolicy(max_cycles=1, max_backtests=10),
    )

    assert summary["cycles_run"] == 1
    assert summary["executed_backtest_count"] == 0
    assert summary["started_systemd_daemon"] is False
    assert summary["scheduled_timer_enabled"] is False
    run_dir = tmp_path / summary["artifacts_dir"] if not str(summary["artifacts_dir"]).startswith(str(tmp_path)) else summary["artifacts_dir"]
    assert (tmp_path / "artifacts/harvest_agent/cycle_0051/correction_plan.json").exists()
    assert json.loads((tmp_path / "artifacts/harvest_agent/latest_cycle.json").read_text())["cycle_id"] == "cycle_0050"
    assert (tmp_path / "artifacts/harvest_agent/controller_runs" / summary["controller_run_id"] / "controller_ledger.jsonl").exists()
    run_dir = tmp_path / "artifacts/harvest_agent/controller_runs" / summary["controller_run_id"]
    assert (run_dir / "latest_decision.json").exists()
    assert (run_dir / "budget_state.json").exists()
    assert "branch_sequence" in json.loads((run_dir / "controller_summary.json").read_text())


def test_controller_controlled_run_executes_metric_bearing_backtests(tmp_path):
    _write_dataset(tmp_path)
    _write_latest_v3_plan(tmp_path)

    summary = run_harvest_autonomous_research_controller(
        root=tmp_path,
        policy=HarvestControllerPolicy(max_cycles=1, max_backtests=10, allow_controlled_execution=True),
    )

    assert summary["cycles_run"] == 1
    assert summary["executed_backtest_count"] > 0
    assert summary["stop_reason"] is None


def test_controller_stops_on_manual_review_before_execution(tmp_path):
    _write_dataset(tmp_path)
    _write_latest_v3_plan(tmp_path, manual=True)

    summary = run_harvest_autonomous_research_controller(
        root=tmp_path,
        policy=HarvestControllerPolicy(max_cycles=1, max_backtests=10, allow_controlled_execution=True),
    )

    assert summary["cycles_run"] == 0
    assert summary["executed_backtest_count"] == 0
    assert summary["stop_reason"] == "manual_approval_required"


def test_controller_blocks_when_budget_would_be_exceeded(tmp_path):
    _write_dataset(tmp_path)
    _write_latest_v3_plan(tmp_path)

    summary = run_harvest_autonomous_research_controller(
        root=tmp_path,
        policy=HarvestControllerPolicy(max_cycles=1, max_backtests=0, allow_controlled_execution=True),
    )

    assert summary["cycles_run"] == 0
    assert summary["executed_backtest_count"] == 0
    assert summary["stop_reason"] == "backtest_budget_exceeded"
    run_dir = tmp_path / "artifacts/harvest_agent/controller_runs" / summary["controller_run_id"]
    assert json.loads((run_dir / "stop_state.json").read_text())["stop_reason"] == "backtest_budget_exceeded"


def test_controller_applies_latest_strategy_plan_when_enabled(tmp_path):
    _write_dataset(tmp_path)
    _write_latest_v3_plan(tmp_path)
    _write_strategy_plan(tmp_path)

    summary = run_harvest_autonomous_research_controller(
        root=tmp_path,
        policy=HarvestControllerPolicy(max_cycles=1, max_backtests=120),
        use_latest_strategy_plan=True,
    )

    assert summary["cycles_run"] == 1
    correction_plan = json.loads((tmp_path / "artifacts/harvest_agent/cycle_0051/correction_plan.json").read_text())
    assert correction_plan["strategy_plan_id"] == "strategy_test"
    assert correction_plan["controller_constraints"]["max_next_backtests"] == 120
    assert correction_plan["research_decision"]["decision"] == "risk_reduction_branch"
    run_dir = tmp_path / "artifacts/harvest_agent/controller_runs" / summary["controller_run_id"]
    controller_summary = json.loads((run_dir / "controller_summary.json").read_text())
    assert controller_summary["branch_sequence"] == ["risk_reduction_branch"]


def test_strategy_plan_blocked_stops_controller_before_execution(tmp_path):
    _write_dataset(tmp_path)
    _write_latest_v3_plan(tmp_path)
    _write_strategy_plan(tmp_path, plan_status="blocked", strategy_decision="request_data")

    summary = run_harvest_autonomous_research_controller(
        root=tmp_path,
        policy=HarvestControllerPolicy(max_cycles=1, max_backtests=120, allow_controlled_execution=True),
        use_latest_strategy_plan=True,
    )

    assert summary["cycles_run"] == 0
    assert summary["executed_backtest_count"] == 0
    assert summary["stop_reason"] == "strategy_plan_blocked"


def test_autonomous_strategy_lab_request_data_stops_controller_before_execution(tmp_path):
    _write_dataset(tmp_path)
    _write_latest_v3_plan(tmp_path)
    _write_autonomous_strategy_lab_decision(tmp_path, decision="request_data")

    summary = run_harvest_autonomous_research_controller(
        root=tmp_path,
        policy=HarvestControllerPolicy(max_cycles=1, max_backtests=10, allow_controlled_execution=True),
        use_autonomous_strategy_lab_decision=True,
    )

    assert summary["cycles_run"] == 0
    assert summary["executed_backtest_count"] == 0
    assert summary["stop_reason"] == "autonomous_strategy_lab_request_data"
    run_dir = tmp_path / "artifacts/harvest_agent/controller_runs" / summary["controller_run_id"]
    stop_state = json.loads((run_dir / "stop_state.json").read_text())
    assert stop_state["autonomous_strategy_lab_decision"]["decision"] == "request_data"
    assert (run_dir / "autonomous_strategy_lab_state.json").exists()


def test_autonomous_strategy_lab_manual_review_stops_controller_before_execution(tmp_path):
    _write_dataset(tmp_path)
    _write_latest_v3_plan(tmp_path)
    _write_autonomous_strategy_lab_decision(tmp_path, decision="manual_review")

    summary = run_harvest_autonomous_research_controller(
        root=tmp_path,
        policy=HarvestControllerPolicy(max_cycles=1, max_backtests=10, allow_controlled_execution=True),
        use_autonomous_strategy_lab_decision=True,
    )

    assert summary["cycles_run"] == 0
    assert summary["executed_backtest_count"] == 0
    assert summary["stop_reason"] == "autonomous_strategy_lab_manual_review"
    run_dir = tmp_path / "artifacts/harvest_agent/controller_runs" / summary["controller_run_id"]
    assert json.loads((run_dir / "stop_state.json").read_text())["manual_approval_required"] is True


def test_autonomous_strategy_lab_continue_route_still_obeys_backtest_budget(tmp_path):
    _write_dataset(tmp_path)
    _write_latest_v3_plan(tmp_path)
    _write_autonomous_strategy_lab_decision(tmp_path, decision="continue_route_with_constraints")

    summary = run_harvest_autonomous_research_controller(
        root=tmp_path,
        policy=HarvestControllerPolicy(max_cycles=1, max_backtests=0, allow_controlled_execution=True),
        use_autonomous_strategy_lab_decision=True,
    )

    assert summary["cycles_run"] == 0
    assert summary["executed_backtest_count"] == 0
    assert summary["stop_reason"] == "backtest_budget_exceeded"
    assert summary["autonomous_strategy_lab_reason"] == "autonomous_strategy_lab_continue_route_with_constraints"

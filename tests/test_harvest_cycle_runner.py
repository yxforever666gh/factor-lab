import pandas as pd

from factor_lab.harvest_cycle_runner import run_harvest_cycle_from_plan


def _write_dataset(root):
    dataset_dir = root / "artifacts/value_route_bucket_aware/runs/value_quality_no_distress_bucket_aware"
    dataset_dir.mkdir(parents=True)
    df = pd.DataFrame({
        "date": ["2021-01-01"] * 6 + ["2021-01-08"] * 6,
        "ticker": ["a", "b", "c", "d", "e", "f"] * 2,
        "industry_relative_book_yield": [6, 5, 4, 3, 2, 1] * 2,
        "industry_relative_earnings_yield": [1, 2, 3, 4, 5, 6] * 2,
        "earnings_yield": [3, 4, 5, 6, 1, 2] * 2,
        "forward_return_5d": [0.1, 0.08, 0.03, -0.02, -0.03, -0.04] * 2,
        "volatility_20": [0.1, 0.2, 0.3, 0.7, 0.8, 0.9] * 2,
        "turnover": [10, 9, 8, 3, 2, 1] * 2,
        "total_mv": [100, 90, 80, 70, 60, 50] * 2,
        "pb": [1, 1, 1, 2, 2, 2] * 2,
        "roe": [0.2, 0.19, 0.18, 0.1, 0.08, 0.05] * 2,
    })
    path = dataset_dir / "dataset.csv"
    df.to_csv(path, index=False)
    return path


def test_run_harvest_cycle_from_plan_writes_standard_artifacts_dry_run(tmp_path):
    dataset_path = _write_dataset(tmp_path)
    plan = {
        "schema_version": 2,
        "cycle_id": "cycle_0001",
        "based_on_cycle": None,
        "plan_status": "planned",
        "dataset_path": str(dataset_path.relative_to(tmp_path)),
        "mechanism_route": {"mechanism_id": "industry_relative_value"},
        "actions": [{"type": "restrict_costs", "cost_bps_values": [30, 60]}],
        "executable": True,
    }

    result = run_harvest_cycle_from_plan(root=tmp_path, plan=plan, previous_cycle_id=None, allow_controlled_execution=False)

    cycle_dir = tmp_path / "artifacts/harvest_agent/cycle_0001"
    assert result["cycle_id"] == "cycle_0001"
    assert result["executed_backtest_count"] == 0
    assert (cycle_dir / "correction_plan.json").exists()
    assert (cycle_dir / "v3_next_cycle_plan.json").exists()
    assert (cycle_dir / "runs/value_quality_cost_sensitivity_v1/result.json").exists()
    assert result["started_systemd_daemon"] is False
    assert result["scheduled_timer_enabled"] is False


def test_run_harvest_cycle_from_plan_executes_real_backtests_when_allowed(tmp_path):
    dataset_path = _write_dataset(tmp_path)
    plan = {
        "schema_version": 2,
        "cycle_id": "cycle_0001",
        "based_on_cycle": None,
        "plan_status": "planned",
        "dataset_path": str(dataset_path.relative_to(tmp_path)),
        "mechanism_route": {"mechanism_id": "industry_relative_value"},
        "actions": [
            {"type": "set_signal_columns", "signal_columns": ["industry_relative_book_yield"]},
            {"type": "restrict_costs", "cost_bps_values": [30]},
            {"type": "set_holding_counts", "holding_counts": [2]},
            {"type": "set_windows", "year_windows": [{"label": "2021", "start_date": "2021-01-01", "end_date": "2021-12-31"}]},
        ],
        "executable": True,
    }

    result = run_harvest_cycle_from_plan(root=tmp_path, plan=plan, previous_cycle_id=None, allow_controlled_execution=True)

    assert result["executed_backtest_count"] > 0
    assert result["real_execution"]["valid"] is True
    assert result["best_result"]

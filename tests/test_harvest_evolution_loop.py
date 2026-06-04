import pandas as pd

from factor_lab.harvest_evolution_loop import run_harvest_evolution_loop


def test_evolution_loop_produces_non_duplicate_result_driven_cycles(tmp_path):
    dataset_dir = tmp_path / "artifacts/value_route_bucket_aware/runs/value_quality_no_distress_bucket_aware"
    dataset_dir.mkdir(parents=True)
    df = pd.DataFrame({
        "date": ["2021-01-01"] * 6 + ["2021-01-08"] * 6 + ["2022-01-01"] * 6 + ["2022-01-08"] * 6,
        "ticker": ["a", "b", "c", "d", "e", "f"] * 4,
        "industry_relative_book_yield": [6, 5, 4, 3, 2, 1] * 4,
        "industry_relative_earnings_yield": [1, 2, 3, 4, 5, 6] * 4,
        "earnings_yield": [3, 4, 5, 6, 1, 2] * 4,
        "forward_return_5d": [0.1, 0.08, 0.03, -0.02, -0.03, -0.04] * 4,
        "volatility_20": [0.1, 0.2, 0.3, 0.7, 0.8, 0.9] * 4,
        "turnover": [10, 9, 8, 3, 2, 1] * 4,
        "total_mv": [100, 90, 80, 70, 60, 50] * 4,
        "pb": [1, 1, 1, 2, 2, 2] * 4,
        "roe": [0.2, 0.19, 0.18, 0.1, 0.08, 0.05] * 4,
    })
    df.to_csv(dataset_dir / "dataset.csv", index=False)
    out = run_harvest_evolution_loop(root=tmp_path, cycles=3, allow_controlled_execution=True)
    assert out["cycles_run"] == 3
    fps = [c["fingerprint"] for c in out["cycles"]]
    assert len(set(fps)) == 3
    assert all(c["executed_backtest_count"] > 0 for c in out["cycles"])
    assert (tmp_path / "artifacts/harvest_agent" / out["latest_cycle_id"] / "correction_plan.json").exists()
    latest_dir = tmp_path / "artifacts/harvest_agent" / out["latest_cycle_id"]
    assert (latest_dir / "semantic_signature.json").exists()
    assert (latest_dir / "mechanism_route.json").exists()
    assert (latest_dir / "oos_validation.json").exists()
    assert out["cycles"][-1]["semantic_hash"]
    assert out["cycles"][-1]["mechanism_id"]
    for artifact in [
        "failure_attribution.json",
        "route_state.json",
        "research_decision.json",
        "data_request.json",
        "v3_next_cycle_plan.json",
    ]:
        assert (latest_dir / artifact).exists()
    assert out["cycles"][-1]["research_decision"]
    assert out["started_systemd_daemon"] is False
    assert out["scheduled_timer_enabled"] is False

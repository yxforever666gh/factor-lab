import json

from factor_lab.harvest_strategy_governor import run_harvest_strategy_governor


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_failed_cycle(root, cycle_id, branch):
    cdir = root / "artifacts/harvest_agent" / cycle_id
    _write_json(cdir / "result_analysis.json", {"cycle_id": cycle_id, "best_sharpe": 0.3, "max_drawdown": -0.56})
    _write_json(cdir / "oos_validation.json", {"oos_class": "fail", "best_sharpe": 0.3, "worst_drawdown": -0.56})
    _write_json(cdir / "research_decision.json", {"decision": branch, "mechanism_id": "industry_relative_value"})
    _write_json(cdir / "semantic_signature.json", {"semantic_hash": f"hash-{cycle_id}"})
    _write_json(cdir / "mechanism_route.json", {"mechanism_id": "industry_relative_value"})


def test_strategy_governor_dry_run_does_not_write_latest_pointer(tmp_path):
    summary = run_harvest_strategy_governor(tmp_path, lookback_cycles=2, write=False)

    assert summary["strategy_status"] == "dry_run"
    assert not (tmp_path / "artifacts/harvest_agent/latest_strategy_run.json").exists()
    assert summary["plan"]["safety"]["no_timer"] is True


def test_strategy_governor_write_creates_strategy_artifacts(tmp_path):
    _write_failed_cycle(tmp_path, "cycle_0056", "portfolio_construction_branch")
    _write_failed_cycle(tmp_path, "cycle_0057", "cost_robustness_branch")

    summary = run_harvest_strategy_governor(tmp_path, lookback_cycles=2, max_next_backtests=120, write=True, strategy_run_id="strategy_test")

    run_dir = tmp_path / summary["artifacts_dir"]
    assert (run_dir / "strategy_evidence.json").exists()
    assert (run_dir / "strategy_decision.json").exists()
    assert (run_dir / "v5_strategy_plan.json").exists()
    pointer = json.loads((tmp_path / "artifacts/harvest_agent/latest_strategy_run.json").read_text())
    assert pointer["strategy_run_id"] == "strategy_test"

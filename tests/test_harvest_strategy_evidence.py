import json

from factor_lab.harvest_strategy_evidence import collect_strategy_evidence, detect_strategy_loops


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_cycle(root, cycle_id, *, branch, oos_class="fail", best_sharpe=0.3, max_drawdown=-0.5, semantic_hash=None, mechanism_id="industry_relative_value"):
    cdir = root / "artifacts/harvest_agent" / cycle_id
    _write_json(cdir / "result_analysis.json", {"cycle_id": cycle_id, "best_sharpe": best_sharpe, "max_drawdown": max_drawdown})
    _write_json(cdir / "oos_validation.json", {"oos_class": oos_class, "best_sharpe": best_sharpe, "worst_drawdown": max_drawdown})
    _write_json(cdir / "failure_attribution.json", {"primary_blockers": ["sharpe_below_threshold", "drawdown_too_deep"]})
    _write_json(cdir / "route_state.json", {"current_route_status": "active"})
    _write_json(cdir / "research_decision.json", {"decision": branch, "mechanism_id": mechanism_id})
    _write_json(cdir / "semantic_signature.json", {"semantic_hash": semantic_hash or f"hash_{cycle_id}"})
    _write_json(cdir / "mechanism_route.json", {"mechanism_id": mechanism_id})
    _write_json(cdir / "v3_next_cycle_plan.json", {"cycle_id": "cycle_next", "plan_status": "planned", "branch": branch})


def test_collect_strategy_evidence_reads_recent_cycles_and_controller_runs(tmp_path):
    _write_cycle(tmp_path, "cycle_0056", branch="portfolio_construction_branch", best_sharpe=0.45, max_drawdown=-0.49)
    _write_cycle(tmp_path, "cycle_0057", branch="cost_robustness_branch", best_sharpe=0.31, max_drawdown=-0.57)
    controller_dir = tmp_path / "artifacts/harvest_agent/controller_runs/controller_test"
    _write_json(controller_dir / "controller_summary.json", {"controller_run_id": "controller_test", "cycles_run": 2, "executed_backtest_count": 72})
    _write_json(tmp_path / "artifacts/harvest_agent/latest_controller_run.json", {"controller_run_id": "controller_test", "artifacts_dir": str(controller_dir)})

    evidence = collect_strategy_evidence(tmp_path, lookback_cycles=2)

    assert evidence["latest_cycle_id"] == "cycle_0057"
    assert evidence["latest_controller_run_id"] == "controller_test"
    assert len(evidence["cycles"]) == 2
    assert evidence["branch_sequence"] == ["portfolio_construction_branch", "cost_robustness_branch"]
    assert evidence["failure_blocker_counts"]["drawdown_too_deep"] == 2
    assert evidence["route_counts"]["industry_relative_value"] == 2


def test_collect_strategy_evidence_handles_missing_artifacts(tmp_path):
    evidence = collect_strategy_evidence(tmp_path, lookback_cycles=5)

    assert evidence["cycles"] == []
    assert evidence["latest_cycle_id"] is None
    assert evidence["latest_controller_run_id"] is None


def test_detect_branch_loop_for_alternating_failed_branches():
    loops = detect_strategy_loops({
        "branch_sequence": [
            "portfolio_construction_branch",
            "cost_robustness_branch",
            "portfolio_construction_branch",
            "cost_robustness_branch",
        ],
        "cycles": [
            {"oos_class": "fail", "best_sharpe": 0.45, "max_drawdown": -0.49},
            {"oos_class": "fail", "best_sharpe": 0.31, "max_drawdown": -0.57},
        ],
        "semantic_hash_counts": {},
    })

    assert loops["loop_detected"] is True
    assert "branch_loop_detected" in loops["reason_codes"]
    assert set(loops["blocked_branches"]) == {"portfolio_construction_branch", "cost_robustness_branch"}


def test_detect_no_improvement_when_sharpe_and_drawdown_worsen():
    loops = detect_strategy_loops({"cycles": [
        {"best_sharpe": 0.45, "max_drawdown": -0.49, "oos_class": "fail"},
        {"best_sharpe": 0.31, "max_drawdown": -0.57, "oos_class": "fail"},
    ]})

    assert "drawdown_not_improving" in loops["reason_codes"]
    assert "sharpe_not_improving" in loops["reason_codes"]


def test_detect_semantic_repeat_limit():
    loops = detect_strategy_loops({"semantic_hash_counts": {"abc": 3}, "cycles": []})

    assert loops["loop_detected"] is True
    assert "semantic_repeat_limit_reached" in loops["reason_codes"]

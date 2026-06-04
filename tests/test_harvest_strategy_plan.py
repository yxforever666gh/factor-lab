import json

from factor_lab.harvest_strategy_plan import build_strategy_plan, write_strategy_plan, write_strategy_summary


def test_build_strategy_plan_contains_required_contract_fields():
    plan = build_strategy_plan(
        strategy_run_id="strategy_test",
        evidence={"latest_cycle_id": "cycle_0057", "latest_controller_run_id": "controller_test"},
        decision={"strategy_decision": "shrink_search_space", "plan_status": "planned"},
        max_next_backtests=120,
    )

    assert plan["schema_version"] == 1
    assert plan["strategy_run_id"] == "strategy_test"
    assert plan["based_on_cycle_id"] == "cycle_0057"
    assert plan["based_on_controller_run_id"] == "controller_test"
    assert plan["experiment_constraints"]["max_next_backtests"] == 120
    assert plan["safety"]["no_live_trading"] is True


def test_write_strategy_plan_writes_json_and_markdown(tmp_path):
    run_dir = tmp_path / "artifacts/harvest_agent/strategy_runs/strategy_test"
    write_strategy_plan(run_dir, {"schema_version": 1, "strategy_decision": "manual_review", "plan_status": "blocked"})

    assert json.loads((run_dir / "v5_strategy_plan.json").read_text())["strategy_decision"] == "manual_review"
    assert (run_dir / "v5_strategy_plan.md").exists()


def test_write_strategy_summary_writes_summary_artifacts(tmp_path):
    run_dir = tmp_path / "strategy_test"
    summary = write_strategy_summary(
        run_dir,
        evidence={"latest_cycle_id": "cycle_0057", "loop_analysis": {"reason_codes": ["branch_loop_detected"]}},
        decision={"strategy_decision": "shrink_search_space", "plan_status": "planned"},
        plan={"strategy_run_id": "strategy_test"},
    )

    assert summary["strategy_decision"] == "shrink_search_space"
    assert (run_dir / "strategy_summary.json").exists()
    assert (run_dir / "strategy_summary.md").exists()

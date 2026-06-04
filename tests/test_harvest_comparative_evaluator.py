from factor_lab.harvest_comparative_evaluator import compare_results


def test_compare_results_detects_improvement_and_regression():
    baseline = {"best_result": {"total_return": 0.9, "sharpe": 0.45, "max_drawdown": -0.49, "cost_bps": 0.0}}
    candidate = {"best_result": {"total_return": 0.6, "sharpe": 0.7, "max_drawdown": -0.32, "cost_bps": 30.0}}
    comp = compare_results(baseline, candidate)
    assert comp["deltas"]["sharpe_delta"] > 0
    assert comp["deltas"]["max_drawdown_delta"] > 0
    assert "total_return_regressed" in comp["regressions"]
    assert comp["decision"] in {"continue_modified_route", "manual_review_for_promotion"}

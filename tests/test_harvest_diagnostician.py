from factor_lab.harvest_diagnostician import diagnose_analysis


def test_diagnose_analysis_maps_metrics_to_failure_classes():
    diagnosis = diagnose_analysis({
        "cycle_id": "cycle_0042",
        "drawdown_too_high": True,
        "sharpe_too_low": True,
        "cost_sensitive": True,
        "window_concentration_risk": True,
        "promotion_ready": False,
    })
    assert diagnosis["cycle_id"] == "cycle_0042"
    assert "drawdown_too_high" in diagnosis["failure_classes"]
    assert "weak_risk_adjusted_return" in diagnosis["failure_classes"]
    assert "zero_cost_best_only" in diagnosis["failure_classes"]
    assert "window_concentration" in diagnosis["failure_classes"]
    assert diagnosis["next_repair_priority"][0] == "reduce_drawdown"
    assert diagnosis["root_cause_hypotheses"]

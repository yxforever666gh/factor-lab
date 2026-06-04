from factor_lab.harvest_oos_validator import validate_oos_robustness


def _result(rows):
    return {
        "schema_version": 2,
        "execution": {"executed_count": len(rows)},
        "results": rows,
        "best_result": max(rows, key=lambda r: r.get("total_return", 0.0), default=None),
    }


def test_validate_oos_robustness_passes_when_cost_and_windows_are_stable():
    payload = _result([
        {"status": "ok", "label": "2020-2021", "cost_bps": 30, "total_return": 0.4, "sharpe": 0.8, "max_drawdown": -0.2},
        {"status": "ok", "label": "2021-2022", "cost_bps": 30, "total_return": 0.3, "sharpe": 0.75, "max_drawdown": -0.25},
        {"status": "ok", "label": "2022-2023", "cost_bps": 60, "total_return": 0.2, "sharpe": 0.71, "max_drawdown": -0.3},
    ])

    validation = validate_oos_robustness(payload)

    assert validation["oos_class"] == "pass"
    assert validation["promotion_manual_review_required"] is True
    assert validation["cost_robust"] is True


def test_validate_oos_robustness_near_miss_for_marginal_sharpe_or_drawdown():
    payload = _result([
        {"status": "ok", "label": "2020-2021", "cost_bps": 30, "total_return": 0.3, "sharpe": 0.68, "max_drawdown": -0.34},
        {"status": "ok", "label": "2021-2022", "cost_bps": 30, "total_return": 0.2, "sharpe": 0.65, "max_drawdown": -0.36},
    ])

    validation = validate_oos_robustness(payload)

    assert validation["oos_class"] == "near_miss"
    assert "sharpe_below_threshold" in validation["reasons"]


def test_validate_oos_robustness_fails_without_cost_positive_windows():
    payload = _result([
        {"status": "ok", "label": "2020-2021", "cost_bps": 30, "total_return": -0.1, "sharpe": 0.9, "max_drawdown": -0.2},
        {"status": "ok", "label": "2021-2022", "cost_bps": 60, "total_return": -0.2, "sharpe": 0.8, "max_drawdown": -0.2},
    ])

    validation = validate_oos_robustness(payload)

    assert validation["oos_class"] == "fail"
    assert validation["cost_robust"] is False

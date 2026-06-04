from factor_lab.harvest_real_execution_guard import validate_real_backtest_result


def test_real_execution_guard_rejects_placeholder():
    result = validate_real_backtest_result({"status": "simulated_controlled_placeholder"})

    assert result == {"valid": False, "reason": "placeholder_status"}


def test_real_execution_guard_rejects_missing_metric_bearing_result():
    result = validate_real_backtest_result({"status": "ok", "execution": {"executed_count": 3}})

    assert result == {"valid": False, "reason": "missing_metric_bearing_result"}


def test_real_execution_guard_accepts_executed_count_and_best_result():
    result = validate_real_backtest_result({
        "status": "ok",
        "execution": {"executed_count": 9},
        "best_result": {"sharpe": 0.8, "max_drawdown": -0.2},
    })

    assert result == {"valid": True, "reason": "metric_bearing_result"}


def test_real_execution_guard_accepts_summary_executed_count():
    result = validate_real_backtest_result({
        "status": "ok",
        "summary": {"executed_count": 9},
        "best_result": {"sharpe": 0.8},
    })

    assert result["valid"] is True

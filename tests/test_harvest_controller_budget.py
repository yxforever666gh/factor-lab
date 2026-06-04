from factor_lab.harvest_controller_budget import budget_gate, estimate_backtest_count


def test_estimate_backtest_count_uses_default_matrix_size():
    assert estimate_backtest_count({"actions": []}) == 27


def test_estimate_backtest_count_uses_latest_plan_actions():
    plan = {
        "actions": [
            {"type": "set_signal_columns", "signal_columns": ["a", "b"]},
            {"type": "restrict_costs", "cost_bps_values": [30, 60]},
            {"type": "set_holding_counts", "holding_counts": [50]},
            {"type": "set_windows", "year_windows": [{"label": "w1"}, {"label": "w2"}]},
        ]
    }

    assert estimate_backtest_count(plan) == 8


def test_budget_gate_allows_within_budget_and_reports_remaining():
    result = budget_gate(estimated_backtests=40, used_backtests=10, max_backtests=100)

    assert result == {"decision": "allow", "reason": "within_budget", "remaining": 50}


def test_budget_gate_blocks_when_estimate_exceeds_remaining():
    result = budget_gate(estimated_backtests=91, used_backtests=10, max_backtests=100)

    assert result["decision"] == "block"
    assert result["reason"] == "backtest_budget_exceeded"
    assert result["remaining"] == 90

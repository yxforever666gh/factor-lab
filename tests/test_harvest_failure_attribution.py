from factor_lab.harvest_failure_attribution import attribute_harvest_failure


def test_empty_result_returns_insufficient_data():
    out = attribute_harvest_failure({"results": []})
    assert out["attribution_class"] == "insufficient_data"
    assert "no_ok_rows" in out["primary_blockers"]


def test_flags_window_drawdown_and_zero_cost_best():
    result = {"results": [
        {"status":"ok","label":"2020","signal_column":"s1","cost_bps":0,"holding_count":50,"sharpe":0.8,"max_drawdown":-0.2,"total_return":0.5},
        {"status":"ok","label":"2021","signal_column":"s1","cost_bps":30,"holding_count":50,"sharpe":0.2,"max_drawdown":-0.6,"total_return":0.1},
        {"status":"ok","label":"2021","signal_column":"s2","cost_bps":30,"holding_count":75,"sharpe":0.1,"max_drawdown":-0.55,"total_return":-0.1},
    ]}
    out = attribute_harvest_failure(result)
    assert "drawdown_concentrated_by_window" in out["primary_blockers"]
    assert "zero_cost_only_best" in out["primary_blockers"]
    assert out["worst_window"]["key"] == "2021"


def test_flags_weak_all_windows():
    result = {"results": [
        {"status":"ok","label":"a","signal_column":"s1","cost_bps":30,"holding_count":50,"sharpe":0.1,"max_drawdown":-0.2,"total_return":0.1},
        {"status":"ok","label":"b","signal_column":"s1","cost_bps":30,"holding_count":50,"sharpe":0.2,"max_drawdown":-0.3,"total_return":0.1},
    ]}
    out = attribute_harvest_failure(result, {"thresholds":{"sharpe_min":0.7}})
    assert "weak_all_windows" in out["primary_blockers"]

import json

from factor_lab.small_institutional_simulation_policy import (
    DEFAULT_SIMULATION_POLICY,
    load_small_institutional_simulation_policy,
)


def test_load_small_institutional_simulation_policy_returns_safe_defaults_when_missing(tmp_path):
    payload = load_small_institutional_simulation_policy(tmp_path / "missing.json")

    assert payload["mode"] == "simulated_backtest_only"
    assert payload["max_combinations_per_run"] == 500
    assert "industry_relative_book_yield" in payload["signal_columns"]
    assert payload["diagnosis_thresholds"]["max_drawdown_limit"] == -0.35


def test_load_small_institutional_simulation_policy_merges_file_overrides(tmp_path):
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(
        json.dumps(
            {
                "max_combinations_per_run": 12,
                "signal_columns": ["roe"],
                "diagnosis_thresholds": {"min_sharpe": 1.1},
            }
        ),
        encoding="utf-8",
    )

    payload = load_small_institutional_simulation_policy(policy_path)

    assert payload["max_combinations_per_run"] == 12
    assert payload["signal_columns"] == ["roe"]
    assert payload["diagnosis_thresholds"]["min_sharpe"] == 1.1
    assert payload["diagnosis_thresholds"]["max_drawdown_limit"] == DEFAULT_SIMULATION_POLICY["diagnosis_thresholds"]["max_drawdown_limit"]


def test_load_small_institutional_simulation_policy_forces_simulated_only_mode(tmp_path):
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(json.dumps({"mode": "live_trading"}), encoding="utf-8")

    payload = load_small_institutional_simulation_policy(policy_path)

    assert payload["mode"] == "simulated_backtest_only"
    assert payload["live_trading_enabled"] is False

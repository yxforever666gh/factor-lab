from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_POLICY_PATH = ROOT / "configs" / "small_institutional_simulation_policy.json"

DEFAULT_SIMULATION_POLICY: dict[str, Any] = {
    "schema_version": 1,
    "mode": "simulated_backtest_only",
    "live_trading_enabled": False,
    "dataset_path": "artifacts/value_route_bucket_aware/runs/value_quality_no_distress_bucket_aware/dataset.csv",
    "signal_columns": ["industry_relative_book_yield", "industry_relative_earnings_yield", "roe"],
    "year_windows": [
        {"label": "2020-2021", "start_date": "2020-01-01", "end_date": "2021-12-31"},
        {"label": "2021-2022", "start_date": "2021-01-01", "end_date": "2022-12-31"},
        {"label": "2022-2023", "start_date": "2022-01-01", "end_date": "2023-12-31"},
    ],
    "holding_counts": [50, 75, 100],
    "rebalance_frequencies": ["monthly", "biweekly"],
    "cost_bps_values": [0, 30, 60],
    "return_column": "forward_return_5d",
    "max_combinations_per_run": 500,
    "diagnosis_thresholds": {
        "max_insufficient_data_ratio": 0.25,
        "max_drawdown_limit": -0.35,
        "min_sharpe": 0.8,
        "max_cost_sensitivity_drop": 0.25,
    },
}


def _deep_merge(base: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_small_institutional_simulation_policy(path: str | Path = DEFAULT_POLICY_PATH) -> dict[str, Any]:
    policy_path = Path(path)
    overrides: dict[str, Any] = {}
    if policy_path.exists():
        try:
            loaded = json.loads(policy_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                overrides = loaded
        except json.JSONDecodeError:
            overrides = {}

    policy = _deep_merge(DEFAULT_SIMULATION_POLICY, overrides)
    # Hard safety overrides: this policy is simulation-only regardless of file input.
    policy["mode"] = "simulated_backtest_only"
    policy["live_trading_enabled"] = False
    return policy

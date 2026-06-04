import json

from factor_lab.harvest_semantic_duplicate import (
    build_semantic_signature,
    find_semantic_duplicate,
    semantic_hash,
    write_semantic_signature,
)


def _plan(actions):
    return {
        "cycle_id": "cycle_0001",
        "dataset_path": "artifacts/data.csv",
        "actions": actions,
        "success_criteria": {"sharpe_min": 0.7},
    }


def test_semantic_hash_ignores_action_order_and_minor_quantile_drift():
    a = _plan([
        {"type": "set_signal_columns", "signal_columns": ["industry_relative_earnings_yield", "earnings_yield"]},
        {"type": "add_filter", "field": "volatility_20", "operator": "<=", "quantile": 0.58},
        {"type": "restrict_costs", "cost_bps_values": [30, 60]},
    ])
    b = _plan([
        {"type": "restrict_costs", "cost_bps_values": [60, 30]},
        {"type": "add_filter", "field": "volatility_20", "operator": "<=", "quantile": 0.61},
        {"type": "set_signal_columns", "signal_columns": ["earnings_yield", "industry_relative_earnings_yield"]},
    ])

    assert build_semantic_signature(a) == build_semantic_signature(b)
    assert semantic_hash(a) == semantic_hash(b)


def test_semantic_hash_changes_for_different_signal_family():
    value_plan = _plan([
        {"type": "set_signal_columns", "signal_columns": ["industry_relative_earnings_yield"]},
    ])
    momentum_plan = _plan([
        {"type": "set_signal_columns", "signal_columns": ["momentum_20"]},
    ])

    assert semantic_hash(value_plan) != semantic_hash(momentum_plan)


def test_find_semantic_duplicate_scans_prior_artifacts(tmp_path):
    plan = _plan([{"type": "restrict_costs", "cost_bps_values": [30, 60]}])
    prior = tmp_path / "artifacts/harvest_agent/cycle_0001"
    path = write_semantic_signature(prior, plan)
    payload = json.loads(path.read_text())

    duplicate = find_semantic_duplicate(tmp_path, plan)

    assert payload["semantic_hash"]
    assert duplicate is not None
    assert duplicate["cycle_id"] == "cycle_0001"
    assert find_semantic_duplicate(tmp_path, _plan([{"type": "restrict_costs", "cost_bps_values": [0]}])) is None

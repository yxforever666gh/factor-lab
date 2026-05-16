from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_lab.pit_non_cashflow_mechanism_preflight import (
    build_coverage_preflight,
    build_direction_preflight,
    build_field_inventory,
    build_mechanism_selection_decision,
    run_non_cashflow_mechanism_preflight,
)


def _sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": ["2020-01-01", "2020-01-01", "2020-01-02", "2020-01-02"],
            "industry": ["A", "A", "A", "A"],
            "forward_return_5d": [0.01, 0.00, 0.02, 0.00],
            "debt_to_assets": [80.0, 20.0, 70.0, 30.0],
            "current_ratio": [1.1, 1.3, None, None],
            "quick_ratio": [0.8, 1.0, None, None],
            "netprofit_yoy": [10.0, -5.0, 9.0, -4.0],
            "tr_yoy": [8.0, 2.0, 7.0, 1.0],
            "operating_cashflow_to_profit": [1.0, 2.0, 1.5, 1.2],
        }
    )


def test_field_inventory_excludes_cashflow_and_ambiguous_legacy_roe() -> None:
    inventory = build_field_inventory(_sample_df())
    rows = {row["field"]: row for row in inventory["rows"]}
    assert rows["operating_cashflow_to_profit"]["usable_for_non_cashflow_pit_preflight"] is False
    assert rows["operating_cashflow_to_profit"]["blocked_reason"] == "cashflow_field_closed_or_excluded_from_non_cashflow_preflight"
    assert rows["roe"]["usable_for_non_cashflow_pit_preflight"] is False
    assert rows["roe"]["blocked_reason"] == "ambiguous_legacy_market_column_not_confirmed_as_pit_surfaced"
    assert rows["debt_to_assets"]["usable_for_non_cashflow_pit_preflight"] is True


def test_coverage_preflight_allows_degraded_debt_only_not_full_distress() -> None:
    df = _sample_df()
    inventory = build_field_inventory(df)
    coverage = build_coverage_preflight(df, inventory)
    by_mech = {m["mechanism_id"]: m for m in coverage["mechanisms"]}
    distress = by_mech["balance_sheet_distress"]
    assert distress["decision"] == "blocked"
    assert distress["degraded_variant"]["mechanism_id"] == "balance_sheet_distress_debt_only"
    assert distress["degraded_variant"]["decision"] == "ready_for_direction_preflight"


def test_direction_preflight_tests_raw_and_reversed() -> None:
    df = _sample_df()
    inventory = build_field_inventory(df)
    direction = build_direction_preflight(df, inventory)
    debt = {row["field"]: row for row in direction["fields"]}["debt_to_assets"]
    variants = {v["variant"] for v in debt["variants"]}
    assert variants == {"raw", "reversed"}
    assert debt["best_variant"] in variants


def test_decision_uses_prior_failed_low_debt_to_stop(tmp_path: Path) -> None:
    df = _sample_df()
    inventory = build_field_inventory(df)
    coverage = build_coverage_preflight(df, inventory)
    direction = build_direction_preflight(df, inventory)
    prior = tmp_path / "artifacts" / "value_route_bucket_aware" / "daemon_runs" / "value_trap_filter_quality_confirmation"
    prior.mkdir(parents=True)
    (prior / "bucket_aware_portfolio_results.json").write_text(json.dumps([{"factor_name": "repaired_debt_to_assets_reverse", "pass_gate": False}]), encoding="utf-8")
    decision = build_mechanism_selection_decision(inventory, coverage, direction, root=tmp_path)
    assert decision["decision"] == "stop_pit_value_trap_expansion_no_non_cashflow_mechanism_passed_preflight"
    assert "prior_controlled_low_debt_probe_failed_or_non_incremental" in decision["reasons"]


def test_run_non_cashflow_mechanism_preflight_writes_artifacts(tmp_path: Path) -> None:
    dataset = tmp_path / "dataset.csv"
    _sample_df().to_csv(dataset, index=False)
    out = tmp_path / "out"
    payload = run_non_cashflow_mechanism_preflight(dataset, out)
    assert payload["decision"]["decision"] in {
        "stop_pit_value_trap_expansion_no_non_cashflow_mechanism_passed_preflight",
        "recommend_single_non_cashflow_controlled_probe_plan",
    }
    assert (out / "field_inventory.json").exists()
    assert (out / "coverage_preflight.md").exists()
    assert (out / "direction_preflight.json").exists()
    assert (out / "mechanism_selection_decision.md").exists()

from __future__ import annotations

import json
from pathlib import Path

from factor_lab.release_integrity import canonical_payload_sha256, file_sha256


ROOT = Path(__file__).resolve().parents[2]
PATH = ROOT / "protocols/13.0-profit-first-real-share-closure.json"
PAYLOAD = "7989478d24cc4597a7066eab5a737e698d3647d5a70704f85854a744ec3fa9d8"
FILE = "9973cbdeb0e2d421e227afe7d22ffa9074e0f847041a8025b294cdb5f642fef2"


def _protocol():
    return json.loads(PATH.read_text(encoding="utf-8"))


def test_13_0_protocol_hashes_and_exact_12_0_targets() -> None:
    value = _protocol()
    assert value["payload_sha256"] == canonical_payload_sha256(value) == PAYLOAD
    assert file_sha256(PATH) == FILE
    inherited = value["inherited_stock_route"]
    assert inherited["development_targets_payload_sha256"] == (
        "1022288372cd07f97b0e963b670c3a7e9ddfc94414b4d21cb7cbea9c643e76be"
    )
    assert inherited["universe_factor_gate_topn_or_weight_change_allowed"] is False


def test_13_0_diemeng_contract_is_execution_only_and_calibrated() -> None:
    value = _protocol()
    source = value["diemeng_minute_source"]
    assert source["target_or_signal_input_allowed"] is False
    assert source["load_after_target_and_order_scope_freeze"] is True
    assert source["exact_open_auction_fill_proven"] is False
    assert source["level"] == "1min"
    assert source["unit_contract_id"].endswith("-v5")
    assert source["capture_range"].startswith(
        "09:30:00 opening-price evidence"
    )
    audit = source["auction_anchor_v6_pre_return_audit"]
    assert audit["candidate_pair_count"] == 4729
    assert audit["positive_liquidity_row_daily_open_mismatch_count"] == 0
    assert audit[
        "zero_liquidity_preclose_placeholder_with_daily_open_mismatch_count"
    ] == 6
    assert audit["strategy_return_opened_before_refreeze"] is False
    assert "{1,100}" in source["unit_inference"]
    examples = source["observed_pre_2016_1min_coverage_examples_not_a_global_boundary"]
    assert set(examples) == {
        "000001.SZ",
        "000002.SZ",
        "600000.SH",
        "600519.SH",
        "002024.SZ",
        "300001.SZ",
    }
    assert all(date < "2016-01-01" for date in examples.values())
    calibration = source["accepted_consumed_window_aggregate_unit_calibration_v5"]
    assert calibration["sample_count"] == 11
    assert calibration["both_volume_multiplier_regimes_observed"] == [1, 100]
    assert calibration["unit_validation_granularity"] == (
        "consumed_five_minute_window_aggregate"
    )
    assert calibration["aggregate_vwap_ohlc_tolerance_rmb"] == 0.01
    assert calibration["strategy_return_opened"] is False


def test_13_0_execution_has_causal_sell_then_buy_windows() -> None:
    execution = _protocol()["real_share_execution"]
    windows = execution["execution_windows"]
    assert "09:35" in windows["A_full_exits"]
    assert "after-A account NAV" in windows["A_observable_at"]
    assert "09:37..09:41" in windows["B_partial_reductions"]
    assert "09:42" in windows["B_observable_at"]
    assert "B close*1.01" in windows["C_buys"]
    assert execution["buy_limit_premium_over_observable_B_close"] == 0.01
    assert "cumulative absolute A+B+C" in execution["capacity"]
    assert "window_amount_rmb" in execution["impact_formula"]
    assert "daily-open fallback forbidden" in _protocol()["diemeng_minute_source"][
        "missing_required_partition"
    ]
    stages = _protocol()["diemeng_minute_source"]["development_data_stages"]
    assert stages["stage_1_candidate_only"]["pair_count"] == 4729
    assert stages["stage_2_all_roles_only_after_stage_1_pass"]["pair_count"] == 33984
    assert stages["stage_1_result_cannot_change_stage_2_scope_or_gate"] is True
    assert "exact file-hash set" in stages["stage_1_implementation_freeze"]
    gate = _protocol()["phase_gate_applied_identically_to_development_and_selection"]
    assert gate["target_gap_fill_ratio_at_least"] == 0.98
    assert gate["capacity_limited_target_gap_ratio_at_most"] == 0.02


def test_13_0_daily_open_result_is_disqualified_and_selection_closed() -> None:
    closure = _protocol()["closure"]
    diagnostic = closure["daily_open_hypothetical_diagnostic"]
    assert diagnostic["eligible_as_real_share_evidence"] is False
    assert diagnostic["formal_phase_gate_complete"] is False
    assert closure["minute_sequential_real_share_return_opened"] is False
    assert closure["winner_freeze_created"] is False
    assert closure["selection_opened"] is False
    assert closure["selection_open_requires_development_freeze_committed_and_pushed"] is True

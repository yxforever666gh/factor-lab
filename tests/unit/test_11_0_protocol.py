from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from factor_lab.release_integrity import canonical_payload_sha256


ROOT = Path(__file__).resolve().parents[2]
PROTOCOL_PATH = ROOT / "protocols" / "11.0-results-first-dual-confirm-blend.json"
PRIOR_PROTOCOL_PATH = ROOT / "protocols" / "10.1-quarterly-prospective-cycle.json"
PRIOR_EVIDENCE_PATH = (
    ROOT / "protocols" / "evidence" / "10.1" / "historical-asof-dry-run.json"
)
REFERENCE_PROTOCOL_PATH = ROOT / "protocols" / "10.0-results-first-quarterly-borda.json"
REFERENCE_EVIDENCE_PATH = (
    ROOT / "protocols" / "evidence" / "10.0" / "results-first-diagnostic.json"
)
PROTOCOL_PAYLOAD = "d23739b85fa02d0cfeca977ba5f60fe003ae5753a387f7b10fa611a6688ae0bf"
PROTOCOL_FILE_SHA256 = "8c6b20996e1e735a020fd71a31b0401570948549a041c5f3848a3dd19ae8fc7c"


def _read(path: Path = PROTOCOL_PATH) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _set_nested(value: Any, path: tuple[Any, ...], replacement: Any) -> None:
    cursor = value
    for key in path[:-1]:
        cursor = cursor[key]
    cursor[path[-1]] = replacement


def test_11_0_protocol_is_exactly_self_hashed() -> None:
    protocol = _read()
    assert protocol["payload_sha256"] == canonical_payload_sha256(protocol)
    assert protocol["payload_sha256"] == PROTOCOL_PAYLOAD
    assert hashlib.sha256(PROTOCOL_PATH.read_bytes()).hexdigest() == PROTOCOL_FILE_SHA256
    assert protocol["release"] == "11.0"
    assert protocol["direction_change"] is True
    assert protocol["route"] == "quarterly_dual_confirm_top3_borda_blend_75_25"
    assert protocol["status"].startswith("frozen_after_fully_exposed_two_stage")


def test_11_0_binds_the_exact_published_10_1_release_and_zero_outcomes() -> None:
    protocol = _read()
    prior = protocol["prior_release"]
    assert prior["tag"] == "10.1"
    assert prior["annotated_tag_object"] == "4eb27037a855e8f5ff12397b07cdfb20f27b405f"
    assert prior["peeled_commit"] == "3c777777ba3446b3302fc444324778964f6a07b8"
    for key, path, file_hash, payload_hash in (
        (
            "protocol",
            PRIOR_PROTOCOL_PATH,
            "81240134127de2fedde6e231f8a3a02dd74950ff9da67e5298e71834c61843b5",
            "0c3f2240cc404c1084230f1efbfe3f9fd3f0fa73dbbdc69ec63e5465ef7610ca",
        ),
        (
            "historical_asof_evidence",
            PRIOR_EVIDENCE_PATH,
            "888313dd86c9c15bf6e915d087a784c1a8e4d48e85f3832b4e0945e77e3e27c3",
            "0d2103896410f8800cf9351cb8fb31b807df7ff06c79413b0c2ed45fbc3fed47",
        ),
    ):
        binding = prior[key]
        value = _read(path)
        assert binding["file_sha256"] == file_hash == hashlib.sha256(path.read_bytes()).hexdigest()
        assert binding["payload_sha256"] == payload_hash == canonical_payload_sha256(value)
    assert prior["prospective_state_at_direction_change"] == {
        "decision_count": 0,
        "confirmed_outcome_count": 0,
        "may_be_relabelled_as_11_0_evidence": False,
    }


def test_11_0_binds_the_exact_published_10_0_formula_and_metrics() -> None:
    reference = _read()["published_10_0_reference"]
    for key, path, file_hash, payload_hash in (
        (
            "protocol",
            REFERENCE_PROTOCOL_PATH,
            "6a949ce4374f407a6084053a08b76dedbb3f1478fbe56edf233bb23befe730dd",
            "dc79550ee9fefe4fdb01f54fe0c299a40c2d118a687f6e5571156dff5701cb7b",
        ),
        (
            "evidence",
            REFERENCE_EVIDENCE_PATH,
            "954be9b434d3d5c7c06ddac1f276ac032248b420956185cc372ae352685b4e89",
            "18c9fb75f79cf71572f65a3eade0d2af8a018e7b8aef066fa8a30dce1f721253",
        ),
    ):
        binding = reference[key]
        value = _read(path)
        assert binding["file_sha256"] == file_hash == hashlib.sha256(path.read_bytes()).hexdigest()
        assert binding["payload_sha256"] == payload_hash == canonical_payload_sha256(value)


def test_11_0_discloses_both_scratch_stages_without_hiding_failed_candidates() -> None:
    scratch = _read()["fully_exposed_scratch"]
    stage1 = scratch["stage_1_quarterly_concentration_search"]
    assert stage1["status"] == "no_candidate_passed"
    assert stage1["external_script_file_sha256"] == (
        "5a1958609cdc404103e646435c92c6b066fc5344c77244b077dff817abcec391"
    )
    assert stage1["external_result_file_sha256"] == (
        "749444e91d1cbf84537d98853e0bfa0e0bd462d667c68d33e268d94f6d71092c"
    )
    assert stage1["external_result_payload_sha256"] == (
        "cc34efe5a5a07e6741787e01b24368270e0b204553c800ecf4bae79f4983f826"
    )
    assert [row["id"] for row in stage1["candidate_order_and_results"]] == [
        "top3_equal",
        "dual_confirm_top3_equal",
        "breadth_scaled_top3_equal",
        "top3_borda",
        "top2_borda",
        "mean_gate_top3_equal",
        "buffered_top3_equal",
        "semiannual_top3_equal",
    ]
    assert len(stage1["candidate_order_and_results"]) == 8
    assert not any(
        row["all_return_and_execution_gates_passed"]
        for row in stage1["candidate_order_and_results"]
    )
    assert stage1["selected_candidate_id"] is None

    stage2 = scratch["stage_2_disclosed_dual_confirm_blend_search"]
    assert stage2["status"] == "winner_found"
    assert stage2["parent_result_file_sha256"] == stage1["external_result_file_sha256"]
    assert stage2["parent_result_payload_sha256"] == stage1["external_result_payload_sha256"]
    assert stage2["external_script_file_sha256"] == (
        "490aa1bbe3e16282da6d0af44de4f2b105048a95c2bea7a31bc8e57044c4f069"
    )
    assert stage2["external_result_file_sha256"] == (
        "2b8553d08d15c3bc9a9cb64186f119eefa3bba813c45b09f55331244e5a0520c"
    )
    assert stage2["external_result_payload_sha256"] == (
        "0688e0c8555d535b7d2ed96a492fdf71362a8b39b67124a3b288c38e412958a0"
    )
    rows = stage2["candidate_order_and_results"]
    assert [(row["id"], row["dual_weight"], row["borda_weight"]) for row in rows] == [
        ("dual25_borda75", 0.25, 0.75),
        ("dual50_borda50", 0.5, 0.5),
        ("dual75_borda25", 0.75, 0.25),
    ]
    assert [row["all_return_and_execution_gates_passed"] for row in rows] == [
        False,
        True,
        True,
    ]
    assert rows[2]["minimum_cagr_edge"] > rows[1]["minimum_cagr_edge"]
    assert stage2["selected_candidate_id"] == "dual75_borda25"


def test_11_0_freezes_the_exact_dual_confirm_and_published_borda_blend() -> None:
    strategy = _read()["frozen_strategy"]
    assert strategy["strategy_id"] == "quarterly_dual_confirm_top3_borda_blend_75_25"
    assert strategy["session_index_semantics"].endswith(
        "t-252, t-126 and t-21 are exact earlier official-session indices"
    )
    assert "t-252" in strategy["long_momentum_formula"]
    assert "t-126" in strategy["short_momentum_formula"]
    assert strategy["dual_positive_set"] == "D(t) = {i in P12(t): m6_i(t) > 0}"
    assert "m12 descending" in strategy["ranking"]
    assert strategy["dual_top_k"] == 3
    assert strategy["dual_weight"] == 0.75
    assert strategy["published_10_0_borda_weight"] == 0.25
    assert "w_i=0.75*d_i+0.25*b_i" in strategy["blend_formula"]
    assert strategy["required_endpoint_rule"]["long_window"].startswith("all six assets")
    assert "both components and the final target" in strategy["required_endpoint_rule"][
        "short_window"
    ]
    assert strategy["stress_reuses_base_targets_exactly"] is True
    assert strategy["future_tail_prefix_invariance_required"] is True
    assert strategy["parameter_or_formula_change_after_selection"] is False


def test_11_0_gates_every_period_at_both_costs_against_all_three_references() -> None:
    protocol = _read()
    periods = protocol["fully_exposed_periods"]
    assert list(periods) == ["D1", "D2", "D3", "full"]
    assert all(period["fresh_cash"] is True for period in periods.values())
    assert [periods[key]["target_signal_count"] for key in periods] == [19, 12, 15, 46]
    execution = protocol["inherited_execution_contract"]
    assert execution["base_cost_bps_per_side"] == 8.0
    assert execution["stress_cost_bps_per_side"] == 16.0
    assert execution["initial_capital_rmb"] == 1_000_000.0
    assert execution["lot_size_units"] == 100
    assert execution["capacity_limit_fraction_of_signal_date_adv20"] == 0.1

    gate = protocol["results_first_gate"]
    assert gate["candidate_cagr_at_least_matching_cash_static_and_published_10_0_plus"] == 0.005
    assert gate["stress_cagr_strictly_above_matching_published_10_0_base_cagr"] is True
    assert gate["requested_notional_fill_ratio_at_least"] == 0.98
    assert gate["capacity_limited_requested_notional_ratio_at_most"] == 0.02
    assert gate["nav_reconciliation_error_at_most"] == 1e-8
    assert gate["base_stress_targets_exact"] is True
    assert gate["target_prefix_mismatch_count_at_most"] == 0
    assert gate["all_eight_period_cost_roles_must_pass"] is True
    assert gate["pooled_period_rescue"] is False
    assert len(gate["applies_to_each"]) == 8


def test_11_0_selected_scratch_metrics_pass_every_frozen_return_and_execution_gate() -> None:
    selected = _read()["selected_scratch_evidence"]
    assert selected["candidate_id"] == "dual75_borda25"
    assert selected["minimum_cagr_edge"] == pytest.approx(0.011575950499677967)
    assert selected["target_prefix_mismatch_count"] == 0
    for period in ("D1", "D2", "D3", "full"):
        values = selected["periods"][period]
        base = values["base"]
        stress = values["stress"]
        assert base["cagr"] >= max(
            values["published_10_0_base_cagr"],
            values["static_base_cagr"],
            values["cash_base_cagr"],
        ) + 0.005
        assert stress["cagr"] >= max(
            values["published_10_0_stress_cagr"],
            values["static_stress_cagr"],
            values["cash_stress_cagr"],
        ) + 0.005
        assert stress["cagr"] > values["published_10_0_base_cagr"]
        for role in (base, stress):
            assert role["fill_ratio"] >= 0.98
            assert role["capacity_limited_ratio"] <= 0.02
            assert role["max_abs_accounting_error"] <= 1e-8
        assert values["passed"] is True
    assert selected["all_return_and_execution_gates_passed"] is True


def test_11_0_is_fully_exposed_and_forbids_alpha_profit_and_stability_claims() -> None:
    protocol = _read()
    disclosure = protocol["results_first_selection_disclosure"]
    assert disclosure["all_D1_D2_D3_and_full_outcomes_observed_before_both_search_stages"] is True
    assert disclosure["historical_rule_was_not_selected_at_each_historical_time"] is True
    assert disclosure["selection_is_independent_oos"] is False
    assert disclosure["stage_count"] == 2
    assert disclosure["total_candidate_evaluation_count"] == 11
    claim = protocol["claim_contract"]
    assert claim == {
        "evidence_class": "fully_exposed_results_first_causal_historical_diagnostic",
        "independent_oos": False,
        "alpha_claim_allowed": False,
        "profit_claim_allowed": False,
        "stable_future_profit_claim_allowed": False,
        "investment_recommendation_allowed": False,
        "fresh_future_evidence_required": True,
        "historical_pass_interpretation": "post-selection fully exposed public-history diagnostic only",
    }


@pytest.mark.parametrize(
    ("path", "replacement"),
    [
        (("prior_release", "peeled_commit"), "0" * 40),
        (("results_first_selection_disclosure", "selection_is_independent_oos"), True),
        (("fully_exposed_scratch", "stage_1_quarterly_concentration_search", "status"), "winner_found"),
        (("fully_exposed_scratch", "stage_2_disclosed_dual_confirm_blend_search", "selected_candidate_id"), "dual50_borda50"),
        (("frozen_strategy", "short_momentum_formula"), "future momentum"),
        (("frozen_strategy", "dual_weight"), 0.5),
        (("frozen_strategy", "published_10_0_borda_weight"), 0.5),
        (("results_first_gate", "candidate_cagr_at_least_matching_cash_static_and_published_10_0_plus"), 0.0),
        (("results_first_gate", "stress_cagr_strictly_above_matching_published_10_0_base_cagr"), False),
        (("results_first_gate", "requested_notional_fill_ratio_at_least"), 0.0),
        (("selection_contract", "parameter_or_formula_change_after_selection"), True),
        (("claim_contract", "independent_oos"), True),
        (("claim_contract", "profit_claim_allowed"), True),
        (("claim_contract", "stable_future_profit_claim_allowed"), True),
    ],
)
def test_11_0_rehashed_relaxations_do_not_match_frozen_payload(
    path: tuple[Any, ...], replacement: Any
) -> None:
    changed = copy.deepcopy(_read())
    _set_nested(changed, path, replacement)
    changed["payload_sha256"] = canonical_payload_sha256(changed)
    assert changed["payload_sha256"] != PROTOCOL_PAYLOAD

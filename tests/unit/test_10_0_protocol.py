from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from factor_lab.release_integrity import canonical_payload_sha256


ROOT = Path(__file__).resolve().parents[2]
PROTOCOL_PATH = ROOT / "protocols" / "10.0-results-first-quarterly-borda.json"
PROTOCOL_PAYLOAD = "dc79550ee9fefe4fdb01f54fe0c299a40c2d118a687f6e5571156dff5701cb7b"
PROTOCOL_FILE_SHA256 = "6a949ce4374f407a6084053a08b76dedbb3f1478fbe56edf233bb23befe730dd"


def _read(path: Path = PROTOCOL_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _set_nested(value: Any, path: tuple[Any, ...], replacement: Any) -> None:
    cursor = value
    for key in path[:-1]:
        cursor = cursor[key]
    cursor[path[-1]] = replacement


def test_10_0_protocol_is_exactly_self_hashed() -> None:
    protocol = _read()
    assert protocol["payload_sha256"] == canonical_payload_sha256(protocol)
    assert protocol["payload_sha256"] == PROTOCOL_PAYLOAD
    assert hashlib.sha256(PROTOCOL_PATH.read_bytes()).hexdigest() == PROTOCOL_FILE_SHA256


def test_10_0_binds_published_9_0_and_retained_source() -> None:
    protocol = _read()
    prior = protocol["prior_release"]
    assert prior["tag"] == "9.0"
    assert prior["annotated_tag_object"] == "c5e00f055183cceab44e3f8d182727e198af5714"
    assert prior["peeled_commit"] == "ed7627c974d9d04cd653be61b2966397e075719f"
    expected = {
        "protocol": (
            "protocols/9.0-causal-volatility-balanced-budget.json",
            "19ecf56b5bd9c8b42b9f4df50761f719e2ca544eaea959a88c62d0ea4178d620",
            "f6c7cce39e8b9a1ae5df10965a2dd607916095b2caf24fcf0a29b625c5bafc3e",
        ),
        "winner_freeze": (
            "protocols/evidence/9.0/winner-freeze.json",
            "98bdff4454b1a9430ca5f343bc4ce08a63924701b3a17e780f57c2454b3b413b",
            "430b45eec730084a3d82e7d392bf609e533d5c7a98b5623f9d13a471171495a7",
        ),
        "historical_audit": (
            "protocols/evidence/9.0/historical-audit.json",
            "737ccdd9146334732a6e6a60e52423e45f0e7b9eb76837200b232e2a34601018",
            "7a034510cc38aaca5ea2b2113265c2ff2b984c302f366cd68f34f8c73af98681",
        ),
        "terminal_result": (
            "protocols/evidence/9.0/result.json",
            "816bdb1837b75a8110bb863fa5584dc54166107a26671a402d17f5dc23f4f076",
            "3b6fbcab3dafb1086be3062109d02c1c05f408d30913dc15146ed2b7eb3aa7b2",
        ),
    }
    for key, (relative, file_hash, payload_hash) in expected.items():
        binding = prior[key]
        path = ROOT / relative
        value = _read(path)
        assert binding == {
            "path": relative,
            "file_sha256": file_hash,
            "payload_sha256": payload_hash,
        }
        assert hashlib.sha256(path.read_bytes()).hexdigest() == file_hash
        assert value["payload_sha256"] == canonical_payload_sha256(value) == payload_hash
    source = prior["retained_audit_source_manifest"]
    assert source["payload_sha256"] == (
        "050ad4ddcb86dc4fbc71befad54c400b48a44f72ab6fecc33936b6da0c8f9aff"
    )
    assert source["price_end_date"] == "2026-08-28"


def test_10_0_discloses_results_first_selection_and_external_hashes() -> None:
    protocol = _read()
    disclosure = protocol["results_first_selection_disclosure"]
    prototypes = protocol["fully_exposed_prototypes"]
    assert disclosure["all_2015_2026_market_outcomes_observed_before_selection"] is True
    assert disclosure["selection_is_independent_oos"] is False
    assert disclosure["selected_strategy_id"] == protocol["route"]
    assert disclosure["post_selection_parameter_or_formula_change_allowed"] is False
    assert set(prototypes) == {
        "monthly_top2_inverse_volatility_dual_momentum",
        "monthly_online_three_expert_selector",
        "quarterly_cash_excess_borda",
    }
    assert sum(value["selected"] for value in prototypes.values()) == 1
    assert prototypes["quarterly_cash_excess_borda"]["selected"] is True
    expected = {
        "monthly_top2_inverse_volatility_dual_momentum": (
            "3b0268399369cf03c3062a3bc59f29b187959ddae35664b460e81a4ef80b472e",
            "64d6e06054f6d8c248520a0c478c764a71db2e03012b0871f8694e66c02ca801",
        ),
        "monthly_online_three_expert_selector": (
            "8bba7fd6095d61d38b9f574c7e1a968162a33fb2922a8369e8216e4d3b26bea1",
            "9d4f0697d18aca4053923f10820cec1a8c22999ba3114071f13dedf56b07014c",
        ),
        "quarterly_cash_excess_borda": (
            "5162dc747320ee91e38cea72382550c7cff688308759485859086d8462ecb54d",
            "db820d0227d7c27f05f3fbee7cdd967de384c5fa1766442c94904c7fe2b73887",
        ),
    }
    for name, (script_hash, result_hash) in expected.items():
        assert prototypes[name]["external_script_file_sha256"] == script_hash
        assert prototypes[name]["external_result_file_sha256"] == result_hash


def test_10_0_formula_is_exact_quarterly_session_borda() -> None:
    protocol = _read()
    strategy = protocol["frozen_strategy"]
    assert protocol["route"] == strategy["strategy_id"]
    assert strategy["signal_schedule"] == (
        "last official SSE session of each natural calendar quarter after close"
    )
    assert strategy["momentum_formula"] == (
        "m_i(t) = log(TRI_i[t-21] / TRI_i[t-252]) - "
        "log(TRI_511880.SH[t-21] / TRI_511880.SH[t-252])"
    )
    assert "all six assets" in strategy["required_endpoint_rule"]
    assert "100% 511880.SH" in strategy["required_endpoint_rule"]
    assert strategy["positive_set"].endswith("m_i(t) > 0}")
    assert strategy["borda_score"] == (
        "for n = |P| and one-based rank r_i, q_i = n - r_i + 1"
    )
    assert strategy["target_weight"] == (
        "w_i = q_i / (n*(n+1)/2) for i in P; all other risk weights are zero; "
        "w_cash = 1 - math.fsum(risk weights); if P is empty w_cash = 1"
    )
    assert strategy["top_k"] is None
    assert strategy["volatility_weighting"] is False
    assert strategy["base_budget_multiplier"] is False
    assert strategy["parameter_grid"] is False
    assert strategy["stress_reuses_base_targets_exactly"] is True
    assert strategy["future_tail_prefix_invariance_required"] is True


def test_10_0_results_gate_is_return_first_and_applies_to_all_six_roles() -> None:
    gate = _read()["results_first_gate"]
    assert gate["applies_to_each"] == [
        "D1.base_8bp",
        "D1.stress_16bp",
        "D2.base_8bp",
        "D2.stress_16bp",
        "D3.base_8bp",
        "D3.stress_16bp",
    ]
    assert gate["candidate_cagr_strictly_above_matching_investable_cash_cagr"] is True
    assert gate["candidate_cagr_strictly_above_matching_static_risk_budget_cagr"] is True
    assert gate["positive_complete_year_ratio_at_least"] == 0.5
    assert gate["requested_notional_fill_ratio_at_least"] == 0.98
    assert gate["capacity_limited_requested_notional_ratio_at_most"] == 0.02
    assert gate["capacity_violation_count_at_most"] == 0
    assert gate["negative_cash_observation_count_at_most"] == 0
    assert gate["leverage_observation_count_at_most"] == 0
    assert gate["nav_reconciliation_error_at_most"] == 1e-8
    assert gate["sharpe_is_disclosure_only"] is True
    assert gate["max_drawdown_is_disclosure_only"] is True
    assert gate["annualized_turnover_is_disclosure_only"] is True
    assert gate["all_six_D1_D2_D3_roles_must_pass"] is True
    assert gate["pooled_period_rescue"] is False


def test_10_0_claims_and_evidence_path_remain_conservative() -> None:
    protocol = _read()
    assert protocol["evidence"] == {
        "path": "protocols/evidence/10.0/results-first-diagnostic.json",
        "create_only": True,
    }
    claim = protocol["claim_contract"]
    assert claim["independent_oos"] is False
    assert claim["alpha_claim_allowed"] is False
    assert claim["profit_claim_allowed"] is False
    assert claim["stable_future_profit_claim_allowed"] is False
    assert claim["investment_recommendation_allowed"] is False
    assert claim["fresh_future_evidence_required"] is True


@pytest.mark.parametrize(
    ("path", "replacement"),
    [
        (("results_first_selection_disclosure", "selection_is_independent_oos"), True),
        (("results_first_selection_disclosure", "selected_strategy_id"), "runner_up"),
        (("frozen_strategy", "momentum_formula"), "full-sample momentum"),
        (("frozen_strategy", "top_k"), 2),
        (("frozen_strategy", "volatility_weighting"), True),
        (("frozen_strategy", "stress_reuses_base_targets_exactly"), False),
        (("results_first_gate", "candidate_cagr_strictly_above_matching_static_risk_budget_cagr"), False),
        (("results_first_gate", "requested_notional_fill_ratio_at_least"), 0.97),
        (("results_first_gate", "capacity_limited_requested_notional_ratio_at_most"), 0.03),
        (("selection_contract", "runner_up_fallback"), True),
        (("claim_contract", "profit_claim_allowed"), True),
        (("claim_contract", "fresh_future_evidence_required"), False),
    ],
)
def test_10_0_rehashed_changes_do_not_match_frozen_payload(
    path: tuple[Any, ...], replacement: Any
) -> None:
    value = copy.deepcopy(_read())
    _set_nested(value, path, replacement)
    value["payload_sha256"] = canonical_payload_sha256(value)
    assert value["payload_sha256"] != PROTOCOL_PAYLOAD

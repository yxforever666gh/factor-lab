from __future__ import annotations

import json
from pathlib import Path

from factor_lab.release_integrity import canonical_payload_sha256


ROOT = Path(__file__).resolve().parents[2]
PATH = ROOT / "protocols/13.0-stage1-terminal.json"
PAYLOAD = "d89b573d0b1aaa54baa3b25abe3e6dfb4ec3394f1301583d8a972d73ea1b4d63"


def _evidence():
    return json.loads(PATH.read_text(encoding="utf-8"))


def test_13_0_terminal_evidence_is_hash_bound_and_failed_closed() -> None:
    value = _evidence()
    assert value["payload_sha256"] == canonical_payload_sha256(value) == PAYLOAD
    assert value["stage_1_result"]["independent_full_verification_passed"] is True
    assert value["stage_1_result"]["stage_1_passed"] is False
    assert value["stage_1_result"]["stage_2_permitted"] is False
    assert value["stage_1_result"]["stage_2_dispatched"] is False
    assert value["terminal_decision"] == {
        "continue_13_0_parameter_search": False,
        "run_13_0_stage_2": False,
        "open_13_0_selection": False,
        "claim_profit_or_stability": False,
        "next_direction": (
            "14.0 industry-and-size-neutral multi-factor sleeves with strictly "
            "lagged fundamentals and whole-day or multi-session participation execution"
        ),
    }


def test_13_0_failure_is_not_misreported_as_no_alpha() -> None:
    value = _evidence()
    assert value["candidate_base"]["validation_quarterly_cagr"] > 0.0
    assert value["candidate_stress"]["validation_quarterly_cagr"] > 0.0
    assert value["candidate_base"]["target_gap_fill_ratio"] < 0.98
    assert (
        value["candidate_stress"]["capacity_limited_target_gap_ratio"] > 0.02
    )
    repeatability = value["cross_sectional_repeatability"]
    assert repeatability["positive_industry_fraction_each_role"] < 0.60
    assert repeatability[
        "small_mid_large_cumulative_net_pnl_strictly_positive_each_role"
    ]
    assert value["claim_contract"]["selection_opened"] is False

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from factor_lab.release_integrity import canonical_payload_sha256


ROOT = Path(__file__).resolve().parents[2]
PROTOCOL_PATH = ROOT / "protocols" / "11.2-quarterly-prospective-cycle.json"
PRIOR_PROTOCOL_PATH = ROOT / "protocols" / "11.1-quarterly-prospective-cycle.json"
PROTOCOL_PAYLOAD = "b9da758aad617d8752f9dbc628f8421fe4c04fe26f9f2a677fee1a8797b50e08"
PROTOCOL_FILE_SHA256 = "d363ae60326b17d3b28c04201f1ab411df544b2e16f0fe93e7fba30010c728a6"


def _read(path: Path = PROTOCOL_PATH) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _set_nested(value: Any, path: tuple[Any, ...], replacement: Any) -> None:
    cursor = value
    for key in path[:-1]:
        cursor = cursor[key]
    cursor[path[-1]] = replacement


def test_11_2_protocol_is_exactly_self_hashed() -> None:
    protocol = _read()
    assert protocol["payload_sha256"] == canonical_payload_sha256(protocol)
    assert protocol["payload_sha256"] == PROTOCOL_PAYLOAD
    assert hashlib.sha256(PROTOCOL_PATH.read_bytes()).hexdigest() == PROTOCOL_FILE_SHA256
    assert protocol["release"] == "11.2"
    assert protocol["direction_change"] is False
    assert protocol["status"] == "frozen_before_first_prospective_cycle"


def test_11_2_binds_the_exact_published_11_1_release() -> None:
    prior = _read()["prior_release"]
    assert prior["tag"] == "11.1"
    assert prior["annotated_tag_object"] == "d3e7652faf8e626e02ae430bdb5ae10eb79ffe29"
    assert prior["peeled_commit"] == "f9a184152e319a0bbe444af9fe0cb9367101e176"
    binding = prior["protocol"]
    value = _read(PRIOR_PROTOCOL_PATH)
    assert binding["file_sha256"] == hashlib.sha256(PRIOR_PROTOCOL_PATH.read_bytes()).hexdigest()
    assert binding["file_sha256"] == "81ce81f15ca43c714cc7d40d5c966850214fee28460608e5a855ce950ce95adf"
    assert binding["payload_sha256"] == canonical_payload_sha256(value)
    assert binding["payload_sha256"] == "457e54d57b3bf821ced04bd4c638f686243ee40ee64431e63a67dbc5ff692a5d"
    assert prior["prospective_state_at_upgrade"]["decision_count"] == 0
    assert prior["prospective_state_at_upgrade"]["confirmed_outcome_count"] == 0


def test_11_2_reuses_every_11_1_strategy_parameter_exactly() -> None:
    protocol = _read()
    prior = _read(PRIOR_PROTOCOL_PATH)
    strategy = protocol["frozen_strategy"]
    frozen = prior["frozen_strategy"]
    assert protocol["route"] == strategy["strategy_id"] == frozen["strategy_id"]
    for key in (
        "signal_schedule", "execution", "session_index_semantics",
        "long_momentum_formula", "short_momentum_formula", "long_positive_set",
        "dual_positive_set", "ranking", "dual_component", "borda_component",
        "blend_formula", "required_endpoint_rule", "dual_top_k", "dual_weight",
        "published_10_0_borda_weight", "volatility_weighting", "leverage",
        "post_selection_parameter_grid", "stress_reuses_base_targets_exactly",
        "future_tail_prefix_invariance_required",
    ):
        assert strategy[key] == frozen[key]
    assert strategy["parameter_or_formula_change_allowed"] is False
    assert strategy["asset_substitution_allowed"] is False


def test_11_2_provider_reliability_contract_is_exact_and_payload_transparent() -> None:
    provider = _read()["provider_reliability_contract"]
    assert provider == {
        "requests_per_minute_ceiling": 300,
        "maximum_attempts_per_request": 3,
        "retryable_failures_only": [
            "temporary_transport_error", "http_429", "http_5xx",
            "provider_frequency_limit_message",
        ],
        "delays_after_first_and_second_failed_attempt_seconds": [1.0, 2.0],
        "non_temporary_transport_error_is_retried": False,
        "http_4xx_other_than_429_is_retried": False,
        "provider_schema_or_payload_validation_error_is_retried": False,
        "successful_provider_payload_is_returned_unmodified": True,
        "retry_layer_may_filter_sort_deduplicate_coerce_or_fill_payload": False,
        "attempt_budget_is_per_provider_request": True,
        "rate_limit_applies_across_initial_and_retry_attempts": True,
        "exhausted_or_non_retryable_failure_is_fail_closed": True,
        "failed_request_may_publish_a_partial_source_stage_or_receipt": False,
        "strategy_data_normalization_and_hash_semantics_change_allowed": False,
    }


def test_11_2_keeps_exact_runtime_capture_time_and_create_only_state_machine() -> None:
    protocol = _read()
    runtime = protocol["formal_runtime_contract"]
    capture = protocol["source_capture_contract"]
    timing = protocol["time_contract"]
    machine = protocol["state_machine"]
    account = protocol["continuous_account_contract"]
    assert runtime["formal_runtime_root"] == "runtime/prospective/11.2"
    assert runtime["formal_source_root"] == "runtime/prospective/11.2/sources"
    assert protocol["release_checkout_contract"]["local_tag_must_be_an_ancestor_of_head"] is True
    assert capture["full_capture_count"] == 2
    assert capture["provider_retries_may_relax_stable_double_capture_or_prefix_requirements"] is False
    assert timing["signal_source_not_before_local_time"] == "17:10:00"
    assert timing["decision_deadline"].startswith("09:15:00")
    assert machine["missing_exact_next_open_may_create_confirmed_outcome"] is False
    assert machine["later_open_substitution_allowed"] is False
    assert "share_fields_scaled_by_the_exact_official_unit_multiplier" in machine["next_open_can_change_only"]
    assert account["fresh_cash_or_empty_holdings_reset_after_genesis"] is False
    assert account["outcome_is_finalized_before_a_same_close_next_decision"] is True
    artifacts = protocol["formal_artifacts"]
    assert set(artifacts) == {"source", "decision", "outcome"}
    assert all(value["create_only"] is True for value in artifacts.values())
    assert artifacts["source"]["failed_capture_or_retry_may_publish"] is False
    assert protocol["atomic_create_only_contract"]["failure_before_final_path_exposure_leaves_no_final_or_partial_artifact"] is True


def test_11_2_freezes_zero_outcomes_and_forbids_profit_claims() -> None:
    claim = _read()["claim_contract"]
    assert claim["completed_prospective_decision_count_at_freeze"] == 0
    assert claim["completed_prospective_outcome_count_at_freeze"] == 0
    assert claim["historical_11_0_diagnostic_is_independent_oos"] is False
    assert claim["alpha_claim_allowed"] is False
    assert claim["profit_claim_allowed"] is False
    assert claim["stable_future_profit_claim_allowed"] is False
    assert claim["investment_recommendation_allowed"] is False


@pytest.mark.parametrize(
    ("path", "replacement"),
    [
        (("prior_release", "peeled_commit"), "0" * 40),
        (("frozen_strategy", "dual_weight"), 0.5),
        (("provider_reliability_contract", "requests_per_minute_ceiling"), 301),
        (("provider_reliability_contract", "maximum_attempts_per_request"), 4),
        (("provider_reliability_contract", "retryable_failures_only"), ["all_errors"]),
        (("provider_reliability_contract", "successful_provider_payload_is_returned_unmodified"), False),
        (("provider_reliability_contract", "failed_request_may_publish_a_partial_source_stage_or_receipt"), True),
        (("source_capture_contract", "full_capture_count"), 1),
        (("state_machine", "later_open_substitution_allowed"), True),
        (("continuous_account_contract", "fresh_cash_or_empty_holdings_reset_after_genesis"), True),
        (("claim_contract", "profit_claim_allowed"), True),
    ],
)
def test_11_2_rehashed_relaxations_do_not_match_frozen_payload(
    path: tuple[Any, ...], replacement: Any
) -> None:
    changed = copy.deepcopy(_read())
    _set_nested(changed, path, replacement)
    changed["payload_sha256"] = canonical_payload_sha256(changed)
    assert changed["payload_sha256"] != PROTOCOL_PAYLOAD

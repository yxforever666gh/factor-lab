from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from factor_lab.release_integrity import canonical_payload_sha256


ROOT = Path(__file__).resolve().parents[2]
PROTOCOL_PATH = ROOT / "protocols" / "11.1-quarterly-prospective-cycle.json"
PRIOR_PROTOCOL_PATH = ROOT / "protocols" / "11.0-results-first-dual-confirm-blend.json"
PRIOR_EVIDENCE_PATH = ROOT / "protocols" / "evidence" / "11.0" / "results-first-diagnostic.json"
PROTOCOL_PAYLOAD = "457e54d57b3bf821ced04bd4c638f686243ee40ee64431e63a67dbc5ff692a5d"
PROTOCOL_FILE_SHA256 = "81ce81f15ca43c714cc7d40d5c966850214fee28460608e5a855ce950ce95adf"


def _read(path: Path = PROTOCOL_PATH) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _set_nested(value: Any, path: tuple[Any, ...], replacement: Any) -> None:
    cursor = value
    for key in path[:-1]:
        cursor = cursor[key]
    cursor[path[-1]] = replacement


def test_11_1_protocol_is_exactly_self_hashed() -> None:
    protocol = _read()
    assert protocol["payload_sha256"] == canonical_payload_sha256(protocol)
    assert protocol["payload_sha256"] == PROTOCOL_PAYLOAD
    assert hashlib.sha256(PROTOCOL_PATH.read_bytes()).hexdigest() == PROTOCOL_FILE_SHA256
    assert protocol["release"] == "11.1"
    assert protocol["direction_change"] is False
    assert protocol["status"] == "frozen_before_first_prospective_cycle"


def test_11_1_binds_the_exact_published_11_0_release_and_evidence() -> None:
    prior = _read()["prior_release"]
    assert prior["tag"] == "11.0"
    assert prior["annotated_tag_object"] == "e1a6527f13838e1a58e48508b5854506b74bef9f"
    assert prior["peeled_commit"] == "e9cbabc6da7b6d75d722a17d5fa6d1d227d3979b"
    for key, path, file_hash, payload_hash in (
        (
            "protocol",
            PRIOR_PROTOCOL_PATH,
            "8c6b20996e1e735a020fd71a31b0401570948549a041c5f3848a3dd19ae8fc7c",
            "d23739b85fa02d0cfeca977ba5f60fe003ae5753a387f7b10fa611a6688ae0bf",
        ),
        (
            "historical_diagnostic",
            PRIOR_EVIDENCE_PATH,
            "6c74d76285c7003ffd509c0866fc6d0b084be6770aadf32d2463293d38d83946",
            "8ceffbf9aaff605c03d7ca87c56244e47722481acaf1042cb90f4ec70b6eda4d",
        ),
    ):
        binding = prior[key]
        value = _read(path)
        assert binding["file_sha256"] == file_hash == hashlib.sha256(path.read_bytes()).hexdigest()
        assert binding["payload_sha256"] == payload_hash == canonical_payload_sha256(value)


def test_11_1_reuses_the_11_0_formula_without_any_parameter_change() -> None:
    protocol = _read()
    prior = _read(PRIOR_PROTOCOL_PATH)
    strategy = protocol["frozen_strategy"]
    frozen = prior["frozen_strategy"]
    assert protocol["route"] == strategy["strategy_id"] == frozen["strategy_id"]
    for key in (
        "signal_schedule",
        "execution",
        "session_index_semantics",
        "long_momentum_formula",
        "short_momentum_formula",
        "long_positive_set",
        "dual_positive_set",
        "ranking",
        "dual_component",
        "borda_component",
        "blend_formula",
        "required_endpoint_rule",
        "dual_top_k",
        "dual_weight",
        "published_10_0_borda_weight",
        "volatility_weighting",
        "leverage",
        "post_selection_parameter_grid",
        "stress_reuses_base_targets_exactly",
        "future_tail_prefix_invariance_required",
    ):
        assert strategy[key] == frozen[key]
    assert strategy["parameter_or_formula_change_allowed"] is False
    assert strategy["asset_substitution_allowed"] is False
    assert strategy["next_open_may_change_target_or_sealed_plan"] is False


def test_11_1_requires_clean_remote_tag_and_exact_double_capture_receipt() -> None:
    protocol = _read()
    checkout = protocol["release_checkout_contract"]
    runtime = protocol["formal_runtime_contract"]
    capture = protocol["source_capture_contract"]
    receipt = protocol["stable_capture_receipt_contract"]
    assert checkout["formal_tag_ref"] == "refs/tags/11.1"
    assert checkout["local_tag_must_be_an_ancestor_of_head"] is True
    assert checkout["worktree_must_be_clean_including_untracked_files"] is True
    assert checkout["remote_annotated_tag_object_must_equal_local"] is True
    assert checkout["remote_peeled_commit_must_equal_local"] is True
    assert runtime["formal_runtime_root"] == "runtime/prospective/11.1"
    assert runtime["formal_source_root"] == "runtime/prospective/11.1/sources"
    assert capture["full_capture_count"] == 2
    assert capture["canonical_market_payload_sha256_must_match_across_both_captures"] is True
    assert capture["one_capture_or_mismatched_capture_may_create_source_receipt"] is False
    assert capture["fresh_clock_is_rechecked_immediately_before_receipt_freeze_and_after_publication"] is True
    assert receipt["contract_id"] == "factor-lab/11.1/stable-source-v1"
    assert receipt["embedded_in_source_manifest_before_atomic_publication"] is True
    assert "canonical_capture_payload_sha256" in receipt["required_fields"]
    assert "inclusive 17:10" in receipt["validated_at_local_time_window"]
    assert "exclusive 09:15" in receipt["validated_at_local_time_window"]
    assert receipt["canonical_capture_market_payload_or_prefix_rewrite_may_be_rehashed_and_accepted"] is False


def test_11_1_state_machine_is_strict_create_only_and_continuous() -> None:
    protocol = _read()
    timing = protocol["time_contract"]
    machine = protocol["state_machine"]
    account = protocol["continuous_account_contract"]
    artifacts = protocol["formal_artifacts"]
    assert timing["signal_source_not_before_local_time"] == "17:10:00"
    assert timing["decision_deadline"].startswith("09:15:00")
    assert timing["next_open_price_is_forbidden_from_decision_inputs"] is True
    assert machine["missing_exact_next_open_may_create_confirmed_outcome"] is False
    assert machine["later_open_substitution_allowed"] is False
    assert "official unit multiplier" in machine["official_unit_event_rule"]
    assert "share_fields_scaled_by_the_exact_official_unit_multiplier" in machine[
        "next_open_can_change_only"
    ]
    assert account["fresh_cash_or_empty_holdings_reset_after_genesis"] is False
    assert account["each_next_cycle_opening_state"].startswith("exact prior confirmed")
    assert account["dividend_receivables_and_unit_events_carry_across_cycles"] is True
    assert account["outcome_is_finalized_before_a_same_close_next_decision"] is True
    assert set(artifacts) == {"source", "decision", "outcome"}
    assert all(value["create_only"] is True for value in artifacts.values())
    assert artifacts["source"]["path_template"].startswith("runtime/prospective/11.1/")
    assert artifacts["decision"]["existing_path_behavior"].startswith("raise FileExistsError")
    assert artifacts["outcome"]["existing_path_behavior"].startswith("raise FileExistsError")
    atomic = protocol["atomic_create_only_contract"]
    assert atomic["temporary_file_is_flushed_and_fsynced_before_final_path_exposure"] is True
    assert atomic["existing_or_concurrently_created_final_path_may_be_overwritten"] is False
    assert atomic["failure_before_final_path_exposure_leaves_no_final_or_partial_artifact"] is True


def test_11_1_freezes_zero_outcomes_and_forbids_every_profit_claim() -> None:
    claim = _read()["claim_contract"]
    assert claim == {
        "evidence_class": "prospective_quarterly_paper_cycle",
        "historical_11_0_diagnostic_is_independent_oos": False,
        "prospective_cycle_is_live_trading": False,
        "alpha_claim_allowed": False,
        "profit_claim_allowed": False,
        "stable_future_profit_claim_allowed": False,
        "investment_recommendation_allowed": False,
        "fresh_future_outcomes_required": True,
        "completed_prospective_decision_count_at_freeze": 0,
        "completed_prospective_outcome_count_at_freeze": 0,
    }


@pytest.mark.parametrize(
    ("path", "replacement"),
    [
        (("prior_release", "peeled_commit"), "0" * 40),
        (("release_checkout_contract", "worktree_must_be_clean_including_untracked_files"), False),
        (("frozen_strategy", "short_momentum_formula"), "future momentum"),
        (("frozen_strategy", "dual_weight"), 0.5),
        (("source_capture_contract", "full_capture_count"), 1),
        (("time_contract", "decision_deadline"), "09:30:00 next session"),
        (("state_machine", "later_open_substitution_allowed"), True),
        (("continuous_account_contract", "fresh_cash_or_empty_holdings_reset_after_genesis"), True),
        (("formal_artifacts", "decision", "create_only"), False),
        (("claim_contract", "profit_claim_allowed"), True),
        (("claim_contract", "stable_future_profit_claim_allowed"), True),
    ],
)
def test_11_1_rehashed_relaxations_do_not_match_frozen_payload(
    path: tuple[Any, ...], replacement: Any
) -> None:
    changed = copy.deepcopy(_read())
    _set_nested(changed, path, replacement)
    changed["payload_sha256"] = canonical_payload_sha256(changed)
    assert changed["payload_sha256"] != PROTOCOL_PAYLOAD

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from factor_lab.release_integrity import canonical_payload_sha256


ROOT = Path(__file__).resolve().parents[2]
PROTOCOL_PATH = ROOT / "protocols" / "10.1-quarterly-prospective-cycle.json"
PRIOR_PROTOCOL_PATH = ROOT / "protocols" / "10.0-results-first-quarterly-borda.json"
PRIOR_EVIDENCE_PATH = (
    ROOT / "protocols" / "evidence" / "10.0" / "results-first-diagnostic.json"
)
PROTOCOL_PAYLOAD = "0c3f2240cc404c1084230f1efbfe3f9fd3f0fa73dbbdc69ec63e5465ef7610ca"
PROTOCOL_FILE_SHA256 = "81240134127de2fedde6e231f8a3a02dd74950ff9da67e5298e71834c61843b5"
EVIDENCE_PATH = ROOT / "protocols" / "evidence" / "10.1" / "historical-asof-dry-run.json"
EVIDENCE_PAYLOAD = "0d2103896410f8800cf9351cb8fb31b807df7ff06c79413b0c2ed45fbc3fed47"
EVIDENCE_FILE_SHA256 = "888313dd86c9c15bf6e915d087a784c1a8e4d48e85f3832b4e0945e77e3e27c3"
IMPLEMENTATION_COMMIT = "699ee3f7687d25364438faca4b0a5bbf9b69a76a"


def _read(path: Path = PROTOCOL_PATH) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _set_nested(value: Any, path: tuple[Any, ...], replacement: Any) -> None:
    cursor = value
    for key in path[:-1]:
        cursor = cursor[key]
    cursor[path[-1]] = replacement


def test_10_1_protocol_is_exactly_self_hashed() -> None:
    protocol = _read()
    assert protocol["payload_sha256"] == canonical_payload_sha256(protocol)
    assert protocol["payload_sha256"] == PROTOCOL_PAYLOAD
    assert hashlib.sha256(PROTOCOL_PATH.read_bytes()).hexdigest() == PROTOCOL_FILE_SHA256
    assert protocol["release"] == "10.1"
    assert protocol["direction_change"] is False
    assert protocol["status"] == "frozen_before_first_prospective_cycle"


def test_10_1_historical_dry_run_evidence_is_exact_nonprospective_and_zero_mismatch() -> None:
    evidence = _read(EVIDENCE_PATH)
    assert evidence["payload_sha256"] == EVIDENCE_PAYLOAD
    assert canonical_payload_sha256(evidence) == EVIDENCE_PAYLOAD
    assert hashlib.sha256(EVIDENCE_PATH.read_bytes()).hexdigest() == EVIDENCE_FILE_SHA256
    assert evidence["implementation"]["git_head"] == IMPLEMENTATION_COMMIT
    assert evidence["source"]["path"] == "runtime/data/multi-asset-9.0/sources/stage=audit"
    assert evidence["prospective"] is False
    assert evidence["summary"] == {
        "signal_count": 46,
        "signal_count_strictly_positive": True,
        "confirmed_outcome_count": 45,
        "target_prefix_mismatch_count": 0,
        "sealed_plan_prefix_mismatch_count": 0,
        "signal_close_state_prefix_mismatch_count": 0,
        "outcome_prefix_mismatch_count": 0,
        "formal_path_write_count": 0,
    }
    assert evidence["claim_contract"]["prospective_label_allowed"] is False
    assert evidence["claim_contract"]["profit_claim_allowed"] is False


def test_10_1_binds_the_exact_published_10_0_release_and_evidence() -> None:
    protocol = _read()
    prior = protocol["prior_release"]
    assert prior["tag"] == "10.0"
    assert prior["annotated_tag_object"] == (
        "c3cfa2221a27a4bed2ace3ed1f3324896cd15828"
    )
    assert prior["peeled_commit"] == "267d446e16af8fd8555cc676de32e4770f39dcde"
    expected = {
        "protocol": (
            PRIOR_PROTOCOL_PATH,
            "6a949ce4374f407a6084053a08b76dedbb3f1478fbe56edf233bb23befe730dd",
            "dc79550ee9fefe4fdb01f54fe0c299a40c2d118a687f6e5571156dff5701cb7b",
        ),
        "historical_diagnostic": (
            PRIOR_EVIDENCE_PATH,
            "954be9b434d3d5c7c06ddac1f276ac032248b420956185cc372ae352685b4e89",
            "18c9fb75f79cf71572f65a3eade0d2af8a018e7b8aef066fa8a30dce1f721253",
        ),
    }
    for key, (path, file_hash, payload_hash) in expected.items():
        binding = prior[key]
        value = _read(path)
        assert binding["file_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
        assert binding["file_sha256"] == file_hash
        assert binding["payload_sha256"] == canonical_payload_sha256(value)
        assert binding["payload_sha256"] == payload_hash
    evidence = _read(PRIOR_EVIDENCE_PATH)
    assert prior["historical_diagnostic"]["status"] == evidence["status"]
    assert prior["historical_diagnostic"]["evidence_class"] == evidence["evidence_class"]


def test_10_1_requires_a_clean_exact_local_and_remote_annotated_checkout() -> None:
    protocol = _read()
    checkout = protocol["release_checkout_contract"]
    runtime = protocol["formal_runtime_contract"]
    assert checkout == {
        "formal_tag_ref": "refs/tags/10.1",
        "local_tag_object_type": "tag",
        "head_must_equal_local_tag_peeled_commit": True,
        "local_tag_must_be_an_ancestor_of_head": True,
        "worktree_must_be_clean_including_untracked_files": True,
        "remote": "origin",
        "remote_annotated_tag_object_must_equal_local": True,
        "remote_peeled_commit_must_equal_local": True,
        "any_local_git_remote_git_or_network_check_failure_is_fail_closed": True,
    }
    assert runtime["formal_runtime_root"] == "runtime/prospective/10.1"
    assert runtime["formal_source_root"] == "runtime/prospective/10.1/sources"
    assert runtime["source_stage_name_template"] == "asof-YYYYMMDD"
    assert runtime["stage_name_must_exactly_encode_price_end_date"] is True
    assert runtime["python_runtime_root_injection_is_test_only_and_not_formal_evidence"] is True


def test_10_1_strategy_is_the_unmodified_10_0_quarterly_borda_route() -> None:
    protocol = _read()
    prior = _read(PRIOR_PROTOCOL_PATH)
    strategy = protocol["frozen_strategy"]
    frozen = prior["frozen_strategy"]
    assert protocol["route"] == strategy["strategy_id"] == prior["route"]
    for key in (
        "signal_schedule",
        "execution",
        "session_index_semantics",
        "momentum_formula",
        "required_endpoint_rule",
        "positive_set",
        "ranking",
        "borda_score",
        "target_weight",
    ):
        assert strategy[key] == frozen[key]
    assert strategy["target_builder"].endswith(".build_monthly_targets")
    assert strategy["execution_simulator"].endswith(".simulate_targets")
    assert strategy["parameter_or_formula_change_allowed"] is False
    assert strategy["asset_substitution_allowed"] is False
    assert strategy["next_open_may_change_target_or_sealed_plan"] is False


def test_10_1_requires_two_complete_captures_and_exact_baseline_prefix() -> None:
    capture = _read()["source_capture_contract"]
    baseline = capture["baseline_prefix"]
    assert capture["full_capture_count"] == 2
    assert capture["captures_are_independent_complete_provider_pulls"] is True
    assert capture["each_capture_not_before_signal_local_time"] == "17:10:00"
    assert capture["each_capture_market_cutoff_is_exact_signal_date"] is True
    assert capture["market_rows_after_signal_date_allowed"] is False
    assert capture["official_calendar_must_include_immediate_next_session"] is True
    assert capture["canonical_market_payload_sha256_must_match_across_both_captures"] is True
    assert capture["one_capture_or_mismatched_capture_may_create_source_receipt"] is False
    assert capture["both_captures_validation_and_atomic_publication_must_finish_before_decision_deadline"] is True
    assert capture["fresh_clock_is_rechecked_immediately_before_receipt_freeze_and_after_publication"] is True
    assert baseline == {
        "source_root": "runtime/data/multi-asset-9.0/sources/stage=audit",
        "cutoff": "2026-08-28",
        "manifest_file_sha256": (
            "cdbf8ba498142adff04216b476522f47ee18df6f0fa02f3395d0e141191adbfa"
        ),
        "manifest_payload_sha256": (
            "050ad4ddcb86dc4fbc71befad54c400b48a44f72ab6fecc33936b6da0c8f9aff"
        ),
        "calendar_and_all_six_asset_rows_through_cutoff_must_match_exactly": True,
        "append_only_after_cutoff": True,
        "historical_revision_or_deletion_allowed": False,
    }


def test_10_1_source_manifest_embeds_the_exact_stable_capture_receipt() -> None:
    receipt = _read()["stable_capture_receipt_contract"]
    assert receipt["contract_id"] == "factor-lab/10.1/stable-source-v1"
    assert receipt["embedded_in_source_manifest_before_atomic_publication"] is True
    assert receipt["source_manifest_payload_sha256_covers_the_receipt"] is True
    assert receipt["required_fields"] == [
        "schema_version",
        "contract_id",
        "release",
        "release_tag",
        "release_annotated_tag_object",
        "release_peeled_commit",
        "protocol_payload_sha256",
        "formal_source_root",
        "stage",
        "price_end_date",
        "full_capture_count",
        "independent_complete_provider_pulls",
        "canonical_payloads_match_exactly",
        "canonical_capture_payload_sha256",
        "baseline_manifest_path",
        "baseline_manifest_file_sha256",
        "baseline_manifest_payload_sha256",
        "validated_at_utc",
    ]
    assert receipt["release_tag_objects_must_equal_the_verified_checkout"] is True
    assert "inclusive 17:10" in receipt["validated_at_local_time_window"]
    assert "exclusive 09:15" in receipt["validated_at_local_time_window"]
    assert receipt["canonical_capture_market_payload_or_prefix_rewrite_may_be_rehashed_and_accepted"] is False
    assert receipt["local_receipt_timestamp_has_external_attestation"] is False
    assert receipt["deliberate_local_receipt_rewrite_is_cryptographically_prevented"] is False


def test_10_1_signal_and_outcome_state_machine_has_strict_causal_times() -> None:
    protocol = _read()
    timing = protocol["time_contract"]
    machine = protocol["state_machine"]
    assert timing["timezone"] == "Asia/Shanghai"
    assert timing["signal_source_not_before_local_time"] == "17:10:00"
    assert timing["decision_deadline"].startswith("09:15:00")
    assert timing["execution_open"].startswith("09:30:00")
    assert "different natural calendar quarters" in timing["quarter_end_identity"]
    assert timing["non_quarter_end_may_select_a_prior_target"] is False
    assert timing["missing_immediate_next_official_session_may_create_a_decision"] is False
    assert timing["decision_recorded_at_or_after_deadline_is_prospective"] is False
    assert timing["next_open_price_is_forbidden_from_decision_inputs"] is True
    assert machine["states"] == [
        "waiting_signal",
        "source_ready",
        "decision_sealed",
        "awaiting_terminal_quarter_close",
        "outcome_confirmed",
        "missed_cycle",
    ]
    assert "exactly six target rows" in machine["decision_sealed"]
    assert "never fall back" in machine["decision_sealed"]
    assert "later open is never substituted" in machine["awaiting_terminal_quarter_close"]
    assert "can never be backfilled" in machine["missed_cycle"]
    assert machine["missing_exact_next_open_may_create_confirmed_outcome"] is False
    assert machine["later_open_substitution_allowed"] is False
    assert machine["future_tail_may_change_prior_decision_or_outcome_prefix"] is False
    assert set(machine["next_open_cannot_change"]) >= {
        "target_weights",
        "requested_signal_notional",
        "planned_signal_notional",
    }
    assert "official unit multiplier" in machine["official_unit_event_rule"]
    assert "share_fields_scaled_by_the_exact_official_unit_multiplier" in machine["next_open_can_change_only"]


def test_10_1_keeps_one_continuous_account_and_only_three_create_only_paths() -> None:
    protocol = _read()
    account = protocol["continuous_account_contract"]
    artifacts = protocol["formal_artifacts"]
    infrastructure = protocol["infrastructure_contract"]
    assert account["first_formal_prospective_cycle_genesis"].startswith("CNY 1000000")
    assert account["fresh_cash_or_empty_holdings_reset_after_genesis"] is False
    assert account["each_next_cycle_opening_state"].startswith("exact prior confirmed")
    assert account["signal_close_order_sizing_uses_continuous_account_nav_and_holdings"] is True
    assert account["dividend_receivables_and_unit_events_carry_across_cycles"] is True
    assert account["outcome_is_finalized_before_a_same_close_next_decision"] is True
    assert account["next_signal_nav_cash_holdings_and_receivables_must_equal_the_exact_prior_terminal_state"] is True
    assert set(artifacts) == {"source", "decision", "outcome"}
    assert len({value["path_template"] for value in artifacts.values()}) == 3
    assert all(value["create_only"] is True for value in artifacts.values())
    assert artifacts["source"]["path_template"] == (
        "runtime/prospective/10.1/sources/stage=asof-YYYYMMDD/manifest.json"
    )
    assert all(
        artifacts[name]["path_template"].startswith(
            "runtime/prospective/10.1/cycle={YYYYQn}/"
        )
        for name in ("decision", "outcome")
    )
    assert artifacts["source"]["requires_embedded_stable_capture_receipt"] is True
    assert artifacts["decision"]["existing_path_behavior"].startswith("raise FileExistsError")
    assert artifacts["outcome"]["existing_path_behavior"].startswith("raise FileExistsError")
    atomic = protocol["atomic_create_only_contract"]
    assert atomic["temporary_file_must_be_flushed_and_fsynced_before_final_path_exposure"] is True
    assert atomic["final_path_is_exposed_by_one_atomic_create_only_hardlink"] is True
    assert atomic["existing_or_concurrently_created_final_path_may_be_overwritten"] is False
    assert infrastructure == {
        "ledger_used": False,
        "watchdog_used": False,
        "attestation_used": False,
        "transparency_log_used": False,
        "formal_create_only_path_count": 3,
        "formal_paths_are_the_only_persistent_protocol_state": True,
    }


def test_10_1_dry_run_cannot_be_promoted_to_prospective_evidence() -> None:
    protocol = _read()
    dry_run = protocol["historical_asof_dry_run"]
    isolation = protocol["mode_isolation"]
    assert dry_run["label"] == "historical_asof_dry_run"
    assert dry_run["implementation_form"] == "test_and_explicit_non-formal_summary_only"
    assert dry_run["prospective_label_allowed"] is False
    assert dry_run["formal_artifact_write_allowed"] is False
    assert dry_run["continuous_account_advance_allowed"] is False
    assert "physically slice every market frame" in dry_run["for_each_historical_signal"]
    assert "official natural-quarter end" in dry_run["outcome_cutoff_rule"]
    assert "official-unit-multiplier" in dry_run["sealed_plan_prefix_exact"]
    assert dry_run["required_summary"] == {
        "signal_count_strictly_positive": True,
        "target_prefix_mismatch_count": 0,
        "sealed_plan_prefix_mismatch_count": 0,
        "signal_close_state_prefix_mismatch_count": 0,
        "outcome_prefix_mismatch_count": 0,
        "formal_path_write_count": 0,
    }
    assert dry_run["may_be_promoted_or_renamed_as_prospective_evidence"] is False
    assert isolation["prospective_label"] == "prospective_quarterly_cycle"
    assert isolation["prospective_requires_release_published_before_signal_close"] is True
    assert isolation["prospective_requires_clean_worktree_and_exact_matching_remote_tag_objects"] is True
    assert isolation["prospective_requires_source_and_decision_created_inside_frozen_time_window"] is True
    assert isolation["historical_dry_run_output_may_enter_formal_paths"] is False
    assert isolation["late_or_reconstructed_decision_may_be_relabelled_prospective"] is False


def test_10_1_forbids_profit_and_stable_future_claims_at_freeze() -> None:
    claim = _read()["claim_contract"]
    assert claim == {
        "evidence_class": "prospective_quarterly_paper_cycle",
        "historical_dry_run_is_independent_oos": False,
        "prospective_cycle_is_live_trading": False,
        "alpha_claim_allowed": False,
        "profit_claim_allowed": False,
        "stable_future_profit_claim_allowed": False,
        "investment_recommendation_allowed": False,
        "fresh_future_outcomes_required": True,
        "completed_prospective_outcome_count_at_freeze": 0,
    }


@pytest.mark.parametrize(
    ("path", "replacement"),
    [
        (("prior_release", "peeled_commit"), "0" * 40),
        (("release_checkout_contract", "worktree_must_be_clean_including_untracked_files"), False),
        (("frozen_strategy", "momentum_formula"), "future momentum"),
        (("source_capture_contract", "full_capture_count"), 1),
        (("source_capture_contract", "market_rows_after_signal_date_allowed"), True),
        (("source_capture_contract", "baseline_prefix", "append_only_after_cutoff"), False),
        (("stable_capture_receipt_contract", "source_manifest_payload_sha256_covers_the_receipt"), False),
        (("time_contract", "signal_source_not_before_local_time"), "15:00:00"),
        (("time_contract", "decision_deadline"), "09:30:00 next session"),
        (("state_machine", "later_open_substitution_allowed"), True),
        (("continuous_account_contract", "fresh_cash_or_empty_holdings_reset_after_genesis"), True),
        (("formal_artifacts", "decision", "create_only"), False),
        (("atomic_create_only_contract", "existing_or_concurrently_created_final_path_may_be_overwritten"), True),
        (("historical_asof_dry_run", "prospective_label_allowed"), True),
        (("infrastructure_contract", "ledger_used"), True),
        (("claim_contract", "profit_claim_allowed"), True),
        (("claim_contract", "stable_future_profit_claim_allowed"), True),
    ],
)
def test_10_1_rehashed_relaxations_do_not_match_frozen_payload(
    path: tuple[Any, ...], replacement: Any
) -> None:
    changed = copy.deepcopy(_read())
    _set_nested(changed, path, replacement)
    changed["payload_sha256"] = canonical_payload_sha256(changed)
    assert changed["payload_sha256"] != PROTOCOL_PAYLOAD

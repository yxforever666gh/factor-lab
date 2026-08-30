from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from factor_lab.release_integrity import (
    AUDIT_EVIDENCE_PATH,
    BASE_PROTOCOL_AMENDMENT_FILE_SHA256,
    BASE_PROTOCOL_AMENDMENT_PATH,
    BASE_PROTOCOL_FILE_SHA256,
    BASE_PROTOCOL_PATH,
    CORRECTIVE_AMENDMENT_FILE_SHA256,
    CORRECTIVE_AMENDMENT_PATH,
    PRIOR_EXECUTION_FAILURE_CREATION_COMMIT,
    PRIOR_EXECUTION_FAILURE_FILE_SHA256,
    PRIOR_EXECUTION_FAILURE_PATH,
    PRIOR_EXECUTION_FAILURE_PAYLOAD_SHA256,
    RUNTIME_FILE_SHA256,
    RUNTIME_PATH,
    RELEASE_RESULT_PATH,
    WINNER_FREEZE_PATH,
    _WINNER_FREEZE_FIELDS,
    _verify_prior_execution_failure_payload,
    _verify_integrated_prior_amendment,
    _verify_prior_release,
    _winner_from_frozen_gates,
    file_sha256,
    verify_corrective_amendment_contract,
    verify_frozen_runtime_contract,
    verify_prior_execution_failure,
    verify_wide_protocol_contract,
)
from factor_lab.research.wide_universe import CHALLENGER_IDS


ROOT = Path(__file__).resolve().parents[2]


def _read_json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def test_6_2_binds_the_published_6_1_pre_return_failure() -> None:
    protocol = _read_json(ROOT / "protocols/6.2-wide-universe.json")

    failure = _verify_prior_release(ROOT, protocol["prior_release"])

    assert failure["status"] == "pre_return_data_admission_failed"
    assert failure["opening_state"]["portfolio_returns_opened"] is False


def test_6_2_protocol_and_code_share_the_exact_admission_contract() -> None:
    protocol = _read_json(ROOT / BASE_PROTOCOL_PATH)

    verify_wide_protocol_contract(protocol)
    changed = copy.deepcopy(protocol)
    changed["common_base"]["finite_score_admission"]["per_signal_per_arm"][
        "finite_score_count_min"
    ] = 24

    with pytest.raises(ValueError, match="finite-score admission"):
        verify_wide_protocol_contract(changed)


def test_6_3_runtime_capsule_has_the_exact_static_identity() -> None:
    runtime = _read_json(ROOT / RUNTIME_PATH)

    verify_frozen_runtime_contract(runtime)
    changed = copy.deepcopy(runtime)
    changed["source_package_version"] = "6.2.0"

    with pytest.raises(ValueError, match="runtime identity"):
        verify_frozen_runtime_contract(changed)


def test_6_2_exactly_integrates_the_published_6_1_red_team_amendment() -> None:
    amendment = _read_json(
        ROOT / "protocols/6.2-wide-universe-amendment-1.json"
    )

    prior = _verify_integrated_prior_amendment(ROOT, amendment)

    assert amendment["effective_overrides"] == prior["effective_overrides"]
    assert amendment["required_tests_add"] == prior["required_tests_add"]


def test_6_3_byte_binds_the_exact_6_2_contract_and_new_capsules() -> None:
    assert file_sha256(ROOT / BASE_PROTOCOL_PATH) == BASE_PROTOCOL_FILE_SHA256
    assert (
        file_sha256(ROOT / BASE_PROTOCOL_AMENDMENT_PATH)
        == BASE_PROTOCOL_AMENDMENT_FILE_SHA256
    )
    assert (
        file_sha256(ROOT / CORRECTIVE_AMENDMENT_PATH)
        == CORRECTIVE_AMENDMENT_FILE_SHA256
    )
    assert file_sha256(ROOT / RUNTIME_PATH) == RUNTIME_FILE_SHA256


def test_6_3_terminal_evidence_isolated_from_6_2() -> None:
    assert WINNER_FREEZE_PATH == "protocols/evidence/6.3/winner-freeze.json"
    assert AUDIT_EVIDENCE_PATH == "protocols/evidence/6.3/historical-audit.json"
    assert RELEASE_RESULT_PATH == "protocols/evidence/6.3/result.json"
    assert "corrective_amendment_payload_sha256" in _WINNER_FREEZE_FIELDS


def test_6_3_corrective_amendment_is_stable_reduction_only() -> None:
    amendment = _read_json(ROOT / CORRECTIVE_AMENDMENT_PATH)

    verify_corrective_amendment_contract(ROOT, amendment)
    assert amendment["stage_reuse"]["reuse_6_2_derived_stages"] is False
    assert amendment["stage_reuse"]["reuse_6_2_status_views"] is False
    assert amendment["correction"]["sole_production_semantic_change"]["to"] == (
        "math.fsum over the same validated additive values"
    )

    changed = copy.deepcopy(amendment)
    changed["stage_reuse"]["reuse_6_2_derived_stages"] = True
    with pytest.raises(ValueError):
        verify_corrective_amendment_contract(ROOT, changed)


def test_6_3_binds_the_published_6_2_execution_failure() -> None:
    failure = verify_prior_execution_failure(
        ROOT,
        {
            "path": PRIOR_EXECUTION_FAILURE_PATH,
            "file_sha256": PRIOR_EXECUTION_FAILURE_FILE_SHA256,
            "payload_sha256": PRIOR_EXECUTION_FAILURE_PAYLOAD_SHA256,
            "status": "selection_inconclusive_software_failure",
            "classification": "floating_point_reduction_order_false_negative",
            "creation_commit": PRIOR_EXECUTION_FAILURE_CREATION_COMMIT,
        },
    )

    assert failure["evidence_boundary"]["return_kernel_executed_in_memory"] is True
    assert failure["evidence_boundary"]["return_metrics_persisted_or_reported"] is False
    assert failure["evidence_boundary"]["train_candidate_gate_evaluated"] is False
    assert failure["corrective_release_contract"]["next_release"] == "6.3"


def test_6_2_failure_semantics_reject_any_reclassification() -> None:
    failure = _read_json(ROOT / PRIOR_EXECUTION_FAILURE_PATH)
    changed = copy.deepcopy(failure)
    changed["evidence_boundary"]["train_candidate_gate_evaluated"] = True

    with pytest.raises(ValueError):
        _verify_prior_execution_failure_payload(changed)


def _passed_gate() -> dict:
    return {
        "passed": True,
        "paired_relative_cagr": {"q20": 0.01, "median": 0.02},
        "worst_capacity_limited_requested_notional_ratio": 0.01,
    }


def test_frozen_gate_replay_recomputes_unique_winner() -> None:
    freeze = {
        "train_passers": list(CHALLENGER_IDS),
        "train_gates": {candidate: _passed_gate() for candidate in CHALLENGER_IDS},
        "validation_gates": {
            candidate: _passed_gate() for candidate in CHALLENGER_IDS
        },
        "turnover_by_candidate": {
            CHALLENGER_IDS[0]: 0.12,
            CHALLENGER_IDS[1]: 0.10,
        },
    }

    assert _winner_from_frozen_gates(freeze) == CHALLENGER_IDS[1]


def test_frozen_gate_replay_rejects_passers_inconsistent_with_gates() -> None:
    freeze = {
        "train_passers": [CHALLENGER_IDS[0]],
        "train_gates": {candidate: _passed_gate() for candidate in CHALLENGER_IDS},
        "validation_gates": {CHALLENGER_IDS[0]: _passed_gate()},
        "turnover_by_candidate": {CHALLENGER_IDS[0]: 0.10},
    }

    with pytest.raises(ValueError, match="train passers"):
        _winner_from_frozen_gates(freeze)

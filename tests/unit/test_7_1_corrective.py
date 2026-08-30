from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import subprocess
from typing import Any

import pytest

from factor_lab.release_integrity import canonical_payload_sha256


ROOT = Path(__file__).resolve().parents[2]
AMENDMENT_PATH = ROOT / "protocols" / "7.1-corrective-amendment-1.json"
TAG = "7.0"
TAG_OBJECT = "25bbc306e8842feab923380416f8329e0dd81100"
TAG_COMMIT = "412026ca0370d53ca704adfd1122a811e768842e"
AMENDMENT_PAYLOAD = "7335cdbb61cd0d7b9c3e6f6896ec576c7e403b87d83cfa3d6679965691984c86"


EXPECTED_PRIOR_RELEASE = {
    "release": "7.0",
    "tag": TAG,
    "annotated_tag_object": TAG_OBJECT,
    "peeled_commit": TAG_COMMIT,
    "protocol": {
        "path": "protocols/7.0-multi-asset.json",
        "file_sha256": (
            "2d2e96a1605b5e088a7cf5952dd816d8aecb10e39b9ba529fe81b00592bfa14f"
        ),
        "payload_sha256": (
            "6f2fcd2a67d52bfae19bedcaecf495faa986195f6840da48a3a67a666589aaf0"
        ),
        "protocol_id": "factor-lab/7.0/fixed-multi-asset-trend-budget-v1",
    },
    "asset_selection": {
        "path": "protocols/7.0-asset-selection.json",
        "file_sha256": (
            "6d2d819db2579db76f8e7830a5de090d8d471c7fdc657abd8aba626cd1b065ec"
        ),
        "payload_sha256": (
            "b00536d618c7fe46e3cbe8d258d2b2032ef4e0c16d40fb9c74ff016c34525e0b"
        ),
    },
    "preclosure_train_disclosure": {
        "path": "protocols/evidence/7.0/preclosure-train.json",
        "file_sha256": (
            "01c3d97f7a3cce81bd8abe4e430c5b35b35deddefc80950403d3b40d109f7c09"
        ),
        "payload_sha256": (
            "6bd2909ddc97ec84d3535d15e8f13330a5752831aead82d8fb50afdd16ac6775"
        ),
        "status": "train_falsified_before_preselection_closure",
    },
    "preselection_closure": {
        "path": "protocols/7.0-release.json",
        "file_sha256": (
            "555e17eca0a9618e21e84133c825c447edf71999a1b8a7a39cc84b27753d4967"
        ),
        "payload_sha256": (
            "d0b6072234d45363144a47517c8c4c535e4c9550ea36925a4b7cc54216110009"
        ),
        "status": "implementation_frozen_after_disclosed_train_failure",
    },
    "execution_failure": {
        "path": "protocols/evidence/7.0/execution-failure.json",
        "file_sha256": (
            "674e62603f7ab9a026e9ef69dc52810889f584302e94be13a685dc708b76da53"
        ),
        "payload_sha256": (
            "04099ab6c2bd03099c9d045120578344bfe9ba3c963dfb82a0cba9f8a49f5df9"
        ),
        "status": "selection_inconclusive_software_failure",
        "classification": "target_order_replay_false_negative",
    },
}

EXPECTED_RELEASE_TRANSITION = {
    "published_7_0_annotated_tag_bound": True,
    "same_release_corrective_amendment_forbidden": True,
    "next_release": "7.1",
    "direction_change": False,
}

EXPECTED_CORRECTION = {
    "created_before_any_7_1_return_replay": True,
    "sole_permitted_change": {
        "path": "scripts/run-multi-asset-evidence.py",
        "function": "_replay_evaluation",
        "from": "compare persisted targets with causal-builder targets in their raw row order",
        "to": (
            "stable-sort both target frames by signal_date and code before their "
            "exact comparison"
        ),
        "sort_kind": "mergesort",
        "sort_key": ["signal_date", "code"],
        "comparison_dtype_check_unchanged": True,
        "comparison_exact_value_check_unchanged": True,
        "target_columns_dtypes_and_values_unchanged": True,
        "simulation_inputs_and_outputs_unchanged": True,
    },
}

EXPECTED_RUNTIME_AND_STAGE = {
    "runtime_root": "runtime/data/multi-asset-7.1",
    "new_runtime_namespace_required": True,
    "new_preselection_closure_required": True,
    "new_7_1_source_manifest_binding_and_evaluation_required": True,
    "reuse_7_0_derived_stages": False,
    "reuse_7_0_evaluations": False,
    "reuse_7_0_gate_or_status_views": False,
    "forbidden_7_0_artifacts": [
        "train_validation_or_audit_stage_manifests_bindings_and_derived_files",
        "candidate_control_or_stress_targets_orders_daily_nav_holdings_or_trades",
        "train_gate_pass_fail_or_selected_candidate_decisions",
        "winner_freeze_historical_audit_terminal_result_or_cli_status_views",
    ],
}

EXPECTED_PHASE_CONTRACT = {
    "formal_7_1_scope": "train corrective replay only",
    "formal_train_must_reproduce_bound_7_0_disclosed_metrics_and_failed_gate": True,
    "selected_candidate_id": None,
    "validation_market_outcomes_opened": False,
    "validation_stage_created": False,
    "audit_market_outcomes_opened": False,
    "audit_stage_created": False,
    "runner_up_fallback": False,
}

EXPECTED_UNCHANGED_CONTRACT = {
    "base_7_0_protocol_applies_in_full": True,
    "asset_selection_unchanged": True,
    "candidate_registry_unchanged": True,
    "signal_unchanged": True,
    "source_semantic_data_admission_unchanged": True,
    "total_return_reconstruction_unchanged": True,
    "portfolio_unchanged": True,
    "execution_unchanged": True,
    "costs_and_capacity_unchanged": True,
    "phase_boundaries_unchanged": True,
    "selection_gates_unchanged": True,
    "audit_rules_unchanged": True,
    "claim_contract_unchanged": True,
}

EXPECTED_REQUIRED_TESTS = [
    (
        "the 7.0 raw-order target comparison fails while stable signal_date-code "
        "mergesort makes candidate, control and stress targets exact"
    ),
    (
        "the correction changes only target comparison order and rejects any asset, "
        "signal, data, portfolio, execution, cost, gate, phase, audit or "
        "claim-contract drift"
    ),
    (
        "no 7.0 derived stage, evaluation, gate, selection or status artifact is "
        "accepted in the 7.1 runtime namespace"
    ),
    (
        "validation and audit remain physically absent because the bound disclosed "
        "train gate is false"
    ),
]

EXPECTED_AMENDMENT = {
    "schema_version": 1,
    "kind": "factor_lab_protocol_amendment",
    "amendment_id": (
        "factor-lab/7.1/fixed-multi-asset-trend-budget-corrective-replay/amendment-1"
    ),
    "release": "7.1",
    "status": "frozen_before_7_1_corrective_return_replay",
    "direction_change": False,
    "route": "fixed_multi_asset_causal_trend_budget",
    "prior_release": EXPECTED_PRIOR_RELEASE,
    "release_transition": EXPECTED_RELEASE_TRANSITION,
    "correction": EXPECTED_CORRECTION,
    "runtime_and_stage_contract": EXPECTED_RUNTIME_AND_STAGE,
    "phase_contract": EXPECTED_PHASE_CONTRACT,
    "unchanged_contract": EXPECTED_UNCHANGED_CONTRACT,
    "required_tests_add": EXPECTED_REQUIRED_TESTS,
    "payload_sha256": AMENDMENT_PAYLOAD,
}


def _read_amendment() -> dict[str, Any]:
    value = json.loads(AMENDMENT_PATH.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _git(*args: str) -> bytes:
    return subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout


def _verify_amendment(value: dict[str, Any]) -> None:
    if value != EXPECTED_AMENDMENT:
        raise ValueError("7.1 corrective amendment differs from its exact whitelist")
    if value.get("payload_sha256") != canonical_payload_sha256(value):
        raise ValueError("7.1 corrective amendment payload hash differs")


def _set_nested(value: dict[str, Any], path: tuple[str, ...], replacement: Any) -> None:
    cursor: dict[str, Any] = value
    for key in path[:-1]:
        nested = cursor[key]
        assert isinstance(nested, dict)
        cursor = nested
    cursor[path[-1]] = replacement


def test_7_1_corrective_amendment_is_exact_and_self_hashed() -> None:
    _verify_amendment(_read_amendment())


def test_7_1_binds_published_annotated_7_0_and_exact_tracked_bytes() -> None:
    amendment = _read_amendment()
    prior = amendment["prior_release"]

    assert _git("cat-file", "-t", f"refs/tags/{TAG}").decode("ascii").strip() == "tag"
    assert _git("rev-parse", f"refs/tags/{TAG}").decode("ascii").strip() == TAG_OBJECT
    assert (
        _git("rev-parse", f"refs/tags/{TAG}^{{}}").decode("ascii").strip()
        == TAG_COMMIT
    )
    assert prior["annotated_tag_object"] == TAG_OBJECT
    assert prior["peeled_commit"] == TAG_COMMIT

    for name in (
        "protocol",
        "asset_selection",
        "preclosure_train_disclosure",
        "preselection_closure",
        "execution_failure",
    ):
        binding = prior[name]
        path = ROOT / binding["path"]
        current = path.read_bytes()
        tagged = _git("show", f"{TAG}:{binding['path']}")
        payload = json.loads(current.decode("utf-8"))

        assert current == tagged
        assert hashlib.sha256(current).hexdigest() == binding["file_sha256"]
        assert payload["payload_sha256"] == binding["payload_sha256"]
        assert canonical_payload_sha256(payload) == binding["payload_sha256"]


RELAXATIONS: list[tuple[tuple[str, ...], Any]] = [
    (("direction_change",), True),
    (("release_transition", "published_7_0_annotated_tag_bound"), False),
    (("release_transition", "same_release_corrective_amendment_forbidden"), False),
    (("release_transition", "next_release"), "7.0"),
    (("correction", "created_before_any_7_1_return_replay"), False),
    (("correction", "sole_permitted_change", "to"), "allow any equivalent target order"),
    (("correction", "sole_permitted_change", "sort_kind"), "quicksort"),
    (("correction", "sole_permitted_change", "sort_key"), ["signal_date"]),
    (("runtime_and_stage_contract", "runtime_root"), "runtime/data/multi-asset-7.0"),
    (("runtime_and_stage_contract", "new_runtime_namespace_required"), False),
    (("runtime_and_stage_contract", "new_preselection_closure_required"), False),
    (("runtime_and_stage_contract", "reuse_7_0_derived_stages"), True),
    (("runtime_and_stage_contract", "reuse_7_0_evaluations"), True),
    (("runtime_and_stage_contract", "reuse_7_0_gate_or_status_views"), True),
    (("phase_contract", "formal_7_1_scope"), "train and validation"),
    (("phase_contract", "selected_candidate_id"), "causal_multi_horizon_trend_budget"),
    (("phase_contract", "validation_market_outcomes_opened"), True),
    (("phase_contract", "validation_stage_created"), True),
    (("phase_contract", "audit_market_outcomes_opened"), True),
    (("phase_contract", "audit_stage_created"), True),
    (("phase_contract", "runner_up_fallback"), True),
]
RELAXATIONS.extend(
    (("unchanged_contract", key), False) for key in EXPECTED_UNCHANGED_CONTRACT
)


@pytest.mark.parametrize(("path", "replacement"), RELAXATIONS)
def test_7_1_corrective_amendment_rejects_any_relaxation(
    path: tuple[str, ...], replacement: Any
) -> None:
    changed = copy.deepcopy(_read_amendment())
    _set_nested(changed, path, replacement)
    changed["payload_sha256"] = canonical_payload_sha256(changed)

    with pytest.raises(ValueError, match="exact whitelist"):
        _verify_amendment(changed)

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from factor_lab.release_integrity import canonical_payload_sha256


ROOT = Path(__file__).resolve().parents[2]
FAILURE_PATH = ROOT / "protocols" / "evidence" / "7.0" / "execution-failure.json"
EXECUTION_COMMIT = "76a47359d54c1de1691c07695bb18d42dcd89220"
CLOSURE_PAYLOAD = "d0b6072234d45363144a47517c8c4c535e4c9550ea36925a4b7cc54216110009"
MANIFEST_PAYLOAD = "58c0477745dd0afd6e8fad686af5379db00cf2736b25da71fd3a314217052130"
BINDING_PAYLOAD = "aed89c679c5a35881dce1b7f214719a8d4d5ade8bb1f29295412378f26bd6494"
EVALUATION_PAYLOAD = "b24a7158e0ebdf1f531533bce7ceeedff63a1d5a28f9c53d0cda8c2df39ead9b"
RUN_NONCE = "9099797bd5a343d59ca4ade722314542"


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_7_0_selection_execution_failure_is_self_hashed_and_bound() -> None:
    failure = json.loads(FAILURE_PATH.read_text(encoding="utf-8"))

    assert set(failure) == {
        "schema_version",
        "kind",
        "release",
        "status",
        "classification",
        "selection_execution",
        "frozen_inputs",
        "train_stage",
        "reproduction_diagnostic",
        "evidence_boundary",
        "corrective_release_contract",
        "payload_sha256",
    }
    assert failure["payload_sha256"] == (
        "04099ab6c2bd03099c9d045120578344bfe9ba3c963dfb82a0cba9f8a49f5df9"
    )
    assert failure["payload_sha256"] == canonical_payload_sha256(failure)
    assert failure["schema_version"] == 1
    assert failure["kind"] == "factor_lab_7_0_execution_failure"
    assert failure["release"] == "7.0"
    assert failure["status"] == "selection_inconclusive_software_failure"
    assert failure["classification"] == "target_order_replay_false_negative"

    execution = failure["selection_execution"]
    assert execution == {
        "command": "python scripts/run-multi-asset-evidence.py --mode selection",
        "execution_commit": EXECUTION_COMMIT,
        "phase": "train",
        "candidate_id": "causal_multi_horizon_trend_budget",
        "failure_function": "run-multi-asset-evidence._replay_evaluation",
        "exception_type": "ValueError",
        "exception_message": "train candidate targets do not match the causal builder",
        "canonical_failure_code": "target_order_replay_mismatch",
    }

    frozen_inputs = failure["frozen_inputs"]
    assert frozen_inputs == {
        "protocol": {
            "path": "protocols/7.0-multi-asset.json",
            "file_sha256": (
                "2d2e96a1605b5e088a7cf5952dd816d8aecb10e39b9ba529fe81b00592bfa14f"
            ),
            "payload_sha256": (
                "6f2fcd2a67d52bfae19bedcaecf495faa986195f6840da48a3a67a666589aaf0"
            ),
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
        },
        "preselection_closure": {
            "path": "protocols/7.0-release.json",
            "file_sha256": (
                "555e17eca0a9618e21e84133c825c447edf71999a1b8a7a39cc84b27753d4967"
            ),
            "payload_sha256": CLOSURE_PAYLOAD,
            "implementation_commit": (
                "67e8c6df57d4436d196dfc9fa6f2fc5e8d31959c"
            ),
        },
    }
    for binding in frozen_inputs.values():
        path = ROOT / binding["path"]
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert _file_sha256(path) == binding["file_sha256"]
        assert payload["payload_sha256"] == binding["payload_sha256"]

    train = failure["train_stage"]
    assert train == {
        "manifest": {
            "path": (
                "runtime/data/multi-asset-7.0/sources/stage=train/manifest.json"
            ),
            "file_sha256": (
                "3d926e16927ea64549755b118b3997e93da287175678f8f04703a637a069f3de"
            ),
            "payload_sha256": MANIFEST_PAYLOAD,
            "price_end_date": "2019-12-31",
            "calendar_end_date": "2020-01-31",
            "asset_count": 6,
            "asset_row_count": 8730,
            "calendar_row_count": 1471,
        },
        "binding": {
            "path": "runtime/data/multi-asset-7.0/stage-bindings/train.json",
            "file_sha256": (
                "62b0f0b42d16ee186a6c682c5ff35d4968b32d2ba0aeb40a119ff31b94390ba2"
            ),
            "payload_sha256": BINDING_PAYLOAD,
            "stage_manifest_file_sha256": (
                "3d926e16927ea64549755b118b3997e93da287175678f8f04703a637a069f3de"
            ),
            "stage_manifest_payload_sha256": MANIFEST_PAYLOAD,
            "implementation_closure_payload_sha256": CLOSURE_PAYLOAD,
            "execution_commit": EXECUTION_COMMIT,
            "run_nonce": RUN_NONCE,
        },
        "evaluation": {
            "path": (
                "runtime/data/multi-asset-7.0/evaluations/stage=train/evaluation.json"
            ),
            "file_sha256": (
                "f142e9dea38d95c94dd8a0fc67749e28fb83e3f7bb94af5813c8673edd0079d9"
            ),
            "payload_sha256": EVALUATION_PAYLOAD,
            "source_manifest_payload_sha256": MANIFEST_PAYLOAD,
            "stage_binding_payload_sha256": BINDING_PAYLOAD,
            "implementation_closure_payload_sha256": CLOSURE_PAYLOAD,
            "execution_commit": EXECUTION_COMMIT,
            "run_nonce": RUN_NONCE,
            "artifact_file_count": 15,
            "artifact_row_count": 33249,
            "gate_passed": False,
        },
        "disclosed_replay_hashes": {
            "candidate_metrics_sha256": (
                "876b70b3e1c821a2163434ac7601d7d37164df5b140a4da048aa3500d55910b3"
            ),
            "stress_metrics_sha256": (
                "5ad62281eb22edb5c9256c7aab06b233e8f74671b0bc3cf3672c3b5d28a29036"
            ),
            "control_metrics_sha256": (
                "fb1b146e34d62486dfd2c7ff39102ca7418419260f7eda99b11b6c2768c12492"
            ),
            "combined_metrics_sha256": (
                "2dda50d1fa41c0490eef41ccba0be469066b21b82ac02d1a6b109e0fe2b2944c"
            ),
            "gate_sha256": (
                "e03937a101011b546dd6195052f5f2ed6183c64b6697c081f621fd72f7a95adf"
            ),
        },
    }

    disclosure = json.loads(
        (ROOT / frozen_inputs["preclosure_train_disclosure"]["path"]).read_text(
            encoding="utf-8"
        )
    )
    hashes = train["disclosed_replay_hashes"]
    assert hashes["candidate_metrics_sha256"] == disclosure["candidate"][
        "canonical_metrics_sha256"
    ]
    assert hashes["stress_metrics_sha256"] == disclosure["stress"][
        "canonical_metrics_sha256"
    ]
    assert hashes["control_metrics_sha256"] == disclosure["control"][
        "canonical_metrics_sha256"
    ]
    assert hashes["combined_metrics_sha256"] == disclosure["relative"][
        "combined_metrics_sha256"
    ]
    assert hashes["gate_sha256"] == disclosure["train_gate"][
        "canonical_gate_sha256"
    ]


def test_7_0_failure_boundary_forbids_a_strategy_conclusion() -> None:
    failure = json.loads(FAILURE_PATH.read_text(encoding="utf-8"))
    diagnostic = failure["reproduction_diagnostic"]
    boundary = failure["evidence_boundary"]
    corrective = failure["corrective_release_contract"]

    assert diagnostic == {
        "target_roles": ["candidate", "control", "stress"],
        "rows_per_role": 348,
        "columns_per_role": 8,
        "columns_and_dtypes_exact_before_sort": True,
        "raw_row_order_exact": False,
        "persisted_first_signal_code_order": [
            "159920.SZ",
            "510300.SH",
            "511010.SH",
            "511880.SH",
            "513100.SH",
            "518880.SH",
        ],
        "causal_builder_first_signal_code_order": [
            "510300.SH",
            "159920.SZ",
            "513100.SH",
            "518880.SH",
            "511010.SH",
            "511880.SH",
        ],
        "stable_sort_key": ["signal_date", "code"],
        "stable_key_sorted_targets_exact": True,
        "all_fifteen_regenerated_artifacts_exact": True,
        "persisted_metrics_exact": True,
        "persisted_gate_exact": True,
        "finding": (
            "simulate_targets canonicalized persisted targets by signal_date and code, "
            "while the causal builder retained registered asset order; the verifier "
            "compared raw row order before comparing the already canonicalized replay "
            "artifacts."
        ),
    }

    assert boundary == {
        "return_kernel_executed": True,
        "return_metrics_persisted": True,
        "return_metrics_reported": True,
        "train_candidate_gate_evaluated": True,
        "train_candidate_gate_passed": False,
        "formal_closure_disclosure_binding_check_reached": True,
        "formal_disclosed_train_replay_hash_check_reached": False,
        "independent_full_disclosure_hash_check_passed": True,
        "validation_market_outcomes_opened": False,
        "validation_stage_created": False,
        "winner_freeze_created": False,
        "audit_market_outcomes_opened": False,
        "audit_stage_created": False,
        "terminal_result_created": False,
        "strategy_conclusion_allowed": False,
        "profit_claim_allowed": False,
    }

    assert corrective == {
        "next_release": "7.1",
        "direction_change": False,
        "permitted_change": (
            "Compare causal and persisted targets only after stable canonical ordering "
            "by the unique signal_date and code key."
        ),
        "candidate_assets_windows_budgets_and_costs_must_remain_unchanged": True,
        "data_total_return_portfolio_and_phase_contracts_must_remain_unchanged": True,
        "selection_and_audit_gates_must_remain_unchanged": True,
        "signal_execution_and_claim_contracts_must_remain_unchanged": True,
        "new_preselection_closure_required": True,
        "new_runtime_namespace_required": True,
        "reuse_of_7_0_derived_stage_evaluation_gate_or_status_forbidden": True,
        "validation_and_audit_must_remain_unopened": True,
        "published_7_0_annotated_tag_must_be_bound": True,
        "same_release_corrective_amendment_forbidden": True,
    }

    evidence_root = FAILURE_PATH.parent
    assert not (evidence_root / "winner-freeze.json").exists()
    assert not (evidence_root / "historical-audit.json").exists()
    assert not (evidence_root / "result.json").exists()

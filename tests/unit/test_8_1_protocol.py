from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Mapping

import pytest

from factor_lab.release_integrity import canonical_payload_sha256, file_sha256


ROOT = Path(__file__).resolve().parents[2]
PROTOCOL_PATH = ROOT / "protocols" / "8.1-policy-operational-metric-reclassification.json"
RECEIPT_PATH = ROOT / "protocols" / "evidence" / "8.0" / "execution-failure.json"
EXPECTED_PAYLOAD = "2fc5ea8316173f7fd19fbf5c34248e5a70b2a901c99345dcf8d933826fa15ee5"
EXPECTED_FILE_SHA256 = "b0a213b62cf6f2723425e77d01565fd8c29721960d50d4a25d19306f3817c583"
EXPECTED_ROLES = ["primary", "stress", "cash", "cash_stress"]
EXPECTED_CLAIM = {
    "research_object": "strategic_asset_allocation_beta",
    "alpha_claim_allowed": False,
    "profit_claim_allowed": False,
    "stable_future_profit_claim_allowed": False,
    "historical_pass_interpretation": (
        "historical fixed-instrument strategic beta diagnostic only"
    ),
    "validation_and_audit_are_public_history_not_fresh_future_evidence": True,
    "fresh_future_evidence_required": True,
    "minimum_fresh_sessions": 252,
    "minimum_fresh_monthly_executions": 12,
    "first_fresh_session_rule": (
        "first official session after the immutable 8.0 closure and never earlier "
        "than 2026-08-31"
    ),
    "investment_recommendation_allowed": False,
}


def _read(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _protocol() -> dict[str, Any]:
    return _read(PROTOCOL_PATH)


def _set_nested(value: Any, path: tuple[Any, ...], replacement: Any) -> None:
    current = value
    for key in path[:-1]:
        current = current[key]
    current[path[-1]] = replacement


def _verify_exact_protocol(value: Mapping[str, Any]) -> None:
    if (
        value.get("payload_sha256") != EXPECTED_PAYLOAD
        or canonical_payload_sha256(value) != EXPECTED_PAYLOAD
    ):
        raise ValueError("8.1 protocol differs from the exact frozen contract")


def test_8_1_protocol_is_self_hashed_and_uses_a_fresh_namespace() -> None:
    protocol = _protocol()

    assert protocol["payload_sha256"] == canonical_payload_sha256(protocol)
    assert protocol["payload_sha256"] == EXPECTED_PAYLOAD
    assert file_sha256(PROTOCOL_PATH) == EXPECTED_FILE_SHA256
    assert protocol["release"] == "8.1"
    assert protocol["direction_change"] is False
    assert protocol["route"] == "policy_operational_metric_reclassification"
    assert protocol["protocol_id"] == (
        "factor-lab/8.1/policy-operational-metric-reclassification-v1"
    )
    phases = protocol["physical_phases"]
    assert phases["train_reclassification"]["runtime_stage"] == (
        "runtime/data/multi-asset-8.0"
    )
    assert phases["train_reclassification"]["evidence_path"] == (
        "protocols/evidence/8.1/train-reclassification.json"
    )
    assert phases["validation"]["source_root"] == (
        "runtime/data/multi-asset-8.1/sources/stage=validation"
    )
    assert phases["audit"]["source_root"] == (
        "runtime/data/multi-asset-8.1/sources/stage=audit"
    )


def test_8_1_protocol_binds_the_published_8_0_receipt_and_exact_role_metrics() -> None:
    protocol = _protocol()
    receipt = _read(RECEIPT_PATH)
    prior = protocol["prior_release"]
    source = protocol["train_reclassification_input"]

    assert prior["tag"] == "8.0"
    assert prior["annotated_tag_object"] == (
        "3fcbd73f7497b074e484ce7793e2d3603bf5a177"
    )
    assert prior["peeled_commit"] == "78aba86bf4e741699afca1acd1470493785fd952"
    assert prior["execution_failure_receipt"] == {
        "path": "protocols/evidence/8.0/execution-failure.json",
        "file_sha256": (
            "6af779495081f6ee391c6388a1e4342b878168b529f8074cf03d9ec2cc50eeaa"
        ),
        "payload_sha256": (
            "751b85c6c2e52b450e9c3549f7f4504af50b634599be4c32e240ee503de9823a"
        ),
        "status": "selection_inconclusive_execution_failure",
        "classification": "post_evaluation_github_ci_transport_failure",
    }
    assert source["role_gate_metrics"] == receipt["train_stage"]["role_gate_metrics"]
    assert source["historical_summary"] == receipt["train_stage"]["historical_summary"]
    assert source["role_gate_metrics_sha256"] == canonical_payload_sha256(
        source["role_gate_metrics"]
    )
    assert source["historical_summary_sha256"] == canonical_payload_sha256(
        source["historical_summary"]
    )
    for role in EXPECTED_ROLES:
        assert source["role_metric_sha256"][role] == canonical_payload_sha256(
            source["role_gate_metrics"][role]
        )
    assert source["economic_role_metrics_source_is_receipt_only"] is True
    assert source["receipt_bound_8_0_runtime_read_required"] is True
    assert source["receipt_bound_artifact_metrics_must_not_replace_or_recompute_receipt_metrics"] is True
    assert source["new_train_market_access_required"] is False


def test_8_1_metric_roles_change_only_policy_operational_aggregation() -> None:
    protocol = _protocol()
    roles = protocol["metric_role_contract"]
    correction = protocol["correction_boundary"]
    gate = protocol["shared_absolute_gate"]

    assert correction["post_hoc_reclassification"] is True
    assert correction["train_strategy_rerun_allowed"] is False
    assert correction["train_artifact_metric_recomputation_allowed"] is False
    assert correction["parameter_weight_asset_cost_date_or_threshold_change_allowed"] is False
    assert roles["required_roles_exact"] == EXPECTED_ROLES
    assert roles["policy_operational_metrics"] == {
        "roles": ["primary", "stress"],
        "annualized_turnover": "maximum across primary and stress",
        "requested_notional_fill_ratio": "minimum across primary and stress",
        "capacity_limited_requested_notional_ratio": "maximum across primary and stress",
        "cash_roles_included": False,
    }
    assert roles["cash_role_diagnostics"]["roles"] == ["cash", "cash_stress"]
    assert roles["cash_role_diagnostics"]["required_and_disclosed"] is True
    assert roles["cash_role_diagnostics"]["required_for_cash_excess"] is True
    assert roles["accounting_validity"]["roles"] == EXPECTED_ROLES
    assert gate["operational"] == {
        "annualized_turnover_at_most": 1.0,
        "requested_notional_fill_ratio_at_least": 0.99,
        "capacity_limited_requested_notional_ratio_at_most": 0.01,
        "nav_reconciliation_error_at_most": 1e-8,
    }
    assert protocol["claim_contract"] == EXPECTED_CLAIM


def test_8_1_four_role_validity_preserves_the_frozen_economic_gate_scope() -> None:
    validity = _protocol()["execution_validity_hard_fail"]

    assert validity["applies_before_gate_classification"] is True
    assert validity["required_role_set_must_match_exactly"] is True
    assert validity[
        "blocked_missing_open_trade_count_must_be_exact_nonnegative_integer"
    ] is True
    assert validity["blocked_missing_open_trade_count_is_hard_failure"] is False
    assert validity[
        "blocked_capacity_trade_count_must_be_exact_nonnegative_integer"
    ] is True
    assert validity["blocked_capacity_trade_count_is_hard_failure"] is False
    assert validity["capacity_violation_count_at_most"] == 0
    assert validity["capacity_violation_is_frozen_execution_contract_breach"] is True
    assert validity["negative_cash_observation_count_at_most"] == 0
    assert validity["leverage_observation_count_at_most"] == 0
    assert validity["capacity_fields_must_be_finite_and_nonnegative"] is True
    assert validity["capacity_aggregation_identity_must_reconcile_exactly"] is True
    assert validity["accounting_error_must_be_finite_for_all_four_roles"] is True
    assert validity["invalid_execution_cannot_be_reclassified_as_gate_failure"] is True


RELAXATIONS: list[tuple[tuple[Any, ...], Any]] = [
    (("direction_change",), True),
    (("route",), "static_beta_retry"),
    (("prior_release", "annotated_tag_object"), "0" * 40),
    (("prior_release", "execution_failure_receipt", "payload_sha256"), "0" * 64),
    (("correction_boundary", "post_hoc_reclassification"), False),
    (("correction_boundary", "train_strategy_rerun_allowed"), True),
    (("correction_boundary", "train_artifact_metric_recomputation_allowed"), True),
    (
        ("correction_boundary", "parameter_weight_asset_cost_date_or_threshold_change_allowed"),
        True,
    ),
    (("train_reclassification_input", "economic_role_metrics_source_is_receipt_only"), False),
    (("train_reclassification_input", "role_gate_metrics", "primary", "cagr"), 0.08),
    (("metric_role_contract", "policy_operational_metrics", "roles"), EXPECTED_ROLES),
    (("metric_role_contract", "policy_operational_metrics", "cash_roles_included"), True),
    (("metric_role_contract", "accounting_validity", "roles"), ["primary", "stress"]),
    (("shared_absolute_gate", "base", "net_sharpe_at_least"), 0.29),
    (("shared_absolute_gate", "operational", "requested_notional_fill_ratio_at_least"), 0.98),
    (
        (
            "execution_validity_hard_fail",
            "blocked_missing_open_trade_count_is_hard_failure",
        ),
        True,
    ),
    (
        (
            "execution_validity_hard_fail",
            "blocked_capacity_trade_count_is_hard_failure",
        ),
        True,
    ),
    (("execution_validity_hard_fail", "capacity_violation_count_at_most"), 1),
    (
        (
            "execution_validity_hard_fail",
            "capacity_violation_is_frozen_execution_contract_breach",
        ),
        False,
    ),
    (("execution_validity_hard_fail", "negative_cash_observation_count_at_most"), 1),
    (("execution_validity_hard_fail", "leverage_observation_count_at_most"), 1),
    (("physical_phases", "validation", "market_outcome_opened"), True),
    (("physical_phases", "audit", "market_outcome_opened"), True),
    (("selection_contract", "runner_up_fallback"), True),
    (("transport_verification", "missing_or_mismatched_remote_identity_allowed"), True),
    (("claim_contract", "profit_claim_allowed"), True),
    (("claim_contract", "fresh_future_evidence_required"), False),
]


@pytest.mark.parametrize(("path", "replacement"), RELAXATIONS)
def test_8_1_protocol_rejects_any_rehashed_relaxation(
    path: tuple[Any, ...], replacement: Any
) -> None:
    changed = copy.deepcopy(_protocol())
    _set_nested(changed, path, replacement)
    changed["payload_sha256"] = canonical_payload_sha256(changed)

    with pytest.raises(ValueError, match="exact frozen contract"):
        _verify_exact_protocol(changed)

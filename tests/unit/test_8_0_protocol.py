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
PROTOCOL_PATH = ROOT / "protocols" / "8.0-static-capital-budget.json"
PROTOCOL_FILE_SHA256 = "ac4a6f94cfbbe709c26120bad7499196fa36fc497f366cf445896cd486519abc"
PROTOCOL_PAYLOAD_SHA256 = "801374f58aa5edd66365e0937ed119082559f2950cc1106134a3cdb58e0099e7"
TAG = "7.1"
TAG_OBJECT = "15ea8e8de95638fdc0786ff0f35177b0ecba878d"
TAG_COMMIT = "e7f09e17646cc44d78a49f6ddc41acc471f205d4"
STATIC_CONTROL_METRICS_SHA256 = (
    "fb1b146e34d62486dfd2c7ff39102ca7418419260f7eda99b11b6c2768c12492"
)

TOP_LEVEL_FIELDS = {
    "schema_version",
    "kind",
    "protocol_id",
    "release",
    "direction_change",
    "route",
    "status",
    "prior_release",
    "prior_train_exposure",
    "research_question",
    "assets",
    "strategy_registry",
    "cash_comparator",
    "inherited_data_execution_contract",
    "cost_contract",
    "physical_phases",
    "metric_contract",
    "shared_absolute_gate",
    "phase_gates",
    "selection_contract",
    "survivorship_and_selection_bias",
    "claim_contract",
    "payload_sha256",
}

EXPECTED_PRIOR_RELEASE = {
    "release": "7.1",
    "tag": TAG,
    "annotated_tag_object": TAG_OBJECT,
    "peeled_commit": TAG_COMMIT,
    "preselection_closure": {
        "path": "protocols/7.1-release.json",
        "file_sha256": (
            "794b11d55cfbdf1f33e5e15c917691b76f244a9fd5f8f400a5f862d7830f11cd"
        ),
        "payload_sha256": (
            "8cd80c7c770477cf29c2fa04348e9ed16f637f7d5ee61f31232d6f1f81ff2e55"
        ),
    },
    "winner_freeze": {
        "path": "protocols/evidence/7.1/winner-freeze.json",
        "file_sha256": (
            "2b239ac699d80db0965d87f1fb96a366b7a2f820c173fa08988fb4801323fa77"
        ),
        "payload_sha256": (
            "451b7de8bbcba9372731b7dd7236e16a46467bdf5499eeff5e17e8e946ffabfd"
        ),
        "status": "selected_null_frozen_train_failed",
        "selected_candidate_id": None,
    },
    "terminal_result": {
        "path": "protocols/evidence/7.1/result.json",
        "file_sha256": (
            "ff0278104d1e7fd5f940671322e1987ea416bb4eeb7b3a343ec814393053449a"
        ),
        "payload_sha256": (
            "869b6f1fe028378e1071a416c7f8d045650a41c17c01bd9a1d48f62b35c3a4b9"
        ),
        "status": "selection_falsified_no_candidate",
        "selected_candidate_id": None,
        "audit_status": "not_opened",
    },
}

EXPECTED_BUDGETS = [
    {
        "ts_code": "510300.SH",
        "role": "mainland_china_large_equity",
        "target_weight": 0.30,
    },
    {
        "ts_code": "159920.SZ",
        "role": "hong_kong_equity",
        "target_weight": 0.10,
    },
    {
        "ts_code": "513100.SH",
        "role": "united_states_equity",
        "target_weight": 0.10,
    },
    {"ts_code": "518880.SH", "role": "gold", "target_weight": 0.20},
    {
        "ts_code": "511010.SH",
        "role": "five_year_government_bond",
        "target_weight": 0.30,
    },
    {
        "ts_code": "511880.SH",
        "role": "exchange_traded_money_market_cash_proxy",
        "target_weight": 0.0,
    },
]

EXPECTED_GATE = {
    "base": {
        "net_cagr_strictly_positive": True,
        "net_sharpe_at_least": 0.30,
        "daily_max_drawdown_at_least": -0.25,
        "positive_complete_year_ratio_at_least": 0.50,
        "cash_excess_cagr_strictly_positive": True,
    },
    "stress_16bp": {
        "net_cagr_strictly_positive": True,
        "net_sharpe_at_least": 0.25,
        "daily_max_drawdown_at_least": -0.25,
        "positive_complete_year_ratio_at_least": 0.50,
        "cash_excess_cagr_strictly_positive": True,
    },
    "operational": {
        "annualized_turnover_at_most": 1.0,
        "requested_notional_fill_ratio_at_least": 0.99,
        "capacity_limited_requested_notional_ratio_at_most": 0.01,
        "nav_reconciliation_error_at_most": 1e-8,
    },
}

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


def _read_protocol() -> dict[str, Any]:
    value = json.loads(PROTOCOL_PATH.read_text(encoding="utf-8"))
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


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _verify_protocol(value: dict[str, Any]) -> None:
    if set(value) != TOP_LEVEL_FIELDS:
        raise ValueError("8.0 protocol field set differs")
    if (
        value.get("payload_sha256") != PROTOCOL_PAYLOAD_SHA256
        or value.get("payload_sha256") != canonical_payload_sha256(value)
        or value.get("schema_version") != 1
        or value.get("kind") != "factor_lab_research_protocol"
        or value.get("protocol_id")
        != "factor-lab/8.0/strategic-static-capital-budget-beta-v1"
        or value.get("release") != "8.0"
        or value.get("direction_change") is not True
        or value.get("route") != "strategic_static_capital_budget_beta"
        or value.get("status")
        != "frozen_after_disclosed_train_before_any_2020_plus_market_access"
        or value.get("prior_release") != EXPECTED_PRIOR_RELEASE
        or value.get("assets", {}).get("fixed_capital_budgets") != EXPECTED_BUDGETS
        or value.get("assets", {}).get("target_weight_sum") != 1.0
        or value.get("assets", {}).get("cash_proxy_residual_only") is not True
        or value.get("assets", {}).get("asset_substitution_allowed") is not False
        or value.get("shared_absolute_gate") != EXPECTED_GATE
        or value.get("claim_contract") != EXPECTED_CLAIM
    ):
        raise ValueError("8.0 protocol differs from its exact frozen contract")
    registry = value.get("strategy_registry")
    if (
        not isinstance(registry, list)
        or len(registry) != 1
        or registry[0].get("strategy_id") != "static_risk_budget"
        or "prior_exposed_alias" in registry[0]
        or registry[0].get("alpha_model") is not None
        or registry[0].get("trend_filter") is not None
        or registry[0].get("parameter_grid") is not None
        or registry[0].get("runner_up") is not None
    ):
        raise ValueError("8.0 strategy registry is not the single static policy")


def _set_nested(value: dict[str, Any], path: tuple[Any, ...], replacement: Any) -> None:
    cursor: Any = value
    for key in path[:-1]:
        cursor = cursor[key]
    cursor[path[-1]] = replacement


def test_8_0_protocol_is_exact_self_hashed_and_byte_frozen() -> None:
    protocol = _read_protocol()

    _verify_protocol(protocol)
    assert _file_sha256(PROTOCOL_PATH) == PROTOCOL_FILE_SHA256


def test_8_0_binds_the_published_7_1_terminal_chain() -> None:
    protocol = _read_protocol()
    prior = protocol["prior_release"]

    assert _git("cat-file", "-t", f"refs/tags/{TAG}").decode("ascii").strip() == "tag"
    assert _git("rev-parse", f"refs/tags/{TAG}").decode("ascii").strip() == TAG_OBJECT
    assert (
        _git("rev-parse", f"refs/tags/{TAG}^{{}}").decode("ascii").strip()
        == TAG_COMMIT
    )
    for name in ("preselection_closure", "winner_freeze", "terminal_result"):
        binding = prior[name]
        path = ROOT / binding["path"]
        current = path.read_bytes()
        tagged = _git("show", f"{TAG}:{binding['path']}")
        payload = json.loads(current.decode("utf-8"))

        assert current == tagged
        assert hashlib.sha256(current).hexdigest() == binding["file_sha256"]
        assert payload["payload_sha256"] == binding["payload_sha256"]
        assert canonical_payload_sha256(payload) == binding["payload_sha256"]


def test_8_0_discloses_the_already_seen_static_train_control() -> None:
    protocol = _read_protocol()
    exposure = protocol["prior_train_exposure"]
    binding = exposure["source"]
    path = ROOT / binding["path"]
    disclosure = json.loads(path.read_text(encoding="utf-8"))

    assert _file_sha256(path) == binding["file_sha256"]
    assert disclosure["payload_sha256"] == binding["payload_sha256"]
    assert canonical_payload_sha256(disclosure) == binding["payload_sha256"]
    assert exposure["static_control_returns_opened"] is True
    assert exposure["independent_train_evidence"] is False
    assert exposure["thresholds_frozen_after_train_exposure"] is True
    assert exposure["static_control_metrics_sha256"] == STATIC_CONTROL_METRICS_SHA256
    assert disclosure["control"]["canonical_metrics_sha256"] == (
        STATIC_CONTROL_METRICS_SHA256
    )
    assert exposure["static_stress_outcome_previously_opened"] is False
    assert exposure["cash_benchmark_outcome_previously_opened"] is False


def test_8_0_binds_the_fixed_asset_and_inherited_execution_contracts() -> None:
    protocol = _read_protocol()
    bindings = (
        protocol["assets"]["asset_selection_evidence"],
        protocol["inherited_data_execution_contract"]["source"],
    )

    for binding in bindings:
        path = ROOT / binding["path"]
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert _file_sha256(path) == binding["file_sha256"]
        assert payload["payload_sha256"] == binding["payload_sha256"]
        assert canonical_payload_sha256(payload) == binding["payload_sha256"]


def test_8_0_has_one_static_policy_one_cash_hurdle_and_identical_phase_gates() -> None:
    protocol = _read_protocol()
    budgets = protocol["assets"]["fixed_capital_budgets"]

    assert budgets == EXPECTED_BUDGETS
    assert sum(item["target_weight"] for item in budgets) == pytest.approx(1.0)
    assert budgets[-1]["ts_code"] == "511880.SH"
    assert budgets[-1]["target_weight"] == 0.0
    assert len(protocol["strategy_registry"]) == 1
    assert protocol["cash_comparator"] == {
        "comparator_id": "cash_only_511880",
        "selection_role": "fixed_policy_hurdle_only",
        "target_weight": 1.0,
        "execution": (
            "same fresh phase seed, next-open, lot, cost and dividend accounting "
            "as the strategic policy"
        ),
        "rebalance": (
            "constant 100% target at every month-end signal for next-open execution; "
            "trades arise only when cash distributions, lot rounding or residual cash "
            "require them"
        ),
        "cash_excess_cagr_definition": (
            "strategic policy CAGR minus investable cash-only CAGR over the "
            "identical phase"
        ),
        "cash_excess_is_alpha_claim": False,
    }
    assert protocol["phase_gates"] == {
        "train": {"gate_ref": "shared_absolute_gate"},
        "validation": {"gate_ref": "shared_absolute_gate"},
        "audit": {"gate_ref": "shared_absolute_gate"},
    }
    assert protocol["shared_absolute_gate"] == EXPECTED_GATE


def test_8_0_validation_audit_bias_and_claim_boundaries_are_explicit() -> None:
    protocol = _read_protocol()
    phases = protocol["physical_phases"]
    bias = protocol["survivorship_and_selection_bias"]
    claim = protocol["claim_contract"]

    assert phases["train"] == {
        "performance_start": "2015-03-02",
        "performance_end": "2019-12-31",
        "role": "disclosed_calibration_replay",
        "market_outcome_previously_opened": True,
        "source_root": "runtime/data/multi-asset-8.0/sources/stage=train",
        "evaluation_root": "runtime/data/multi-asset-8.0/evaluations/stage=train",
        "binding_path": "runtime/data/multi-asset-8.0/stage-bindings/train.json",
    }
    assert phases["validation"]["performance_start"] == "2020-01-02"
    assert phases["validation"]["performance_end"] == "2022-12-30"
    assert phases["validation"]["market_outcome_opened"] is False
    assert phases["validation"]["opened_only_after_committed_train_pass"] is True
    assert phases["validation"]["source_root"] == (
        "runtime/data/multi-asset-8.0/sources/stage=validation"
    )
    assert phases["validation"]["evaluation_root"] == (
        "runtime/data/multi-asset-8.0/evaluations/stage=validation"
    )
    assert phases["validation"]["binding_path"] == (
        "runtime/data/multi-asset-8.0/stage-bindings/validation.json"
    )
    assert phases["audit"]["performance_start"] == "2023-01-03"
    assert phases["audit"]["performance_end"] == "2026-08-28"
    assert phases["audit"]["market_outcome_opened"] is False
    assert (
        phases["audit"]["opened_only_after_committed_validation_pass_and_policy_freeze"]
        is True
    )
    assert phases["audit"]["source_root"] == (
        "runtime/data/multi-asset-8.0/sources/stage=audit"
    )
    assert phases["audit"]["evaluation_root"] == (
        "runtime/data/multi-asset-8.0/evaluations/stage=audit"
    )
    assert phases["audit"]["binding_path"] == (
        "runtime/data/multi-asset-8.0/stage-bindings/audit.json"
    )
    assert bias["survivorship_bias_eliminated"] is False
    assert bias["current_2026_survival_known_when_direction_chosen"] is True
    assert bias["researcher_asset_class_and_representative_choice_bias_present"] is True
    assert bias["universe_generalization_allowed"] is False
    assert bias["asset_class_index_generalization_allowed"] is False
    assert claim == EXPECTED_CLAIM


RELAXATIONS: list[tuple[tuple[Any, ...], Any]] = [
    (("direction_change",), False),
    (("route",), "strategic_timing_alpha"),
    (("prior_train_exposure", "static_control_returns_opened"), False),
    (("prior_train_exposure", "independent_train_evidence"), True),
    (("prior_train_exposure", "thresholds_frozen_after_train_exposure"), False),
    (("assets", "fixed_capital_budgets", 0, "target_weight"), 0.35),
    (("assets", "fixed_capital_budgets", 5, "target_weight"), 0.10),
    (("assets", "asset_substitution_allowed"), True),
    (("strategy_registry", 0, "strategy_id"), "static_capital_budget"),
    (("cash_comparator", "target_weight"), 0.90),
    (("cash_comparator", "selection_role"), "optional comparator"),
    (("cash_comparator", "cash_excess_is_alpha_claim"), True),
    (("inherited_data_execution_contract", "future_input_allowed"), True),
    (
        ("inherited_data_execution_contract", "capacity_limit_fraction_of_signal_date_adv20"),
        0.20,
    ),
    (("shared_absolute_gate", "base", "net_cagr_strictly_positive"), False),
    (("shared_absolute_gate", "base", "net_sharpe_at_least"), 0.20),
    (("shared_absolute_gate", "base", "daily_max_drawdown_at_least"), -0.30),
    (("shared_absolute_gate", "base", "positive_complete_year_ratio_at_least"), 0.33),
    (("shared_absolute_gate", "base", "cash_excess_cagr_strictly_positive"), False),
    (("shared_absolute_gate", "stress_16bp", "net_cagr_strictly_positive"), False),
    (("shared_absolute_gate", "stress_16bp", "net_sharpe_at_least"), 0.20),
    (("shared_absolute_gate", "stress_16bp", "daily_max_drawdown_at_least"), -0.30),
    (
        ("shared_absolute_gate", "stress_16bp", "positive_complete_year_ratio_at_least"),
        0.33,
    ),
    (
        ("shared_absolute_gate", "stress_16bp", "cash_excess_cagr_strictly_positive"),
        False,
    ),
    (("shared_absolute_gate", "operational", "annualized_turnover_at_most"), 2.0),
    (
        ("shared_absolute_gate", "operational", "requested_notional_fill_ratio_at_least"),
        0.98,
    ),
    (
        (
            "shared_absolute_gate",
            "operational",
            "capacity_limited_requested_notional_ratio_at_most",
        ),
        0.02,
    ),
    (
        ("shared_absolute_gate", "operational", "nav_reconciliation_error_at_most"),
        1e-6,
    ),
    (("physical_phases", "train", "source_root"), "runtime/data/multi-asset-7.1"),
    (("physical_phases", "validation", "evaluation_root"), "runtime/data/shared"),
    (("physical_phases", "audit", "binding_path"), "runtime/data/shared.json"),
    (("physical_phases", "validation", "market_outcome_opened"), True),
    (("physical_phases", "audit", "market_outcome_opened"), True),
    (("selection_contract", "best_of_n_selection"), True),
    (("selection_contract", "parameter_or_weight_change_after_any_phase"), True),
    (("selection_contract", "asset_or_comparator_substitution"), True),
    (("selection_contract", "runner_up_fallback"), True),
    (("survivorship_and_selection_bias", "survivorship_bias_eliminated"), True),
    (("survivorship_and_selection_bias", "universe_generalization_allowed"), True),
    (("survivorship_and_selection_bias", "asset_class_index_generalization_allowed"), True),
    (("claim_contract", "alpha_claim_allowed"), True),
    (("claim_contract", "profit_claim_allowed"), True),
    (("claim_contract", "stable_future_profit_claim_allowed"), True),
    (("claim_contract", "fresh_future_evidence_required"), False),
    (("claim_contract", "minimum_fresh_sessions"), 251),
    (("claim_contract", "minimum_fresh_monthly_executions"), 11),
    (("claim_contract", "investment_recommendation_allowed"), True),
]


@pytest.mark.parametrize(("path", "replacement"), RELAXATIONS)
def test_8_0_protocol_rejects_any_rehashed_relaxation(
    path: tuple[Any, ...], replacement: Any
) -> None:
    changed = copy.deepcopy(_read_protocol())
    _set_nested(changed, path, replacement)
    changed["payload_sha256"] = canonical_payload_sha256(changed)

    with pytest.raises(ValueError, match="exact frozen contract"):
        _verify_protocol(changed)

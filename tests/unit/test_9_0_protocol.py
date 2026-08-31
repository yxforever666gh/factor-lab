import copy
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from factor_lab.release_integrity import canonical_payload_sha256


ROOT = Path(__file__).resolve().parents[2]
SCOUT_PATH = ROOT / "protocols" / "9.0-preprotocol-scout.json"
PROTOCOL_PATH = ROOT / "protocols" / "9.0-causal-volatility-balanced-budget.json"
SCOUT_PAYLOAD = "71926f08ce5ca2ab1b6470f7d3ee385371c4bfaf3243c5f942a891f63a8075a0"
SCOUT_FILE_SHA256 = "44b90b964ecca9a30029b1dfad45ae313ae4a5c12a91d82ba885ceecb826b857"
PROTOCOL_PAYLOAD = "f6c7cce39e8b9a1ae5df10965a2dd607916095b2caf24fcf0a29b625c5bafc3e"
PROTOCOL_FILE_SHA256 = "19ecf56b5bd9c8b42b9f4df50761f719e2ca544eaea959a88c62d0ea4178d620"
BINARY64_NORMALIZATION = (
    "in fixed RISK_CODES order compute the first four weights as raw_i / "
    "math.fsum(raw), then set the fifth weight to 1.0 - math.fsum(the first "
    "four); the residual-normalized risk-weight sum is accepted within the "
    "frozen 1e-12 target-sum tolerance"
)


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _set_nested(value: Any, path: tuple[Any, ...], replacement: Any) -> None:
    cursor = value
    for key in path[:-1]:
        cursor = cursor[key]
    cursor[path[-1]] = replacement


def test_9_0_scout_and_protocol_are_exactly_self_hashed() -> None:
    scout = _read(SCOUT_PATH)
    protocol = _read(PROTOCOL_PATH)
    assert scout["payload_sha256"] == canonical_payload_sha256(scout) == SCOUT_PAYLOAD
    assert protocol["payload_sha256"] == canonical_payload_sha256(protocol) == PROTOCOL_PAYLOAD
    assert hashlib.sha256(SCOUT_PATH.read_bytes()).hexdigest() == SCOUT_FILE_SHA256
    assert hashlib.sha256(PROTOCOL_PATH.read_bytes()).hexdigest() == PROTOCOL_FILE_SHA256
    assert protocol["preprotocol_scout"] == {
        "path": "protocols/9.0-preprotocol-scout.json",
        "file_sha256": SCOUT_FILE_SHA256,
        "payload_sha256": SCOUT_PAYLOAD,
        "status": "selected_volatility_balanced_after_fully_exposed_development",
        "prototype_count": 2,
        "formal_candidate_count": 1,
    }


def test_9_0_binds_the_exact_published_8_1_null_chain() -> None:
    protocol = _read(PROTOCOL_PATH)
    prior = protocol["prior_release"]
    assert prior["tag"] == "8.1"
    assert prior["annotated_tag_object"] == "8f575ed3833c8cc01f89e7a951d4234bd7ee6622"
    assert prior["peeled_commit"] == "a4c0d36f727e99f6b2353facf24fd3cdedba958e"
    expected = {
        "protocol": (
            ROOT / "protocols/8.1-policy-operational-metric-reclassification.json",
            "b0a213b62cf6f2723425e77d01565fd8c29721960d50d4a25d19306f3817c583",
            "2fc5ea8316173f7fd19fbf5c34248e5a70b2a901c99345dcf8d933826fa15ee5",
        ),
        "preselection_closure": (
            ROOT / "protocols/8.1-release.json",
            "ef1596fa5cfbfdfd0c27d74c2747dcc852b7f209a4e27de2b7c01c6d8dbcc557",
            "f4a47421d08ca77eca6b27fd6417909a04c3eaf789c11d9ca069366412440ef5",
        ),
        "train_reclassification": (
            ROOT / "protocols/evidence/8.1/train-reclassification.json",
            "bfd2c0c801259394861eba000a8e34bc9617cba3adcf6629d7e8b501ccf3c51b",
            "4f498ffc12deac61144c77c56ba89cb9abccc034d2d73df4f1df8a6c50184c79",
        ),
        "winner_freeze": (
            ROOT / "protocols/evidence/8.1/winner-freeze.json",
            "b865e80cb899f7e5274d72b46ab1e0d88dad64b0ab2eb4e46750c5cec2167387",
            "d10f51b522a16838a4744fa16d770a720d34c2d340c2bf0bd5a05bedc61ceb76",
        ),
        "terminal_result": (
            ROOT / "protocols/evidence/8.1/result.json",
            "bcbcb09974e6314190de7a835560c4abbc1cde79734ed4fcef759061653cd95d",
            "d4496b9a64def6a443827737987d44ec77532cc9d11137a247302376a00ad6a4",
        ),
    }
    for key, (path, file_hash, payload_hash) in expected.items():
        assert hashlib.sha256(path.read_bytes()).hexdigest() == file_hash
        value = _read(path)
        assert value["payload_sha256"] == canonical_payload_sha256(value) == payload_hash
        assert prior[key]["file_sha256"] == file_hash
        assert prior[key]["payload_sha256"] == payload_hash
    assert prior["winner_freeze"]["status"] == "selected_null_frozen_validation_failed"
    assert prior["terminal_result"]["status"] == "selection_falsified_no_candidate"
    assert prior["terminal_result"]["selected_candidate_id"] is None
    assert prior["terminal_result"]["audit_status"] == "not_opened"


def test_scout_discloses_both_prototypes_and_selects_only_volatility_balance() -> None:
    scout = _read(SCOUT_PATH)
    prototypes = scout["prototypes"]
    assert set(prototypes) == {
        "causal_volatility_balanced_budget_v0",
        "causal_three_expert_exponentiated_gradient_v0",
    }
    vol = prototypes["causal_volatility_balanced_budget_v0"]
    eg = prototypes["causal_three_expert_exponentiated_gradient_v0"]
    assert vol["selected_for_formalization"] is True
    assert vol["formula"]["volatility_return_count"] == 126
    assert vol["formula"]["required_observed_total_return_levels"] == 127
    assert vol["formula"]["volatility_floor"] == 1e-12
    assert vol["formula"]["normalization"] == BINARY64_NORMALIZATION
    assert scout["uniform_development_contract"][
        "fresh_cash_each_development_subperiod"
    ] is True
    assert scout["uniform_development_contract"][
        "development_subperiod_account_reuse"
    ] is False
    captured = vol["captured_artifacts"]
    assert captured["prototype_file_sha256"] == (
        "8dcf6617a25d49be7929b9f5348983f84daaf81c07e24a4c3432e602e1f5b218"
    )
    assert captured["result_file_sha256"] == (
        "665344f3bced8f7dad1437fac60a6c10e74d5be36dcd37b072582ff48e0dbcf3"
    )
    annotation = captured["accounting_annotation_audit"]
    assert annotation["prior_source_result_annotation_mismatch_disclosed"] is True
    assert annotation["corrected_source_and_result_annotations_match"] is True
    assert annotation["deterministic_second_execution_bytes_matched"] is True
    assert vol["formula"]["per_asset_cap"] is None
    assert vol["formula"]["portfolio_target_volatility"] is None
    assert vol["formula"]["parameter_grid"] is False
    for period in ("D1", "D2"):
        for role in ("base", "stress_16bp"):
            values = vol["development"][period][role]
            assert values["gate_passed"] is True
            assert values["cagr"] > values["cash_cagr"]
            assert values["sharpe_minus_static"] >= 0.0
            assert values["max_drawdown_minus_static_positive_is_better"] >= 0.0
            assert values["positive_complete_year_ratio_minus_static"] >= 0.0
    assert vol["development"]["D1"]["base"]["cagr_minus_static"] < 0.0
    assert vol["target_diagnostics"]["signal_count"] == 94
    assert vol["target_diagnostics"]["adaptive_signal_count"] == 94
    assert vol["target_diagnostics"]["static_fallback_signal_count"] == 0
    assert eg["selected_for_formalization"] is False
    assert eg["canonical_result_payload_sha256"] == (
        "cf2b2481783c94e128ef4b197438fa84bf138394a55ae71fd382418ce8e46166"
    )
    assert eg["development_2017_2022"]["base_sharpe_minus_static"] < 0.0
    assert eg["development_2017_2022"]["stress_sharpe_minus_static"] < 0.0
    assert eg["stability_folds"]["folds_beating_cash"] == 1
    assert scout["selection_decision"]["selected_strategy_id"] == (
        "causal_monthly_volatility_balanced_budget"
    )
    assert scout["selection_decision"]["formal_candidate_count"] == 1
    assert scout["selection_decision"]["runner_up_fallback"] is False


def test_9_0_formula_has_one_fixed_causal_126_return_definition() -> None:
    protocol = _read(PROTOCOL_PATH)
    assert protocol["strategy_id"] == protocol["route"] == (
        "causal_monthly_volatility_balanced_budget"
    )
    assert protocol["protocol_id"] == (
        "factor-lab/9.0/causal-monthly-volatility-balanced-budget-v1"
    )
    registry = protocol["candidate_registry"]
    strategy = registry["strategy"]
    assert registry["candidate_count"] == registry["maximum_candidate_count"] == 1
    assert registry["ordered_strategy_ids"] == [protocol["strategy_id"]]
    assert strategy["volatility_return_count"] == 126
    assert strategy["required_observed_total_return_levels"] == 127
    assert strategy["volatility_floor"] == 1e-12
    assert strategy["normalization"] == BINARY64_NORMALIZATION
    assert strategy["raw_risk_target"] == (
        "frozen_base_budget_i / annualized_realized_volatility_i"
    )
    assert strategy["cash_target_weight"] == 0.0
    assert strategy["per_asset_cap"] is None
    assert strategy["portfolio_target_volatility"] is None
    assert strategy["leverage"] is False
    assert strategy["rebalance_band"] is None
    assert strategy["parameter_grid"] is False
    assert strategy["second_model"] is False
    assert strategy["runner_up_fallback"] is False


def test_9_0_development_uses_retained_source_and_direct_freeze() -> None:
    protocol = _read(PROTOCOL_PATH)
    formal = protocol["formal_phases"]
    physical = protocol["physical_phases"]
    for value in (formal["development"], physical["development"]):
        assert value["source_root"] == (
            "runtime/data/multi-asset-8.1/sources/stage=validation"
        )
        assert value["runtime_stage"] is None
        assert value["fresh_source_stage_created"] is False
        assert value["fresh_cash_each_development_subperiod"] is True
        assert value["development_subperiod_account_reuse"] is False
    assert protocol["development_disclosure"][
        "fresh_cash_each_development_subperiod"
    ] is True
    assert protocol["development_disclosure"][
        "development_subperiod_account_reuse"
    ] is False
    assert protocol["development_gate"][
        "fresh_cash_each_development_subperiod"
    ] is True
    assert protocol["development_gate"][
        "development_subperiod_account_reuse"
    ] is False
    assert formal["development"]["winner_freeze_is_created_directly_by_development_mode"] is True
    assert formal["development"]["winner_freeze_path"] == (
        "protocols/evidence/9.0/winner-freeze.json"
    )
    assert "candidate_freeze" not in formal
    assert physical["audit"]["market_outcome_opened"] is False
    assert physical["audit"]["performance_start"] == "2023-01-03"
    assert formal["audit"]["minimum_positive_complete_year_count"] == 2
    assert formal["audit"]["complete_year_count"] == 3


def test_9_0_gates_allow_static_cagr_sacrifice_but_not_cash_underperformance() -> None:
    protocol = _read(PROTOCOL_PATH)
    relative = protocol["relative_stability_gate"]
    assert relative == {
        "sharpe_delta_at_least": 0.0,
        "max_drawdown_delta_at_least": 0.0,
        "positive_complete_year_ratio_delta_at_least": 0.0,
    }
    development = protocol["development_gate"]
    assert development["relative_stability"]["candidate_minus_static_cagr_gate_exists"] is False
    assert development["absolute"][
        "candidate_cagr_strictly_above_matching_investable_cash_cagr"
    ] is True
    assert development["applies_to_each"] == [
        "D1.base",
        "D1.stress_16bp",
        "D2.base",
        "D2.stress_16bp",
    ]
    audit = protocol["audit_gate"]
    assert audit["minimum_positive_complete_year_count"] == 2
    assert audit["complete_year_count"] == 3
    assert audit["positive_complete_year_ratio_at_least"] == pytest.approx(2.0 / 3.0)
    assert audit["candidate_minus_static_cagr_gate_exists"] is False


def test_analyst_archive_is_parallel_data_engineering_not_a_9_0_candidate() -> None:
    scout = _read(SCOUT_PATH)["parallel_analyst_archive"]
    protocol = _read(PROTOCOL_PATH)["parallel_analyst_archive"]
    assert scout["belongs_to_release_9_0_candidate"] is False
    assert scout["workstream"] == "data_engineering_only"
    assert scout["returns_or_labels_may_be_joined"] is False
    assert protocol["release_9_0_candidate_input"] is False
    assert protocol["returns_or_labels_join_allowed"] is False
    assert protocol["may_create_a_9_0_runner_up"] is False
    assert protocol["future_route_requires_new_major_release"] is True


@pytest.mark.parametrize(
    ("document", "path", "replacement"),
    [
        ("scout", ("scope", "audit_2023_plus_read"), True),
        ("scout", ("selection_decision", "formal_candidate_count"), 2),
        (
            "scout",
            ("uniform_development_contract", "fresh_cash_each_development_subperiod"),
            False,
        ),
        (
            "scout",
            (
                "prototypes",
                "causal_three_expert_exponentiated_gradient_v0",
                "selected_for_formalization",
            ),
            True,
        ),
        ("protocol", ("candidate_registry", "candidate_count"), 2),
        ("protocol", ("candidate_registry", "strategy", "volatility_return_count"), 125),
        ("protocol", ("candidate_registry", "strategy", "volatility_floor"), 1e-8),
        (
            "protocol",
            ("development_disclosure", "fresh_cash_each_development_subperiod"),
            False,
        ),
        ("protocol", ("relative_stability_gate", "sharpe_delta_at_least"), -0.01),
        (
            "protocol",
            ("development_gate", "relative_stability", "candidate_minus_static_cagr_gate_exists"),
            True,
        ),
        ("protocol", ("physical_phases", "development", "runtime_stage"), "stage=development"),
        ("protocol", ("physical_phases", "audit", "market_outcome_opened"), True),
        ("protocol", ("selection_contract", "runner_up_fallback"), True),
        ("protocol", ("claim_contract", "profit_claim_allowed"), True),
    ],
)
def test_9_0_rehashed_relaxations_do_not_match_frozen_payload(
    document: str, path: tuple[Any, ...], replacement: Any
) -> None:
    value = copy.deepcopy(_read(SCOUT_PATH if document == "scout" else PROTOCOL_PATH))
    _set_nested(value, path, replacement)
    value["payload_sha256"] = canonical_payload_sha256(value)
    expected = SCOUT_PAYLOAD if document == "scout" else PROTOCOL_PAYLOAD
    assert value["payload_sha256"] != expected

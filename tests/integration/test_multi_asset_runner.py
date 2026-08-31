import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest


ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "scripts" / "run-multi-asset-evidence.py"


def _load_runner(name: str = "factor_lab_v90_runner_test"):
    spec = importlib.util.spec_from_file_location(name, RUNNER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _protocol() -> dict:
    return json.loads(
        (ROOT / "protocols/9.0-causal-volatility-balanced-budget.json").read_text(
            encoding="utf-8"
        )
    )


def _metric(
    *,
    cagr: float = 0.05,
    sharpe: float = 1.0,
    drawdown: float = -0.05,
    positive_ratio: float = 1.0,
    turnover: float = 0.5,
    fill: float = 1.0,
    capacity: float = 0.0,
    accounting: float = 1e-10,
) -> dict:
    return {
        "cagr": cagr,
        "sharpe": sharpe,
        "max_drawdown": drawdown,
        "positive_complete_year_ratio": positive_ratio,
        "positive_complete_year_count": 3,
        "complete_year_count": 3,
        "annualized_turnover": turnover,
        "requested_notional_fill_ratio": fill,
        "capacity_limited_requested_notional_ratio": capacity,
        "nav_reconciliation_error": accounting,
    }


def _reference(*, passed: bool = True) -> dict:
    phases = {
        name: {"passed": passed, "roles": {}}
        for name in ("D1", "D2")
    }
    return {
        "source_manifest_payload_sha256": "1" * 64,
        "stage_binding_payload_sha256": "2" * 64,
        "evaluation_payload_sha256": "3" * 64,
        "evaluation_file_sha256": "4" * 64,
        "metrics": {"prefix_replay_signal_count": 94, "phases": phases},
        "gate": {
            "passed": passed,
            "phase_passes": {"D1": passed, "D2": passed},
        },
    }


def _freeze(module, *, passed: bool = True, commit: str = "a" * 40) -> dict:
    value = {
        "schema_version": 1,
        "kind": "factor_lab_multi_asset_winner_freeze",
        "release": "9.0",
        "status": (
            "selected_policy_frozen"
            if passed
            else "selected_null_frozen_development_failed"
        ),
        "protocol_payload_sha256": "5" * 64,
        "implementation_closure_payload_sha256": "6" * 64,
        "development_execution_commit": commit,
        "run_nonce": "7" * 32,
        "candidate_registry": [module.PRIMARY_ID],
        "selected_candidate_id": module.PRIMARY_ID if passed else None,
        "development_source": {
            "source_manifest_payload_sha256": module.PRIOR_VALIDATION_MANIFEST_PAYLOAD,
            "tag_object": module.PRIOR_TAG_OBJECT,
            "tag_commit": module.PRIOR_COMMIT,
        },
        "development": _reference(passed=passed),
        "audit_market_outcomes_opened": False,
        "runner_up_fallback": False,
        "claim_contract": {"profit_claim_allowed": False},
    }
    value["payload_sha256"] = module.canonical_payload_sha256(value)
    return value


def test_runner_namespace_roles_stages_and_modes_are_exact(monkeypatch) -> None:
    module = _load_runner()
    assert module.RELEASE == "9.0"
    assert module.ROUTE == "causal_monthly_volatility_balanced_budget"
    assert module.PRIMARY_ID == "causal_monthly_volatility_balanced_budget"
    assert module.PROTOCOL_ID == (
        "factor-lab/9.0/causal-monthly-volatility-balanced-budget-v1"
    )
    assert module.PROTOCOL_PATH.as_posix() == (
        "protocols/9.0-causal-volatility-balanced-budget.json"
    )
    assert module.CLOSURE_PATH.as_posix() == "protocols/9.0-release.json"
    assert module.EVIDENCE_ROOT.as_posix() == "protocols/evidence/9.0"
    assert module.WORK_ROOT == ROOT / "runtime/data/multi-asset-9.0"
    assert set(module.STAGES) == {"development", "audit"}
    assert module.EVALUATION_ROLES == (
        "candidate",
        "candidate_stress",
        "static",
        "static_stress",
        "cash",
        "cash_stress",
    )

    called: list[str] = []
    monkeypatch.setattr(module, "run_development", lambda: called.append("development") or 0)
    monkeypatch.setattr(module, "run_audit", lambda: called.append("audit") or 0)
    monkeypatch.setattr(module, "run_finalize", lambda: called.append("finalize") or 0)
    for mode in ("development", "audit", "finalize"):
        assert module.main(["--mode", mode]) == 0
    assert called == ["development", "audit", "finalize"]


def test_protocol_hash_and_contract_are_exact() -> None:
    module = _load_runner("factor_lab_v90_protocol_test")
    protocol = _protocol()
    assert protocol["payload_sha256"] == module.PROTOCOL_PAYLOAD
    assert module.file_sha256(ROOT / module.PROTOCOL_PATH) == module.PROTOCOL_FILE_SHA256
    module._v9_verify_protocol_contract(protocol)


@pytest.mark.parametrize(
    ("attribute", "replacement"),
    [
        ("RISK_CODES", tuple(reversed(("510300.SH", "159920.SZ", "513100.SH", "518880.SH", "511010.SH")))),
        ("VOLATILITY_LEVEL_LOOKBACK", 128),
        ("INITIAL_CAPITAL_RMB", 2_000_000.0),
        ("BASE_COST_BPS_PER_SIDE", 9.0),
    ],
)
def test_protocol_rejects_kernel_constant_drift(
    monkeypatch, attribute: str, replacement: object
) -> None:
    module = _load_runner(f"factor_lab_v90_kernel_{attribute}")
    monkeypatch.setattr(module, attribute, replacement)
    with pytest.raises(ValueError, match="exact runner contract"):
        module._v9_verify_protocol_contract(_protocol())


def test_protocol_rejects_frozen_source_file_tamper(monkeypatch) -> None:
    module = _load_runner("factor_lab_v90_source_tamper")
    actual = module.file_sha256

    def file_hash(path: Path) -> str:
        if path.name == "8.0-static-capital-budget.json":
            return "0" * 64
        return actual(path)

    monkeypatch.setattr(module, "file_sha256", file_hash)
    with pytest.raises(ValueError, match="exact runner contract"):
        module._v9_verify_protocol_contract(_protocol())


def test_prior_archive_identity_excludes_runtime_verification_flags(monkeypatch) -> None:
    module = _load_runner("factor_lab_v90_archive_test")

    def git(*args: str, **_kwargs) -> bytes:
        if args[:2] == ("cat-file", "-t"):
            return b"tag\n"
        if args[:2] == ("rev-parse", "refs/tags/8.1"):
            return (module.PRIOR_TAG_OBJECT + "\n").encode()
        if args[:2] == ("rev-parse", "refs/tags/8.1^{}"):
            return (module.PRIOR_COMMIT + "\n").encode()
        raise AssertionError(args)

    monkeypatch.setattr(module, "_git", git)
    monkeypatch.setattr(
        module,
        "_v9_remote_prior_tag_refs",
        lambda: {
            "refs/tags/8.1": module.PRIOR_TAG_OBJECT,
            "refs/tags/8.1^{}": module.PRIOR_COMMIT,
        },
    )
    values = {
        module.PRIOR_PROTOCOL_PATH: {"release": "8.1"},
        module.PRIOR_CLOSURE_PATH: {"release": "8.1"},
        module.PRIOR_RECLASSIFICATION_PATH: {
            "status": "train_reclassification_passed"
        },
        module.PRIOR_FREEZE_PATH: {
            "status": "selected_null_frozen_validation_failed",
            "selected_candidate_id": None,
            "validation_market_outcomes_opened": True,
            "audit_market_outcomes_opened": False,
            "validation": {},
        },
        module.PRIOR_RESULT_PATH: {
            "status": "selection_falsified_no_candidate",
            "selected_candidate_id": None,
            "audit_status": "not_opened",
        },
    }
    monkeypatch.setattr(
        module, "_v9_tagged_json", lambda path, **_kwargs: values[path]
    )
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=0),
    )
    state = module._verify_prior_8_1_archive(
        verify_data=False, verify_runtime=False
    )
    identity = state["archive_identity_sha256"]
    changed = dict(state)
    changed["deep_data_verified"] = True
    changed["deep_runtime_verified"] = True
    immutable = {
        key: value
        for key, value in changed.items()
        if key
        not in {
            "deep_data_verified",
            "deep_runtime_verified",
            "archive_identity_sha256",
        }
    }
    assert identity == module.canonical_payload_sha256(immutable)


def test_absolute_and_relative_gate_allow_static_cagr_sacrifice() -> None:
    module = _load_runner("factor_lab_v90_gate_test")
    candidate = _metric(cagr=0.04, sharpe=1.2, drawdown=-0.04)
    static = _metric(cagr=0.07, sharpe=0.7, drawdown=-0.17, positive_ratio=0.75)
    cash = _metric(cagr=0.03, sharpe=9.0, drawdown=-0.002)
    operational = {
        "annualized_turnover": 0.7,
        "annualized_turnover_at_most": 1.0,
        "requested_notional_fill_ratio": 1.0,
        "requested_notional_fill_ratio_at_least": 0.99,
        "capacity_limited_requested_notional_ratio": 0.0,
        "capacity_limited_requested_notional_ratio_at_most": 0.01,
        "nav_reconciliation_error": 1e-10,
        "nav_reconciliation_error_at_most": 1e-8,
    }
    absolute = module._v9_absolute_gate(
        candidate,
        cash,
        thresholds=_protocol()["shared_absolute_gate"]["base"],
        operational=operational,
    )
    relative = module._v9_relative_gate(
        candidate, static, _protocol()["relative_stability_gate"]
    )
    assert absolute["passed"] is True
    assert relative["passed"] is True
    assert "cagr" not in " ".join(relative["checks"])


def test_cash_excess_is_strict_and_relative_drawdown_direction_is_positive() -> None:
    module = _load_runner("factor_lab_v90_cash_gate_test")
    candidate = _metric(cagr=0.03, drawdown=-0.05)
    cash = _metric(cagr=0.03)
    static = _metric(cagr=0.08, drawdown=-0.10)
    operational = {
        "annualized_turnover": 0.5,
        "annualized_turnover_at_most": 1.0,
        "requested_notional_fill_ratio": 1.0,
        "requested_notional_fill_ratio_at_least": 0.99,
        "capacity_limited_requested_notional_ratio": 0.0,
        "capacity_limited_requested_notional_ratio_at_most": 0.01,
        "nav_reconciliation_error": 0.0,
        "nav_reconciliation_error_at_most": 1e-8,
    }
    absolute = module._v9_absolute_gate(
        candidate,
        cash,
        thresholds=_protocol()["shared_absolute_gate"]["base"],
        operational=operational,
    )
    assert absolute["checks"]["cash_excess_cagr_strictly_positive"] is False
    relative = module._v9_relative_gate(
        candidate, static, _protocol()["relative_stability_gate"]
    )
    assert relative["values"]["max_drawdown_delta_at_least"]["metric"] == pytest.approx(0.05)


def test_development_requires_both_d1_and_d2(monkeypatch) -> None:
    module = _load_runner("factor_lab_v90_period_gate_test")
    passes = iter([True, False])
    monkeypatch.setattr(
        module,
        "_v9_phase_bundle",
        lambda *_args, **_kwargs: {"passed": next(passes)},
    )
    phases = module._v9_development_phases({"D1": {}, "D2": {}}, _protocol())
    assert phases == {"D1": {"passed": True}, "D2": {"passed": False}}


def test_hard_validity_covers_all_six_roles() -> None:
    module = _load_runner("factor_lab_v90_validity_test")
    valid = {
        "artifact_set_complete": True,
        "status_values_allowed": True,
        "status_execution_identity_exact": True,
        "blocked_missing_open_trade_count": 0,
        "blocked_capacity_trade_count": 0,
        "capacity_violation_count": 0,
        "negative_cash_observation_count": 0,
        "leverage_observation_count": 0,
        "minimum_cash": 1.0,
        "maximum_gross_exposure_ratio": 0.9,
        "maximum_nav_reconciliation_error": 1e-10,
        "requested_notional_total": 100.0,
        "executed_notional_total": 99.0,
        "capacity_limited_requested_notional": 0.0,
        "capacity_fields_finite_and_nonnegative": True,
        "executed_notional_not_above_requested": True,
        "capacity_limited_notional_not_above_requested": True,
        "capacity_aggregation_identity_exact": True,
        "requested_fill_identity_exact": True,
        "daily_trade_notional_identity_exact": True,
    }
    roles = {
        role: {"execution_validity": dict(valid), "nav_reconciliation_error": 1e-10}
        for role in module.EVALUATION_ROLES
    }
    module._v9_require_hard_validity(roles)
    roles["cash_stress"]["execution_validity"]["negative_cash_observation_count"] = 1
    with pytest.raises(RuntimeError, match="cash_stress execution validity"):
        module._v9_require_hard_validity(roles)
    roles["cash_stress"]["execution_validity"]["negative_cash_observation_count"] = 0
    roles["cash_stress"]["execution_validity"]["blocked_capacity_trade_count"] = True
    with pytest.raises(RuntimeError, match="cash_stress execution validity"):
        module._v9_require_hard_validity(roles)
    roles["cash_stress"]["execution_validity"]["blocked_capacity_trade_count"] = 0
    roles["cash_stress"]["execution_validity"]["executed_notional_total"] = 101.0
    with pytest.raises(RuntimeError, match="cash_stress execution validity"):
        module._v9_require_hard_validity(roles)
    roles["cash_stress"]["execution_validity"]["executed_notional_total"] = 99.0
    for field in (
        "maximum_gross_exposure_ratio",
        "maximum_nav_reconciliation_error",
    ):
        roles["cash_stress"]["execution_validity"][field] = -1e-10
        with pytest.raises(RuntimeError, match="cash_stress execution validity"):
            module._v9_require_hard_validity(roles)
        roles["cash_stress"]["execution_validity"][field] = (
            0.9 if field == "maximum_gross_exposure_ratio" else 1e-10
        )


def test_runtime_layout_development_has_no_copied_source(tmp_path, monkeypatch) -> None:
    module = _load_runner("factor_lab_v90_layout_test")
    work = tmp_path / "runtime/data/multi-asset-9.0"
    monkeypatch.setattr(module, "WORK_ROOT", work)
    monkeypatch.setattr(module, "SOURCE_ROOT", work / "sources")
    monkeypatch.setattr(module, "EVALUATION_ROOT", work / "evaluations")
    monkeypatch.setattr(module, "BINDING_ROOT", work / "stage-bindings")
    monkeypatch.setattr(module, "PRIOR_WORK_ROOT", tmp_path / "prior")
    (module.EVALUATION_ROOT / "stage=development").mkdir(parents=True)
    module.BINDING_ROOT.mkdir(parents=True)
    (module.BINDING_ROOT / "development.json").write_text("{}", encoding="utf-8")
    module._assert_runtime_layout({"development"})
    (module.SOURCE_ROOT / "stage=development").mkdir(parents=True)
    with pytest.raises(RuntimeError, match="unexpected or renamed"):
        module._assert_runtime_layout({"development"})


@pytest.mark.parametrize("passed", [True, False])
def test_development_uses_retained_source_and_creates_one_freeze(
    tmp_path, monkeypatch, passed: bool
) -> None:
    module = _load_runner(f"factor_lab_v90_development_{passed}")
    monkeypatch.setattr(module, "ROOT", tmp_path)
    monkeypatch.setattr(module, "WORK_ROOT", tmp_path / "runtime/data/multi-asset-9.0")
    monkeypatch.setattr(module, "SOURCE_ROOT", module.WORK_ROOT / "sources")
    monkeypatch.setattr(module, "EVALUATION_ROOT", module.WORK_ROOT / "evaluations")
    monkeypatch.setattr(module, "BINDING_ROOT", module.WORK_ROOT / "stage-bindings")
    monkeypatch.setattr(module, "PRIOR_SOURCE_ROOT", tmp_path / "prior/sources")
    monkeypatch.setattr(module, "_require_clean_main", lambda: "a" * 40)
    closure = {
        "payload_sha256": "6" * 64,
    }
    protocol = {"payload_sha256": "5" * 64, "claim_contract": {"profit_claim_allowed": False}}
    monkeypatch.setattr(module, "_v9_verify_closure", lambda **_kwargs: (closure, protocol))
    monkeypatch.setattr(module, "_require_head_pushed_and_ci_success", lambda _head: None)
    monkeypatch.setattr(
        module,
        "_verify_prior_8_1_archive",
        lambda **_kwargs: {
            "validation_manifest_payload_sha256": "1" * 64,
        },
    )
    stage = SimpleNamespace(
        manifest={"price_end_date": "2022-12-30", "payload_sha256": "1" * 64}
    )
    monkeypatch.setattr(module, "load_multi_asset_stage", lambda *_args: stage)
    predecessor = {
        "kind": "published_8_1_validation_source",
        "tag_object": module.PRIOR_TAG_OBJECT,
        "tag_commit": module.PRIOR_COMMIT,
    }
    monkeypatch.setattr(
        module,
        "_v9_create_development_binding",
        lambda *_args, **_kwargs: {"predecessor": predecessor},
    )
    monkeypatch.setattr(
        module,
        "_v9_evaluate_stage",
        lambda *_args, **_kwargs: _reference(passed=passed),
    )
    monkeypatch.setattr(module, "_assert_runtime_layout", lambda _stages: None)
    monkeypatch.setattr(module, "_assert_evidence_layout", lambda _names: None)
    monkeypatch.setattr(
        module,
        "_client",
        lambda: (_ for _ in ()).throw(AssertionError("development must not query provider")),
    )
    monkeypatch.setattr(
        module,
        "capture_multi_asset_stage",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("development must not capture a source")
        ),
    )

    def git(*args: str, **_kwargs) -> bytes:
        if args == ("rev-parse", "HEAD"):
            return b"a" * 40 + b"\n"
        if args == ("status", "--porcelain"):
            return b""
        raise AssertionError(args)

    monkeypatch.setattr(module, "_git", git)
    assert module.run_development() == 0
    freeze = json.loads((tmp_path / module.WINNER_FREEZE_PATH).read_text())
    assert freeze["selected_candidate_id"] == (module.PRIMARY_ID if passed else None)
    assert freeze["status"] == (
        "selected_policy_frozen"
        if passed
        else "selected_null_frozen_development_failed"
    )
    assert not (tmp_path / module.AUDIT_PATH).exists()


def test_audit_rejects_null_freeze_before_capture(tmp_path, monkeypatch) -> None:
    module = _load_runner("factor_lab_v90_null_audit")
    freeze = _freeze(module, passed=False)
    monkeypatch.setattr(module, "_require_clean_main", lambda: "b" * 40)
    monkeypatch.setattr(module, "_v9_verify_closure", lambda **_kwargs: ({}, {}))
    monkeypatch.setattr(module, "_read_json", lambda _path: freeze)
    monkeypatch.setattr(module, "_require_committed", lambda _path: b"freeze")
    monkeypatch.setattr(module, "_v9_verify_freeze", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        module,
        "_stage",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("null freeze must not open audit")
        ),
    )
    with pytest.raises(RuntimeError, match="non-null winner freeze"):
        module.run_audit()


def test_freeze_status_is_derived_from_both_development_gates(monkeypatch) -> None:
    module = _load_runner("factor_lab_v90_freeze_contract")
    closure = {"payload_sha256": "6" * 64}
    protocol = {"payload_sha256": "5" * 64, "claim_contract": {"profit_claim_allowed": False}}
    freeze = _freeze(module, passed=True)
    freeze["protocol_payload_sha256"] = "5" * 64
    freeze["implementation_closure_payload_sha256"] = "6" * 64
    freeze["claim_contract"] = protocol["claim_contract"]
    freeze["payload_sha256"] = module.canonical_payload_sha256(freeze)
    monkeypatch.setattr(module, "_v9_verify_development_reference", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(module, "_v9_verify_execution_lineage", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=0),
    )
    module._v9_verify_freeze(
        freeze, closure=closure, protocol=protocol, verify_data=False
    )
    forged = dict(freeze)
    forged["unexpected"] = True
    forged["payload_sha256"] = module.canonical_payload_sha256(forged)
    with pytest.raises(ValueError, match="winner freeze contract differs"):
        module._v9_verify_freeze(
            forged, closure=closure, protocol=protocol, verify_data=False
        )
    freeze["development"]["gate"]["phase_passes"]["D2"] = False
    freeze["development"]["gate"]["passed"] = False
    freeze["payload_sha256"] = module.canonical_payload_sha256(freeze)
    with pytest.raises(ValueError, match="selection differs"):
        module._v9_verify_freeze(
            freeze, closure=closure, protocol=protocol, verify_data=False
        )


def test_execution_lineage_rejects_missing_exact_predecessor(
    tmp_path, monkeypatch
) -> None:
    module = _load_runner("factor_lab_v90_lineage")
    monkeypatch.setattr(module, "ROOT", tmp_path)
    required = Path("protocols/9.0-release.json")
    target = tmp_path / required
    target.parent.mkdir(parents=True)
    target.write_bytes(b"working")

    def git(*args: str, **_kwargs) -> bytes:
        if args[:2] == ("rev-parse", "--verify"):
            return ("a" * 40 + "\n").encode()
        if args[0] == "show":
            return b"different"
        raise AssertionError(args)

    monkeypatch.setattr(module, "_git", git)
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=0),
    )
    with pytest.raises(ValueError, match="lacks exact predecessor"):
        module._v9_verify_execution_lineage(
            "a" * 40,
            evidence_path=module.WINNER_FREEZE_PATH,
            required_files=(required,),
        )


def test_audit_history_prefix_rejects_pre_2023_tamper(monkeypatch) -> None:
    module = _load_runner("factor_lab_v90_prefix_tamper")
    calendar = pd.DataFrame(
        {
            "trade_date": pd.to_datetime(["2022-12-29", "2022-12-30"]),
            "marker": [1, 2],
        }
    )
    asset = pd.DataFrame(
        {
            "trade_date": pd.to_datetime(["2022-12-29", "2022-12-30"]),
            "total_return_index": [1.0, 1.01],
        }
    )
    prior = SimpleNamespace(
        calendar=calendar,
        assets={"asset": asset},
        manifest={"payload_sha256": module.PRIOR_VALIDATION_MANIFEST_PAYLOAD},
    )
    tampered = asset.copy()
    tampered.loc[1, "total_return_index"] = 9.0
    audit = SimpleNamespace(
        calendar=calendar.copy(),
        assets={"asset": tampered},
        manifest={"payload_sha256": "a" * 64},
    )
    monkeypatch.setattr(module, "load_multi_asset_stage", lambda *_args: prior)
    with pytest.raises(ValueError, match="does not replay exactly"):
        module._v9_verify_audit_history_prefix(audit)


def test_development_causal_stage_is_invariant_to_future_tail_mutation() -> None:
    module = _load_runner("factor_lab_v90_future_tail")
    calendar = pd.DataFrame(
        {"trade_date": pd.to_datetime(["2019-12-30", "2019-12-31", "2020-01-02"])}
    )
    asset = pd.DataFrame(
        {
            "trade_date": pd.to_datetime(["2019-12-30", "2019-12-31", "2020-01-02"]),
            "total_return_index": [1.0, 1.01, 1.02],
        }
    )
    first = module.MultiAssetStage(
        path=Path("retained"),
        manifest={"price_end_date": "2022-12-30"},
        calendar=calendar,
        assets={"asset": asset},
    )
    changed = asset.copy()
    changed.loc[2, "total_return_index"] = 99.0
    second = module.MultiAssetStage(
        path=Path("retained"),
        manifest={"price_end_date": "2022-12-30"},
        calendar=calendar,
        assets={"asset": changed},
    )
    first_prefix = module._v9_causal_stage_through(first, end="2019-12-31")
    second_prefix = module._v9_causal_stage_through(second, end="2019-12-31")
    pd.testing.assert_frame_equal(
        first_prefix.calendar, second_prefix.calendar, check_exact=True
    )
    pd.testing.assert_frame_equal(
        first_prefix.assets["asset"], second_prefix.assets["asset"], check_exact=True
    )


def test_audit_shallow_rejects_forged_envelope_gate(monkeypatch) -> None:
    module = _load_runner("factor_lab_v90_audit_envelope")
    stored_phase = {"passed": True, "roles": {}}
    reference = {
        "source_manifest_payload_sha256": "1" * 64,
        "stage_binding_payload_sha256": "2" * 64,
        "evaluation_payload_sha256": "3" * 64,
        "evaluation_file_sha256": "4" * 64,
        "metrics": {"prefix_replay_signal_count": 1, "phases": {"audit": stored_phase}},
        "gate": {"passed": False, "phase_passes": {"audit": False}},
    }
    source_prefix = {
        "cutoff": "2022-12-30",
        "retained_source_manifest_payload_sha256": module.PRIOR_VALIDATION_MANIFEST_PAYLOAD,
        "audit_source_manifest_payload_sha256": "5" * 64,
        "prefix_content_sha256": {},
    }
    source_prefix["payload_sha256"] = module.canonical_payload_sha256(source_prefix)
    freeze = {"payload_sha256": "6" * 64}
    closure = {"payload_sha256": "7" * 64}
    protocol = {"payload_sha256": "8" * 64, "claim_contract": {"profit_claim_allowed": False}}
    audit = {
        "schema_version": 1,
        "kind": "factor_lab_multi_asset_historical_audit",
        "release": "9.0",
        "status": "historical_audit_failed",
        "selected_candidate_id": module.PRIMARY_ID,
        "winner_freeze_payload_sha256": freeze["payload_sha256"],
        "protocol_payload_sha256": protocol["payload_sha256"],
        "implementation_closure_payload_sha256": closure["payload_sha256"],
        "audit_execution_commit": "a" * 40,
        "run_nonce": "b" * 32,
        "audit": reference,
        "pre_2023_source_prefix": source_prefix,
        "runner_up_fallback": False,
        "claim_contract": protocol["claim_contract"],
    }
    audit["payload_sha256"] = module.canonical_payload_sha256(audit)
    monkeypatch.setattr(module, "_v9_verify_execution_lineage", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        module,
        "_v9_bundle_from_roles",
        lambda *_args, **_kwargs: stored_phase,
    )
    with pytest.raises(ValueError, match="historical audit contract differs"):
        module._v9_verify_audit(
            audit,
            freeze=freeze,
            closure=closure,
            protocol=protocol,
            verify_data=False,
        )
    source_prefix["prefix_content_sha256"] = {
        key: "9" * 64 for key in {"calendar", *module.ALL_CODES}
    }
    source_prefix["payload_sha256"] = module.canonical_payload_sha256(source_prefix)
    audit["pre_2023_source_prefix"] = source_prefix
    audit["payload_sha256"] = module.canonical_payload_sha256(audit)
    with pytest.raises(ValueError, match="historical audit contract differs"):
        module._v9_verify_audit(
            audit,
            freeze=freeze,
            closure=closure,
            protocol=protocol,
            verify_data=False,
        )
    source_prefix["audit_source_manifest_payload_sha256"] = "1" * 64
    source_prefix["payload_sha256"] = module.canonical_payload_sha256(source_prefix)
    audit["pre_2023_source_prefix"] = source_prefix
    audit["payload_sha256"] = module.canonical_payload_sha256(audit)
    forged = dict(audit)
    forged["unexpected"] = True
    forged["payload_sha256"] = module.canonical_payload_sha256(forged)
    with pytest.raises(ValueError, match="historical audit contract differs"):
        module._v9_verify_audit(
            forged,
            freeze=freeze,
            closure=closure,
            protocol=protocol,
            verify_data=False,
        )
    with pytest.raises(ValueError, match="envelope gate differs"):
        module._v9_verify_audit(
            audit,
            freeze=freeze,
            closure=closure,
            protocol=protocol,
            verify_data=False,
        )


def test_verify_release_state_closure_only_never_opens_audit(monkeypatch) -> None:
    module = _load_runner("factor_lab_v90_closure_state")
    closure = {"status": "implementation_frozen_before_formal_development_replay"}
    protocol = {"release": "9.0"}
    monkeypatch.setattr(module, "_v9_verify_closure", lambda **_kwargs: (closure, protocol))
    monkeypatch.setattr(module.Path, "is_file", lambda path: False)
    monkeypatch.setattr(module.Path, "exists", lambda path: False)
    monkeypatch.setattr(module, "_assert_evidence_layout", lambda _names: None)
    monkeypatch.setattr(module, "_assert_runtime_layout", lambda _stages: None)
    state = module.verify_release_state()
    assert state["status"] == closure["status"]
    assert state["freeze"] is None
    assert state["audit"] is None
    assert state["result"] is None


def test_closure_and_terminal_result_reject_extra_or_mutated_semantics(monkeypatch) -> None:
    module = _load_runner("factor_lab_v90_exact_evidence_shapes")
    claim = {"profit_claim_allowed": False}
    protocol = {"claim_contract": claim}
    closure = {
        "schema_version": 2,
        "kind": "factor_lab_release_closure",
        "release": module.RELEASE,
        "closure_role": "causal_volatility_balanced_preselection_root",
        "direction_change": True,
        "route": module.ROUTE,
        "status": "implementation_frozen_before_formal_development_replay",
        "development_outcomes_opened": True,
        "audit_market_outcomes_opened": False,
        "protocol": {
            "path": module.PROTOCOL_PATH.as_posix(),
            "file_sha256": module.PROTOCOL_FILE_SHA256,
            "payload_sha256": module.PROTOCOL_PAYLOAD,
            "protocol_id": module.PROTOCOL_ID,
        },
        "preprotocol_scout": {
            "path": module.SCOUT_PATH.as_posix(),
            "file_sha256": module.SCOUT_FILE_SHA256,
            "payload_sha256": module.SCOUT_PAYLOAD,
            "status": "selected_volatility_balanced_after_fully_exposed_development",
        },
        "prior_8_1_archive": {
            "tag_object": module.PRIOR_TAG_OBJECT,
            "tag_commit": module.PRIOR_COMMIT,
        },
        "implementation_commit": "a" * 40,
        "implementation_tree": "b" * 40,
        "implementation": {},
        "runtime": {},
        "formal_data": {},
        "claim_contract": claim,
    }
    closure["payload_sha256"] = module.canonical_payload_sha256(closure)
    monkeypatch.setattr(
        module,
        "_read_json",
        lambda path: closure if path == module.CLOSURE_PATH else protocol,
    )
    monkeypatch.setattr(module, "_v9_verify_protocol_contract", lambda _value: None)
    with pytest.raises(ValueError, match="preselection closure contract differs"):
        module._v9_verify_closure(verify_runtime=False)

    result = {key: None for key in module.RESULT_FIELDS}
    result["unexpected"] = True
    result["payload_sha256"] = module.canonical_payload_sha256(result)
    with pytest.raises(ValueError, match="terminal result contract differs"):
        module._v9_verify_result(
            result,
            freeze={},
            audit=None,
            closure={},
            protocol={},
        )


def test_finalize_selected_audit_happy_path_rechecks_commit_and_ci(
    tmp_path, monkeypatch
) -> None:
    module = _load_runner("factor_lab_v90_finalize_happy")
    monkeypatch.setattr(module, "ROOT", tmp_path)
    closure = {"payload_sha256": "1" * 64}
    claim = {"profit_claim_allowed": False}
    protocol = {"payload_sha256": "2" * 64, "claim_contract": claim}
    freeze = {
        "payload_sha256": "3" * 64,
        "selected_candidate_id": module.PRIMARY_ID,
    }
    audit = {
        "payload_sha256": "4" * 64,
        "status": "historical_audit_passed",
    }
    for path, value in (
        (module.WINNER_FREEZE_PATH, freeze),
        (module.AUDIT_PATH, audit),
    ):
        target = tmp_path / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(value), encoding="utf-8")
    monkeypatch.setattr(module, "_require_clean_main", lambda: "a" * 40)
    closure_calls: list[bool] = []

    def verify_closure(**_kwargs):
        closure_calls.append(True)
        return closure, protocol

    monkeypatch.setattr(module, "_v9_verify_closure", verify_closure)
    monkeypatch.setattr(
        module,
        "_read_json",
        lambda path: freeze if path == module.WINNER_FREEZE_PATH else audit,
    )
    committed_calls = []

    def require_committed(path):
        committed_calls.append(path)
        return b"committed"

    monkeypatch.setattr(module, "_require_committed", require_committed)
    monkeypatch.setattr(module, "_v9_verify_freeze", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(module, "_v9_verify_audit", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(module, "_assert_runtime_layout", lambda _stages: None)
    monkeypatch.setattr(module, "_assert_evidence_layout", lambda _names: None)
    ci_calls: list[str] = []
    monkeypatch.setattr(
        module,
        "_require_head_pushed_and_ci_success",
        lambda head: ci_calls.append(head),
    )

    def git(*args: str, **_kwargs) -> bytes:
        if args == ("rev-parse", "HEAD"):
            return b"a" * 40 + b"\n"
        if args == ("status", "--porcelain"):
            return b""
        raise AssertionError(args)

    monkeypatch.setattr(module, "_git", git)
    assert module.run_finalize() == 0
    result = json.loads((tmp_path / module.RESULT_PATH).read_text())
    assert result["status"] == (
        "historical_adaptive_beta_diagnostic_passed_fresh_evidence_required"
    )
    assert result["audit_status"] == "historical_audit_passed"
    assert ci_calls == ["a" * 40, "a" * 40]
    assert len(closure_calls) == 2
    assert committed_calls.count(module.WINNER_FREEZE_PATH) == 2
    assert committed_calls.count(module.AUDIT_PATH) == 2

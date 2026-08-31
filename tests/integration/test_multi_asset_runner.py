from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pandas as pd
import pytest


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "run-multi-asset-evidence.py"


def _load_runner() -> ModuleType:
    spec = importlib.util.spec_from_file_location("factor_lab_v81_runner", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _gate() -> dict:
    return {
        "base": {
            "net_cagr_strictly_positive": True,
            "net_sharpe_at_least": 0.3,
            "daily_max_drawdown_at_least": -0.25,
            "positive_complete_year_ratio_at_least": 0.5,
            "cash_excess_cagr_strictly_positive": True,
        },
        "stress_16bp": {
            "net_cagr_strictly_positive": True,
            "net_sharpe_at_least": 0.25,
            "daily_max_drawdown_at_least": -0.25,
            "positive_complete_year_ratio_at_least": 0.5,
            "cash_excess_cagr_strictly_positive": True,
        },
        "operational": {
            "annualized_turnover_at_most": 1.0,
            "requested_notional_fill_ratio_at_least": 0.99,
            "capacity_limited_requested_notional_ratio_at_most": 0.01,
            "nav_reconciliation_error_at_most": 1e-8,
        },
    }


def _validity_config() -> dict:
    return {
        "blocked_missing_open_trade_count_must_be_exact_nonnegative_integer": True,
        "blocked_missing_open_trade_count_is_hard_failure": False,
        "blocked_capacity_trade_count_must_be_exact_nonnegative_integer": True,
        "blocked_capacity_trade_count_is_hard_failure": False,
        "capacity_violation_count_at_most": 0,
        "capacity_violation_is_frozen_execution_contract_breach": True,
        "negative_cash_observation_count_at_most": 0,
        "leverage_observation_count_at_most": 0,
    }


def _role_validity(**updates: object) -> dict:
    value = {
        "artifact_set_complete": True,
        "status_values_allowed": True,
        "status_execution_identity_exact": True,
        "blocked_missing_open_trade_count": 0,
        "blocked_capacity_trade_count": 0,
        "capacity_violation_count": 0,
        "negative_cash_observation_count": 0,
        "leverage_observation_count": 0,
        "minimum_cash": 10.0,
        "maximum_gross_exposure_ratio": 0.9,
        "maximum_nav_reconciliation_error": 2e-10,
        "requested_notional_total": 100.0,
        "executed_notional_total": 100.0,
        "capacity_limited_requested_notional": 0.0,
        "capacity_fields_finite_and_nonnegative": True,
        "executed_notional_not_above_requested": True,
        "capacity_limited_notional_not_above_requested": True,
        "capacity_aggregation_identity_exact": True,
        "requested_fill_identity_exact": True,
        "daily_trade_notional_identity_exact": True,
    }
    value.update(updates)
    return value


def _execution_validity(module: ModuleType) -> dict:
    role_metrics = _role_gate_metrics()
    return {
        "source": "receipt_bound_8_0_train_artifacts",
        "receipt_train_phase_deep_verified": True,
        "artifact_parquet_count": 20,
        "artifact_row_count": 43222,
        "roles": {
            role: _role_validity(
                maximum_nav_reconciliation_error=role_metrics[role][
                    "nav_reconciliation_error"
                ]
            )
            for role in module.EVALUATION_ROLES
        },
    }


def _role_gate_metrics() -> dict:
    def role(cagr: float, turnover: float, fill: float, accounting: float) -> dict:
        return {
            "cagr": cagr,
            "sharpe": 0.7,
            "max_drawdown": -0.15,
            "positive_complete_year_ratio": 0.75,
            "annualized_turnover": turnover,
            "requested_notional_fill_ratio": fill,
            "capacity_limited_requested_notional_ratio": 0.0,
            "nav_reconciliation_error": accounting,
        }

    return {
        "primary": role(0.07, 0.50, 0.996, 2e-10),
        "stress": role(0.068, 0.55, 0.995, 3e-10),
        "cash": role(0.03, 0.20, 0.89, 4e-10),
        "cash_stress": role(0.029, 0.21, 0.88, 1e-10),
    }


def _valid_artifact_result() -> dict:
    dates = pd.to_datetime(["2019-12-30", "2019-12-31"])
    return {
        "targets": pd.DataFrame({"x": [1]}),
        "orders": pd.DataFrame({"x": [1]}),
        "trades": pd.DataFrame(
            {
                "status": ["executed"],
                "requested_execution_notional": [10.0],
                "actual_executed_notional": [10.0],
                "capacity_limited_execution_notional": [0.0],
                "planned_signal_notional": [10.0],
                "capacity_rmb": [100.0],
            }
        ),
        "daily_nav": pd.DataFrame(
            {
                "trade_date": dates,
                "cash": [20.0, 15.0],
                "nav": [100.0, 101.0],
                "requested_notional": [10.0, 0.0],
                "executed_notional": [10.0, 0.0],
                "capacity_limited_requested_notional": [0.0, 0.0],
                "accounting_error": [0.0, 0.0],
            }
        ),
        "holdings": pd.DataFrame(
            {"trade_date": dates, "market_value": [80.0, 86.0]}
        ),
        "capacity": {
            "requested_notional_total": 10.0,
            "executed_notional_total": 10.0,
            "capacity_limited_requested_notional": 0.0,
            "capacity_limited_requested_notional_ratio": 0.0,
            "requested_notional_fill_ratio": 1.0,
            "capacity_violation_count": 0,
        },
    }


def _contracts(module: ModuleType) -> tuple[dict, dict, dict]:
    protocol = {
        "payload_sha256": "p" * 64,
        "shared_absolute_gate": _gate(),
        "execution_validity_hard_fail": _validity_config(),
        "claim_contract": {"profit_claim_allowed": False},
    }
    return (
        {"payload_sha256": "c" * 64},
        protocol,
        {"payload_sha256": "s" * 64},
    )


def _phase(module: ModuleType, stage: str, passed: bool = True) -> dict:
    spec = module.STAGES[stage]
    return {
        "source_manifest_payload_sha256": "a" * 64,
        "stage_binding_payload_sha256": "b" * 64,
        "evaluation_payload_sha256": "e" * 64,
        "evaluation_file_sha256": "f" * 64,
        "metrics": {
            "start_date": spec["performance_start"],
            "end_date": spec["performance_end"],
        },
        "gate": {"passed": passed},
    }


def _set_tmp_roots(module: ModuleType, tmp_path: Path, monkeypatch) -> None:
    work = tmp_path / "runtime" / "data" / "multi-asset-8.1"
    monkeypatch.setattr(module, "ROOT", tmp_path)
    monkeypatch.setattr(module, "WORK_ROOT", work)
    monkeypatch.setattr(module, "SOURCE_ROOT", work / "sources")
    monkeypatch.setattr(module, "EVALUATION_ROOT", work / "evaluations")
    monkeypatch.setattr(module, "BINDING_ROOT", work / "stage-bindings")


def _clean_git(module: ModuleType, monkeypatch, head: str) -> None:
    monkeypatch.setattr(module, "_require_clean_main", lambda: head)
    monkeypatch.setattr(module, "_require_head_pushed_and_ci_success", lambda _head: None)

    def git(*args: str, **_: object) -> bytes:
        if args == ("rev-parse", "HEAD"):
            return head.encode() + b"\n"
        if args == ("status", "--porcelain"):
            return b""
        raise AssertionError(args)

    monkeypatch.setattr(module, "_git", git)


def _reclassification(module: ModuleType, *, passed: bool, commit: str = "a" * 40) -> dict:
    value = {
        "reclassification_execution_commit": commit,
        "run_nonce": "1" * 32,
        "corrected_gate": {"passed": passed},
        "payload_sha256": "r" * 64,
    }
    return value


def test_runner_namespace_and_modes_are_exact(monkeypatch) -> None:
    module = _load_runner()
    assert module.RELEASE == "8.1"
    assert module.ROUTE == "policy_operational_metric_reclassification"
    assert module.PROTOCOL_ID.endswith("policy-operational-metric-reclassification-v1")
    assert module.TRAIN_RECLASSIFICATION_PATH.as_posix().endswith(
        "protocols/evidence/8.1/train-reclassification.json"
    )
    assert set(module.STAGES) == {"validation", "audit"}

    called: list[str] = []
    for mode in ("reclassify", "validation", "audit", "finalize"):
        monkeypatch.setattr(module, f"run_{mode}", lambda mode=mode: called.append(mode) or 0)
        assert module.main(["--mode", mode]) == 0
    assert called == ["reclassify", "validation", "audit", "finalize"]


def test_closure_prior_runtime_admission_is_deep_or_explicitly_shallow(
    monkeypatch,
) -> None:
    module = _load_runner()
    validity = _execution_validity(module)
    receipt = {"train_stage": {"role_gate_metrics": _role_gate_metrics()}}
    protocol = {"execution_validity_hard_fail": _validity_config()}
    binding = {
        "execution_validity_sha256": module.canonical_payload_sha256(validity),
        "artifact_parquet_count": 20,
        "artifact_row_count": 43222,
    }
    calls: list[object] = []
    monkeypatch.setattr(
        module,
        "_verify_prior_train_artifacts",
        lambda value: calls.append(value) or validity,
    )

    assert module._verify_prior_runtime_closure_admission(
        binding,
        prior_receipt=receipt,
        protocol=protocol,
        verify_runtime=True,
    ) == binding
    assert calls == [receipt]

    calls.clear()
    assert module._verify_prior_runtime_closure_admission(
        binding,
        prior_receipt=receipt,
        protocol=protocol,
        verify_runtime=False,
    ) == binding
    assert calls == []

    with pytest.raises(ValueError, match="prior-runtime admission differs"):
        module._verify_prior_runtime_closure_admission(
            {**binding, "execution_validity_sha256": "0" * 64},
            prior_receipt=receipt,
            protocol=protocol,
            verify_runtime=True,
        )


def test_shallow_reclassification_must_bind_closure_validity_hash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_runner()
    receipt_path = ROOT / module.PRIOR_RECEIPT_PATH
    receipt_bytes = receipt_path.read_bytes()
    receipt = json.loads(receipt_bytes.decode("utf-8"))
    role_metrics = receipt["train_stage"]["role_gate_metrics"]
    validity = _execution_validity(module)
    for role in module.EVALUATION_ROLES:
        validity["roles"][role]["maximum_nav_reconciliation_error"] = (
            role_metrics[role]["nav_reconciliation_error"]
        )
    metrics = module._combine_receipt_role_gate_metrics(
        role_metrics, execution_validity=validity
    )
    gate = module._evaluate_static_gate(metrics, _gate(), _validity_config())
    closure = {
        "payload_sha256": "c" * 64,
        "train_reclassification_source": {
            "execution_validity_sha256": module.canonical_payload_sha256(validity),
            "artifact_parquet_count": 20,
            "artifact_row_count": 43222,
        },
    }
    protocol = {
        "payload_sha256": "p" * 64,
        "shared_absolute_gate": _gate(),
        "execution_validity_hard_fail": _validity_config(),
        "train_reclassification_input": {"role_gate_metrics": role_metrics},
        "claim_contract": {"profit_claim_allowed": False},
    }
    selection = {"payload_sha256": "s" * 64}
    value = {
        "schema_version": 1,
        "kind": "factor_lab_policy_operational_train_reclassification",
        "release": "8.1",
        "status": "train_reclassification_passed",
        "protocol_payload_sha256": protocol["payload_sha256"],
        "asset_selection_payload_sha256": selection["payload_sha256"],
        "implementation_closure_payload_sha256": closure["payload_sha256"],
        "reclassification_execution_commit": "a" * 40,
        "run_nonce": "1" * 32,
        "source_receipt": {
            "path": module.PRIOR_RECEIPT_PATH.as_posix(),
            "file_sha256": module.hashlib.sha256(receipt_bytes).hexdigest(),
            "payload_sha256": receipt["payload_sha256"],
        },
        "role_gate_metrics": role_metrics,
        "metrics": metrics,
        "corrected_gate": gate,
        "execution_validity": validity,
        "post_hoc_non_independent": True,
        "new_market_data_queried": False,
        "retained_8_0_train_artifacts_accessed": True,
        "runtime_created": False,
        "validation_market_outcomes_opened": False,
        "audit_market_outcomes_opened": False,
        "claim_contract": protocol["claim_contract"],
    }
    value["payload_sha256"] = module.canonical_payload_sha256(value)
    monkeypatch.setattr(module, "_read_json", lambda *_args, **_kwargs: receipt)
    monkeypatch.setattr(module, "_require_committed", lambda _path: receipt_bytes)
    monkeypatch.setattr(module, "_verify_execution_lineage", lambda *a, **k: None)

    module._verify_train_reclassification_contract(
        value,
        closure=closure,
        protocol=protocol,
        selection=selection,
        verify_data=False,
    )
    forged = json.loads(json.dumps(value))
    forged["execution_validity"]["roles"]["primary"]["minimum_cash"] += 1.0
    forged["payload_sha256"] = module.canonical_payload_sha256(forged)

    with pytest.raises(ValueError, match="validity differs from the closure"):
        module._verify_train_reclassification_contract(
            forged,
            closure=closure,
            protocol=protocol,
            selection=selection,
            verify_data=False,
        )


def test_receipt_reclassification_excludes_cash_from_policy_operations() -> None:
    module = _load_runner()
    roles = _role_gate_metrics()
    combined = module._combine_receipt_role_gate_metrics(
        roles, execution_validity=_execution_validity(module)
    )
    assert combined["annualized_turnover"] == 0.55
    assert combined["minimum_requested_notional_fill_ratio"] == 0.995
    assert combined["maximum_capacity_limited_requested_notional_ratio"] == 0.0
    assert combined["maximum_nav_reconciliation_error"] == 4e-10
    assert combined["cash"]["requested_notional_fill_ratio"] == 0.89
    assert module._evaluate_static_gate(
        combined, _gate(), _validity_config()
    )["passed"] is True


@pytest.mark.parametrize(
    ("role", "field", "value"),
    [
        ("cash", "negative_cash_observation_count", 1),
        ("cash_stress", "leverage_observation_count", 1),
    ],
)
def test_every_role_execution_invalidity_is_hard_fail(
    role: str, field: str, value: int
) -> None:
    module = _load_runner()
    validity = _execution_validity(module)
    validity["roles"][role][field] = value
    metrics = module._combine_receipt_role_gate_metrics(
        _role_gate_metrics(), execution_validity=validity
    )
    with pytest.raises(RuntimeError, match=f"{role} execution validity hard fail"):
        module._evaluate_static_gate(metrics, _gate(), _validity_config())


def test_missing_open_and_capacity_counts_are_strictly_disclosed_not_new_gates() -> None:
    module = _load_runner()
    validity = _execution_validity(module)
    validity["roles"]["primary"]["blocked_missing_open_trade_count"] = 2
    validity["roles"]["cash"]["blocked_capacity_trade_count"] = 3
    metrics = module._combine_receipt_role_gate_metrics(
        _role_gate_metrics(), execution_validity=validity
    )

    assert module._evaluate_static_gate(
        metrics, _gate(), _validity_config()
    )["passed"] is True


@pytest.mark.parametrize("value", [True, 0.5, "0"])
def test_validity_counts_must_be_exact_nonboolean_integers(value: object) -> None:
    module = _load_runner()
    validity = _execution_validity(module)
    validity["roles"]["primary"]["blocked_missing_open_trade_count"] = value
    metrics = module._combine_receipt_role_gate_metrics(
        _role_gate_metrics(), execution_validity=validity
    )

    with pytest.raises(RuntimeError, match="primary execution validity hard fail"):
        module._evaluate_static_gate(metrics, _gate(), _validity_config())


def test_runtime_metric_aggregation_uses_policy_roles_and_four_role_accounting() -> None:
    module = _load_runner()

    def role(turnover: float, fill: float, capacity: float, accounting: float) -> dict:
        return {
            "observations": 3,
            "start_date": "2020-01-02",
            "performance_start": "2020-01-02",
            "baseline_date": "2019-12-31",
            "end_date": "2020-01-06",
            "start_nav": 100.0,
            "cagr": 0.07,
            "sharpe": 0.7,
            "max_drawdown": -0.1,
            "positive_complete_year_ratio": 1.0,
            "annualized_turnover": turnover,
            "requested_notional_fill_ratio": fill,
            "capacity_limited_requested_notional_ratio": capacity,
            "nav_reconciliation_error": accounting,
            "execution_validity": _role_validity(),
        }

    combined = module._combine_static_metrics(
        role(0.4, 0.997, 0.002, 1e-10),
        role(0.6, 0.994, 0.004, 2e-10),
        role(0.9, 0.8, 0.2, 7e-10),
        role(0.8, 0.7, 0.3, 5e-10),
    )
    assert combined["annualized_turnover"] == 0.6
    assert combined["minimum_requested_notional_fill_ratio"] == 0.994
    assert combined["maximum_capacity_limited_requested_notional_ratio"] == 0.004
    assert combined["maximum_nav_reconciliation_error"] == 7e-10


def test_role_validity_is_derived_from_complete_artifacts() -> None:
    module = _load_runner()
    dates = pd.to_datetime(["2020-01-02", "2020-01-03"])
    trades = pd.DataFrame(
        {
            "status": ["executed"],
            "requested_execution_notional": [10.0],
            "actual_executed_notional": [10.0],
            "capacity_limited_execution_notional": [0.0],
            "planned_signal_notional": [10.0],
            "capacity_rmb": [100.0],
        }
    )
    result = {
        "targets": pd.DataFrame({"x": [1]}),
        "orders": pd.DataFrame({"x": [1]}),
        "trades": trades,
        "daily_nav": pd.DataFrame(
            {
                "trade_date": dates,
                "cash": [20.0, 15.0],
                "nav": [100.0, 101.0],
                "requested_notional": [10.0, 0.0],
                "executed_notional": [10.0, 0.0],
                "capacity_limited_requested_notional": [0.0, 0.0],
                "accounting_error": [0.0, 0.0],
            }
        ),
        "holdings": pd.DataFrame(
            {"trade_date": dates, "market_value": [80.0, 86.0]}
        ),
        "capacity": {
            "requested_notional_total": 10.0,
            "executed_notional_total": 10.0,
            "capacity_limited_requested_notional": 0.0,
            "capacity_limited_requested_notional_ratio": 0.0,
            "requested_notional_fill_ratio": 1.0,
            "capacity_violation_count": 0,
        },
    }
    validity = module._role_execution_validity(result)
    assert validity["minimum_cash"] == 15.0
    assert validity["maximum_gross_exposure_ratio"] == pytest.approx(86.0 / 101.0)
    assert validity["capacity_aggregation_identity_exact"] is True

    result["trades"].loc[0, "status"] = "blocked_missing_open"
    result["trades"].loc[0, "actual_executed_notional"] = 0.0
    result["daily_nav"].loc[0, "executed_notional"] = 0.0
    result["capacity"]["executed_notional_total"] = 0.0
    result["capacity"]["requested_notional_fill_ratio"] = 0.0
    assert module._role_execution_validity(result)[
        "blocked_missing_open_trade_count"
    ] == 1


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_status",
        "unknown_status",
        "blocked_with_execution",
        "executed_above_requested",
        "capacity_violation",
        "accounting_nan",
        "accounting_metric_mismatch",
    ],
)
def test_role_validity_rejects_invalid_status_capacity_and_accounting(
    mutation: str,
) -> None:
    module = _load_runner()
    result = _valid_artifact_result()
    expected_metrics = {
        "requested_notional_fill_ratio": 1.0,
        "capacity_limited_requested_notional_ratio": 0.0,
        "nav_reconciliation_error": 0.0,
    }
    if mutation == "missing_status":
        result["trades"] = result["trades"].drop(columns="status")
    elif mutation == "unknown_status":
        result["trades"].loc[0, "status"] = "unknown_future_status"
    elif mutation == "blocked_with_execution":
        result["trades"].loc[0, "status"] = "blocked_cash"
    elif mutation == "executed_above_requested":
        result["trades"].loc[0, "actual_executed_notional"] = 11.0
        result["daily_nav"].loc[0, "executed_notional"] = 11.0
        result["capacity"]["executed_notional_total"] = 11.0
        result["capacity"]["requested_notional_fill_ratio"] = 1.1
        expected_metrics["requested_notional_fill_ratio"] = 1.1
    elif mutation == "capacity_violation":
        result["trades"].loc[0, "planned_signal_notional"] = 101.0
        result["capacity"]["capacity_violation_count"] = 1
    elif mutation == "accounting_nan":
        result["daily_nav"].loc[0, "accounting_error"] = float("nan")
    elif mutation == "accounting_metric_mismatch":
        expected_metrics["nav_reconciliation_error"] = 1e-9
    else:  # pragma: no cover - parametrization is exhaustive
        raise AssertionError(mutation)

    with pytest.raises(RuntimeError):
        module._role_execution_validity(
            result, expected_role_metrics=expected_metrics
        )


@pytest.mark.parametrize(
    ("mutation", "identity"),
    [
        ("capacity_fill", "requested_fill_identity_exact"),
        ("daily_totals", "daily_trade_notional_identity_exact"),
    ],
)
def test_fill_and_daily_trade_identities_are_hard_validity(
    mutation: str, identity: str
) -> None:
    module = _load_runner()
    result = _valid_artifact_result()
    if mutation == "capacity_fill":
        result["capacity"]["requested_notional_fill_ratio"] = 0.9
    else:
        result["daily_nav"].loc[0, "executed_notional"] = 9.0
    role_validity = module._role_execution_validity(result)
    assert role_validity[identity] is False
    execution_validity = {
        "source": "new_phase_artifacts",
        "roles": {
            role: dict(role_validity) for role in module.EVALUATION_ROLES
        },
    }
    role_metrics = {"nav_reconciliation_error": 0.0}
    metrics = {
        **role_metrics,
        "stress": role_metrics,
        "cash": role_metrics,
        "cash_stress": role_metrics,
        "execution_validity": execution_validity,
    }
    with pytest.raises(RuntimeError, match="primary execution validity hard fail"):
        module._require_execution_validity(metrics, _validity_config())


def test_hard_validity_rejects_negative_gross_and_accounting_mismatch() -> None:
    module = _load_runner()
    validity = _execution_validity(module)
    validity["source"] = "new_phase_artifacts"
    validity.pop("receipt_train_phase_deep_verified")
    validity.pop("artifact_parquet_count")
    validity.pop("artifact_row_count")
    validity["roles"]["primary"]["maximum_gross_exposure_ratio"] = -0.1
    metrics = module._combine_receipt_role_gate_metrics(
        _role_gate_metrics(), execution_validity=validity
    )
    with pytest.raises(RuntimeError, match="primary execution validity hard fail"):
        module._require_execution_validity(metrics, _validity_config())

    validity["roles"]["primary"]["maximum_gross_exposure_ratio"] = 0.9
    validity["roles"]["primary"]["maximum_nav_reconciliation_error"] = 0.0
    with pytest.raises(RuntimeError, match="primary execution validity hard fail"):
        module._require_execution_validity(metrics, _validity_config())


def test_git_remote_transport_failure_uses_exact_github_api_and_same_push_ci(
    monkeypatch,
) -> None:
    module = _load_runner()
    head = "a" * 40
    monkeypatch.setattr(
        module,
        "_git",
        lambda *args, **_: (head + "\n").encode()
        if args == ("rev-parse", "--verify", "refs/remotes/origin/main")
        else (_ for _ in ()).throw(AssertionError(args)),
    )
    commands: list[list[str]] = []

    def run(command: list[str], **_: object) -> SimpleNamespace:
        commands.append(command)
        if command[:2] == ["git", "ls-remote"]:
            return SimpleNamespace(
                returncode=128,
                stdout=b"",
                stderr=b"fatal: unable to access GitHub: Empty reply from server",
            )
        if command[:3] == ["gh", "api", "repos/yxforever666gh/factor-lab/commits/main"]:
            return SimpleNamespace(returncode=0, stdout=(head + "\n").encode(), stderr=b"")
        if command[:3] == ["gh", "run", "list"]:
            payload = [
                {
                    "headSha": head,
                    "status": "completed",
                    "conclusion": "success",
                    "event": "push",
                }
            ]
            return SimpleNamespace(returncode=0, stdout=json.dumps(payload).encode(), stderr=b"")
        raise AssertionError(command)

    monkeypatch.setattr(module.subprocess, "run", run)
    module._require_head_pushed_and_ci_success(head)
    assert any(command[:2] == ["gh", "api"] for command in commands)
    run_command = next(
        command for command in commands if command[:3] == ["gh", "run", "list"]
    )
    assert run_command[3:5] == ["--repo", "yxforever666gh/factor-lab"]


def test_github_api_fallback_never_accepts_a_mismatched_sha(monkeypatch) -> None:
    module = _load_runner()
    head = "a" * 40
    monkeypatch.setattr(module, "_git", lambda *_args, **_kwargs: (head + "\n").encode())

    def run(command: list[str], **_: object) -> SimpleNamespace:
        if command[:2] == ["git", "ls-remote"]:
            return SimpleNamespace(
                returncode=128,
                stdout=b"",
                stderr=b"fatal: Could not resolve host: github.com",
            )
        if command[:2] == ["gh", "api"]:
            return SimpleNamespace(returncode=0, stdout=("b" * 40 + "\n").encode(), stderr=b"")
        raise AssertionError("CI must not be queried after remote identity mismatch")

    monkeypatch.setattr(module.subprocess, "run", run)
    with pytest.raises(RuntimeError, match="not the current pushed origin/main"):
        module._require_head_pushed_and_ci_success(head)


@pytest.mark.parametrize(
    ("returncode", "stdout", "stderr"),
    [
        (2, b"", b""),
        (0, b"", b""),
        (0, b"a" * 40 + b"\trefs/heads/wrong\n", b""),
        (0, b"a" * 40 + b"\trefs/heads/main\nextra\n", b""),
        (128, b"", b"fatal: authentication failed"),
    ],
)
def test_missing_malformed_or_mismatched_remote_main_never_uses_api_fallback(
    returncode: int, stdout: bytes, stderr: bytes, monkeypatch
) -> None:
    module = _load_runner()
    head = "a" * 40
    monkeypatch.setattr(module, "_git", lambda *_args, **_kwargs: (head + "\n").encode())
    commands: list[list[str]] = []

    def run(command: list[str], **_: object) -> SimpleNamespace:
        commands.append(command)
        if command[:2] == ["git", "ls-remote"]:
            return SimpleNamespace(
                returncode=returncode, stdout=stdout, stderr=stderr
            )
        raise AssertionError("API/CI must not run for a non-transport ref failure")

    monkeypatch.setattr(module.subprocess, "run", run)
    with pytest.raises(RuntimeError):
        module._require_head_pushed_and_ci_success(head)
    assert not any(command[:2] == ["gh", "api"] for command in commands)


def test_successful_ls_remote_requires_one_exact_main_ref(monkeypatch) -> None:
    module = _load_runner()
    head = "a" * 40
    monkeypatch.setattr(module, "_git", lambda *_args, **_kwargs: (head + "\n").encode())

    def run(command: list[str], **_: object) -> SimpleNamespace:
        if command[:2] == ["git", "ls-remote"]:
            return SimpleNamespace(
                returncode=0,
                stdout=f"{head}\trefs/heads/main\n".encode(),
                stderr=b"",
            )
        if command[:3] == ["gh", "run", "list"]:
            return SimpleNamespace(
                returncode=0,
                stdout=json.dumps(
                    [
                        {
                            "headSha": head,
                            "status": "completed",
                            "conclusion": "success",
                            "event": "push",
                        }
                    ]
                ).encode(),
                stderr=b"",
            )
        raise AssertionError(command)

    monkeypatch.setattr(module.subprocess, "run", run)
    module._require_head_pushed_and_ci_success(head)


def test_runtime_layout_rejects_any_train_stage(tmp_path, monkeypatch) -> None:
    module = _load_runner()
    _set_tmp_roots(module, tmp_path, monkeypatch)
    with pytest.raises(RuntimeError, match="only validation and audit"):
        module._assert_runtime_layout({"train"})
    module._assert_runtime_layout(set())


def test_reclassify_reads_retained_artifacts_but_never_runs_or_creates_train(
    tmp_path, monkeypatch
) -> None:
    module = _load_runner()
    _set_tmp_roots(module, tmp_path, monkeypatch)
    head = "a" * 40
    _clean_git(module, monkeypatch, head)
    closure, protocol, selection = _contracts(module)
    monkeypatch.setattr(module, "_verify_closure", lambda: (closure, protocol, selection))
    receipt = {
        "payload_sha256": "q" * 64,
        "train_stage": {"role_gate_metrics": _role_gate_metrics()},
    }
    receipt_bytes = b"published receipt\n"
    monkeypatch.setattr(module, "_read_json", lambda path, **_: receipt)
    monkeypatch.setattr(module, "_require_committed", lambda path: receipt_bytes)
    accessed: list[dict] = []

    def verify_prior(value: dict) -> dict:
        accessed.append(value)
        return _execution_validity(module)

    monkeypatch.setattr(module, "_verify_prior_train_artifacts", verify_prior)
    monkeypatch.setattr(
        module,
        "_evaluate_stage",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("reclassification must not execute a stage")
        ),
    )
    assert module.run_reclassify() == 0
    assert accessed == [receipt]
    assert not module.WORK_ROOT.exists()
    value = json.loads(
        (tmp_path / module.TRAIN_RECLASSIFICATION_PATH).read_text(encoding="utf-8")
    )
    assert value["status"] == "train_reclassification_passed"
    assert value["post_hoc_non_independent"] is True
    assert value["new_market_data_queried"] is False
    assert value["retained_8_0_train_artifacts_accessed"] is True
    assert value["runtime_created"] is False


def test_validation_waits_for_later_committed_reclassification_and_opens_only_validation(
    tmp_path, monkeypatch
) -> None:
    module = _load_runner()
    _set_tmp_roots(module, tmp_path, monkeypatch)
    head = "b" * 40
    _clean_git(module, monkeypatch, head)
    closure, protocol, selection = _contracts(module)
    monkeypatch.setattr(module, "_verify_closure", lambda: (closure, protocol, selection))
    reclassification = _reclassification(module, passed=True)
    monkeypatch.setattr(module, "_read_json", lambda path, **_: reclassification)
    monkeypatch.setattr(module, "_require_committed", lambda path: b"reclass\n")
    monkeypatch.setattr(module, "_verify_train_reclassification_contract", lambda *a, **k: None)
    layouts: list[set[str]] = []
    monkeypatch.setattr(module, "_assert_runtime_layout", lambda stages: layouts.append(set(stages)))
    monkeypatch.setattr(module, "_assert_evidence_layout", lambda _names: None)
    calls: list[tuple[str, dict]] = []

    def evaluate(stage: str, **kwargs: object) -> dict:
        calls.append((stage, kwargs))
        return _phase(module, stage, True)

    monkeypatch.setattr(module, "_evaluate_stage", evaluate)
    monkeypatch.setattr(module.uuid, "uuid4", lambda: SimpleNamespace(hex="2" * 32))
    assert module.run_validation() == 0
    assert [stage for stage, _ in calls] == ["validation"]
    assert calls[0][1]["predecessor"] == {
        "kind": "train_reclassification",
        "payload_sha256": reclassification["payload_sha256"],
    }
    assert set().issubset(layouts[0]) and {"validation"} in layouts
    freeze = json.loads((tmp_path / module.WINNER_FREEZE_PATH).read_text())
    assert freeze["selected_candidate_id"] == module.PRIMARY_ID
    assert "train_reclassification" in freeze
    assert "train" not in freeze


def test_validation_rejects_same_execution_commit_before_opening_market(
    tmp_path, monkeypatch
) -> None:
    module = _load_runner()
    _set_tmp_roots(module, tmp_path, monkeypatch)
    head = "a" * 40
    _clean_git(module, monkeypatch, head)
    closure, protocol, selection = _contracts(module)
    monkeypatch.setattr(module, "_verify_closure", lambda: (closure, protocol, selection))
    reclassification = _reclassification(module, passed=True, commit=head)
    monkeypatch.setattr(module, "_read_json", lambda path, **_: reclassification)
    monkeypatch.setattr(module, "_require_committed", lambda path: b"reclass\n")
    monkeypatch.setattr(module, "_verify_train_reclassification_contract", lambda *a, **k: None)
    monkeypatch.setattr(
        module,
        "_evaluate_stage",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must stay closed")),
    )
    with pytest.raises(RuntimeError, match="later commit containing reclassification"):
        module.run_validation()


def test_reclassification_only_release_state_uses_new_key(tmp_path, monkeypatch) -> None:
    module = _load_runner()
    _set_tmp_roots(module, tmp_path, monkeypatch)
    closure, protocol, selection = _contracts(module)
    monkeypatch.setattr(
        module,
        "_verify_closure",
        lambda **_kwargs: (closure, protocol, selection),
    )
    path = tmp_path / module.TRAIN_RECLASSIFICATION_PATH
    path.parent.mkdir(parents=True)
    path.write_text("{}\n", encoding="utf-8")
    reclassification = _reclassification(module, passed=True)
    monkeypatch.setattr(module, "_read_json", lambda _path, **_: reclassification)
    monkeypatch.setattr(module, "_require_committed", lambda _path: b"{}\n")
    monkeypatch.setattr(module, "_verify_train_reclassification_contract", lambda *a, **k: None)
    state = module.verify_release_state()
    assert state["status"] == "train_reclassification_passed_pending_validation"
    assert state["train_reclassification"] is reclassification
    assert set(state) == {
        "status",
        "closure",
        "protocol",
        "selection",
        "train_reclassification",
        "freeze",
        "audit",
        "result",
    }

from __future__ import annotations

import importlib.util
import copy
import json
from pathlib import Path
from types import ModuleType
from typing import Any

import pandas as pd
import pytest


ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "scripts" / "run-11.0-results-first.py"


def _load(name: str = "factor_lab_v110_results") -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, RUNNER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _validity(passed: bool = True) -> dict[str, Any]:
    return {
        "passed": passed,
        "artifact_set_complete": True,
        "status_values_allowed": True,
        "status_execution_identity_exact": True,
        "capacity_violation_count": 0,
        "negative_cash_observation_count": 0,
        "leverage_observation_count": 0,
        "maximum_gross_exposure_ratio": 0.9,
        "maximum_nav_reconciliation_error": 0.0,
        "capacity_aggregation_identity_exact": True,
        "daily_trade_notional_identity_exact": True,
    }


def _metric(
    cagr: float,
    *,
    years: int = 3,
    positive: float = 0.75,
    fill: float = 1.0,
    capacity: float = 0.0,
    valid: bool = True,
) -> dict[str, Any]:
    return {
        "cagr": cagr,
        "positive_complete_year_count": years,
        "positive_complete_year_ratio": positive,
        "requested_notional_fill_ratio": fill,
        "capacity_limited_requested_notional_ratio": capacity,
        "nav_reconciliation_error": 0.0,
        "sharpe": -99.0,
        "max_drawdown": -0.99,
        "annualized_turnover": 99.0,
        "execution_validity": _validity(valid),
    }


def _target(strategy_id: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "strategy_id": [strategy_id],
            "signal_date": [pd.Timestamp("2020-03-31")],
            "execution_date": [pd.Timestamp("2020-04-01")],
            "code": ["511880.SH"],
            "target_weight": [1.0],
            "signal_adv20_rmb": [1e9],
        }
    )


def test_namespace_protocol_and_hashes_are_exact() -> None:
    module = _load()
    assert module.RELEASE == "11.0"
    assert module.ROUTE == module.QUARTERLY_DUAL_CONFIRM_BLEND_ID
    assert module.PROTOCOL_ID == "factor-lab/11.0/results-first-dual-confirm-blend-v1"
    assert module.PROTOCOL_PATH.as_posix() == "protocols/11.0-results-first-dual-confirm-blend.json"
    assert module.EVIDENCE_PATH.as_posix() == "protocols/evidence/11.0/results-first-diagnostic.json"
    protocol = module._read_protocol()
    assert protocol["payload_sha256"] == module.PROTOCOL_PAYLOAD
    assert module.file_sha256(ROOT / module.PROTOCOL_PATH) == module.PROTOCOL_FILE_SHA256


def test_gate_is_return_first_and_uses_all_three_comparators() -> None:
    module = _load("factor_lab_v110_gate")
    v10 = _metric(0.10, years=3)
    static = _metric(0.08, years=2)
    cash = _metric(0.02, years=3)
    candidate = _metric(0.105, years=3)
    gate = module._gate(candidate, v10, static, cash)
    assert gate["passed"] is True
    assert not any(word in " ".join(gate["checks"]) for word in ("sharpe", "drawdown", "turnover"))
    candidate["cagr"] = 0.104999
    assert module._gate(candidate, v10, static, cash)["passed"] is False
    candidate = _metric(0.11, years=2)
    assert module._gate(candidate, v10, static, cash)["passed"] is False
    candidate = _metric(0.11, years=3, fill=0.979)
    assert module._gate(candidate, v10, static, cash)["passed"] is False


def test_stress_must_beat_published_10_base() -> None:
    module = _load("factor_lab_v110_stress")
    candidate = _metric(0.099, years=3)
    gate = module._gate(
        candidate,
        _metric(0.09, years=3),
        _metric(0.08, years=3),
        _metric(0.02, years=3),
        v10_base=_metric(0.10, years=3),
    )
    assert gate["checks"]["cagr_margin_over_best_comparator_at_least"] is True
    assert gate["checks"]["stress_cagr_strictly_above_v10_base"] is False
    assert gate["passed"] is False


def test_run_period_reuses_targets_and_gates_all_roles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load("factor_lab_v110_period")
    stage = type(
        "Stage",
        (),
        {
            "calendar": pd.DataFrame(
                {"trade_date": pd.to_datetime(["2020-03-31", "2020-04-01"])}
            ),
            "assets": {},
        },
    )()
    monkeypatch.setattr(module.V10, "_stage_through", lambda *_args: stage)
    monkeypatch.setattr(module, "build_monthly_targets", lambda _a, _s, strategy_id: _target(strategy_id))
    monkeypatch.setattr(module.V10, "_filter_targets", lambda value, **_kwargs: value)
    monkeypatch.setattr(module, "_prefix_replay_count", lambda *_args: 1)
    monkeypatch.setattr(
        module,
        "simulate_targets",
        lambda _a, targets, _s, config: {
            "targets": targets.copy(),
            "strategy": str(targets.iloc[0]["strategy_id"]),
            "cost": config.cost_bps_per_side,
        },
    )
    cagr = {
        module.ROUTE: 0.20,
        module.QUARTERLY_BORDA_ID: 0.10,
        module.CONTROL_ID: 0.08,
        module.CASH_ONLY_ID: 0.02,
    }

    def metric(result, **_kwargs):
        reduction = 0.01 if result["cost"] == 16.0 else 0.0
        return _metric(cagr[result["strategy"]] - reduction, years=3)

    monkeypatch.setattr(module.V10, "_metric_view", metric)
    period = module._run_period(stage, "D2", "2020-01-02", "2022-12-30")
    assert period["passed"] is True
    assert period["all_base_stress_targets_exact"] is True
    assert period["all_eight_roles_hard_valid"] is True
    assert period["candidate_target_prefix_mismatch_count"] == 0


def test_all_four_periods_are_hard_and_claim_is_non_oos(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load("factor_lab_v110_evidence")
    monkeypatch.setattr(
        module,
        "_run_period",
        lambda _stage, name, start, end: {
            "period": name,
            "start_date": start,
            "end_date": end,
            "passed": name != "full",
        },
    )
    monkeypatch.setattr(module, "file_sha256", lambda _path: module.PROTOCOL_FILE_SHA256)
    monkeypatch.setattr(
        module,
        "_scratch_replay_summary",
        lambda _periods, _protocol: {
            "matched": True,
            "projection_payload_sha256": "b" * 64,
            "minimum_cagr_edge": 0.01,
            "target_prefix_mismatch_count": 0,
            "all_return_and_execution_gates_passed": False,
        },
    )
    protocol = {"payload_sha256": module.PROTOCOL_PAYLOAD}
    evidence = module._build_evidence(object(), protocol, {"commit_bound": False})
    assert set(evidence["periods"]) == {"D1", "D2", "D3", "full"}
    assert evidence["selection"]["gate_passed"] is False
    assert evidence["selection"]["selected_candidate_id"] is None
    assert evidence["claim_contract"]["independent_oos"] is False
    assert evidence["claim_contract"]["profit_claim_allowed"] is False
    assert evidence["payload_sha256"] == module.canonical_payload_sha256(evidence)


def test_formal_scratch_metric_drift_nulls_selection_even_when_gates_pass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load("factor_lab_v110_scratch_drift")
    protocol = module._read_protocol()
    frozen = protocol["selected_scratch_evidence"]
    periods = {}
    for name, source in frozen["periods"].items():
        metrics = {}
        for role, source_role in (("candidate", source["base"]), ("candidate_stress", source["stress"])):
            value = _metric(source_role["cagr"], years=3)
            value.update(
                {
                    "sharpe": source_role["sharpe"],
                    "max_drawdown": source_role["max_drawdown"],
                    "annualized_turnover": source_role["annualized_turnover"],
                    "requested_notional_fill_ratio": source_role["fill_ratio"],
                    "capacity_limited_requested_notional_ratio": source_role["capacity_limited_ratio"],
                    "nav_reconciliation_error": source_role["max_abs_accounting_error"],
                }
            )
            value["execution_validity"]["maximum_nav_reconciliation_error"] = value["nav_reconciliation_error"]
            metrics[role] = value
        for key, role in (
            ("published_10_0_base_cagr", "v10"),
            ("published_10_0_stress_cagr", "v10_stress"),
            ("static_base_cagr", "static"),
            ("static_stress_cagr", "static_stress"),
            ("cash_base_cagr", "cash"),
            ("cash_stress_cagr", "cash_stress"),
        ):
            metrics[role] = _metric(source[key], years=2)
        periods[name] = {
            "period": name,
            "start_date": module.PERIODS[name][0],
            "end_date": module.PERIODS[name][1],
            "candidate_prefix_replay_signal_count": 1,
            "candidate_target_prefix_mismatch_count": 0,
            "metrics": metrics,
            "passed": True,
        }
    assert module._scratch_projection(periods) == frozen
    drifted = copy.deepcopy(periods)
    drifted["D1"]["metrics"]["candidate"]["cagr"] += 1e-6
    assert module._scratch_replay_summary(drifted, protocol)["matched"] is False
    monkeypatch.setattr(module, "_run_period", lambda _stage, name, *_args: drifted[name])
    evidence = module._build_evidence(object(), protocol, {"commit_bound": False})
    assert evidence["status"] == "formal_exact_replay_mismatch"
    assert evidence["selection"]["gate_passed"] is False
    assert evidence["selection"]["selected_candidate_id"] is None


def test_main_writes_create_only_custom_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load("factor_lab_v110_main")
    evidence = {
        "status": "candidate_failed_results_first_gates",
        "payload_sha256": "a" * 64,
    }
    monkeypatch.setattr(module.V10, "load_multi_asset_stage", lambda *_args: object())
    monkeypatch.setattr(module.V10, "_verify_source", lambda _stage: None)
    monkeypatch.setattr(module, "_read_protocol", lambda: {})
    monkeypatch.setattr(module, "_implementation_identity", lambda **_kwargs: {})
    monkeypatch.setattr(module, "_build_evidence", lambda *_args: evidence)
    output = tmp_path / "evidence.json"
    assert module.main(["--output", str(output)]) == 0
    assert json.loads(output.read_text(encoding="utf-8")) == evidence
    with pytest.raises(FileExistsError):
        module.main(["--output", str(output)])

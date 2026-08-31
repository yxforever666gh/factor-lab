import importlib.util
import json
from pathlib import Path
from types import ModuleType
from typing import Any

import pandas as pd
import pytest


ROOT = Path(__file__).resolve().parents[2]
RUNNER_PATH = ROOT / "scripts" / "run-10.0-results-first.py"


def _load_runner(name: str = "factor_lab_v100_results_first") -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, RUNNER_PATH)
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
        "requested_notional_total": 10.0,
        "executed_notional_total": 10.0,
        "capacity_limited_requested_notional": 0.0,
        "capacity_aggregation_identity_exact": True,
        "daily_trade_notional_identity_exact": True,
    }


def _metric(
    *,
    cagr: float,
    positive: float = 0.75,
    fill: float = 1.0,
    capacity: float = 0.0,
    accounting: float = 0.0,
    sharpe: float = -99.0,
    drawdown: float = -0.99,
    turnover: float = 99.0,
    valid: bool = True,
) -> dict[str, Any]:
    return {
        "cagr": cagr,
        "positive_complete_year_ratio": positive,
        "requested_notional_fill_ratio": fill,
        "capacity_limited_requested_notional_ratio": capacity,
        "nav_reconciliation_error": accounting,
        "sharpe": sharpe,
        "max_drawdown": drawdown,
        "annualized_turnover": turnover,
        "execution_validity": _validity(valid),
    }


def _protocol(module: ModuleType) -> dict[str, Any]:
    return json.loads((ROOT / module.PROTOCOL_PATH).read_text(encoding="utf-8"))


def _minimal_target(module: ModuleType, strategy_id: str) -> pd.DataFrame:
    signal = pd.Timestamp("2020-03-31")
    execution = pd.Timestamp("2020-04-01")
    return pd.DataFrame(
        {
            "strategy_id": [strategy_id],
            "signal_date": [signal],
            "execution_date": [execution],
            "code": ["511880.SH"],
            "target_weight": [1.0],
            "signal_adv20_rmb": [1e9],
        }
    )


def _valid_result(targets: pd.DataFrame) -> dict[str, Any]:
    dates = pd.to_datetime(["2020-03-31", "2020-04-01"])
    return {
        "targets": targets.copy(),
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
                "cash": [90.0, 90.0],
                "nav": [100.0, 101.0],
                "accounting_error": [0.0, 0.0],
                "requested_notional": [10.0, 0.0],
                "executed_notional": [10.0, 0.0],
                "capacity_limited_requested_notional": [0.0, 0.0],
            }
        ),
        "holdings": pd.DataFrame(
            {"trade_date": dates, "market_value": [10.0, 11.0]}
        ),
        "capacity": {
            "requested_notional_total": 10.0,
            "executed_notional_total": 10.0,
            "capacity_limited_requested_notional": 0.0,
            "capacity_violation_count": 0,
        },
    }


def test_runner_namespace_protocol_and_source_are_exact() -> None:
    module = _load_runner()
    assert module.RELEASE == "10.0"
    assert module.ROUTE == module.QUARTERLY_BORDA_ID == (
        "quarterly_12_1_dual_momentum_rank_budget"
    )
    assert module.PROTOCOL_ID == "factor-lab/10.0/results-first-quarterly-borda-v1"
    assert module.PROTOCOL_PAYLOAD == (
        "dc79550ee9fefe4fdb01f54fe0c299a40c2d118a687f6e5571156dff5701cb7b"
    )
    assert module.PROTOCOL_FILE_SHA256 == (
        "6a949ce4374f407a6084053a08b76dedbb3f1478fbe56edf233bb23befe730dd"
    )
    assert module.SOURCE_MANIFEST_PAYLOAD == (
        "050ad4ddcb86dc4fbc71befad54c400b48a44f72ab6fecc33936b6da0c8f9aff"
    )
    assert module.EVIDENCE_PATH.as_posix() == (
        "protocols/evidence/10.0/results-first-diagnostic.json"
    )
    assert module.RUNNER_PATH.as_posix() == "scripts/run-10.0-results-first.py"
    assert module.CORE_PATH.as_posix() == "src/factor_lab/research/multi_asset.py"
    protocol = module._read_protocol()
    assert protocol["frozen_strategy"]["strategy_id"] == module.QUARTERLY_BORDA_ID
    stage = module.load_multi_asset_stage(module.SOURCE_ROOT, module.SOURCE_STAGE)
    module._verify_source(stage)


@pytest.mark.parametrize(
    ("attribute", "replacement"),
    [
        ("BASE_COST_BPS", 7.0),
        ("MINIMUM_FILL_RATIO", 0.97),
        ("QUARTERLY_BORDA_START_LAG", 251),
    ],
)
def test_protocol_contract_rejects_executable_constant_drift(
    monkeypatch: pytest.MonkeyPatch, attribute: str, replacement: Any
) -> None:
    module = _load_runner(f"factor_lab_v100_contract_{attribute}")
    monkeypatch.setattr(module, attribute, replacement)
    with pytest.raises(ValueError, match="protocol identity differs"):
        module._read_protocol()


def test_results_first_gate_ignores_sharpe_drawdown_and_turnover() -> None:
    module = _load_runner("factor_lab_v100_gate")
    candidate = _metric(cagr=0.10, sharpe=-100.0, drawdown=-0.999, turnover=100.0)
    static = _metric(cagr=0.08)
    cash = _metric(cagr=0.02)
    gate = module._absolute_gate(candidate, static, cash)
    assert gate["passed"] is True
    assert not any(
        word in " ".join(gate["checks"])
        for word in ("sharpe", "drawdown", "turnover")
    )
    candidate["cagr"] = static["cagr"]
    assert module._absolute_gate(candidate, static, cash)["passed"] is False
    candidate["cagr"] = 0.10
    candidate["requested_notional_fill_ratio"] = 0.979
    assert module._absolute_gate(candidate, static, cash)["passed"] is False


def test_hard_execution_validity_rejects_tampering() -> None:
    module = _load_runner("factor_lab_v100_validity")
    target = _minimal_target(module, module.QUARTERLY_BORDA_ID)
    result = _valid_result(target)
    assert module._execution_validity(result)["passed"] is True
    result["daily_nav"].loc[0, "cash"] = -1.0
    validity = module._execution_validity(result)
    assert validity["passed"] is False
    assert validity["negative_cash_observation_count"] == 1
    result = _valid_result(target)
    result["trades"].loc[0, "status"] = "invented"
    with pytest.raises(RuntimeError, match="unknown trade status"):
        module._execution_validity(result)


def test_executed_status_means_all_capacity_capped_shares_filled() -> None:
    module = _load_runner("factor_lab_v100_capacity_status")
    target = _minimal_target(module, module.QUARTERLY_BORDA_ID)
    result = _valid_result(target)
    result["trades"].loc[0, "actual_executed_notional"] = 8.0
    result["trades"].loc[0, "capacity_limited_execution_notional"] = 2.0
    result["daily_nav"].loc[0, "executed_notional"] = 8.0
    result["daily_nav"].loc[0, "capacity_limited_requested_notional"] = 2.0
    result["capacity"]["executed_notional_total"] = 8.0
    result["capacity"]["capacity_limited_requested_notional"] = 2.0
    validity = module._execution_validity(result)
    assert validity["passed"] is True
    assert validity["status_execution_identity_exact"] is True


def test_stage_through_physically_removes_future_tail() -> None:
    module = _load_runner("factor_lab_v100_cutoff")
    dates = pd.to_datetime(["2019-12-30", "2019-12-31", "2020-01-02"])
    frame = pd.DataFrame({"trade_date": dates, "value": [1, 2, 999]})
    stage = module.MultiAssetStage(
        path=Path("stage"),
        manifest={"price_end_date": "2020-01-02"},
        calendar=frame.copy(),
        assets={"A": frame.copy()},
    )
    cutoff = module._stage_through(stage, "2019-12-31")
    assert cutoff.manifest["price_end_date"] == "2019-12-31"
    assert cutoff.calendar["trade_date"].max() == pd.Timestamp("2019-12-31")
    assert cutoff.assets["A"]["value"].tolist() == [1, 2]


def test_prefix_replay_uses_only_signal_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_runner("factor_lab_v100_prefix")
    signal = pd.Timestamp("2020-03-31")
    execution = pd.Timestamp("2020-04-01")
    target = _minimal_target(module, module.QUARTERLY_BORDA_ID)
    calendar = pd.DataFrame(
        {"trade_date": pd.to_datetime(["2020-03-30", signal, execution])}
    )
    asset = pd.DataFrame(
        {
            "trade_date": pd.to_datetime(["2020-03-30", signal, execution]),
            "future_marker": [0, 0, 999],
        }
    )
    stage = module.MultiAssetStage(
        path=Path("stage"), manifest={}, calendar=calendar, assets={"A": asset}
    )

    def build(market_data, official_sessions, strategy_id):
        assert strategy_id == module.QUARTERLY_BORDA_ID
        assert market_data["A"]["trade_date"].max() == signal
        assert max(official_sessions) == execution
        return target.copy()

    monkeypatch.setattr(module, "build_monthly_targets", build)
    assert module._prefix_replay_count(
        stage, target, tuple(calendar["trade_date"])
    ) == 1


def test_run_period_reuses_targets_and_gates_only_six_roles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_runner("factor_lab_v100_period")
    stage = object()
    monkeypatch.setattr(module, "_stage_through", lambda *_args, **_kwargs: stage)
    monkeypatch.setattr(
        module.pd,
        "to_datetime",
        pd.to_datetime,
    )
    # The fake stage needs only a calendar for the runner's session extraction.
    stage = type("Stage", (), {"calendar": pd.DataFrame({"trade_date": pd.to_datetime(["2020-03-31", "2020-04-01"])})})()

    def build(_assets, _sessions, strategy_id):
        return _minimal_target(module, strategy_id)

    stage.assets = {}
    monkeypatch.setattr(module, "build_monthly_targets", build)
    monkeypatch.setattr(module, "_filter_targets", lambda value, **_: value)
    monkeypatch.setattr(module, "_prefix_replay_count", lambda *_args: 1)

    def simulate_targets(_assets, targets, _sessions, config):
        return {"targets": targets.copy(), "cost": config.cost_bps_per_side}

    monkeypatch.setattr(module, "simulate_targets", simulate_targets)

    cagr = {
        module.QUARTERLY_BORDA_ID: 0.10,
        module.CONTROL_ID: 0.05,
        module.VOLATILITY_BALANCED_ID: 0.07,
        module.CASH_ONLY_ID: 0.02,
    }

    def metric(result, **_kwargs):
        strategy = result["targets"]["strategy_id"].iloc[0]
        value = cagr[strategy] - (0.005 if result["cost"] == 16.0 else 0.0)
        return _metric(cagr=value)

    monkeypatch.setattr(module, "_metric_view", metric)
    period = module._run_period(stage, "D2", "2020-01-02", "2022-12-30")
    assert period["passed"] is True
    assert period["candidate_base_stress_targets_exact"] is True
    assert period["all_six_gate_roles_hard_valid"] is True
    assert "v9" in period["metrics"] and "v9_stress" in period["metrics"]
    period["metrics"]["v9"]["execution_validity"]["passed"] = False
    assert period["all_six_gate_roles_hard_valid"] is True


def test_full_period_cannot_rescue_or_falsify_selection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_runner("factor_lab_v100_full_disclosure")
    protocol = _protocol(module)
    monkeypatch.setattr(module, "_git_head", lambda: "a" * 40)
    monkeypatch.setattr(module, "file_sha256", lambda _path: "b" * 64)

    def run(_stage, name, start, end):
        return {"period": name, "start_date": start, "end_date": end, "passed": name != "full"}

    monkeypatch.setattr(module, "_run_period", run)
    implementation = {
        "git_head": "a" * 40,
        "commit_bound": True,
        "files": {
            path.as_posix(): {"path": path.as_posix(), "file_sha256": "b" * 64}
            for path in (module.RUNNER_PATH, module.CORE_PATH, module.PROTOCOL_PATH)
        },
    }
    value = module._build_evidence(object(), protocol, implementation)
    assert value["selection"]["gate_passed"] is True
    assert value["selection"]["selected_candidate_id"] == module.QUARTERLY_BORDA_ID
    assert value["periods"]["full"]["passed"] is False
    assert value["implementation"] == implementation
    assert value["claim_contract"]["profit_claim_allowed"] is False
    assert value["claim_contract"]["stable_future_profit_claim_allowed"] is False
    assert value["payload_sha256"] == module.canonical_payload_sha256(value)


def test_main_writes_create_only_to_explicit_temp_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load_runner("factor_lab_v100_main")
    protocol = _protocol(module)
    stage = object()
    evidence = {
        "status": "candidate_failed_results_first_gates",
        "payload_sha256": "c" * 64,
    }
    monkeypatch.setattr(module, "load_multi_asset_stage", lambda *_args: stage)
    monkeypatch.setattr(module, "_verify_source", lambda _stage: None)
    monkeypatch.setattr(module, "_read_protocol", lambda: protocol)
    monkeypatch.setattr(module, "_build_evidence", lambda *_args: evidence)
    output = tmp_path / "results.json"
    assert module.main(["--output", str(output)]) == 0
    assert json.loads(output.read_text(encoding="utf-8")) == evidence
    with pytest.raises(FileExistsError):
        module.main(["--output", str(output)])


def test_custom_output_records_unbound_implementation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_runner("factor_lab_v100_unbound")
    monkeypatch.setattr(module, "_git_head", lambda: "a" * 40)
    value = module._implementation_identity(commit_bound=False)
    assert value["commit_bound"] is False
    assert value["git_head"] == "a" * 40
    assert set(value["files"]) == {
        module.RUNNER_PATH.as_posix(),
        module.CORE_PATH.as_posix(),
        module.PROTOCOL_PATH.as_posix(),
    }
    for relative, binding in value["files"].items():
        assert binding["path"] == relative
        assert len(binding["file_sha256"]) == 64


def test_default_formal_output_rejects_dirty_worktree(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_runner("factor_lab_v100_dirty")
    monkeypatch.setattr(module, "load_multi_asset_stage", lambda *_args: object())
    monkeypatch.setattr(module, "_verify_source", lambda _stage: None)
    monkeypatch.setattr(module, "_read_protocol", lambda: _protocol(module))
    monkeypatch.setattr(module, "_git_head", lambda: "a" * 40)

    def git(*args: str) -> bytes:
        if args == ("branch", "--show-current"):
            return b"main\n"
        if args == ("status", "--porcelain"):
            return b" M src/factor_lab/research/multi_asset.py\n"
        raise AssertionError(args)

    monkeypatch.setattr(module, "_git", git)
    monkeypatch.setattr(
        module,
        "_build_evidence",
        lambda *_args: pytest.fail("dirty formal run must stop before evaluation"),
    )
    with pytest.raises(RuntimeError, match="clean worktree"):
        module.main([])


def test_formal_identity_rejects_core_head_blob_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_runner("factor_lab_v100_core_mismatch")
    monkeypatch.setattr(module, "_git_head", lambda: "a" * 40)

    def git(*args: str) -> bytes:
        if args == ("branch", "--show-current"):
            return b"main\n"
        if args == ("status", "--porcelain"):
            return b""
        if len(args) == 2 and args[0] == "show":
            relative = Path(args[1].split(":", 1)[1])
            if relative == module.CORE_PATH:
                return b"not the working core blob"
            return (module.ROOT / relative).read_bytes()
        raise AssertionError(args)

    monkeypatch.setattr(module, "_git", git)
    with pytest.raises(RuntimeError, match="multi_asset.py"):
        module._implementation_identity(commit_bound=True)


def test_default_formal_evidence_is_not_created_by_tests() -> None:
    module = _load_runner("factor_lab_v100_absence")
    assert not (ROOT / module.EVIDENCE_PATH).exists()

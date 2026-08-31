from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType

import numpy as np
import pandas as pd
import pytest


ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "scripts" / "run-10.1-historical-dry-run.py"
ALL_CODES = (
    "510300.SH",
    "159920.SZ",
    "513100.SH",
    "518880.SH",
    "511010.SH",
    "511880.SH",
)


def _load(name: str = "factor_lab_v101_dry_run") -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, RUNNER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _stage(module: ModuleType, tmp_path: Path) -> object:
    dates = pd.bdate_range("2018-01-02", "2018-12-31")
    calendar_dates = dates.append(pd.DatetimeIndex([dates[-1] + pd.offsets.BDay()]))
    calendar = pd.DataFrame(
        {
            "trade_date": calendar_dates,
            "previous_open_date": pd.Series(calendar_dates).shift(1).to_numpy(),
        }
    )
    growth = {
        "510300.SH": 1.0008,
        "159920.SZ": 0.9998,
        "513100.SH": 1.0006,
        "518880.SH": 1.0004,
        "511010.SH": 1.0002,
        "511880.SH": 1.0001,
    }
    assets = {}
    index = np.arange(len(dates), dtype=float)
    for offset, code in enumerate(ALL_CODES):
        tri = np.power(growth[code], index)
        close = (10.0 + offset * 5.0) * tri
        assets[code] = pd.DataFrame(
            {
                "trade_date": dates,
                "open": close,
                "close": close,
                "dividend_cash": np.zeros(len(dates)),
                "dividend_pay_date": pd.Series(
                    [pd.NaT] * len(dates), dtype="datetime64[ns]"
                ),
                "total_return_index": tri,
                "adv20_rmb": np.full(len(dates), 100_000_000.0),
            }
        )
    return module.MultiAssetStage(
        path=tmp_path / "synthetic-stage",
        manifest={
            "stage": "synthetic",
            "price_start_date": dates[0].date().isoformat(),
            "price_end_date": dates[-1].date().isoformat(),
            "payload_sha256": "a" * 64,
        },
        calendar=calendar,
        assets=assets,
    )


def _protocol(module: ModuleType) -> dict:
    return module._read_protocol()


def test_historical_dry_run_uses_physical_prefixes_and_continuous_account(
    tmp_path: Path,
) -> None:
    module = _load()
    stage = _stage(module, tmp_path)
    protocol = _protocol(module)
    assert protocol["payload_sha256"] == module.PROTOCOL_PAYLOAD
    assert module.file_sha256(ROOT / module.PROTOCOL_PATH) == module.PROTOCOL_FILE_SHA256
    report = module._build_report(stage, protocol)
    rerun = module._build_report(stage, protocol)

    assert rerun == report
    assert report["payload_sha256"] == module.canonical_payload_sha256(report)
    assert report["kind"] == "factor_lab_10_1_historical_asof_dry_run"
    assert report["label"] == "historical_asof_dry_run"
    assert report["prospective"] is False
    assert report["strategy_id"] == module.QUARTERLY_BORDA_ID
    assert report["source"]["path"] == "external/synthetic-stage"
    assert len(report["implementation"]["git_head"]) == 40
    assert set(report["implementation"]["files"]) == {
            module.DRY_RUN_PATH.as_posix(),
            module.CYCLE_PATH.as_posix(),
            module.CORE_PATH.as_posix(),
            module.CAPTURE_PATH.as_posix(),
            module.CLI_PATH.as_posix(),
            module.PROTOCOL_PATH.as_posix(),
        }
    assert report["execution"] == {
        "cost_bps_per_side": 8.0,
        "continuous_account": True,
        "fresh_cash_reset_after_genesis": False,
        "physical_market_prefix_per_signal": True,
    }
    summary = report["summary"]
    assert summary["signal_count"] == 4
    assert summary["signal_count_strictly_positive"] is True
    assert summary["confirmed_outcome_count"] == 3
    assert summary["target_prefix_mismatch_count"] == 0
    assert summary["sealed_plan_prefix_mismatch_count"] == 0
    assert summary["signal_close_state_prefix_mismatch_count"] == 0
    assert summary["outcome_prefix_mismatch_count"] == 0
    assert summary["formal_path_write_count"] == 0
    assert [row["cumulative_target_signal_count"] for row in report["cycles"]] == [
        1,
        2,
        3,
        4,
    ]
    assert all(row["target_prefix_exact"] for row in report["cycles"])
    assert all(row["sealed_plan_prefix_exact"] for row in report["cycles"])
    assert all(
        row["outcome_prefix_exact"] is True for row in report["cycles"][:-1]
    )
    assert report["cycles"][-1]["outcome_prefix_exact"] is None
    assert report["cycles"][1]["signal_close_nav"] != 1_000_000.0
    assert not module.FORMAL_ROOT.exists()


def test_signal_prefix_seals_plan_without_next_open_market_row(tmp_path: Path) -> None:
    module = _load("factor_lab_v101_pending_plan")
    stage = _stage(module, tmp_path)
    sessions = tuple(module._dates(stage.calendar))
    targets = module.build_monthly_targets(
        stage.assets, sessions, module.QUARTERLY_BORDA_ID
    )
    signal = pd.Timestamp(targets["signal_date"].min()).normalize()
    execution = pd.Timestamp(
        targets.loc[targets["signal_date"].eq(signal), "execution_date"].iloc[0]
    ).normalize()
    prefix = module._stage_prefix(
        stage, market_end=signal, calendar_end=execution
    )
    assert all(module._dates(frame).max() == signal for frame in prefix.assets.values())
    assert module._dates(prefix.calendar).max() == execution
    prefix_targets = module.build_monthly_targets(
        prefix.assets, tuple(module._dates(prefix.calendar)), module.QUARTERLY_BORDA_ID
    )
    result = module.simulate_targets(
        prefix.assets,
        prefix_targets,
        tuple(module._dates(prefix.calendar)),
        module.SimulationConfig(cost_bps_per_side=8.0),
    )
    current = result["orders"].loc[result["orders"]["signal_date"].eq(signal)]
    assert not current.empty
    assert current["status"].eq("pending").all()
    assert current["execution_price"].isna().all()
    assert module._plan(module._orders_at(result, signal))


def test_official_unit_event_scales_shares_without_rewriting_rmb_seal(
    tmp_path: Path,
) -> None:
    module = _load("factor_lab_v101_unit_event")
    stage = _stage(module, tmp_path)
    for frame in stage.assets.values():
        frame["unit_multiplier"] = 1.0
    sessions = tuple(module._dates(stage.calendar))
    targets = module.build_monthly_targets(
        stage.assets, sessions, module.QUARTERLY_BORDA_ID
    )
    signal = pd.Timestamp(targets["signal_date"].min()).normalize()
    execution = pd.Timestamp(
        targets.loc[targets["signal_date"].eq(signal), "execution_date"].iloc[0]
    ).normalize()
    prefix = module._stage_prefix(stage, market_end=signal, calendar_end=execution)
    prefix_targets = module.build_monthly_targets(
        prefix.assets, tuple(module._dates(prefix.calendar)), module.QUARTERLY_BORDA_ID
    )
    sealed_result = module.simulate_targets(
        prefix.assets,
        prefix_targets,
        tuple(module._dates(prefix.calendar)),
        module.SimulationConfig(cost_bps_per_side=8.0),
    )
    sealed = module._orders_at(sealed_result, signal)
    assert not sealed.empty
    code = str(sealed.iloc[0]["code"])
    event = module._dates(stage.assets[code]).eq(execution)
    stage.assets[code].loc[event, "unit_multiplier"] = 2.0

    report = module._build_report(stage, _protocol(module))
    assert report["summary"]["sealed_plan_prefix_mismatch_count"] == 0
    full_targets = module.build_monthly_targets(
        stage.assets, sessions, module.QUARTERLY_BORDA_ID
    )
    full_result = module.simulate_targets(
        stage.assets,
        full_targets,
        sessions,
        module.SimulationConfig(cost_bps_per_side=8.0),
    )
    executed = module._orders_at(full_result, signal)
    assert module._execution_plan_matches_seal(executed, sealed)
    original = sealed.set_index("code").loc[code]
    changed = executed.set_index("code").loc[code]
    assert changed["planned_shares"] == original["planned_shares"] * 2
    for field in (
        "target_weight",
        "signal_price",
        "signal_adv20_rmb",
        "requested_signal_notional",
        "planned_signal_notional",
    ):
        assert changed[field] == original[field]


def test_outcome_uses_official_next_quarter_end_even_when_target_is_missing(
    tmp_path: Path,
) -> None:
    module = _load("factor_lab_v101_missing_next_target")
    stage = _stage(module, tmp_path)
    quarter_two = max(
        date
        for date in module._dates(stage.calendar)
        if date.year == 2018 and date.month <= 6
    )
    code = "159920.SZ"
    stage.assets[code] = stage.assets[code].loc[
        module._dates(stage.assets[code]).ne(quarter_two)
    ].reset_index(drop=True)
    report = module._build_report(stage, _protocol(module))

    signals = {row["signal_date"] for row in report["cycles"]}
    assert quarter_two.date().isoformat() not in signals
    first = report["cycles"][0]
    assert first["outcome_date"] == quarter_two.date().isoformat()
    assert first["outcome_prefix_exact"] is True
    assert report["summary"]["outcome_prefix_mismatch_count"] == 0


def test_adversarial_future_tail_cannot_change_first_signal_plan(
    tmp_path: Path,
) -> None:
    module = _load("factor_lab_v101_future_tail")
    stage = _stage(module, tmp_path)
    original = module._build_report(stage, _protocol(module))
    first_signal = pd.Timestamp(original["cycles"][0]["signal_date"])
    poisoned_assets = {code: frame.copy() for code, frame in stage.assets.items()}
    for frame in poisoned_assets.values():
        future = frame["trade_date"].gt(first_signal)
        frame.loc[future, ["open", "close", "total_return_index"]] *= 7.0
    poisoned = module.MultiAssetStage(
        path=stage.path,
        manifest=dict(stage.manifest),
        calendar=stage.calendar.copy(),
        assets=poisoned_assets,
    )
    rerun = module._build_report(poisoned, _protocol(module))
    assert rerun["summary"]["target_prefix_mismatch_count"] == 0
    assert rerun["summary"]["sealed_plan_prefix_mismatch_count"] == 0
    assert rerun["summary"]["outcome_prefix_mismatch_count"] == 0
    assert rerun["cycles"][0]["signal_close_nav"] == original["cycles"][0][
        "signal_close_nav"
    ]
    assert rerun["cycles"][0]["sealed_order_count"] == original["cycles"][0][
        "sealed_order_count"
    ]


def test_target_prefix_drift_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load("factor_lab_v101_target_drift")
    stage = _stage(module, tmp_path)
    original = module.build_monthly_targets
    full_length = max(len(frame) for frame in stage.assets.values())

    def drift(assets, sessions, strategy_id):
        value = original(assets, sessions, strategy_id)
        if max(len(frame) for frame in assets.values()) < full_length:
            value = value.copy()
            value.loc[value.index[0], "borda_points"] += 1
        return value

    monkeypatch.setattr(module, "build_monthly_targets", drift)
    with pytest.raises(ValueError, match="target prefix"):
        module._build_report(stage, _protocol(module))


def test_full_history_plan_or_outcome_drift_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load("factor_lab_v101_artifact_drift")
    stage = _stage(module, tmp_path)
    original = module.simulate_targets
    calls = 0

    def drift(*args, **kwargs):
        nonlocal calls
        value = original(*args, **kwargs)
        if calls == 0:
            value = dict(value)
            value["orders"] = value["orders"].copy()
            value["orders"].loc[value["orders"].index[0], "signal_price"] *= 2.0
        calls += 1
        return value

    monkeypatch.setattr(module, "simulate_targets", drift)
    with pytest.raises(ValueError, match="sealed plan prefix"):
        module._build_report(stage, _protocol(module))


def test_explicit_output_is_create_only_nonformal_and_never_prospective(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load("factor_lab_v101_output")
    stage = _stage(module, tmp_path)
    monkeypatch.setattr(module, "load_multi_asset_stage", lambda *_args: stage)
    formal = tmp_path / "formal"
    formal.mkdir()
    (formal / "sentinel.json").write_text('{"frozen":true}\n', encoding="utf-8")
    monkeypatch.setattr(module, "FORMAL_ROOT", formal)
    formal_before = module._tree_snapshot(formal)
    output = tmp_path / "dry-run.json"
    assert module.main(
        ["--source-root", str(tmp_path), "--stage", "synthetic", "--output", str(output)]
    ) == 0
    value = json.loads(output.read_text(encoding="utf-8"))
    expected_bytes = (
        json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    ).encode("utf-8")
    assert output.read_bytes() == expected_bytes
    assert value["payload_sha256"] == module.canonical_payload_sha256(value)
    assert module._tree_snapshot(formal) == formal_before
    assert value["prospective"] is False
    assert value["label"] == "historical_asof_dry_run"
    assert value["summary"]["formal_path_write_count"] == 0
    with pytest.raises(FileExistsError):
        module.main(
            [
                "--source-root",
                str(tmp_path),
                "--stage",
                "synthetic",
                "--output",
                str(output),
            ]
        )
    with pytest.raises(ValueError, match="cannot enter formal paths"):
        module._create_only(module.FORMAL_ROOT / "forbidden.json", value)

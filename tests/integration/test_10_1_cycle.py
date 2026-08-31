import importlib.util
import json
from datetime import datetime
from pathlib import Path
from types import ModuleType, SimpleNamespace

import numpy as np
import pandas as pd
import pytest


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "run-10.1-quarterly-cycle.py"


def _load(name: str = "factor_lab_v101_cycle") -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _frame(code: str, sessions: pd.DatetimeIndex, growth: float) -> pd.DataFrame:
    index = np.arange(len(sessions), dtype=float)
    close = 10.0 * np.power(1.0 + growth, index)
    return pd.DataFrame(
        {
            "trade_date": sessions,
            "open": close * 1.001,
            "close": close,
            "dividend_cash": np.zeros(len(sessions)),
            "dividend_pay_date": pd.NaT,
            "total_return_index": np.power(1.0 + growth, index),
            "adv20_rmb": np.full(len(sessions), 1e9),
        }
    )


def _stage(module: ModuleType, root: Path, end: str, *, drift: bool = False):
    sessions = pd.bdate_range("2024-01-01", "2026-04-02")
    cutoff = pd.Timestamp(end)
    codes = tuple(module.ALL_CODES)
    growth = dict(zip(codes, (0.0005, 0.0002, 0.0007, 0.0004, 0.0003, 0.0001)))
    assets = {
        code: _frame(code, sessions[sessions <= cutoff], growth[code])
        for code in codes
    }
    if drift:
        assets[codes[0]].loc[0, "close"] *= 2.0
    stage_path = root / f"stage=asof-{cutoff:%Y%m%d}"
    stage_path.mkdir(parents=True, exist_ok=True)
    (stage_path / "manifest.json").write_text("{}\n", encoding="utf-8")
    return module.MultiAssetStage(
        path=stage_path,
        manifest={
            "stage": f"asof-{cutoff:%Y%m%d}",
            "price_start_date": "2024-01-01",
            "price_end_date": cutoff.date().isoformat(),
            "payload_sha256": ("a" if end == "2025-12-31" else "b") * 64,
        },
        calendar=pd.DataFrame(
            {
                "trade_date": sessions,
                "previous_open_date": pd.Series(sessions).shift(1),
            }
        ),
        assets=assets,
    )


def _clock(module: ModuleType, date: str, hour: int, minute: int = 0):
    value = datetime.combine(pd.Timestamp(date).date(), datetime.min.time()).replace(
        hour=hour, minute=minute, tzinfo=module.SHANGHAI
    )
    return lambda: value


def _install_sources(monkeypatch, module: ModuleType, mapping: dict[str, object]) -> None:
    monkeypatch.setattr(
        module,
        "_require_release_tag",
        lambda: {
            "annotated_tag_object": "d" * 40,
            "peeled_commit": "e" * 40,
        },
    )
    monkeypatch.setattr(module, "_validate_source_receipt", lambda *_args: None)

    def load(root, stage):
        key = f"{Path(root).resolve()}::{stage}"
        return mapping[key]

    monkeypatch.setattr(module, "load_multi_asset_stage", load)


def test_signal_then_outcome_then_next_signal_is_continuous_and_create_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load()
    runtime = tmp_path / "prospective"
    source_root = runtime / "sources"
    first = _stage(module, source_root, "2025-12-31")
    second = _stage(module, source_root, "2026-03-31")
    mapping = {
        f"{source_root.resolve()}::asof-20251231": first,
        f"{source_root.resolve()}::asof-20260331": second,
    }
    _install_sources(monkeypatch, module, mapping)

    decision = module.create_signal(
        source_root,
        "asof-20251231",
        pd.Timestamp("2025-12-31"),
        runtime_root=runtime,
        clock=_clock(module, "2025-12-31", 18),
    )
    assert len(decision["targets"]) == 6
    assert decision["sealed_order_plan"]
    assert decision["signal_close_nav"] == 1_000_000.0
    decision_path = runtime / "cycle=2025Q4" / "decision.json"
    before = decision_path.read_bytes()
    with pytest.raises(FileExistsError, match="create-only"):
        module.create_signal(
            source_root,
            "asof-20251231",
            pd.Timestamp("2025-12-31"),
            runtime_root=runtime,
            clock=_clock(module, "2025-12-31", 18),
        )
    assert decision_path.read_bytes() == before

    outcome = module.create_outcome(
        source_root,
        "asof-20260331",
        pd.Timestamp("2025-12-31"),
        pd.Timestamp("2026-03-31"),
        runtime_root=runtime,
        clock=_clock(module, "2026-03-31", 18),
    )
    assert outcome["sealed_plan_exact"] is True
    assert outcome["end_nav"] != 1_000_000.0
    assert outcome["daily_nav"]
    assert outcome["maximum_reconciliation_error"] <= 1e-8
    outcome_path = runtime / "cycle=2025Q4" / "outcome.json"
    outcome_before = outcome_path.read_bytes()
    with pytest.raises(FileExistsError, match="create-only"):
        module.create_outcome(
            source_root,
            "asof-20260331",
            pd.Timestamp("2025-12-31"),
            pd.Timestamp("2026-03-31"),
            runtime_root=runtime,
            clock=_clock(module, "2026-03-31", 18),
        )
    assert outcome_path.read_bytes() == outcome_before

    next_decision = module.create_signal(
        source_root,
        "asof-20260331",
        pd.Timestamp("2026-03-31"),
        runtime_root=runtime,
        clock=_clock(module, "2026-03-31", 18),
    )
    assert next_decision["predecessor_outcome_payload_sha256"] == outcome["payload_sha256"]
    assert next_decision["signal_close_nav"] == outcome["end_nav"]


@pytest.mark.parametrize(
    ("when", "message"),
    [
        (("2025-12-31", 16, 59), "17:10"),
        (("2026-01-01", 9, 15), "missed"),
    ],
)
def test_signal_rejects_outside_the_frozen_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    when: tuple[str, int, int],
    message: str,
) -> None:
    module = _load(f"factor_lab_v101_time_{message}")
    runtime = tmp_path / "runtime"
    source_root = runtime / "sources"
    stage = _stage(module, source_root, "2025-12-31")
    _install_sources(
        monkeypatch,
        module,
        {f"{source_root.resolve()}::asof-20251231": stage},
    )
    with pytest.raises(RuntimeError, match=message):
        module.create_signal(
            source_root,
            "asof-20251231",
            pd.Timestamp("2025-12-31"),
            runtime_root=runtime,
            clock=_clock(module, *when),
        )


def test_signal_rechecks_deadline_immediately_before_atomic_seal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load("factor_lab_v101_signal_toctou")
    runtime = tmp_path / "runtime"
    source_root = runtime / "sources"
    stage = _stage(module, source_root, "2025-12-31")
    _install_sources(
        monkeypatch,
        module,
        {f"{source_root.resolve()}::asof-20251231": stage},
    )
    times = iter(
        [
            datetime(2025, 12, 31, 18, tzinfo=module.SHANGHAI),
            datetime(2025, 12, 31, 18, tzinfo=module.SHANGHAI),
            datetime(2026, 1, 1, 9, 15, tzinfo=module.SHANGHAI),
        ]
    )
    with pytest.raises(RuntimeError, match="missed"):
        module.create_signal(
            source_root,
            "asof-20251231",
            pd.Timestamp("2025-12-31"),
            runtime_root=runtime,
            clock=lambda: next(times),
        )
    assert not (runtime / "cycle=2025Q4" / "decision.json").exists()
    assert not list((runtime / "cycle=2025Q4").glob(".decision.json.partial-*"))


def test_signal_rejects_non_quarter_end_and_source_cutoff_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load("factor_lab_v101_bad_signal")
    runtime = tmp_path / "runtime"
    source_root = runtime / "sources"
    stage = _stage(module, source_root, "2025-12-30")
    _install_sources(
        monkeypatch,
        module,
        {f"{source_root.resolve()}::asof-20251230": stage},
    )
    with pytest.raises(ValueError, match="not the last official session"):
        module.create_signal(
            source_root,
            "asof-20251230",
            pd.Timestamp("2025-12-30"),
            runtime_root=runtime,
            clock=_clock(module, "2025-12-30", 18),
        )
    with pytest.raises(ValueError, match="stage name"):
        module.create_signal(
            source_root,
            "asof-20251230",
            pd.Timestamp("2025-12-31"),
            runtime_root=runtime,
            clock=_clock(module, "2025-12-31", 18),
        )


def test_outcome_rejects_wrong_quarter_and_rewritten_decision_prefix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load("factor_lab_v101_bad_outcome")
    runtime = tmp_path / "runtime"
    source_root = runtime / "sources"
    first = _stage(module, source_root, "2025-12-31")
    wrong = _stage(module, source_root, "2026-03-30")
    second = _stage(module, source_root, "2026-03-31", drift=True)
    mapping = {
        f"{source_root.resolve()}::asof-20251231": first,
        f"{source_root.resolve()}::asof-20260330": wrong,
        f"{source_root.resolve()}::asof-20260331": second,
    }
    _install_sources(monkeypatch, module, mapping)
    module.create_signal(
        source_root,
        "asof-20251231",
        pd.Timestamp("2025-12-31"),
        runtime_root=runtime,
        clock=_clock(module, "2025-12-31", 18),
    )
    with pytest.raises(ValueError, match="immediate next official quarter-end"):
        module.create_outcome(
            source_root,
            "asof-20260330",
            pd.Timestamp("2025-12-31"),
            pd.Timestamp("2026-03-30"),
            runtime_root=runtime,
            clock=_clock(module, "2026-03-31", 18),
        )
    with pytest.raises(ValueError, match="rewrote the decision-time prefix"):
        module.create_outcome(
            source_root,
            "asof-20260331",
            pd.Timestamp("2025-12-31"),
            pd.Timestamp("2026-03-31"),
            runtime_root=runtime,
            clock=_clock(module, "2026-03-31", 18),
        )


def test_artifact_payload_tamper_is_rejected(tmp_path: Path) -> None:
    module = _load("factor_lab_v101_tamper")
    path = tmp_path / "decision.json"
    path.write_text(
        json_text := '{"kind":"factor_lab_10_1_quarterly_decision","payload_sha256":"' + "0" * 64 + '"}',
        encoding="utf-8",
    )
    assert json_text
    with pytest.raises(ValueError, match="artifact differs"):
        module._read_artifact(path, kind="factor_lab_10_1_quarterly_decision")
    fake_outcome = module._payload(
        {"kind": "factor_lab_10_1_quarterly_outcome"}
    )
    outcome_path = tmp_path / "outcome.json"
    outcome_path.write_text(json.dumps(fake_outcome) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="artifact differs"):
        module._read_artifact(
            outcome_path, kind="factor_lab_10_1_quarterly_outcome"
        )


def test_outcome_rejects_same_quarter_alias_for_a_different_signal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load("factor_lab_v101_signal_alias")
    runtime = tmp_path / "runtime"
    source_root = runtime / "sources"
    first = _stage(module, source_root, "2025-12-31")
    second = _stage(module, source_root, "2026-03-31")
    _install_sources(
        monkeypatch,
        module,
        {
            f"{source_root.resolve()}::asof-20251231": first,
            f"{source_root.resolve()}::asof-20260331": second,
        },
    )
    module.create_signal(
        source_root,
        "asof-20251231",
        pd.Timestamp("2025-12-31"),
        runtime_root=runtime,
        clock=_clock(module, "2025-12-31", 18),
    )
    with pytest.raises(ValueError, match="does not match its sealed decision"):
        module.create_outcome(
            source_root,
            "asof-20260331",
            pd.Timestamp("2025-10-01"),
            pd.Timestamp("2026-03-31"),
            runtime_root=runtime,
            clock=_clock(module, "2026-03-31", 18),
        )


def test_outcome_rejects_missing_exact_next_open_and_writes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load("factor_lab_v101_missing_open")
    runtime = tmp_path / "runtime"
    source_root = runtime / "sources"
    first = _stage(module, source_root, "2025-12-31")
    second = _stage(module, source_root, "2026-03-31")
    _install_sources(
        monkeypatch,
        module,
        {
            f"{source_root.resolve()}::asof-20251231": first,
            f"{source_root.resolve()}::asof-20260331": second,
        },
    )
    decision = module.create_signal(
        source_root,
        "asof-20251231",
        pd.Timestamp("2025-12-31"),
        runtime_root=runtime,
        clock=_clock(module, "2025-12-31", 18),
    )
    code = decision["sealed_order_plan"][0]["code"]
    execution = pd.Timestamp(decision["execution_date"])
    row = pd.to_datetime(second.assets[code]["trade_date"]).dt.normalize().eq(execution)
    second.assets[code].loc[row, "open"] = np.nan
    with pytest.raises(RuntimeError, match="next-open execution evidence"):
        module.create_outcome(
            source_root,
            "asof-20260331",
            pd.Timestamp("2025-12-31"),
            pd.Timestamp("2026-03-31"),
            runtime_root=runtime,
            clock=_clock(module, "2026-03-31", 18),
        )
    assert not (runtime / "cycle=2025Q4" / "outcome.json").exists()


def test_outcome_rejects_replaced_bound_source_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load("factor_lab_v101_source_identity")
    runtime = tmp_path / "runtime"
    source_root = runtime / "sources"
    first = _stage(module, source_root, "2025-12-31")
    second = _stage(module, source_root, "2026-03-31")
    _install_sources(
        monkeypatch,
        module,
        {
            f"{source_root.resolve()}::asof-20251231": first,
            f"{source_root.resolve()}::asof-20260331": second,
        },
    )
    module.create_signal(
        source_root,
        "asof-20251231",
        pd.Timestamp("2025-12-31"),
        runtime_root=runtime,
        clock=_clock(module, "2025-12-31", 18),
    )
    (first.path / "manifest.json").write_text('{"replaced":true}\n', encoding="utf-8")
    with pytest.raises(ValueError, match="source identity"):
        module.create_outcome(
            source_root,
            "asof-20260331",
            pd.Timestamp("2025-12-31"),
            pd.Timestamp("2026-03-31"),
            runtime_root=runtime,
            clock=_clock(module, "2026-03-31", 18),
        )


def test_next_signal_rejects_self_hashed_fake_terminal_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load("factor_lab_v101_fake_terminal")
    runtime = tmp_path / "runtime"
    source_root = runtime / "sources"
    first = _stage(module, source_root, "2025-12-31")
    second = _stage(module, source_root, "2026-03-31")
    _install_sources(
        monkeypatch,
        module,
        {
            f"{source_root.resolve()}::asof-20251231": first,
            f"{source_root.resolve()}::asof-20260331": second,
        },
    )
    module.create_signal(
        source_root,
        "asof-20251231",
        pd.Timestamp("2025-12-31"),
        runtime_root=runtime,
        clock=_clock(module, "2025-12-31", 18),
    )
    module.create_outcome(
        source_root,
        "asof-20260331",
        pd.Timestamp("2025-12-31"),
        pd.Timestamp("2026-03-31"),
        runtime_root=runtime,
        clock=_clock(module, "2026-03-31", 18),
    )
    outcome_path = runtime / "cycle=2025Q4" / "outcome.json"
    fake = json.loads(outcome_path.read_text(encoding="utf-8"))
    fake["terminal_holdings"][0]["shares"] += 100
    module._payload(fake)
    outcome_path.write_text(
        json.dumps(fake, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="terminal account"):
        module.create_signal(
            source_root,
            "asof-20260331",
            pd.Timestamp("2026-03-31"),
            runtime_root=runtime,
            clock=_clock(module, "2026-03-31", 18),
        )


def test_exact_unit_event_scales_execution_plan_without_rewriting_the_seal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load("factor_lab_v101_unit_event")
    runtime = tmp_path / "runtime"
    source_root = runtime / "sources"
    first = _stage(module, source_root, "2025-12-31")
    second = _stage(module, source_root, "2026-03-31")
    for stage in (first, second):
        for frame in stage.assets.values():
            frame["unit_multiplier"] = 1.0
    _install_sources(
        monkeypatch,
        module,
        {
            f"{source_root.resolve()}::asof-20251231": first,
            f"{source_root.resolve()}::asof-20260331": second,
        },
    )
    decision = module.create_signal(
        source_root,
        "asof-20251231",
        pd.Timestamp("2025-12-31"),
        runtime_root=runtime,
        clock=_clock(module, "2025-12-31", 18),
    )
    sealed = json.loads(json.dumps(decision["sealed_order_plan"]))
    code = sealed[0]["code"]
    execution = pd.Timestamp(decision["execution_date"])
    event = pd.to_datetime(second.assets[code]["trade_date"]).dt.normalize().eq(execution)
    second.assets[code].loc[event, "unit_multiplier"] = 2.0
    outcome = module.create_outcome(
        source_root,
        "asof-20260331",
        pd.Timestamp("2025-12-31"),
        pd.Timestamp("2026-03-31"),
        runtime_root=runtime,
        clock=_clock(module, "2026-03-31", 18),
    )
    assert decision["sealed_order_plan"] == sealed
    trade = next(row for row in outcome["trades"] if row["code"] == code)
    original = next(row for row in sealed if row["code"] == code)
    assert trade["execution_unit_multiplier"] == 2.0
    assert trade["planned_shares"] == original["planned_shares"] * 2
    for field in (
        "target_weight",
        "signal_price",
        "signal_adv20_rmb",
        "requested_signal_notional",
        "planned_signal_notional",
    ):
        assert trade[field] == original[field]


def test_no_trade_next_signal_is_a_valid_empty_sealed_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load("factor_lab_v101_empty_plan")
    runtime = tmp_path / "runtime"
    source_root = runtime / "sources"
    first = _stage(module, source_root, "2025-12-31")
    second = _stage(module, source_root, "2026-03-31")
    for stage in (first, second):
        for frame in stage.assets.values():
            frame.loc[:, "open"] = 10.0
            frame.loc[:, "close"] = 10.0
            frame.loc[:, "total_return_index"] = 1.0
    _install_sources(
        monkeypatch,
        module,
        {
            f"{source_root.resolve()}::asof-20251231": first,
            f"{source_root.resolve()}::asof-20260331": second,
        },
    )
    module.create_signal(
        source_root,
        "asof-20251231",
        pd.Timestamp("2025-12-31"),
        runtime_root=runtime,
        clock=_clock(module, "2025-12-31", 18),
    )
    module.create_outcome(
        source_root,
        "asof-20260331",
        pd.Timestamp("2025-12-31"),
        pd.Timestamp("2026-03-31"),
        runtime_root=runtime,
        clock=_clock(module, "2026-03-31", 18),
    )
    next_decision = module.create_signal(
        source_root,
        "asof-20260331",
        pd.Timestamp("2026-03-31"),
        runtime_root=runtime,
        clock=_clock(module, "2026-03-31", 18),
    )
    assert next_decision["sealed_order_plan"] == []


def test_dividend_receivable_continues_exactly_into_the_next_signal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load("factor_lab_v101_receivable")
    runtime = tmp_path / "runtime"
    source_root = runtime / "sources"
    first = _stage(module, source_root, "2025-12-31")
    second = _stage(module, source_root, "2026-03-31")
    _install_sources(
        monkeypatch,
        module,
        {
            f"{source_root.resolve()}::asof-20251231": first,
            f"{source_root.resolve()}::asof-20260331": second,
        },
    )
    decision = module.create_signal(
        source_root,
        "asof-20251231",
        pd.Timestamp("2025-12-31"),
        runtime_root=runtime,
        clock=_clock(module, "2025-12-31", 18),
    )
    code = decision["sealed_order_plan"][0]["code"]
    terminal = pd.to_datetime(second.assets[code]["trade_date"]).dt.normalize().eq(
        pd.Timestamp("2026-03-31")
    )
    second.assets[code].loc[terminal, "dividend_cash"] = 0.01
    second.assets[code].loc[terminal, "dividend_pay_date"] = pd.Timestamp("2026-04-02")
    outcome = module.create_outcome(
        source_root,
        "asof-20260331",
        pd.Timestamp("2025-12-31"),
        pd.Timestamp("2026-03-31"),
        runtime_root=runtime,
        clock=_clock(module, "2026-03-31", 18),
    )
    assert outcome["daily_nav"][-1]["dividend_receivable"] > 0.0
    next_decision = module.create_signal(
        source_root,
        "asof-20260331",
        pd.Timestamp("2026-03-31"),
        runtime_root=runtime,
        clock=_clock(module, "2026-03-31", 18),
    )
    assert next_decision["signal_close_nav"] == outcome["end_nav"]
    assert next_decision["signal_close_cash"] == outcome["daily_nav"][-1]["cash"]
    assert next_decision["signal_close_holdings"] == outcome["terminal_holdings"]


def test_capture_command_uses_fixed_runtime_and_injected_client(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    module = _load("factor_lab_v101_capture")
    calls = []

    def capture(as_of, **kwargs):
        calls.append((as_of, kwargs))
        return SimpleNamespace(
            manifest={"stage": "asof-20251231", "payload_sha256": "a" * 64}
        )

    monkeypatch.setattr(module, "capture_source", capture)
    client = object()
    assert module.main(
        ["capture", "--as-of", "2025-12-31"],
        clock=_clock(module, "2025-12-31", 18),
        runtime_root=tmp_path,
        client_factory=lambda: client,
    ) == 0
    assert calls[0][0] == pd.Timestamp("2025-12-31")
    assert calls[0][1]["runtime_root"] == tmp_path.resolve()
    assert "stage=asof-20251231" in capsys.readouterr().out


def test_capture_rejects_before_close_without_touching_client(tmp_path: Path) -> None:
    module = _load("factor_lab_v101_capture_time")
    calls = []
    with pytest.raises(RuntimeError, match="17:10"):
        module.capture_source(
            pd.Timestamp("2025-12-31"),
            runtime_root=tmp_path,
            clock=_clock(module, "2025-12-31", 17, 9),
            client_factory=lambda: calls.append(True),
        )
    assert calls == []


def _install_release_git(
    monkeypatch: pytest.MonkeyPatch,
    module: ModuleType,
    *,
    dirty: bool = False,
    remote_tag: str | None = None,
    remote_peeled: str | None = None,
) -> tuple[str, str]:
    tag_object = "a" * 40
    peeled = "b" * 40

    def run(command, **_kwargs):
        args = tuple(command)
        if args[:3] == ("git", "cat-file", "-t"):
            return SimpleNamespace(returncode=0, stdout="tag\n", stderr="")
        if args[:3] == ("git", "merge-base", "--is-ancestor"):
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        if args == ("git", "rev-parse", "HEAD"):
            return SimpleNamespace(returncode=0, stdout=peeled + "\n", stderr="")
        if args == ("git", "rev-parse", "refs/tags/10.1^{}"):
            return SimpleNamespace(returncode=0, stdout=peeled + "\n", stderr="")
        if args == ("git", "rev-parse", "refs/tags/10.1"):
            return SimpleNamespace(returncode=0, stdout=tag_object + "\n", stderr="")
        if args == ("git", "status", "--porcelain"):
            return SimpleNamespace(
                returncode=0, stdout="?? untracked\n" if dirty else "", stderr=""
            )
        if args[:4] == ("git", "ls-remote", "--exit-code", "origin"):
            output = (
                f"{remote_tag or tag_object}\trefs/tags/10.1\n"
                f"{remote_peeled or peeled}\trefs/tags/10.1^{{}}\n"
            )
            return SimpleNamespace(returncode=0, stdout=output, stderr="")
        raise AssertionError(args)

    monkeypatch.setattr(module.subprocess, "run", run)
    return tag_object, peeled


def test_release_checkout_requires_exact_clean_local_and_remote_annotated_tag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load("factor_lab_v101_release_exact")
    tag_object, peeled = _install_release_git(monkeypatch, module)
    assert module._require_release_tag() == {
        "annotated_tag_object": tag_object,
        "peeled_commit": peeled,
    }


@pytest.mark.parametrize(
    "kwargs",
    [
        {"dirty": True},
        {"remote_tag": "c" * 40},
        {"remote_peeled": "c" * 40},
    ],
)
def test_release_checkout_fails_closed_on_dirty_or_remote_mismatch(
    monkeypatch: pytest.MonkeyPatch, kwargs: dict[str, object]
) -> None:
    module = _load(f"factor_lab_v101_release_bad_{len(kwargs)}_{next(iter(kwargs))}")
    _install_release_git(monkeypatch, module, **kwargs)
    with pytest.raises(RuntimeError, match="published annotated 10.1 tag"):
        module._require_release_tag()


def test_atomic_create_only_never_exposes_partial_or_overwrites_existing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load("factor_lab_v101_atomic")
    path = tmp_path / "cycle=2025Q4" / "decision.json"
    with monkeypatch.context() as scoped:
        scoped.setattr(module.os, "fsync", lambda _fd: (_ for _ in ()).throw(OSError("boom")))
        with pytest.raises(OSError, match="boom"):
            module._create_only(path, {"value": 1})
    assert not path.exists()
    assert not list(path.parent.glob(".decision.json.partial-*"))

    module._create_only(path, {"value": 1})
    before = path.read_bytes()
    with pytest.raises(FileExistsError):
        module._create_only(path, {"value": 2})
    assert path.read_bytes() == before
    assert not list(path.parent.glob(".decision.json.partial-*"))


def test_retained_2026q2_signal_matches_10_0_and_seals_pending_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load("factor_lab_v101_retained_regression")
    retained_root = ROOT / "runtime/data/multi-asset-9.0/sources"
    if not (retained_root / "stage=audit").is_dir():
        pytest.skip("retained 9.0 source is not present")
    retained = module.load_multi_asset_stage(retained_root, "audit")
    signal = pd.Timestamp("2026-06-30")
    assets = {
        code: frame.loc[
            pd.to_datetime(frame["trade_date"]).dt.normalize().le(signal)
        ].copy()
        for code, frame in retained.assets.items()
    }
    stage_path = tmp_path / "source/stage=asof-20260630"
    stage_path.mkdir(parents=True)
    (stage_path / "manifest.json").write_text("{}\n", encoding="utf-8")
    stage = module.MultiAssetStage(
        path=stage_path,
        manifest={
            **dict(retained.manifest),
            "stage": "asof-20260630",
            "price_end_date": "2026-06-30",
            "payload_sha256": "c" * 64,
        },
        calendar=retained.calendar.copy(),
        assets=assets,
    )
    monkeypatch.setattr(module, "_require_release_tag", lambda: None)
    monkeypatch.setattr(module, "_load_source", lambda *_args: stage)
    runtime = tmp_path / "runtime"
    decision = module.create_signal(
        runtime / "sources",
        "asof-20260630",
        signal,
        runtime_root=runtime,
        clock=_clock(module, "2026-06-30", 18),
    )
    sessions = module._sessions(retained)
    expected = module.build_monthly_targets(
        retained.assets, sessions, module.QUARTERLY_BORDA_ID
    ).loc[lambda frame: pd.to_datetime(frame["signal_date"]).dt.normalize().eq(signal)]
    actual = module._target_frame(decision["targets"])
    pd.testing.assert_frame_equal(
        actual.reset_index(drop=True), expected.reset_index(drop=True), check_exact=True
    )
    assert decision["sealed_order_plan"]

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pandas as pd
import pytest

from factor_lab.data.diemeng_minute_store import (
    MINUTE_AUCTION_COLUMNS,
    MINUTE_AUCTION_READ_COLUMNS,
    CandidateMinuteStore,
)
from factor_lab.release_integrity import canonical_payload_sha256, file_sha256
from factor_lab.research.pit_stock import PITStockContractError
from factor_lab.research.pit_stock_minute_execution import (
    MINUTE_EXECUTION_BAR_COLUMNS,
    MINUTE_EXECUTION_CONTEXT_COLUMNS,
)


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/capture-13.0-candidate-minutes.py"


def _module():
    spec = importlib.util.spec_from_file_location(
        "capture_13_candidate_minutes", SCRIPT
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _raw_rows():
    rows = []
    for minute in range(30, 48):
        time = f"09:{minute:02d}:00"
        price = 10.0 + (minute - 31) * 0.01
        rows.append(
            {
                "stock_code": "000001.SZ",
                "trade_time": f"2021-01-04 {time}",
                "open": price,
                "high": price + 0.1,
                "low": price - 0.1,
                "close": price,
                "vol": 100.0,
                "amount": price * 100.0 * 100.0,
            }
        )
    return rows


class _Diemeng:
    def __init__(self, *, empty=False, mismatch=False, drop_clocks=()):
        self.empty = empty
        self.mismatch = mismatch
        self.drop_clocks = set(drop_clocks)
        self.calls = []

    def query_history(self, **payload):
        self.calls.append(payload)
        rows = [] if self.empty else _raw_rows()
        rows = [
            value
            for value in rows
            if str(value["trade_time"])[-8:] not in self.drop_clocks
        ]
        capture_index = (len(self.calls) - 1) // 2
        if self.mismatch and capture_index == 1 and rows:
            rows = [dict(value) for value in rows]
            rows[0]["close"] = 10.05
        page_rows = rows if payload["page"] == 0 else []
        return {"code": 200, "data": {"total": len(rows), "list": page_rows}}


class _Limits:
    def __init__(self, *, missing=False, pre_close=10.0):
        self.calls = 0
        self.missing = missing
        self.pre_close = pre_close

    def query(self, endpoint, **kwargs):
        self.calls += 1
        assert endpoint == "stk_limit"
        rows = (
            []
            if self.missing
            else [
                {
                    "trade_date": "20210104",
                    "ts_code": "000001.SZ",
                    "pre_close": self.pre_close,
                    "up_limit": 11.0,
                    "down_limit": 9.0,
                }
            ]
        )
        return pd.DataFrame(
            rows,
            columns=(
                "trade_date",
                "ts_code",
                "pre_close",
                "up_limit",
                "down_limit",
            ),
        )


def _pair(*, mark_only: bool = False):
    return {
        "signal_date": "2020-12-31",
        "execution_date": "2021-01-04",
        "ticker": "000001.SZ",
        "in_previous_target": False,
        "in_current_target": True,
        "in_cumulative_target": True,
        "mark_only": mark_only,
    }


def _snapshot():
    return pd.DataFrame(
        [
            {
                "ticker": "000001.SZ",
                "adv20": 100_000_000.0,
                "vol63": 0.2,
                "mom12": 1.0,
                "mom6": 1.0,
                "industry": "bank",
                "size_bucket": "large",
                "universe_member": True,
            }
        ]
    )


def _plan(module, *, mark_only: bool = False):
    snapshot = _snapshot()
    value = {
        "schema_version": 1,
        "kind": "factor_lab_13_0_candidate_minute_plan",
        "release": "13.0",
        "signal_count": 1,
        "pair_count": 1,
        "unique_ticker_count": 1,
        "nonempty_execution_count": 1,
        "executions": [
            {
                "signal_date": "2020-12-31",
                "execution_date": "2021-01-04",
                "previous_target_count": 0,
                "current_target_count": 1,
                "ticker_count": 1,
                "tickers": ["000001.SZ"],
                "snapshot_payload_sha256": module.candidate_snapshot_payload(
                    snapshot
                ),
                "mark_only": mark_only,
            }
        ],
        "pairs": [_pair(mark_only=mark_only)],
        "pair_payload_sha256": "scope",
        "target_payload_sha256": module.TARGETS_PAYLOAD_SHA256,
        "panel_file_sha256": module.PANEL_FILE_SHA256,
        "panel_payload_sha256": module.PANEL_PAYLOAD_SHA256,
        "protocol_payload_sha256": module.PROTOCOL_PAYLOAD_SHA256,
    }
    value["payload_sha256"] = canonical_payload_sha256(value)
    return value


def test_pair_stage_resumes_and_tamper_fails(tmp_path: Path) -> None:
    module = _module()
    staging = tmp_path / "staging"
    client = _Diemeng()
    assert module._capture_pair(staging, _pair(), client=client, resume=True) is True
    calls = len(client.calls)
    assert module._capture_pair(staging, _pair(), client=client, resume=True) is False
    assert len(client.calls) == calls
    path = module._pair_dir(staging, _pair()) / "data.parquet"
    changed = pd.read_parquet(path)
    changed.loc[0, "close"] = 99.0
    changed.to_parquet(path, index=False)
    with pytest.raises(ValueError, match="differs"):
        module._capture_pair(staging, _pair(), client=client, resume=True)


def test_pair_stage_accepts_stable_empty_and_rejects_mismatch(tmp_path: Path) -> None:
    module = _module()
    empty_pair = dict(_pair(), ticker="000002.SZ")
    assert module._capture_pair(
        tmp_path / "empty", empty_pair, client=_Diemeng(empty=True), resume=True
    )
    assert pd.read_parquet(
        module._pair_dir(tmp_path / "empty", empty_pair) / "data.parquet"
    ).empty
    with pytest.raises(AssertionError):
        module._capture_pair(
            tmp_path / "bad", _pair(), client=_Diemeng(mismatch=True), resume=True
        )
    assert not module._pair_dir(tmp_path / "bad", _pair()).exists()


def test_pair_resume_rejects_a_self_consistent_prior_unit_contract(
    tmp_path: Path,
) -> None:
    module = _module()
    staging = tmp_path / "mixed"
    pair = _pair()
    assert module._capture_pair(
        staging, pair, client=_Diemeng(), resume=True
    )
    receipt_path = module._pair_dir(staging, pair) / "receipt.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    for key in ("first_capture", "second_capture"):
        receipt[key]["unit_contract_id"] = (
            "diemeng-a-share-per-slice-unique-vwap-unit-v2"
        )
    receipt["payload_sha256"] = canonical_payload_sha256(receipt)
    module._write_json(receipt_path, receipt)
    with pytest.raises(ValueError, match="capture contract"):
        module._capture_pair(staging, pair, client=_Diemeng(), resume=True)


def test_final_verifier_rejects_resigned_manifest_artifact_anchor(
    tmp_path: Path, monkeypatch
) -> None:
    module = _module()
    monkeypatch.setattr(module, "_derive_plan", lambda: _plan(module))
    monkeypatch.setattr(module, "_verify_calibration", lambda: None)
    output = tmp_path / "anchor-tamper"
    module.capture(
        output,
        resume=False,
        max_new_pairs=None,
        diemeng_client=_Diemeng(),
        limit_client=_Limits(),
    )
    path = output / "manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    first = next(
        value
        for value in manifest["artifacts"]
        if value["role"] == "minute_pair"
    )
    first["data_file_sha256"] = "0" * 64
    manifest["payload_sha256"] = canonical_payload_sha256(manifest)
    module._write_json(path, manifest)
    with pytest.raises(ValueError, match="pair anchor"):
        module.verify_capture(output)


def test_partial_capture_resumes_then_final_is_create_only(
    tmp_path: Path, monkeypatch
) -> None:
    module = _module()
    plan = _plan(module)
    monkeypatch.setattr(module, "_derive_plan", lambda: plan)
    monkeypatch.setattr(module, "_verify_calibration", lambda: None)
    output = tmp_path / "final"
    diemeng = _Diemeng()
    limits = _Limits()
    partial = module.capture(
        output,
        resume=False,
        max_new_pairs=1,
        diemeng_client=diemeng,
        limit_client=limits,
    )
    assert partial["status"] == "partial_staging"
    pair_calls = len(diemeng.calls)
    final = module.capture(
        output,
        resume=True,
        max_new_pairs=None,
        diemeng_client=diemeng,
        limit_client=limits,
    )
    assert final["status"] == "candidate_minutes_captured_return_unopened"
    assert len(diemeng.calls) == pair_calls
    assert module.verify_capture(output)["payload_sha256"] == final["payload_sha256"]
    manifest_file_sha256 = file_sha256(output / "manifest.json")
    with pytest.raises(PITStockContractError, match="external.*payload"):
        CandidateMinuteStore(output)
    with pytest.raises(PITStockContractError, match="external.*file"):
        CandidateMinuteStore(
            output,
            expected_manifest_payload_sha256=final["payload_sha256"],
        )
    store = CandidateMinuteStore(
        output,
        expected_manifest_payload_sha256=final["payload_sha256"],
        expected_manifest_file_sha256=manifest_file_sha256,
    )
    common = {
        "signal_date": "2020-12-31",
        "execution_date": "2021-01-04",
        "required_tickers": {"000001.SZ"},
    }
    slice_requests = []
    original_slice_reader = store._read_pair_slice

    def spy_slice(date, ticker, *, trade_times, columns):
        slice_requests.append(
            {
                "times": tuple(
                    pd.Timestamp(value).strftime("%H:%M:%S")
                    for value in trade_times
                ),
                "columns": tuple(columns),
            }
        )
        return original_slice_reader(
            date,
            ticker,
            trade_times=trade_times,
            columns=columns,
        )

    monkeypatch.setattr(store, "_read_pair_slice", spy_slice)
    context = store.build_context(
        **common,
        signal_snapshot=_snapshot(),
    )
    assert tuple(context.columns) == MINUTE_EXECUTION_CONTEXT_COLUMNS
    assert context.to_dict("records") == [
        {
            "ticker": "000001.SZ",
            "signal_adv20": 100_000_000.0,
            "signal_vol_daily": pytest.approx(0.2 / (252.0**0.5)),
            "up_limit": 11.0,
            "down_limit": 9.0,
        }
    ]
    assert slice_requests == []
    assert store._pair_artifact_cache == {}

    anchors, complete_no_anchor = store.build_auction_anchors(**common)
    assert tuple(anchors.columns) == MINUTE_AUCTION_COLUMNS
    assert complete_no_anchor == set()
    assert anchors.loc[0, "trade_time"] == pd.Timestamp("2021-01-04 09:30:00")
    assert anchors.loc[0, "observable_at"] == pd.Timestamp(
        "2021-01-04 09:31:00"
    )
    assert anchors.loc[0, "open"] == pytest.approx(9.99)
    assert not bool(anchors.loc[0, "zero_liquidity_flat_price"])
    assert slice_requests == [
        {
            "times": ("09:30:00",),
            "columns": MINUTE_AUCTION_READ_COLUMNS,
        }
    ]

    expected_vwap = {"A": 10.02, "B": 10.08, "C": 10.14}
    expected_clocks = {
        "A": tuple(f"09:{minute:02d}:00" for minute in range(31, 36)),
        "B": tuple(f"09:{minute:02d}:00" for minute in range(37, 42)),
        "C": tuple(f"09:{minute:02d}:00" for minute in range(43, 48)),
    }
    for window, expected in expected_vwap.items():
        bars, complete_no_bar = store.build_window(**common, window=window)
        assert tuple(bars.columns) == MINUTE_EXECUTION_BAR_COLUMNS
        assert complete_no_bar == set()
        assert bars.loc[0, "amount_rmb"] / bars.loc[0, "volume_shares"] == (
            pytest.approx(expected)
        )
        assert not any(
            str(column).startswith(("a_", "b_", "c_"))
            for column in bars.columns
        )
        assert slice_requests[-1] == {
            "times": expected_clocks[window],
            "columns": MINUTE_EXECUTION_BAR_COLUMNS,
        }
    with pytest.raises(PITStockContractError, match="must be A, B, or C"):
        store.build_window(**common, window="a")
    with pytest.raises(FileExistsError):
        module.capture(
            output,
            resume=True,
            max_new_pairs=None,
            diemeng_client=diemeng,
            limit_client=limits,
        )
    combined = b"".join(
        path.read_bytes() for path in output.rglob("*.*") if path.is_file()
    )
    assert b"fake-secret-marker" not in combined


def test_mark_only_sentinel_reads_anchor_but_no_context_or_windows(
    tmp_path: Path, monkeypatch
) -> None:
    module = _module()
    plan = _plan(module, mark_only=True)
    monkeypatch.setattr(module, "_derive_plan", lambda: plan)
    monkeypatch.setattr(module, "_verify_calibration", lambda: None)
    output = tmp_path / "sentinel"
    final = module.capture(
        output,
        resume=False,
        max_new_pairs=None,
        diemeng_client=_Diemeng(),
        limit_client=_Limits(missing=True),
    )
    store = CandidateMinuteStore(
        output,
        expected_manifest_payload_sha256=final["payload_sha256"],
        expected_manifest_file_sha256=file_sha256(output / "manifest.json"),
    )
    common = {
        "signal_date": "2020-12-31",
        "execution_date": "2021-01-04",
        "required_tickers": {"000001.SZ"},
    }
    requests = []
    original_slice_reader = store._read_pair_slice

    def sentinel_slice(date, ticker, *, trade_times, columns):
        requests.append(
            tuple(
                pd.Timestamp(value).strftime("%H:%M:%S")
                for value in trade_times
            )
        )
        return original_slice_reader(
            date,
            ticker,
            trade_times=trade_times,
            columns=columns,
        )

    monkeypatch.setattr(store, "_read_pair_slice", sentinel_slice)
    with pytest.raises(PITStockContractError, match="execution context"):
        store.build_context(**common, signal_snapshot=_snapshot())
    assert requests == []
    assert store._pair_artifact_cache == {}
    anchors, complete_no_anchor = store.build_auction_anchors(**common)
    assert tuple(anchors.columns) == MINUTE_AUCTION_COLUMNS
    assert complete_no_anchor == set()
    assert requests == [("09:30:00",)]
    with pytest.raises(PITStockContractError, match="mark-only sentinel"):
        store.build_window(**common, window="A")
    assert requests == [("09:30:00",)]


def test_missing_limits_empty_partitions_and_partial_windows_fail_correctly(
    tmp_path: Path, monkeypatch
) -> None:
    module = _module()
    monkeypatch.setattr(module, "_derive_plan", lambda: _plan(module))
    monkeypatch.setattr(module, "_verify_calibration", lambda: None)
    common = {
        "signal_date": "2020-12-31",
        "execution_date": "2021-01-04",
        "required_tickers": {"000001.SZ"},
    }

    def finalized(name, *, diemeng, limits):
        output = tmp_path / name
        manifest = module.capture(
            output,
            resume=False,
            max_new_pairs=None,
            diemeng_client=diemeng,
            limit_client=limits,
        )
        return CandidateMinuteStore(
            output,
            expected_manifest_payload_sha256=manifest["payload_sha256"],
            expected_manifest_file_sha256=file_sha256(output / "manifest.json"),
        )

    empty_store = finalized(
        "empty", diemeng=_Diemeng(empty=True), limits=_Limits(missing=True)
    )
    context = empty_store.build_context(**common, signal_snapshot=_snapshot())
    assert context[["up_limit", "down_limit"]].isna().all(axis=None)
    anchors, complete_no_anchor = empty_store.build_auction_anchors(**common)
    assert anchors.empty
    assert complete_no_anchor == {"000001.SZ"}
    bars, complete_no_bar = empty_store.build_window(**common, window="A")
    assert tuple(bars.columns) == MINUTE_EXECUTION_BAR_COLUMNS
    assert bars.empty
    assert complete_no_bar == {"000001.SZ"}

    tradable_without_limits = finalized(
        "missing-limits", diemeng=_Diemeng(), limits=_Limits(missing=True)
    )
    missing_context = tradable_without_limits.build_context(
        **common, signal_snapshot=_snapshot()
    )
    assert missing_context[["up_limit", "down_limit"]].isna().all(axis=None)
    assert tradable_without_limits._pair_artifact_cache == {}
    tradable_bars, no_bar = tradable_without_limits.build_window(
        **common, window="A"
    )
    assert not tradable_bars.empty
    assert no_bar == set()

    nullable_pre_close = finalized(
        "nullable-pre-close",
        diemeng=_Diemeng(),
        limits=_Limits(pre_close=float("nan")),
    )
    nullable_context = nullable_pre_close.build_context(
        **common, signal_snapshot=_snapshot()
    )
    assert nullable_context.loc[0, "up_limit"] == pytest.approx(11.0)
    assert nullable_context.loc[0, "down_limit"] == pytest.approx(9.0)

    with pytest.raises(RuntimeError, match="execution window is partial"):
        finalized(
            "partial",
            diemeng=_Diemeng(drop_clocks={"09:33:00"}),
            limits=_Limits(),
        )

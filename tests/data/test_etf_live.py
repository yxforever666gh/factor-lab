from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

from factor_lab.data.etf_assets import (
    CALENDAR_COLUMNS,
    ETF_TICKERS,
    HISTORY_COLUMNS,
    MultiAssetStage,
)
from factor_lab.data import etf_live


START = "2024-01-01"
BASELINE_END = "2024-01-03"
AS_OF = "2024-01-04"
STAGE = "asof-20240104"


def _calendar(end: str) -> pd.DataFrame:
    dates = pd.date_range(START, end, freq="D")
    previous = pd.Series(dates).shift(1)
    previous.iloc[0] = dates[0] - pd.Timedelta(days=1)
    return pd.DataFrame(
        {"trade_date": dates, "previous_open_date": previous},
        columns=list(CALENDAR_COLUMNS),
    )


def _history(ticker: str, end: str, *, prefix_drift: bool = False) -> pd.DataFrame:
    dates = pd.date_range(START, end, freq="D")
    count = len(dates)
    close = 10.0 + np.arange(count, dtype=float) / 10.0
    if prefix_drift:
        close[1] += 0.01
    frame = pd.DataFrame(
        {
            "ticker": [ticker] * count,
            "trade_date": dates,
            "pre_close": close - 0.05,
            "open": close - 0.02,
            "high": close + 0.05,
            "low": close - 0.05,
            "close": close,
            "volume_shares": np.arange(count, dtype=float) + 100.0,
            "amount_rmb": np.arange(count, dtype=float) + 1_000.0,
            "dividend_cash": np.zeros(count, dtype=float),
            "dividend_pay_date": pd.Series(pd.NaT, index=range(count), dtype="datetime64[ns]"),
            "adj_factor_diagnostic": np.ones(count, dtype=float),
            "unit_multiplier": np.ones(count, dtype=float),
            "reference_price_reset": np.zeros(count, dtype=bool),
            "total_return_index": 1.0 + np.arange(count, dtype=float) / 100.0,
            "adv20_rmb": np.arange(count, dtype=float) + 900.0,
        },
        columns=list(HISTORY_COLUMNS),
    )
    return frame


def _stage(
    path: Path,
    *,
    end: str,
    manifest_variant: str = "stable",
    asset_tail_delta: float = 0.0,
    prefix_drift: bool = False,
) -> MultiAssetStage:
    assets = {
        ticker: _history(ticker, end, prefix_drift=prefix_drift)
        for ticker in ETF_TICKERS
    }
    if asset_tail_delta:
        assets[ETF_TICKERS[0]].loc[
            assets[ETF_TICKERS[0]].index[-1], "close"
        ] += asset_tail_delta
    manifest = {
        "schema_version": 1,
        "stage": STAGE if end == AS_OF else "baseline",
        "price_start_date": START,
        "price_end_date": end,
        "payload_sha256": manifest_variant,
    }
    return MultiAssetStage(
        path=path,
        manifest=manifest,
        calendar=_calendar("2024-01-06" if end == AS_OF else "2024-01-05"),
        assets=assets,
    )


class FakeClient:
    def __init__(self, snapshots: list[dict[str, Any]]) -> None:
        self.snapshots = list(snapshots)
        self.capture_count = 0


def _install_fake_io(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_capture(
        client: FakeClient,
        root: str | Path,
        start: str,
        as_of: str,
        stage_name: str,
    ) -> MultiAssetStage:
        assert start == START
        assert as_of == AS_OF
        assert stage_name == STAGE
        snapshot = client.snapshots[client.capture_count]
        client.capture_count += 1
        path = Path(root).resolve() / f"stage={stage_name}"
        path.mkdir(parents=True)
        (path / "snapshot.json").write_text(
            json.dumps(snapshot, sort_keys=True), encoding="utf-8"
        )
        return _stage(path, end=AS_OF, **snapshot)

    def fake_load(root: str | Path, stage_name: str) -> MultiAssetStage:
        path = Path(root).resolve() / f"stage={stage_name}"
        snapshot = json.loads((path / "snapshot.json").read_text(encoding="utf-8"))
        stage = _stage(path, end=AS_OF, **snapshot)
        manifest_path = path / "manifest.json"
        if manifest_path.is_file():
            stage = MultiAssetStage(
                path=stage.path,
                manifest=json.loads(manifest_path.read_text(encoding="utf-8")),
                calendar=stage.calendar,
                assets=stage.assets,
            )
        return stage

    monkeypatch.setattr(etf_live, "capture_multi_asset_stage", fake_capture)
    monkeypatch.setattr(etf_live, "load_multi_asset_stage", fake_load)


def _baseline(tmp_path: Path) -> MultiAssetStage:
    return _stage(tmp_path / "baseline", end=BASELINE_END)


def _assert_no_transaction_or_stage(root: Path) -> None:
    assert not (root / f"stage={STAGE}").exists()
    assert not list(root.glob(f".stable-capture-{STAGE}-*"))


def test_stable_capture_publishes_once_and_existing_stage_is_zero_query(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install_fake_io(monkeypatch)
    root = tmp_path / "prospective" / "sources"
    client = FakeClient([{}, {}])

    first = etf_live.stable_capture_multi_asset_stage(
        client, root, START, AS_OF, STAGE, _baseline(tmp_path)
    )

    assert client.capture_count == 2
    assert first.path == (root / f"stage={STAGE}").resolve()
    assert first.path.is_dir()
    assert not list(root.glob(f".stable-capture-{STAGE}-*"))

    second = etf_live.stable_capture_multi_asset_stage(
        FakeClient([]), root, START, AS_OF, STAGE, _baseline(tmp_path)
    )
    assert second.path == first.path


def test_stable_capture_embeds_and_reuses_only_the_exact_frozen_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install_fake_io(monkeypatch)
    root = tmp_path / "prospective" / "sources"
    receipt = {
        "contract_id": "factor-lab/test/stable-source-v1",
        "full_capture_count": 2,
        "validated_at_utc": "2024-01-04T10:00:00+00:00",
    }
    first = etf_live.stable_capture_multi_asset_stage(
        FakeClient([{}, {}]),
        root,
        START,
        AS_OF,
        STAGE,
        _baseline(tmp_path),
        publication_receipt_factory=lambda _stage: receipt,
    )
    actual_receipt = first.manifest["stable_capture_receipt"]
    assert {
        key: actual_receipt[key] for key in receipt
    } == receipt
    assert len(actual_receipt["canonical_capture_payload_sha256"]) == 64

    again = etf_live.stable_capture_multi_asset_stage(
        FakeClient([]),
        root,
        START,
        AS_OF,
        STAGE,
        _baseline(tmp_path),
        publication_receipt=receipt,
    )
    assert again.manifest["stable_capture_receipt"] == actual_receipt
    with pytest.raises(ValueError, match="stable-capture receipt"):
        etf_live.stable_capture_multi_asset_stage(
            FakeClient([]),
            root,
            START,
            AS_OF,
            STAGE,
            _baseline(tmp_path),
            publication_receipt={**receipt, "full_capture_count": 1},
        )

    manifest_path = first.path / "manifest.json"
    rewritten = json.loads(manifest_path.read_text(encoding="utf-8"))
    rewritten["append_tail_rewrite"] = True
    rewritten["payload_sha256"] = etf_live.canonical_payload_sha256(rewritten)
    manifest_path.write_text(
        json.dumps(rewritten, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match="stable-capture receipt differs"):
        etf_live.stable_capture_multi_asset_stage(
            FakeClient([]),
            root,
            START,
            AS_OF,
            STAGE,
            _baseline(tmp_path),
            publication_receipt=receipt,
        )


@pytest.mark.parametrize(
    "snapshots,match",
    [
        ([{}, {"manifest_variant": "changed"}], "manifests differ"),
        ([{}, {"asset_tail_delta": 0.01}], "stable capture asset"),
    ],
)
def test_sample_difference_never_publishes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    snapshots: list[dict[str, Any]],
    match: str,
) -> None:
    _install_fake_io(monkeypatch)
    root = tmp_path / "prospective" / "sources"
    with pytest.raises(ValueError, match=match):
        etf_live.stable_capture_multi_asset_stage(
            FakeClient(snapshots), root, START, AS_OF, STAGE, _baseline(tmp_path)
        )
    _assert_no_transaction_or_stage(root)


def test_baseline_prefix_drift_never_publishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install_fake_io(monkeypatch)
    root = tmp_path / "prospective" / "sources"
    snapshots = [{"prefix_drift": True}, {"prefix_drift": True}]
    with pytest.raises(ValueError, match="baseline prefix"):
        etf_live.stable_capture_multi_asset_stage(
            FakeClient(snapshots), root, START, AS_OF, STAGE, _baseline(tmp_path)
        )
    _assert_no_transaction_or_stage(root)


def test_external_validator_runs_before_publication_and_on_idempotent_load(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install_fake_io(monkeypatch)
    root = tmp_path / "prospective" / "sources"
    calls = []

    def reject(stage: MultiAssetStage) -> None:
        calls.append(stage.path)
        raise ValueError("not an eligible quarter-end capture")

    with pytest.raises(ValueError, match="eligible quarter-end"):
        etf_live.stable_capture_multi_asset_stage(
            FakeClient([{}, {}]),
            root,
            START,
            AS_OF,
            STAGE,
            _baseline(tmp_path),
            validator=reject,
        )
    assert calls
    _assert_no_transaction_or_stage(root)

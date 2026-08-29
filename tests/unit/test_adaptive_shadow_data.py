from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from factor_lab.data import prospective


def _price_rows(ticker: str, dates: pd.DatetimeIndex) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": ticker,
            "trade_date": dates,
            "close_hfq": np.arange(1.0, len(dates) + 1.0),
            "turnover_rate": np.arange(1.0, len(dates) + 1.0) / 100.0,
        }
    )


def test_frozen_shadow_formulas_sort_full_history_and_ignore_future_rows() -> None:
    dates = pd.bdate_range("2024-01-02", periods=254)
    signal_date = dates[252]
    complete = pd.concat(
        [
            _price_rows("000001.SZ", dates),
            _price_rows("000002.SZ", dates[:252]),
        ],
        ignore_index=True,
    ).sample(frac=1.0, random_state=59)

    result = prospective._shadow_price_history(
        complete,
        signal_date=signal_date,
    )
    keys = list(zip(result["ts_code"].astype(str), result["trade_date"]))
    assert keys == sorted(keys)
    assert result["trade_date"].max() == signal_date

    first = result.loc[result["ts_code"].eq("000001.SZ")].reset_index(drop=True)
    second = result.loc[result["ts_code"].eq("000002.SZ")].reset_index(drop=True)
    assert first["low_turnover_20_v1"].iloc[:19].isna().all()
    assert first.loc[19, "low_turnover_20_v1"] == pytest.approx(-0.105)
    assert first["low_volatility_252_v1"].iloc[:252].isna().all()
    expected_log_returns = np.log(
        np.arange(2.0, 254.0) / np.arange(1.0, 253.0)
    )
    assert first.loc[252, "low_volatility_252_v1"] == pytest.approx(
        -np.std(expected_log_returns, ddof=1)
    )
    assert pd.isna(second.loc[251, "low_volatility_252_v1"])

    without_future = complete.loc[complete["trade_date"].le(signal_date)]
    replay = prospective._shadow_price_history(
        without_future,
        signal_date=signal_date,
    )
    pd.testing.assert_frame_equal(result, replay)


def test_history_window_uses_exact_three_hundred_execution_sessions() -> None:
    canonical = list(pd.bdate_range("2024-01-02", periods=380))
    cutoff = canonical[-7]
    official = list(pd.bdate_range(canonical[-1] + pd.offsets.BDay(), periods=10))
    signal = official[5]
    combined = sorted({*canonical, *official[:6]})
    expected = combined[combined.index(signal) - 300]

    selected, audit = prospective._shadow_history_window(
        canonical,
        official,
        canonical_cutoff=cutoff,
        signal_date=signal,
    )

    assert selected == expected
    assert audit == {
        "minimum_prior_sessions": 300,
        "available_prior_sessions": 300,
        "requirement_satisfied": True,
        "signal_history_start": expected.date().isoformat(),
        "selected_supplement_start": expected.date().isoformat(),
        "selected_calendar_session_count": 301,
        "selection_basis": "canonical_execution_sessions_plus_official_extension",
    }
    assert (signal - selected).days > 300


def test_shadow_eligibility_is_a_finite_intersection_without_formal_mutation() -> None:
    frame = pd.DataFrame(
        {
            "eligible": [True, True, False, True],
            "low_turnover_20_v1": [-1.0, np.nan, -0.9, -0.8],
            "low_volatility_252_v1": [-0.1, -0.1, -0.2, np.inf],
        }
    )
    formal_before = frame["eligible"].copy()

    shadow = prospective._shadow_eligibility(frame)

    assert shadow.tolist() == [True, False, False, False]
    assert frame["eligible"].equals(formal_before)
    assert bool(frame.loc[1, "eligible"]) is True
    assert bool(shadow.loc[1]) is False


def _snapshot_with_shadow_frame(tmp_path: Path) -> prospective.ProspectiveInputSnapshot:
    formal_columns = [
        "date",
        "ticker",
        "eligible",
        "universe_member",
        "earnings_yield",
        "pb",
        "book_yield",
        "volatility_20",
    ]
    frame = pd.DataFrame(
        [
            {
                "date": "2026-08-31",
                "ticker": "000001.SZ",
                "eligible": True,
                "shadow_eligible": True,
                "universe_member": True,
                "earnings_yield": 0.1,
                "pb": 1.0,
                "book_yield": 1.0,
                "volatility_20": 0.2,
                "low_turnover_20_v1": -0.95,
                "low_volatility_252_v1": -0.03,
            },
            {
                "date": "2026-08-31",
                "ticker": "000002.SZ",
                "eligible": True,
                "shadow_eligible": False,
                "universe_member": True,
                "earnings_yield": 0.2,
                "pb": 2.0,
                "book_yield": 0.5,
                "volatility_20": 0.3,
                "low_turnover_20_v1": np.nan,
                "low_volatility_252_v1": -0.04,
            },
        ]
    )
    shadow_columns = list(prospective._SHADOW_TARGET_COLUMNS)
    formal_sha = prospective._sha256_bytes(
        prospective._canonical_json_bytes(
            prospective._frame_records(frame[formal_columns])
        )
    )
    shadow_sha = prospective._sha256_bytes(
        prospective._canonical_json_bytes(
            prospective._frame_records(frame[shadow_columns])
        )
    )
    manifest = {
        "target_adapter": {
            "columns": formal_columns,
            "target_rows_sha256": formal_sha,
        },
        "shadow_target_adapter": {
            "columns": shadow_columns,
            "target_rows_sha256": shadow_sha,
            "eligibility_column": "shadow_eligible",
            "formal_eligibility_column": "eligible",
            "common_universe_rule": (
                "formal eligible and both frozen shadow scores are finite"
            ),
            "formulas": {
                name: dict(specification)
                for name, specification in prospective._SHADOW_FORMULAS.items()
            },
            "history_lineage": "frozen_bridge_plus_provider_complete",
            "history_calendar": {},
            "pristine_after_activation_sessions": 253,
            "signal_availability": "signal_t_close",
            "execution_availability": "t_plus_1_open",
            "future_labels_used": False,
            "rows_selection": "rows.json projected to columns in this exact order",
        },
    }
    return prospective.ProspectiveInputSnapshot(
        signal_date="2026-08-31",
        trade_date="2026-09-01",
        snapshot_sha256="0" * 64,
        directory=tmp_path,
        manifest_path=tmp_path / "manifest.json",
        rows_path=tmp_path / "rows.json",
        build_receipt_path=tmp_path / "build-receipt.json",
        build_completed_at_utc="2026-08-31T10:00:00Z",
        inputs_available_at_utc="2026-08-31T09:00:00Z",
        frame=frame,
        manifest=manifest,
    )


def test_shadow_projection_is_read_only_and_formal_adapter_stays_exactly_eight_columns(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot_with_shadow_frame(tmp_path)
    expected_formal_sha = prospective._sha256_bytes(
        prospective._canonical_json_bytes(
            prospective._frame_records(snapshot.frame[snapshot.target_adapter["columns"]])
        )
    )

    assert len(snapshot.target_adapter["columns"]) == 8
    assert list(snapshot.target_frame.columns) == snapshot.target_adapter["columns"]
    assert snapshot.target_rows_sha256 == expected_formal_sha
    assert "shadow_eligible" not in snapshot.target_frame.columns
    assert list(snapshot.shadow_target_frame.columns) == list(
        prospective._SHADOW_TARGET_COLUMNS
    )
    assert snapshot.shadow_target_rows_sha256 == snapshot.shadow_target_sha256
    projected = snapshot.shadow_target_frame
    projected.loc[0, "low_turnover_20_v1"] = 0.0
    assert snapshot.frame.loc[0, "low_turnover_20_v1"] == pytest.approx(-0.95)


def test_shadow_adapter_binds_every_projected_value_and_common_eligibility(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot_with_shadow_frame(tmp_path)
    rows = prospective._frame_records(snapshot.frame)
    prospective._verify_shadow_target_adapter(
        snapshot.manifest,
        rows,
        list(snapshot.frame.columns),
    )

    changed = deepcopy(rows)
    changed[0]["low_turnover_20_v1"] = float(-0.94).hex()
    with pytest.raises(
        prospective.ProspectiveDataError,
        match="shadow target rows binding mismatch",
    ):
        prospective._verify_shadow_target_adapter(
            snapshot.manifest,
            changed,
            list(snapshot.frame.columns),
        )

    missing_score = deepcopy(rows)
    missing_score[0]["low_turnover_20_v1"] = None
    resigned = deepcopy(snapshot.manifest)
    columns = resigned["shadow_target_adapter"]["columns"]
    resigned["shadow_target_adapter"]["target_rows_sha256"] = (
        prospective._sha256_bytes(
            prospective._canonical_json_bytes(
                [{column: row[column] for column in columns} for row in missing_score]
            )
        )
    )
    with pytest.raises(
        prospective.ProspectiveDataError,
        match="shadow eligibility binding mismatch",
    ):
        prospective._verify_shadow_target_adapter(
            resigned,
            missing_score,
            list(snapshot.frame.columns),
        )

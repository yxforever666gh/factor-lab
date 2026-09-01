from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from factor_lab.data.pit_stock import PITStockRawStore, build_pit_snapshot
from factor_lab.research.pit_stock import PITStockContractError


def _inputs(count: int = 1100):
    sessions = tuple(pd.bdate_range("2022-01-03", periods=253))
    signal = sessions[-1]
    tickers = [f"{index:06d}.SZ" for index in range(count)]
    master = pd.DataFrame(
        {
            "ts_code": tickers,
            "exchange": "SZSE",
            "curr_type": "CNY",
            "list_date": pd.Timestamp("2020-01-02"),
            "delist_date": pd.NaT,
        }
    )
    prices = np.exp(np.linspace(0, 0.2, len(sessions)))
    close = pd.DataFrame(
        np.tile(prices, (count, 1)), index=tickers, columns=sessions
    )
    amount = pd.DataFrame(
        np.arange(count, 0, -1, dtype=float)[:, None]
        * np.ones((count, 20))
        * 1_000_000,
        index=tickers,
        columns=sessions[-20:],
    )
    market = pd.DataFrame(
        {"ts_code": tickers, "close_adj": close.iloc[:, -1], "amount_rmb": amount.iloc[:, -1]}
    )
    basic = pd.DataFrame(
        {"ts_code": tickers, "total_mv": np.arange(count) + 1, "circ_mv": np.arange(count) + 1}
    )
    industry = pd.Series("industry", index=tickers)
    return sessions, signal, master, close, amount, market, basic, industry


def test_snapshot_keeps_all_active_rows_and_marks_exact_top1000() -> None:
    sessions, signal, master, close, amount, market, basic, industry = _inputs()
    result = build_pit_snapshot(
        signal_date=signal,
        official_sessions=sessions,
        security_master=master,
        close_history=close,
        amount_history=amount,
        current_market=market,
        daily_basic=basic,
        st_tickers=set(),
        industry=industry,
        industry_source_date=signal,
    )
    assert len(result) == 1100
    assert result["universe_member"].sum() == 1000
    assert (result["exclusion_reason"] == "outside_top_adv_universe").sum() == 100
    assert set(result.loc[result["universe_member"], "size_bucket"]) == {"small", "mid", "large"}


def test_future_delist_does_not_remove_historical_member() -> None:
    sessions, signal, master, close, amount, market, basic, industry = _inputs()
    master.loc[0, "delist_date"] = signal + pd.Timedelta(days=30)
    result = build_pit_snapshot(
        signal_date=signal,
        official_sessions=sessions,
        security_master=master,
        close_history=close,
        amount_history=amount,
        current_market=market,
        daily_basic=basic,
        st_tickers=set(),
        industry=industry,
        industry_source_date=signal,
    )
    assert result.loc[result["ticker"].eq("000000.SZ"), "universe_member"].item()


def test_st_and_incomplete_history_are_excluded_without_dropping_rows() -> None:
    sessions, signal, master, close, amount, market, basic, industry = _inputs()
    close.loc["000001.SZ", sessions[10]] = np.nan
    result = build_pit_snapshot(
        signal_date=signal,
        official_sessions=sessions,
        security_master=master,
        close_history=close,
        amount_history=amount,
        current_market=market,
        daily_basic=basic,
        st_tickers={"000002.SZ"},
        industry=industry,
        industry_source_date=signal,
    ).set_index("ticker")
    assert result.at["000001.SZ", "exclusion_reason"] == "incomplete_253_session_history"
    assert result.at["000002.SZ", "exclusion_reason"] == "st_on_signal"
    assert not result.at["000001.SZ", "universe_member"]
    assert not result.at["000002.SZ", "universe_member"]


def test_history_columns_must_be_exact_official_sessions() -> None:
    sessions, signal, master, close, amount, market, basic, industry = _inputs()
    close = close.rename(columns={sessions[0]: sessions[0] + pd.Timedelta(days=1)})
    with pytest.raises(PITStockContractError, match="exact 253-session"):
        build_pit_snapshot(
            signal_date=signal,
            official_sessions=sessions,
            security_master=master,
            close_history=close,
            amount_history=amount,
            current_market=market,
            daily_basic=basic,
            st_tickers=set(),
            industry=industry,
            industry_source_date=signal,
        )


def test_alias_config_rejects_overlapping_vendor_intervals(tmp_path: Path) -> None:
    config = tmp_path / "configs" / "data.json"
    config.parent.mkdir()
    config.write_text(
        json.dumps(
            {
                "enrichment": {
                    "security_code_aliases": [
                        {
                            "canonical_ts_code": "000001.SZ",
                            "vendor_ts_code": "000999.SZ",
                            "effective_from": "2020-01-01",
                            "effective_to": "2020-12-31",
                            "source": "first",
                        },
                        {
                            "canonical_ts_code": "000002.SZ",
                            "vendor_ts_code": "000999.SZ",
                            "effective_from": "2020-06-01",
                            "effective_to": "2021-01-01",
                            "source": "second",
                        },
                    ]
                }
            }
        ),
        encoding="utf-8",
    )
    store = PITStockRawStore.__new__(PITStockRawStore)
    store.project_root = tmp_path
    with pytest.raises(PITStockContractError, match="overlapping alias"):
        store._load_aliases()


def test_calendar_store_rejects_truncated_through_date(tmp_path: Path) -> None:
    dates = pd.date_range("2026-09-01", "2026-09-28", freq="D")
    open_mask = dates.dayofweek < 5
    frame = pd.DataFrame(
        {
            "exchange": "SSE",
            "cal_date": dates,
            "is_open": open_mask,
            "pretrade_date": pd.Series(dates).where(open_mask).ffill().shift(1).fillna(dates[0]),
        }
    )
    calendar = tmp_path / "calendar.parquet"
    manifest = tmp_path / "manifest.json"
    frame.to_parquet(calendar, index=False)
    manifest.write_text("{}", encoding="utf-8")
    sessions = [pd.Timestamp(value) for value in dates[open_mask]]
    partitions = {}
    for dataset in ("daily", "daily_basic", "adj_factor"):
        for date in sessions:
            partitions[f"{dataset}/{date.date().isoformat()}"] = {
                "status": "complete",
                "dataset": dataset,
                "trade_date": str(date.date()),
            }
    store = PITStockRawStore.__new__(PITStockRawStore)
    store.maximum_read_date = sessions[-1]
    store.calendar_through_date = pd.Timestamp("2026-09-30")
    store._checkpoint = {
        "partitions": partitions,
        "calendars": {
            "calendar": {
                "status": "complete",
                "exchange": "SSE",
                "start_date": "2026-09-01",
                "end_date": "2026-09-28",
                "path": str(calendar),
                "artifact_sha256": "0" * 64,
                "manifest_path": str(manifest),
                "manifest_sha256": "0" * 64,
                "row_count": len(frame),
            }
        },
    }
    store._verified_artifact = lambda path, **_: Path(path)
    with pytest.raises(PITStockContractError, match="exactly cover"):
        store._load_sessions()

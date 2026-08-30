from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest

from factor_lab.data.etf_assets import (
    CALENDAR_LOOKAHEAD_DAYS,
    ETF_ASSETS,
    ETF_TARGET_WEIGHTS,
    ETF_TICKERS,
    FUND_ADJ_FIELDS,
    FUND_DAILY_FIELDS,
    FUND_DIV_FIELDS,
    HISTORY_COLUMNS,
    OFFICIAL_UNIT_EVENTS,
    PRE_CLOSE_ABS_TOLERANCE,
    TRADE_CAL_FIELDS,
    _visible_unit_event,
    build_total_return_history,
    capture_multi_asset_stage,
    load_multi_asset_stage,
    normalize_fund_adj,
    normalize_fund_daily,
    normalize_fund_div,
    normalize_trade_calendar,
)


def test_future_unit_event_is_absent_from_earlier_stage_manifest() -> None:
    assert _visible_unit_event("513100.SH", pd.Timestamp("2019-12-31")) is None
    assert _visible_unit_event("513100.SH", pd.Timestamp("2022-12-30")) == (
        OFFICIAL_UNIT_EVENTS["513100.SH"]
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _manifest_payload_sha256(value: dict[str, Any]) -> str:
    payload = dict(value)
    payload.pop("payload_sha256", None)
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _daily_rows(
    ticker: str = "510300.SH",
    *,
    periods: int = 25,
    dividends: dict[int, float] | None = None,
) -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=periods)
    cash = dividends or {}
    closes: list[float] = []
    rows: list[dict[str, Any]] = []
    for index, date in enumerate(dates):
        dividend = float(cash.get(index, 0.0))
        if index == 0:
            pre_close = 10.0
            close = 10.0
        else:
            pre_close = closes[-1] - dividend
            close = pre_close + 0.01
        closes.append(close)
        rows.append(
            {
                "ts_code": ticker,
                "trade_date": date.strftime("%Y%m%d"),
                "pre_close": pre_close,
                "open": close - 0.005,
                "high": close + 0.02,
                "low": close - 0.02,
                "close": close,
                "vol": 1_000.0 + index,
                "amount": 2_000.0 + index,
            }
        )
    return pd.DataFrame(rows, columns=list(FUND_DAILY_FIELDS))


def _dividend_rows(
    ticker: str,
    daily: pd.DataFrame,
    dividends: dict[int, float],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for index, value in dividends.items():
        ex_date = pd.Timestamp(daily.iloc[index]["trade_date"])
        rows.append(
            {
                "ts_code": ticker,
                "ann_date": (ex_date - pd.Timedelta(days=10)).strftime("%Y%m%d"),
                "imp_anndate": (ex_date - pd.Timedelta(days=7)).strftime("%Y%m%d"),
                "div_proc": "实施",
                "record_date": (ex_date - pd.Timedelta(days=1)).strftime("%Y%m%d"),
                "ex_date": ex_date.strftime("%Y%m%d"),
                "pay_date": (ex_date + pd.Timedelta(days=2)).strftime("%Y%m%d"),
                "div_cash": value,
            }
        )
    return pd.DataFrame(rows, columns=list(FUND_DIV_FIELDS))


def _trade_calendar_rows(start: str, end: str) -> pd.DataFrame:
    dates = pd.date_range(pd.Timestamp(start), pd.Timestamp(end), freq="D")
    previous_open = dates[0] - pd.offsets.BDay(1)
    closed_holidays = {pd.Timestamp("2024-01-01")}
    rows: list[dict[str, Any]] = []
    for date in dates:
        is_open = int(date.weekday() < 5 and date not in closed_holidays)
        rows.append(
            {
                "exchange": "SSE",
                "cal_date": date.strftime("%Y%m%d"),
                "is_open": is_open,
                "pretrade_date": previous_open.strftime("%Y%m%d"),
            }
        )
        if is_open:
            previous_open = date
    return pd.DataFrame(rows, columns=list(TRADE_CAL_FIELDS))


def _custom_daily_rows(
    ticker: str,
    dates: list[str],
    pre_closes: list[float],
    closes: list[float],
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": ticker,
            "trade_date": dates,
            "pre_close": pre_closes,
            "open": closes,
            "high": np.asarray(closes) + 0.01,
            "low": np.asarray(closes) - 0.01,
            "close": closes,
            "vol": 1_000.0,
            "amount": 2_000.0,
        },
        columns=list(FUND_DAILY_FIELDS),
    )


def test_fixed_asset_registry_uses_correct_us_etf_and_weights() -> None:
    assert ETF_TICKERS == (
        "510300.SH",
        "159920.SZ",
        "513100.SH",
        "518880.SH",
        "511010.SH",
        "511880.SH",
    )
    assert "513500.SH" not in ETF_TICKERS
    assert sum(ETF_TARGET_WEIGHTS.values()) == pytest.approx(1.0)
    assert ETF_TARGET_WEIGHTS["511880.SH"] == 0.0


def test_normalize_fund_daily_converts_vendor_units_and_is_strict() -> None:
    raw = _daily_rows(periods=2)
    normalized = normalize_fund_daily(raw, expected_ticker="510300.SH")

    assert normalized.loc[0, "volume_shares"] == 100_000.0
    assert normalized.loc[0, "amount_rmb"] == 2_000_000.0
    assert list(normalized.columns) == [
        "ticker",
        "trade_date",
        "pre_close",
        "open",
        "high",
        "low",
        "close",
        "volume_shares",
        "amount_rmb",
    ]

    with pytest.raises(ValueError, match="columns mismatch"):
        normalize_fund_daily(raw.assign(pct_chg=1.0))
    with pytest.raises(ValueError, match="strictly positive"):
        normalize_fund_daily(raw.assign(close=[10.0, 0.0]))
    with pytest.raises(ValueError, match="unsupported ETF codes"):
        normalize_fund_daily(raw.assign(ts_code="513500.SH"))
    with pytest.raises(ValueError, match="exact YYYYMMDD"):
        normalize_fund_daily(raw.assign(trade_date="2024-01-02"))
    with pytest.raises(ValueError, match="unit conversion"):
        normalize_fund_daily(raw.assign(amount=np.finfo(float).max))


def test_trade_calendar_normalizes_only_linked_official_open_sessions() -> None:
    raw = _trade_calendar_rows("2024-01-02", "2024-01-08")
    result = normalize_trade_calendar(
        raw, start="20240102", end="20240108"
    )
    assert result["trade_date"].dt.strftime("%Y%m%d").tolist() == [
        "20240102",
        "20240103",
        "20240104",
        "20240105",
        "20240108",
    ]
    assert result["previous_open_date"].iloc[1:].reset_index(drop=True).equals(
        result["trade_date"].iloc[:-1].reset_index(drop=True)
    )

    with pytest.raises(ValueError, match="cover every requested"):
        normalize_trade_calendar(
            raw.iloc[1:].copy(), start="20240102", end="20240108"
        )
    with pytest.raises(ValueError, match="unexpected exchange"):
        normalize_trade_calendar(
            raw.assign(exchange="SZSE"), start="20240102", end="20240108"
        )


def test_normalize_fund_div_collapses_duplicates_and_rejects_conflicts() -> None:
    daily = _daily_rows(periods=5)
    original = _dividend_rows("510300.SH", daily, {3: 0.12})
    duplicate = original.copy()
    duplicate["ann_date"] = (
        pd.Timestamp(original.loc[0, "ann_date"]) + pd.Timedelta(days=1)
    ).strftime("%Y%m%d")
    rows = pd.concat([original, duplicate], ignore_index=True)
    result = normalize_fund_div(rows)
    assert len(result) == 1
    assert result.loc[0, "ann_date"] == pd.Timestamp(original.loc[0, "ann_date"])
    assert result.loc[0, "record_date"] <= result.loc[0, "ex_date"]
    assert result.loc[0, "pay_date"] >= result.loc[0, "ex_date"]
    assert result.loc[0, "div_proc"] == "实施"
    assert result.loc[0, "div_cash"] == pytest.approx(0.12)

    conflict = pd.concat([rows, original.assign(div_cash=0.13)], ignore_index=True)
    with pytest.raises(ValueError, match="conflicting events"):
        normalize_fund_div(conflict)
    with pytest.raises(ValueError, match="ann<=record<=ex<=pay"):
        normalize_fund_div(
            rows.assign(
                record_date=(
                    pd.to_datetime(rows["ex_date"], format="%Y%m%d")
                    + pd.Timedelta(days=1)
                ).dt.strftime("%Y%m%d")
            )
        )

    optional_implementation_date = normalize_fund_div(
        original.assign(imp_anndate="")
    )
    assert pd.isna(optional_implementation_date.loc[0, "imp_anndate"])
    assert normalize_fund_div(original.assign(div_proc="预案")).empty


def test_159920_implemented_dividend_allows_record_date_equal_ex_date() -> None:
    daily = _daily_rows("159920.SZ", periods=5, dividends={3: 0.08})
    event = _dividend_rows("159920.SZ", daily, {3: 0.08})
    event["record_date"] = event["ex_date"]
    normalized = normalize_fund_div(event, expected_ticker="159920.SZ")
    assert len(normalized) == 1
    assert normalized.loc[0, "record_date"] == normalized.loc[0, "ex_date"]
    history = build_total_return_history(normalize_fund_daily(daily), normalized)
    assert history.loc[3, "dividend_cash"] == pytest.approx(0.08)


def test_normalize_fund_adj_is_positive_diagnostic_only() -> None:
    rows = pd.DataFrame(
        [["510300.SH", "20240102", 1.25]], columns=list(FUND_ADJ_FIELDS)
    )
    result = normalize_fund_adj(rows)
    assert result.to_dict("records") == [
        {
            "ticker": "510300.SH",
            "trade_date": pd.Timestamp("2024-01-02"),
            "adj_factor": 1.25,
        }
    ]
    with pytest.raises(ValueError, match="strictly positive"):
        normalize_fund_adj(rows.assign(adj_factor=0.0))

    duplicate = pd.concat([rows, rows], ignore_index=True)
    assert len(normalize_fund_adj(duplicate)) == 1
    conflict = pd.concat(
        [rows, rows.assign(adj_factor=1.26)], ignore_index=True
    )
    with pytest.raises(ValueError, match="conflicting factors"):
        normalize_fund_adj(conflict)


def test_total_return_uses_cash_dividend_and_adv20() -> None:
    cash = {1: 0.5}
    raw = _daily_rows(periods=21, dividends=cash)
    daily = normalize_fund_daily(raw)
    dividends = normalize_fund_div(_dividend_rows("510300.SH", raw, cash))
    result = build_total_return_history(daily, dividends)

    expected_second = (daily.loc[1, "close"] + 0.5) / daily.loc[0, "close"]
    assert result.loc[1, "dividend_cash"] == pytest.approx(0.5)
    assert result.loc[1, "dividend_pay_date"] == dividends.loc[0, "pay_date"]
    assert result.loc[1, "total_return_index"] == pytest.approx(expected_second)
    assert result.loc[:18, "adv20_rmb"].isna().all()
    assert result.loc[19, "adv20_rmb"] == pytest.approx(
        daily.loc[:19, "amount_rmb"].mean()
    )
    assert list(result.columns) == list(HISTORY_COLUMNS)
    assert "trade_date" in result and "date" not in result

    scaled = daily.copy()
    scaled[["pre_close", "open", "high", "low", "close"]] *= 10.0
    scaled_dividends = dividends.copy()
    scaled_dividends["div_cash"] *= 10.0
    scaled_result = build_total_return_history(scaled, scaled_dividends)
    np.testing.assert_allclose(
        result["total_return_index"], scaled_result["total_return_index"], rtol=0, atol=1e-12
    )


def test_513100_official_unit_event_and_reference_reset_are_explicit() -> None:
    event = OFFICIAL_UNIT_EVENTS["513100.SH"]
    assert event == {
        "split_date": "2022-01-13",
        "first_resumed_trade_date": "2022-01-14",
        "unit_multiplier": 5.0,
        "source_url": (
            "https://www.sse.com.cn/disclosure/fund/announcement/c/new/"
            "2022-01-04/513100_20220104_1_0U96WMU8.pdf"
        ),
        "source_sha256": (
            "7dd4e55669fcd2ec0645a61e2dba648e000a2947e6bd160b43ea6cf3898abef1"
        ),
        "source_size_bytes": 303_782,
    }
    split_daily = normalize_fund_daily(
        _custom_daily_rows(
            "513100.SH",
            ["20220112", "20220114", "20220117"],
            [5.0, 1.0, 1.02],
            [5.0, 1.02, 1.03],
        )
    )
    split_adj = normalize_fund_adj(
        pd.DataFrame(
            {
                "ts_code": "513100.SH",
                "trade_date": ["20220112", "20220114", "20220117"],
                "adj_factor": [1.0, 5.0, 5.0],
            },
            columns=list(FUND_ADJ_FIELDS),
        )
    )
    split = build_total_return_history(split_daily, adjustments=split_adj)
    assert split["unit_multiplier"].tolist() == [1.0, 5.0, 1.0]
    assert not split["reference_price_reset"].any()
    assert split.loc[1, "total_return_index"] == pytest.approx(1.02)

    with pytest.raises(ValueError, match="unit event does not reconcile"):
        build_total_return_history(
            split_daily,
            adjustments=split_adj.assign(adj_factor=[1.0, 4.9, 4.9]),
        )

    reset_daily = normalize_fund_daily(
        _custom_daily_rows(
            "513100.SH",
            ["20210723", "20210726", "20210727"],
            [1.0, 1.01, 1.02],
            [1.0, 1.02, 1.03],
        )
    )
    reset_adj = normalize_fund_adj(
        pd.DataFrame(
            {
                "ts_code": "513100.SH",
                "trade_date": ["20210723", "20210726", "20210727"],
                "adj_factor": [2.0, 2.0, 2.0],
            },
            columns=list(FUND_ADJ_FIELDS),
        )
    )
    reset = build_total_return_history(reset_daily, adjustments=reset_adj)
    assert reset["unit_multiplier"].eq(1.0).all()
    assert reset["reference_price_reset"].tolist() == [False, True, False]
    assert reset.loc[1, "total_return_index"] == pytest.approx(1.02)


def test_total_return_fails_on_unexplained_action() -> None:
    daily = normalize_fund_daily(_daily_rows(periods=3))
    expected = float(daily.loc[0, "close"])
    at_boundary = daily.copy()
    at_boundary.loc[1, "pre_close"] = expected + PRE_CLOSE_ABS_TOLERANCE
    build_total_return_history(at_boundary)

    daily.loc[1, "pre_close"] = expected + PRE_CLOSE_ABS_TOLERANCE + 1e-6
    with pytest.raises(ValueError, match="unexplained fund corporate action"):
        build_total_return_history(daily)


def test_future_append_cannot_change_existing_total_return_history() -> None:
    cash = {23: 0.2}
    raw = _daily_rows(periods=26, dividends=cash)
    all_daily = normalize_fund_daily(raw)
    all_dividends = normalize_fund_div(_dividend_rows("510300.SH", raw, cash))

    prefix = build_total_return_history(all_daily.iloc[:22].copy(), all_dividends)
    full = build_total_return_history(all_daily, all_dividends)
    pdt.assert_frame_equal(prefix, full.iloc[: len(prefix)].reset_index(drop=True))


class _FakeFundClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def query(self, endpoint: str, **kwargs: Any) -> pd.DataFrame:
        self.calls.append((endpoint, dict(kwargs)))
        if endpoint == "trade_cal":
            return _trade_calendar_rows(kwargs["start_date"], kwargs["end_date"])
        if endpoint == "fund_div":
            assert set(kwargs) == {"ex_date", "fields"}
            raw = _daily_rows("510300.SH", periods=25, dividends={1: 0.5})
            event = _dividend_rows("510300.SH", raw, {1: 0.5})
            return event.loc[
                event["ex_date"].eq(str(kwargs["ex_date"]))
            ].reset_index(drop=True)
        ticker = str(kwargs["ts_code"])
        if endpoint == "fund_daily":
            return _daily_rows(
                ticker,
                periods=25,
                dividends={1: 0.5} if ticker == "510300.SH" else None,
            )
        if endpoint == "fund_adj":
            daily = _daily_rows(ticker, periods=25)
            return pd.DataFrame(
                {
                    "ts_code": ticker,
                    "trade_date": daily["trade_date"],
                    "adj_factor": np.linspace(1.0, 1.1, len(daily)),
                },
                columns=list(FUND_ADJ_FIELDS),
            )
        raise AssertionError(f"unexpected endpoint: {endpoint}")


class _YearChunkFundClient(_FakeFundClient):
    def query(self, endpoint: str, **kwargs: Any) -> pd.DataFrame:
        if endpoint != "fund_adj":
            return super().query(endpoint, **kwargs)
        self.calls.append((endpoint, dict(kwargs)))
        ticker = str(kwargs["ts_code"])
        daily = _daily_rows(ticker, periods=25)
        frame = pd.DataFrame(
            {
                "ts_code": ticker,
                "trade_date": daily["trade_date"],
                "adj_factor": np.linspace(1.0, 1.1, len(daily)),
            },
            columns=list(FUND_ADJ_FIELDS),
        )
        within = frame["trade_date"].between(
            str(kwargs["start_date"]), str(kwargs["end_date"])
        )
        return frame.loc[within].reset_index(drop=True)


class _MissingAssetDayClient(_FakeFundClient):
    def query(self, endpoint: str, **kwargs: Any) -> pd.DataFrame:
        if endpoint != "fund_daily" or kwargs.get("ts_code") != "513100.SH":
            return super().query(endpoint, **kwargs)
        self.calls.append((endpoint, dict(kwargs)))
        frame = _daily_rows("513100.SH", periods=25)
        prior_close = float(frame.loc[9, "close"])
        frame = frame.drop(index=10).reset_index(drop=True)
        frame.loc[10, "pre_close"] = prior_close
        return frame


def test_capture_load_tamper_and_create_only(tmp_path: Path) -> None:
    client = _FakeFundClient()
    stage = capture_multi_asset_stage(
        client,
        tmp_path,
        "2024-01-02",
        "2024-02-05",
        "train",
    )
    assert stage.path == (tmp_path / "stage=train").resolve()
    assert set(stage.assets) == set(ETF_TICKERS)
    manifest = json.loads((stage.path / "manifest.json").read_text(encoding="utf-8"))
    assert len(manifest["payload_sha256"]) == 64
    assert set(manifest["assets"]) == set(ETF_TICKERS)
    assert manifest["assets"]["513100.SH"]["official_unit_event"] == (
        OFFICIAL_UNIT_EVENTS["513100.SH"]
    )
    assert manifest["price_end_date"] == "2024-02-05"
    assert manifest["calendar_end_date"] == "2024-03-07"
    assert (
        pd.Timestamp(manifest["calendar_end_date"])
        <= pd.Timestamp(manifest["price_end_date"])
        + pd.Timedelta(days=CALENDAR_LOOKAHEAD_DAYS)
    )
    assert stage.calendar["trade_date"].max() <= pd.Timestamp("2024-03-07")
    assert stage.calendar["trade_date"].max() > pd.Timestamp("2024-02-05")
    assert "calendar" in manifest and (stage.path / "calendar.parquet").is_file()
    assert all(len(frame) == 25 for frame in stage.assets.values())
    assert all("trade_date" in frame and "date" not in frame for frame in stage.assets.values())
    china = stage.assets["510300.SH"]
    assert china["dividend_cash"].gt(0.0).sum() == 1
    assert china.loc[china["dividend_cash"].gt(0.0), "dividend_pay_date"].notna().all()

    dividend_calls = [kwargs for name, kwargs in client.calls if name == "fund_div"]
    assert len(dividend_calls) == 1
    assert all(set(call) == {"ex_date", "fields"} for call in dividend_calls)
    assert [call["ex_date"] for call in dividend_calls] == ["20240103"]
    assert manifest["fund_div_ex_date_query"] == {
        "bounded_by": "ex_date",
        "queried_ex_dates": ["2024-01-03"],
        "query_count": 1,
        "unbounded_query_count": 0,
    }

    reloaded = load_multi_asset_stage(tmp_path, "train")
    pdt.assert_frame_equal(reloaded.calendar, stage.calendar)
    pdt.assert_frame_equal(
        reloaded.assets["513100.SH"], stage.assets["513100.SH"]
    )

    calls_before = len(client.calls)
    with pytest.raises(FileExistsError, match="already exists"):
        capture_multi_asset_stage(
            client,
            tmp_path,
            "2024-01-02",
            "2024-02-05",
            "train",
        )
    assert len(client.calls) == calls_before

    with (stage.path / "calendar.parquet").open("ab") as handle:
        handle.write(b"tamper")
    with pytest.raises(ValueError, match="calendar file hash or size mismatch"):
        load_multi_asset_stage(tmp_path, "train")


def test_load_rejects_rehashed_manifest_identity_tamper(tmp_path: Path) -> None:
    client = _FakeFundClient()
    stage = capture_multi_asset_stage(
        client, tmp_path, "20240102", "20240205", "selection"
    )
    manifest_path = stage.path / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["stage"] = "audit"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="payload hash mismatch"):
        load_multi_asset_stage(tmp_path, "selection")


def test_load_rejects_truncated_asset_even_after_file_and_manifest_rehash(
    tmp_path: Path,
) -> None:
    stage = capture_multi_asset_stage(
        _FakeFundClient(), tmp_path, "2024-01-02", "2024-02-05", "truncated"
    )
    ticker = "159920.SZ"
    asset_path = stage.path / f"{ticker}.parquet"
    truncated = pd.read_parquet(asset_path).iloc[:-1].copy()
    truncated.to_parquet(asset_path, index=False, compression="zstd")

    manifest_path = stage.path / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["assets"][ticker]["row_count"] = len(truncated)
    manifest["assets"][ticker]["size_bytes"] = asset_path.stat().st_size
    manifest["assets"][ticker]["file_sha256"] = _sha256(asset_path)
    manifest["payload_sha256"] = _manifest_payload_sha256(manifest)
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True), encoding="utf-8"
    )

    with pytest.raises(ValueError, match="full price boundary"):
        load_multi_asset_stage(tmp_path, "truncated")


def test_capture_chunks_fund_adj_by_natural_year(tmp_path: Path) -> None:
    client = _YearChunkFundClient()
    stage = capture_multi_asset_stage(
        client, tmp_path, "2023-12-31", "2024-02-05", "cross-year"
    )
    adjustment_calls = [call for call in client.calls if call[0] == "fund_adj"]
    assert len(adjustment_calls) == len(ETF_ASSETS) * 2
    by_ticker = {
        ticker: [kwargs for _endpoint, kwargs in adjustment_calls if kwargs["ts_code"] == ticker]
        for ticker in ETF_TICKERS
    }
    assert all(
        [(row["start_date"], row["end_date"]) for row in rows]
        == [("20231231", "20231231"), ("20240101", "20240205")]
        for rows in by_ticker.values()
    )
    assert all(
        entry["fund_adj_diagnostic_query_count"] == 2
        for entry in stage.manifest["assets"].values()
    )


def test_capture_keeps_real_asset_gap_for_research_and_respects_future_cutoffs(
    tmp_path: Path,
) -> None:
    client = _MissingAssetDayClient()
    stage = capture_multi_asset_stage(
        client, tmp_path, "2024-01-02", "2024-02-05", "missing-day"
    )
    missing_date = pd.Timestamp(_daily_rows("513100.SH").loc[10, "trade_date"])
    us_history = stage.assets["513100.SH"]
    assert missing_date in set(stage.calendar["trade_date"])
    assert missing_date not in set(us_history["trade_date"])
    assert len(us_history) == 24

    price_end = pd.Timestamp(stage.manifest["price_end_date"])
    calendar_end = pd.Timestamp(stage.manifest["calendar_end_date"])
    assert calendar_end == price_end + pd.Timedelta(days=CALENDAR_LOOKAHEAD_DAYS)
    assert all(frame["trade_date"].max() <= price_end for frame in stage.assets.values())
    assert all(
        pd.Timestamp(call["ex_date"]) <= price_end
        for endpoint, call in client.calls
        if endpoint == "fund_div"
    )
    assert all(
        pd.Timestamp(call["end_date"]) <= price_end
        for endpoint, call in client.calls
        if endpoint in {"fund_daily", "fund_adj"}
    )
    calendar_calls = [call for endpoint, call in client.calls if endpoint == "trade_cal"]
    assert len(calendar_calls) == 1
    assert pd.Timestamp(calendar_calls[0]["end_date"]) == calendar_end

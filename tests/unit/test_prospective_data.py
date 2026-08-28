from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from factor_lab.data.catalog import sha256_file
from factor_lab.data import RuntimeLayout, sync_data
from factor_lab.data import prospective
from factor_lab.data.sources import DATASET_FIELDS


TICKERS = [f"{index:06d}.SZ" for index in range(1, 11)]
CUTOFF = pd.Timestamp("2026-08-13")
BRIDGE_END = pd.Timestamp("2026-08-21")
COMPLETED_AT = "2026-08-25T08:00:00Z"


def _canonical_bytes(payload: object) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_canonical_bytes(payload))


def _market_history() -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = pd.bdate_range(end=BRIDGE_END, periods=32)
    price_rows: list[dict] = []
    basic_rows: list[dict] = []
    for ticker_index, ticker in enumerate(TICKERS):
        scale = 2.0 + ticker_index * 0.01
        for date_index, date in enumerate(dates):
            close_hfq = 100.0 + ticker_index + date_index * (0.2 + ticker_index * 0.001)
            open_hfq = close_hfq / 1.001
            price_rows.append(
                {
                    "ts_code": ticker,
                    "trade_date": date,
                    "open_hfq": open_hfq,
                    "high_hfq": close_hfq * 1.002,
                    "low_hfq": open_hfq * 0.998,
                    "close_hfq": close_hfq,
                    "amount_akshare": 100_000_000.0 + ticker_index * 1_000_000 + date_index,
                    "turnover_akshare": 0.01,
                    "price_source": "fixture_hfq",
                    "scale_factor": scale,
                }
            )
            basic_rows.append(
                {
                    "ts_code": ticker,
                    "trade_date": date,
                    "pe_ttm": 8.0 + ticker_index * 0.2 + date_index * 0.001,
                    "pb": 0.8 + ticker_index * 0.03 + date_index * 0.001,
                }
            )
    return pd.DataFrame(price_rows), pd.DataFrame(basic_rows)


def _raw_daily(price: pd.DataFrame, date: pd.Timestamp) -> pd.DataFrame:
    selected = price.loc[price["trade_date"].eq(date)].copy()
    previous = price.loc[price["trade_date"].lt(date)].groupby("ts_code").tail(1)
    previous_close = previous.set_index("ts_code")["close_hfq"]
    rows: list[dict] = []
    for row in selected.itertuples(index=False):
        prior_hfq = float(previous_close.get(row.ts_code, row.close_hfq))
        scale = float(row.scale_factor)
        close = float(row.close_hfq) / scale
        pre_close = prior_hfq / scale
        rows.append(
            {
                "ts_code": row.ts_code,
                "trade_date": date.strftime("%Y%m%d"),
                "open": float(row.open_hfq) / scale,
                "high": float(row.high_hfq) / scale,
                "low": float(row.low_hfq) / scale,
                "close": close,
                "pre_close": pre_close,
                "change": close - pre_close,
                "pct_chg": (close / pre_close - 1.0) * 100.0,
                "vol": 1_000_000.0,
                "amount": float(row.amount_akshare) / 1000.0,
            }
        )
    return pd.DataFrame(rows)


def _future_frames(
    price: pd.DataFrame,
    basic: pd.DataFrame,
    date: pd.Timestamp,
    prior_date: pd.Timestamp,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    prior_prices = price.loc[price["trade_date"].eq(prior_date)].set_index("ts_code")
    latest_basic = basic.loc[basic["trade_date"].eq(BRIDGE_END)].set_index("ts_code")
    daily_rows: list[dict] = []
    basic_rows: list[dict] = []
    adj_rows: list[dict] = []
    for ticker_index, ticker in enumerate(TICKERS):
        prior = prior_prices.loc[ticker]
        scale = float(prior["scale_factor"])
        pre_close = float(prior["close_hfq"]) / scale
        close = pre_close * 1.01
        daily_rows.append(
            {
                "ts_code": ticker,
                "trade_date": date.strftime("%Y%m%d"),
                "open": pre_close * 1.002,
                "high": close * 1.002,
                "low": pre_close * 0.998,
                "close": close,
                "pre_close": pre_close,
                "change": close - pre_close,
                "pct_chg": 1.0,
                "vol": 1_000_000.0,
                "amount": 120_000.0 + ticker_index,
            }
        )
        basic_rows.append(
            {
                "ts_code": ticker,
                "trade_date": date.strftime("%Y%m%d"),
                "pe_ttm": float(latest_basic.loc[ticker, "pe_ttm"]) + 0.1,
                "pb": float(latest_basic.loc[ticker, "pb"]) + 0.01,
            }
        )
        adj_rows.append(
            {
                "ts_code": ticker,
                "trade_date": date.strftime("%Y%m%d"),
                "adj_factor": scale / 1.5,
            }
        )
    return pd.DataFrame(daily_rows), pd.DataFrame(basic_rows), pd.DataFrame(adj_rows)


def _partition(
    root: Path,
    checkpoint: dict,
    dataset: str,
    date: pd.Timestamp,
    frame: pd.DataFrame,
) -> None:
    text = date.date().isoformat()
    path = root / "runtime/data/raw" / dataset / f"trade_date={text}" / "part-000.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    checkpoint["partitions"][f"{dataset}/{text}"] = {
        "status": "complete",
        "dataset": dataset,
        "trade_date": text,
        "path": str(path.resolve()),
        "row_count": len(frame),
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
        "completed_at_utc": COMPLETED_AT,
    }


def _calendar(root: Path, checkpoint: dict) -> None:
    dates = pd.date_range(CUTOFF, "2026-08-26", freq="D")
    frame = pd.DataFrame(
        {
            "exchange": "SSE",
            "cal_date": dates,
            "is_open": dates.dayofweek < 5,
            "pretrade_date": pd.NaT,
        }
    )
    previous: pd.Timestamp | None = None
    for index, row in frame.iterrows():
        frame.loc[index, "pretrade_date"] = previous
        if bool(row["is_open"]):
            previous = pd.Timestamp(row["cal_date"])
    records = [
        {
            "cal_date": row.cal_date.date().isoformat(),
            "exchange": "SSE",
            "is_open": bool(row.is_open),
            "pretrade_date": (
                row.pretrade_date.date().isoformat()
                if not pd.isna(row.pretrade_date)
                else None
            ),
        }
        for row in frame.itertuples(index=False)
    ]
    content_sha = hashlib.sha256(_canonical_bytes(records)).hexdigest()
    path = (
        root
        / "runtime/data/raw/trade_cal"
        / f"calendar_sha256={content_sha}"
        / "part-000.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    checkpoint["calendars"][content_sha] = {
        "status": "complete",
        "exchange": "SSE",
        "start_date": CUTOFF.date().isoformat(),
        "end_date": dates[-1].date().isoformat(),
        "row_count": len(frame),
        "open_day_count": int(frame["is_open"].sum()),
        "path": str(path.resolve()),
        "artifact_sha256": sha256_file(path),
        "calendar_content_sha256": content_sha,
        "completed_at_utc": COMPLETED_AT,
    }
    entry = checkpoint["calendars"][content_sha]
    manifest_path = path.with_name("manifest.json")
    _write_json(
        manifest_path,
        {
            "schema_version": 1,
            **entry,
            "records_sha256": content_sha,
        },
    )
    entry["manifest_path"] = str(manifest_path.resolve())
    entry["manifest_sha256"] = sha256_file(manifest_path)


@pytest.fixture()
def data_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, pd.DataFrame, pd.DataFrame]:
    monkeypatch.setattr(prospective, "EXPECTED_MEMBERSHIP_SIZE", len(TICKERS))
    price, basic = _market_history()
    fixture_sessions = sorted(price["trade_date"].dt.strftime("%Y-%m-%d").unique())
    monkeypatch.setattr(prospective, "CANONICAL_CALENDAR_ANCHOR", fixture_sessions[0])
    monkeypatch.setattr(prospective, "CANONICAL_CALENDAR_COUNT", len(fixture_sessions))
    monkeypatch.setattr(
        prospective,
        "CANONICAL_CALENDAR_SHA256",
        prospective._calendar_prefix_sha256(fixture_sessions),
    )
    top500 = tmp_path / "runtime/data/top500"
    top500.mkdir(parents=True)

    history = price.sort_values(["ts_code", "trade_date"]).copy()
    history["return_1d"] = history.groupby("ts_code")["close_hfq"].pct_change(
        fill_method=None
    )
    history["volatility_20"] = history.groupby("ts_code")["return_1d"].transform(
        lambda values: values.rolling(20, min_periods=20).std(ddof=1)
    )
    boundary = history.loc[history["trade_date"].eq(CUTOFF)].merge(
        basic.loc[basic["trade_date"].eq(CUTOFF)],
        on=["ts_code", "trade_date"],
        validate="one_to_one",
    )
    features = pd.DataFrame(
        {
            "ticker": boundary["ts_code"],
            "date": boundary["trade_date"],
            "open_hfq": boundary["open_hfq"],
            "high_hfq": boundary["high_hfq"],
            "low_hfq": boundary["low_hfq"],
            "close_hfq": boundary["close_hfq"],
            "amount_akshare": boundary["amount_akshare"],
            "pe_ttm": boundary["pe_ttm"],
            "pb": boundary["pb"],
            "earnings_yield": 1.0 / boundary["pe_ttm"],
            "book_yield": 1.0 / boundary["pb"],
            "volatility_20": boundary["volatility_20"],
        }
    )
    features.to_parquet(top500 / "features.parquet", index=False)
    pd.DataFrame({"date": pd.to_datetime(fixture_sessions)}).to_parquet(
        top500 / "execution.parquet", index=False
    )
    membership = pd.DataFrame(
        {
            "ts_code": TICKERS,
            "membership_month": "2026-08",
            "as_of_date": pd.Timestamp("2026-07-31"),
            "state_available_date": pd.Timestamp("2026-08-03"),
            "effective_start_date": pd.Timestamp("2026-08-03"),
            "effective_end_date": CUTOFF,
            "eligible": True,
        }
    )
    membership.to_parquet(top500 / "membership.parquet", index=False)

    supplement_root = tmp_path / "runtime/data/raw/supplements"
    supplement_checkpoint: dict[str, dict] = {
        "akshare_hfq": {},
        "tushare_daily_basic": {},
    }
    for ticker in TICKERS:
        for dataset, source in (("akshare_hfq", price), ("tushare_daily_basic", basic)):
            selected = source.loc[source["ts_code"].eq(ticker)].drop(
                columns=["scale_factor"], errors="ignore"
            )
            path = supplement_root / dataset / f"ticker={ticker}" / "history.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            selected.to_parquet(path, index=False)
            supplement_checkpoint[dataset][ticker] = {
                "status": "complete",
                "path": str(path.resolve()),
                "rows": len(selected),
                "sha256": sha256_file(path),
                "start_date": selected["trade_date"].min().date().isoformat(),
                "end_date": selected["trade_date"].max().date().isoformat(),
            }
    _write_json(supplement_root / "supplement_checkpoint.json", supplement_checkpoint)

    checkpoint: dict = {"schema_version": 1, "partitions": {}, "calendars": {}}
    for date in pd.bdate_range(CUTOFF, BRIDGE_END):
        _partition(tmp_path, checkpoint, "daily", date, _raw_daily(price, date))
    future_daily, future_basic, future_adj = _future_frames(price, basic, pd.Timestamp("2026-08-24"), BRIDGE_END)
    _partition(tmp_path, checkpoint, "daily", pd.Timestamp("2026-08-24"), future_daily)
    _partition(tmp_path, checkpoint, "daily_basic", pd.Timestamp("2026-08-24"), future_basic)
    _partition(tmp_path, checkpoint, "adj_factor", pd.Timestamp("2026-08-24"), future_adj)
    _calendar(tmp_path, checkpoint)
    _write_json(tmp_path / "runtime/data/raw/checkpoint.json", checkpoint)
    return tmp_path, price, basic


def test_bridge_snapshot_reproduces_fixed_core_fields(data_root) -> None:
    root, _, _ = data_root
    result = prospective.build_prospective_input_snapshot(root, "2026-08-14")

    assert result.snapshot_sha256 == result.directory.name
    assert result.trade_date == "2026-08-17"
    assert len(result.frame) == len(TICKERS)
    assert result.frame["eligible"].all()
    assert np.allclose(result.frame["earnings_yield"], 1.0 / result.frame["pe_ttm"])
    assert np.allclose(result.frame["book_yield"], 1.0 / result.frame["pb"])
    assert result.manifest["canonical_bridge_calibration"]["volatility_exact"] is True
    assert result.manifest["time_semantics"]["deadline_enforcement"] == "caller_or_prospective_ledger"
    assert result.manifest["rows"]["sha256"] == sha256_file(result.rows_path)
    adapter = result.manifest["target_adapter"]
    encoded_rows = json.loads(result.rows_path.read_text(encoding="utf-8"))
    target_rows = [
        {column: row[column] for column in adapter["columns"]}
        for row in encoded_rows
    ]
    assert adapter["target_rows_sha256"] == hashlib.sha256(
        _canonical_bytes(target_rows)
    ).hexdigest()
    assert adapter["input_sources_sha256"] == hashlib.sha256(
        _canonical_bytes(result.manifest["inputs"])
    ).hexdigest()
    assert adapter["membership_artifact_sha256"] == next(
        row["sha256"] for row in result.manifest["inputs"] if row["role"] == "membership"
    )
    assert adapter["calendar_sessions"][-1] == result.trade_date
    assert result.target_rows_sha256 == adapter["target_rows_sha256"]
    assert result.input_sources_sha256 == adapter["input_sources_sha256"]
    assert result.membership_artifact_sha256 == adapter["membership_artifact_sha256"]
    assert result.calendar_sessions == tuple(adapter["calendar_sessions"])
    assert list(result.target_frame.columns) == adapter["columns"]
    loaded = prospective.load_prospective_input_snapshot(result.directory)
    assert [value.hex() for value in loaded.frame["earnings_yield"]] == [
        value.hex() for value in result.frame["earnings_yield"]
    ]


def test_future_partition_does_not_change_old_snapshot_hash(data_root) -> None:
    root, price, basic = data_root
    first = prospective.build_prospective_input_snapshot(root, "2026-08-24")

    checkpoint_path = root / "runtime/data/raw/checkpoint.json"
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    prior = price.loc[price["trade_date"].eq(BRIDGE_END)].copy()
    prior["trade_date"] = pd.Timestamp("2026-08-24")
    prior["close_hfq"] *= 1.01
    prior["open_hfq"] *= 1.01
    prior["high_hfq"] *= 1.01
    prior["low_hfq"] *= 1.01
    future_daily, future_basic, future_adj = _future_frames(
        prior, basic, pd.Timestamp("2026-08-25"), pd.Timestamp("2026-08-24")
    )
    _partition(root, checkpoint, "daily", pd.Timestamp("2026-08-25"), future_daily)
    _partition(root, checkpoint, "daily_basic", pd.Timestamp("2026-08-25"), future_basic)
    _partition(root, checkpoint, "adj_factor", pd.Timestamp("2026-08-25"), future_adj)
    _write_json(checkpoint_path, checkpoint)

    second = prospective.build_prospective_input_snapshot(root, "2026-08-24")
    assert second.snapshot_sha256 == first.snapshot_sha256
    assert second.rows_path.read_bytes() == first.rows_path.read_bytes()


def test_future_adjusted_prices_follow_adj_factor_across_corporate_action(
    data_root,
) -> None:
    root, _, _ = data_root
    first = prospective.build_prospective_input_snapshot(root, "2026-08-24")
    checkpoint_path = root / "runtime/data/raw/checkpoint.json"
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    prior_daily = pd.read_parquet(
        Path(checkpoint["partitions"]["daily/2026-08-24"]["path"])
    )
    prior_basic = pd.read_parquet(
        Path(checkpoint["partitions"]["daily_basic/2026-08-24"]["path"])
    )
    prior_adj = pd.read_parquet(
        Path(checkpoint["partitions"]["adj_factor/2026-08-24"]["path"])
    )

    next_daily = prior_daily.copy()
    next_daily["trade_date"] = "20260825"
    next_daily["pre_close"] = prior_daily["close"] / 2.0
    next_daily["open"] = next_daily["pre_close"] * 1.002
    next_daily["close"] = next_daily["pre_close"] * 1.01
    next_daily["high"] = next_daily["close"] * 1.002
    next_daily["low"] = next_daily["pre_close"] * 0.998
    next_daily["change"] = next_daily["close"] - next_daily["pre_close"]
    next_daily["pct_chg"] = 1.0
    next_basic = prior_basic.copy()
    next_basic["trade_date"] = "20260825"
    next_adj = prior_adj.copy()
    next_adj["trade_date"] = "20260825"
    next_adj["adj_factor"] = prior_adj["adj_factor"] * 2.0
    for dataset, frame in (
        ("daily", next_daily),
        ("daily_basic", next_basic),
        ("adj_factor", next_adj),
    ):
        _partition(root, checkpoint, dataset, pd.Timestamp("2026-08-25"), frame)
    _write_json(checkpoint_path, checkpoint)

    second = prospective.build_prospective_input_snapshot(root, "2026-08-25")
    first_close = first.frame.set_index("ticker")["close_adj"].sort_index()
    second_close = second.frame.set_index("ticker")["close_adj"].sort_index()
    assert np.allclose(second_close / first_close, 1.01, rtol=0.0, atol=1e-12)
    assert second.frame["price_source"].eq(
        "tushare_raw_times_adj_factor_calibrated_to_bridge"
    ).all()


def test_public_loader_uses_immutable_sources_after_canonical_replacement(data_root) -> None:
    root, _, _ = data_root
    first = prospective.build_prospective_input_snapshot(root, "2026-08-24")
    input_root = root / "runtime/prospective/5.0/inputs"
    before = sorted(path.name for path in input_root.iterdir())
    source_root = root / "runtime/prospective/5.0/source-artifacts"
    frozen_before = sorted(path.as_posix() for path in source_root.rglob("*"))

    checkpoint_path = root / "runtime/data/raw/checkpoint.json"
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    entry = checkpoint["partitions"]["daily/2026-08-24"]
    partition_path = Path(entry["path"])
    changed = pd.read_parquet(partition_path)
    changed.loc[0, "amount"] = float(changed.loc[0, "amount"]) + 1.0
    changed.to_parquet(partition_path, index=False)
    entry["size_bytes"] = partition_path.stat().st_size
    entry["sha256"] = sha256_file(partition_path)
    _write_json(checkpoint_path, checkpoint)

    loaded = prospective.load_prospective_input_snapshot(first.directory)
    assert loaded.snapshot_sha256 == first.snapshot_sha256
    assert loaded.rows_path.read_bytes() == first.rows_path.read_bytes()
    assert sorted(path.name for path in input_root.iterdir()) == before
    assert sorted(path.as_posix() for path in source_root.rglob("*")) == frozen_before


def test_raw_availability_requires_checkpoint_completed_at(data_root) -> None:
    root, _, _ = data_root
    checkpoint_path = root / "runtime/data/raw/checkpoint.json"
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    del checkpoint["partitions"]["daily/2026-08-24"]["completed_at_utc"]
    _write_json(checkpoint_path, checkpoint)

    with pytest.raises(prospective.ProspectiveDataError, match="completed_at_utc"):
        prospective.build_prospective_input_snapshot(root, "2026-08-24")


def test_signal_build_rejects_boolean_raw_checkpoint_schema(data_root) -> None:
    root, _, _ = data_root
    checkpoint_path = root / "runtime/data/raw/checkpoint.json"
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    checkpoint["schema_version"] = True
    _write_json(checkpoint_path, checkpoint)

    with pytest.raises(prospective.ProspectiveDataError, match="checkpoint schema"):
        prospective.build_prospective_input_snapshot(root, "2026-08-24")


def test_later_month_requires_content_addressed_membership(data_root) -> None:
    root, _, _ = data_root
    with pytest.raises(prospective.ProspectiveDataError, match="content-addressed monthly snapshot"):
        prospective._membership_source(
            root,
            pd.Timestamp("2026-08-31"),
            pd.Timestamp("2026-09-01"),
            membership_snapshot_path=None,
            availability_cap=None,
        )


def test_later_month_rejects_self_consistent_but_unverified_membership(data_root) -> None:
    root, _, _ = data_root
    canonical = pd.read_parquet(root / "runtime/data/top500/membership.parquet")
    canonical["membership_month"] = "2026-09"
    canonical["as_of_date"] = pd.Timestamp("2026-08-31")
    canonical["state_available_date"] = pd.Timestamp("2026-09-01")
    canonical["effective_start_date"] = pd.Timestamp("2026-09-01")
    staging = root / "runtime/prospective/5.0/membership/staging.parquet"
    staging.parent.mkdir(parents=True, exist_ok=True)
    canonical.to_parquet(staging, index=False)
    artifact_sha = sha256_file(staging)
    path = (
        root
        / "runtime/prospective/5.0/membership/2026-09"
        / artifact_sha
        / "membership.parquet"
    )
    path.parent.mkdir(parents=True)
    staging.replace(path)
    _write_json(
        path.parent / "manifest.json",
        {
            "schema_version": 1,
            "membership_month": "2026-09",
            "row_count": len(canonical),
            "artifact_sha256": artifact_sha,
            "completed_at_utc": "2026-09-01T01:00:00Z",
        },
    )

    with pytest.raises(
        prospective.ProspectiveDataError,
        match="failed immutable source replay",
    ):
        prospective._membership_source(
            root,
            pd.Timestamp("2026-09-01"),
            pd.Timestamp("2026-09-02"),
            membership_snapshot_path=path,
            availability_cap=pd.Timestamp("2026-09-01T02:00:00Z"),
        )


def test_calendar_can_extend_past_partition_end_without_future_download(tmp_path: Path) -> None:
    config = {
        "schema_version": 1,
        "runtime_root": "runtime",
        "paths": {
            "data": "data",
            "raw": "data/raw",
            "top500": "data/top500",
            "runs": "runs",
            "legacy": "legacy",
        },
        "top500": {
            "features_file": "features.parquet",
            "execution_file": "execution.parquet",
            "membership_file": "membership.parquet",
        },
        "sync": {
            "datasets": ["daily"],
            "checkpoint_file": "checkpoint.json",
            "request_rate_per_minute": 0,
            "verify_hashes_on_resume": True,
        },
    }
    config_path = tmp_path / "configs/data.json"
    _write_json(config_path, config)
    layout = RuntimeLayout.from_config(config, config_path=config_path)

    class Client:
        def __init__(self) -> None:
            self.data_dates: list[str] = []

        def query(self, endpoint: str, **kwargs) -> pd.DataFrame:
            if endpoint == "trade_cal":
                return pd.DataFrame(
                    {
                        "exchange": "SSE",
                        "cal_date": ["20260821", "20260822", "20260823", "20260824"],
                        "is_open": [1, 0, 0, 1],
                        "pretrade_date": ["20260820", "20260821", "20260821", "20260821"],
                    }
                )
            self.data_dates.append(str(kwargs["trade_date"]))
            fields = DATASET_FIELDS[endpoint].split(",")
            row = {field: 1.0 for field in fields}
            row["ts_code"] = "000001.SZ"
            row["trade_date"] = kwargs["trade_date"]
            return pd.DataFrame([row], columns=fields)

    client = Client()
    result = sync_data(
        "2026-08-21",
        "2026-08-21",
        calendar_end_date="2026-08-24",
        config_path=config_path,
        layout=layout,
        client=client,
        datasets=("daily",),
    )
    checkpoint = json.loads(layout.checkpoint_path.read_text(encoding="utf-8"))
    _, next_trade, _, _ = prospective._official_calendar(
        tmp_path,
        start_date=pd.Timestamp("2026-08-21"),
        signal_date=pd.Timestamp("2026-08-21"),
        checkpoint=checkpoint,
        availability_cap=None,
    )

    assert client.data_dates == ["20260821"]
    assert result["partition_request_end_date"] == "2026-08-21"
    assert result["calendar_end_date"] == "2026-08-24"
    assert next_trade == pd.Timestamp("2026-08-24")
    assert "daily/2026-08-24" not in checkpoint["partitions"]


def test_binary64_tokens_round_trip_without_rank_boundary_loss() -> None:
    lower = 0.3
    upper = float(np.nextafter(lower, np.inf))
    records = prospective._frame_records(
        pd.DataFrame({"ticker": ["A", "B"], "score": [lower, upper]})
    )

    assert records[0]["score"] == lower.hex()
    assert records[1]["score"] == upper.hex()
    assert float.fromhex(records[0]["score"]) == lower
    assert float.fromhex(records[1]["score"]) == upper
    assert records[0]["score"] != records[1]["score"]

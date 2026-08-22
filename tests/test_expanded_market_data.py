from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from factor_lab.expanded_market_data import (
    HistoricalSTSnapshot,
    add_adjusted_open_close,
    add_t_plus_1_to_t_plus_6_open_label,
    advance_raw_checkpoint,
    apply_historical_st_filter,
    audit_expanded_market_data,
    audit_raw_partition,
    build_expanded_market_data_plan,
    build_factor_panels,
    build_monthly_top_n_membership,
    build_sha256_manifest,
    calculate_warmup_start_date,
    fetch_daily_raw_partition,
    fetch_historical_st_history,
    fetch_stock_metadata,
    filter_mainland_common_a_shares,
    verify_sha256_manifest,
)


class FixtureTushareClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.stock_rows = {
            "L": [
                {"ts_code": "600001.SH", "symbol": "600001", "name": "沪A", "list_status": "L", "list_date": "20000101", "delist_date": None},
                {"ts_code": "000001.SZ", "symbol": "000001", "name": "深A", "list_status": "L", "list_date": "20000101", "delist_date": None},
                {"ts_code": "900901.SH", "symbol": "900901", "name": "沪B", "list_status": "L", "list_date": "20000101", "delist_date": None},
            ],
            "P": [
                {"ts_code": "301001.SZ", "symbol": "301001", "name": "待上市A", "list_status": "P", "list_date": "20250101", "delist_date": None},
            ],
            "D": [
                {"ts_code": "600002.SH", "symbol": "600002", "name": "退市A", "list_status": "D", "list_date": "20010101", "delist_date": "20231201"},
                {"ts_code": "200001.SZ", "symbol": "200001", "name": "深B", "list_status": "D", "list_date": "20010101", "delist_date": "20231201"},
                {"ts_code": "430001.BJ", "symbol": "430001", "name": "北交所", "list_status": "D", "list_date": "20010101", "delist_date": "20231201"},
            ],
        }

    def stock_basic(self, **kwargs):
        self.calls.append(("stock_basic", kwargs))
        return pd.DataFrame(self.stock_rows[kwargs["list_status"]])

    def daily(self, **kwargs):
        self.calls.append(("daily", kwargs))
        return pd.DataFrame(
            [{"ts_code": "600001.SH", "trade_date": kwargs["trade_date"], "open": 10.0, "close": 10.1}]
        )

    def namechange(self, **kwargs):
        self.calls.append(("namechange", kwargs))
        return pd.DataFrame(
            [
                {
                    "ts_code": "600001.SH",
                    "name": "*ST沪A",
                    "start_date": "20231201",
                    "end_date": "20240110",
                    "change_reason": "ST",
                },
                {
                    "ts_code": "000001.SZ",
                    "name": "正常名称",
                    "start_date": "20230101",
                    "end_date": None,
                    "change_reason": "更名",
                },
            ]
        )


class BrokenNameChangeClient:
    def namechange(self, **kwargs):
        raise RuntimeError("permission denied")


def test_warmup_and_partition_plan_are_deterministic_and_checkpoint_aware(tmp_path: Path):
    calendar = pd.bdate_range("2023-01-02", periods=140)
    analysis_start = calendar[120]
    analysis_end = calendar[125]

    assert calculate_warmup_start_date(calendar, analysis_start) == calendar[0].strftime("%Y-%m-%d")
    initial = build_expanded_market_data_plan(
        calendar,
        analysis_start=analysis_start,
        analysis_end=analysis_end,
        raw_root=tmp_path / "raw",
    )
    assert initial["fetch_start"] == calendar[0].strftime("%Y-%m-%d")
    assert initial["fetch_end"] == calendar[131].strftime("%Y-%m-%d")
    assert initial["partition_count"] == 132 * 3
    first = initial["partitions"][0]
    assert Path(first["path"]) == tmp_path / "raw" / "daily" / f"trade_date={calendar[0]:%Y-%m-%d}" / "part-000.parquet"
    assert first["request"]["trade_date"] == calendar[0].strftime("%Y%m%d")

    checkpoint = advance_raw_checkpoint(
        {},
        first,
        sha256="a" * 64,
        row_count=10,
        size_bytes=123,
        completed_at_utc="2024-01-01T00:00:00Z",
    )
    replanned = build_expanded_market_data_plan(
        calendar,
        analysis_start=analysis_start,
        analysis_end=analysis_end,
        raw_root=tmp_path / "raw",
        checkpoint=checkpoint,
    )
    assert replanned["partitions"][0]["status"] == "complete"
    assert replanned["pending_partition_count"] == replanned["partition_count"] - 1
    assert initial["partitions"][0]["status"] == "pending"  # advancing is pure


def test_warmup_rejects_silent_short_history():
    calendar = pd.bdate_range("2024-01-01", periods=120)
    with pytest.raises(ValueError, match="sessions before"):
        calculate_warmup_start_date(calendar, calendar[-1], warmup_sessions=120)


def test_injected_client_fetches_l_p_d_metadata_and_daily_partition(tmp_path: Path):
    client = FixtureTushareClient()
    metadata = fetch_stock_metadata(client)
    assert set(metadata["queried_list_status"]) == {"L", "P", "D"}
    assert [call[1]["list_status"] for call in client.calls[:3]] == ["L", "P", "D"]

    ordinary = filter_mainland_common_a_shares(metadata)
    assert set(ordinary["ts_code"]) == {"600001.SH", "000001.SZ", "301001.SZ", "600002.SH"}

    plan = build_expanded_market_data_plan(
        pd.bdate_range("2023-01-02", periods=130),
        analysis_start=pd.Timestamp("2023-06-19"),
        analysis_end=pd.Timestamp("2023-06-19"),
        raw_root=tmp_path,
        warmup_sessions=120,
        forward_label_sessions=1,
        datasets=["daily"],
    )["partitions"][-1]
    daily = fetch_daily_raw_partition(client, plan)
    assert daily.iloc[0]["trade_date"] == plan["request"]["trade_date"]
    assert client.calls[-1][0] == "daily"


def test_historical_st_fetch_can_degrade_without_network_or_permission():
    client = FixtureTushareClient()
    snapshot = fetch_historical_st_history(
        client, start_date="2023-01-01", end_date="2024-01-31"
    )
    assert snapshot.available is True
    assert snapshot.records["name"].tolist() == ["*ST沪A"]

    degraded = fetch_historical_st_history(
        BrokenNameChangeClient(),
        start_date="2023-01-01",
        end_date="2024-01-31",
        allow_degraded=True,
    )
    assert degraded.available is False
    assert degraded.degraded is True
    assert "permission denied" in (degraded.reason or "")
    with pytest.raises(RuntimeError, match="permission denied"):
        fetch_historical_st_history(
            BrokenNameChangeClient(),
            start_date="2023-01-01",
            end_date="2024-01-31",
            allow_degraded=False,
        )


def _monthly_fixture():
    calendar = pd.bdate_range("2023-09-01", "2024-01-31")
    codes = ["600001.SH", "000001.SZ", "300001.SZ", "600002.SH", "300002.SZ"]
    base_amount = {
        "600001.SH": 100.0,
        "000001.SZ": 200.0,
        "300001.SZ": 300.0,
        "600002.SH": 400.0,
        "300002.SZ": 500.0,
    }
    rows = []
    for date in calendar:
        for code in codes:
            amount = base_amount[code]
            if date.month == 1 and code == "600001.SH":
                amount = 10000.0  # must not leak into January membership
            rows.append({"ts_code": code, "trade_date": date, "amount": amount})
    metadata = pd.DataFrame(
        [
            {"ts_code": "600001.SH", "symbol": "600001", "list_status": "L", "list_date": "20000101", "delist_date": None},
            {"ts_code": "000001.SZ", "symbol": "000001", "list_status": "L", "list_date": "20000101", "delist_date": None},
            {"ts_code": "300001.SZ", "symbol": "300001", "list_status": "P", "list_date": "20240115", "delist_date": None},
            {"ts_code": "600002.SH", "symbol": "600002", "list_status": "D", "list_date": "20000101", "delist_date": "20231201"},
            {"ts_code": "300002.SZ", "symbol": "300002", "list_status": "L", "list_date": "20231001", "delist_date": None},
        ]
    )
    st = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "start_date": "20231201",
                "end_date": "20240110",
            }
        ]
    )
    return calendar, pd.DataFrame(rows), metadata, st


def test_monthly_top_n_uses_prior_month_end_liquidity_and_pit_metadata():
    calendar, amounts, metadata, st = _monthly_fixture()
    result = build_monthly_top_n_membership(
        amounts,
        metadata,
        calendar,
        start_date="2024-01-01",
        end_date="2024-01-31",
        top_n=2,
        lookback_sessions=60,
        historical_st=st,
    )
    membership = result.membership
    assert membership["ts_code"].tolist() == ["600001.SH"]
    assert membership.iloc[0]["as_of_date"] == pd.Timestamp("2023-12-29")
    assert membership.iloc[0]["liquidity_window_end"] == pd.Timestamp("2023-12-29")
    assert membership.iloc[0]["median_amount_60d"] == 100.0
    assert "300001.SZ" not in set(membership["ts_code"])  # not listed as of prior month-end
    assert "600002.SH" not in set(membership["ts_code"])  # already delisted
    assert "300002.SZ" not in set(membership["ts_code"])  # listed for fewer than 180 days
    assert result.audit["min_listing_days"] == 180
    assert result.audit["months"][0]["excluded_recent_listing_count"] == 1
    assert result.audit["months"][0]["st_filter"]["excluded_count"] == 1


def test_monthly_membership_marks_optional_st_degradation():
    calendar, amounts, metadata, _ = _monthly_fixture()
    result = build_monthly_top_n_membership(
        amounts,
        metadata,
        calendar,
        start_date="2024-01-01",
        end_date="2024-01-31",
        top_n=2,
        historical_st=None,
        allow_st_degraded=True,
    )
    assert result.membership["ts_code"].tolist() == ["000001.SZ", "600001.SH"]
    assert set(result.membership["st_filter_status"]) == {"degraded_unavailable"}
    assert result.audit["degraded_st_month_count"] == 1

    candidates = pd.DataFrame([{"ts_code": "600001.SH", "as_of_date": "2023-12-29"}])
    unavailable = HistoricalSTSnapshot(pd.DataFrame(), available=False, degraded=True, reason="missing")
    with pytest.raises(RuntimeError, match="missing"):
        apply_historical_st_filter(candidates, unavailable, allow_degraded=False)


def test_monthly_membership_can_override_min_listing_days_for_research():
    calendar, amounts, metadata, st = _monthly_fixture()
    result = build_monthly_top_n_membership(
        amounts,
        metadata,
        calendar,
        start_date="2024-01-01",
        end_date="2024-01-31",
        top_n=2,
        min_listing_days=0,
        historical_st=st,
    )
    assert result.membership["ts_code"].tolist() == ["300002.SZ", "600001.SH"]
    assert result.audit["months"][0]["excluded_recent_listing_count"] == 0


def test_adjusted_prices_and_t_plus_1_to_t_plus_6_open_label_use_market_calendar():
    calendar = pd.bdate_range("2024-01-02", periods=7)
    daily = pd.DataFrame(
        {
            "ts_code": ["600001.SH"] * 7,
            "trade_date": calendar,
            "open": [10.0, 10.0, 5.1, 5.2, 5.3, 5.4, 6.0],
            "close": [10.1, 10.1, 5.2, 5.3, 5.4, 5.5, 6.1],
        }
    )
    adj = pd.DataFrame(
        {
            "ts_code": ["600001.SH"] * 7,
            "trade_date": calendar,
            "adj_factor": [1.0, 1.0, 2.0, np.nan, 2.0, 2.0, 2.0],
        }
    )
    adjusted = add_adjusted_open_close(daily, adj)
    assert adjusted.loc[2, "open_adj"] == pytest.approx(10.2)
    assert adjusted.loc[2, "close_adj"] == pytest.approx(10.4)
    assert pd.isna(adjusted.loc[3, "open_adj"])

    labeled = add_t_plus_1_to_t_plus_6_open_label(adjusted, calendar)
    assert labeled.loc[0, "label_entry_date"] == calendar[1]
    assert labeled.loc[0, "label_exit_date"] == calendar[6]
    assert labeled.loc[0, "forward_return_5d_open"] == pytest.approx(0.2)
    assert pd.isna(labeled.loc[1, "forward_return_5d_open"])


def test_factor_missingness_is_filtered_independently_and_audited():
    frame = pd.DataFrame(
        {
            "ts_code": ["A", "B", "C"],
            "trade_date": pd.to_datetime(["2024-01-02", "2024-01-02", "2024-01-02"]),
            "factor_a": [1.0, np.nan, 3.0],
            "factor_b": [np.nan, 2.0, 4.0],
            "forward_return_5d_open": [0.1, 0.2, np.nan],
        }
    )
    panels = build_factor_panels(frame, ["factor_a", "factor_b"])
    assert panels["factor_a"]["ts_code"].tolist() == ["A"]
    assert panels["factor_b"]["ts_code"].tolist() == ["B"]

    audit = audit_expanded_market_data(frame, factor_columns=["factor_a", "factor_b"])
    assert audit["factor_coverage"]["factor_a"]["coverage"] == pytest.approx(2 / 3, abs=1e-6)
    assert audit["factor_coverage"]["factor_b"]["coverage"] == pytest.approx(2 / 3, abs=1e-6)
    assert audit["factor_coverage"]["factor_a"]["usable_with_label_count"] == 1
    assert audit["factor_coverage"]["factor_b"]["usable_with_label_count"] == 1
    assert audit["label"]["coverage"] == pytest.approx(2 / 3, abs=1e-6)


def test_audits_detect_partition_and_pit_violations():
    partition = {
        "dataset": "daily",
        "trade_date": "2024-01-02",
        "key": "daily/2024-01-02",
        "path": "raw/daily/trade_date=2024-01-02/part-000.parquet",
    }
    raw = pd.DataFrame(
        [
            {"ts_code": "A", "trade_date": "2024-01-02"},
            {"ts_code": "A", "trade_date": "2024-01-02"},
            {"ts_code": "B", "trade_date": "2024-01-03"},
        ]
    )
    raw_audit = audit_raw_partition(raw, partition)
    assert raw_audit["status"] == "fail"
    assert raw_audit["duplicate_key_count"] == 1
    assert raw_audit["date_mismatch_count"] == 1

    expanded = raw.assign(
        as_of_date="2024-01-02",
        effective_start_date="2024-01-02",
        liquidity_window_end="2024-01-03",
        forward_return_5d_open=0.1,
    )
    expanded_audit = audit_expanded_market_data(expanded)
    assert expanded_audit["status"] == "fail"
    assert expanded_audit["pit_violation_count"] == 3
    assert expanded_audit["liquidity_future_leak_count"] == 3


def test_sha256_manifest_is_deterministic_and_verifiable(tmp_path: Path):
    first = tmp_path / "raw" / "daily" / "a.parquet"
    second = tmp_path / "raw" / "adj_factor" / "b.parquet"
    first.parent.mkdir(parents=True)
    second.parent.mkdir(parents=True)
    first.write_bytes(b"daily fixture")
    second.write_bytes(b"adj fixture")

    manifest = build_sha256_manifest([tmp_path / "raw"], base_dir=tmp_path)
    assert manifest["file_count"] == 2
    assert [row["path"] for row in manifest["files"]] == [
        "raw/adj_factor/b.parquet",
        "raw/daily/a.parquet",
    ]
    assert verify_sha256_manifest(manifest, base_dir=tmp_path)["valid"] is True

    first.write_bytes(b"changed")
    verification = verify_sha256_manifest(manifest, base_dir=tmp_path)
    assert verification["valid"] is False
    assert verification["mismatches"][0]["path"] == "raw/daily/a.parquet"

    tampered = {**manifest, "files": [dict(row) for row in manifest["files"]]}
    tampered["files"][0]["size_bytes"] += 1
    assert verify_sha256_manifest(tampered, base_dir=tmp_path)["manifest_digest_valid"] is False

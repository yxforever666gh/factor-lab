from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import factor_lab.data.analyst as analyst
from factor_lab.data.analyst import (
    REPORT_RC_FIELDS,
    assign_report_availability,
    build_analyst_revision_features,
    normalize_analyst_reports,
    sync_analyst_reports,
)


def _raw_row(
    *,
    ticker: str = "600519.SH",
    report_date: str = "20240430",
    title: str = "业绩点评",
    broker: str = "测试证券",
    author: str = "研究员甲",
    quarter: str = "2024Q4",
    eps: float = 2.0,
    net_profit: float = 20.0,
    create_time: str = "2024-04-30 21:00:00",
) -> dict[str, object]:
    return {
        "ts_code": ticker,
        "name": "样本公司",
        "report_date": report_date,
        "report_title": title,
        "report_type": "公司点评",
        "classify": "一般报告",
        "org_name": broker,
        "author_name": author,
        "quarter": quarter,
        "op_rt": 100.0,
        "op_pr": 30.0,
        "tp": 25.0,
        "np": net_profit,
        "eps": eps,
        "rating": "买入",
        "create_time": create_time,
    }


def _calendar(start: str = "2024-01-02", periods: int = 90) -> pd.DataFrame:
    sessions = pd.bdate_range(start, periods=periods)
    return pd.DataFrame({"cal_date": sessions, "is_open": 1})


def test_normalize_is_order_stable_and_deduplicates_exact_rows() -> None:
    first = _raw_row()
    second = _raw_row(
        ticker="000001.SZ",
        title="另一份报告",
        broker="另一证券",
        eps=1.0,
    )
    forward = normalize_analyst_reports(
        pd.DataFrame([first, second, first]), expected_report_date="2024-04-30"
    )
    reverse = normalize_analyst_reports(
        pd.DataFrame([second, first]), expected_report_date="2024-04-30"
    )

    pd.testing.assert_frame_equal(forward, reverse)
    assert list(forward.columns) == [*REPORT_RC_FIELDS, "source_row_sha256"]
    assert len(forward) == 2
    assert forward["source_row_sha256"].str.fullmatch(r"[0-9a-f]{64}").all()


def test_normalize_rejects_conflicting_same_report_identity() -> None:
    first = _raw_row(eps=2.0)
    conflicting = _raw_row(eps=2.1)
    with pytest.raises(ValueError, match="conflicting duplicate identities"):
        normalize_analyst_reports(pd.DataFrame([first, conflicting]))


def test_availability_is_next_open_and_never_uses_create_time() -> None:
    reports = normalize_analyst_reports(
        pd.DataFrame(
            [
                _raw_row(
                    ticker="600519.SH",
                    report_date="20240426",
                    create_time="2030-01-01 00:00:00",
                ),
                _raw_row(
                    ticker="000001.SZ",
                    report_date="20240426",
                    title="不同报告",
                    broker="另一证券",
                    create_time="2010-01-01 00:00:00",
                ),
            ]
        )
    )
    calendar = pd.DataFrame(
        {
            "cal_date": ["20240426", "20240427", "20240428", "20240429"],
            "is_open": [1, 0, 0, 1],
        }
    )

    available = assign_report_availability(reports, calendar)

    assert available["available_session"].eq(pd.Timestamp("2024-04-29")).all()
    assert available["create_time"].min() == pd.Timestamp("2010-01-01")
    assert available["create_time"].max() == pd.Timestamp("2030-01-01")


def _feature_row(
    ticker: str,
    broker: str,
    available: pd.Timestamp,
    eps: float | None,
    net_profit: float | None,
    identity: int,
) -> dict[str, object]:
    return {
        "ts_code": ticker,
        "quarter": "2024Q4",
        "org_name": broker,
        "report_date": available - pd.Timedelta(days=1),
        "available_session": available,
        "source_row_sha256": f"{identity:064x}",
        "eps": eps,
        "np": net_profit,
    }


def test_features_use_latest_broker_state_and_exact_20_60_session_lookbacks() -> None:
    calendar = _calendar()
    sessions = pd.DatetimeIndex(calendar["cal_date"])
    rows: list[dict[str, object]] = []
    identity = 1
    for available, eps_values, np_values in (
        (sessions[10], (1.0, 2.0, 3.0), (10.0, 20.0, 30.0)),
        (sessions[50], (1.1, 2.1, 3.1), (11.0, 21.0, 31.0)),
        (sessions[70], (1.2, 2.2, 3.2), (12.0, 22.0, 32.0)),
    ):
        for broker, eps, net_profit in zip(("A", "B", "C"), eps_values, np_values):
            rows.append(_feature_row("600519.SH", broker, available, eps, net_profit, identity))
            identity += 1

    features = build_analyst_revision_features(
        pd.DataFrame(rows),
        calendar,
        signal_sessions=[sessions[70]],
    )

    assert len(features) == 1
    row = features.iloc[0]
    assert row["eps_broker_count"] == 3
    assert row["eps_paired_brokers_20d"] == 3
    assert row["eps_consensus_revision_20d"] == pytest.approx(2 * 0.1 / 4.3)
    assert row["eps_consensus_revision_60d"] == pytest.approx(2 * 0.2 / 4.2)
    assert row["np_consensus_revision_20d"] == pytest.approx(2 * 1.0 / 43.0)
    assert row["np_consensus_revision_60d"] == pytest.approx(2 * 2.0 / 42.0)
    assert row["eps_revision_breadth_20d"] == 1.0
    assert row["np_revision_breadth_60d"] == 1.0
    assert row["fiscal_horizon"] == "FY0"


def test_same_day_broker_reports_use_median_not_lineage_order() -> None:
    calendar = _calendar()
    sessions = pd.DatetimeIndex(calendar["cal_date"])
    rows: list[dict[str, object]] = []
    identity = 1
    for broker, old in zip(("A", "B", "C"), (1.0, 2.0, 3.0)):
        rows.append(_feature_row("600519.SH", broker, sessions[10], old, old * 10, identity))
        identity += 1
    for broker, new in zip(("A", "B", "C"), (2.0, 3.0, 4.0)):
        rows.append(_feature_row("600519.SH", broker, sessions[30], new, new * 10, identity))
        identity += 1
    duplicate_a = _feature_row(
        "600519.SH", "A", sessions[30], 4.0, 40.0, identity
    )
    rows.append(duplicate_a)

    first = build_analyst_revision_features(
        pd.DataFrame(rows), calendar, signal_sessions=[sessions[30]], horizons=(20,)
    )
    changed_lineage = pd.DataFrame(rows)
    changed_lineage.loc[changed_lineage.index[-1], "source_row_sha256"] = "f" * 64
    second = build_analyst_revision_features(
        changed_lineage, calendar, signal_sessions=[sessions[30]], horizons=(20,)
    )

    pd.testing.assert_frame_equal(first, second)
    assert first.iloc[0]["eps_consensus_revision_20d"] == pytest.approx(
        2 * (3.0 - 2.0) / 5.0
    )


def test_insufficient_or_missing_brokers_stay_nan_not_zero() -> None:
    calendar = _calendar()
    sessions = pd.DatetimeIndex(calendar["cal_date"])
    rows: list[dict[str, object]] = []
    identity = 1
    for ticker, brokers in (("000001.SZ", ("A", "B")), ("600000.SH", ("A", "B", "C"))):
        for available, multiplier in ((sessions[10], 1.0), (sessions[30], 1.1)):
            for index, broker in enumerate(brokers, start=1):
                eps = None if ticker == "600000.SH" and broker == "C" else index * multiplier
                rows.append(
                    _feature_row(
                        ticker,
                        broker,
                        available,
                        eps,
                        index * 10.0 * multiplier,
                        identity,
                    )
                )
                identity += 1

    features = build_analyst_revision_features(
        pd.DataFrame(rows),
        calendar,
        signal_sessions=[sessions[30]],
        horizons=(20,),
    ).set_index("ts_code")

    assert features.loc["000001.SZ", "eps_paired_brokers_20d"] == 2
    assert np.isnan(features.loc["000001.SZ", "eps_consensus_revision_20d"])
    assert np.isnan(features.loc["000001.SZ", "eps_revision_breadth_20d"])
    assert features.loc["600000.SH", "eps_paired_brokers_20d"] == 2
    assert np.isnan(features.loc["600000.SH", "eps_consensus_revision_20d"])
    assert features.loc["600000.SH", "np_paired_brokers_20d"] == 3
    assert not np.isnan(features.loc["600000.SH", "np_consensus_revision_20d"])


def test_features_reject_any_price_return_or_label_column() -> None:
    frame = pd.DataFrame(
        [
            {
                **_feature_row("600519.SH", "A", pd.Timestamp("2024-02-01"), 1.0, 2.0, 1),
                "forward_return_5d": 99.0,
            }
        ]
    )
    with pytest.raises(ValueError, match="reject price/return/label"):
        build_analyst_revision_features(frame, _calendar())


@pytest.mark.parametrize("availability_offset", [0, 2])
def test_features_reject_noncanonical_availability(
    availability_offset: int,
) -> None:
    calendar = _calendar()
    sessions = pd.DatetimeIndex(calendar["cal_date"])
    row = _feature_row(
        "600519.SH",
        "A",
        sessions[20] + pd.offsets.BDay(availability_offset),
        1.0,
        2.0,
        1,
    )
    row["report_date"] = sessions[20]

    with pytest.raises(
        ValueError,
        match="first official open session strictly after report_date",
    ):
        build_analyst_revision_features(
            pd.DataFrame([row]),
            calendar,
            signal_sessions=[sessions[25]],
            horizons=(5,),
        )


def test_features_drop_expired_and_nonannual_target_quarters() -> None:
    calendar = _calendar()
    sessions = pd.DatetimeIndex(calendar["cal_date"])
    rows = [
        {
            **_feature_row("600519.SH", "A", sessions[20], 1.0, 2.0, index),
            "quarter": quarter,
        }
        for index, quarter in enumerate(
            ("2019Q4", "2024Q2", "2024Q4", "2025Q4", "2026Q4"), start=1
        )
    ]

    features = build_analyst_revision_features(
        pd.DataFrame(rows),
        calendar,
        signal_sessions=[sessions[20]],
        horizons=(5,),
    )

    assert features[["quarter", "fiscal_horizon"]].to_dict("records") == [
        {"quarter": "2024Q4", "fiscal_horizon": "FY0"},
        {"quarter": "2025Q4", "fiscal_horizon": "FY1"},
    ]


def _config(tmp_path: Path, *, token_file: str = "secrets/tushare_token") -> Path:
    payload = {
        "schema_version": 2,
        "runtime_root": "runtime",
        "paths": {"data": "data", "raw": "data/raw", "top500": "data/top500", "runs": "runs", "legacy": "legacy"},
        "top500": {"features_file": "features.parquet", "execution_file": "execution.parquet", "membership_file": "membership.parquet"},
        "sync": {"token_env": "TUSHARE_TOKEN", "token_file": token_file},
    }
    path = tmp_path / "configs/data.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


class FixtureClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    def query(self, endpoint: str, **kwargs: object) -> pd.DataFrame:
        self.calls.append((endpoint, kwargs))
        return pd.DataFrame([_raw_row(report_date=str(kwargs["report_date"]))])


def _page(row_count: int, *, start: int = 0) -> pd.DataFrame:
    return pd.DataFrame(
        [
            _raw_row(
                title=f"报告-{value:05d}",
                author=f"研究员-{value:05d}",
            )
            for value in range(start, start + row_count)
        ]
    )


def test_sync_atomically_publishes_and_resumes_verified_date_partition(tmp_path: Path) -> None:
    config = _config(tmp_path)
    raw_root = tmp_path / "raw"
    client = FixtureClient()
    first = sync_analyst_reports(
        "2024-04-30",
        "2024-04-30",
        config_path=config,
        raw_root=raw_root,
        client=client,
        request_rate_per_minute=0,
    )

    directory = raw_root / "report_rc/report_date=2024-04-30"
    assert first["status"] == "complete"
    assert (directory / "part-000.parquet").is_file()
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["row_count"] == 1
    assert manifest["fields"] == list(REPORT_RC_FIELDS)
    assert manifest["page_limit"] == 3_000
    assert manifest["page_count"] == 1
    assert manifest["provider_row_count"] == 1
    assert manifest["schema_version"] == analyst.REPORT_RC_PARTITION_SCHEMA
    assert manifest["pages"][0]["normalized_row_count"] == 1
    assert len(manifest["pages"][0]["file_sha256"]) == 64
    assert (directory / manifest["pages"][0]["path"]).is_file()
    assert client.calls[0][1]["limit"] == 3_000
    assert client.calls[0][1]["offset"] == 0
    assert not list((raw_root / "report_rc").glob("*.tmp"))

    class BombClient:
        def query(self, *_args: object, **_kwargs: object) -> pd.DataFrame:
            raise AssertionError("verified resume must not query")

    second = sync_analyst_reports(
        "2024-04-30",
        "2024-04-30",
        config_path=config,
        raw_root=raw_root,
        client=BombClient(),
        request_rate_per_minute=0,
    )
    assert second["completed_before"] == 1
    assert second["completed_this_run"] == 0


def test_sync_paginates_full_page_then_publishes_short_tail(tmp_path: Path) -> None:
    config = _config(tmp_path)

    class PagedClient:
        def __init__(self) -> None:
            self.offsets: list[int] = []

        def query(self, *_args: object, **kwargs: object) -> pd.DataFrame:
            offset = int(kwargs["offset"])
            self.offsets.append(offset)
            return _page(3_000) if offset == 0 else _page(2, start=3_000)

    client = PagedClient()
    raw_root = tmp_path / "raw"
    result = sync_analyst_reports(
        "2024-04-30",
        "2024-04-30",
        config_path=config,
        raw_root=raw_root,
        client=client,
        request_rate_per_minute=0,
    )

    assert result["status"] == "complete"
    assert client.offsets == [0, 3_000]
    directory = raw_root / "report_rc/report_date=2024-04-30"
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["page_count"] == 2
    assert manifest["provider_row_count"] == 3_002
    assert manifest["row_count"] == 3_002
    assert [page["row_count"] for page in manifest["pages"]] == [3_000, 2]
    assert all(len(page["content_sha256"]) == 64 for page in manifest["pages"])


def test_sync_exact_full_page_requires_and_records_empty_tail(tmp_path: Path) -> None:
    config = _config(tmp_path)

    class FullThenEmptyClient:
        def query(self, *_args: object, **kwargs: object) -> pd.DataFrame:
            return _page(3_000) if int(kwargs["offset"]) == 0 else pd.DataFrame()

    raw_root = tmp_path / "raw"
    result = sync_analyst_reports(
        "2024-04-30",
        "2024-04-30",
        config_path=config,
        raw_root=raw_root,
        client=FullThenEmptyClient(),
        request_rate_per_minute=0,
    )

    assert result["status"] == "complete"
    manifest = json.loads(
        (raw_root / "report_rc/report_date=2024-04-30/manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest["page_count"] == 2
    assert [page["row_count"] for page in manifest["pages"]] == [3_000, 0]
    assert manifest["provider_row_count"] == manifest["row_count"] == 3_000


def test_sync_blocks_when_provider_ignores_offset_and_repeats_page(tmp_path: Path) -> None:
    config = _config(tmp_path)
    repeated = _page(3_000)

    class OffsetIgnoringClient:
        def query(self, *_args: object, **_kwargs: object) -> pd.DataFrame:
            return repeated.copy()

    raw_root = tmp_path / "raw"
    result = sync_analyst_reports(
        "2024-04-30",
        "2024-04-30",
        config_path=config,
        raw_root=raw_root,
        client=OffsetIgnoringClient(),
        request_rate_per_minute=0,
    )

    assert result["status"] == "blocked"
    assert result["reason"] == "provider_repeated_page"
    assert result["offset"] == 3_000
    assert not (raw_root / "report_rc/report_date=2024-04-30").exists()


def test_sync_blocks_partial_cross_page_overlap(tmp_path: Path) -> None:
    config = _config(tmp_path)
    first_page = _page(3_000)

    class OverlappingClient:
        def query(self, *_args: object, **kwargs: object) -> pd.DataFrame:
            if int(kwargs["offset"]) == 0:
                return first_page.copy()
            return pd.concat([first_page.iloc[[-1]], _page(1, start=3_000)], ignore_index=True)

    raw_root = tmp_path / "raw"
    result = sync_analyst_reports(
        "2024-04-30",
        "2024-04-30",
        config_path=config,
        raw_root=raw_root,
        client=OverlappingClient(),
        request_rate_per_minute=0,
    )

    assert result["status"] == "blocked"
    assert result["reason"] == "provider_cross_page_duplicate"
    assert not (raw_root / "report_rc/report_date=2024-04-30").exists()


def test_sync_second_page_failure_never_publishes_partial_date(tmp_path: Path) -> None:
    config = _config(tmp_path)

    class FailingSecondPageClient:
        def query(self, *_args: object, **kwargs: object) -> pd.DataFrame:
            if int(kwargs["offset"]) == 0:
                return _page(3_000)
            raise RuntimeError("second page unavailable")

    raw_root = tmp_path / "raw"
    result = sync_analyst_reports(
        "2024-04-30",
        "2024-04-30",
        config_path=config,
        raw_root=raw_root,
        client=FailingSecondPageClient(),
        request_rate_per_minute=0,
    )

    assert result["status"] == "blocked"
    assert result["reason"] == "provider_request_failed"
    assert result["page_index"] == 1
    assert not (raw_root / "report_rc/report_date=2024-04-30").exists()


def test_sync_blocks_page_larger_than_documented_limit(tmp_path: Path) -> None:
    config = _config(tmp_path)

    class OversizedClient:
        def query(self, *_args: object, **_kwargs: object) -> pd.DataFrame:
            return pd.DataFrame(index=range(5_000))

    result = sync_analyst_reports(
        "2024-04-30",
        "2024-04-30",
        config_path=config,
        raw_root=tmp_path / "raw",
        client=OversizedClient(),
        request_rate_per_minute=0,
    )

    assert result["status"] == "blocked"
    assert result["reason"] == "provider_page_exceeds_documented_limit"
    assert result["row_count"] == 5_000
    assert not (tmp_path / "raw/report_rc/report_date=2024-04-30").exists()


def test_sync_rejects_max_pages_above_frozen_limit(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="between 1 and 100"):
        sync_analyst_reports(
            "2024-04-30",
            "2024-04-30",
            config_path=_config(tmp_path),
            raw_root=tmp_path / "raw",
            client=FixtureClient(),
            request_rate_per_minute=0,
            max_pages_per_date=101,
        )


def test_sync_never_publishes_current_shanghai_date(tmp_path: Path) -> None:
    config = _config(tmp_path)
    client = FixtureClient()
    result = sync_analyst_reports(
        "2024-04-30",
        "2024-04-30",
        config_path=config,
        raw_root=tmp_path / "raw",
        client=client,
        request_rate_per_minute=0,
        now_utc_fn=lambda: datetime(2024, 4, 30, 0, tzinfo=timezone.utc),
    )

    assert result["status"] == "blocked"
    assert result["reason"] == "report_date_not_mature"
    assert client.calls == []
    assert not (tmp_path / "raw/report_rc/report_date=2024-04-30").exists()


@pytest.mark.parametrize("tamper", ["endpoint", "fields", "page_hash"])
def test_resume_recomputes_manifest_identity_and_page_hashes(
    tmp_path: Path,
    tamper: str,
) -> None:
    config = _config(tmp_path)
    raw_root = tmp_path / "raw"
    first = sync_analyst_reports(
        "2024-04-30",
        "2024-04-30",
        config_path=config,
        raw_root=raw_root,
        client=FixtureClient(),
        request_rate_per_minute=0,
    )
    assert first["status"] == "complete"
    directory = raw_root / "report_rc/report_date=2024-04-30"
    manifest_path = directory / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if tamper == "endpoint":
        manifest["endpoint"] = "wrong"
    elif tamper == "fields":
        manifest["fields"] = []
    else:
        manifest["pages"][0]["content_sha256"] = "0" * 64
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    assert not analyst._valid_partition(directory, "2024-04-30")


def test_provider_errors_never_echo_secret_message(tmp_path: Path) -> None:
    config = _config(tmp_path)

    class FailingClient:
        def query(self, *_args: object, **_kwargs: object) -> pd.DataFrame:
            raise RuntimeError("super-secret-token")

    result = sync_analyst_reports(
        "2024-04-30",
        "2024-04-30",
        config_path=config,
        raw_root=tmp_path / "raw",
        client=FailingClient(),
        request_rate_per_minute=0,
    )
    assert result["reason"] == "provider_request_failed"
    assert "super-secret-token" not in json.dumps(result)


def test_configured_token_is_consumed_but_never_persisted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secret = "local-secret-value"
    config = _config(tmp_path)
    token_path = tmp_path / "secrets/tushare_token"
    token_path.parent.mkdir()
    token_path.write_text(secret, encoding="utf-8")
    captured: dict[str, object] = {}

    class ConfiguredClient(FixtureClient):
        def __init__(self, token: str | None, *, token_env: str) -> None:
            super().__init__()
            captured.update(token=token, token_env=token_env)

    monkeypatch.setattr(analyst, "TushareClient", ConfiguredClient)
    raw_root = tmp_path / "raw"
    result = sync_analyst_reports(
        "2024-04-30",
        "2024-04-30",
        config_path=config,
        raw_root=raw_root,
        request_rate_per_minute=0,
    )

    assert result["status"] == "complete"
    assert captured == {"token": secret, "token_env": "TUSHARE_TOKEN"}
    persisted = "".join(
        path.read_text(encoding="utf-8")
        for path in raw_root.rglob("*.json")
    )
    assert secret not in persisted

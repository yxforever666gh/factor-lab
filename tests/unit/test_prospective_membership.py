from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import threading
import time
from typing import Any

import pandas as pd
import pytest

from factor_lab.data.catalog import sha256_file
from factor_lab.data import prospective, prospective_membership
import factor_lab.data.sources as sources
from factor_lab.data.prospective import ProspectiveDataError, _membership_source
from factor_lab.data.prospective_membership import (
    EXPECTED_MEMBERSHIP_SIZE,
    LIQUIDITY_SESSION_COUNT,
    MINIMUM_LIQUIDITY_OBSERVATIONS,
    ProspectiveMembershipError,
    build_prospective_membership_snapshot,
    load_prospective_membership_snapshot,
)
from factor_lab.data.sources import (
    DATASET_FIELDS,
    ENRICHMENT_DATASET_FIELDS,
    EXACT_REFERENCE_CONTRACT_ID,
)


AVAILABLE_AT = "2026-08-31T16:00:00Z"


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


class FakeClient:
    def __init__(self, reference: pd.DataFrame) -> None:
        self.reference = reference
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def query(self, endpoint: str, **kwargs: Any) -> pd.DataFrame:
        self.calls.append((endpoint, dict(kwargs)))
        assert endpoint == "bak_basic"
        return self.reference.copy()


def _write_config(root: Path) -> None:
    path = root / "configs/data.json"
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "schema_version": 2,
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
                    "token_env": "TUSHARE_TOKEN",
                    "checkpoint_file": "checkpoint.json",
                },
            }
        ),
        encoding="utf-8",
    )


def _calendar_artifact(
    root: Path, *, start_date: str = "2026-05-01"
) -> tuple[dict[str, Any], list[pd.Timestamp]]:
    all_dates = pd.date_range(start_date, "2026-09-30", freq="D")
    frame = pd.DataFrame(
        {
            "exchange": "SSE",
            "cal_date": all_dates,
            "is_open": all_dates.weekday < 5,
            "pretrade_date": pd.NaT,
        }
    )
    records = [
        {
            "cal_date": row.cal_date.date().isoformat(),
            "exchange": "SSE",
            "is_open": bool(row.is_open),
            "pretrade_date": None,
        }
        for row in frame.itertuples(index=False)
    ]
    content_sha = hashlib.sha256(_canonical_bytes(records)).hexdigest()
    directory = root / "runtime/data/raw/trade_cal" / f"calendar_sha256={content_sha}"
    directory.mkdir(parents=True)
    path = directory / "part-000.parquet"
    frame.to_parquet(path, index=False)
    manifest_path = directory / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "calendar_content_sha256": content_sha,
                "completed_at_utc": "2026-08-31T14:00:00Z",
            }
        ),
        encoding="utf-8",
    )
    entry = {
        "status": "complete",
        "exchange": "SSE",
        "start_date": start_date,
        "end_date": "2026-09-30",
        "row_count": int(len(frame)),
        "open_day_count": int(frame["is_open"].sum()),
        "path": str(path.resolve()),
        "artifact_sha256": sha256_file(path),
        "calendar_content_sha256": content_sha,
        "completed_at_utc": "2026-08-31T14:00:00Z",
        "manifest_path": str(manifest_path.resolve()),
        "manifest_sha256": sha256_file(manifest_path),
    }
    sessions = frame.loc[
        frame["is_open"] & frame["cal_date"].lt(pd.Timestamp("2026-09-01")),
        "cal_date",
    ].tolist()[-LIQUIDITY_SESSION_COUNT:]
    return entry, [pd.Timestamp(value) for value in sessions]


def _reference(tickers: list[str]) -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "trade_date": "20260831",
            "ts_code": tickers,
            "name": [f"公司{index}" for index in range(len(tickers))],
            "industry": "测试行业",
            "list_date": "20100101",
            "delist_date": None,
            "list_status": "L",
        }
    )
    frame.loc[frame["ts_code"].eq("000001.SZ"), "name"] = "*ST一号"
    frame.loc[frame["ts_code"].eq("000002.SZ"), "list_date"] = "20260902"
    frame.loc[frame["ts_code"].eq("000003.SZ"), "delist_date"] = "20260831"
    frame.loc[frame["ts_code"].eq("000003.SZ"), "list_status"] = "D"
    return frame


def _write_sources(
    root: Path,
    *,
    original_count: int = 510,
    late_partition_at: str | None = None,
    narrow_calendar: bool = False,
) -> FakeClient:
    _write_config(root)
    calendar_entry, sessions = _calendar_artifact(
        root, start_date="2026-08-22" if narrow_calendar else "2026-05-01"
    )
    if len(sessions) < LIQUIDITY_SESSION_COUNT:
        frozen_sessions = list(pd.bdate_range("2026-05-01", "2026-08-21"))
        execution_path = root / "runtime/data/top500/execution.parquet"
        execution_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"date": frozen_sessions}).to_parquet(execution_path, index=False)
        sessions = sorted({*frozen_sessions, *sessions})[-LIQUIDITY_SESSION_COUNT:]
    original = [f"{index:06d}.SZ" for index in range(1, original_count + 1)]
    twenty_day = "900001.SH"
    nineteen_day = "900002.SH"
    partitions: dict[str, Any] = {}
    for position, session in enumerate(sessions):
        tickers = list(original)
        amounts = [float(10_000 - index) for index in range(1, original_count + 1)]
        # Freeze a true boundary tie.  The ascending ticker is selected.
        if original_count >= 500:
            amounts[498] = amounts[499]
        if position >= len(sessions) - MINIMUM_LIQUIDITY_OBSERVATIONS:
            tickers.append(twenty_day)
            amounts.append(20_000.0)
        if position >= len(sessions) - (MINIMUM_LIQUIDITY_OBSERVATIONS - 1):
            tickers.append(nineteen_day)
            amounts.append(30_000.0)
        date = session.date().isoformat()
        path = (
            root / "runtime/data/raw/daily" / f"trade_date={date}" / "part-000.parquet"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        daily = pd.DataFrame(
            {
                field: [1.0] * len(tickers)
                for field in DATASET_FIELDS["daily"].split(",")
            }
        )
        daily["ts_code"] = tickers
        daily["trade_date"] = date.replace("-", "")
        daily["amount"] = amounts
        daily.to_parquet(path, index=False)
        completed = (
            late_partition_at
            if late_partition_at is not None and position == len(sessions) - 1
            else "2026-08-31T15:00:00Z"
        )
        key = f"daily/{date}"
        partitions[key] = {
            "status": "complete",
            "dataset": "daily",
            "trade_date": date,
            "path": str(path.resolve()),
            "row_count": len(tickers),
            "size_bytes": path.stat().st_size,
            "sha256": sha256_file(path),
            "completed_at_utc": completed,
        }
        if sources.provider_completion_required(date):
            sample: dict[str, pd.DataFrame] = {"daily": daily}
            for dataset in ("daily_basic", "adj_factor"):
                frame = pd.DataFrame(
                    {
                        field: [1.0] * len(tickers)
                        for field in DATASET_FIELDS[dataset].split(",")
                    }
                )
                frame["ts_code"] = tickers
                frame["trade_date"] = date.replace("-", "")
                sample[dataset] = frame
            partitions[key]["provider_completion"] = (
                sources._build_provider_completion_evidence(
                    date,
                    [
                        sample,
                        {name: frame.copy() for name, frame in sample.items()},
                    ],
                    observations=[
                        {
                            "request_id": f"membership-fixture-{date}-1",
                            "request_started_at_utc": f"{date}T09:20:00Z",
                            "response_completed_at_utc": f"{date}T09:20:01Z",
                        },
                        {
                            "request_id": f"membership-fixture-{date}-2",
                            "request_started_at_utc": f"{date}T09:21:00Z",
                            "response_completed_at_utc": f"{date}T09:21:01Z",
                        },
                    ],
                )
            )

    # A huge post-as-of print must be irrelevant and must never be opened.
    future_path = root / "runtime/data/raw/daily/trade_date=2026-09-01/part-000.parquet"
    future_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "ts_code": [original[-1]],
            "trade_date": ["20260901"],
            "amount": [999_999_999.0],
        }
    ).to_parquet(future_path, index=False)
    partitions["daily/2026-09-01"] = {
        "status": "complete",
        "dataset": "daily",
        "trade_date": "2026-09-01",
        "path": str(future_path.resolve()),
        "row_count": 1,
        "size_bytes": future_path.stat().st_size,
        "sha256": sha256_file(future_path),
        "completed_at_utc": "2026-09-01T08:00:00Z",
    }
    checkpoint = root / "runtime/data/raw/checkpoint.json"
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    checkpoint.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "partitions": partitions,
                "calendars": {
                    calendar_entry["calendar_content_sha256"]: calendar_entry
                },
            }
        ),
        encoding="utf-8",
    )
    reference_tickers = [*original, twenty_day, nineteen_day]
    reference = _reference(reference_tickers)
    reference["source_trade_date"] = reference["trade_date"]
    reference_path = (
        root
        / "runtime/data/raw/bak_basic/trade_date=2026-08-31/part-000.parquet"
    )
    reference_path.parent.mkdir(parents=True, exist_ok=True)
    reference.to_parquet(reference_path, index=False)
    daily_entry = partitions["daily/2026-08-31"]
    reference_entry = {
        "status": "complete",
        "dataset": "bak_basic",
        "trade_date": "2026-08-31",
        "request_trade_date": "2026-08-31",
        "source_trade_date": "2026-08-31",
        "capture_contract_id": EXACT_REFERENCE_CONTRACT_ID,
        "capture_mode": "exact_only",
        "fallback_used": False,
        "fields": ENRICHMENT_DATASET_FIELDS["bak_basic"],
        "path": str(reference_path.resolve()),
        "row_count": len(reference),
        "size_bytes": reference_path.stat().st_size,
        "sha256": sha256_file(reference_path),
        "completed_at_utc": "2026-08-31T15:30:00Z",
        "exact_source_required": True,
        "stability_sample_count": 2,
        "daily_partition_sha256": daily_entry["sha256"],
        "daily_ticker_count": daily_entry["row_count"],
        "covered_ticker_count": daily_entry["row_count"],
        "reference_ticker_count": len(reference),
    }
    (root / "runtime/data/raw/enrichment-checkpoint.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "partitions": {
                    "bak_basic/trade_date=2026-08-31": reference_entry
                },
            }
        ),
        encoding="utf-8",
    )
    return FakeClient(reference)


def test_build_freezes_causal_top500_tie_units_and_separate_eligibility(
    tmp_path: Path,
) -> None:
    client = _write_sources(tmp_path)
    capsule_config = tmp_path.parent / f"{tmp_path.name}-release-capsule-data.json"
    capsule_config.parent.mkdir(parents=True, exist_ok=True)
    capsule_config.write_bytes((tmp_path / "configs/data.json").read_bytes())

    result = build_prospective_membership_snapshot(
        tmp_path,
        "2026-09",
        client=client,
        available_at_utc=AVAILABLE_AT,
        config_path=capsule_config,
    )

    frame = result.frame.set_index("ts_code")
    assert len(frame) == EXPECTED_MEMBERSHIP_SIZE
    assert result.as_of_date == "2026-08-31"
    assert result.membership_path.parent.name == sha256_file(result.membership_path)
    assert frame.index.is_unique
    assert "900001.SH" in frame.index
    assert "900002.SH" not in frame.index
    assert (
        frame.loc["900001.SH", "liquidity_observations"]
        == MINIMUM_LIQUIDITY_OBSERVATIONS
    )
    assert frame.loc["000001.SZ", "median_amount_60d"] == pytest.approx(9_999_000.0)
    assert "000499.SZ" in frame.index
    assert "000500.SZ" not in frame.index
    assert not bool(frame.loc["000001.SZ", "eligible"])
    assert frame.loc["000001.SZ", "eligibility_reason"] == "st_name_marker"
    assert not bool(frame.loc["000002.SZ", "eligible"])
    assert frame.loc["000002.SZ", "eligibility_reason"] == "not_listed_at_as_of"
    assert not bool(frame.loc["000003.SZ", "eligible"])
    assert frame.loc["000003.SZ", "eligibility_reason"] == "delisted_at_as_of"
    assert len(frame) == 500  # ineligible members were not replaced
    assert client.calls == []
    assert result.manifest["liquidity_window_end"] == "2026-08-31"
    assert result.manifest["historical_equivalence_claimed"] is False
    assert not any(
        source.get("trade_date") == "2026-09-01"
        for source in result.manifest["input_sources"]
    )
    guarded_daily = [
        source
        for source in result.manifest["input_sources"]
        if source.get("role") == "liquidity_daily_partition"
        and sources.provider_completion_required(source["trade_date"])
    ]
    assert guarded_daily
    checkpoint = json.loads(
        (tmp_path / "runtime/data/raw/checkpoint.json").read_text(
            encoding="utf-8"
        )
    )
    for source in guarded_daily:
        entry = checkpoint["partitions"][source["checkpoint_key"]]
        assert source["provider_completion"] == entry["provider_completion"]
        assert source["checkpoint_entry_sha256"] == (
            prospective_membership._checkpoint_entry_digest(entry)
        )
    accepted, source, _ = _membership_source(
        tmp_path,
        pd.Timestamp("2026-08-31"),
        pd.Timestamp("2026-09-01"),
        membership_snapshot_path=result.membership_path,
        availability_cap=pd.Timestamp(AVAILABLE_AT),
    )
    assert len(accepted) == EXPECTED_MEMBERSHIP_SIZE
    assert source["kind"] == "content_addressed_monthly_snapshot"


def test_direct_membership_builder_rejects_missing_post_cutover_guard(
    tmp_path: Path,
) -> None:
    _write_sources(tmp_path)
    checkpoint_path = tmp_path / "runtime/data/raw/checkpoint.json"
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    checkpoint["partitions"]["daily/2026-08-31"].pop(
        "provider_completion"
    )
    checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")

    with pytest.raises(
        ProspectiveMembershipError,
        match="provider-completion evidence",
    ):
        build_prospective_membership_snapshot(
            tmp_path,
            "2026-09",
            available_at_utc=AVAILABLE_AT,
        )


def test_null_cutoff_membership_publish_is_durable_manifest_last_and_immediately_visible(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_sources(tmp_path)
    monotonic_start = time.monotonic()
    witnessed_start = pd.Timestamp("2026-09-01T02:00:00Z")

    def witnessed_now() -> pd.Timestamp:
        return witnessed_start + pd.Timedelta(
            seconds=time.monotonic() - monotonic_start
        )

    monkeypatch.setattr(prospective, "_now_utc", witnessed_now)
    monkeypatch.setattr(prospective_membership, "_now_utc", witnessed_now)
    original = prospective_membership._write_verified
    publication_order: list[str] = []

    def record(path: Path, payload: bytes) -> None:
        if (
            path.parent.parent.name == "2026-09"
            and path.parent.parent.parent.name == "membership"
        ):
            publication_order.append(path.name)
        original(path, payload)

    monkeypatch.setattr(prospective_membership, "_write_verified", record)
    result = build_prospective_membership_snapshot(
        tmp_path,
        "2026-09",
        available_at_utc=None,
    )

    assert publication_order[-4:] == [
        "membership.parquet",
        "bak-basic-raw.json",
        "source-contract.json",
        "manifest.json",
    ]
    assert pd.Timestamp(result.completed_at_utc) <= witnessed_now()
    assert load_prospective_membership_snapshot(
        result.membership_path,
        project_root=tmp_path,
        available_at_utc=witnessed_now(),
    ).artifact_sha256 == result.artifact_sha256


def test_concurrent_membership_publishers_share_one_manifest(tmp_path: Path) -> None:
    _write_sources(tmp_path)
    barrier = threading.Barrier(2)

    def build() -> str:
        barrier.wait(timeout=10)
        return build_prospective_membership_snapshot(
            tmp_path,
            "2026-09",
            available_at_utc=AVAILABLE_AT,
        ).completed_at_utc

    with ThreadPoolExecutor(max_workers=2) as pool:
        first_future = pool.submit(build)
        second_future = pool.submit(build)
        first = first_future.result(timeout=90)
        second = second_future.result(timeout=90)

    assert first == second
    candidates = [
        path
        for path in (
            tmp_path / "runtime/prospective/5.0/membership/2026-09"
        ).iterdir()
        if path.is_dir()
    ]
    assert len(candidates) == 1
    assert load_prospective_membership_snapshot(
        candidates[0], project_root=tmp_path, available_at_utc=AVAILABLE_AT
    ).completed_at_utc == first


def test_loader_independently_rebuilds_and_build_is_idempotent(tmp_path: Path) -> None:
    client = _write_sources(tmp_path)
    first = build_prospective_membership_snapshot(
        tmp_path, "2026-09", client=client, available_at_utc=AVAILABLE_AT
    )
    loaded = load_prospective_membership_snapshot(
        first.directory, project_root=tmp_path, available_at_utc=AVAILABLE_AT
    )
    second = build_prospective_membership_snapshot(
        tmp_path,
        "2026-09",
        client=FakeClient(client.reference.copy()),
        available_at_utc="2026-08-31T16:30:00Z",
    )

    assert loaded.artifact_sha256 == first.artifact_sha256
    assert second.artifact_sha256 == first.artifact_sha256
    assert second.completed_at_utc == first.completed_at_utc


def test_public_loader_replays_only_from_cas_after_live_sources_are_removed(
    tmp_path: Path,
) -> None:
    client = _write_sources(tmp_path, narrow_calendar=True)
    result = build_prospective_membership_snapshot(
        tmp_path, "2026-09", client=client, available_at_utc=AVAILABLE_AT
    )

    for source in result.manifest["input_sources"]:
        role = source["role"]
        if role in {
            "official_trade_calendar",
            "frozen_execution_session_prefix",
            "liquidity_daily_partition",
        }:
            assert "immutable_path" in source
            origin = tmp_path / source["path"]
            origin.unlink(missing_ok=True)
        if role == "official_trade_calendar":
            assert "immutable_manifest_path" in source
            (tmp_path / source["manifest_path"]).unlink(missing_ok=True)
        if role == "point_in_time_reference":
            assert "immutable_response_path" in source

    (tmp_path / "runtime/data/raw/checkpoint.json").unlink(missing_ok=True)
    loaded = load_prospective_membership_snapshot(
        result.membership_path,
        project_root=tmp_path,
        available_at_utc=AVAILABLE_AT,
    )
    assert loaded.artifact_sha256 == result.artifact_sha256
    accepted, source, _ = _membership_source(
        tmp_path,
        pd.Timestamp("2026-08-31"),
        pd.Timestamp("2026-09-01"),
        membership_snapshot_path=result.membership_path,
        availability_cap=pd.Timestamp(AVAILABLE_AT),
    )
    assert len(accepted) == EXPECTED_MEMBERSHIP_SIZE
    assert source["sha256"] == result.artifact_sha256


def test_fractional_source_availability_is_ceiled(tmp_path: Path) -> None:
    client = _write_sources(
        tmp_path,
        late_partition_at="2026-08-31T15:00:00.123456Z",
    )
    result = build_prospective_membership_snapshot(
        tmp_path, "2026-09", client=client, available_at_utc=AVAILABLE_AT
    )
    latest_daily = next(
        source
        for source in result.manifest["input_sources"]
        if source.get("role") == "liquidity_daily_partition"
        and source.get("trade_date") == "2026-08-31"
    )
    assert latest_daily["completed_at_utc"] == "2026-08-31T15:00:01Z"


def test_narrow_prospective_calendar_uses_frozen_session_prefix(tmp_path: Path) -> None:
    client = _write_sources(tmp_path, narrow_calendar=True)
    result = build_prospective_membership_snapshot(
        tmp_path, "2026-09", client=client, available_at_utc=AVAILABLE_AT
    )

    roles = [source["role"] for source in result.manifest["input_sources"]]
    assert roles.count("official_trade_calendar") == 1
    assert roles.count("frozen_execution_session_prefix") == 1
    assert roles.count("liquidity_daily_partition") == LIQUIDITY_SESSION_COUNT
    assert result.as_of_date == "2026-08-31"


def test_loader_uses_reference_cas_not_the_redundant_bundle_copy(tmp_path: Path) -> None:
    client = _write_sources(tmp_path)
    result = build_prospective_membership_snapshot(
        tmp_path, "2026-09", client=client, available_at_utc=AVAILABLE_AT
    )
    original = result.reference_raw_path.read_bytes()
    result.reference_raw_path.write_bytes(original + b" ")
    loaded = load_prospective_membership_snapshot(
        result.membership_path, project_root=tmp_path
    )
    assert loaded.artifact_sha256 == result.artifact_sha256
    result.reference_raw_path.unlink()
    loaded = load_prospective_membership_snapshot(
        result.membership_path, project_root=tmp_path
    )
    assert loaded.artifact_sha256 == result.artifact_sha256

    frame = pd.read_parquet(result.membership_path)
    frame.loc[0, "eligible"] = not bool(frame.loc[0, "eligible"])
    frame.to_parquet(result.membership_path, index=False)
    with pytest.raises(ProspectiveMembershipError, match="directory does not match"):
        load_prospective_membership_snapshot(
            result.membership_path, project_root=tmp_path
        )


def test_provider_reference_must_cover_the_full_asof_daily_universe(
    tmp_path: Path,
) -> None:
    client = _write_sources(tmp_path)
    # This low-liquidity name is not selected into the Top-500, so the old
    # selected-only 99% check would not notice its absence.
    incomplete = client.reference.loc[
        client.reference["ts_code"].ne("000510.SZ")
    ].copy()
    path = (
        tmp_path
        / "runtime/data/raw/bak_basic/trade_date=2026-08-31/part-000.parquet"
    )
    incomplete.to_parquet(path, index=False)
    checkpoint_path = tmp_path / "runtime/data/raw/enrichment-checkpoint.json"
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    entry = checkpoint["partitions"]["bak_basic/trade_date=2026-08-31"]
    entry.update(
        row_count=len(incomplete),
        size_bytes=path.stat().st_size,
        sha256=sha256_file(path),
        reference_ticker_count=len(incomplete),
    )
    checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")

    with pytest.raises(ProspectiveMembershipError, match="provider-universe coverage"):
        build_prospective_membership_snapshot(
            tmp_path,
            "2026-09",
            client=client,
            available_at_utc=AVAILABLE_AT,
        )


@pytest.mark.parametrize(
    "binding",
    ["daily_partition_sha256", "daily_ticker_count"],
    ids=("daily-sha", "daily-ticker-count"),
)
def test_build_rejects_reference_binding_that_differs_from_asof_daily(
    tmp_path: Path,
    binding: str,
) -> None:
    _write_sources(tmp_path)
    checkpoint_path = tmp_path / "runtime/data/raw/enrichment-checkpoint.json"
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    entry = checkpoint["partitions"]["bak_basic/trade_date=2026-08-31"]
    if binding == "daily_partition_sha256":
        observed_sha = str(entry[binding])
        replacement = "0" if observed_sha[0] != "0" else "1"
        entry[binding] = replacement + observed_sha[1:]
    else:
        wrong_count = int(entry[binding]) + 1
        entry[binding] = wrong_count
        entry["covered_ticker_count"] = wrong_count
    checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")

    with pytest.raises(
        ProspectiveMembershipError,
        match="daily-universe binding differs from exact source bytes",
    ):
        build_prospective_membership_snapshot(
            tmp_path,
            "2026-09",
            available_at_utc=AVAILABLE_AT,
        )

    assert not (
        tmp_path / "runtime/prospective/5.0/membership/2026-09"
    ).exists()


def test_sealed_membership_replays_every_monthly_source_cas(tmp_path: Path) -> None:
    client = _write_sources(tmp_path)
    result = build_prospective_membership_snapshot(
        tmp_path, "2026-09", client=client, available_at_utc=AVAILABLE_AT
    )
    _accepted, sealed_source, _availability = _membership_source(
        tmp_path,
        pd.Timestamp("2026-08-31"),
        pd.Timestamp("2026-09-01"),
        membership_snapshot_path=result.membership_path,
        availability_cap=pd.Timestamp(AVAILABLE_AT),
    )
    for origin in (
        result.membership_path,
        result.manifest_path,
        result.source_contract_path,
        result.reference_raw_path,
    ):
        origin.unlink(missing_ok=True)
    replayed, _replayed_source, _availability = _membership_source(
        tmp_path,
        pd.Timestamp("2026-08-31"),
        pd.Timestamp("2026-09-01"),
        membership_snapshot_path=None,
        availability_cap=pd.Timestamp(AVAILABLE_AT),
        sealed_source=sealed_source,
    )
    assert len(replayed) == EXPECTED_MEMBERSHIP_SIZE
    daily_source = next(
        row
        for row in result.manifest["input_sources"]
        if row.get("role") == "liquidity_daily_partition"
    )
    (tmp_path / daily_source["immutable_path"]).write_bytes(b"tampered old daily CAS")

    with pytest.raises(
        ProspectiveDataError,
        match="sealed monthly membership failed full immutable source replay",
    ):
        _membership_source(
            tmp_path,
            pd.Timestamp("2026-08-31"),
            pd.Timestamp("2026-09-01"),
            membership_snapshot_path=None,
            availability_cap=pd.Timestamp(AVAILABLE_AT),
            sealed_source=sealed_source,
        )


def test_membership_schema_rejects_boolean_version(tmp_path: Path) -> None:
    client = _write_sources(tmp_path)
    result = build_prospective_membership_snapshot(
        tmp_path, "2026-09", client=client, available_at_utc=AVAILABLE_AT
    )
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    manifest["schema_version"] = True
    manifest["manifest_core_sha256"] = hashlib.sha256(
        _canonical_bytes(
            {key: value for key, value in manifest.items() if key != "manifest_core_sha256"}
        )
    ).hexdigest()
    result.manifest_path.write_bytes(_canonical_bytes(manifest))

    with pytest.raises(ProspectiveMembershipError, match="manifest schema"):
        load_prospective_membership_snapshot(
            result.membership_path, project_root=tmp_path
        )


def test_loader_uses_daily_cas_after_origin_mutation_and_rejects_cas_tamper(
    tmp_path: Path,
) -> None:
    client = _write_sources(tmp_path)
    result = build_prospective_membership_snapshot(
        tmp_path, "2026-09", client=client, available_at_utc=AVAILABLE_AT
    )
    source = next(
        row
        for row in result.manifest["input_sources"]
        if row.get("role") == "liquidity_daily_partition"
    )
    path = tmp_path / source["path"]
    frame = pd.read_parquet(path)
    frame.loc[0, "amount"] += 1.0
    frame.to_parquet(path, index=False)

    loaded = load_prospective_membership_snapshot(
        result.membership_path, project_root=tmp_path
    )
    assert loaded.artifact_sha256 == result.artifact_sha256

    immutable_path = tmp_path / source["immutable_path"]
    immutable_path.write_bytes(b"tampered immutable daily source")
    with pytest.raises(ProspectiveMembershipError, match="daily immutable source"):
        load_prospective_membership_snapshot(
            result.membership_path, project_root=tmp_path
        )


def test_build_rejects_source_unavailable_by_cutoff(tmp_path: Path) -> None:
    client = _write_sources(tmp_path, late_partition_at="2026-08-31T16:00:01Z")

    with pytest.raises(ProspectiveMembershipError, match="not available by cutoff"):
        build_prospective_membership_snapshot(
            tmp_path,
            "2026-09",
            client=client,
            available_at_utc=AVAILABLE_AT,
        )


def test_checkpointed_reference_replays_from_cas_after_checkpoint_mutation(
    tmp_path: Path,
) -> None:
    _write_sources(tmp_path)
    checkpoint_path = tmp_path / "runtime/data/raw/enrichment-checkpoint.json"
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    entry = dict(checkpoint["partitions"]["bak_basic/trade_date=2026-08-31"])
    result = build_prospective_membership_snapshot(
        tmp_path, "2026-09", available_at_utc=AVAILABLE_AT
    )
    reference_source = next(
        source
        for source in result.manifest["input_sources"]
        if source.get("role") == "point_in_time_reference"
    )
    assert reference_source["kind"] == "checkpointed_bak_basic"

    entry["completed_at_utc"] = "2026-08-31T15:29:59Z"
    checkpoint["partitions"]["bak_basic/trade_date=2026-08-31"] = entry
    checkpoint_path.write_text(
        json.dumps(checkpoint),
        encoding="utf-8",
    )
    loaded = load_prospective_membership_snapshot(
        result.membership_path, project_root=tmp_path
    )
    assert loaded.artifact_sha256 == result.artifact_sha256

    immutable_source = tmp_path / reference_source["immutable_source_path"]
    immutable_source.write_bytes(b"tampered immutable bak_basic source")
    with pytest.raises(ProspectiveMembershipError, match="bak_basic immutable source"):
        load_prospective_membership_snapshot(
            result.membership_path, project_root=tmp_path
        )


def test_build_fails_closed_when_exact_500_cannot_be_selected(tmp_path: Path) -> None:
    client = _write_sources(tmp_path, original_count=498)

    with pytest.raises(ProspectiveMembershipError, match="fewer than 500"):
        build_prospective_membership_snapshot(
            tmp_path,
            "2026-09",
            client=client,
            available_at_utc=AVAILABLE_AT,
        )


def test_rule_is_explicitly_forward_only(tmp_path: Path) -> None:
    with pytest.raises(ProspectiveMembershipError, match="forward-only"):
        build_prospective_membership_snapshot(
            tmp_path,
            "2026-08",
            client=FakeClient(pd.DataFrame()),
            available_at_utc=AVAILABLE_AT,
        )

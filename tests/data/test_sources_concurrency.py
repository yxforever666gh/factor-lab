from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import threading
import time
from typing import Any, Iterator

import pandas as pd
import pytest

import factor_lab.data.sources as sources
from factor_lab.data import RuntimeLayout, sync_data, sync_enrichment
from factor_lab.data.catalog import sha256_file
from factor_lab.data.sources import DATASET_FIELDS, ENRICHMENT_DATASET_FIELDS


TRADE_DATE = "2024-01-02"
COMPACT_TRADE_DATE = "20240102"


def _config(
    tmp_path: Path,
    *,
    enrichment: bool = False,
) -> tuple[Path, RuntimeLayout]:
    payload: dict[str, Any] = {
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
            "datasets": ["daily", "daily_basic"],
            "checkpoint_file": "checkpoint.json",
            "request_rate_per_minute": 0,
            "verify_hashes_on_resume": True,
        },
    }
    if enrichment:
        payload.update(
            enrichment={
                "checkpoint_file": "enrichment-checkpoint.json",
                "request_rate_per_minute": 0,
            },
            fundamentals={
                "start_period": "20231231",
                "checkpoint_file": "fundamentals-checkpoint.json",
                "request_rate_per_minute": 0,
            },
        )
    config_path = tmp_path / "configs/data.json"
    config_path.parent.mkdir(parents=True)
    config_path.write_text(json.dumps(payload), encoding="utf-8")
    layout = RuntimeLayout.from_config(payload, config_path=config_path)
    layout.ensure_directories()
    return config_path, layout


def _calendar_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "exchange": ["SSE"],
            "cal_date": [COMPACT_TRADE_DATE],
            "is_open": [1],
            "pretrade_date": ["20231229"],
        }
    )


def _raw_frame(dataset: str, ticker: str = "000001.SZ") -> pd.DataFrame:
    fields = DATASET_FIELDS[dataset].split(",")
    row = {field: 1.0 for field in fields}
    row["ts_code"] = ticker
    row["trade_date"] = COMPACT_TRADE_DATE
    return pd.DataFrame([row], columns=fields)


class ConcurrentRawClient:
    def __init__(self, dataset: str, barrier: threading.Barrier) -> None:
        self.dataset = dataset
        self.barrier = barrier

    def query(self, endpoint: str, **_kwargs: Any) -> pd.DataFrame:
        if endpoint == "trade_cal":
            return _calendar_frame()
        assert endpoint == self.dataset
        self.barrier.wait(timeout=10)
        return _raw_frame(endpoint)


class SimpleRawClient:
    def __init__(self, ticker: str) -> None:
        self.ticker = ticker
        self.data_calls = 0

    def query(self, endpoint: str, **_kwargs: Any) -> pd.DataFrame:
        if endpoint == "trade_cal":
            return _calendar_frame()
        self.data_calls += 1
        return _raw_frame(endpoint, self.ticker)


def test_concurrent_raw_checkpoint_writers_merge_latest_partitions(
    tmp_path: Path,
) -> None:
    config_path, layout = _config(tmp_path)
    barrier = threading.Barrier(2)

    def run(dataset: str) -> dict[str, Any]:
        return sync_data(
            TRADE_DATE,
            TRADE_DATE,
            config_path=config_path,
            layout=layout,
            client=ConcurrentRawClient(dataset, barrier),
            datasets=(dataset,),
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(run, ("daily", "daily_basic")))

    assert [result["status"] for result in results] == ["complete", "complete"]
    checkpoint = json.loads(layout.checkpoint_path.read_text(encoding="utf-8"))
    assert set(checkpoint["partitions"]) == {
        f"daily/{TRADE_DATE}",
        f"daily_basic/{TRADE_DATE}",
    }
    assert len(checkpoint["calendars"]) == 1
    for entry in checkpoint["partitions"].values():
        artifact = Path(entry["path"])
        assert artifact.is_file()
        assert entry["sha256"] == sha256_file(artifact)


def test_raw_no_resume_refreshes_calendar_and_partition(
    tmp_path: Path,
) -> None:
    config_path, layout = _config(tmp_path)
    first_client = SimpleRawClient("FIRST.SZ")
    sync_data(
        TRADE_DATE,
        TRADE_DATE,
        config_path=config_path,
        layout=layout,
        client=first_client,
        datasets=("daily",),
    )
    first_checkpoint = json.loads(
        layout.checkpoint_path.read_text(encoding="utf-8")
    )
    calendar_key = next(iter(first_checkpoint["calendars"]))
    first_calendar_completed = first_checkpoint["calendars"][calendar_key][
        "completed_at_utc"
    ]
    first_partition_completed = first_checkpoint["partitions"][
        f"daily/{TRADE_DATE}"
    ]["completed_at_utc"]

    second_client = SimpleRawClient("SECOND.SZ")
    sync_data(
        TRADE_DATE,
        TRADE_DATE,
        config_path=config_path,
        layout=layout,
        client=second_client,
        datasets=("daily",),
        resume=False,
    )

    second_checkpoint = json.loads(
        layout.checkpoint_path.read_text(encoding="utf-8")
    )
    saved = pd.read_parquet(
        layout.raw_root
        / "daily"
        / f"trade_date={TRADE_DATE}"
        / "part-000.parquet"
    )
    assert first_client.data_calls == second_client.data_calls == 1
    assert saved["ts_code"].tolist() == ["SECOND.SZ"]
    assert (
        second_checkpoint["calendars"][calendar_key]["completed_at_utc"]
        > first_calendar_completed
    )
    assert (
        second_checkpoint["partitions"][f"daily/{TRADE_DATE}"][
            "completed_at_utc"
        ]
        > first_partition_completed
    )


def test_atomic_replaces_fsync_the_parent_after_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    json_path = tmp_path / "checkpoint.json"
    parquet_path = tmp_path / "partition.parquet"
    observations: list[tuple[Path, bool, bool]] = []

    def observe(directory: Path) -> None:
        observations.append(
            (directory, json_path.is_file(), parquet_path.is_file())
        )

    monkeypatch.setattr(sources, "_fsync_directory", observe)
    sources._write_json_atomic(json_path, {"schema_version": 1})
    sources._write_parquet_atomic(
        parquet_path,
        pd.DataFrame({"value": [1]}),
    )

    assert observations == [
        (tmp_path, True, False),
        (tmp_path, True, True),
    ]


def test_exact_reference_lock_order_is_raw_then_reference(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = tmp_path / "checkpoint.json"
    reference = tmp_path / "reference-checkpoint.json"
    events: list[tuple[str, Path]] = []

    @contextmanager
    def recording_lock(path: Path) -> Iterator[None]:
        events.append(("acquire", path))
        try:
            yield
        finally:
            events.append(("release", path))

    monkeypatch.setattr(sources, "_checkpoint_lock", recording_lock)
    with sources._raw_reference_checkpoint_locks(raw, reference):
        events.append(("body", tmp_path))

    assert events == [
        ("acquire", raw.resolve()),
        ("acquire", reference.resolve()),
        ("body", tmp_path),
        ("release", reference.resolve()),
        ("release", raw.resolve()),
    ]


def _daily_frame(tickers: tuple[str, ...]) -> pd.DataFrame:
    fields = DATASET_FIELDS["daily"].split(",")
    rows: list[dict[str, Any]] = []
    for index, ticker in enumerate(tickers, start=1):
        row = {field: float(index) for field in fields}
        row["ts_code"] = ticker
        row["trade_date"] = COMPACT_TRADE_DATE
        rows.append(row)
    return pd.DataFrame(rows, columns=fields)


def _publish_daily(layout: RuntimeLayout, tickers: tuple[str, ...]) -> dict[str, Any]:
    path = (
        layout.raw_root
        / "daily"
        / f"trade_date={TRADE_DATE}"
        / "part-000.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = _daily_frame(tickers)
    frame.to_parquet(path, index=False)
    entry = {
        "status": "complete",
        "dataset": "daily",
        "trade_date": TRADE_DATE,
        "path": str(path),
        "row_count": len(frame),
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
        "completed_at_utc": "2024-01-02T08:00:00Z",
    }
    layout.checkpoint_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "partitions": {f"daily/{TRADE_DATE}": entry},
            }
        ),
        encoding="utf-8",
    )
    return entry


def _reference_frame(tickers: tuple[str, ...]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "trade_date": [COMPACT_TRADE_DATE] * len(tickers),
            "ts_code": list(tickers),
            "name": [f"name-{ticker}" for ticker in tickers],
            "industry": ["test"] * len(tickers),
            "list_date": ["20100101"] * len(tickers),
        },
        columns=ENRICHMENT_DATASET_FIELDS["bak_basic"].split(","),
    )


class ChangingDailyReferenceClient:
    def __init__(self, layout: RuntimeLayout) -> None:
        self.layout = layout
        self.calls = 0

    def query(self, endpoint: str, **_kwargs: Any) -> pd.DataFrame:
        assert endpoint == "bak_basic"
        self.calls += 1
        if self.calls == 2:
            _publish_daily(
                self.layout,
                ("000001.SZ", "600000.SH", "300001.SZ"),
            )
        return _reference_frame(("000001.SZ", "600000.SH"))


def test_exact_reference_refuses_daily_change_between_samples(
    tmp_path: Path,
) -> None:
    config_path, layout = _config(tmp_path, enrichment=True)
    initial = _publish_daily(layout, ("000001.SZ", "600000.SH"))
    client = ChangingDailyReferenceClient(layout)

    result = sources.sync_exact_reference(
        TRADE_DATE,
        config_path=config_path,
        layout=layout,
        client=client,
    )

    assert result["status"] == "waiting"
    assert result["reason"] == "daily_universe_changed_during_capture"
    assert result["initial_daily_partition_sha256"] == initial["sha256"]
    assert result["current_daily_partition_sha256"] != initial["sha256"]
    assert client.calls == 2
    assert not (layout.raw_root / "enrichment-checkpoint.json").exists()
    assert not (
        layout.raw_root
        / "bak_basic"
        / f"trade_date={TRADE_DATE}"
        / "part-000.parquet"
    ).exists()


def test_exact_reference_completion_time_follows_final_lock_and_artifact_fsync(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path, layout = _config(tmp_path, enrichment=True)
    _publish_daily(layout, ("000001.SZ", "600000.SH"))
    real_locks = sources._raw_reference_checkpoint_locks
    real_write = sources._write_parquet_atomic
    real_json_write = sources._write_json_atomic
    observed: dict[str, Any] = {"lock_calls": 0}

    @contextmanager
    def delayed_locks(raw: Path, reference: Path) -> Iterator[None]:
        observed["lock_calls"] += 1
        final = observed["lock_calls"] == 2
        if final:
            time.sleep(0.05)
        with real_locks(raw, reference):
            if final:
                observed["final_lock_acquired_at"] = datetime.now(timezone.utc)
            yield

    def recording_write(path: Path, frame: pd.DataFrame) -> None:
        real_write(path, frame)
        if "bak_basic" in path.parts:
            observed["artifact_durable_at"] = datetime.now(timezone.utc)

    def recording_json_write(path: Path, payload: Any) -> None:
        real_json_write(path, payload)
        if path.name == "enrichment-checkpoint.json":
            observed["checkpoint_published_at"] = datetime.now(timezone.utc)

    monkeypatch.setattr(sources, "_raw_reference_checkpoint_locks", delayed_locks)
    monkeypatch.setattr(sources, "_write_parquet_atomic", recording_write)
    monkeypatch.setattr(sources, "_write_json_atomic", recording_json_write)

    frame = _reference_frame(("000001.SZ", "600000.SH"))
    result = sources.sync_exact_reference(
        TRADE_DATE,
        config_path=config_path,
        layout=layout,
        client=StableReferenceClient(frame),
    )

    checkpoint = json.loads(
        Path(result["checkpoint_path"]).read_text(encoding="utf-8")
    )
    completed = datetime.fromisoformat(
        checkpoint["partitions"][f"bak_basic/trade_date={TRADE_DATE}"][
            "completed_at_utc"
        ]
    )
    assert observed["lock_calls"] == 2
    assert completed >= observed["final_lock_acquired_at"]
    assert completed >= observed["artifact_durable_at"]
    assert completed >= observed["checkpoint_published_at"]
    assert completed <= datetime.now(timezone.utc)


class StableReferenceClient:
    def __init__(self, frame: pd.DataFrame) -> None:
        self.frame = frame

    def query(self, endpoint: str, **_kwargs: Any) -> pd.DataFrame:
        assert endpoint == "bak_basic"
        return self.frame.copy()


def _financial_frame(ticker: str, *, early: bool = False) -> pd.DataFrame:
    fields = ENRICHMENT_DATASET_FIELDS["fina_indicator_vip"].split(",")
    row = {field: 1.0 for field in fields}
    row.update(
        {
            "ts_code": ticker,
            "ann_date": "20231230" if early else "20240130",
            "end_date": "20231231",
            "update_flag": "0",
        }
    )
    return pd.DataFrame([row], columns=fields)


def _write_membership(layout: RuntimeLayout) -> None:
    pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "membership_month": ["2024-01"],
            "as_of_date": pd.to_datetime(["2023-12-29"]),
            "effective_start_date": pd.to_datetime(["2024-01-02"]),
            "effective_end_date": pd.to_datetime(["2024-01-31"]),
        }
    ).to_parquet(layout.membership_path, index=False)


class SimpleFinancialClient:
    def __init__(self, ticker: str) -> None:
        self.ticker = ticker

    def query(self, endpoint: str, **_kwargs: Any) -> pd.DataFrame:
        assert endpoint == "fina_indicator_vip"
        return _financial_frame(self.ticker)


def test_enrichment_no_resume_refreshes_an_unchanged_baseline(
    tmp_path: Path,
) -> None:
    config_path, layout = _config(tmp_path, enrichment=True)
    _write_membership(layout)
    sync_enrichment(
        "2024-01-01",
        "2024-01-31",
        config_path=config_path,
        layout=layout,
        client=SimpleFinancialClient("FIRST.SZ"),
        datasets=("fina_indicator_vip",),
    )
    checkpoint_path = layout.raw_root / "fundamentals-checkpoint.json"
    key = "fina_indicator_vip/period=2023-12-31"
    first_completed = json.loads(checkpoint_path.read_text(encoding="utf-8"))[
        "partitions"
    ][key]["completed_at_utc"]

    sync_enrichment(
        "2024-01-01",
        "2024-01-31",
        config_path=config_path,
        layout=layout,
        client=SimpleFinancialClient("SECOND.SZ"),
        datasets=("fina_indicator_vip",),
        resume=False,
    )

    partition = (
        layout.raw_root
        / "fina_indicator_vip"
        / "period=2023-12-31"
        / "part-000.parquet"
    )
    second_entry = json.loads(checkpoint_path.read_text(encoding="utf-8"))[
        "partitions"
    ][key]
    assert pd.read_parquet(partition)["ts_code"].tolist() == ["SECOND.SZ"]
    assert second_entry["completed_at_utc"] > first_completed


class ConcurrentQuarantineWinnerClient:
    def __init__(self, layout: RuntimeLayout) -> None:
        self.layout = layout
        self.winner_quarantine_sha256: str | None = None

    def query(self, endpoint: str, **_kwargs: Any) -> pd.DataFrame:
        assert endpoint == "fina_indicator_vip"
        path = (
            self.layout.raw_root
            / "fina_indicator_vip"
            / "period=2023-12-31"
            / "part-000.parquet"
        )
        quarantine = path.with_name("part-000.quarantine.parquet")
        path.parent.mkdir(parents=True, exist_ok=True)
        winner = _financial_frame("WINNER.SZ")
        winner_quarantine = _financial_frame("QUARANTINE.SZ", early=True)
        winner.to_parquet(path, index=False)
        winner_quarantine.to_parquet(quarantine, index=False)
        self.winner_quarantine_sha256 = sha256_file(quarantine)
        entry = {
            "status": "complete",
            "dataset": "fina_indicator_vip",
            "period": "2023-12-31",
            "source_trade_date": None,
            "path": str(path),
            "row_count": len(winner),
            "size_bytes": path.stat().st_size,
            "sha256": sha256_file(path),
            "quarantine_row_count": len(winner_quarantine),
            "quarantine_path": str(quarantine),
            "quarantine_sha256": self.winner_quarantine_sha256,
            "completed_at_utc": "2024-01-30T08:00:00Z",
        }
        checkpoint = self.layout.raw_root / "fundamentals-checkpoint.json"
        checkpoint.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "partitions": {
                        "fina_indicator_vip/period=2023-12-31": entry
                    },
                }
            ),
            encoding="utf-8",
        )
        return _financial_frame("LOSER.SZ")


def test_enrichment_loser_cannot_delete_quarantine_winner(
    tmp_path: Path,
) -> None:
    config_path, layout = _config(tmp_path, enrichment=True)
    _write_membership(layout)
    client = ConcurrentQuarantineWinnerClient(layout)

    result = sync_enrichment(
        "2024-01-01",
        "2024-01-31",
        config_path=config_path,
        layout=layout,
        client=client,
        datasets=("fina_indicator_vip",),
        resume=False,
    )

    partition = (
        layout.raw_root
        / "fina_indicator_vip"
        / "period=2023-12-31"
        / "part-000.parquet"
    )
    quarantine = partition.with_name("part-000.quarantine.parquet")
    assert result["status"] == "complete"
    assert pd.read_parquet(partition)["ts_code"].tolist() == ["WINNER.SZ"]
    assert quarantine.is_file()
    assert sha256_file(quarantine) == client.winner_quarantine_sha256
    checkpoint = json.loads(
        (layout.raw_root / "fundamentals-checkpoint.json").read_text(
            encoding="utf-8"
        )
    )
    entry = checkpoint["partitions"][
        "fina_indicator_vip/period=2023-12-31"
    ]
    assert entry["quarantine_sha256"] == client.winner_quarantine_sha256


GUARDED_DATE = "2026-08-24"
GUARDED_COMPACT = "20260824"
GUARDED_TICKERS = ("000001.SZ", "600000.SH")


def _guarded_calendar() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "exchange": ["SSE"],
            "cal_date": [GUARDED_COMPACT],
            "is_open": [1],
            "pretrade_date": ["20260821"],
        }
    )


def _guarded_frame(dataset: str, tickers=GUARDED_TICKERS) -> pd.DataFrame:
    fields = DATASET_FIELDS[dataset].split(",")
    rows = []
    for index, ticker in enumerate(tickers, start=1):
        row = {field: float(index) for field in fields}
        row["ts_code"] = ticker
        row["trade_date"] = GUARDED_COMPACT
        rows.append(row)
    return pd.DataFrame(rows, columns=fields)


class GuardBundleClient:
    def __init__(
        self,
        *,
        tickers=GUARDED_TICKERS,
        first_request_barrier: threading.Barrier | None = None,
    ) -> None:
        self.frames = {
            dataset: _guarded_frame(dataset, tickers)
            for dataset in sources.PROVIDER_COMPLETION_DATASETS
        }
        self.first_request_barrier = first_request_barrier
        self.waited = False

    def query(self, endpoint: str, **_kwargs: Any) -> pd.DataFrame:
        if endpoint == "trade_cal":
            return _guarded_calendar()
        if self.first_request_barrier is not None and not self.waited:
            self.waited = True
            self.first_request_barrier.wait(timeout=10)
        return self.frames[endpoint].copy()


def test_guarded_bundle_crash_leaves_reconciling_marker_and_resume_recovers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path, layout = _config(tmp_path)
    real_write = sources._write_parquet_atomic
    legacy_entries: dict[str, dict[str, Any]] = {}
    legacy_shas: dict[str, str] = {}
    for dataset in sources.PROVIDER_COMPLETION_DATASETS:
        path = sources._partition_path(layout.raw_root, dataset, GUARDED_DATE)
        real_write(path, _guarded_frame(dataset, GUARDED_TICKERS[:1]))
        legacy_shas[dataset] = sha256_file(path)
        legacy_entries[f"{dataset}/{GUARDED_DATE}"] = {
            "status": "complete",
            "dataset": dataset,
            "trade_date": GUARDED_DATE,
            "path": str(path),
            "row_count": 1,
            "size_bytes": path.stat().st_size,
            "sha256": legacy_shas[dataset],
            "completed_at_utc": "2026-08-24T08:00:00Z",
        }
    layout.checkpoint_path.write_text(
        json.dumps({"schema_version": 1, "partitions": legacy_entries}),
        encoding="utf-8",
    )
    provider_writes = 0

    def crash_second_provider_write(path: Path, frame: pd.DataFrame) -> None:
        nonlocal provider_writes
        if any(dataset in path.parts for dataset in sources.PROVIDER_COMPLETION_DATASETS):
            provider_writes += 1
            if provider_writes == 2:
                raise RuntimeError("simulated provider bundle crash")
        real_write(path, frame)

    monkeypatch.setattr(sources, "_write_parquet_atomic", crash_second_provider_write)
    with pytest.raises(RuntimeError, match="simulated provider bundle crash"):
        sync_data(
            GUARDED_DATE,
            GUARDED_DATE,
            config_path=config_path,
            layout=layout,
            client=GuardBundleClient(),
            datasets=sources.PROVIDER_COMPLETION_DATASETS,
        )

    checkpoint = json.loads(layout.checkpoint_path.read_text(encoding="utf-8"))
    assert {
        checkpoint["partitions"][f"{dataset}/{GUARDED_DATE}"]["status"]
        for dataset in sources.PROVIDER_COMPLETION_DATASETS
    } == {"reconciling"}
    original_reconciliation = {
        dataset: dict(
            checkpoint["partitions"][f"{dataset}/{GUARDED_DATE}"][
                "reconciliation"
            ]
        )
        for dataset in sources.PROVIDER_COMPLETION_DATASETS
    }
    for dataset, reconciliation in original_reconciliation.items():
        assert reconciliation["previous_status"] == "complete"
        assert reconciliation["previous_artifact_sha256"] == legacy_shas[dataset]
        assert reconciliation["resume_count"] == 0
        assert len(reconciliation["attempts"]) == 1

    monkeypatch.setattr(sources, "_write_parquet_atomic", real_write)
    recovered = sync_data(
        GUARDED_DATE,
        GUARDED_DATE,
        config_path=config_path,
        layout=layout,
        client=GuardBundleClient(),
        datasets=sources.PROVIDER_COMPLETION_DATASETS,
        resume=True,
    )
    assert recovered["status"] == "complete"
    checkpoint = json.loads(layout.checkpoint_path.read_text(encoding="utf-8"))
    assert {
        checkpoint["partitions"][f"{dataset}/{GUARDED_DATE}"]["status"]
        for dataset in sources.PROVIDER_COMPLETION_DATASETS
    } == {"complete"}
    for dataset in sources.PROVIDER_COMPLETION_DATASETS:
        reconciliation = checkpoint["partitions"][
            f"{dataset}/{GUARDED_DATE}"
        ]["reconciliation"]
        assert reconciliation["started_at_utc"] == original_reconciliation[
            dataset
        ]["started_at_utc"]
        assert reconciliation["previous_checkpoint_entry_sha256"] == (
            original_reconciliation[dataset]["previous_checkpoint_entry_sha256"]
        )
        assert reconciliation["previous_artifact_sha256"] == legacy_shas[dataset]
        assert reconciliation["resume_count"] == 1
        assert len(reconciliation["attempts"]) == 2


def test_crashed_reconcile_blocks_a_later_provider_revision_without_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path, layout = _config(tmp_path)
    real_write = sources._write_parquet_atomic
    provider_writes = 0

    def crash_second_provider_write(path: Path, frame: pd.DataFrame) -> None:
        nonlocal provider_writes
        if any(dataset in path.parts for dataset in sources.PROVIDER_COMPLETION_DATASETS):
            provider_writes += 1
            if provider_writes == 2:
                raise RuntimeError("simulated provider bundle crash")
        real_write(path, frame)

    monkeypatch.setattr(sources, "_write_parquet_atomic", crash_second_provider_write)
    with pytest.raises(RuntimeError, match="simulated provider bundle crash"):
        sync_data(
            GUARDED_DATE,
            GUARDED_DATE,
            config_path=config_path,
            layout=layout,
            client=GuardBundleClient(),
            datasets=sources.PROVIDER_COMPLETION_DATASETS,
        )
    before = json.loads(layout.checkpoint_path.read_text(encoding="utf-8"))[
        "partitions"
    ]
    before_bytes = {
        dataset: path.read_bytes()
        for dataset in sources.PROVIDER_COMPLETION_DATASETS
        if (
            path := sources._partition_path(
                layout.raw_root, dataset, GUARDED_DATE
            )
        ).is_file()
    }

    monkeypatch.setattr(sources, "_write_parquet_atomic", real_write)
    blocked = sync_data(
        GUARDED_DATE,
        GUARDED_DATE,
        config_path=config_path,
        layout=layout,
        client=GuardBundleClient(tickers=("000001.SZ", "300001.SZ")),
        datasets=sources.PROVIDER_COMPLETION_DATASETS,
        resume=True,
    )

    assert blocked["status"] == "blocked"
    assert blocked["reason"] == "provider_revision_conflict"
    assert json.loads(layout.checkpoint_path.read_text(encoding="utf-8"))[
        "partitions"
    ] == before
    assert {
        dataset: sources._partition_path(
            layout.raw_root, dataset, GUARDED_DATE
        ).read_bytes()
        for dataset in before_bytes
    } == before_bytes


def test_completion_clock_rollback_after_marker_never_publishes_complete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path, layout = _config(tmp_path)
    real_write_json = sources._write_json_atomic
    real_time_ns = sources.time.time_ns
    marker_written = False

    def track_marker(path: Path, payload: dict[str, Any]) -> None:
        nonlocal marker_written
        real_write_json(path, payload)
        partitions = payload.get("partitions")
        if isinstance(partitions, dict) and any(
            isinstance(entry, dict) and entry.get("status") == "reconciling"
            for entry in partitions.values()
        ):
            marker_written = True

    rollback_ns = int(pd.Timestamp("2026-08-24T09:00:00Z").timestamp() * 1e9)
    monkeypatch.setattr(sources, "_write_json_atomic", track_marker)
    monkeypatch.setattr(
        sources.time,
        "time_ns",
        lambda: rollback_ns if marker_written else real_time_ns(),
    )

    with pytest.raises(
        ValueError,
        match="completion clock precedes its provider evidence",
    ):
        sync_data(
            GUARDED_DATE,
            GUARDED_DATE,
            config_path=config_path,
            layout=layout,
            client=GuardBundleClient(),
            datasets=sources.PROVIDER_COMPLETION_DATASETS,
        )

    checkpoint = json.loads(layout.checkpoint_path.read_text(encoding="utf-8"))
    assert marker_written is True
    assert {
        checkpoint["partitions"][f"{dataset}/{GUARDED_DATE}"]["status"]
        for dataset in sources.PROVIDER_COMPLETION_DATASETS
    } == {"reconciling"}
    assert all(
        "completed_at_utc"
        not in checkpoint["partitions"][f"{dataset}/{GUARDED_DATE}"]
        for dataset in sources.PROVIDER_COMPLETION_DATASETS
    )

    monkeypatch.setattr(sources, "_write_json_atomic", real_write_json)
    monkeypatch.setattr(sources.time, "time_ns", real_time_ns)
    recovered = sync_data(
        GUARDED_DATE,
        GUARDED_DATE,
        config_path=config_path,
        layout=layout,
        client=GuardBundleClient(),
        datasets=sources.PROVIDER_COMPLETION_DATASETS,
        resume=True,
    )
    assert recovered["status"] == "complete"


def test_concurrent_subset_publishers_adopt_one_shared_full_bundle_proof(
    tmp_path: Path,
) -> None:
    config_path, layout = _config(tmp_path)
    barrier = threading.Barrier(2)

    def run(dataset: str) -> dict[str, Any]:
        return sync_data(
            GUARDED_DATE,
            GUARDED_DATE,
            config_path=config_path,
            layout=layout,
            client=GuardBundleClient(first_request_barrier=barrier),
            datasets=(dataset,),
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(run, ("daily", "adj_factor")))

    assert [result["status"] for result in results] == ["complete", "complete"]
    checkpoint = json.loads(layout.checkpoint_path.read_text(encoding="utf-8"))
    proofs = {
        checkpoint["partitions"][f"{dataset}/{GUARDED_DATE}"][
            "provider_completion"
        ]["evidence_sha256"]
        for dataset in ("daily", "adj_factor")
    }
    assert len(proofs) == 1

    completed = sync_data(
        GUARDED_DATE,
        GUARDED_DATE,
        config_path=config_path,
        layout=layout,
        client=GuardBundleClient(),
        datasets=sources.PROVIDER_COMPLETION_DATASETS,
        resume=True,
    )
    assert completed["status"] == "complete"
    checkpoint = json.loads(layout.checkpoint_path.read_text(encoding="utf-8"))
    assert len(
        {
            checkpoint["partitions"][f"{dataset}/{GUARDED_DATE}"][
                "provider_completion"
            ]["evidence_sha256"]
            for dataset in sources.PROVIDER_COMPLETION_DATASETS
        }
    ) == 1


def test_guarded_no_resume_does_not_overwrite_a_stable_provider_revision(
    tmp_path: Path,
) -> None:
    config_path, layout = _config(tmp_path)
    first = sync_data(
        GUARDED_DATE,
        GUARDED_DATE,
        config_path=config_path,
        layout=layout,
        client=GuardBundleClient(),
        datasets=sources.PROVIDER_COMPLETION_DATASETS,
    )
    assert first["status"] == "complete"
    before_partitions = json.loads(
        layout.checkpoint_path.read_text(encoding="utf-8")
    )["partitions"]
    before_artifacts = {
        dataset: sources._partition_path(
            layout.raw_root, dataset, GUARDED_DATE
        ).read_bytes()
        for dataset in sources.PROVIDER_COMPLETION_DATASETS
    }

    conflict = sync_data(
        GUARDED_DATE,
        GUARDED_DATE,
        config_path=config_path,
        layout=layout,
        client=GuardBundleClient(tickers=("000001.SZ", "300001.SZ")),
        datasets=sources.PROVIDER_COMPLETION_DATASETS,
        resume=False,
    )

    assert conflict["status"] == "blocked"
    assert conflict["reason"] == "provider_revision_conflict"
    assert json.loads(
        layout.checkpoint_path.read_text(encoding="utf-8")
    )["partitions"] == before_partitions
    assert {
        dataset: sources._partition_path(
            layout.raw_root, dataset, GUARDED_DATE
        ).read_bytes()
        for dataset in sources.PROVIDER_COMPLETION_DATASETS
    } == before_artifacts

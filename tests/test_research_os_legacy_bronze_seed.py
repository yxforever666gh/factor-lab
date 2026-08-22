from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import io
import json
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import pytest

pytest.importorskip("sqlalchemy")
from sqlalchemy import create_engine, event

from factor_lab.research_os.catalog import ResearchCatalog
from factor_lab.research_os.data_quality import sha256_path
from factor_lab.research_os.fingerprint import content_fingerprint
from factor_lab.research_os.legacy_bronze_seed import (
    LEGACY_SEED_SOURCE_ID,
    LEGACY_SEED_TRUST_LABELS,
    LegacyBronzeSeedError,
    LegacyExpandedBronzeSeeder,
    SnapshotPromotionBlocked,
    assert_snapshot_promotion_allowed,
)
from factor_lab.research_os.object_store import S3ImmutableArchive
from factor_lab.research_os.orm import Base
from factor_lab.research_os.production_ledger import (
    PartitionIdentity,
    PartitionStatus,
    ProductionLedger,
)
from factor_lab.research_os.snapshots import build_immutable_snapshot_manifest


NOW = datetime(2026, 8, 23, 8, 0, tzinfo=timezone.utc)
ENVIRONMENT_HASHES = {
    "code_hash": "a" * 64,
    "dependency_lock_hash": "b" * 64,
    "config_hash": "c" * 64,
    "dirty_patch_hash": "d" * 64,
}


class _MemoryFileSystem:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def exists(self, path: str) -> bool:
        return path in self.objects

    def open(self, path: str, mode: str = "rb"):
        if mode == "rb":
            return io.BytesIO(self.objects[path])
        if mode != "wb":
            raise ValueError(mode)
        filesystem = self

        class _Writer(io.BytesIO):
            def close(self) -> None:
                filesystem.objects[path] = self.getvalue()
                super().close()

        return _Writer()


class _FailOnceArchive:
    def __init__(self, delegate: S3ImmutableArchive) -> None:
        self.delegate = delegate
        self.failed = False

    def archive_file(self, path, *, logical_path):
        if not self.failed:
            self.failed = True
            raise RuntimeError("injected object-store interruption")
        return self.delegate.archive_file(path, logical_path=logical_path)


def _contract(dataset: str) -> dict:
    fields = {
        "daily": (
            "ts_code",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "vol",
            "amount",
        ),
        "daily_basic": (
            "ts_code",
            "trade_date",
            "turnover_rate",
            "volume_ratio",
            "pe",
            "pb",
            "total_mv",
            "circ_mv",
        ),
        "adj_factor": ("ts_code", "trade_date", "adj_factor"),
    }[dataset]
    return {
        "dataset": dataset,
        "key_fields": ["ts_code", "trade_date"],
        "event_time_field": "trade_date",
        "release_timing": "after close",
        "allows_empty": False,
        "fields": [
            {
                "name": name,
                "dtype": "string" if name in {"ts_code", "trade_date"} else "float64",
                "nullable": False,
            }
            for name in fields
        ],
    }


def _config(seed_root: Path) -> dict:
    return {
        "daily": {
            "bootstrap": {
                "legacy_bronze_seed": {
                    "mode": "hash_verified_checkpoint",
                    "root": str(seed_root),
                    "checkpoint": "download_checkpoint.json",
                    "datasets": ["daily", "daily_basic", "adj_factor"],
                    "promotion_policy": "bronze_only_fail_closed",
                }
            },
            "sources": [
                {
                    "source": "tushare",
                    "profile_name": "primary-tushare",
                    "partition_cadence": {"kind": "trading_session"},
                    "request": {
                        "dataset": dataset,
                        "fields": [
                            field["name"] for field in _contract(dataset)["fields"]
                        ],
                    },
                    "contract": _contract(dataset),
                }
                for dataset in ("daily", "daily_basic", "adj_factor")
            ],
        }
    }


def _frame(dataset: str, partition_key: str) -> pd.DataFrame:
    rows = {
        "daily": {
            "ts_code": ["000001.SZ"],
            "trade_date": [partition_key],
            "open": [10.0],
            "high": [11.0],
            "low": [9.0],
            "close": [10.5],
            "pre_close": [10.0],
            "vol": [100.0],
            "amount": [1_000.0],
        },
        "daily_basic": {
            "ts_code": ["000001.SZ"],
            "trade_date": [partition_key],
            "turnover_rate": [1.0],
            "volume_ratio": [1.0],
            "pe": [10.0],
            "pb": [1.0],
            "total_mv": [100.0],
            "circ_mv": [80.0],
        },
        "adj_factor": {
            "ts_code": ["000001.SZ"],
            "trade_date": [partition_key],
            "adj_factor": [1.0],
        },
    }
    return pd.DataFrame(rows[dataset])


def _write_seed(
    seed_root: Path,
    entries: list[tuple[str, str]],
    *,
    mutate: Callable[[str, str, dict[str, Any], Path], None] | None = None,
) -> dict:
    partitions: dict[str, dict] = {}
    for dataset, partition_key in entries:
        target = (
            seed_root
            / "raw_market"
            / dataset
            / f"trade_date={partition_key}"
            / "part-000.parquet"
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        frame = _frame(dataset, partition_key)
        frame.to_parquet(target, index=False)
        row = {
            "status": "complete",
            "dataset": dataset,
            "trade_date": partition_key,
            "path": (
                f"artifacts\\expanded_long_only\\raw_market\\{dataset}\\"
                f"trade_date={partition_key}\\part-000.parquet"
            ),
            "sha256": sha256_path(target),
            "row_count": len(frame),
            "size_bytes": target.stat().st_size,
            "completed_at_utc": "2026-08-21T17:00:00+08:00",
        }
        if mutate is not None:
            mutate(dataset, partition_key, row, target)
        partitions[f"{dataset}/{partition_key}"] = row
    seed_root.mkdir(parents=True, exist_ok=True)
    checkpoint = {"schema_version": 1, "partitions": partitions}
    (seed_root / "download_checkpoint.json").write_text(
        json.dumps(checkpoint), encoding="utf-8"
    )
    reference = seed_root / "reference"
    reference.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        columns=("ts_code", "name", "start_date", "end_date", "change_reason")
    ).to_parquet(reference / "historical_st.parquet", index=False)
    return checkpoint


@pytest.fixture
def authority(tmp_path: Path):
    database = f"sqlite:///{tmp_path / 'authority.db'}"
    engine = create_engine(database)

    @event.listens_for(engine, "connect")
    def enable_sqlite_foreign_keys(dbapi_connection, _connection_record) -> None:
        dbapi_connection.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(engine)
    catalog = ResearchCatalog(database)
    ledger = ProductionLedger(engine)
    for index, partition_key in enumerate(
        ("2016-06-01", "2016-07-07", "2020-01-02")
    ):
        identity = PartitionIdentity(
            "research_os", "accepted_trade_calendar", partition_key
        )
        ledger.ensure_partition(identity, created_at=NOW + timedelta(seconds=index))
        lease = ledger.claim(
            owner=f"calendar-{index}",
            identity=identity,
            now=NOW + timedelta(seconds=index + 1),
            lease_for=timedelta(minutes=5),
        )
        assert lease is not None
        ledger.finish(
            lease,
            status=PartitionStatus.SUCCEEDED,
            completed_at=NOW + timedelta(seconds=index + 2),
            output_hash=str(index + 1) * 64,
        )
    yield tmp_path, catalog, ledger
    ledger.close()
    catalog.close()
    engine.dispose()


def _seeder(
    tmp_path: Path,
    catalog: ResearchCatalog,
    ledger: ProductionLedger,
    *,
    archive=None,
) -> tuple[LegacyExpandedBronzeSeeder, Path]:
    runtime_data = tmp_path / "runtime-data"
    seed_root = runtime_data / "legacy" / "expanded_long_only"
    selected_archive = archive or S3ImmutableArchive(
        bucket="factor-lab", filesystem=_MemoryFileSystem()
    )
    return (
        LegacyExpandedBronzeSeeder(
            catalog=catalog,
            ledger=ledger,
            archive=selected_archive,
            config=_config(seed_root),
            runtime_data_root=runtime_data,
            lake_root=tmp_path / "lake",
            snapshot_root=tmp_path / "snapshots",
            environment_hashes=ENVIRONMENT_HASHES,
        ),
        seed_root,
    )


def test_seed_is_blocked_bronze_only_and_all_vendor_partitions_remain_pending(
    authority,
) -> None:
    tmp_path, catalog, ledger = authority
    seeder, seed_root = _seeder(tmp_path, catalog, ledger)
    _write_seed(
        seed_root,
        [(dataset, "2016-07-07") for dataset in ("daily", "daily_basic", "adj_factor")],
    )

    first = seeder.prepare(through=date(2020, 1, 2), now=NOW + timedelta(hours=1))

    assert first.seed_imported_count == 3
    assert first.seed_failed_count == 0
    assert first.canonical_partition_count == 9
    assert first.canonical_pending_count == 9
    assert first.pending_reason_counts["missing_2016_prewarm"] == 3
    assert first.pending_reason_counts["missing_post_2019_vendor_partition"] == 2
    assert first.missing_by_dataset == {"daily": 2, "daily_basic": 2, "adj_factor": 2}
    assert first.st_history_unverified is True
    assert first.st_history_row_count == 0
    assert first.st_history_reason == "legacy_st_history_empty"

    seed_run_ids: set[str] = set()
    for dataset in ("daily", "daily_basic", "adj_factor"):
        canonical = ledger.get_partition(
            PartitionIdentity("primary-tushare", dataset, "2016-07-07")
        )
        assert canonical is not None and canonical.status is PartitionStatus.PENDING
        seed = ledger.get_partition(
            PartitionIdentity(
                LEGACY_SEED_SOURCE_ID, f"bronze-seed-{dataset}", "2016-07-07"
            )
        )
        assert seed is not None and seed.status is PartitionStatus.SUCCEEDED
        assert seed.run_id is not None
        seed_run_ids.add(seed.run_id)
        snapshot = catalog.get_snapshot(seed.output_snapshot_id)
        assert snapshot is not None
        assert snapshot.reference.tier.value == "bronze"
        assert snapshot.reference.quality_status.value == "quarantined"
        assert snapshot.reference.manifest["quality_status"] == "blocked"
        assert set(LEGACY_SEED_TRUST_LABELS).issubset(
            snapshot.reference.trust_labels
        )
        with pytest.raises(SnapshotPromotionBlocked):
            assert_snapshot_promotion_allowed(catalog, (seed.output_snapshot_id,))

    assert len(seed_run_ids) == 1
    seed_run = catalog.get_run(next(iter(seed_run_ids)))
    assert seed_run is not None
    assert seed_run.run_id.startswith("legacy_seed_")
    assert seed_run.run_type == "legacy_bronze_seed_import"
    assert seed_run.status == "succeeded"
    assert seed_run.metadata["seed_imported_count"] == 3
    original_started_at = seed_run.started_at

    seed_daily = ledger.get_partition(
        PartitionIdentity(
            LEGACY_SEED_SOURCE_ID, "bronze-seed-daily", "2016-07-07"
        )
    )
    derived = tmp_path / "lake" / "silver" / "derived.parquet"
    derived.parent.mkdir(parents=True, exist_ok=True)
    _frame("daily", "2016-07-07").to_parquet(derived, index=False)
    silver = build_immutable_snapshot_manifest(
        (derived,),
        base_dir=tmp_path / "lake",
        tier="silver",
        as_of=NOW,
        parent_snapshot_ids=(seed_daily.output_snapshot_id,),
        environment_hashes=ENVIRONMENT_HASHES,
        quality_report={"status": "pass"},
        trust_labels=("point_in_time", "field_reconciled"),
    )
    catalog.register_snapshot(silver.to_snapshot_ref(uri="s3://factor-lab/forged-silver"))
    with pytest.raises(SnapshotPromotionBlocked, match="st_history_unverified"):
        assert_snapshot_promotion_allowed(catalog, (silver.snapshot_id,))

    retried = seeder.prepare(through=date(2020, 1, 2), now=NOW + timedelta(hours=2))
    assert retried.seed_imported_count == 0
    assert retried.seed_reused_count == 3
    assert len(catalog.list_snapshots(limit=100)) == 4
    stored_runs = catalog.list_runs(run_type="legacy_bronze_seed_import")
    assert len(stored_runs) == 1
    assert stored_runs[0].run_id == seed_run.run_id
    assert stored_runs[0].started_at == original_started_at
    assert stored_runs[0].status == "succeeded"


@pytest.mark.parametrize("failure", ["hash", "path", "rows"])
def test_seed_tampering_never_satisfies_canonical_backfill(authority, failure: str) -> None:
    tmp_path, catalog, ledger = authority
    seeder, seed_root = _seeder(tmp_path, catalog, ledger)

    def mutate(_dataset, _partition_key, row, _target):
        if failure == "hash":
            row["sha256"] = "0" * 64
        elif failure == "path":
            row["path"] = "artifacts\\expanded_long_only\\..\\outside.parquet"
        else:
            row["row_count"] = 2

    _write_seed(seed_root, [("daily", "2016-07-07")], mutate=mutate)
    result = seeder.prepare(through=date(2020, 1, 2), now=NOW + timedelta(hours=1))

    assert result.seed_failed_count == 1
    assert result.seed_imported_count == 0
    assert result.missing_by_dataset["daily"] == 3
    canonical = ledger.get_partition(
        PartitionIdentity("primary-tushare", "daily", "2016-07-07")
    )
    assert canonical is not None and canonical.status is PartitionStatus.PENDING
    failed = ledger.get_partition(
        PartitionIdentity(
            LEGACY_SEED_SOURCE_ID, "bronze-seed-daily", "2016-07-07"
        )
    )
    assert failed is not None and failed.status is PartitionStatus.FAILED
    assert failed.run_id is not None
    failed_run = catalog.get_run(failed.run_id)
    assert failed_run is not None
    assert failed_run.status == "failed"
    assert catalog.list_snapshots(limit=100) == []


def test_fixed_seed_root_cannot_traverse_a_reparse_point(
    authority, monkeypatch: pytest.MonkeyPatch
) -> None:
    from factor_lab.research_os import legacy_bronze_seed as seed_module

    tmp_path, catalog, ledger = authority
    seeder, seed_root = _seeder(tmp_path, catalog, ledger)
    _write_seed(seed_root, [("daily", "2016-07-07")])
    original = seed_module._is_link_or_reparse
    fixed_root = seed_root.absolute()

    def marks_fixed_root(path: Path) -> bool:
        return Path(path).absolute() == fixed_root or original(path)

    monkeypatch.setattr(seed_module, "_is_link_or_reparse", marks_fixed_root)
    with pytest.raises(LegacyBronzeSeedError, match="symlink/reparse"):
        seeder.prepare(through=date(2020, 1, 2), now=NOW + timedelta(hours=1))

    canonical = ledger.get_partition(
        PartitionIdentity("primary-tushare", "daily", "2016-07-07")
    )
    assert canonical is not None and canonical.status is PartitionStatus.PENDING


def test_interrupted_seed_import_resumes_exactly(authority) -> None:
    tmp_path, catalog, ledger = authority
    filesystem = _MemoryFileSystem()
    stable = S3ImmutableArchive(bucket="factor-lab", filesystem=filesystem)
    interrupted = _FailOnceArchive(stable)
    seeder, seed_root = _seeder(
        tmp_path, catalog, ledger, archive=interrupted
    )
    _write_seed(seed_root, [("daily", "2016-07-07")])

    first = seeder.prepare(through=date(2020, 1, 2), now=NOW + timedelta(hours=1))
    assert first.seed_failed_count == 1
    identity = PartitionIdentity(
        LEGACY_SEED_SOURCE_ID, "bronze-seed-daily", "2016-07-07"
    )
    failed_partition = ledger.get_partition(identity)
    assert failed_partition.attempts == 1
    assert failed_partition.run_id is not None
    failed_run = catalog.get_run(failed_partition.run_id)
    assert failed_run is not None and failed_run.status == "failed"
    original_started_at = failed_run.started_at

    resumed, _ = _seeder(tmp_path, catalog, ledger, archive=stable)
    second = resumed.prepare(
        through=date(2020, 1, 2), now=NOW + timedelta(hours=2)
    )
    assert second.seed_imported_count == 1
    completed = ledger.get_partition(identity)
    assert completed is not None and completed.status is PartitionStatus.SUCCEEDED
    assert completed.attempts == 2
    assert completed.output_snapshot_id
    assert completed.run_id != failed_run.run_id
    resumed_run = catalog.get_run(completed.run_id)
    assert resumed_run is not None and resumed_run.status == "succeeded"
    assert resumed_run.started_at == NOW + timedelta(hours=2)
    assert catalog.get_run(failed_run.run_id).status == "failed"
    assert catalog.get_run(failed_run.run_id).started_at == original_started_at
    stored_runs = catalog.list_runs(run_type="legacy_bronze_seed_import")
    assert len(stored_runs) == 2
    assert {run.status for run in stored_runs} == {"failed", "succeeded"}


def test_active_seed_lease_is_not_stolen(authority) -> None:
    tmp_path, catalog, ledger = authority
    seeder, seed_root = _seeder(tmp_path, catalog, ledger)
    checkpoint = _write_seed(seed_root, [("daily", "2016-07-07")])
    checkpoint_path = seed_root / "download_checkpoint.json"
    checkpoint_sha256 = sha256_path(checkpoint_path)
    key = "daily/2016-07-07"
    input_hash = content_fingerprint(
        {
            "checkpoint_sha256": checkpoint_sha256,
            "partition_key": key,
            "checkpoint_entry": checkpoint["partitions"][key],
            "trust_labels": LEGACY_SEED_TRUST_LABELS,
        },
        domain="factor-lab/research-os/v1/legacy-bronze-seed-input",
    )
    identity = PartitionIdentity(
        LEGACY_SEED_SOURCE_ID, "bronze-seed-daily", "2016-07-07"
    )
    ledger.ensure_partition(identity, created_at=NOW, input_hash=input_hash)
    lease = ledger.claim(
        identity=identity,
        owner="other-importer",
        now=NOW + timedelta(minutes=1),
        lease_for=timedelta(hours=1),
    )
    assert lease is not None

    result = seeder.prepare(
        through=date(2020, 1, 2), now=NOW + timedelta(minutes=2)
    )
    assert result.seed_busy_count == 1
    assert result.seed_imported_count == 0
    assert ledger.get_partition(identity).lease_owner == "other-importer"
    parent_runs = catalog.list_runs(run_type="legacy_bronze_seed_import")
    assert len(parent_runs) == 1
    assert parent_runs[0].status == "running"

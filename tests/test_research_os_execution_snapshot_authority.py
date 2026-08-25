from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import hashlib
import inspect
import io
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping

import pandas as pd
import pytest

pytest.importorskip("sqlalchemy")
from sqlalchemy import create_engine, select, update

from factor_lab.research_os import orm
from factor_lab.research_os.catalog import ResearchCatalog
from factor_lab.research_os.contracts import (
    DataQualityStatus,
    DataSnapshotRef,
    SnapshotTier,
)
from factor_lab.research_os.data_sources import (
    DatasetContract,
    DiemengSourceAdapter,
    FetchRequest,
    FieldContract,
    ProbeResult,
    SourceBatch,
    SourceAdapter,
    SourceHealth,
    validate_source_frame,
)
from factor_lab.research_os.execution_snapshot_authority import (
    BUNDLE_ROLE,
    CAPABILITY_DATASET,
    ExecutionCapabilityDecision,
    ExecutionEvidenceConflict,
    ExecutionEvidenceUnavailable,
    ExecutionNetworkBlocked,
    ExecutionSnapshotAuthority,
    ExecutionSnapshotPolicy,
    FORMAL_EXECUTION_CAPABILITY_SCHEMA_VERSION,
    OPEN_DATASET,
    OPEN_SOURCE_DATASET,
    OUTPUT_DATASET,
    PyIcebergRegisteredGoldReader,
)
from factor_lab.research_os.execution_open_sources import TushareRealtimeOpenAdapter
import factor_lab.research_os.execution_snapshot_authority as execution_authority_module
from factor_lab.research_os.fingerprint import content_fingerprint
from factor_lab.research_os.object_store import S3ImmutableArchive
from factor_lab.research_os.production_config import ProductionConfigurationError
from factor_lab.research_os.production_ledger import (
    CapabilityRecord,
    CapabilityStatus,
    PartitionIdentity,
    PartitionStatus,
    ProductionLedger,
)


SESSION = date(2026, 8, 21)
PRIOR = date(2026, 8, 20)
DECISION_PRIOR = date(2026, 8, 19)
NOW = datetime(2026, 8, 21, 9, 0, tzinfo=timezone.utc)
OPEN_UTC = datetime(2026, 8, 21, 1, 30, tzinfo=timezone.utc)
MARK_UTC = datetime(2026, 8, 21, 7, 0, tzinfo=timezone.utc)
GOLD_AS_OF = datetime(2026, 8, 21, 8, 0, tzinfo=timezone.utc)
PRIOR_AVAILABLE = datetime(2026, 8, 20, 8, 0, tzinfo=timezone.utc)
TICKERS = ("000001.SZ", "000002.SZ", "600000.SH")


class _MemoryFileSystem:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.fail_role: str | None = None
        self.failed = False

    def exists(self, path: str) -> bool:
        return path in self.objects

    def open(self, path: str, mode: str = "rb"):
        if mode == "rb":
            return io.BytesIO(self.objects[path])
        if mode != "wb":
            raise ValueError(mode)
        if self.fail_role and self.fail_role in path and not self.failed:
            self.failed = True
            raise OSError("injected object-store interruption")
        filesystem = self

        class _Writer(io.BytesIO):
            def close(self) -> None:
                filesystem.objects[path] = self.getvalue()
                super().close()

        return _Writer()


class _FixtureGoldReader:
    def __init__(self, frames: Mapping[str, pd.DataFrame]) -> None:
        self.frames = {
            str(key): value.copy(deep=True) for key, value in frames.items()
        }
        self.calls = 0

    @property
    def production_attested(self) -> bool:
        return False

    def read(self, reference: DataSnapshotRef) -> pd.DataFrame:
        self.calls += 1
        return self.frames[reference.snapshot_id].copy(deep=True)


def _open_contract() -> DatasetContract:
    return DatasetContract(
        dataset=OPEN_SOURCE_DATASET,
        key_fields=("stock_code", "trade_time"),
        fields=(
            FieldContract("stock_code", "string", nullable=False),
            FieldContract("trade_time", "datetime", nullable=False),
            FieldContract("open", "float64", nullable=False),
            FieldContract("high", "float64", nullable=False),
            FieldContract("low", "float64", nullable=False),
            FieldContract("close", "float64", nullable=False),
            FieldContract("vol", "float64", nullable=False),
            FieldContract("amount", "float64", nullable=False),
        ),
        event_time_field="trade_time",
        release_timing="physical vendor observation at event timestamp",
        allows_empty=True,
    )


class _FixtureDiemengAdapter(SourceAdapter):
    def __init__(
        self,
        prices: Mapping[str, float],
        *,
        missing: set[str] | None = None,
        event_time: datetime = OPEN_UTC,
        ingested_at: datetime = OPEN_UTC,
        extra_forward_column: bool = False,
    ) -> None:
        super().__init__(
            source_id="diemeng",
            priority=20,
            contracts=(_open_contract(),),
            lineage={"fixture": True},
        )
        self.prices = dict(prices)
        self.missing = set(missing or ())
        self.event_time = event_time
        self.ingested_at = ingested_at
        self.extra_forward_column = extra_forward_column
        self.probe_calls = 0
        self.fetch_calls = 0

    def probe(self) -> ProbeResult:
        self.probe_calls += 1
        return ProbeResult(
            source_id="diemeng",
            health=SourceHealth.HEALTHY,
            checked_at=NOW,
            latency_ms=1.0,
            datasets=(OPEN_SOURCE_DATASET,),
            message="fixture reachable",
        )

    def _fetch_frame(self, request: FetchRequest) -> pd.DataFrame:
        self.fetch_calls += 1
        ticker = str(request.parameters["stock_code"])
        if ticker in self.missing:
            return pd.DataFrame(
                columns=[item.name for item in self.contract_for(OPEN_SOURCE_DATASET).fields]
            )
        price = self.prices[ticker]
        row: dict[str, Any] = {
            "stock_code": ticker,
            "trade_time": self.event_time,
            "open": price,
            "high": price + 0.1,
            "low": price - 0.1,
            "close": price,
            "vol": 1000.0,
            "amount": 10_000.0,
        }
        if self.extra_forward_column:
            row["forward_return"] = 99.0
        return pd.DataFrame([row])

    def fetch(self, request: FetchRequest) -> SourceBatch:
        contract = self.contract_for(request.dataset)
        frame = self._fetch_frame(request)
        validate_source_frame(frame, contract)
        return SourceBatch(
            source_id=self.source_id,
            source_priority=self.priority,
            dataset=request.dataset,
            frame=frame,
            ingested_at=self.ingested_at,
            vendor_revision=hashlib.sha256(
                frame.to_json(orient="split", date_format="iso").encode("utf-8")
            ).hexdigest(),
            contract=contract,
            request=request,
            lineage={"fixture": True},
        )


def _calendar() -> dict[str, Any]:
    sessions = [
        DECISION_PRIOR.isoformat(),
        PRIOR.isoformat(),
        SESSION.isoformat(),
    ]
    return {
        "source": "reconciled_silver:trade_calendar",
        "quality_status": "accepted",
        "sessions": sessions,
        "content_hash": hashlib.sha256("\n".join(sessions).encode("ascii")).hexdigest(),
    }


def _gold_frame(
    *,
    missing_status: bool = False,
    status_available_at: datetime = datetime(2026, 8, 21, 1, 0, tzinfo=timezone.utc),
    conflicting_limits: bool = False,
    suspended: set[str] | None = None,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    suspended = set(suspended or ())
    for index, ticker in enumerate(TICKERS):
        prior_close = 10.0 + index
        rows.append(
            {
                "ts_code": ticker,
                "trade_date": PRIOR.isoformat(),
                "universe_member": True,
                "benchmark_weight": 1.0 / 3.0,
                "close_adj": prior_close,
                "close": prior_close,
                "adv_20": 10_000_000.0,
                "volatility_20": 0.2,
                "up_limit": prior_close * 1.1,
                "down_limit": prior_close * 0.9,
                "is_suspended": False,
                "is_delisted": False,
                "trade_status_available_at": status_available_at,
                "delist_status_available_at": status_available_at,
                "daily_available_at": PRIOR_AVAILABLE,
                "adj_factor_available_at": PRIOR_AVAILABLE,
                "has_company_action": False,
                "company_action_available_at": pd.NaT,
                "stk_div": 0.0,
                "cash_div": 0.0,
            }
        )
        today_close = prior_close * (1.0 + 0.01 * (index + 1))
        up_limit = prior_close * 1.1
        down_limit = up_limit if conflicting_limits else prior_close * 0.9
        row = {
            "ts_code": ticker,
            "trade_date": SESSION.isoformat(),
            "universe_member": True,
            "benchmark_weight": 1.0 / 3.0,
            "close_adj": today_close,
            "close": today_close,
            "adv_20": 10_000_000.0 + index,
            "volatility_20": 0.2 + index * 0.01,
            "up_limit": up_limit,
            "down_limit": down_limit,
            "is_suspended": ticker in suspended,
            "is_delisted": False,
            "trade_status_available_at": status_available_at,
            "delist_status_available_at": status_available_at,
            "daily_available_at": GOLD_AS_OF,
            "adj_factor_available_at": GOLD_AS_OF,
            "has_company_action": False,
            "company_action_available_at": pd.NaT,
            "stk_div": 0.0,
            "cash_div": 0.0,
        }
        if missing_status:
            row.pop("trade_status_available_at")
        rows.append(row)
    frame = pd.DataFrame(rows)
    frame["company_action_available_at"] = pd.to_datetime(
        frame["company_action_available_at"], errors="coerce", utc=True
    )
    if missing_status:
        frame = frame.drop(columns="trade_status_available_at")
    return frame


def _finish_partition(
    ledger: ProductionLedger,
    identity: PartitionIdentity,
    *,
    output_hash: str,
    output_snapshot_id: str | None = None,
    details: Mapping[str, Any] | None = None,
    completed_at: datetime = NOW,
) -> None:
    ledger.ensure_partition(identity, created_at=completed_at)
    lease = ledger.claim(
        owner="fixture",
        identity=identity,
        now=completed_at,
        lease_for=timedelta(minutes=30),
    )
    assert lease is not None
    ledger.finish(
        lease,
        status=PartitionStatus.SUCCEEDED,
        completed_at=completed_at,
        output_hash=output_hash,
        output_snapshot_id=output_snapshot_id,
        details=dict(details or {}),
    )


@pytest.fixture
def authority_factory(tmp_path: Path):
    resources: list[Any] = []

    def factory(
        *,
        gold_frame: pd.DataFrame | None = None,
        adapter: _FixtureDiemengAdapter | None = None,
        runtime_mode: str = "test",
        filesystem: _MemoryFileSystem | None = None,
    ):
        database = tmp_path / f"authority-{len(resources)}.db"
        catalog = ResearchCatalog(database)
        catalog.initialize_schema()
        engine = create_engine(f"sqlite:///{database}")
        # ProductionLedger is Alembic-owned in production.  The native
        # ResearchCatalog SQLite fallback intentionally initializes only its
        # own compatibility schema, so tests create the 0007 ORM tables here.
        orm.Base.metadata.create_all(engine)
        ledger = ProductionLedger(engine)
        filesystem = filesystem or _MemoryFileSystem()
        archive = S3ImmutableArchive(
            bucket="factor-lab", filesystem=filesystem, prefix="research-os"
        )

        silver_ref = DataSnapshotRef(
            snapshot_id="silver_dq",
            tier=SnapshotTier.SILVER,
            uri="s3://factor-lab/research-os/fixture/silver",
            content_hash="1" * 64,
            as_of=GOLD_AS_OF,
            quality_status=DataQualityStatus.ACCEPTED,
        )
        catalog.register_snapshot(silver_ref)
        gold_ref = DataSnapshotRef(
            snapshot_id="gold_authoritative",
            tier=SnapshotTier.GOLD,
            uri="iceberg://factorlab/factor_lab.gold_research_panel#ros_gold",
            content_hash="2" * 64,
            parent_snapshot_ids=(silver_ref.snapshot_id,),
            as_of=GOLD_AS_OF,
            quality_status=DataQualityStatus.ACCEPTED,
            trust_labels=(
                "point_in_time",
                "quality_accepted",
                "historical_st_verified",
                "field_reconciled",
            ),
            manifest={"trading_calendar": _calendar()},
        )
        catalog.register_snapshot(gold_ref)
        decision_frame = (
            gold_frame.copy(deep=True)
            if gold_frame is not None
            else _gold_frame()
        )
        decision_frame = decision_frame.loc[
            pd.to_datetime(decision_frame["trade_date"], errors="coerce").dt.date
            == PRIOR
        ].copy()
        decision_gold_ref = DataSnapshotRef(
            snapshot_id="gold_decision",
            tier=SnapshotTier.GOLD,
            uri="iceberg://factorlab/factor_lab.gold_research_panel#ros_gold_prior",
            content_hash="6" * 64,
            parent_snapshot_ids=(silver_ref.snapshot_id,),
            as_of=PRIOR_AVAILABLE,
            quality_status=DataQualityStatus.ACCEPTED,
            trust_labels=(
                "point_in_time",
                "quality_accepted",
                "historical_st_verified",
                "field_reconciled",
            ),
            manifest={"trading_calendar": _calendar()},
        )
        catalog.register_snapshot(decision_gold_ref)
        _finish_partition(
            ledger,
            PartitionIdentity("research_os", "accepted_trade_calendar", PRIOR.isoformat()),
            output_hash="7" * 64,
            completed_at=PRIOR_AVAILABLE,
        )
        _finish_partition(
            ledger,
            PartitionIdentity("research_os", "accepted_trade_calendar", SESSION.isoformat()),
            output_hash="3" * 64,
        )
        _finish_partition(
            ledger,
            PartitionIdentity("research_os", "stage_data_quality", PRIOR.isoformat()),
            output_hash="8" * 64,
            output_snapshot_id=silver_ref.snapshot_id,
            details={
                "operation_result": {
                    "status": "completed",
                    "outputs": {"quality_report": {"status": "pass"}},
                }
            },
            completed_at=PRIOR_AVAILABLE,
        )
        _finish_partition(
            ledger,
            PartitionIdentity("research_os", "stage_gold", PRIOR.isoformat()),
            output_hash="9" * 64,
            output_snapshot_id=decision_gold_ref.snapshot_id,
            details={
                "operation_result": {
                    "status": "completed",
                    "outputs": {"snapshot_id": decision_gold_ref.snapshot_id},
                }
            },
            completed_at=PRIOR_AVAILABLE,
        )
        _finish_partition(
            ledger,
            PartitionIdentity("research_os", "stage_data_quality", SESSION.isoformat()),
            output_hash="4" * 64,
            output_snapshot_id=silver_ref.snapshot_id,
            details={
                "operation_result": {
                    "status": "completed",
                    "outputs": {"quality_report": {"status": "pass"}},
                }
            },
        )
        _finish_partition(
            ledger,
            PartitionIdentity("research_os", "stage_gold", SESSION.isoformat()),
            output_hash="5" * 64,
            output_snapshot_id=gold_ref.snapshot_id,
            details={
                "operation_result": {
                    "status": "completed",
                    "outputs": {"snapshot_id": gold_ref.snapshot_id},
                }
            },
        )
        current_frame = gold_frame if gold_frame is not None else _gold_frame()
        reader = _FixtureGoldReader(
            {
                gold_ref.snapshot_id: current_frame,
                decision_gold_ref.snapshot_id: decision_frame,
            }
        )
        adapter = adapter or _FixtureDiemengAdapter(
            {ticker: 10.0 + index for index, ticker in enumerate(TICKERS)}
        )
        authority = ExecutionSnapshotAuthority(
            catalog=catalog,
            ledger=ledger,
            archive=archive,
            gold_reader=reader,
            cache_root=tmp_path / f"cache-{len(resources)}",
            diemeng_adapter=adapter,
            runtime_mode=runtime_mode,
            policy=ExecutionSnapshotPolicy(target_universe_size=3),
            now=lambda: NOW,
        )
        resources.append((catalog, ledger, engine))
        return authority, catalog, ledger, filesystem, adapter, reader

    yield factory
    for catalog, ledger, engine in resources:
        ledger.close()
        catalog.close()
        engine.dispose()


def _production_execution_payload() -> dict[str, Any]:
    return {
        "iceberg": {"catalog_name": "factorlab"},
        "daily": {
            "shadow": {
                "execution_market_data": {
                    "source": "diemeng",
                    "profile_name": "primary-diemeng",
                    "credential_ref": "secret://diemeng_api_key",
                    "base_url": "https://mg.diemeng.chat/api",
                    "dataset": "minute_history",
                    "endpoint": "/stock/history",
                    "method": "POST",
                    "response_path": "data.list",
                    "request": {
                        "stock_code": "${ticker}",
                        "level": "1min",
                        "start_time": "${partition_key} 09:30:00",
                        "end_time": "${partition_key} 15:00:00",
                        "page": 0,
                        "page_size": 10000,
                    },
                    "contract": {
                        "key_fields": ["stock_code", "trade_time"],
                        "event_time_field": "trade_time",
                        "fields": [
                            "stock_code",
                            "trade_time",
                            "open",
                            "high",
                            "low",
                            "close",
                            "vol",
                            "amount",
                        ],
                    },
                }
            }
        },
    }


def _production_tushare_execution_payload() -> dict[str, Any]:
    return {
        "iceberg": {"catalog_name": "factorlab"},
        "daily": {
            "sources": [
                {
                    "source": "tushare",
                    "rate_limits": {
                        "__account__": {
                            "requests": 60,
                            "per_seconds": 60,
                            "burst": 1,
                        }
                    },
                }
            ],
            "shadow": {
                "execution_market_data": {
                    "source": "tushare",
                    "profile_name": "primary-tushare",
                    "credential_ref": "secret://tushare_token",
                    "dataset": "rt_min",
                    "endpoint": "rt_min",
                    "method": "SDK",
                    "rate_limits": {
                        "__account__": {
                            "requests": 60,
                            "per_seconds": 60,
                            "burst": 1,
                        }
                    },
                    "request": {
                        "ts_code": "${decision_universe_csv}",
                        "freq": "1MIN",
                    },
                    "batching": {
                        "mode": "sorted_deterministic_chunks",
                        "maximum_symbols_per_request": 300,
                    },
                    "contract": {
                        "key_fields": ["ts_code", "time"],
                        "event_time_field": "time",
                        "fields": [
                            "ts_code",
                            "time",
                            "open",
                            "close",
                            "high",
                            "low",
                            "vol",
                            "amount",
                        ],
                    },
                    "availability": {
                        "mode": "collector_ingested_at",
                        "event_time_field": "time",
                        "available_at_field": "ingested_at",
                        "maximum_delay_minutes": 5,
                    },
                    "formal_capability": {
                        "status": "runtime_probe_required",
                        "formal_shadow_projection": "runtime_probe_gated",
                    },
                }
            }
        },
    }


def _operator_retention_waiver(
    credential_ref: str = "secret://tushare_token",
    *,
    accepted_at: str = "2026-08-20T00:00:00+00:00",
) -> dict[str, str]:
    return {
        "status": "retained_unrotated_operator_accepted",
        "vendor_confirmation": "not_rotated",
        "credential_ref": credential_ref,
        "accepted_at": accepted_at,
        "reason": "operator_declined_rotation_for_local_research_only_runtime",
    }


def _production_tushare_retention_payload() -> dict[str, Any]:
    payload = _production_tushare_execution_payload()
    payload["security"] = {
        "source_transport": {
            "tushare": {
                "status": "verified_vendor_https",
                "vendor_confirmation": "recorded",
                "api_origin": "https://api.waditu.com/dataapi",
            },
        },
        "credential_rotation": {
            "tushare_token": _operator_retention_waiver(),
        },
    }
    return payload


@pytest.fixture
def production_factory(authority_factory, tmp_path: Path, monkeypatch):
    count = 0

    def factory(
        *,
        payload: Mapping[str, Any] | None = None,
        credential_retention_waivers: tuple[str, ...] = (),
    ):
        nonlocal count
        _, catalog, ledger, filesystem, *_ = authority_factory()
        root = tmp_path / f"production-factory-{count}"
        count += 1
        artifact_root = root / "artifacts"
        secrets_root = root / "secrets"
        artifact_root.mkdir(parents=True)
        secrets_root.mkdir(parents=True)
        (secrets_root / "diemeng_api_key").write_text(
            "fixture-new-diemeng-key-0001\n", encoding="utf-8"
        )
        (secrets_root / "tushare_token").write_text(
            "fixture-new-tushare-token-0001\n", encoding="utf-8"
        )
        config_path = root / "research_os_orchestration.production.json"
        config_path.write_text(
            json.dumps(
                _production_execution_payload() if payload is None else payload
            ),
            encoding="utf-8",
        )
        evidence = SimpleNamespace(
            path=config_path.resolve(),
            runtime_artifact_root=artifact_root.resolve(),
            credential_retention_waivers=credential_retention_waivers,
        )
        monkeypatch.setattr(
            execution_authority_module,
            "validate_production_config",
            lambda *_args, **_kwargs: evidence,
        )
        # The factory intentionally rejects SQLite.  This unit uses the SQLite
        # ORM tables but marks the already-created engine for construction;
        # SQL compilation and storage remain SQLite for the local migration.
        monkeypatch.setattr(ledger.engine.dialect, "name", "postgresql")
        archive = S3ImmutableArchive(
            bucket="factor-lab", filesystem=filesystem, prefix="research-os"
        )
        env = {
            "FACTOR_LAB_PRODUCTION_ROLE": "worker",
            "FACTOR_LAB_SECRETS_ROOT": str(secrets_root),
        }
        authority = ExecutionSnapshotAuthority.from_production_config(
            config_path=config_path,
            env=env,
            catalog=catalog,
            ledger=ledger,
            archive=archive,
            cache_root=artifact_root / "execution-cache",
        )
        return authority, ledger, secrets_root, config_path

    return factory


def _write_rotation_evidence(
    secrets_root: Path,
    *,
    current_hash_override: str | None = None,
    credential: str = "diemeng_api_key",
    vendor: str = "diemeng",
) -> None:
    current = (secrets_root / credential).read_text(encoding="utf-8").strip()
    current_hash = hashlib.sha256(current.encode("utf-8")).hexdigest()
    previous_hash = "a" * 64
    now = datetime.now(timezone.utc)
    baseline = {
        "schema_version": "research-os/credential-rotation-baseline/v1",
        "credential": credential,
        "previous_credential_sha256": previous_hash,
        "captured_at": (now - timedelta(days=2)).isoformat(),
    }
    confirmation = {
        "schema_version": "research-os/vendor-rotation-confirmation/v1",
        "vendor": vendor,
        "credential": credential,
        "previous_credential_sha256": previous_hash,
        "current_credential_sha256": current_hash_override or current_hash,
        "confirmed_at": (now - timedelta(days=1)).isoformat(),
        "confirmation_reference": f"{vendor}-support-ticket-20260823-001",
    }
    (secrets_root / f"{credential}.rotation-baseline.json").write_text(
        json.dumps(baseline, sort_keys=True), encoding="utf-8"
    )
    (secrets_root / f"{credential}.vendor-confirmation.json").write_text(
        json.dumps(confirmation, sort_keys=True), encoding="utf-8"
    )


def test_successful_typed_bars_are_physical_distinct_and_idempotent(
    authority_factory,
) -> None:
    authority, catalog, ledger, filesystem, adapter, reader = authority_factory()

    opening = authority.observe_open(SESSION)
    first = authority.build_session(SESSION)
    second = authority.build_session(SESSION)

    assert opening.quality_status is DataQualityStatus.QUARANTINED
    assert first.capability.decision is ExecutionCapabilityDecision.NON_FORWARD
    assert "non_production_runtime" in first.capability.reasons
    assert first.execution_snapshot.snapshot_id != first.mark_snapshot.snapshot_id
    assert first.bundle_snapshot.tier is SnapshotTier.SILVER
    assert first.execution_snapshot.quality_status is DataQualityStatus.QUARANTINED
    assert first.mark_snapshot.quality_status is DataQualityStatus.QUARANTINED
    assert len(first.bars) == 3
    assert set(first.bars["ticker"]) == set(TICKERS)
    assert first.bars["execution_snapshot_id"].nunique() == 1
    assert first.bars["mark_snapshot_id"].nunique() == 1
    assert first.bars["benchmark_return"].nunique() == 1
    assert first.benchmark_return == pytest.approx((0.01 + 0.02 + 0.03) / 3.0)
    assert second.reused is True
    assert second.execution_snapshot == first.execution_snapshot
    assert second.mark_snapshot == first.mark_snapshot
    assert second.bars.equals(first.bars)
    assert any(BUNDLE_ROLE in key for key in filesystem.objects)
    assert adapter.probe_calls == 1
    assert adapter.fetch_calls == 3
    assert reader.calls >= 2
    record = ledger.get_partition(
        PartitionIdentity("research_os", OUTPUT_DATASET, SESSION.isoformat())
    )
    assert record is not None and record.attempts == 1


def test_typed_execution_schema_rejects_even_benign_extra_columns(
    authority_factory,
) -> None:
    authority, *_ = authority_factory()
    authority.observe_open(SESSION)
    result = authority.build_session(SESSION)
    tampered = result.bars.assign(operator_note="looks harmless")

    with pytest.raises(ExecutionEvidenceConflict, match="schema exactly"):
        authority._validate_typed_bars(
            tampered,
            session=SESSION,
            execution_snapshot_id=result.execution_snapshot.snapshot_id,
            mark_snapshot_id=result.mark_snapshot.snapshot_id,
        )


def test_public_production_methods_have_no_frame_path_snapshot_or_capability_payload() -> None:
    for method_name in ("observe_open", "build_session"):
        parameters = tuple(
            inspect.signature(getattr(ExecutionSnapshotAuthority, method_name)).parameters
        )
        assert parameters == ("self", "trade_date")


@pytest.mark.parametrize(
    ("frame", "message"),
    [
        (_gold_frame(missing_status=True), "omits trade_status_available_at"),
        (_gold_frame(conflicting_limits=True), "price-limit bounds conflict"),
        (
            _gold_frame(
                status_available_at=datetime(2026, 8, 21, 2, 0, tzinfo=timezone.utc)
            ),
            "not available by 09:30",
        ),
    ],
)
def test_missing_conflicting_or_late_status_fails_closed(
    authority_factory, frame: pd.DataFrame, message: str
) -> None:
    authority, *_ = authority_factory(gold_frame=frame)
    authority.observe_open(SESSION)
    with pytest.raises((ExecutionEvidenceUnavailable, ExecutionEvidenceConflict), match=message):
        authority.build_session(SESSION)


def test_missing_active_0930_observation_is_rejected(authority_factory) -> None:
    adapter = _FixtureDiemengAdapter(
        {ticker: 10.0 + index for index, ticker in enumerate(TICKERS)},
        missing={TICKERS[1]},
    )
    authority, _, ledger, *_ = authority_factory(adapter=adapter)

    authority.observe_open(SESSION)
    with pytest.raises(Exception, match="active security lacks a 09:30 observation"):
        authority.build_session(SESSION)

    record = ledger.get_partition(
        PartitionIdentity("diemeng", OPEN_DATASET, SESSION.isoformat())
    )
    assert record is not None and record.status is PartitionStatus.SUCCEEDED
    closure = ledger.get_partition(
        PartitionIdentity("research_os", OUTPUT_DATASET, SESSION.isoformat())
    )
    assert closure is not None and closure.status is PartitionStatus.FAILED


def test_open_time_leak_and_forward_source_column_are_rejected(authority_factory) -> None:
    late = _FixtureDiemengAdapter(
        {ticker: 10.0 + index for index, ticker in enumerate(TICKERS)},
        event_time=datetime(2026, 8, 21, 1, 31, tzinfo=timezone.utc),
    )
    authority, *_ = authority_factory(adapter=late)
    with pytest.raises(ExecutionEvidenceUnavailable, match="09:30"):
        authority.observe_open(SESSION)

    forward = _FixtureDiemengAdapter(
        {ticker: 10.0 + index for index, ticker in enumerate(TICKERS)},
        extra_forward_column=True,
    )
    authority, *_ = authority_factory(adapter=forward)
    with pytest.raises(ValueError, match="forbids forward/label columns"):
        authority.observe_open(SESSION)


class _FixtureTushareRealtimeClient:
    def __init__(self, *, missing: set[str] | None = None) -> None:
        self.missing = set(missing or ())

    def query(self, endpoint: str, **parameters: Any) -> pd.DataFrame:
        assert endpoint == "rt_min"
        requested = str(parameters["ts_code"]).split(",")
        return pd.DataFrame(
            [
                {
                    "ts_code": ticker,
                    "time": "2026-08-21 09:30:00",
                    "open": 10.0 + TICKERS.index(ticker),
                    "close": 10.0 + TICKERS.index(ticker),
                    "high": 10.1 + TICKERS.index(ticker),
                    "low": 9.9 + TICKERS.index(ticker),
                    "vol": 1_000.0,
                    "amount": 10_000.0,
                }
                for ticker in requested
                if ticker not in self.missing
            ],
            columns=(
                "ts_code",
                "time",
                "open",
                "close",
                "high",
                "low",
                "vol",
                "amount",
            ),
        )


def _fixture_tushare_adapter(
    *,
    missing: set[str] | None = None,
    max_symbols_per_request: int = 300,
):
    return TushareRealtimeOpenAdapter(
        _FixtureTushareRealtimeClient(missing=missing),
        receive_clock=lambda: datetime(2026, 8, 21, 1, 31, tzinfo=timezone.utc),
        max_universe_size=3,
        max_symbols_per_request=max_symbols_per_request,
    )


def test_tushare_realtime_batch_uses_dynamic_source_identity_and_receive_time(
    authority_factory,
) -> None:
    authority, _, ledger, *_ = authority_factory(adapter=_fixture_tushare_adapter())

    opening = authority.observe_open(SESSION)
    result = authority.build_session(SESSION)

    assert opening.manifest["role"] == "tushare_open_observation"
    assert opening.manifest["source_id"] == "tushare"
    assert ledger.get_partition(
        PartitionIdentity("tushare", OPEN_DATASET, SESSION.isoformat())
    ) is not None
    assert set(pd.to_datetime(result.bars["execution_available_at"], utc=True)) == {
        pd.Timestamp("2026-08-21T01:31:00Z")
    }


def test_tushare_missing_active_stock_and_closed_open_conflict_fail_closed(
    authority_factory,
) -> None:
    missing, *_ = authority_factory(
        adapter=_fixture_tushare_adapter(missing={TICKERS[1]})
    )
    missing.observe_open(SESSION)
    with pytest.raises(ExecutionEvidenceUnavailable, match="active security lacks"):
        missing.build_session(SESSION)

    conflicting_gold = _gold_frame()
    conflicting_gold["open"] = conflicting_gold["close"]
    current = pd.to_datetime(conflicting_gold["trade_date"]).dt.date.eq(SESSION)
    conflicting_gold.loc[current & conflicting_gold["ts_code"].eq(TICKERS[0]), "open"] = 99.0
    conflicting, *_ = authority_factory(
        gold_frame=conflicting_gold,
        adapter=_fixture_tushare_adapter(),
    )
    conflicting.observe_open(SESSION)
    with pytest.raises(ExecutionEvidenceConflict, match="sources conflict"):
        conflicting.build_session(SESSION)


def test_tushare_two_batches_allow_only_gold_verified_suspended_missing(
    authority_factory,
) -> None:
    missing_tickers = {TICKERS[1], TICKERS[2]}
    authority, *_ = authority_factory(
        gold_frame=_gold_frame(suspended=missing_tickers),
        adapter=_fixture_tushare_adapter(
            missing=missing_tickers,
            max_symbols_per_request=2,
        ),
    )

    opening = authority.observe_open(SESSION)
    result = authority.build_session(SESSION)

    lineage = opening.manifest["source_lineage"]
    assert lineage["request_batch_count"] == 2
    assert [item["missing_ticker_count"] for item in lineage["request_batches"]] == [
        1,
        1,
    ]
    assert lineage["missing_ticker_count"] == 2
    assert len(lineage["missing_ticker_hashes"]) == 2
    bars = result.bars.set_index("ticker")
    assert bars.loc[list(missing_tickers), "is_suspended"].all()
    assert bars.loc[list(missing_tickers), "open_adj"].notna().all()


def test_tushare_collector_clock_cannot_fake_postgresql_receive_time(
    authority_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    authority, *_ = authority_factory(
        adapter=_fixture_tushare_adapter(), runtime_mode="production"
    )
    monkeypatch.setattr(authority, "_require_live_open_window", lambda _session: None)
    monkeypatch.setattr(authority, "_real_diemeng_adapter", lambda: True)
    monkeypatch.setattr(
        authority,
        "_rotation_attestation",
        lambda: SimpleNamespace(evidence_hash="a" * 64),
    )
    monkeypatch.setattr(
        execution_authority_module,
        "_database_now",
        lambda _ledger: datetime(2026, 8, 21, 1, 33, tzinfo=timezone.utc),
    )

    with pytest.raises(ExecutionEvidenceConflict, match="PostgreSQL authority"):
        authority.observe_open(SESSION)


def test_execution_risk_uses_prior_session_and_rejects_late_availability(
    authority_factory,
) -> None:
    frame = _gold_frame()
    current = pd.to_datetime(frame["trade_date"]).dt.date == SESSION
    frame.loc[current, "adv_20"] = 999_999_999.0
    frame.loc[current, "volatility_20"] = 9.0
    authority, *_ = authority_factory(gold_frame=frame)
    authority.observe_open(SESSION)
    result = authority.build_session(SESSION)
    assert result.bars["adv_20"].tolist() == pytest.approx(
        [10_000_000.0, 10_000_001.0, 10_000_002.0]
    )
    assert result.bars["volatility_20"].tolist() == pytest.approx([0.2, 0.2, 0.2])

    late = _gold_frame()
    prior = pd.to_datetime(late["trade_date"]).dt.date == PRIOR
    late.loc[prior, "adj_factor_available_at"] = datetime(
        2026, 8, 21, 2, 0, tzinfo=timezone.utc
    )
    authority, *_ = authority_factory(gold_frame=late)
    authority.observe_open(SESSION)
    with pytest.raises(ExecutionEvidenceConflict, match="risk was unavailable by 09:30"):
        authority.build_session(SESSION)


def test_suspended_security_uses_accepted_status_without_fake_open(
    authority_factory,
) -> None:
    adapter = _FixtureDiemengAdapter(
        {ticker: 10.0 + index for index, ticker in enumerate(TICKERS)},
        missing={TICKERS[0]},
    )
    authority, *_ = authority_factory(
        gold_frame=_gold_frame(suspended={TICKERS[0]}), adapter=adapter
    )

    authority.observe_open(SESSION)
    result = authority.build_session(SESSION)

    suspended = result.bars.set_index("ticker").loc[TICKERS[0]]
    assert bool(suspended["is_suspended"]) is True
    assert suspended["open_adj"] == pytest.approx(10.0)  # prior accepted close
    assert adapter.fetch_calls == 3


def test_evening_closure_requires_persisted_open_observation(authority_factory) -> None:
    authority, _, ledger, *_ = authority_factory()

    with pytest.raises(ExecutionEvidenceUnavailable, match="execution_open_0930"):
        authority.build_session(SESSION)

    closure = ledger.get_partition(
        PartitionIdentity("research_os", OUTPUT_DATASET, SESSION.isoformat())
    )
    assert closure is None


def test_open_is_bound_only_to_prior_closed_decision_snapshot(authority_factory) -> None:
    authority, _, ledger, *_ = authority_factory()

    opening = authority.observe_open(SESSION)

    assert opening.manifest["decision_trade_date"] == PRIOR.isoformat()
    assert opening.manifest["decision_snapshot_id"] == "gold_decision"
    assert opening.parent_snapshot_ids == ("gold_decision",)
    assert opening.manifest["decision_gold_partition_hash"] == "9" * 64
    # The same-day Gold closure is deliberately absent from the open input
    # identity; only the previous immutable close can authorize ticker fetches.
    record = ledger.get_partition(
        PartitionIdentity("diemeng", OPEN_DATASET, SESSION.isoformat())
    )
    assert record is not None
    assert record.details["decision_snapshot_id"] == "gold_decision"
    assert "gold_authoritative" not in json.dumps(record.details, sort_keys=True)


def test_company_action_is_typed_and_must_be_known_before_execution(
    authority_factory,
) -> None:
    frame = _gold_frame()
    target = frame["ts_code"].eq(TICKERS[0]) & pd.to_datetime(
        frame["trade_date"]
    ).dt.date.eq(SESSION)
    frame.loc[target, "has_company_action"] = True
    frame.loc[target, "stk_div"] = 0.1
    frame.loc[target, "cash_div"] = 0.2
    frame.loc[target, "company_action_available_at"] = datetime(
        2026, 8, 21, 0, 30, tzinfo=timezone.utc
    )
    authority, *_ = authority_factory(gold_frame=frame)
    authority.observe_open(SESSION)
    result = authority.build_session(SESSION)
    row = result.bars.set_index("ticker").loc[TICKERS[0]]
    assert row["split_ratio"] == pytest.approx(1.1)
    assert row["cash_dividend"] == pytest.approx(0.2)

    late = frame.copy(deep=True)
    late.loc[target, "company_action_available_at"] = datetime(
        2026, 8, 21, 2, 0, tzinfo=timezone.utc
    )
    authority, *_ = authority_factory(gold_frame=late)
    authority.observe_open(SESSION)
    with pytest.raises(ExecutionEvidenceConflict, match="not known by the opening"):
        authority.build_session(SESSION)


def test_role_swap_is_rejected_even_when_attacker_rehashes_partition(
    authority_factory,
) -> None:
    authority, _, ledger, *_ = authority_factory()
    authority.observe_open(SESSION)
    result = authority.build_session(SESSION)
    identity = PartitionIdentity("research_os", OUTPUT_DATASET, SESSION.isoformat())
    record = ledger.get_partition(identity)
    assert record is not None
    tampered = dict(record.details)
    tampered["execution_snapshot_id"], tampered["mark_snapshot_id"] = (
        tampered["mark_snapshot_id"],
        tampered["execution_snapshot_id"],
    )
    tampered["execution_snapshot_hash"], tampered["mark_snapshot_hash"] = (
        tampered["mark_snapshot_hash"],
        tampered["execution_snapshot_hash"],
    )
    forged_hash = content_fingerprint(
        tampered,
        domain="factor-lab/research-os/v1/typed-execution-partition-result",
    )
    with ledger.engine.begin() as connection:
        connection.execute(
            update(orm.PartitionRunModel)
            .where(orm.PartitionRunModel.partition_run_id == identity.partition_run_id)
            .values(details_json=tampered, output_hash=forged_hash)
        )

    with pytest.raises(ExecutionEvidenceConflict, match="role binding"):
        authority.build_session(SESSION)

    assert result.execution_snapshot.snapshot_id != result.mark_snapshot.snapshot_id


def test_synthetic_success_cannot_write_accepted_capability(authority_factory) -> None:
    authority, _, ledger, *_ = authority_factory()
    authority.observe_open(SESSION)
    result = authority.build_session(SESSION)

    with ledger.engine.connect() as connection:
        capability = connection.execute(
            select(orm.SourceCapabilityModel).where(
                orm.SourceCapabilityModel.source_id == "research_os",
                orm.SourceCapabilityModel.dataset == CAPABILITY_DATASET,
            )
        ).mappings().one()
    assert capability["status"] == "degraded"
    assert result.capability.decision is ExecutionCapabilityDecision.NON_FORWARD
    assert all(
        reference.quality_status is DataQualityStatus.QUARANTINED
        for reference in (
            result.execution_snapshot,
            result.mark_snapshot,
            result.bundle_snapshot,
        )
    )


def test_production_pending_rotation_blocks_before_probe_or_network(
    authority_factory,
) -> None:
    adapter = _FixtureDiemengAdapter(
        {ticker: 10.0 + index for index, ticker in enumerate(TICKERS)}
    )
    authority, *_ = authority_factory(adapter=adapter, runtime_mode="production")

    with pytest.raises(
        ExecutionNetworkBlocked, match="persisted accepted-use evidence"
    ):
        authority.observe_open(SESSION)

    assert adapter.probe_calls == 0
    assert adapter.fetch_calls == 0


def test_production_open_cannot_be_backfilled_after_its_live_window(
    authority_factory,
) -> None:
    adapter = _FixtureDiemengAdapter(
        {ticker: 10.0 + index for index, ticker in enumerate(TICKERS)}
    )
    authority, *_ = authority_factory(adapter=adapter, runtime_mode="production")
    authority._rotation_attestation = lambda: SimpleNamespace(
        evidence_hash="a" * 64
    )
    authority._real_diemeng_adapter = lambda: True

    with pytest.raises(ExecutionNetworkBlocked, match="live-only"):
        authority.observe_open(SESSION)

    assert adapter.probe_calls == 0
    assert adapter.fetch_calls == 0


def test_rotation_attestation_is_read_only_and_bound_to_exact_persisted_contract(
    authority_factory,
) -> None:
    authority, _, ledger, *_ = authority_factory()
    assert authority.rotation_capability_identity == (
        "security",
        "diemeng_api_key_rotation",
    )
    assert authority.persisted_rotation_attestation() is None
    ledger.upsert_capability(
        CapabilityRecord(
            source_id="security",
            dataset="diemeng_api_key_rotation",
            status=CapabilityStatus.ACCEPTED,
            contract_hash=authority.rotation_contract_hash,
            fields=("credential_ref", "vendor_confirmation_id"),
            detail="vendor_confirmed_rotation",
            probed_at=NOW,
            probe_hash="9" * 64,
        )
    )

    attestation = authority.persisted_rotation_attestation()

    assert attestation is not None
    assert attestation.credential == "diemeng_api_key"
    assert attestation.evidence_hash == "9" * 64

    # A later, malformed self-report replaces the global row but cannot be
    # interpreted as the trusted vendor confirmation contract.
    ledger.upsert_capability(
        CapabilityRecord(
            source_id="security",
            dataset="diemeng_api_key_rotation",
            status=CapabilityStatus.ACCEPTED,
            contract_hash=authority.rotation_contract_hash,
            fields=("credential_ref",),
            detail="vendor_confirmed_rotation",
            probed_at=NOW,
            probe_hash="8" * 64,
        )
    )
    assert authority.persisted_rotation_attestation() is None


def test_production_factory_has_no_market_payload_seams_and_normalizes_host(
    production_factory,
) -> None:
    authority, _, _, _ = production_factory()

    assert tuple(
        inspect.signature(ExecutionSnapshotAuthority.from_production_config).parameters
    ) == ("config_path", "env", "catalog", "ledger", "archive", "cache_root")
    assert tuple(inspect.signature(authority.migrate_rotation_evidence).parameters) == ()
    assert type(authority.diemeng_adapter) is DiemengSourceAdapter
    assert authority.diemeng_adapter.base_url == "https://data.diemeng.chat/api"
    assert type(authority.gold_reader) is PyIcebergRegisteredGoldReader
    assert authority.gold_reader.expected_catalog_name == "factorlab"
    assert authority.production_configuration_hash is not None
    # The currently verified endpoint is a historical query and therefore
    # cannot be mistaken for a real-time opening collector.
    assert authority.formal_open_collection_capable is False


@pytest.mark.parametrize(
    "configured_origin",
    [
        None,
        "http://api.tushare.pro/dataapi",
        "https://attacker.invalid/dataapi",
        "https://api.tushare.pro/dataapi?redirect=attacker",
    ],
)
def test_missing_or_unsafe_tushare_origin_blocks_before_secret_or_client_creation(
    production_factory,
    monkeypatch: pytest.MonkeyPatch,
    configured_origin: str | None,
) -> None:
    base, ledger, secrets_root, config_path = production_factory()
    (secrets_root / "tushare_token").write_text(
        "fixture-new-tushare-token-0001\n", encoding="utf-8"
    )
    payload = _production_tushare_execution_payload()
    if configured_origin is not None:
        payload["security"] = {
            "source_transport": {
                "tushare": {"api_origin": configured_origin},
            }
        }
    config_path.write_text(json.dumps(payload), encoding="utf-8")
    import tushare as ts

    credential_reads = 0
    client_creations = 0
    production_validations = 0

    def forbidden_secret_read(*_args, **_kwargs):
        nonlocal credential_reads
        credential_reads += 1
        raise AssertionError("HTTP-only transport reached the credential")

    def forbidden_client_creation(*_args, **_kwargs):
        nonlocal client_creations
        client_creations += 1
        raise AssertionError("HTTP-only transport created a client")

    def forbidden_production_validation(*_args, **_kwargs):
        nonlocal production_validations
        production_validations += 1
        raise AssertionError("HTTP-only transport reached secret validation")

    monkeypatch.setattr(
        execution_authority_module, "_fixed_secret_line", forbidden_secret_read
    )
    monkeypatch.setattr(
        execution_authority_module,
        "validate_production_config",
        forbidden_production_validation,
    )
    monkeypatch.setattr(ts, "pro_api", forbidden_client_creation)
    with pytest.raises(
        ProductionConfigurationError,
        match="reviewed direct HTTPS origin",
    ):
        ExecutionSnapshotAuthority.from_production_config(
            config_path=config_path,
            env={
                "FACTOR_LAB_PRODUCTION_ROLE": "worker",
                "FACTOR_LAB_SECRETS_ROOT": str(secrets_root),
            },
            catalog=base.catalog,
            ledger=ledger,
            archive=base.archive,
            cache_root=base.cache_root / "tushare",
        )

    assert credential_reads == 0
    assert client_creations == 0
    assert production_validations == 0


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("missing", "rate_limits must contain only the account bucket"),
        ("conflict", "account rate limit must match every daily source"),
    ],
)
def test_tushare_execution_rate_limit_blocks_before_secret_or_client_creation(
    production_factory,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
    message: str,
) -> None:
    base, ledger, secrets_root, config_path = production_factory()
    payload = _production_tushare_execution_payload()
    execution = payload["daily"]["shadow"]["execution_market_data"]
    if mutation == "missing":
        execution.pop("rate_limits")
    else:
        execution["rate_limits"]["__account__"]["requests"] = 59
    config_path.write_text(json.dumps(payload), encoding="utf-8")
    import tushare as ts

    credential_reads = 0
    client_creations = 0
    production_validations = 0

    def forbidden_secret_read(*_args, **_kwargs):
        nonlocal credential_reads
        credential_reads += 1
        raise AssertionError("rate-limit failure reached the credential")

    def forbidden_client_creation(*_args, **_kwargs):
        nonlocal client_creations
        client_creations += 1
        raise AssertionError("rate-limit failure created a client")

    def forbidden_production_validation(*_args, **_kwargs):
        nonlocal production_validations
        production_validations += 1
        raise AssertionError("rate-limit failure reached secret validation")

    monkeypatch.setattr(
        execution_authority_module, "_fixed_secret_line", forbidden_secret_read
    )
    monkeypatch.setattr(
        execution_authority_module,
        "validate_production_config",
        forbidden_production_validation,
    )
    monkeypatch.setattr(ts, "pro_api", forbidden_client_creation)

    with pytest.raises(ProductionConfigurationError, match=message) as caught:
        ExecutionSnapshotAuthority.from_production_config(
            config_path=config_path,
            env={
                "FACTOR_LAB_PRODUCTION_ROLE": "worker",
                "FACTOR_LAB_SECRETS_ROOT": str(secrets_root),
            },
            catalog=base.catalog,
            ledger=ledger,
            archive=base.archive,
            cache_root=base.cache_root / "tushare-rate-limit",
        )

    assert "credential" not in str(caught.value)
    assert credential_reads == 0
    assert client_creations == 0
    assert production_validations == 0


def test_formal_factory_rejects_rt_min_daily_before_secret_or_client_creation(
    production_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base, ledger, secrets_root, config_path = production_factory()
    payload = _production_tushare_execution_payload()
    execution = payload["daily"]["shadow"]["execution_market_data"]
    execution["dataset"] = "rt_min_daily"
    execution["endpoint"] = "rt_min_daily"
    config_path.write_text(json.dumps(payload), encoding="utf-8")
    import tushare as ts

    events: list[str] = []
    monkeypatch.setattr(
        execution_authority_module,
        "_fixed_secret_line",
        lambda *_args, **_kwargs: events.append("secret_read"),
    )
    monkeypatch.setattr(
        execution_authority_module,
        "validate_production_config",
        lambda *_args, **_kwargs: events.append("production_validation"),
    )
    monkeypatch.setattr(
        ts,
        "pro_api",
        lambda *_args, **_kwargs: events.append("client_constructed"),
    )

    with pytest.raises(
        ProductionConfigurationError,
        match="endpoint must be rt_min",
    ):
        ExecutionSnapshotAuthority.from_production_config(
            config_path=config_path,
            env={
                "FACTOR_LAB_PRODUCTION_ROLE": "worker",
                "FACTOR_LAB_SECRETS_ROOT": str(secrets_root),
            },
            catalog=base.catalog,
            ledger=ledger,
            archive=base.archive,
            cache_root=base.cache_root / "tushare-rt-min-daily",
        )

    assert events == []


def test_reviewed_tushare_origin_is_bound_and_transport_is_sealed(
    production_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    base, ledger, secrets_root, config_path = production_factory()
    (secrets_root / "tushare_token").write_text(
        "fixture-new-tushare-token-0001\n", encoding="utf-8"
    )
    reviewed_origin = "https://api.waditu.com/dataapi"
    payload = _production_tushare_execution_payload()
    payload["security"] = {
        "source_transport": {
            "tushare": {"api_origin": reviewed_origin},
        }
    }
    config_path.write_text(json.dumps(payload), encoding="utf-8")

    import tushare as ts

    events: list[str] = []
    observed_origins: list[str] = []
    clients: list[Any] = []
    original_validator = execution_authority_module.validate_tushare_https_origin
    original_secret_reader = execution_authority_module._fixed_secret_line
    original_pro_api = ts.pro_api

    def validate_origin(value: str) -> str:
        events.append("transport_preflight")
        observed_origins.append(value)
        return original_validator(value)

    def read_secret(*args, **kwargs) -> str:
        events.append("secret_read")
        return original_secret_reader(*args, **kwargs)

    def create_client(token: str):
        events.append("client_constructed")
        client = original_pro_api(token)
        clients.append(client)
        return client

    monkeypatch.setattr(
        execution_authority_module, "validate_tushare_https_origin", validate_origin
    )
    monkeypatch.setattr(execution_authority_module, "_fixed_secret_line", read_secret)
    monkeypatch.setattr(ts, "pro_api", create_client)

    authority = ExecutionSnapshotAuthority.from_production_config(
        config_path=config_path,
        env={
            "FACTOR_LAB_PRODUCTION_ROLE": "worker",
            "FACTOR_LAB_SECRETS_ROOT": str(secrets_root),
        },
        catalog=base.catalog,
        ledger=ledger,
        archive=base.archive,
        cache_root=base.cache_root / "tushare",
    )

    assert observed_origins == [reviewed_origin, reviewed_origin]
    assert events == [
        "transport_preflight",
        "transport_preflight",
        "secret_read",
        "client_constructed",
    ]
    assert len(clients) == 1
    assert authority.open_adapter.client is clients[0]
    assert authority.open_adapter.client._DataApi__http_url == reviewed_origin
    assert execution_authority_module.tushare_client_uses_direct_transport(
        authority.open_adapter.client
    )
    session = getattr(
        authority.open_adapter.client, "_factor_lab_direct_http_session"
    )
    assert session.trust_env is False
    assert session.proxies == {}
    assert session.verify is True
    assert authority.open_adapter.production_attested is True
    assert authority.open_adapter.public_execution_contract["rate_limits"] == {
        "__account__": {"requests": 60, "per_seconds": 60.0, "burst": 1}
    }
    assert authority._real_diemeng_adapter() is True

    authority.open_adapter.collection_window_minutes = 6
    assert authority.open_adapter.production_attested is True
    assert authority._real_diemeng_adapter() is False
    authority.open_adapter.collection_window_minutes = 5
    assert authority._real_diemeng_adapter() is True

    authority.open_adapter.rate_limiter = object()
    assert authority.open_adapter.production_attested is False
    assert authority._real_diemeng_adapter() is False


def test_operator_retention_authority_is_idempotent_secret_free_and_admitted(
    production_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    authority, ledger, secrets_root, _ = production_factory(
        payload=_production_tushare_retention_payload(),
        credential_retention_waivers=("tushare_token",),
    )

    assert authority.rotation_capability_identity == (
        "security",
        "tushare_token_retention",
    )
    first = authority.migrate_credential_use_evidence()
    second = authority.migrate_credential_use_evidence()

    assert first == second
    assert first.credential == "tushare_token"
    assert first.disposition == "operator_accepted_unrotated_retention"
    assert authority.persisted_rotation_attestation() == first
    with ledger.engine.connect() as connection:
        rows = connection.execute(
            select(orm.SourceCapabilityModel).where(
                orm.SourceCapabilityModel.source_id == "security",
                orm.SourceCapabilityModel.dataset == "tushare_token_retention",
            )
        ).mappings().all()
    assert len(rows) == 1
    row = rows[0]
    assert row["status"] == "accepted"
    assert row["detail"] == "operator_accepted_unrotated_retention"
    assert row["probe_hash"] == first.evidence_hash
    serialized_row = json.dumps(dict(row), default=str, sort_keys=True)
    secret = (secrets_root / "tushare_token").read_text(encoding="utf-8").strip()
    assert secret not in serialized_row
    assert "secret://tushare_token" not in serialized_row

    network_calls = 0

    def forbidden_network(*_args, **_kwargs):
        nonlocal network_calls
        network_calls += 1
        raise AssertionError("retention precondition must not trigger the provider here")

    monkeypatch.setattr(authority.open_adapter, "fetch_open_batch", forbidden_network)
    with pytest.raises(ExecutionNetworkBlocked, match="live-only") as caught:
        authority.observe_open(SESSION)
    assert "accepted-use evidence" not in str(caught.value)
    assert network_calls == 0


@pytest.mark.parametrize(
    "mutation",
    ["configuration", "accepted_at", "credential_ref", "transport_origin"],
)
def test_operator_retention_authority_fails_closed_after_bound_input_changes(
    production_factory,
    mutation: str,
) -> None:
    initial_payload = _production_tushare_retention_payload()
    authority, ledger, secrets_root, config_path = production_factory(
        payload=initial_payload,
        credential_retention_waivers=("tushare_token",),
    )
    authority.migrate_credential_use_evidence()

    changed_payload = json.loads(json.dumps(initial_payload))
    waiver = changed_payload["security"]["credential_rotation"]["tushare_token"]
    if mutation == "configuration":
        changed_payload["operator_policy_revision"] = "changed-after-acceptance"
    elif mutation == "accepted_at":
        waiver["accepted_at"] = "2026-08-19T00:00:00+00:00"
    elif mutation == "credential_ref":
        waiver["credential_ref"] = "secret://diemeng_api_key"
    elif mutation == "transport_origin":
        changed_payload["security"]["source_transport"]["tushare"][
            "api_origin"
        ] = "https://api.tushare.pro/dataapi"
    else:  # pragma: no cover - the parameter list is closed above
        raise AssertionError(mutation)
    config_path.write_text(json.dumps(changed_payload), encoding="utf-8")

    construction = lambda: ExecutionSnapshotAuthority.from_production_config(
        config_path=config_path,
        env={
            "FACTOR_LAB_PRODUCTION_ROLE": "worker",
            "FACTOR_LAB_SECRETS_ROOT": str(secrets_root),
        },
        catalog=authority.catalog,
        ledger=ledger,
        archive=authority.archive,
        cache_root=authority.cache_root / f"changed-{mutation}",
    )
    if mutation == "credential_ref":
        with pytest.raises(
            ProductionConfigurationError,
            match="retention waiver differs from the reviewed unrotated contract",
        ):
            construction()
        return

    changed = construction()
    assert changed.persisted_rotation_attestation() is None
    with pytest.raises(
        ExecutionEvidenceConflict,
        match="credential-retention authority is immutable and differs",
    ):
        changed.migrate_credential_use_evidence()


def test_rotation_capability_can_only_migrate_fixed_local_secret_evidence(
    production_factory,
) -> None:
    authority, ledger, secrets_root, _ = production_factory()
    _write_rotation_evidence(secrets_root)

    first = authority.migrate_rotation_evidence()
    second = authority.migrate_rotation_evidence()

    assert first == second
    assert authority.persisted_rotation_attestation() == first
    with ledger.engine.connect() as connection:
        row = connection.execute(
            select(orm.SourceCapabilityModel).where(
                orm.SourceCapabilityModel.source_id == "security",
                orm.SourceCapabilityModel.dataset == "diemeng_api_key_rotation",
            )
        ).mappings().one()
    assert row["status"] == "accepted"
    assert row["probe_hash"] == first.evidence_hash
    assert "fixture-new-diemeng-key" not in str(dict(row))


def test_rotation_migration_rejects_confirmation_not_bound_to_current_secret(
    production_factory,
) -> None:
    authority, ledger, secrets_root, _ = production_factory()
    _write_rotation_evidence(secrets_root, current_hash_override="b" * 64)

    with pytest.raises(
        ExecutionEvidenceConflict, match="not the vendor-confirmed replacement"
    ):
        authority.migrate_rotation_evidence()

    with ledger.engine.connect() as connection:
        row = connection.execute(
            select(orm.SourceCapabilityModel).where(
                orm.SourceCapabilityModel.source_id == "security",
                orm.SourceCapabilityModel.dataset == "diemeng_api_key_rotation",
            )
        ).mappings().one_or_none()
    assert row is None


def test_historical_minute_response_uses_ingestion_availability_and_cannot_advance(
    authority_factory,
) -> None:
    adapter = _FixtureDiemengAdapter(
        {ticker: 10.0 + index for index, ticker in enumerate(TICKERS)},
        ingested_at=MARK_UTC,
    )
    authority, _, ledger, *_ = authority_factory(adapter=adapter)

    opening = authority.observe_open(SESSION)

    assert opening.quality_status is DataQualityStatus.QUARANTINED
    assert datetime.fromisoformat(str(opening.manifest["as_of"])) == MARK_UTC
    with pytest.raises(ExecutionEvidenceConflict, match="leaks after 09:30"):
        authority.build_session(SESSION)
    with ledger.engine.connect() as connection:
        accepted = connection.execute(
            select(orm.SourceCapabilityModel).where(
                orm.SourceCapabilityModel.source_id == "research_os",
                orm.SourceCapabilityModel.dataset == CAPABILITY_DATASET,
                orm.SourceCapabilityModel.status == "accepted",
            )
        ).mappings().all()
    assert accepted == []


def test_object_store_interruption_resumes_without_new_identity(authority_factory) -> None:
    filesystem = _MemoryFileSystem()
    authority, catalog, ledger, *_ = authority_factory(filesystem=filesystem)
    authority.observe_open(SESSION)
    filesystem.fail_role = BUNDLE_ROLE

    with pytest.raises(OSError, match="injected object-store interruption"):
        authority.build_session(SESSION)

    failed = ledger.get_partition(
        PartitionIdentity("research_os", OUTPUT_DATASET, SESSION.isoformat())
    )
    assert failed is not None and failed.status is PartitionStatus.FAILED
    partial_ids = {
        record.reference.snapshot_id
        for record in catalog.list_snapshots(limit=100)
        if record.reference.manifest.get("role") in {"execution", "mark"}
    }
    assert len(partial_ids) == 2

    recovered = authority.build_session(SESSION)
    completed = ledger.get_partition(
        PartitionIdentity("research_os", OUTPUT_DATASET, SESSION.isoformat())
    )
    assert completed is not None and completed.status is PartitionStatus.SUCCEEDED
    assert completed.attempts == 2
    assert {
        recovered.execution_snapshot.snapshot_id,
        recovered.mark_snapshot.snapshot_id,
    } == partial_ids


def test_capability_write_failure_keeps_partition_retryable_and_preserves_root_cause(
    authority_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority, _, ledger, *_ = authority_factory()
    authority.observe_open(SESSION)

    class CapabilityPersistenceFailure(RuntimeError):
        pass

    real_upsert = ledger.upsert_capability

    secret_marker = "bare-provider-secret-marker-9f34a7"
    injected = False

    def fail_formal_capability(record: CapabilityRecord) -> CapabilityRecord:
        nonlocal injected
        if (
            not injected
            and record.source_id == "research_os"
            and record.dataset == CAPABILITY_DATASET
        ):
            injected = True
            raise CapabilityPersistenceFailure(
                f"formal capability persistence failed {secret_marker}"
            )
        return real_upsert(record)

    monkeypatch.setattr(ledger, "upsert_capability", fail_formal_capability)

    with pytest.raises(
        CapabilityPersistenceFailure,
        match="formal capability persistence failed",
    ):
        authority.build_session(SESSION)

    record = ledger.get_partition(
        PartitionIdentity("research_os", OUTPUT_DATASET, SESSION.isoformat())
    )
    assert record is not None
    assert record.status is PartitionStatus.FAILED
    assert record.error_code == "typed_execution_rejected"
    assert "CapabilityPersistenceFailure" in str(record.error)
    assert secret_marker not in str(record.error)
    with ledger.engine.connect() as connection:
        stored = connection.execute(
            select(orm.SourceCapabilityModel).where(
                orm.SourceCapabilityModel.source_id == "research_os",
                orm.SourceCapabilityModel.dataset == CAPABILITY_DATASET,
            )
        ).mappings().one()
    assert secret_marker not in json.dumps(dict(stored), default=str)


def test_open_provider_exception_prose_never_reaches_durable_ledger(
    authority_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority, _, ledger, _, adapter, _ = authority_factory()
    secret_marker = "bare-open-provider-secret-marker-71d4c2"

    def fail_fetch(*_args, **_kwargs):
        raise RuntimeError(f"upstream response included {secret_marker}")

    monkeypatch.setattr(adapter, "fetch", fail_fetch)

    with pytest.raises(RuntimeError, match=secret_marker):
        authority.observe_open(SESSION)

    record = ledger.get_partition(
        PartitionIdentity("diemeng", OPEN_DATASET, SESSION.isoformat())
    )
    assert record is not None
    assert record.status is PartitionStatus.FAILED
    assert record.error_code == "execution_open_rejected"
    assert record.error == "RuntimeError"
    assert secret_marker not in json.dumps(vars(record), default=str)


def test_post_commit_failure_cannot_overwrite_success_or_mask_root_cause(
    authority_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority, _, ledger, *_ = authority_factory()
    authority.observe_open(SESSION)

    class CommitAcknowledgementLost(RuntimeError):
        pass

    real_finish = ledger.finish

    def commit_then_lose_acknowledgement(lease, **kwargs):
        completed = real_finish(lease, **kwargs)
        if (
            lease.identity.dataset == OUTPUT_DATASET
            and kwargs["status"] is PartitionStatus.SUCCEEDED
        ):
            raise CommitAcknowledgementLost("success commit acknowledgement lost")
        return completed

    monkeypatch.setattr(ledger, "finish", commit_then_lose_acknowledgement)

    with pytest.raises(
        CommitAcknowledgementLost,
        match="success commit acknowledgement lost",
    ) as caught:
        authority.build_session(SESSION)

    record = ledger.get_partition(
        PartitionIdentity("research_os", OUTPUT_DATASET, SESSION.isoformat())
    )
    assert record is not None
    assert record.status is PartitionStatus.SUCCEEDED
    assert record.error_code is None
    if hasattr(caught.value, "add_note"):
        assert any(
            "ImmutablePartition" in note
            for note in getattr(caught.value, "__notes__", ())
        )
    with ledger.engine.connect() as connection:
        capability = connection.execute(
            select(orm.SourceCapabilityModel).where(
                orm.SourceCapabilityModel.source_id == "research_os",
                orm.SourceCapabilityModel.dataset == CAPABILITY_DATASET,
            )
        ).mappings().one()
    assert capability["status"] == "unavailable"


def test_deleted_physical_bundle_blocks_idempotent_reuse(authority_factory) -> None:
    authority, _, _, filesystem, *_ = authority_factory()
    authority.observe_open(SESSION)
    result = authority.build_session(SESSION)
    physical = result.bundle_snapshot.manifest["physical_object"]
    filesystem.objects.pop(f"factor-lab/{physical['key']}")

    with pytest.raises(Exception, match="missing|absent"):
        authority.build_session(SESSION)


def test_registered_iceberg_reader_resolves_exact_tag_and_snapshot_key() -> None:
    gold_id = "a" * 64

    class _Ref:
        snapshot_id = 42

    class _Summary:
        additional_properties = {"factor_lab.snapshot_key": gold_id}

    class _Snapshot:
        snapshot_id = 42
        summary = _Summary()

    class _Arrow:
        def to_pandas(self):
            return pd.DataFrame([{"ts_code": "000001.SZ", "close": 10.0}])

    class _Scan:
        def to_arrow(self):
            return _Arrow()

    class _Table:
        metadata = type(
            "Metadata",
            (),
            {"refs": {"ros_gold": _Ref()}, "snapshots": [_Snapshot()]},
        )()

        def scan(self, *, snapshot_id: int):
            assert snapshot_id == 42
            return _Scan()

    class _Catalog:
        def load_table(self, identifier: str):
            assert identifier == "factor_lab.gold_research_panel"
            return _Table()

    reader = PyIcebergRegisteredGoldReader(catalog_loader=lambda name: _Catalog())
    reference = DataSnapshotRef(
        snapshot_id=gold_id,
        tier=SnapshotTier.GOLD,
        uri="iceberg://factorlab/factor_lab.gold_research_panel#ros_gold",
        content_hash=gold_id,
        as_of=GOLD_AS_OF,
        quality_status=DataQualityStatus.ACCEPTED,
        manifest={"snapshot_id": gold_id},
    )

    frame = reader.read(reference)

    assert len(frame) == 1
    assert reader.production_attested is False


def test_registered_iceberg_reader_rejects_tag_snapshot_key_mismatch() -> None:
    gold_id = "a" * 64

    class _Ref:
        snapshot_id = 42

    class _Snapshot:
        snapshot_id = 42
        summary = {"factor_lab.snapshot_key": "a_different_snapshot"}

    class _Table:
        metadata = type(
            "Metadata",
            (),
            {"refs": {"ros_gold": _Ref()}, "snapshots": [_Snapshot()]},
        )()

    class _Catalog:
        def load_table(self, identifier: str):
            return _Table()

    reader = PyIcebergRegisteredGoldReader(catalog_loader=lambda name: _Catalog())
    reference = DataSnapshotRef(
        snapshot_id=gold_id,
        tier=SnapshotTier.GOLD,
        uri="iceberg://factorlab/factor_lab.gold_research_panel#ros_gold",
        content_hash=gold_id,
        as_of=GOLD_AS_OF,
        quality_status=DataQualityStatus.ACCEPTED,
        manifest={"snapshot_id": gold_id},
    )

    with pytest.raises(ExecutionEvidenceConflict, match="snapshot key differs"):
        reader.read(reference)

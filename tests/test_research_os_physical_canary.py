from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
import io
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

pytest.importorskip("sqlalchemy")
from sqlalchemy import create_engine, insert
from sqlalchemy.orm import Session

from factor_lab.research_os import catalog as catalog_module
from factor_lab.research_os import physical_canary as physical_canary_module
from factor_lab.research_os.catalog import ResearchCatalog, RunRecord
from factor_lab.research_os.contracts import (
    DataSnapshotRef,
    PHYSICAL_CANARY_SNAPSHOT_REFERENCE_SCHEMA,
    SnapshotTier,
)
from factor_lab.research_os.data_sources import (
    DatasetContract,
    DiemengSourceAdapter,
    FetchRequest,
    FieldContract,
    ProbeResult,
    SourceAdapter,
    SourceHealth,
)
from factor_lab.research_os.fingerprint import content_fingerprint
from factor_lab.research_os.execution_open_sources import (
    diemeng_engineering_canary_execution_mapping,
)
from factor_lab.research_os.object_store import (
    ObjectStoreIntegrityError,
    S3ImmutableArchive,
)
from factor_lab.research_os.orm import (
    Base,
    DataIncidentModel,
    EvidenceEpochModel,
    EvidenceEpochPointerModel,
    PartitionRunModel,
)
from factor_lab.research_os.physical_canary import (
    CANARY_OBJECT_PREFIX,
    CONTROLLED_TEST_RUN_TYPE,
    PHYSICAL_RUN_TYPE,
    PhysicalCanaryAdmissionError,
    PhysicalCanaryBusy,
    PhysicalCanaryDataRejected,
    PhysicalCanaryFormalEpochDenied,
    PhysicalEngineeringCanaryService,
    _gold_attempted_input_hash,
    _opening_source_payload,
    _labels,
    deny_physical_canary_formal_epoch,
    require_physical_canary_credential_rotation,
)
from factor_lab.research_os.production_ledger import (
    IncidentStage,
    IncidentStatus,
    PartitionIdentity,
    PartitionStatus,
    ProductionLedger,
)
from factor_lab.research_os.readiness_audit import (
    ProductionReadinessAuditor,
    physical_canary_evidence_hash,
)
from factor_lab.research_os.shadow_authority import (
    ShadowEvidenceAuthority,
    ShadowEvidenceClass,
)


NOW = datetime(2026, 8, 23, 12, 0, tzinfo=timezone.utc)
SESSIONS = tuple(item.date() for item in pd.bdate_range("2026-06-01", periods=21))
CURRENT_STAGE_SOURCE = f"engineering_canary_{'c' * 24}"
LEGACY_STAGE_SOURCE = f"engineering_canary_{'a' * 24}"
TICKERS = tuple(f"{index:06d}.SZ" for index in range(1, 61))


@pytest.fixture(autouse=True)
def _freeze_catalog_registration_clock(monkeypatch) -> None:
    monkeypatch.setattr(catalog_module, "_utc_now", lambda: NOW)


def _opening_shadow(request: dict[str, object]) -> dict[str, object]:
    execution = diemeng_engineering_canary_execution_mapping()
    execution["request"] = request
    return {
        "execution_market_data": execution,
    }


def test_opening_payload_keeps_provider_required_level_and_bounded_paging() -> None:
    request = {
        "stock_code": "${ticker}",
        "level": "1min",
        "start_time": "${partition_key} 09:30:00",
        "end_time": "${partition_key} 15:00:00",
        "page": 0,
        "page_size": 10000,
    }

    payload = _opening_source_payload(_opening_shadow(request))

    assert payload["request"]["parameters"] == request
    assert payload["source"] == "diemeng"
    assert payload["base_url"] == "https://data.diemeng.chat/api"


def test_opening_payload_never_replays_formal_tushare_realtime_source() -> None:
    realtime = {
        "execution_market_data": {
            "source": "tushare",
            "profile_name": "primary-tushare",
            "credential_ref": "secret://tushare_token",
            "dataset": "rt_min",
            "endpoint": "rt_min",
            "method": "SDK",
            "request": {
                "ts_code": "${decision_universe_csv}",
                "freq": "1MIN",
            },
        }
    }

    with pytest.raises(
        PhysicalCanaryAdmissionError,
        match="Diemeng source|closed retrospective contract",
    ):
        _opening_source_payload(realtime)


def test_physical_canary_scope_is_explicitly_retrospective_and_never_formal(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        with pytest.raises(
            PhysicalCanaryAdmissionError,
            match="never formal-ready",
        ):
            PhysicalEngineeringCanaryService(
                catalog=runtime.catalog,
                production_ledger=runtime.ledger,
                shadow_authority=runtime.authority,
                object_store_archive=runtime.archive,
                local_root=tmp_path / "rejected-runtime",
                bindings=runtime.service.bindings,
                production_evidence=None,
                controlled_test=True,
                opening_execution_formal_ready=True,
                now=lambda: NOW,
            )
    finally:
        _close(runtime)
    runtime.engine.dispose()


@pytest.mark.parametrize(
    "request_payload",
    [
        {
            "stock_code": "${ticker}",
            "start_time": "${partition_key} 09:30:00",
            "end_time": "${partition_key} 15:00:00",
            "page": 0,
            "page_size": 10000,
        },
        {
            "stock_code": "${ticker}",
            "level": "1min",
            "start_time": "${partition_key} 09:30:00",
            "end_time": "${partition_key} 15:00:00",
            "page": 0,
            "page_size": 10001,
        },
        {
            "stock_code": "${ticker}",
            "level": "60min",
            "start_time": "${partition_key} 09:30:00",
            "end_time": "${partition_key} 15:00:00",
            "page": 0,
            "page_size": 10000,
        },
        {
            "stock_code": "${ticker}",
            "level": "1min",
            "start_time": "${partition_key} 09:30:00",
            "end_time": "${partition_key} 15:00:00",
            "page": 7,
            "page_size": 10000,
        },
        {
            "stock_code": "000001.SZ",
            "level": "1min",
            "start_time": "2026-08-21 09:30:00",
            "end_time": "2026-08-21 15:00:00",
            "page": 0,
            "page_size": 10000,
        },
    ],
)
def test_opening_payload_rejects_incomplete_provider_contract(
    request_payload: dict[str, object],
) -> None:
    with pytest.raises(PhysicalCanaryAdmissionError):
        _opening_source_payload(_opening_shadow(request_payload))


class _MemoryWriter(io.BytesIO):
    def __init__(self, filesystem, path):
        super().__init__()
        self.filesystem = filesystem
        self.path = path

    def close(self):
        self.filesystem.objects[self.path] = self.getvalue()
        super().close()


class _MemoryFileSystem:
    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def exists(self, path: str) -> bool:
        return path in self.objects

    def open(self, path: str, mode: str = "rb"):
        if mode == "rb":
            return io.BytesIO(self.objects[path])
        return _MemoryWriter(self, path)


def _field(name: str, dtype: str = "float64") -> FieldContract:
    return FieldContract(name=name, dtype=dtype, nullable=False)


CALENDAR = DatasetContract(
    dataset="trade_calendar",
    key_fields=("exchange", "cal_date"),
    fields=(_field("exchange", "string"), _field("cal_date", "date"), _field("is_open", "int64")),
    event_time_field="cal_date",
    release_timing="before open",
)
DAILY = DatasetContract(
    dataset="daily",
    key_fields=("ts_code", "trade_date"),
    fields=(
        _field("ts_code", "string"),
        _field("trade_date", "date"),
        _field("open"),
        _field("high"),
        _field("low"),
        _field("close"),
        _field("amount"),
    ),
    event_time_field="trade_date",
    release_timing="after close",
)
ADJUSTMENT = DatasetContract(
    dataset="adj_factor",
    key_fields=("ts_code", "trade_date"),
    fields=(
        _field("ts_code", "string"),
        _field("trade_date", "date"),
        _field("adj_factor"),
    ),
    event_time_field="trade_date",
    release_timing="after close",
)
HISTORICAL_ST = DatasetContract(
    dataset="historical_st",
    key_fields=("ts_code", "trade_date", "type"),
    fields=(
        _field("ts_code", "string"),
        _field("trade_date", "date"),
        _field("name", "string"),
        _field("type", "string"),
        _field("type_name", "string"),
    ),
    event_time_field="trade_date",
    release_timing="conservative next day",
    allows_empty=False,
)
LIMITS = DatasetContract(
    dataset="stock_limit",
    key_fields=("ts_code", "trade_date"),
    fields=(
        _field("ts_code", "string"),
        _field("trade_date", "date"),
        _field("up_limit"),
        _field("down_limit"),
    ),
    event_time_field="trade_date",
    release_timing="conservative next day",
)
OPENING = DatasetContract(
    dataset="opening_execution",
    key_fields=("stock_code", "trade_time"),
    fields=(
        _field("stock_code", "string"),
        _field("trade_time", "datetime"),
        _field("open"),
        _field("high"),
        _field("low"),
        _field("close"),
        _field("vol"),
        _field("amount"),
    ),
    event_time_field="trade_time",
    release_timing="event timestamp",
)


class _ProviderState:
    def __init__(self):
        self.calendar_conflict: date | None = None
        self.empty_st: date | None = None
        self.fail_opening_once: date | None = None
        self.failed_opening = False
        self.calls: dict[tuple[str, date], int] = {}

    @staticmethod
    def _date(request: FetchRequest) -> date:
        parameters = request.parameters
        raw = (
            parameters.get("trade_date")
            or parameters.get("start_date")
            or str(parameters.get("start_time") or "")[:10]
        )
        text = str(raw)
        return date.fromisoformat(text) if "-" in text else datetime.strptime(text, "%Y%m%d").date()

    @staticmethod
    def _price(ticker: str, session: date) -> float:
        ticker_index = TICKERS.index(ticker) + 1
        day_index = SESSIONS.index(session)
        return 8.0 + ticker_index * 0.05 + day_index * 0.01

    def frame(self, source_id: str, request: FetchRequest) -> pd.DataFrame:
        session = self._date(request)
        key = (request.dataset, session)
        self.calls[key] = self.calls.get(key, 0) + 1
        if request.dataset == "trade_calendar":
            is_open = int(not (source_id == "fixture_calendar_2" and session == self.calendar_conflict))
            return pd.DataFrame(
                {"exchange": ["SSE"], "cal_date": [session], "is_open": [is_open]}
            )
        if request.dataset == "historical_st":
            if session == self.empty_st:
                return pd.DataFrame(columns=list(HISTORICAL_ST.field_map))
            return pd.DataFrame(
                {
                    "ts_code": [TICKERS[-1]],
                    "trade_date": [session],
                    "name": ["*ST fixture"],
                    "type": ["S"],
                    "type_name": ["ST"],
                }
            )
        if request.dataset == "opening_execution":
            if session == self.fail_opening_once and not self.failed_opening:
                self.failed_opening = True
                raise RuntimeError("controlled transient provider interruption")
            ticker = str(request.parameters["stock_code"])
            opening = self._price(ticker, session)
            return pd.DataFrame(
                {
                    "stock_code": [ticker],
                    "trade_time": [f"{session.isoformat()}T09:30:00+08:00"],
                    "open": [opening],
                    "high": [opening * 1.01],
                    "low": [opening * 0.99],
                    "close": [opening * 1.001],
                    "vol": [10_000_000.0],
                    "amount": [200_000_000.0],
                }
            )
        if request.dataset == "daily":
            rows = []
            for index, ticker in enumerate(TICKERS, start=1):
                opening = self._price(ticker, session)
                close = opening * (1.0 + ((index % 7) - 3) * 0.0002)
                rows.append(
                    {
                        "ts_code": ticker,
                        "trade_date": session,
                        "open": opening,
                        "high": max(opening, close) * 1.01,
                        "low": min(opening, close) * 0.99,
                        "close": close,
                        "amount": float(100_000_000 - index * 100_000),
                    }
                )
            return pd.DataFrame(rows)
        if request.dataset == "adj_factor":
            factor = 1.0 + SESSIONS.index(session) * 0.0001
            return pd.DataFrame(
                {
                    "ts_code": TICKERS,
                    "trade_date": [session] * len(TICKERS),
                    "adj_factor": [factor] * len(TICKERS),
                }
            )
        if request.dataset == "stock_limit":
            rows = []
            for ticker in TICKERS:
                opening = self._price(ticker, session)
                rows.append(
                    {
                        "ts_code": ticker,
                        "trade_date": session,
                        "up_limit": opening * 1.1,
                        "down_limit": opening * 0.9,
                    }
                )
            return pd.DataFrame(rows)
        raise AssertionError(request.dataset)


class _ControlledAdapter(SourceAdapter):
    physical_canary_controlled_test_adapter = True

    def __init__(self, source_id: str, priority: int, contracts, state: _ProviderState):
        super().__init__(
            source_id=source_id,
            priority=priority,
            contracts=contracts,
            lineage={"controlled_test_adapter": True},
        )
        self.state = state

    def probe(self) -> ProbeResult:
        return ProbeResult(
            source_id=self.source_id,
            health=SourceHealth.HEALTHY,
            checked_at=NOW,
            latency_ms=1.0,
            datasets=tuple(sorted(self.contracts)),
            message="controlled bounded probe",
        )

    def _fetch_frame(self, request: FetchRequest) -> pd.DataFrame:
        return self.state.frame(self.source_id, request)


def _adapters(state: _ProviderState):
    return (
        _ControlledAdapter(
            "fixture_calendar_1",
            10,
            (CALENDAR, DAILY, ADJUSTMENT, HISTORICAL_ST, LIMITS),
            state,
        ),
        _ControlledAdapter("fixture_calendar_2", 20, (CALENDAR,), state),
        _ControlledAdapter("fixture_opening", 20, (OPENING,), state),
    )


def _seed_calendar(
    *,
    catalog: ResearchCatalog,
    ledger: ProductionLedger,
    archive: S3ImmutableArchive,
    tmp_path: Path,
) -> DataSnapshotRef:
    payload = tmp_path / "accepted-calendar.parquet"
    pd.DataFrame({"session": [item.isoformat() for item in SESSIONS]}).to_parquet(
        payload, index=False
    )
    stored = S3ImmutableArchive(
        bucket=archive.bucket,
        filesystem=archive.filesystem,
        prefix="research-os/accepted-calendar/v1",
    ).archive_file(payload, logical_path="sse")
    reference = DataSnapshotRef(
        snapshot_id="controlled-calendar-silver",
        tier="silver",
        uri=stored.uri,
        content_hash=content_fingerprint(
            stored.to_dict(), domain="tests/controlled-accepted-calendar"
        ),
        as_of=NOW,
        quality_status="accepted",
        trust_labels=("controlled_test_adapter", "accepted_trade_calendar"),
        manifest={"physical_object": stored.to_dict()},
    )
    catalog.register_snapshot(reference)
    for session in SESSIONS:
        identity = PartitionIdentity(
            "research_os", "accepted_trade_calendar", session.isoformat()
        )
        ledger.ensure_partition(
            identity,
            created_at=NOW,
            input_hash=content_fingerprint(
                session.isoformat(), domain="tests/accepted-calendar-input"
            ),
        )
        lease = ledger.claim(
            identity=identity,
            owner="controlled-calendar",
            now=NOW,
            lease_for=pd.Timedelta(minutes=10).to_pytimedelta(),
        )
        assert lease is not None
        ledger.finish(
            lease,
            status=PartitionStatus.SUCCEEDED,
            completed_at=NOW,
            output_snapshot_id=reference.snapshot_id,
            output_hash=content_fingerprint(
                session.isoformat(), domain="tests/accepted-calendar-output"
            ),
            details={"controlled_test_adapter": True},
        )
    return reference


def _runtime(tmp_path: Path, *, active_epoch: bool = False):
    database = tmp_path / "catalog.db"
    catalog = ResearchCatalog(database)
    catalog.initialize_schema()
    engine = create_engine(f"sqlite:///{database}")
    Base.metadata.create_all(engine)
    ledger = ProductionLedger(engine)
    authority = ShadowEvidenceAuthority(engine)
    filesystem = _MemoryFileSystem()
    archive = S3ImmutableArchive(bucket="factor-lab", filesystem=filesystem)
    calendar = _seed_calendar(
        catalog=catalog,
        ledger=ledger,
        archive=archive,
        tmp_path=tmp_path,
    )
    if active_epoch:
        with Session(engine) as session, session.begin():
            session.add(
                EvidenceEpochModel(
                    epoch_slot="research_os",
                    epoch_id="active-epoch",
                    schema_version="research-os/evidence-epoch/v1",
                    architecture_version="test-v1",
                    frozen_at=NOW,
                    code_hash="1" * 64,
                    configuration_hash="2" * 64,
                    dependency_lock_hash="3" * 64,
                    dirty_patch_hash="4" * 64,
                    epoch_hash="5" * 64,
                    first_forward_session=SESSIONS[1].isoformat(),
                    calendar_snapshot_id=calendar.snapshot_id,
                    calendar_snapshot_hash=calendar.content_hash,
                    calendar_content_hash="6" * 64,
                    evidence_window_hash="7" * 64,
                    activated_at=NOW,
                )
            )
            session.add(
                EvidenceEpochPointerModel(
                    pointer_key="research_os",
                    epoch_id="active-epoch",
                    updated_at=NOW,
                )
            )
    state = _ProviderState()
    service = PhysicalEngineeringCanaryService.for_controlled_test(
        adapters=_adapters(state),
        catalog=catalog,
        production_ledger=ledger,
        shadow_authority=authority,
        object_store_archive=archive,
        local_root=tmp_path / "runtime-data",
        now=lambda: NOW,
    )
    return SimpleNamespace(
        database=database,
        catalog=catalog,
        engine=engine,
        ledger=ledger,
        authority=authority,
        filesystem=filesystem,
        archive=archive,
        state=state,
        service=service,
    )


def _persisted_boundary_frame() -> pd.DataFrame:
    timestamps = pd.Series(
        pd.to_datetime(
            [
                "2026-08-21T01:30:00.123456789Z",
                None,
                "2026-08-21T07:00:00.987654321Z",
            ],
            utc=True,
        )
    ).dt.tz_convert("Asia/Shanghai")
    return pd.DataFrame(
        {
            "object_text": pd.Series(["alpha", None, "中文"], dtype=object),
            "string_text": pd.Series(["one", pd.NA, "three"], dtype="string"),
            "event_time": timestamps,
            "nullable_count": pd.Series([1, pd.NA, 3], dtype="Int64"),
            "value": [1.25, float("nan"), -3.5],
        }
    )


def _snapshot_reference_from_manifest(
    reference: DataSnapshotRef,
    manifest: dict[str, object],
    *,
    tier: SnapshotTier | None = None,
    uri: str | None = None,
    parent_snapshot_ids: tuple[str, ...] | None = None,
    snapshot_id: str | None = None,
    content_hash: str | None = None,
    as_of: datetime | None = None,
) -> DataSnapshotRef:
    selected_tier = tier or reference.tier
    selected_content_hash = content_hash or (
        physical_canary_module._snapshot_content_hash(manifest)
    )
    return DataSnapshotRef(
        snapshot_id=snapshot_id
        or physical_canary_module._snapshot_id(
            selected_tier,
            selected_content_hash,
        ),
        tier=selected_tier,
        uri=uri or reference.uri,
        content_hash=selected_content_hash,
        parent_snapshot_ids=(
            reference.parent_snapshot_ids
            if parent_snapshot_ids is None
            else parent_snapshot_ids
        ),
        as_of=reference.as_of if as_of is None else as_of,
        quality_status=reference.quality_status,
        trust_labels=reference.trust_labels,
        manifest=manifest,
    )


def _legacy_prewrite_snapshot_reference(
    runtime,
    reference: DataSnapshotRef,
    frame: pd.DataFrame,
    *,
    path: Path,
    logical_name: str,
    include_reference_binding: bool = False,
) -> DataSnapshotRef:
    frame.to_parquet(path, index=False)
    archived = runtime.service.archive.archive_file(
        path,
        logical_path=f"legacy/{logical_name}",
    )
    manifest = dict(reference.manifest)
    manifest.pop("frame_digest_schema")
    if not include_reference_binding:
        manifest.pop("snapshot_reference_schema", None)
        manifest.pop("snapshot_as_of", None)
    manifest.update(
        {
            "rows": len(frame),
            "columns": list(map(str, frame.columns)),
            "frame_digest": physical_canary_module._legacy_frame_digest(frame),
            "physical_object": archived.to_dict(),
        }
    )
    return _snapshot_reference_from_manifest(
        reference,
        manifest,
        uri=archived.uri,
    )


def test_parquet_boundary_digest_uses_exact_persisted_arrow_table(
    tmp_path: Path,
) -> None:
    frame = _persisted_boundary_frame()

    persisted = physical_canary_module._write_frame_once(
        tmp_path / "persisted",
        frame,
    )
    table = physical_canary_module._read_persisted_table(
        persisted.path,
        context="test",
    )

    assert physical_canary_module._normalize_logical_arrow_table(table).equals(
        physical_canary_module._canonical_arrow_table(frame)
    )
    assert persisted.frame_digest == (
        physical_canary_module._persisted_frame_digest(table)
    )
    assert persisted.path.name == f"{persisted.frame_digest}.parquet"
    assert persisted.rows == len(frame)
    assert persisted.columns == tuple(frame.columns)
    assert not list(persisted.path.parent.glob(".candidate-frame.*"))


@pytest.mark.parametrize(
    "string_type",
    [
        physical_canary_module.pa.string(),
        physical_canary_module.pa.large_string(),
        physical_canary_module.pa.string_view(),
    ],
)
def test_logical_arrow_normalizes_equivalent_string_layouts(string_type) -> None:
    pa = physical_canary_module.pa
    baseline = pa.table(
        {"text": pa.array(["alpha", None, "中文"], type=pa.string())}
    )
    variant = pa.table(
        {"text": pa.array(["alpha", None, "中文"], type=string_type)}
    )

    assert physical_canary_module._normalize_logical_arrow_table(
        baseline
    ).equals(physical_canary_module._normalize_logical_arrow_table(variant))
    assert physical_canary_module._normalize_logical_arrow_table(
        variant
    ).schema.field("text").type == pa.large_string()


def test_parquet_boundary_rejects_lossy_candidate_before_immutable_link(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = _persisted_boundary_frame()
    original = physical_canary_module.pq.write_table

    def lossy_write_table(table, path, *args, **kwargs):
        changed = table.to_pandas()
        changed.loc[0, "object_text"] = "changed-during-write"
        return original(
            physical_canary_module._canonical_arrow_table(changed),
            path,
            *args,
            **kwargs,
        )

    monkeypatch.setattr(
        physical_canary_module.pq,
        "write_table",
        lossy_write_table,
    )
    directory = tmp_path / "lossy"

    with pytest.raises(
        PhysicalCanaryDataRejected,
        match="changed logical frame values or schema",
    ):
        physical_canary_module._write_frame_once(directory, frame)

    assert not list(directory.glob("*.parquet"))
    assert not list(directory.glob(".candidate-frame.*"))


def test_parquet_boundary_is_idempotent_and_concurrent(
    tmp_path: Path,
) -> None:
    frame = _persisted_boundary_frame()
    directory = tmp_path / "concurrent"

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = tuple(
            executor.map(
                lambda _index: physical_canary_module._write_frame_once(
                    directory,
                    frame.copy(),
                ),
                range(8),
            )
        )

    first = results[0]
    assert all(item == first for item in results)
    assert list(directory.glob("*.parquet")) == [first.path]
    assert not list(directory.glob(".candidate-frame.*"))

    round_tripped = pd.read_parquet(first.path)
    assert physical_canary_module._write_frame_once(
        directory,
        round_tripped,
    ) == first


def test_parquet_boundary_rejects_tampered_existing_cache(
    tmp_path: Path,
) -> None:
    frame = _persisted_boundary_frame()
    directory = tmp_path / "tampered-cache"
    persisted = physical_canary_module._write_frame_once(directory, frame)
    tampered = frame.copy()
    tampered.loc[0, "object_text"] = "tampered"
    tampered.to_parquet(persisted.path, index=False)

    with pytest.raises(
        PhysicalCanaryDataRejected,
        match="concurrent immutable canary cache differs",
    ):
        physical_canary_module._write_frame_once(directory, frame)


def test_snapshot_publication_rejects_cache_swap_during_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        frame = _persisted_boundary_frame()
        real_file_integrity = physical_canary_module._file_integrity
        calls = 0

        def swap_before_second_integrity(path: Path) -> tuple[str, int]:
            nonlocal calls
            calls += 1
            if calls == 2:
                replacement = frame.copy()
                replacement.loc[0, "object_text"] = "swapped-after-table-read"
                replacement_path = path.with_suffix(".replacement.parquet")
                replacement.to_parquet(replacement_path, index=False)
                replacement_path.replace(path)
            return real_file_integrity(path)

        monkeypatch.setattr(
            physical_canary_module,
            "_file_integrity",
            swap_before_second_integrity,
        )
        snapshots_before = runtime.catalog.list_snapshots(limit=1_000)

        with pytest.raises(
            PhysicalCanaryDataRejected,
            match="changed while it was being verified",
        ):
            runtime.service._publish_frame_snapshot(
                run_id="cache-swap-fixture",
                session=SESSIONS[1],
                tier=SnapshotTier.BRONZE,
                role="source",
                frame=frame,
                parent_snapshot_ids=(),
                as_of=NOW,
            )

        assert calls == 2
        assert runtime.catalog.list_snapshots(limit=1_000) == snapshots_before
    finally:
        _close(runtime)
    runtime.engine.dispose()


def test_snapshot_manifest_binds_verified_parquet_bytes_and_roundtrip(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        frame = _persisted_boundary_frame()
        reference = runtime.service._publish_frame_snapshot(
            run_id="persisted-boundary-fixture",
            session=SESSIONS[1],
            tier=SnapshotTier.BRONZE,
            role="roundtrip",
            frame=frame,
            parent_snapshot_ids=(),
            as_of=NOW,
        )
        manifest = reference.manifest
        physical = manifest["physical_object"]
        local_path = (
            runtime.service.local_root
            / "persisted-boundary-fixture"
            / SnapshotTier.BRONZE.value
            / SESSIONS[1].isoformat()
            / "roundtrip"
            / f"{manifest['frame_digest']}.parquet"
        )

        assert manifest["frame_digest_schema"] == (
            physical_canary_module._PERSISTED_FRAME_DIGEST_SCHEMA
        )
        assert manifest["snapshot_reference_schema"] == (
            PHYSICAL_CANARY_SNAPSHOT_REFERENCE_SCHEMA
        )
        assert manifest["snapshot_as_of"] == NOW.isoformat()
        assert reference.as_of.isoformat() == manifest["snapshot_as_of"]
        assert local_path.is_file()
        assert physical_canary_module._file_integrity(local_path) == (
            physical["sha256"],
            physical["size_bytes"],
        )
        assert runtime.filesystem.objects[
            f"{runtime.archive.bucket}/{physical['key']}"
        ] == local_path.read_bytes()

        loaded, restored = runtime.service._load_snapshot_frame(
            reference.snapshot_id
        )
        assert loaded == reference
        assert physical_canary_module._canonical_arrow_table(restored).equals(
            physical_canary_module._canonical_arrow_table(frame)
        )
    finally:
        _close(runtime)
    runtime.engine.dispose()


def test_snapshot_restore_uses_one_pinned_arrow_table_for_digest_and_frame(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        frame = _persisted_boundary_frame()
        reference = runtime.service._publish_frame_snapshot(
            run_id="single-pinned-table-fixture",
            session=SESSIONS[1],
            tier=SnapshotTier.BRONZE,
            role="source",
            frame=frame,
            parent_snapshot_ids=(),
            as_of=NOW,
        )
        real_read_table = physical_canary_module.pq.read_table
        arrow_reads = 0

        def counted_read_table(*args, **kwargs):
            nonlocal arrow_reads
            arrow_reads += 1
            return real_read_table(*args, **kwargs)

        def forbidden_pandas_read(*_args, **_kwargs):
            raise AssertionError("snapshot loader must not parse Parquet twice")

        monkeypatch.setattr(
            physical_canary_module.pq,
            "read_table",
            counted_read_table,
        )
        monkeypatch.setattr(
            physical_canary_module.pd,
            "read_parquet",
            forbidden_pandas_read,
        )

        loaded, restored = runtime.service._load_snapshot_frame(
            reference.snapshot_id
        )

        assert loaded == reference
        assert arrow_reads == 1
        assert physical_canary_module._canonical_arrow_table(restored).equals(
            physical_canary_module._canonical_arrow_table(frame)
        )
    finally:
        _close(runtime)
    runtime.engine.dispose()


def test_snapshot_restore_rejects_substituted_payload_between_integrity_checks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        frame = _persisted_boundary_frame()
        reference = runtime.service._publish_frame_snapshot(
            run_id="pinned-payload-substitution-fixture",
            session=SESSIONS[1],
            tier=SnapshotTier.BRONZE,
            role="source",
            frame=frame,
            parent_snapshot_ids=(),
            as_of=NOW,
        )
        tampered = frame.copy()
        tampered.loc[0, "object_text"] = "substituted-between-checks"
        tampered_payload = physical_canary_module._write_frame_once(
            tmp_path / "substituted-payload",
            tampered,
        ).path.read_bytes()
        target = runtime.service._snapshot_cache_path(reference.snapshot_id)
        real_read_bytes = physical_canary_module._read_file_bytes

        def substitute_payload(path: Path) -> bytes:
            if path == target:
                return tampered_payload
            return real_read_bytes(path)

        monkeypatch.setattr(
            physical_canary_module,
            "_read_file_bytes",
            substitute_payload,
        )

        with pytest.raises(
            PhysicalCanaryDataRejected,
            match="pinned snapshot Parquet bytes differ",
        ):
            runtime.service._load_snapshot_frame(reference.snapshot_id)
    finally:
        _close(runtime)
    runtime.engine.dispose()


def test_snapshot_restore_rejects_cache_swap_after_pinned_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        frame = _persisted_boundary_frame()
        reference = runtime.service._publish_frame_snapshot(
            run_id="restore-cache-swap-fixture",
            session=SESSIONS[1],
            tier=SnapshotTier.BRONZE,
            role="source",
            frame=frame,
            parent_snapshot_ids=(),
            as_of=NOW,
        )
        replacement = frame.copy()
        replacement.loc[0, "object_text"] = "swapped-after-pinned-read"
        replacement_path = physical_canary_module._write_frame_once(
            tmp_path / "restore-cache-replacement",
            replacement,
        ).path
        target = runtime.service._snapshot_cache_path(reference.snapshot_id)
        real_file_integrity = physical_canary_module._file_integrity
        target_integrity_calls = 0

        def swap_before_second_integrity(path: Path) -> tuple[str, int]:
            nonlocal target_integrity_calls
            if path == target:
                target_integrity_calls += 1
                if target_integrity_calls == 2:
                    replacement_path.replace(path)
            return real_file_integrity(path)

        monkeypatch.setattr(
            physical_canary_module,
            "_file_integrity",
            swap_before_second_integrity,
        )

        with pytest.raises(
            PhysicalCanaryDataRejected,
            match="changed while bytes were pinned",
        ):
            runtime.service._load_snapshot_frame(reference.snapshot_id)
        assert target_integrity_calls == 2
    finally:
        _close(runtime)
    runtime.engine.dispose()


def test_snapshot_restore_rejects_unbound_legacy_prewrite_digest_and_accepts_successor(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        legacy_input = pd.DataFrame(
            {
                "security_id": [1, 2, 3],
                "value": [1.25, 2.5, -3.0],
            }
        )
        reference = runtime.service._publish_frame_snapshot(
            run_id="digest-schema-fixture",
            session=SESSIONS[1],
            tier=SnapshotTier.GOLD,
            role="mark",
            frame=legacy_input,
            parent_snapshot_ids=(),
            as_of=NOW,
        )

        legacy = _legacy_prewrite_snapshot_reference(
            runtime,
            reference,
            legacy_input,
            path=tmp_path / "recoverable-legacy.parquet",
            logical_name="recoverable-numeric",
        )
        runtime.catalog.register_snapshot(legacy)
        with pytest.raises(
            PhysicalCanaryDataRejected,
            match="reference schema is missing or unsupported",
        ):
            runtime.service._load_snapshot_frame(legacy.snapshot_id)

        loaded_successor, restored_successor = (
            runtime.service._load_snapshot_frame(reference.snapshot_id)
        )
        assert loaded_successor == reference
        assert reference.snapshot_id != legacy.snapshot_id
        assert physical_canary_module._canonical_arrow_table(
            restored_successor
        ).equals(
            physical_canary_module._canonical_arrow_table(legacy_input)
        )

        unknown_manifest = dict(reference.manifest)
        unknown_manifest["frame_digest_schema"] = "unknown/frame-digest/v99"
        unknown = _snapshot_reference_from_manifest(
            reference,
            unknown_manifest,
        )
        runtime.catalog.register_snapshot(unknown)
        with pytest.raises(
            PhysicalCanaryDataRejected,
            match="digest schema is unsupported",
        ):
            runtime.service._load_snapshot_frame(unknown.snapshot_id)
    finally:
        _close(runtime)
    runtime.engine.dispose()


def test_snapshot_restore_rejects_legacy_silver_object_string_digest_and_requires_successor(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        legacy_input = pd.DataFrame(
            {
                "ticker": pd.Series(
                    ["000001.SZ", None, "600000.SH"],
                    dtype=object,
                ),
                "value": [1.0, 2.0, 3.0],
            }
        )
        successor = runtime.service._publish_frame_snapshot(
            run_id="legacy-silver-object-string-fixture",
            session=SESSIONS[1],
            tier=SnapshotTier.SILVER,
            role="accepted_reconciled",
            frame=legacy_input,
            parent_snapshot_ids=(),
            as_of=NOW,
        )
        legacy = _legacy_prewrite_snapshot_reference(
            runtime,
            successor,
            legacy_input,
            path=tmp_path / "legacy-silver-object-string.parquet",
            logical_name="silver-object-string",
            include_reference_binding=True,
        )
        runtime.catalog.register_snapshot(legacy)

        with pd.option_context("future.infer_string", True):
            with pytest.raises(
                PhysicalCanaryDataRejected,
                match="restored snapshot frame digest differs",
            ):
                runtime.service._load_snapshot_frame(legacy.snapshot_id)

        loaded_successor, restored_successor = (
            runtime.service._load_snapshot_frame(successor.snapshot_id)
        )
        assert loaded_successor == successor
        assert successor.snapshot_id != legacy.snapshot_id
        assert successor.manifest["frame_digest_schema"] == (
            physical_canary_module._PERSISTED_FRAME_DIGEST_SCHEMA
        )
        assert physical_canary_module._canonical_arrow_table(
            restored_successor
        ).equals(
            physical_canary_module._canonical_arrow_table(legacy_input)
        )
    finally:
        _close(runtime)
    runtime.engine.dispose()


@pytest.mark.parametrize("invalid_schema", [None, "", 0, False])
def test_snapshot_restore_rejects_present_but_invalid_digest_schema(
    tmp_path: Path,
    invalid_schema: object,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        reference = runtime.service._publish_frame_snapshot(
            run_id="invalid-digest-schema-fixture",
            session=SESSIONS[1],
            tier=SnapshotTier.BRONZE,
            role="source",
            frame=_persisted_boundary_frame(),
            parent_snapshot_ids=(),
            as_of=NOW,
        )
        invalid_manifest = {
            **reference.manifest,
            "frame_digest_schema": invalid_schema,
        }
        invalid = _snapshot_reference_from_manifest(
            reference,
            invalid_manifest,
        )
        runtime.catalog.register_snapshot(invalid)

        with pytest.raises(
            PhysicalCanaryDataRejected,
            match="digest schema is unsupported",
        ):
            runtime.service._load_snapshot_frame(invalid.snapshot_id)
    finally:
        _close(runtime)
    runtime.engine.dispose()


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("reference_schema", "reference schema is missing or unsupported"),
        ("missing_as_of", "as_of binding is missing or malformed"),
        ("manifest_as_of", "as_of differs from its manifest"),
        ("reference_as_of", "as_of differs from its manifest"),
        ("noncanonical_as_of", "as_of binding is not canonical UTC"),
        ("noncanonical_reference_as_of", "reference as_of is not canonical UTC"),
    ],
)
def test_snapshot_restore_rejects_snapshot_reference_schema_and_as_of_tamper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
    message: str,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        reference = runtime.service._publish_frame_snapshot(
            run_id=f"snapshot-reference-{mutation}-fixture",
            session=SESSIONS[1],
            tier=SnapshotTier.BRONZE,
            role="source",
            frame=_persisted_boundary_frame(),
            parent_snapshot_ids=(),
            as_of=NOW,
        )
        record = runtime.catalog.get_snapshot(reference.snapshot_id)
        assert record is not None
        manifest = dict(reference.manifest)
        if mutation == "reference_schema":
            manifest["snapshot_reference_schema"] = (
                "unknown/physical-canary-snapshot-reference/v99"
            )
            tampered = _snapshot_reference_from_manifest(reference, manifest)
        elif mutation == "missing_as_of":
            manifest.pop("snapshot_as_of")
            tampered = _snapshot_reference_from_manifest(reference, manifest)
        elif mutation == "manifest_as_of":
            manifest["snapshot_as_of"] = (NOW + timedelta(seconds=1)).isoformat()
            tampered = _snapshot_reference_from_manifest(reference, manifest)
        elif mutation == "reference_as_of":
            tampered = _snapshot_reference_from_manifest(
                reference,
                manifest,
                as_of=NOW + timedelta(seconds=1),
            )
        elif mutation == "noncanonical_as_of":
            manifest["snapshot_as_of"] = NOW.astimezone(
                timezone(timedelta(hours=8))
            ).isoformat()
            tampered = _snapshot_reference_from_manifest(reference, manifest)
        elif mutation == "noncanonical_reference_as_of":
            tampered = _snapshot_reference_from_manifest(
                reference,
                manifest,
                as_of=NOW.astimezone(timezone(timedelta(hours=8))),
            )
        else:  # pragma: no cover - the parametrization is closed above.
            raise AssertionError(mutation)

        monkeypatch.setattr(
            runtime.catalog,
            "get_snapshot",
            lambda _snapshot_id: replace(record, reference=tampered),
        )

        with pytest.raises(PhysicalCanaryDataRejected, match=message):
            runtime.service._load_snapshot_frame(tampered.snapshot_id)
    finally:
        _close(runtime)
    runtime.engine.dispose()


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("manifest_content", "content hash differs"),
        ("snapshot_id", "canonical identity"),
        ("tier", "tier differs"),
        ("parents", "parents differ"),
        ("uri", "URI differs"),
    ],
)
def test_snapshot_restore_rejects_catalog_reference_manifest_identity_tamper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
    message: str,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        reference = runtime.service._publish_frame_snapshot(
            run_id=f"catalog-identity-{mutation}-fixture",
            session=SESSIONS[1],
            tier=SnapshotTier.BRONZE,
            role="source",
            frame=_persisted_boundary_frame(),
            parent_snapshot_ids=(),
            as_of=NOW,
        )
        record = runtime.catalog.get_snapshot(reference.snapshot_id)
        assert record is not None
        requested_snapshot_id = reference.snapshot_id
        tampered = reference
        if mutation == "manifest_content":
            tampered = reference.model_copy(
                update={
                    "manifest": {
                        **reference.manifest,
                        "role": "tampered-role",
                    }
                }
            )
        elif mutation == "snapshot_id":
            requested_snapshot_id = "pec_bronze_" + "f" * 56
            tampered = reference.model_copy(
                update={"snapshot_id": requested_snapshot_id}
            )
        elif mutation == "tier":
            requested_snapshot_id = physical_canary_module._snapshot_id(
                SnapshotTier.SILVER,
                reference.content_hash,
            )
            tampered = reference.model_copy(
                update={
                    "snapshot_id": requested_snapshot_id,
                    "tier": SnapshotTier.SILVER,
                }
            )
        elif mutation == "parents":
            tampered = reference.model_copy(
                update={"parent_snapshot_ids": ("unexpected-parent",)}
            )
        elif mutation == "uri":
            tampered = reference.model_copy(
                update={
                    "uri": (
                        f"s3://{runtime.archive.bucket}/{CANARY_OBJECT_PREFIX}/"
                        "unexpected-object"
                    )
                }
            )
        else:  # pragma: no cover - the parametrization is closed above.
            raise AssertionError(mutation)

        monkeypatch.setattr(
            runtime.catalog,
            "get_snapshot",
            lambda _snapshot_id: replace(record, reference=tampered),
        )

        with pytest.raises(PhysicalCanaryDataRejected, match=message):
            runtime.service._load_snapshot_frame(requested_snapshot_id)
    finally:
        _close(runtime)
    runtime.engine.dispose()


def test_snapshot_restore_rejects_tampered_archived_bytes(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        reference = runtime.service._publish_frame_snapshot(
            run_id="tampered-object-fixture",
            session=SESSIONS[1],
            tier=SnapshotTier.BRONZE,
            role="source",
            frame=_persisted_boundary_frame(),
            parent_snapshot_ids=(),
            as_of=NOW,
        )
        physical = reference.manifest["physical_object"]
        remote = f"{runtime.archive.bucket}/{physical['key']}"
        payload = bytearray(runtime.filesystem.objects[remote])
        payload[len(payload) // 2] ^= 0x01
        runtime.filesystem.objects[remote] = bytes(payload)

        with pytest.raises(ObjectStoreIntegrityError):
            runtime.service._load_snapshot_frame(reference.snapshot_id)
    finally:
        _close(runtime)
    runtime.engine.dispose()


def _close(runtime) -> None:
    runtime.authority.close()
    runtime.ledger.close()
    runtime.catalog.close()


def _enable_generation_audit_fixture(runtime) -> None:
    """Enable production-only audits without invoking host build admission."""

    runtime.service.controlled_test = False
    runtime.service.physical_source_attested = True
    runtime.service._partition_source = lambda binding: (
        f"engcan_{binding.adapter.source_id}_{'c' * 24}"
    )


def _enable_stable_evaluator_fixture(runtime) -> None:
    runtime.service._evaluator_identity = lambda: {
        "mode": "production_image",
        "runtime_deployment": {
            "build_identity_hash": "a" * 64,
            "oci_image_id": "sha256:" + "b" * 64,
            "runtime_contract_hash": "c" * 64,
        },
    }


def test_production_partition_source_versions_contract_without_rewriting_quarantine(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        binding = runtime.service.bindings[0]
        assert runtime.service._partition_source(binding).startswith("engtest_")

        runtime.service.controlled_test = False
        _enable_stable_evaluator_fixture(runtime)
        original = runtime.service._partition_source(binding)
        revised = runtime.service._partition_source(
            replace(
                binding,
                request_parameters={**binding.request_parameters, "contract_revision": 2},
            )
        )

        assert original.startswith("engcan_")
        assert revised.startswith("engcan_")
        assert original != revised
        assert len(original) <= 80
        assert len(revised) <= 80
    finally:
        _close(runtime)
    runtime.engine.dispose()


def test_calendar_admission_requires_two_independent_provider_identities(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        calendars = [
            binding
            for binding in runtime.service.bindings
            if binding.dataset == "trade_calendar"
        ]
        assert len(calendars) == 2
        duplicate = replace(calendars[0])
        runtime.service.bindings = tuple(
            binding
            for binding in runtime.service.bindings
            if binding.dataset != "trade_calendar"
        ) + (calendars[0], duplicate)

        with pytest.raises(
            PhysicalCanaryAdmissionError,
            match="independent calendar providers",
        ):
            runtime.service._validate_binding_shape()
    finally:
        _close(runtime)
    runtime.engine.dispose()


def test_production_partition_source_versions_transport_and_parser_contract(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        binding = runtime.service.bindings[0]
        first = DiemengSourceAdapter(
            base_url="https://data.diemeng.chat/api",
            api_key="first-secret",
            contracts=(CALENDAR,),
            endpoint_map={"trade_calendar": "/calendar/trading"},
            method_map={"trade_calendar": "POST"},
            response_paths={"trade_calendar": "data.list"},
            column_mapping={"provider_date": "cal_date"},
            constant_fields={"exchange": "SSE"},
            probe_dataset="trade_calendar",
        )
        second = DiemengSourceAdapter(
            base_url="https://mirror.example/api",
            api_key="second-secret",
            contracts=(CALENDAR,),
            endpoint_map={"trade_calendar": "/v2/calendar"},
            method_map={"trade_calendar": "GET"},
            response_paths={"trade_calendar": "payload.rows"},
            column_mapping={"date": "cal_date"},
            constant_fields={"exchange": "SZSE"},
            probe_dataset="trade_calendar",
        )
        runtime.service.controlled_test = False
        _enable_stable_evaluator_fixture(runtime)

        first_source = runtime.service._partition_source(replace(binding, adapter=first))
        second_source = runtime.service._partition_source(replace(binding, adapter=second))

        assert first_source.startswith("engcan_diemeng_")
        assert second_source.startswith("engcan_diemeng_")
        assert first_source != second_source
        assert "first-secret" not in str(first.public_contract_identity())
        assert "second-secret" not in str(second.public_contract_identity())
    finally:
        _close(runtime)
    runtime.engine.dispose()


def test_provider_generation_identity_never_matches_nested_provider_prefix() -> None:
    generation = "1" * 24

    assert PhysicalEngineeringCanaryService._provider_generation_source_matches(
        f"engcan_tushare_{generation}", "tushare"
    )
    assert PhysicalEngineeringCanaryService._provider_generation_source_matches(
        f"engcan_tushare_pro_{generation}", "tushare_pro"
    )
    assert not PhysicalEngineeringCanaryService._provider_generation_source_matches(
        f"engcan_tushare_pro_{generation}", "tushare"
    )
    assert PhysicalEngineeringCanaryService._provider_generation_source_matches(
        f"engcan_tushare_{'2' * 12}", "tushare"
    )


@pytest.mark.parametrize(
    ("source_identity", "generation_base", "expected"),
    [
        ("engineering_canary", "engineering_canary", True),
        (f"engineering_canary_{'1' * 12}", "engineering_canary", True),
        (f"engineering_canary_{'2' * 24}", "engineering_canary", True),
        ("engineering_canary_backup", "engineering_canary", False),
        ("engineering_canary_12345678901g", "engineering_canary", False),
        ("engcan_tushare", "engcan_tushare", True),
        (f"engcan_tushare_{'3' * 12}", "engcan_tushare", True),
        (f"engcan_tushare_{'4' * 24}", "engcan_tushare", True),
        ("engcan_tushare_old", "engcan_tushare", False),
    ],
)
def test_generation_family_accepts_only_exact_or_hex_generation_suffix(
    source_identity: str,
    generation_base: str,
    expected: bool,
) -> None:
    assert (
        physical_canary_module.physical_canary_generation_source_matches(
            source_identity,
            generation_base,
        )
        is expected
    )


def test_legacy_generation_selects_only_exact_nested_provider_family(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        record = runtime.ledger.ensure_partition(
            PartitionIdentity(
                f"engcan_foo_bar_{'1' * 12}",
                "daily",
                SESSIONS[0].isoformat(),
            ),
            created_at=NOW,
            input_hash="a" * 64,
        )
        foo_generation = f"engcan_foo_{'2' * 24}"
        nested_generation = f"engcan_foo_bar_{'3' * 24}"

        selected = runtime.service._current_generation_for_legacy(
            record,
            source_generations={
                ("foo", "daily"): foo_generation,
                ("foo_bar", "daily"): nested_generation,
            },
            stage_generation="",
        )

        assert selected == nested_generation
    finally:
        _close(runtime)
    runtime.engine.dispose()


def test_opening_probe_keeps_descriptor_and_source_generation_stable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Response:
        status_code = 200

        @staticmethod
        def json():
            return {
                "code": 200,
                "data": {
                    "items": [
                        {
                            "stock_code": "000001.SZ",
                            "trade_time": f"{SESSIONS[1].isoformat()}T09:30:00+08:00",
                            "open": 10.0,
                            "high": 10.1,
                            "low": 9.9,
                            "close": 10.0,
                            "vol": 1000.0,
                            "amount": 10000.0,
                        }
                    ]
                },
            }

    class SessionFixture:
        @staticmethod
        def request(_method, _url, **_kwargs):
            return Response()

    runtime = _runtime(tmp_path)
    try:
        opening = next(
            binding
            for binding in runtime.service.bindings
            if binding.dataset == "opening_execution"
        )
        adapter = DiemengSourceAdapter(
            base_url="https://data.diemeng.chat/api",
            api_key="fixture-secret",
            contracts=(OPENING,),
            endpoint_map={"opening_execution": "/stock/history/minute"},
            method_map={"opening_execution": "GET"},
            response_paths={"opening_execution": "data.items"},
            probe_dataset="opening_execution",
            probe_parameters={"level": "1min", "page": 0},
            session=SessionFixture(),
            max_attempts=1,
        )
        binding = replace(opening, adapter=adapter)
        runtime.service.bindings = tuple(
            binding if item is opening else item
            for item in runtime.service.bindings
        )
        runtime.service.controlled_test = False
        monkeypatch.setattr(
            runtime.service,
            "_evaluator_identity",
            lambda: {
                "mode": "production_image",
                "runtime_deployment": {
                    "build_identity_hash": "a" * 64,
                    "oci_image_id": "sha256:" + "b" * 64,
                    "runtime_contract_hash": "c" * 64,
                },
            },
        )
        descriptor_before = binding.descriptor()
        source_before = runtime.service._partition_source(binding)
        probe_parameters_before = dict(adapter.probe_parameters)

        runtime.service._probe_sources(SESSIONS)

        assert adapter.probe_parameters == probe_parameters_before
        assert binding.descriptor() == descriptor_before
        assert runtime.service._partition_source(binding) == source_before
    finally:
        _close(runtime)
    runtime.engine.dispose()


def test_legacy_pending_generation_never_creates_isolation_claim(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        binding = runtime.service.bindings[0]
        legacy_identity = PartitionIdentity(
            f"engcan_{binding.adapter.source_id}",
            binding.dataset,
            SESSIONS[0].isoformat(),
        )
        runtime.ledger.ensure_partition(
            legacy_identity,
            created_at=NOW,
            input_hash="9" * 64,
            details={"legacy_generation": True},
        )
        _enable_generation_audit_fixture(runtime)

        runtime.service._audit_legacy_source_generations()

        incidents = runtime.ledger.list_incidents()
        matching = [
            item
            for item in incidents
            if item.error_code == "legacy_canary_generation_isolated"
            and item.partition_run_id == legacy_identity.partition_run_id
        ]
        assert matching == []
        replacement = _successful_partition_with_snapshot(
            runtime,
            identity=PartitionIdentity(
                runtime.service._partition_source(binding),
                binding.dataset,
                SESSIONS[0].isoformat(),
            ),
            tier=SnapshotTier.BRONZE,
            role="current_source_generation",
        )
        runtime.service._audit_legacy_source_generations(replacement=replacement)
        assert runtime.ledger.list_incidents() == ()
    finally:
        _close(runtime)
    runtime.engine.dispose()


def _failed_partition_with_incident(
    runtime,
    *,
    identity: PartitionIdentity,
    stage: IncidentStage,
    error_code: str = "fixture_generation_failed",
    terminal_status: PartitionStatus = PartitionStatus.FAILED,
):
    run_id = "legacy-generation-failure-fixture"
    if runtime.catalog.get_run(run_id) is None:
        runtime.catalog.save_run(
            RunRecord(
                run_id=run_id,
                run_type=PHYSICAL_RUN_TYPE,
                status="running",
                input_fingerprint=content_fingerprint(
                    run_id, domain="tests/legacy-generation-failure-run"
                ),
                started_at=NOW - timedelta(hours=1),
                metadata={"fixture": True},
            )
        )
    input_hash = content_fingerprint(
        identity.partition_run_id, domain="tests/legacy-canary-partition-input"
    )
    stage_name = {
        IncidentStage.SOURCE: "bronze",
        IncidentStage.SILVER: "silver",
        IncidentStage.DATA_QUALITY: "data_quality",
        IncidentStage.GOLD: "gold",
    }[stage]
    stage_details: dict[str, object] = {"stage": stage_name}
    if stage is IncidentStage.SOURCE:
        providers = {
            binding.adapter.source_id
            for binding in runtime.service.bindings
            if binding.dataset == identity.dataset
            and runtime.service._provider_generation_source_matches(
                identity.source_id,
                binding.adapter.source_id,
            )
        }
        if len(providers) == 1:
            stage_details.update(
                {"source_id": next(iter(providers)), "dataset": identity.dataset}
            )
    elif stage is IncidentStage.GOLD:
        role = identity.dataset.removeprefix("gold_")
        stage_details.update({"role": role, "gold_role": role})
    lineage = runtime.service._partition_claim_lineage(
        identity,
        input_hash=input_hash,
        stage=stage,
        details=stage_details,
        evidence_hashes=(),
    )
    lineage_hash = content_fingerprint(
        lineage,
        domain=(
            "factor-lab/research-os/v1/"
            "physical-canary-partition-claim-lineage"
        ),
    )
    labels = _labels(
        physical_source_attested=runtime.service.physical_source_attested,
        controlled_test=runtime.service.controlled_test,
    )
    record_details = {
        **labels,
        "run_id": run_id,
        **stage_details,
        "claim_lineage": lineage,
        "claim_lineage_hash": lineage_hash,
    }
    runtime.ledger.ensure_partition(
        identity,
        created_at=NOW,
        input_hash=input_hash,
        details=record_details,
    )
    lease = runtime.ledger.claim(
        identity=identity,
        owner="legacy-canary-fixture",
        now=NOW,
        lease_for=pd.Timedelta(minutes=10).to_pytimedelta(),
    )
    assert lease is not None
    failed = runtime.ledger.finish(
        lease,
        status=terminal_status,
        completed_at=NOW,
        run_id=run_id,
        error_code=error_code,
        error="fixture legacy generation failure",
        details=record_details,
    )
    incident = runtime.ledger.record_incident(
        partition_key=identity.partition_key,
        stage=stage,
        error_code=error_code,
        message="fixture legacy generation failure",
        occurred_at=NOW,
        partition_run_id=identity.partition_run_id,
        source_ids=(identity.source_id,),
        payload={
            **labels,
            "run_id": run_id,
            "failure_type": "FixtureGenerationFailure",
            "partition_terminalized": True,
            "partition_terminal_status": terminal_status.value,
            "terminalization_failure_type": None,
            "claim_lineage": lineage,
            "claim_lineage_hash": lineage_hash,
        },
    )
    return failed, incident


def _successful_partition_with_snapshot(
    runtime,
    *,
    identity: PartitionIdentity,
    tier: SnapshotTier,
    role: str,
    override_output_hash: str | None = None,
    completed_at: datetime = NOW,
    details: dict[str, object] | None = None,
):
    run_id = "generation-replacement-fixture"
    if runtime.catalog.get_run(run_id) is None:
        runtime.catalog.save_run(
            RunRecord(
                run_id=run_id,
                run_type=PHYSICAL_RUN_TYPE,
                status="running",
                input_fingerprint=content_fingerprint(
                    run_id, domain="tests/generation-replacement-run"
                ),
                started_at=NOW - timedelta(hours=1),
                metadata={"fixture": True},
            )
        )
    stage = (
        IncidentStage.SILVER
        if identity.dataset == "silver_accepted"
        else IncidentStage.DATA_QUALITY
        if identity.dataset == "dq_accepted"
        else IncidentStage.GOLD
        if identity.dataset.startswith("gold_")
        else IncidentStage.SOURCE
    )
    providers = {
        binding.adapter.source_id
        for binding in runtime.service.bindings
        if binding.dataset == identity.dataset
        and runtime.service._provider_generation_source_matches(
            identity.source_id,
            binding.adapter.source_id,
        )
    }
    published_role = (
        f"{next(iter(providers))}_{identity.dataset}"
        if stage is IncidentStage.SOURCE and len(providers) == 1
        else role
    )
    report = {
        "schema_version": "test/v1",
        "status": "accepted",
        "trade_date": identity.partition_key,
    }
    if tier is SnapshotTier.BRONZE:
        parent_references = ()
    else:
        parent_tier = (
            SnapshotTier.BRONZE
            if tier is SnapshotTier.SILVER
            else SnapshotTier.SILVER
        )
        parent_role = (
            "fixture_parent_raw"
            if parent_tier is SnapshotTier.BRONZE
            else "accepted_reconciled"
        )
        parent_references = (
            runtime.service._publish_frame_snapshot(
                run_id=run_id,
                session=date.fromisoformat(identity.partition_key),
                tier=parent_tier,
                role=parent_role,
                frame=pd.DataFrame(
                    {
                        "ticker": ["000001.SZ"],
                        "trade_date": [identity.partition_key],
                        "value": [0.5],
                    }
                ),
                parent_snapshot_ids=(),
                as_of=NOW,
            ),
        )
    reference = runtime.service._publish_frame_snapshot(
        run_id=run_id,
        session=date.fromisoformat(identity.partition_key),
        tier=tier,
        role=published_role,
        frame=pd.DataFrame(
            {
                "ticker": ["000001.SZ"],
                "trade_date": [identity.partition_key],
                "value": [1.0],
            }
        ),
        parent_snapshot_ids=tuple(item.snapshot_id for item in parent_references),
        as_of=NOW,
        extra_manifest=(
            {"quality_report": report}
            if identity.dataset == "dq_accepted"
            else None
        ),
    )
    output_hash = (
        content_fingerprint(
            report,
            domain="factor-lab/research-os/v1/physical-canary-dq-report",
        )
        if identity.dataset == "dq_accepted"
        else reference.content_hash
    )
    input_hash = content_fingerprint(
        identity.partition_run_id, domain="tests/current-canary-partition-input"
    )
    stage_name = {
        IncidentStage.SOURCE: "bronze",
        IncidentStage.SILVER: "silver",
        IncidentStage.DATA_QUALITY: "data_quality",
        IncidentStage.GOLD: "gold",
    }[stage]
    stage_details: dict[str, object] = {
        "stage": stage_name,
        "role": published_role,
        **dict(details or {}),
    }
    if stage is IncidentStage.SOURCE:
        if len(providers) == 1:
            stage_details.update(
                {"source_id": next(iter(providers)), "dataset": identity.dataset}
            )
    elif stage is IncidentStage.SILVER:
        stage_details["parent_snapshot_ids"] = list(reference.parent_snapshot_ids)
    elif stage is IncidentStage.DATA_QUALITY:
        stage_details.update(
            {
                "silver_snapshot_id": reference.snapshot_id,
                "quality_report_hash": output_hash,
            }
        )
    elif stage is IncidentStage.GOLD:
        stage_details.update(
            {
                "gold_role": published_role,
                "silver_snapshot_id": reference.parent_snapshot_ids[0],
            }
        )
        if all(
            str(stage_details.get(field) or "")
            for field in ("stage_source", "calendar_hash")
        ):
            stage_details["attempted_gold_input_hash"] = _gold_attempted_input_hash(
                stage_source=str(stage_details["stage_source"]),
                session=date.fromisoformat(identity.partition_key),
                role=published_role,
                silver_snapshot_id=reference.parent_snapshot_ids[0],
                calendar_hash=str(stage_details["calendar_hash"]),
            )
    logical_parent_references = (
        (reference,)
        if stage is IncidentStage.DATA_QUALITY
        else parent_references
    )
    lineage = runtime.service._partition_claim_lineage(
        identity,
        input_hash=input_hash,
        stage=stage,
        details=stage_details,
        evidence_hashes=tuple(
            item.content_hash for item in logical_parent_references
        ),
    )
    lineage_hash = content_fingerprint(
        lineage,
        domain=(
            "factor-lab/research-os/v1/"
            "physical-canary-partition-claim-lineage"
        ),
    )
    record_details = {
        **_labels(
            physical_source_attested=runtime.service.physical_source_attested,
            controlled_test=runtime.service.controlled_test,
        ),
        "run_id": run_id,
        **stage_details,
        "claim_lineage": lineage,
        "claim_lineage_hash": lineage_hash,
    }
    runtime.ledger.ensure_partition(
        identity,
        created_at=NOW,
        input_hash=input_hash,
        details=record_details,
    )
    lease = runtime.ledger.claim(
        identity=identity,
        owner="current-canary-fixture",
        now=NOW,
        lease_for=pd.Timedelta(minutes=10).to_pytimedelta(),
    )
    assert lease is not None
    return runtime.ledger.finish(
        lease,
        status=PartitionStatus.SUCCEEDED,
        completed_at=completed_at,
        run_id=run_id,
        output_snapshot_id=reference.snapshot_id,
        output_hash=override_output_hash or output_hash,
        details=record_details,
    )


def test_legacy_source_generation_closes_exact_original_failure_and_bridge(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        _enable_generation_audit_fixture(runtime)
        binding = runtime.service.bindings[0]
        legacy_identity = PartitionIdentity(
            f"engcan_{binding.adapter.source_id}",
            binding.dataset,
            SESSIONS[0].isoformat(),
        )
        legacy, failure = _failed_partition_with_incident(
            runtime,
            identity=legacy_identity,
            stage=IncidentStage.SOURCE,
        )

        runtime.service._audit_legacy_source_generations()
        open_incidents = runtime.ledger.list_incidents(status=IncidentStatus.OPEN)
        assert [item.incident_id for item in open_incidents] == [
            failure.incident_id
        ]

        replacement = _successful_partition_with_snapshot(
            runtime,
            identity=PartitionIdentity(
                runtime.service._partition_source(binding),
                binding.dataset,
                SESSIONS[0].isoformat(),
            ),
            tier=SnapshotTier.BRONZE,
            role="current_source_generation",
        )
        runtime.service._audit_legacy_source_generations(replacement=replacement)
        closed = runtime.ledger.list_incidents(status=IncidentStatus.SUPERSEDED)
        assert len(closed) == 2
        by_error = {item.error_code: item for item in closed}
        assert set(by_error) == {
            "fixture_generation_failed",
            "legacy_canary_generation_isolated",
        }
        assert by_error["fixture_generation_failed"].incident_id == failure.incident_id
        assert runtime.service._verified_replacement_evidence(replacement) is not None
        partitions = {
            legacy.identity.partition_run_id: legacy,
            replacement.identity.partition_run_id: replacement,
        }
        readiness = object.__new__(ProductionReadinessAuditor)
        original_failure = by_error["fixture_generation_failed"]
        assert readiness._physical_canary_producer_labels_are_valid(
            {
                key: value
                for key, value in original_failure.payload.items()
                if key != "resolution"
            }
        )
        assert readiness._physical_canary_claim_lineage_is_valid(legacy)
        assert readiness._physical_canary_claim_lineage_is_valid(replacement)
        assert readiness._physical_canary_failure_incident_matches_partition(
            original_failure, legacy
        )
        schema_validity = {
            incident.error_code: readiness._physical_canary_resolution_schema_is_valid(
                incident,
                replacement=replacement,
                partitions=partitions,
            )
            for incident in closed
        }
        assert all(schema_validity.values()), schema_validity
        for incident in closed:
            resolution = incident.payload["resolution"]
            assert (
                resolution["legacy_partition_run_id"]
                == legacy_identity.partition_run_id
            )
            assert (
                resolution["replacement_partition_run_id"]
                == replacement.identity.partition_run_id
            )
            assert (
                resolution["replacement_output_snapshot_id"]
                == replacement.output_snapshot_id
            )
            assert resolution["replacement_output_hash"] == replacement.output_hash

        resolution_hashes = {item.incident_id: item.resolution_hash for item in closed}
        runtime.service._audit_legacy_source_generations(replacement=replacement)
        repeated = runtime.ledger.list_incidents(status=IncidentStatus.SUPERSEDED)
        assert {
            item.incident_id: item.resolution_hash for item in repeated
        } == resolution_hashes
        assert runtime.ledger.get_partition(legacy_identity) == legacy
        assert runtime.ledger.list_incidents(status=IncidentStatus.OPEN) == ()
    finally:
        _close(runtime)
    runtime.engine.dispose()


@pytest.mark.parametrize(
    "tamper",
    ["missing_lineage", "bad_lineage_hash", "wrong_run_id", "wrong_role"],
)
def test_generation_bridge_is_not_written_for_unverifiable_replacement_metadata(
    tmp_path: Path,
    tamper: str,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        _enable_generation_audit_fixture(runtime)
        binding = runtime.service.bindings[0]
        legacy_identity = PartitionIdentity(
            f"engcan_{binding.adapter.source_id}",
            binding.dataset,
            SESSIONS[0].isoformat(),
        )
        runtime.ledger.ensure_partition(
            legacy_identity,
            created_at=NOW - timedelta(minutes=2),
            input_hash="8" * 64,
        )
        lease = runtime.ledger.claim(
            identity=legacy_identity,
            owner="unverifiable-legacy",
            now=NOW - timedelta(minutes=2),
            lease_for=timedelta(minutes=10),
        )
        assert lease is not None
        runtime.ledger.finish(
            lease,
            status=PartitionStatus.FAILED,
            completed_at=NOW - timedelta(minutes=1),
            error_code="fixture_legacy_failed",
            error="fixture legacy failed",
        )
        replacement = _successful_partition_with_snapshot(
            runtime,
            identity=PartitionIdentity(
                runtime.service._partition_source(binding),
                binding.dataset,
                SESSIONS[0].isoformat(),
            ),
            tier=SnapshotTier.BRONZE,
            role=f"{binding.adapter.source_id}_{binding.dataset}",
        )
        tampered_details = dict(replacement.details)
        if tamper == "missing_lineage":
            tampered_details.pop("claim_lineage", None)
        elif tamper == "bad_lineage_hash":
            tampered_details["claim_lineage_hash"] = "0" * 64
        elif tamper == "wrong_run_id":
            tampered_details["run_id"] = "wrong-run"
        else:
            tampered_details["role"] = "wrong-role"
        unverifiable = replace(replacement, details=tampered_details)

        runtime.service._audit_legacy_source_generations(
            replacement=unverifiable
        )

        assert runtime.service._verified_replacement_evidence(unverifiable) is None
        assert runtime.ledger.list_incidents() == ()
    finally:
        _close(runtime)
    runtime.engine.dispose()


def test_unverified_generation_creates_no_stale_claim_for_later_generation(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        binding = runtime.service.bindings[0]
        runtime.service.controlled_test = False
        generation_b = f"engcan_{binding.adapter.source_id}_{'b' * 24}"
        generation_c = f"engcan_{binding.adapter.source_id}_{'c' * 24}"
        legacy_identity = PartitionIdentity(
            f"engcan_{binding.adapter.source_id}",
            binding.dataset,
            SESSIONS[0].isoformat(),
        )
        runtime.ledger.ensure_partition(
            legacy_identity,
            created_at=NOW,
            input_hash="9" * 64,
            details={"legacy_generation": True},
        )
        legacy_lease = runtime.ledger.claim(
            identity=legacy_identity,
            owner="legacy-generation-a",
            now=NOW,
            lease_for=timedelta(minutes=10),
        )
        assert legacy_lease is not None
        runtime.ledger.finish(
            legacy_lease,
            status=PartitionStatus.FAILED,
            completed_at=NOW,
            error_code="fixture_generation_a_failed",
            error="fixture generation A failed",
        )

        runtime.service._partition_source = lambda _binding: generation_b
        runtime.service._audit_legacy_source_generations()
        assert runtime.ledger.list_incidents() == ()

        runtime.service._partition_source = lambda _binding: generation_c
        replacement = _successful_partition_with_snapshot(
            runtime,
            identity=PartitionIdentity(
                generation_c,
                binding.dataset,
                SESSIONS[0].isoformat(),
            ),
            tier=SnapshotTier.BRONZE,
            role="current_source_generation",
        )
        runtime.service._audit_legacy_source_generations(replacement=replacement)

        assert runtime.ledger.list_incidents(status=IncidentStatus.OPEN) == ()
        closed = runtime.ledger.list_incidents(status=IncidentStatus.SUPERSEDED)
        assert len(closed) == 1
        assert closed[0].payload["current_source_id"] == generation_c
        assert (
            closed[0].payload["resolution"]["replacement_partition_run_id"]
            == replacement.identity.partition_run_id
        )
    finally:
        _close(runtime)
    runtime.engine.dispose()


def test_success_status_with_unverified_output_hash_does_not_close_incident(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        _enable_generation_audit_fixture(runtime)
        runtime.service._stage_source = lambda: CURRENT_STAGE_SOURCE
        legacy_identity = PartitionIdentity(
            LEGACY_STAGE_SOURCE,
            "gold_mark",
            SESSIONS[1].isoformat(),
        )
        _failed_partition_with_incident(
            runtime, identity=legacy_identity, stage=IncidentStage.GOLD
        )
        replacement = _successful_partition_with_snapshot(
            runtime,
            identity=PartitionIdentity(
                CURRENT_STAGE_SOURCE,
                "gold_mark",
                SESSIONS[1].isoformat(),
            ),
            tier=SnapshotTier.GOLD,
            role="mark",
            override_output_hash="f" * 64,
        )

        runtime.service._audit_legacy_source_generations(replacement=replacement)

        assert runtime.ledger.list_incidents(status=IncidentStatus.SUPERSEDED) == ()
        assert len(runtime.ledger.list_incidents(status=IncidentStatus.OPEN)) == 1
    finally:
        _close(runtime)
    runtime.engine.dispose()


@pytest.mark.parametrize(
    "terminal_status",
    [PartitionStatus.DISPUTED, PartitionStatus.QUARANTINED],
)
def test_non_retryable_current_partition_incident_is_not_auto_resolved(
    tmp_path: Path, terminal_status: PartitionStatus
) -> None:
    runtime = _runtime(tmp_path)
    try:
        replacement = _successful_partition_with_snapshot(
            runtime,
            identity=PartitionIdentity(
                "engineering_canary_test",
                "gold_mark",
                SESSIONS[1].isoformat(),
            ),
            tier=SnapshotTier.GOLD,
            role="mark",
        )
        incident = runtime.ledger.record_incident(
            partition_key=SESSIONS[1].isoformat(),
            stage=IncidentStage.GOLD,
            error_code=f"fixture_{terminal_status.value}",
            message="non-retryable terminal incident",
            occurred_at=NOW,
            partition_run_id=replacement.identity.partition_run_id,
            payload={"partition_terminal_status": terminal_status.value},
        )

        runtime.service._resolve_retried_partition_incidents(
            replace(replacement, attempts=2)
        )

        remaining = runtime.ledger.list_incidents(status=IncidentStatus.OPEN)
        assert [item.incident_id for item in remaining] == [incident.incident_id]
        assert runtime.ledger.list_incidents(status=IncidentStatus.RESOLVED) == ()
    finally:
        _close(runtime)
    runtime.engine.dispose()


@pytest.mark.parametrize(
    "origin_tamper",
    ["stage", "error_code", "source_ids", "evidence_hashes"],
)
def test_retry_resolution_rejects_wrong_original_incident_origin(
    tmp_path: Path,
    origin_tamper: str,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        replacement = _successful_partition_with_snapshot(
            runtime,
            identity=PartitionIdentity(
                "engineering_canary_test",
                "gold_mark",
                SESSIONS[1].isoformat(),
            ),
            tier=SnapshotTier.GOLD,
            role="mark",
        )
        lineage = replacement.details["claim_lineage"]
        origin = {
            "stage": IncidentStage.GOLD,
            "error_code": "gold_publication_failed",
            "source_ids": (replacement.identity.source_id,),
            "evidence_hashes": tuple(lineage["parent_evidence_hashes"]),
        }
        if origin_tamper == "stage":
            origin["stage"] = IncidentStage.SILVER
        elif origin_tamper == "error_code":
            origin["error_code"] = "source_fetch_failed"
        elif origin_tamper == "source_ids":
            origin["source_ids"] = ("engineering_canary_test_other",)
        else:
            origin["evidence_hashes"] = ("f" * 64,)
        incident = runtime.ledger.record_incident(
            partition_key=replacement.identity.partition_key,
            stage=origin["stage"],
            error_code=origin["error_code"],
            message="tampered retry origin must remain open",
            occurred_at=NOW,
            partition_run_id=replacement.identity.partition_run_id,
            source_ids=origin["source_ids"],
            evidence_hashes=origin["evidence_hashes"],
            payload={
                **_labels(
                    physical_source_attested=(
                        runtime.service.physical_source_attested
                    ),
                    controlled_test=runtime.service.controlled_test,
                ),
                "run_id": replacement.run_id,
                "partition_terminalized": True,
                "partition_terminal_status": PartitionStatus.FAILED.value,
                "terminalization_failure_type": None,
                "claim_lineage": lineage,
                "claim_lineage_hash": replacement.details["claim_lineage_hash"],
            },
        )

        runtime.service._now = lambda: NOW + timedelta(minutes=1)
        runtime.service._resolve_retried_partition_incidents(
            replace(replacement, attempts=2)
        )

        assert [
            item.incident_id
            for item in runtime.ledger.list_incidents(status=IncidentStatus.OPEN)
        ] == [incident.incident_id]
        assert runtime.ledger.list_incidents(status=IncidentStatus.RESOLVED) == ()
    finally:
        _close(runtime)
    runtime.engine.dispose()


def test_verified_replacement_rejects_partition_completed_after_audit_now(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        replacement = _successful_partition_with_snapshot(
            runtime,
            identity=PartitionIdentity(
                "engineering_canary_test",
                "gold_mark",
                SESSIONS[1].isoformat(),
            ),
            tier=SnapshotTier.GOLD,
            role="mark",
            completed_at=NOW + timedelta(seconds=1),
        )

        assert runtime.service._verified_replacement_evidence(replacement) is None
    finally:
        _close(runtime)
    runtime.engine.dispose()


def test_retry_resolution_scans_past_ten_thousand_newer_open_incidents(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        replacement = _successful_partition_with_snapshot(
            runtime,
            identity=PartitionIdentity(
                "engineering_canary_test",
                "gold_mark",
                SESSIONS[1].isoformat(),
            ),
            tier=SnapshotTier.GOLD,
            role="mark",
            completed_at=NOW + timedelta(minutes=2),
        )
        related = runtime.ledger.record_incident(
            partition_key=SESSIONS[1].isoformat(),
            stage=IncidentStage.GOLD,
            error_code="gold_publication_failed",
            message="older retryable failure must remain discoverable",
            occurred_at=NOW,
            partition_run_id=replacement.identity.partition_run_id,
            source_ids=(replacement.identity.source_id,),
            evidence_hashes=tuple(
                replacement.details["claim_lineage"]["parent_evidence_hashes"]
            ),
            payload={
                **_labels(
                    physical_source_attested=(
                        runtime.service.physical_source_attested
                    ),
                    controlled_test=runtime.service.controlled_test,
                ),
                "run_id": replacement.run_id,
                "partition_terminalized": True,
                "partition_terminal_status": PartitionStatus.FAILED.value,
                "terminalization_failure_type": None,
                "claim_lineage": replacement.details["claim_lineage"],
                "claim_lineage_hash": replacement.details["claim_lineage_hash"],
            },
        )
        newer_at = NOW + timedelta(minutes=1)
        decoys = [
            {
                "incident_id": f"incident_bulk_decoy_{index:05d}",
                "incident_hash": content_fingerprint(
                    index, domain="tests/physical-canary-bulk-incident-decoy"
                ),
                "partition_run_id": None,
                "partition_key": SESSIONS[2].isoformat(),
                "stage": IncidentStage.SOURCE.value,
                "status": IncidentStatus.OPEN.value,
                "error_code": "fixture_unrelated_open",
                "message": "newer unrelated open incident",
                "source_ids_json": [],
                "evidence_hashes_json": [],
                "payload_json": {"fixture_index": index},
                "occurred_at": newer_at,
                "resolved_at": None,
                "resolution_hash": None,
            }
            for index in range(10_001)
        ]
        with Session(runtime.engine) as session, session.begin():
            session.execute(insert(DataIncidentModel), decoys)

        truncated = runtime.ledger.list_incidents(
            status=IncidentStatus.OPEN, limit=10_000
        )
        assert related.incident_id not in {item.incident_id for item in truncated}

        runtime.service._now = lambda: NOW + timedelta(minutes=3)
        runtime.service._resolve_retried_partition_incidents(
            replace(replacement, attempts=2)
        )

        resolved = runtime.ledger.list_incidents(status=IncidentStatus.RESOLVED)
        assert [item.incident_id for item in resolved] == [related.incident_id]
        assert resolved[0].payload["resolution"]["disposition"] == (
            "resolved_by_successful_partition_retry"
        )
        assert runtime.ledger.list_incidents(
            status=IncidentStatus.OPEN, limit=1
        )[0].error_code == "fixture_unrelated_open"
    finally:
        _close(runtime)
    runtime.engine.dispose()


@pytest.mark.parametrize(
    ("dataset", "stage", "terminal_status", "error_code", "tier", "role"),
    [
        (
            "silver_accepted",
            IncidentStage.SILVER,
            PartitionStatus.DISPUTED,
            "source_reconciliation_disputed",
            SnapshotTier.SILVER,
            "accepted_reconciled",
        ),
        (
            "silver_accepted",
            IncidentStage.DATA_QUALITY,
            PartitionStatus.QUARANTINED,
            "data_quality_blocked",
            SnapshotTier.SILVER,
            "accepted_reconciled",
        ),
        (
            "dq_accepted",
            IncidentStage.DATA_QUALITY,
            PartitionStatus.FAILED,
            "fixture_generation_failed",
            SnapshotTier.SILVER,
            "accepted_reconciled",
        ),
        (
            "gold_mark",
            IncidentStage.GOLD,
            PartitionStatus.FAILED,
            "fixture_generation_failed",
            SnapshotTier.GOLD,
            "mark",
        ),
        (
            "gold_execution",
            IncidentStage.GOLD,
            PartitionStatus.FAILED,
            "fixture_generation_failed",
            SnapshotTier.GOLD,
            "execution",
        ),
    ],
)
def test_stage_generation_incidents_require_same_dataset_date_replacement(
    tmp_path: Path,
    dataset: str,
    stage: IncidentStage,
    terminal_status: PartitionStatus,
    error_code: str,
    tier: SnapshotTier,
    role: str,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        _enable_generation_audit_fixture(runtime)
        runtime.service._stage_source = lambda: CURRENT_STAGE_SOURCE
        legacy_identity = PartitionIdentity(
            LEGACY_STAGE_SOURCE, dataset, SESSIONS[1].isoformat()
        )
        _legacy, failure = _failed_partition_with_incident(
            runtime,
            identity=legacy_identity,
            stage=stage,
            error_code=error_code,
            terminal_status=terminal_status,
        )
        wrong_date = _successful_partition_with_snapshot(
            runtime,
            identity=PartitionIdentity(
                CURRENT_STAGE_SOURCE, dataset, SESSIONS[2].isoformat()
            ),
            tier=tier,
            role=role,
        )
        runtime.service._audit_legacy_source_generations(replacement=wrong_date)
        assert runtime.ledger.list_incidents(status=IncidentStatus.SUPERSEDED) == ()

        replacement = _successful_partition_with_snapshot(
            runtime,
            identity=PartitionIdentity(
                CURRENT_STAGE_SOURCE, dataset, SESSIONS[1].isoformat()
            ),
            tier=tier,
            role=role,
        )
        current_incident = runtime.ledger.record_incident(
            partition_key=SESSIONS[1].isoformat(),
            stage=stage,
            error_code="current_generation_diagnostic",
            message="current generation incident must not be closed",
            occurred_at=NOW,
            partition_run_id=replacement.identity.partition_run_id,
            source_ids=(replacement.identity.source_id,),
        )
        runtime.service._audit_legacy_source_generations(replacement=replacement)

        superseded = runtime.ledger.list_incidents(
            status=IncidentStatus.SUPERSEDED
        )
        assert {item.error_code for item in superseded} == {
            error_code,
            "legacy_canary_generation_isolated",
        }
        assert failure.incident_id in {
            item.incident_id for item in superseded
        }
        partitions = {
            legacy_identity.partition_run_id: _legacy,
            replacement.identity.partition_run_id: replacement,
        }
        readiness = object.__new__(ProductionReadinessAuditor)
        schema_validity = {
            incident.error_code: readiness._physical_canary_resolution_schema_is_valid(
                incident,
                replacement=replacement,
                partitions=partitions,
            )
            for incident in superseded
        }
        assert all(schema_validity.values()), schema_validity
        remaining = runtime.ledger.list_incidents(status=IncidentStatus.OPEN)
        assert [item.incident_id for item in remaining] == [
            current_incident.incident_id
        ]
    finally:
        _close(runtime)
    runtime.engine.dispose()


def test_exact_base_stage_generation_isolated_by_current_generation(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        _enable_generation_audit_fixture(runtime)
        runtime.service._stage_source = lambda: CURRENT_STAGE_SOURCE
        legacy_identity = PartitionIdentity(
            "engineering_canary",
            "gold_mark",
            SESSIONS[1].isoformat(),
        )
        _legacy, failure = _failed_partition_with_incident(
            runtime,
            identity=legacy_identity,
            stage=IncidentStage.GOLD,
        )
        replacement = _successful_partition_with_snapshot(
            runtime,
            identity=PartitionIdentity(
                CURRENT_STAGE_SOURCE,
                "gold_mark",
                SESSIONS[1].isoformat(),
            ),
            tier=SnapshotTier.GOLD,
            role="mark",
        )

        runtime.service._audit_legacy_source_generations(
            replacement=replacement
        )

        superseded = runtime.ledger.list_incidents(
            status=IncidentStatus.SUPERSEDED
        )
        assert {item.error_code for item in superseded} == {
            "fixture_generation_failed",
            "legacy_canary_generation_isolated",
        }
        assert failure.incident_id in {
            item.incident_id for item in superseded
        }
        assert runtime.ledger.list_incidents(
            status=IncidentStatus.OPEN
        ) == ()
    finally:
        _close(runtime)
    runtime.engine.dispose()


@pytest.mark.parametrize(
    ("projected", "role"), [(False, "mark"), (True, "execution")]
)
def test_prelease_gold_semantics_incident_closes_after_causal_gold_success(
    tmp_path: Path, projected: bool, role: str
) -> None:
    runtime = _runtime(tmp_path)
    try:
        runtime.service.controlled_test = False
        runtime.service._stage_source = lambda: CURRENT_STAGE_SOURCE
        silver_snapshot_id = "fixture-silver-snapshot"
        calendar_hash = "c" * 64
        attempted_gold_input_hash = _gold_attempted_input_hash(
            stage_source=CURRENT_STAGE_SOURCE,
            session=SESSIONS[1],
            role=role,
            silver_snapshot_id=silver_snapshot_id,
            calendar_hash=calendar_hash,
        )
        lineage = {
            "stage_source": CURRENT_STAGE_SOURCE,
            "silver_snapshot_id": silver_snapshot_id,
            "calendar_hash": calendar_hash,
            "gold_role": role,
            "attempted_gold_input_hash": attempted_gold_input_hash,
        }
        replacement = _successful_partition_with_snapshot(
            runtime,
            identity=PartitionIdentity(
                CURRENT_STAGE_SOURCE,
                f"gold_{role}",
                SESSIONS[1].isoformat(),
            ),
            tier=SnapshotTier.GOLD,
            role=role,
            completed_at=NOW + timedelta(seconds=1),
            details={
                "stage_source": lineage["stage_source"],
                "silver_snapshot_id": lineage["silver_snapshot_id"],
                "calendar_hash": lineage["calendar_hash"],
                "role": lineage["gold_role"],
                "attempted_gold_input_hash": lineage[
                    "attempted_gold_input_hash"
                ],
            },
        )
        lineage = {
            "stage_source": replacement.details["stage_source"],
            "silver_snapshot_id": replacement.details["silver_snapshot_id"],
            "calendar_hash": replacement.details["calendar_hash"],
            "gold_role": role,
            "attempted_gold_input_hash": replacement.details[
                "attempted_gold_input_hash"
            ],
        }
        incident = runtime.ledger.record_incident(
            partition_key=SESSIONS[1].isoformat(),
            stage=IncidentStage.GOLD,
            error_code="gold_market_semantics_rejected",
            message="fixture pre-lease market semantic rejection",
            occurred_at=NOW,
            payload={"projected": projected, **lineage},
        )
        assert incident.status is IncidentStatus.OPEN
        attached_current = runtime.ledger.record_incident(
            partition_key=SESSIONS[1].isoformat(),
            stage=IncidentStage.GOLD,
            error_code="gold_market_semantics_rejected",
            message="partition-attached current incident must stay open",
            occurred_at=NOW,
            partition_run_id=replacement.identity.partition_run_id,
            payload={"projected": projected, **lineage},
        )
        runtime.service._now = lambda: NOW + timedelta(seconds=2)
        runtime.service._audit_prelease_gold_semantics(
            session=SESSIONS[1], role=role, replacement=replacement
        )

        closed = runtime.ledger.list_incidents(status=IncidentStatus.SUPERSEDED)
        assert [item.incident_id for item in closed] == [incident.incident_id]
        resolution = closed[0].payload["resolution"]
        assert (
            resolution["replacement_partition_run_id"]
            == replacement.identity.partition_run_id
        )
        assert (
            resolution["replacement_output_snapshot_id"]
            == replacement.output_snapshot_id
        )
        remaining = runtime.ledger.list_incidents(status=IncidentStatus.OPEN)
        assert [item.incident_id for item in remaining] == [attached_current.incident_id]
    finally:
        _close(runtime)
    runtime.engine.dispose()


def test_prelease_gold_semantics_does_not_retroactively_use_older_gold(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        runtime.service.controlled_test = False
        runtime.service._stage_source = lambda: CURRENT_STAGE_SOURCE
        role = "execution"
        silver_snapshot_id = "fixture-silver-snapshot"
        calendar_hash = "c" * 64
        attempted_gold_input_hash = _gold_attempted_input_hash(
            stage_source=CURRENT_STAGE_SOURCE,
            session=SESSIONS[1],
            role=role,
            silver_snapshot_id=silver_snapshot_id,
            calendar_hash=calendar_hash,
        )
        replacement = _successful_partition_with_snapshot(
            runtime,
            identity=PartitionIdentity(
                CURRENT_STAGE_SOURCE,
                "gold_execution",
                SESSIONS[1].isoformat(),
            ),
            tier=SnapshotTier.GOLD,
            role=role,
            completed_at=NOW - timedelta(seconds=1),
            details={
                "stage_source": CURRENT_STAGE_SOURCE,
                "silver_snapshot_id": silver_snapshot_id,
                "calendar_hash": calendar_hash,
                "role": role,
                "attempted_gold_input_hash": attempted_gold_input_hash,
            },
        )
        incident = runtime.ledger.record_incident(
            partition_key=SESSIONS[1].isoformat(),
            stage=IncidentStage.GOLD,
            error_code="gold_market_semantics_rejected",
            message="detected after an older same-day Gold partition",
            occurred_at=NOW,
            payload={
                "projected": True,
                "stage_source": CURRENT_STAGE_SOURCE,
                "silver_snapshot_id": silver_snapshot_id,
                "calendar_hash": calendar_hash,
                "gold_role": role,
                "attempted_gold_input_hash": attempted_gold_input_hash,
            },
        )

        runtime.service._audit_prelease_gold_semantics(
            session=SESSIONS[1], role=role, replacement=replacement
        )

        assert runtime.ledger.list_incidents(status=IncidentStatus.SUPERSEDED) == ()
        assert [
            item.incident_id
            for item in runtime.ledger.list_incidents(status=IncidentStatus.OPEN)
        ] == [incident.incident_id]
    finally:
        _close(runtime)
    runtime.engine.dispose()


def test_run_records_prelease_gold_detection_time_and_full_lineage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = _runtime(tmp_path)
    detected_at = NOW + timedelta(minutes=5)
    runtime.service._now = lambda: detected_at

    def reject_market_semantics(**_kwargs):
        raise PhysicalCanaryDataRejected("fixture Gold semantic rejection")

    monkeypatch.setattr(runtime.service, "_build_gold_bars", reject_market_semantics)
    try:
        with pytest.raises(PhysicalCanaryDataRejected, match="semantic rejection"):
            runtime.service.run()

        incidents = [
            item
            for item in runtime.ledger.list_incidents(status=IncidentStatus.OPEN)
            if item.error_code == "gold_market_semantics_rejected"
        ]
        assert len(incidents) == 1
        incident = incidents[0]
        assert incident.occurred_at == detected_at
        assert incident.payload["stage_source"] == "engineering_canary_test"
        assert incident.payload["gold_role"] == "mark"
        assert incident.payload["projected"] is False
        assert len(incident.payload["calendar_hash"]) == 64
        assert incident.payload["silver_snapshot_id"]
        assert incident.payload["attempted_gold_input_hash"] == (
            _gold_attempted_input_hash(
                stage_source=incident.payload["stage_source"],
                session=SESSIONS[0],
                role=incident.payload["gold_role"],
                silver_snapshot_id=incident.payload["silver_snapshot_id"],
                calendar_hash=incident.payload["calendar_hash"],
            )
        )
    finally:
        _close(runtime)
    runtime.engine.dispose()


def test_canary_plan_and_stage_are_bound_to_measured_build_provenance(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)

    def provenance(marker: str) -> SimpleNamespace:
        return SimpleNamespace(
            public_dict=lambda: {
                "architecture_version": "research-os/v1",
                "code_hash": marker * 64,
                "configuration_hash": "2" * 64,
                "dependency_lock_hash": "3" * 64,
                "dirty_patch_hash": "4" * 64,
                "provenance_kind": "daemon_inspected_oci_image",
                "build_identity_hash": marker * 64,
                "git_commit": None,
                "image_source_digest": "5" * 64,
                "oci_image_id": f"sha256:{marker * 64}",
                "oci_repo_digests": [],
                "oci_base_digests": [f"sha256:{'6' * 64}"],
                "formal_epoch_eligible": True,
            }
        )

    try:
        sessions, calendar_records = runtime.service._accepted_sessions(
            as_of=SESSIONS[-1]
        )
        runtime.service.controlled_test = False
        runtime.service.production_evidence = SimpleNamespace(provenance=provenance("a"))
        first_plan = runtime.service._plan_fingerprint(
            sessions=sessions,
            calendar_records=calendar_records,
        )
        first_stage = runtime.service._stage_source()
        first_source = runtime.service._partition_source(runtime.service.bindings[0])

        runtime.service.production_evidence = SimpleNamespace(provenance=provenance("b"))
        del runtime.service._cached_evaluator_identity
        second_plan = runtime.service._plan_fingerprint(
            sessions=sessions,
            calendar_records=calendar_records,
        )
        second_stage = runtime.service._stage_source()
        second_source = runtime.service._partition_source(runtime.service.bindings[0])

        assert first_plan != second_plan
        assert first_stage.startswith("engineering_canary_")
        assert second_stage.startswith("engineering_canary_")
        assert first_stage != second_stage
        assert first_source != second_source
    finally:
        _close(runtime)
    runtime.engine.dispose()


def test_refreshed_host_proof_keeps_stable_plan_but_changed_build_does_not(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from factor_lab.research_os.readiness_audit import ProductionReadinessAuditor

    runtime = _runtime(tmp_path)
    original_dialect_name = runtime.ledger.engine.dialect.name
    measured = {
        "architecture_version": "research-os/v1",
        "code_hash": "1" * 64,
        "configuration_hash": "2" * 64,
        "dependency_lock_hash": "3" * 64,
        "dirty_patch_hash": "4" * 64,
        "provenance_kind": "daemon_inspected_oci_image",
        "build_identity_hash": "5" * 64,
        "git_commit": None,
        "image_source_digest": "6" * 64,
        "oci_image_id": "sha256:" + "7" * 64,
        "oci_repo_digests": ["factor-lab@sha256:" + "8" * 64],
        "oci_base_digests": ["sha256:" + "9" * 64],
        "formal_epoch_eligible": True,
    }
    current = {
        "controlled_test_backend": False,
        "host_attestation_run_id": "docker_attestation_" + "a" * 64,
        "host_attestation_hash": "a" * 64,
        "attested_at": "2026-08-23T10:00:00+00:00",
        "container_started_at": "2026-08-23T09:00:00+00:00",
        "container_id": "b" * 64,
        "deployment_identity_hash": "c" * 64,
        "compose_config_hash": "d" * 64,
        "build_identity_hash": "5" * 64,
        "runtime_contract_hash": "6" * 64,
        "oci_image_id": "sha256:" + "7" * 64,
        "oci_repo_digests": ["factor-lab@sha256:" + "8" * 64],
        "oci_base_digests": ["sha256:" + "9" * 64],
        "epoch_fields": {
            "architecture_version": "research-os/v1",
            "code_hash": "1" * 64,
            "configuration_hash": "2" * 64,
            "dependency_lock_hash": "3" * 64,
            "dirty_patch_hash": "4" * 64,
        },
    }
    process_identity = {"value": "1" * 64}

    monkeypatch.setattr(
        "factor_lab.research_os.physical_canary.load_production_config",
        lambda _path: {},
    )
    monkeypatch.setattr(
        ProductionReadinessAuditor,
        "verified_oci_deployment_evidence",
        lambda _self: dict(current),
    )
    try:
        sessions, calendar_records = runtime.service._accepted_sessions(
            as_of=SESSIONS[-1]
        )
        runtime.ledger.engine.dialect.name = "postgresql"
        runtime.service.controlled_test = False
        runtime.service.production_evidence = SimpleNamespace(
            path=tmp_path / "production.json",
            provenance=SimpleNamespace(public_dict=lambda: dict(measured)),
        )
        monkeypatch.setattr(
            runtime.service,
            "_workload_container_identity",
            lambda: str(current["container_id"])[:12],
        )
        monkeypatch.setattr(
            runtime.service,
            "_workload_process_identity",
            lambda: process_identity["value"],
        )
        monkeypatch.setattr(
            runtime.service,
            "_assert_workload_root_matches_init",
            lambda: None,
        )
        first_plan = runtime.service._plan_fingerprint(
            sessions=sessions,
            calendar_records=calendar_records,
        )
        first_stage = runtime.service._stage_source()
        first_source = runtime.service._partition_source(runtime.service.bindings[0])
        first_proof = runtime.service._runtime_attestation_evidence()

        current.update(
            host_attestation_run_id="docker_attestation_" + "e" * 64,
            host_attestation_hash="e" * 64,
            attested_at="2026-08-23T10:05:00+00:00",
        )
        runtime.service.__dict__.pop("_cached_evaluator_identity", None)
        runtime.service.__dict__.pop("_runtime_attestation_proof", None)
        second_plan = runtime.service._plan_fingerprint(
            sessions=sessions,
            calendar_records=calendar_records,
        )
        second_stage = runtime.service._stage_source()
        second_source = runtime.service._partition_source(runtime.service.bindings[0])
        second_proof = runtime.service._runtime_attestation_evidence()

        assert first_plan == second_plan
        assert first_stage == second_stage
        assert first_source == second_source
        assert first_proof["host_attestation_hash"] != second_proof[
            "host_attestation_hash"
        ]
        assert first_proof["executing_process_identity"] == "1" * 64
        assert second_proof["executing_process_identity"] == "1" * 64
        assert "executing_container_started_at" not in first_proof
        assert "executing_container_started_at" not in second_proof

        # Docker can restart PID 1 while retaining the container ID.  A WSL
        # wall-clock/btime correction is no longer an identity input, while a
        # genuinely different boot-id/start-tick hash is preserved in the
        # runtime proof and starts a distinct continuity segment.
        current.update(
            host_attestation_run_id="docker_attestation_" + "f" * 64,
            host_attestation_hash="f" * 64,
            attested_at="2026-08-23T10:10:00+00:00",
            container_started_at="2026-08-23T10:09:00+00:00",
            deployment_identity_hash="0" * 64,
        )
        process_identity["value"] = "2" * 64
        runtime.service.__dict__.pop("_cached_evaluator_identity", None)
        runtime.service.__dict__.pop("_runtime_attestation_proof", None)
        restarted_plan = runtime.service._plan_fingerprint(
            sessions=sessions,
            calendar_records=calendar_records,
        )
        restarted_stage = runtime.service._stage_source()
        restarted_source = runtime.service._partition_source(
            runtime.service.bindings[0]
        )
        restarted_proof = runtime.service._runtime_attestation_evidence()

        assert restarted_plan == second_plan
        assert restarted_stage == second_stage
        assert restarted_source == second_source
        assert restarted_proof["executing_process_identity"] == "2" * 64
        assert restarted_proof["executing_process_identity"] != second_proof[
            "executing_process_identity"
        ]
        assert "executing_container_started_at" not in restarted_proof

        current.update(
            oci_image_id="sha256:" + "3" * 64,
            oci_base_digests=["sha256:" + "4" * 64],
        )
        runtime.service.__dict__.pop("_cached_evaluator_identity", None)
        runtime.service.__dict__.pop("_runtime_attestation_proof", None)
        third_plan = runtime.service._plan_fingerprint(
            sessions=sessions,
            calendar_records=calendar_records,
        )
        third_stage = runtime.service._stage_source()
        third_source = runtime.service._partition_source(runtime.service.bindings[0])

        assert third_plan != restarted_plan
        assert third_stage != restarted_stage
        assert third_source != restarted_source
    finally:
        runtime.ledger.engine.dialect.name = original_dialect_name
        _close(runtime)
    runtime.engine.dispose()


def test_workload_root_must_match_container_init_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current = SimpleNamespace(st_dev=1, st_ino=2)
    init = SimpleNamespace(st_dev=1, st_ino=3)
    monkeypatch.setattr(
        physical_canary_module.os,
        "stat",
        lambda path: current if path == "/" else init,
    )

    with pytest.raises(PhysicalCanaryAdmissionError, match="init root"):
        PhysicalEngineeringCanaryService._assert_workload_root_matches_init()


def test_stage_generation_does_not_alias_same_twelve_hex_prefix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    marker = {"value": "first"}
    original = physical_canary_module.content_fingerprint

    def fingerprint(value, *, domain):
        if domain == "factor-lab/research-os/v1/physical-canary-stage-generation":
            suffix = "b" * 52 if marker["value"] == "first" else "c" * 52
            return "a" * 12 + suffix
        return original(value, domain=domain)

    try:
        runtime.service.controlled_test = False
        monkeypatch.setattr(
            runtime.service,
            "_evaluator_identity",
            lambda: {"marker": marker["value"]},
        )
        monkeypatch.setattr(
            physical_canary_module,
            "content_fingerprint",
            fingerprint,
        )
        first = runtime.service._stage_source()
        marker["value"] = "second"
        second = runtime.service._stage_source()

        assert first != second
        assert first.startswith("engineering_canary_" + "a" * 12)
        assert len(first.removeprefix("engineering_canary_")) == 24
    finally:
        _close(runtime)
    runtime.engine.dispose()


def test_completed_canary_rejects_tampered_snapshot_partition_authority(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        result = runtime.service.run()
        target_snapshot = result.object_evidence[0].snapshot_id
        with Session(runtime.engine) as session:
            target = session.query(PartitionRunModel).filter(
                PartitionRunModel.output_snapshot_id == target_snapshot
            ).one()
            target.status = PartitionStatus.FAILED.value
            session.commit()

        with pytest.raises(PhysicalCanaryDataRejected, match="partition binding"):
            runtime.service.run()
    finally:
        _close(runtime)
    runtime.engine.dispose()


def test_per_ticker_fetch_heartbeats_before_and_after_every_request(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    count = 0

    def heartbeat() -> None:
        nonlocal count
        count += 1

    try:
        binding = next(
            item
            for item in runtime.service.bindings
            if item.dataset == "opening_execution"
        )
        runtime.service._fetch_binding(
            binding,
            session=SESSIONS[1],
            tickers=TICKERS[:50],
            lease_heartbeat=heartbeat,
        )
        assert count == 100
    finally:
        _close(runtime)
    runtime.engine.dispose()


def test_failed_partition_writes_open_incident_when_terminalization_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    identity = PartitionIdentity("lease_orphan_fixture", "daily", "2026-08-21")
    runtime.ledger.ensure_partition(
        identity,
        created_at=NOW,
        input_hash="a" * 64,
    )
    lease = runtime.ledger.claim(
        identity=identity,
        owner="fixture-worker",
        now=NOW,
        lease_for=timedelta(minutes=30),
    )
    assert lease is not None

    def fail_finish(*_args, **_kwargs):
        raise RuntimeError("simulated lease loss")

    monkeypatch.setattr(runtime.ledger, "finish", fail_finish)
    try:
        runtime.service._finish_failed_partition(
            lease,
            run_id="fixture-run",
            status=PartitionStatus.FAILED,
            stage=IncidentStage.SOURCE,
            error_code="source_fetch_failed",
            exc=RuntimeError("fixture fetch failure"),
        )
        incidents = runtime.ledger.list_incidents(status=IncidentStatus.OPEN)
        assert len(incidents) == 1
        assert incidents[0].partition_run_id == identity.partition_run_id
        assert incidents[0].payload["partition_terminalized"] is False
        assert incidents[0].payload["terminalization_failure_type"] == "RuntimeError"
    finally:
        _close(runtime)
    runtime.engine.dispose()


def test_gold_claim_input_hash_collision_records_one_full_lineage_incident_and_fails_parent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    original_ensure = runtime.ledger.ensure_partition
    injected = False

    def collide_once(identity, *, created_at, input_hash=None, details=None):
        nonlocal injected
        if identity.dataset == "gold_mark" and not injected:
            injected = True
            original_ensure(
                identity,
                created_at=created_at,
                input_hash="f" * 64,
                details={"fixture": "conflicting immutable Gold lineage"},
            )
        return original_ensure(
            identity,
            created_at=created_at,
            input_hash=input_hash,
            details=details,
        )

    monkeypatch.setattr(runtime.ledger, "ensure_partition", collide_once)
    try:
        with pytest.raises(Exception, match="input hash changed"):
            runtime.service.run()

        parent = runtime.catalog.list_runs(run_type=CONTROLLED_TEST_RUN_TYPE)[0]
        assert parent.status == "failed"
        incidents = [
            item
            for item in runtime.ledger.list_incidents(status=IncidentStatus.OPEN)
            if item.error_code == "gold_partition_claim_failed"
        ]
        assert len(incidents) == 1
        incident = incidents[0]
        identity = incident.payload["partition_identity"]
        assert incident.partition_run_id == identity["partition_run_id"]
        assert identity == {
            "source_id": "engineering_canary_test",
            "dataset": "gold_mark",
            "partition_key": SESSIONS[0].isoformat(),
            "partition_run_id": incident.partition_run_id,
        }
        assert len(incident.payload["attempted_input_hash"]) == 64
        assert incident.payload["claim_attempt_status"] == "failed"
        assert incident.payload["partition_terminalized"] is False
        stage_lineage = incident.payload["stage_lineage"]
        assert stage_lineage["stage_source"] == "engineering_canary_test"
        assert stage_lineage["gold_role"] == "mark"
        assert stage_lineage["silver_snapshot_id"]
        assert len(stage_lineage["calendar_hash"]) == 64
        assert len(stage_lineage["attempted_gold_input_hash"]) == 64
        assert incident.payload["claim_lineage"]["stage_lineage"] == stage_lineage
        assert len(incident.payload["claim_lineage"]["parent_evidence_hashes"]) == 1

        with pytest.raises(PhysicalCanaryDataRejected, match="already terminal.*failed"):
            runtime.service.run()
        repeated = [
            item
            for item in runtime.ledger.list_incidents(status=IncidentStatus.OPEN)
            if item.error_code == "gold_partition_claim_failed"
        ]
        assert [item.incident_id for item in repeated] == [incident.incident_id]
    finally:
        _close(runtime)
    runtime.engine.dispose()


def test_gold_claim_db_error_keeps_exact_lineage_open_after_terminal_parent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    original_claim = runtime.ledger.claim
    failed = False

    def fail_gold_claim_once(*, identity, owner, now, lease_for, maximum_attempts=None):
        nonlocal failed
        if identity.dataset == "gold_mark" and not failed:
            failed = True
            raise RuntimeError("fixture Gold claim database error")
        kwargs = {
            "identity": identity,
            "owner": owner,
            "now": now,
            "lease_for": lease_for,
        }
        if maximum_attempts is not None:
            kwargs["maximum_attempts"] = maximum_attempts
        return original_claim(**kwargs)

    monkeypatch.setattr(runtime.ledger, "claim", fail_gold_claim_once)
    try:
        with pytest.raises(RuntimeError, match="claim database error"):
            runtime.service.run()

        failed_parent = runtime.catalog.list_runs(
            run_type=CONTROLLED_TEST_RUN_TYPE
        )[0]
        assert failed_parent.status == "failed"
        exact = [
            item
            for item in runtime.ledger.list_incidents(status=IncidentStatus.OPEN)
            if item.error_code == "gold_partition_claim_failed"
        ]
        assert len(exact) == 1
        exact_incident = exact[0]
        decoy_lineage = {
            **exact_incident.payload["claim_lineage"],
            "stage_lineage": {
                **exact_incident.payload["claim_lineage"]["stage_lineage"],
                "attempted_gold_input_hash": "d" * 64,
            },
        }
        decoy_lineage_hash = content_fingerprint(
            decoy_lineage,
            domain=(
                "factor-lab/research-os/v1/"
                "physical-canary-partition-claim-lineage"
            ),
        )
        decoy = runtime.ledger.record_incident(
            partition_key=exact_incident.partition_key,
            stage=IncidentStage.GOLD,
            error_code="gold_partition_claim_failed",
            message="different claim lineage must stay open",
            occurred_at=NOW,
            partition_run_id=exact_incident.partition_run_id,
            source_ids=exact_incident.source_ids,
            evidence_hashes=exact_incident.evidence_hashes,
            payload={
                **exact_incident.payload,
                "claim_lineage": decoy_lineage,
                "claim_lineage_hash": decoy_lineage_hash,
            },
        )

        calls_before_retry = dict(runtime.state.calls)
        with pytest.raises(PhysicalCanaryDataRejected, match="already terminal.*failed"):
            runtime.service.run()

        retained_parent = runtime.catalog.get_run(failed_parent.run_id)
        assert retained_parent == failed_parent
        assert runtime.state.calls == calls_before_retry
        assert runtime.ledger.list_incidents(status=IncidentStatus.RESOLVED) == ()
        remaining = runtime.ledger.list_incidents(status=IncidentStatus.OPEN)
        assert {item.incident_id for item in remaining} == {
            exact_incident.incident_id,
            decoy.incident_id,
        }
    finally:
        _close(runtime)
    runtime.engine.dispose()


def test_completed_canary_rejects_mismatched_evaluator_identity(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    try:
        sessions, calendar_records = runtime.service._accepted_sessions(
            as_of=SESSIONS[-1]
        )
        plan = runtime.service._plan_fingerprint(
            sessions=sessions,
            calendar_records=calendar_records,
        )
        run_id = f"physical_canary_{plan[:48]}"
        sleeve_id = f"physical_canary_sleeve_{plan[:32]}"
        account_id = f"physical_canary_account_{plan[:32]}"
        labels = _labels(physical_source_attested=False, controlled_test=True)
        metadata = {
            **labels,
            "evaluator_identity": {"tampered": True},
            "run_id": run_id,
            "run_type": CONTROLLED_TEST_RUN_TYPE,
            "input_fingerprint": plan,
            "calendar_sessions": [item.isoformat() for item in sessions],
            "accepted_calendar_partition_ids": [
                item.identity.partition_run_id for item in calendar_records
            ],
            "accepted_calendar_output_hashes": [
                item.output_hash for item in calendar_records
            ],
            "security_count": 50,
            "projected_session_count": 20,
            "account_id": account_id,
            "sleeve_id": sleeve_id,
            "sleeve_state": "shadow",
        }
        with pytest.raises(PhysicalCanaryDataRejected, match="identity"):
            runtime.service._authoritative_result(
                metadata=metadata,
                run_id=run_id,
                run_type=CONTROLLED_TEST_RUN_TYPE,
                plan_fingerprint=plan,
                sessions=sessions,
                calendar_records=calendar_records,
                sleeve_id=sleeve_id,
                account_id=account_id,
                labels=labels,
            )
    finally:
        _close(runtime)
    runtime.engine.dispose()


def _gold_inputs_with_minute_open(
    state: _ProviderState,
    *,
    session: date,
    tickers: tuple[str, ...],
    physical_open: float,
) -> tuple[tuple[object, object, object], ...]:
    def frame(dataset: str) -> pd.DataFrame:
        if dataset == "opening_execution":
            values = pd.concat(
                [
                    state.frame(
                        "fixture_opening",
                        FetchRequest(
                            dataset,
                            parameters={
                                "stock_code": ticker,
                                "start_time": f"{session.isoformat()} 09:30:00",
                            },
                        ),
                    )
                    for ticker in tickers
                ],
                ignore_index=True,
            )
            values.loc[values["stock_code"] == tickers[0], "open"] = physical_open
            values.loc[values["stock_code"] == tickers[0], "low"] = physical_open - 0.01
            values.loc[values["stock_code"] == tickers[0], "high"] = physical_open + 0.01
            return values
        return state.frame(
            "fixture_calendar_1",
            FetchRequest(dataset, parameters={"trade_date": session.isoformat()}),
        )

    return tuple(
        (
            object(),
            SimpleNamespace(dataset=dataset, frame=frame(dataset)),
            object(),
        )
        for dataset in (
            "daily",
            "adj_factor",
            "historical_st",
            "stock_limit",
            "opening_execution",
        )
    )


def test_minute_open_is_audited_as_distinct_from_daily_auction_open(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    session = SESSIONS[1]
    tickers = TICKERS[:50]
    daily_open = runtime.state._price(tickers[0], session)
    try:
        bars = runtime.service._build_gold_bars(
            session=session,
            inputs=_gold_inputs_with_minute_open(
                runtime.state,
                session=session,
                tickers=tickers,
                physical_open=daily_open + 0.02,
            ),
            tickers=tickers,
            projected=True,
            amount_history={},
            close_history={},
        )

        observed = bars.set_index("ticker").loc[tickers[0]]
        assert observed["execution_minute_open_raw"] == pytest.approx(
            daily_open + 0.02
        )
        assert observed["execution_vs_daily_open_abs_diff"] == pytest.approx(0.02)
        assert bool(observed["execution_vs_daily_open_one_tick_match"]) is False
    finally:
        _close(runtime)
    runtime.engine.dispose()


def test_minute_open_outside_daily_range_is_rejected(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    session = SESSIONS[1]
    tickers = TICKERS[:50]
    daily_open = runtime.state._price(tickers[0], session)
    try:
        with pytest.raises(PhysicalCanaryDataRejected, match="daily range"):
            runtime.service._build_gold_bars(
                session=session,
                inputs=_gold_inputs_with_minute_open(
                    runtime.state,
                    session=session,
                    tickers=tickers,
                    physical_open=daily_open * 1.2,
                ),
                tickers=tickers,
                projected=True,
                amount_history={},
                close_history={},
            )
    finally:
        _close(runtime)
    runtime.engine.dispose()


def test_controlled_physical_path_closes_objects_partitions_shadow_and_rejects_readiness(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path, active_epoch=True)
    try:
        result = runtime.service.run()

        assert result.run_type == CONTROLLED_TEST_RUN_TYPE
        assert result.physical_source_attested is False
        assert result.controlled_test_adapter is True
        assert result.readiness_admission == "rejected_controlled_test_adapter"
        assert result.evidence_scope == "retrospective_non_forward"
        assert result.formal_epoch_eligible is False
        assert result.security_count == 50
        assert result.projected_session_count == 20
        assert result.sleeve_state == "shadow"
        assert len(result.shadow_sessions) == 20
        assert all(
            item.evidence_class is ShadowEvidenceClass.ENGINEERING
            and item.epoch_id is None
            and item.evidence_window_hash is None
            for item in result.shadow_sessions
        )
        assert {item.tier for item in result.object_evidence} == {
            "bronze",
            "silver",
            "gold",
        }
        assert all(
            item.uri.startswith(f"s3://factor-lab/{CANARY_OBJECT_PREFIX}/")
            for item in result.object_evidence
        )
        assert all(
            runtime.filesystem.exists(item.uri.removeprefix("s3://"))
            for item in result.object_evidence
        )
        assert runtime.ledger.progress(dataset="dq_accepted").counts == {
            "succeeded": 21
        }
        assert runtime.catalog.list_runs(run_type=PHYSICAL_RUN_TYPE) == []
        persisted = runtime.catalog.get_run(result.run_id)
        assert persisted is not None
        assert persisted.metadata["canary_evidence_hash"] == result.canary_evidence_hash
        assert persisted.metadata["opening_execution_formal_ready"] is False
        assert persisted.metadata["canary_execution_contract_hash"] == (
            runtime.service.canary_execution_contract_hash
        )
        assert all(
            runtime.catalog.get_snapshot(item.snapshot_id).reference.manifest[
                "canary_execution_contract_hash"
            ]
            == runtime.service.canary_execution_contract_hash
            for item in result.object_evidence
        )
        assert physical_canary_evidence_hash(persisted.metadata) == result.canary_evidence_hash
        assert persisted.metadata["physical_object_count"] == len(result.object_evidence)
        with pytest.raises(PhysicalCanaryFormalEpochDenied):
            deny_physical_canary_formal_epoch(result)
    finally:
        _close(runtime)


def test_parent_terminal_fault_is_immutable_and_retry_is_read_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    original_save_run = runtime.catalog.save_run
    fault = {"pending": True}

    def save_run_with_terminal_fault(record):
        if fault["pending"] and record.status == "succeeded":
            fault["pending"] = False
            raise RuntimeError("injected parent success write fault")
        return original_save_run(record)

    monkeypatch.setattr(runtime.catalog, "save_run", save_run_with_terminal_fault)
    try:
        with pytest.raises(RuntimeError, match="parent success write fault"):
            runtime.service.run()

        failed_parent = runtime.catalog.list_runs(
            run_type=CONTROLLED_TEST_RUN_TYPE
        )[0]
        assert failed_parent.status == "failed"
        execution_identity = PartitionIdentity(
            "engineering_canary_test",
            "physical_canary_execution",
            failed_parent.input_fingerprint[:32],
        )
        sealed_execution = runtime.ledger.get_partition(execution_identity)
        assert sealed_execution is not None
        assert sealed_execution.status is PartitionStatus.SUCCEEDED
        assert sealed_execution.attempts == 1

        calls_after_children = dict(runtime.state.calls)
        objects_after_children = dict(runtime.filesystem.objects)
        partition_attempts = {
            item.identity.partition_run_id: item.attempts
            for item in runtime.ledger.list_partitions(limit=100_000)
        }
        monkeypatch.setattr(runtime.catalog, "save_run", original_save_run)

        with pytest.raises(PhysicalCanaryDataRejected, match="already terminal.*failed"):
            runtime.service.run()

        retained_parent = runtime.catalog.get_run(failed_parent.run_id)
        assert retained_parent == failed_parent
        assert runtime.state.calls == calls_after_children
        assert runtime.filesystem.objects == objects_after_children
        assert {
            item.identity.partition_run_id: item.attempts
            for item in runtime.ledger.list_partitions(limit=100_000)
        } == partition_attempts
    finally:
        _close(runtime)


def test_same_fingerprint_live_claim_is_busy_before_any_child_side_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    initial_objects = dict(runtime.filesystem.objects)
    observations = {"probe_entries": 0}

    def overlap_at_first_child(_sessions):
        observations["probe_entries"] += 1
        running_parent = runtime.catalog.list_runs(
            run_type=CONTROLLED_TEST_RUN_TYPE
        )[0]
        assert running_parent.status == "running"
        execution = runtime.ledger.get_partition(
            PartitionIdentity(
                "engineering_canary_test",
                "physical_canary_execution",
                running_parent.input_fingerprint[:32],
            )
        )
        assert execution is not None
        assert execution.status is PartitionStatus.RUNNING
        with pytest.raises(PhysicalCanaryBusy, match="already running"):
            runtime.service.run()
        assert runtime.state.calls == {}
        raise RuntimeError("stop after concurrent claim assertion")

    monkeypatch.setattr(runtime.service, "_probe_sources", overlap_at_first_child)
    try:
        with pytest.raises(RuntimeError, match="concurrent claim assertion"):
            runtime.service.run()

        assert observations == {"probe_entries": 1}
        assert runtime.state.calls == {}
        assert runtime.filesystem.objects == initial_objects
        assert runtime.catalog.list_shadow_accounts() == []
        parent = runtime.catalog.list_runs(run_type=CONTROLLED_TEST_RUN_TYPE)[0]
        assert parent.status == "failed"
        execution = runtime.ledger.get_partition(
            PartitionIdentity(
                "engineering_canary_test",
                "physical_canary_execution",
                parent.input_fingerprint[:32],
            )
        )
        assert execution is not None
        assert execution.status is PartitionStatus.FAILED
        assert execution.attempts == 1
    finally:
        _close(runtime)


def test_expired_incomplete_execution_is_reclaimed_without_resetting_parent_start(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)

    def hard_crash_before_children(_sessions):
        raise SystemExit("injected process loss")

    monkeypatch.setattr(runtime.service, "_probe_sources", hard_crash_before_children)
    try:
        with pytest.raises(SystemExit, match="process loss"):
            runtime.service.run()

        incomplete_parent = runtime.catalog.list_runs(
            run_type=CONTROLLED_TEST_RUN_TYPE
        )[0]
        assert incomplete_parent.status == "running"
        execution_identity = PartitionIdentity(
            "engineering_canary_test",
            "physical_canary_execution",
            incomplete_parent.input_fingerprint[:32],
        )
        incomplete_execution = runtime.ledger.get_partition(execution_identity)
        assert incomplete_execution is not None
        assert incomplete_execution.status is PartitionStatus.RUNNING
        assert incomplete_execution.attempts == 1

        monkeypatch.setattr(
            runtime.service,
            "_now",
            lambda: NOW + pd.Timedelta(hours=25).to_pytimedelta(),
        )

        def stop_after_reclaim(_sessions):
            reclaimed_parent = runtime.catalog.get_run(incomplete_parent.run_id)
            reclaimed_execution = runtime.ledger.get_partition(execution_identity)
            assert reclaimed_parent is not None
            assert reclaimed_parent.status == "running"
            assert reclaimed_parent.started_at == incomplete_parent.started_at
            assert reclaimed_execution is not None
            assert reclaimed_execution.status is PartitionStatus.RUNNING
            assert reclaimed_execution.attempts == 2
            raise RuntimeError("stop after expired lease reclaim")

        monkeypatch.setattr(runtime.service, "_probe_sources", stop_after_reclaim)
        with pytest.raises(RuntimeError, match="expired lease reclaim"):
            runtime.service.run()

        failed_parent = runtime.catalog.get_run(incomplete_parent.run_id)
        failed_execution = runtime.ledger.get_partition(execution_identity)
        assert failed_parent is not None
        assert failed_parent.status == "failed"
        assert failed_parent.started_at == incomplete_parent.started_at
        assert failed_execution is not None
        assert failed_execution.status is PartitionStatus.FAILED
        assert failed_execution.attempts == 2
        assert runtime.state.calls == {}
        assert runtime.catalog.list_shadow_accounts() == []
    finally:
        _close(runtime)


def test_causal_child_error_survives_coordinator_and_parent_terminal_write_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    original_finish = runtime.ledger.finish
    original_save_run = runtime.catalog.save_run

    def causal_child_failure(_sessions):
        raise RuntimeError("causal child failure")

    def fail_coordinator_terminal(lease, **kwargs):
        if (
            lease.identity.dataset == "physical_canary_execution"
            and kwargs.get("status") is PartitionStatus.FAILED
        ):
            raise RuntimeError("coordinator terminal write failed")
        return original_finish(lease, **kwargs)

    def fail_parent_terminal(record):
        if record.status == "failed":
            raise RuntimeError("parent terminal write failed")
        return original_save_run(record)

    monkeypatch.setattr(runtime.service, "_probe_sources", causal_child_failure)
    monkeypatch.setattr(runtime.ledger, "finish", fail_coordinator_terminal)
    monkeypatch.setattr(runtime.catalog, "save_run", fail_parent_terminal)
    try:
        with pytest.raises(RuntimeError, match="causal child failure"):
            runtime.service.run()

        parent = runtime.catalog.list_runs(run_type=CONTROLLED_TEST_RUN_TYPE)[0]
        execution = runtime.ledger.get_partition(
            PartitionIdentity(
                "engineering_canary_test",
                "physical_canary_execution",
                parent.input_fingerprint[:32],
            )
        )
        assert parent.status == "running"
        assert execution is not None
        assert execution.status is PartitionStatus.RUNNING
        assert runtime.state.calls == {}
    finally:
        _close(runtime)


def test_physical_canary_failed_parent_requires_new_attempt_run_id(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    failed_session = SESSIONS[11]
    runtime.state.fail_opening_once = failed_session
    try:
        with pytest.raises(RuntimeError, match="controlled transient"):
            runtime.service.run()
        failed_parent = runtime.catalog.list_runs(
            run_type=CONTROLLED_TEST_RUN_TYPE
        )[0]
        assert failed_parent.status == "failed"
        execution_identity = PartitionIdentity(
            "engineering_canary_test",
            "physical_canary_execution",
            failed_parent.input_fingerprint[:32],
        )
        failed_execution = runtime.ledger.get_partition(execution_identity)
        assert failed_execution is not None
        assert failed_execution.status is PartitionStatus.FAILED
        assert failed_execution.attempts == 1
        opening_identity = PartitionIdentity(
            "engtest_fixture_opening",
            "opening_execution",
            failed_session.isoformat(),
        )
        failed_opening_incidents = [
            item
            for item in runtime.ledger.list_incidents(status=IncidentStatus.OPEN)
            if item.partition_run_id == opening_identity.partition_run_id
        ]
        assert len(failed_opening_incidents) == 1
        assert (
            failed_opening_incidents[0].payload["partition_terminal_status"]
            == PartitionStatus.FAILED.value
        )
        accounts = runtime.catalog.list_shadow_accounts()
        assert len(accounts) == 1
        assert runtime.catalog.count_shadow_sessions(
            account_id=accounts[0].account_id,
            since=SESSIONS[0],
            through=SESSIONS[-1],
        ) == 10
        early_opening_calls = {
            key: value
            for key, value in runtime.state.calls.items()
            if key[0] == "opening_execution" and key[1] < failed_session
        }

        calls_before_retry = dict(runtime.state.calls)
        with pytest.raises(PhysicalCanaryDataRejected, match="already terminal.*failed"):
            runtime.service.run()

        assert runtime.catalog.get_run(failed_parent.run_id) == failed_parent
        retained_execution = runtime.ledger.get_partition(execution_identity)
        assert retained_execution == failed_execution
        assert runtime.state.calls == calls_before_retry
        assert all(runtime.state.calls[key] == value for key, value in early_opening_calls.items())
        identity = opening_identity
        retained = runtime.ledger.get_partition(identity)
        assert retained is not None
        assert retained.status is PartitionStatus.FAILED
        assert retained.attempts == 1
        retry_incidents = [
            item
            for item in runtime.ledger.list_incidents()
            if item.partition_run_id == identity.partition_run_id
        ]
        assert len(retry_incidents) == 1
        assert retry_incidents[0].status is IncidentStatus.OPEN
        assert runtime.catalog.count_shadow_sessions(
            account_id=accounts[0].account_id,
            since=SESSIONS[0],
            through=SESSIONS[-1],
        ) == 10
    finally:
        _close(runtime)


def test_calendar_conflict_is_disputed_and_never_reaches_shadow(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    runtime.state.calendar_conflict = SESSIONS[0]
    try:
        with pytest.raises(Exception, match="reconciliation"):
            runtime.service.run()
        record = runtime.ledger.get_partition(
            PartitionIdentity(
                "engineering_canary_test", "silver_accepted", SESSIONS[0].isoformat()
            )
        )
        assert record is not None
        assert record.status is PartitionStatus.DISPUTED
        assert runtime.ledger.list_incidents(status=IncidentStatus.OPEN)
        account = runtime.catalog.list_shadow_accounts()[0]
        assert runtime.catalog.list_shadow_events_by_type(
            account_id=account.account_id,
            event_type="account_projected",
        ) == []
    finally:
        _close(runtime)


def test_empty_open_session_st_is_quarantined_and_incident_is_persisted(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    runtime.state.empty_st = SESSIONS[0]
    try:
        with pytest.raises(Exception, match="no rows"):
            runtime.service.run()
        record = runtime.ledger.get_partition(
            PartitionIdentity(
                "engtest_fixture_calendar_1",
                "historical_st",
                SESSIONS[0].isoformat(),
            )
        )
        assert record is not None
        assert record.status is PartitionStatus.QUARANTINED
        incidents = runtime.ledger.list_incidents(status=IncidentStatus.OPEN)
        assert any(item.error_code == "historical_st_empty" for item in incidents)
    finally:
        _close(runtime)


def test_fake_adapter_is_rejected_and_pending_rotation_blocks_nonforward_canary(
    tmp_path: Path,
) -> None:
    state = _ProviderState()
    unmarked = _ControlledAdapter("unmarked", 10, (CALENDAR,), state)
    unmarked.physical_canary_controlled_test_adapter = False
    with pytest.raises(PhysicalCanaryAdmissionError, match="marked fake"):
        # The other dependencies are deliberately absent: adapter admission
        # must fail before any persistence or object-store operation.
        PhysicalEngineeringCanaryService.for_controlled_test(
            adapters=(unmarked,),
            catalog=None,  # type: ignore[arg-type]
            production_ledger=None,  # type: ignore[arg-type]
            shadow_authority=None,  # type: ignore[arg-type]
            object_store_archive=None,  # type: ignore[arg-type]
            local_root=tmp_path,
            now=lambda: NOW,
        )

    evidence = SimpleNamespace(
        credential_rotation_blockers=("tushare_token_post_exposure_rotation_pending",),
        historical_backfill_allowed=False,
    )
    with pytest.raises(PhysicalCanaryAdmissionError, match="rotation_pending"):
        require_physical_canary_credential_rotation(evidence)  # type: ignore[arg-type]

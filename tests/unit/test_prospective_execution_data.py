from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest

from factor_lab.data.catalog import sha256_file
from factor_lab.data import prospective_execution as execution_data
from factor_lab.data.prospective_execution import (
    ProspectiveExecutionDataError,
    build_prospective_execution_snapshot,
    load_prospective_execution_snapshot,
)
from factor_lab.prospective_targets import (
    GenerationResult,
    InputSnapshot,
    SleeveState,
    TenSleeveState,
)


def _bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _write_json(path: Path, value: object, *, canonical: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if canonical:
        path.write_bytes(_bytes(value))
    else:
        path.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")


@dataclass(frozen=True)
class _SignalSource:
    signal_date: str
    trade_date: str
    snapshot_sha256: str
    directory: Path
    build_completed_at_utc: str
    inputs_available_at_utc: str
    frame: pd.DataFrame
    manifest: dict[str, Any]
    calendar_sessions: tuple[str, ...]
    target_frame: pd.DataFrame
    target_rows_sha256: str
    input_sources_sha256: str
    membership_artifact_sha256: str


def _calendar_artifact(
    root: Path,
    sessions: list[str],
    checkpoint: dict[str, Any],
    *,
    completed_at: str,
) -> None:
    start = pd.Timestamp(sessions[0])
    end = pd.Timestamp(sessions[-1])
    open_set = set(sessions)
    rows: list[dict[str, Any]] = []
    previous: str | None = None
    for value in pd.date_range(start, end, freq="D"):
        text = value.date().isoformat()
        is_open = text in open_set
        rows.append(
            {
                "cal_date": text,
                "exchange": "SSE",
                "is_open": is_open,
                "pretrade_date": previous,
            }
        )
        if is_open:
            previous = text
    content_sha = hashlib.sha256(_bytes(rows)).hexdigest()
    directory = (
        root
        / "runtime/data/raw/trade_cal"
        / f"calendar_sha256={content_sha}"
    )
    path = directory / "part-000.parquet"
    directory.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows)
    frame["cal_date"] = pd.to_datetime(frame["cal_date"])
    frame["pretrade_date"] = pd.to_datetime(frame["pretrade_date"])
    frame.to_parquet(path, index=False)
    entry: dict[str, Any] = {
        "status": "complete",
        "exchange": "SSE",
        "start_date": rows[0]["cal_date"],
        "end_date": rows[-1]["cal_date"],
        "row_count": len(rows),
        "open_day_count": len(sessions),
        "path": str(path.resolve()),
        "artifact_sha256": sha256_file(path),
        "calendar_content_sha256": content_sha,
        "completed_at_utc": completed_at,
    }
    manifest = {
        "schema_version": 1,
        **entry,
        "records_sha256": content_sha,
    }
    manifest_path = directory / "manifest.json"
    _write_json(manifest_path, manifest)
    entry["manifest_path"] = str(manifest_path.resolve())
    entry["manifest_sha256"] = sha256_file(manifest_path)
    checkpoint["calendars"][content_sha] = entry


def _partition(
    root: Path,
    checkpoint: dict[str, Any],
    dataset: str,
    trade_date: str,
    frame: pd.DataFrame,
    *,
    completed_at: str,
) -> Path:
    path = (
        root
        / "runtime/data/raw"
        / dataset
        / f"trade_date={trade_date}"
        / "part-000.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    checkpoint["partitions"][f"{dataset}/{trade_date}"] = {
        "status": "complete",
        "dataset": dataset,
        "trade_date": trade_date,
        "path": str(path.resolve()),
        "row_count": len(frame),
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
        "completed_at_utc": completed_at,
    }
    return path


def _daily(tickers: list[str], trade_date: str, day_index: int) -> pd.DataFrame:
    rows = []
    for ticker_index, ticker in enumerate(tickers):
        prior = 100.0 + ticker_index * 20.0 + day_index
        close = prior + 1.0
        rows.append(
            {
                "ts_code": ticker,
                "trade_date": trade_date.replace("-", ""),
                "open": prior + 0.25,
                "high": close + 0.5,
                "low": prior - 0.5,
                "close": close,
                "pre_close": prior,
                "change": 1.0,
                "pct_chg": (close / prior - 1.0) * 100.0,
                "vol": 1_000_000.0,
                "amount": 100_000.0,
            }
        )
    return pd.DataFrame(rows)


def _daily_basic(tickers: list[str], trade_date: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": tickers,
            "trade_date": trade_date.replace("-", ""),
            "turnover_rate": 1.0,
            "turnover_rate_f": 1.0,
            "volume_ratio": 1.0,
            "pe": 10.0,
            "pe_ttm": 10.0,
            "pb": 1.0,
            "ps": 1.0,
            "ps_ttm": 1.0,
            "dv_ratio": 0.0,
            "dv_ttm": 0.0,
            "total_share": 1_000_000.0,
            "float_share": 1_000_000.0,
            "free_share": 1_000_000.0,
            "total_mv": 1_000_000.0,
            "circ_mv": 1_000_000.0,
        }
    )


def _adj(tickers: list[str], trade_date: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": tickers,
            "trade_date": trade_date.replace("-", ""),
            "adj_factor": [2.0] * len(tickers),
        }
    )


def _suspensions(root: Path, *, start: str, end: str, completed_at: str) -> None:
    top500 = root / "runtime/data/top500"
    top500.mkdir(parents=True, exist_ok=True)
    path = top500 / "suspensions.parquet"
    frame = pd.DataFrame(
        {
            "ticker": pd.Series(dtype="string"),
            "date": pd.Series(dtype="datetime64[ns]"),
            "suspend_type": pd.Series(dtype="string"),
            "suspend_timing": pd.Series(dtype="string"),
        }
    )
    frame.to_parquet(path, index=False)
    metadata_path = top500 / "suspensions.meta.json"
    metadata = {
        "schema_version": 1,
        "status": "complete",
        "source": "tushare",
        "endpoint": "suspend_d",
        "query": {
            "start_date": start,
            "end_date": end,
            "window": "calendar_year",
            "limit": 5_000,
        },
        "retrieved_at_utc": completed_at,
        "rows": 0,
        "date": {"min": None, "max": None},
        "security": 0,
        "S": 0,
        "R": 0,
        "file": {
            "path": str(path.resolve()),
            "size_bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        },
        "metadata_path": str(metadata_path.resolve()),
    }
    _write_json(metadata_path, metadata)


def _replace_suspensions(
    root: Path, frame: pd.DataFrame, *, completed_at: str
) -> None:
    path = root / "runtime/data/top500/suspensions.parquet"
    metadata_path = path.with_name("suspensions.meta.json")
    frame.to_parquet(path, index=False)
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    dates = pd.to_datetime(frame["date"], errors="coerce")
    types = frame["suspend_type"].astype("string").str.upper()
    metadata.update(
        {
            "retrieved_at_utc": completed_at,
            "rows": len(frame),
            "date": {
                "min": dates.min().date().isoformat() if len(frame) else None,
                "max": dates.max().date().isoformat() if len(frame) else None,
            },
            "security": int(frame["ticker"].nunique()),
            "S": int(types.eq("S").sum()),
            "R": int(types.eq("R").sum()),
            "file": {
                "path": str(path.resolve()),
                "size_bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            },
        }
    )
    _write_json(metadata_path, metadata)


def _generation(input_sha: str, sessions: list[str]) -> GenerationResult:
    deployment = "d" * 64
    sleeves = [SleeveState(offset=offset) for offset in range(10)]
    sleeves[1] = SleeveState(
        offset=1,
        initialized=True,
        last_signal_date=sessions[1],
        last_calendar_index=1,
        targets_ppm={"000001.SZ": 100_000},
        cash_ppm=900_000,
    )
    state = TenSleeveState(
        deployment_sha256=deployment,
        activation_record_sha256="a" * 64,
        implementation_upgrade_record_sha256="b" * 64,
        last_processed_calendar_index=1,
        last_processed_session=sessions[1],
        sleeves=sleeves,
    )
    plans = []
    for sleeve in state.sleeves:
        action = "seed" if sleeve.offset == 1 else "cash"
        plans.append({"action": action, **sleeve.to_dict()})
    return GenerationResult(
        deployment_sha256=deployment,
        input_snapshot_sha256=input_sha,
        previous_state_sha256="c" * 64,
        signal_date=sessions[1],
        trade_date=sessions[2],
        calendar_index=1,
        due_offset=1,
        skipped_sessions=(),
        sleeve_plans=plans,
        aggregate_targets_ppm={"000001.SZ": 10_000},
        aggregate_cash_ppm=990_000,
        next_state=state,
    )


@pytest.fixture
def execution_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Path, GenerationResult, _SignalSource, list[str], dict[str, Any]]:
    root = tmp_path
    monkeypatch.setattr(execution_data, "_OFFICIAL_DELIST_MINIMUM_ROWS", 1)
    tickers = ["000001.SZ", "000002.SZ", "600000.SH"]
    sessions = [value.date().isoformat() for value in pd.bdate_range("2026-08-03", periods=14)]

    class _OfficialClient:
        def stock_basic(self, **kwargs: Any) -> pd.DataFrame:
            assert kwargs == {
                "list_status": "D",
                "fields": "ts_code,list_status,delist_date",
            }
            return pd.DataFrame(
                {
                    # Historical, unrelated control row: an empty provider
                    # response must fail closed, but candidate filtering must
                    # remain exact.
                    "ts_code": ["900001.SH"],
                    "list_status": ["D"],
                    "delist_date": ["20000101"],
                }
            )

    monkeypatch.setattr(
        execution_data,
        "_official_delist_client",
        lambda _root: _OfficialClient(),
    )
    monkeypatch.setattr(
        execution_data,
        "_official_retrieved_at_utc",
        lambda: f"{sessions[12]}T08:29:00Z",
    )
    signal_date = sessions[1]
    trade_date = sessions[2]
    source_sha = "e" * 64
    target_frame = pd.DataFrame(
        {
            "date": signal_date,
            "ticker": tickers,
            "eligible": [True, True, False],
            "universe_member": True,
            "earnings_yield": [0.1, 0.09, 0.08],
            "pb": [1.0, 1.1, 1.2],
            "book_yield": [1.0, 1 / 1.1, 1 / 1.2],
            "volatility_20": [0.2, 0.21, 0.22],
        }
    )
    signal_frame = target_frame.copy()
    signal_frame["close"] = [101.0, 121.0, 141.0]
    signal_frame["close_adj"] = [202.0, 242.0, 282.0]
    signal_frame["adj_factor"] = [2.0, 2.0, 2.0]
    signal_frame["adj_calibration_multiplier"] = [1.0, 1.0, 1.0]
    signal_frame["adv_20"] = [100_000_000.0] * 3
    target_rows_sha = "1" * 64
    inputs_sha = "2" * 64
    membership_sha = "3" * 64
    source_directory = root / "runtime/prospective/5.0/inputs" / source_sha
    source_directory.mkdir(parents=True)
    top500 = root / "runtime/data/top500"
    top500.mkdir(parents=True, exist_ok=True)
    features_path = top500 / "features.parquet"
    pd.DataFrame(
        {
            "ticker": tickers,
            "delist_date": pd.Series([pd.NaT] * 3, dtype="datetime64[ns]"),
        }
    ).to_parquet(features_path, index=False)
    _features_cas, features_binding = execution_data._capture_immutable_artifact(
        root, features_path
    )
    source = _SignalSource(
        signal_date=signal_date,
        trade_date=trade_date,
        snapshot_sha256=source_sha,
        directory=source_directory,
        build_completed_at_utc=f"{signal_date}T08:30:00Z",
        inputs_available_at_utc=f"{signal_date}T08:00:00Z",
        frame=signal_frame,
        manifest={
            "inputs": [
                {
                    "role": "canonical_features",
                    "path": "runtime/data/top500/features.parquet",
                    **features_binding,
                    "availability_basis": "pre_activation_frozen_canonical",
                }
            ]
        },
        calendar_sessions=tuple(sessions[:3]),
        target_frame=target_frame,
        target_rows_sha256=target_rows_sha,
        input_sources_sha256=inputs_sha,
        membership_artifact_sha256=membership_sha,
    )
    input_snapshot = InputSnapshot(
        signal_date=signal_date,
        calendar_sessions=sessions[:3],
        rows=target_frame,
        source_data_snapshot_sha256=source_sha,
        target_rows_sha256=target_rows_sha,
        input_sources_sha256=inputs_sha,
        membership_artifact_sha256=membership_sha,
        source_build_checkpoint_utc=source.inputs_available_at_utc,
        max_available_at_utc=source.inputs_available_at_utc,
        information_cutoff_utc=source.inputs_available_at_utc,
        signal_close_utc=f"{signal_date}T07:00:00Z",
        admission_deadline_utc=f"{trade_date}T01:15:00Z",
    )
    generation = _generation(input_snapshot.snapshot_sha256, sessions)
    monkeypatch.setattr(
        execution_data,
        "load_prospective_input_snapshot",
        lambda path: source,
    )

    checkpoint: dict[str, Any] = {"schema_version": 1, "partitions": {}, "calendars": {}}
    _calendar_artifact(
        root,
        sessions,
        checkpoint,
        completed_at=f"{signal_date}T06:00:00Z",
    )
    window = sessions[2:13]
    for day_index, session in enumerate(window, start=1):
        completion = f"{session}T08:00:00Z"
        _partition(
            root,
            checkpoint,
            "daily",
            session,
            _daily(tickers, session, day_index),
            completed_at=completion,
        )
        _partition(
            root,
            checkpoint,
            "daily_basic",
            session,
            _daily_basic(tickers, session),
            completed_at=completion,
        )
        _partition(
            root,
            checkpoint,
            "adj_factor",
            session,
            _adj(tickers, session),
            completed_at=completion,
        )
    checkpoint_path = root / "runtime/data/raw/checkpoint.json"
    _write_json(checkpoint_path, checkpoint)
    _suspensions(
        root,
        start=sessions[0],
        end=window[-1],
        completed_at=f"{window[-1]}T08:30:00Z",
    )
    return root, generation, source, sessions, checkpoint


def test_build_load_binds_sources_calendar_and_decision_roster(execution_root) -> None:
    root, generation, source, sessions, _checkpoint = execution_root
    built = build_prospective_execution_snapshot(
        root,
        generation,
        source_data_snapshot_sha256=source.snapshot_sha256,
    )

    assert built.directory == (
        root
        / "runtime/prospective/5.0/executions"
        / built.snapshot.snapshot_sha256
    ).resolve()
    assert built.snapshot.generation_result_sha256 == generation.result_sha256
    assert built.source_contract["source_data_snapshot_sha256"] == source.snapshot_sha256
    assert built.source_contract["target_input_snapshot_sha256"] == generation.input_snapshot_sha256
    expected_benchmark = tuple(
        sorted(source.frame.loc[source.frame["eligible"], "ticker"])
    )
    assert built.snapshot.benchmark_tickers == expected_benchmark
    assert built.snapshot.holding_start_date == sessions[2]
    assert built.snapshot.holding_end_date == sessions[12]
    assert built.snapshot.calendar_sessions[-1] == sessions[12]
    assert len(built.snapshot.rows) == len(expected_benchmark) * 11
    assert max(row.date for row in built.snapshot.rows) == sessions[12]
    assert all(
        row.execution_input_date == generation.signal_date
        for row in built.snapshot.rows
        if row.date == sessions[2]
    )
    assert all(
        row.execution_input_date is None
        for row in built.snapshot.rows
        if row.date != sessions[2]
    )

    loaded = load_prospective_execution_snapshot(built.directory, generation)
    assert loaded.snapshot.to_dict() == built.snapshot.to_dict()
    assert loaded.sources_path.read_bytes() == built.sources_path.read_bytes()


def test_future_delist_is_officially_captured_and_replayed_from_cas(
    execution_root, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, generation, source, sessions, _checkpoint = execution_root

    class _OfficialClient:
        def stock_basic(self, **kwargs: Any) -> pd.DataFrame:
            assert kwargs["list_status"] == "D"
            return pd.DataFrame(
                {
                    "ts_code": ["000001.SZ"],
                    "list_status": ["D"],
                    "delist_date": [sessions[7].replace("-", "")],
                }
            )

    monkeypatch.setattr(
        execution_data,
        "_official_delist_client",
        lambda _root: _OfficialClient(),
    )
    # The tiny fixture has only two benchmark names.  The production 95%
    # completeness rule is orthogonal to proving delist event capture here.
    monkeypatch.setattr(execution_data, "MINIMUM_BENCHMARK_COVERAGE_PPM", 0)
    built = build_prospective_execution_snapshot(
        root,
        generation,
        source_data_snapshot_sha256=source.snapshot_sha256,
    )

    affected = [
        row
        for row in built.snapshot.rows
        if row.ticker == "000001.SZ" and row.date >= sessions[7]
    ]
    assert affected
    assert all(row.is_delisted and row.open_adj_hex is None for row in affected)
    official = built.source_contract["delists"]["official_stock_basic"]
    assert official["selected_security_count"] == 1
    assert (root / official["immutable_path"]).is_file()

    def _network_must_not_run(_root: Path) -> Any:
        raise AssertionError("sealed replay must not query the live provider")

    monkeypatch.setattr(
        execution_data,
        "_official_delist_client",
        _network_must_not_run,
    )
    loaded = load_prospective_execution_snapshot(built.directory, generation)
    assert loaded.snapshot.to_dict() == built.snapshot.to_dict()


def test_production_delist_full_table_floor_is_not_a_one_row_sentinel() -> None:
    assert execution_data._OFFICIAL_DELIST_MINIMUM_ROWS >= 200


def test_independent_official_delist_queries_must_match(
    execution_root, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, generation, source, _sessions, _checkpoint = execution_root

    class _ChangingOfficialClient:
        calls = 0

        def stock_basic(self, **_kwargs: Any) -> pd.DataFrame:
            self.calls += 1
            return pd.DataFrame(
                {
                    "ts_code": ["900001.SH" if self.calls == 1 else "900002.SH"],
                    "list_status": ["D"],
                    "delist_date": ["20000101"],
                }
            )

    monkeypatch.setattr(
        execution_data,
        "_official_delist_client",
        lambda _root: _ChangingOfficialClient(),
    )
    with pytest.raises(
        ProspectiveExecutionDataError,
        match="independent official delist queries differ",
    ):
        build_prospective_execution_snapshot(
            root,
            generation,
            source_data_snapshot_sha256=source.snapshot_sha256,
        )


def test_official_delist_retrieval_time_is_ceiled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Clock:
        @classmethod
        def now(cls, tz):
            assert tz is timezone.utc
            return datetime(2026, 8, 28, 12, 0, 0, 1, tzinfo=timezone.utc)

    monkeypatch.setattr(execution_data, "datetime", _Clock)
    assert execution_data._official_retrieved_at_utc() == "2026-08-28T12:00:01Z"


def test_execution_rejects_boolean_raw_checkpoint_schema(execution_root) -> None:
    root, generation, source, _sessions, checkpoint = execution_root
    checkpoint["schema_version"] = True
    _write_json(root / "runtime/data/raw/checkpoint.json", checkpoint)

    with pytest.raises(ProspectiveExecutionDataError, match="checkpoint schema"):
        build_prospective_execution_snapshot(
            root,
            generation,
            source_data_snapshot_sha256=source.snapshot_sha256,
        )


def test_loader_rejects_tampered_official_delist_cas(
    execution_root,
) -> None:
    root, generation, source, _sessions, _checkpoint = execution_root
    built = build_prospective_execution_snapshot(
        root,
        generation,
        source_data_snapshot_sha256=source.snapshot_sha256,
    )
    official = built.source_contract["delists"]["official_stock_basic"]
    (root / official["immutable_path"]).write_bytes(b"tampered official status")

    with pytest.raises(ProspectiveExecutionDataError, match="official delist artifact"):
        load_prospective_execution_snapshot(built.directory, generation)


def test_official_delist_query_cannot_succeed_with_an_empty_response() -> None:
    with pytest.raises(ProspectiveExecutionDataError, match="implausibly empty"):
        execution_data._normalise_official_delist_rows(
            pd.DataFrame(columns=["ts_code", "list_status", "delist_date"])
        )


def test_future_partition_is_not_selected(execution_root) -> None:
    root, generation, source, sessions, checkpoint = execution_root
    first = build_prospective_execution_snapshot(
        root, generation, source_data_snapshot_sha256=source.snapshot_sha256
    )
    future = sessions[13]
    _partition(
        root,
        checkpoint,
        "daily",
        future,
        _daily(list(source.frame["ticker"]), future, 99),
        completed_at=f"{future}T08:00:00Z",
    )
    _write_json(root / "runtime/data/raw/checkpoint.json", checkpoint)

    second = build_prospective_execution_snapshot(
        root, generation, source_data_snapshot_sha256=source.snapshot_sha256
    )
    assert second.snapshot.snapshot_sha256 == first.snapshot.snapshot_sha256
    assert all(
        item["trade_date"] <= first.snapshot.holding_end_date
        for item in second.source_contract["raw_partitions"]
    )


def test_missing_checkpointed_partition_fails_closed(execution_root) -> None:
    root, generation, source, sessions, checkpoint = execution_root
    del checkpoint["partitions"][f"adj_factor/{sessions[5]}"]
    _write_json(root / "runtime/data/raw/checkpoint.json", checkpoint)
    with pytest.raises(ProspectiveExecutionDataError, match="missing checkpointed partition"):
        build_prospective_execution_snapshot(
            root, generation, source_data_snapshot_sha256=source.snapshot_sha256
        )


def test_future_row_inside_selected_partition_fails_closed(execution_root) -> None:
    root, generation, source, sessions, checkpoint = execution_root
    selected = sessions[5]
    frame = _daily(list(source.frame["ticker"]), selected, 4)
    future_row = frame.iloc[[0]].copy()
    future_row["trade_date"] = sessions[13].replace("-", "")
    poisoned = pd.concat([frame, future_row], ignore_index=True)
    _partition(
        root,
        checkpoint,
        "daily",
        selected,
        poisoned,
        completed_at=f"{selected}T08:00:00Z",
    )
    _write_json(root / "runtime/data/raw/checkpoint.json", checkpoint)
    with pytest.raises(ProspectiveExecutionDataError, match="invalid/future/duplicate"):
        build_prospective_execution_snapshot(
            root, generation, source_data_snapshot_sha256=source.snapshot_sha256
        )


def test_public_loader_uses_only_immutable_sources_after_canonical_refresh(
    execution_root,
) -> None:
    root, generation, source, sessions, checkpoint = execution_root
    built = build_prospective_execution_snapshot(
        root, generation, source_data_snapshot_sha256=source.snapshot_sha256
    )
    prospective_root = root / "runtime/prospective/5.0"
    before = {
        path.relative_to(prospective_root).as_posix(): path.read_bytes()
        for path in prospective_root.rglob("*")
        if path.is_file()
    }
    path = Path(checkpoint["partitions"][f"daily/{sessions[5]}"]["path"])
    frame = pd.read_parquet(path)
    frame.loc[0, "open"] = float(frame.loc[0, "open"]) + 0.01
    frame.to_parquet(path, index=False)
    calendar_entry = next(iter(checkpoint["calendars"].values()))
    Path(calendar_entry["path"]).write_bytes(b"later calendar refresh")
    Path(calendar_entry["manifest_path"]).write_bytes(b"later calendar manifest")
    (root / "runtime/data/top500/features.parquet").write_bytes(b"later feature refresh")
    (root / "runtime/data/top500/suspensions.parquet").write_bytes(
        b"later suspension refresh"
    )
    _write_json(
        root / "runtime/data/raw/checkpoint.json",
        {"schema_version": 1, "partitions": {}, "calendars": {}},
    )

    loaded = load_prospective_execution_snapshot(built.directory, generation)
    after = {
        item.relative_to(prospective_root).as_posix(): item.read_bytes()
        for item in prospective_root.rglob("*")
        if item.is_file()
    }
    assert loaded.snapshot.snapshot_sha256 == built.snapshot.snapshot_sha256
    assert after == before


def test_public_loader_rejects_tampered_immutable_partition(execution_root) -> None:
    root, generation, source, _sessions, _checkpoint = execution_root
    built = build_prospective_execution_snapshot(
        root, generation, source_data_snapshot_sha256=source.snapshot_sha256
    )
    raw_source = built.source_contract["raw_partitions"][0]
    (root / raw_source["immutable_path"]).write_bytes(b"tampered CAS bytes")

    with pytest.raises(ProspectiveExecutionDataError, match="CAS binding|bytes differ"):
        load_prospective_execution_snapshot(built.directory, generation)


def test_fractional_availability_is_ceiled_without_claiming_earlier_time(
    execution_root,
) -> None:
    root, generation, source, sessions, checkpoint = execution_root
    calendar_entry = next(iter(checkpoint["calendars"].values()))
    calendar_entry["completed_at_utc"] = f"{sessions[1]}T06:00:00.123456Z"
    for entry in checkpoint["partitions"].values():
        entry["completed_at_utc"] = f"{entry['trade_date']}T08:00:00.123456Z"
    _write_json(root / "runtime/data/raw/checkpoint.json", checkpoint)
    suspension_frame = pd.read_parquet(root / "runtime/data/top500/suspensions.parquet")
    _replace_suspensions(
        root,
        suspension_frame,
        completed_at=f"{sessions[12]}T08:30:00.123456Z",
    )

    built = build_prospective_execution_snapshot(
        root, generation, source_data_snapshot_sha256=source.snapshot_sha256
    )

    assert built.snapshot.calendar_available_at_utc.endswith("06:00:01Z")
    assert built.snapshot.start_open_available_at_utc.endswith("08:00:01Z")
    assert built.snapshot.end_open_available_at_utc.endswith("08:00:01Z")
    assert built.snapshot.observation_available_at_utc.endswith("08:30:01Z")
    assert all(
        item["completed_at_utc"].endswith("08:00:01Z")
        for item in built.source_contract["raw_partitions"]
    )
    loaded = load_prospective_execution_snapshot(built.directory, generation)
    assert loaded.snapshot.to_dict() == built.snapshot.to_dict()


def test_calendar_must_reach_holding_end_before_trade_deadline(execution_root) -> None:
    root, generation, source, sessions, checkpoint = execution_root
    key = next(iter(checkpoint["calendars"]))
    checkpoint["calendars"][key]["completed_at_utc"] = f"{sessions[2]}T02:00:00Z"
    _write_json(root / "runtime/data/raw/checkpoint.json", checkpoint)
    with pytest.raises(ProspectiveExecutionDataError, match="no checkpointed official calendar"):
        build_prospective_execution_snapshot(
            root, generation, source_data_snapshot_sha256=source.snapshot_sha256
        )


def test_adj_factor_jump_is_a_corporate_action_not_calibration_drift(execution_root) -> None:
    root, generation, source, sessions, checkpoint = execution_root
    tickers = list(source.frame["ticker"])
    # At sessions[5], ticker 000001.SZ's factor doubles from 2 to 4 while its
    # raw pre-close halves.  The adjusted pre-close remains continuous at 208.
    prior_raw = 52.0
    for session in sessions[5:13]:
        daily_path = Path(checkpoint["partitions"][f"daily/{session}"]["path"])
        daily = pd.read_parquet(daily_path)
        mask = daily["ts_code"].eq("000001.SZ")
        daily.loc[mask, "pre_close"] = prior_raw
        daily.loc[mask, "open"] = prior_raw + 0.25
        daily.loc[mask, "low"] = prior_raw - 0.5
        daily.loc[mask, "close"] = prior_raw + 1.0
        daily.loc[mask, "high"] = prior_raw + 1.5
        daily.loc[mask, "change"] = 1.0
        daily.loc[mask, "pct_chg"] = 100.0 / prior_raw
        _partition(
            root,
            checkpoint,
            "daily",
            session,
            daily,
            completed_at=f"{session}T08:00:00Z",
        )
        adj_path = Path(checkpoint["partitions"][f"adj_factor/{session}"]["path"])
        adj = pd.read_parquet(adj_path)
        adj.loc[adj["ts_code"].eq("000001.SZ"), "adj_factor"] = 4.0
        _partition(
            root,
            checkpoint,
            "adj_factor",
            session,
            adj,
            completed_at=f"{session}T08:00:00Z",
        )
        prior_raw += 1.0
    _write_json(root / "runtime/data/raw/checkpoint.json", checkpoint)

    built = build_prospective_execution_snapshot(
        root, generation, source_data_snapshot_sha256=source.snapshot_sha256
    )
    event = next(
        row
        for row in built.snapshot.rows
        if row.date == sessions[5] and row.ticker == "000001.SZ"
    )
    assert float.fromhex(event.open_adj_hex) == pytest.approx((52.0 + 0.25) * 4.0)


def test_missing_suspension_timing_is_full_day_and_not_ambiguous(execution_root) -> None:
    root, generation, source, sessions, _checkpoint = execution_root
    frame = pd.DataFrame(
        {
            "ticker": ["000001.SZ", "000001.SZ"],
            "date": pd.to_datetime([sessions[5], sessions[6]]),
            "suspend_type": pd.Series(["S", "R"], dtype="string"),
            "suspend_timing": pd.Series([pd.NA, pd.NA], dtype="string"),
        }
    )
    _replace_suspensions(root, frame, completed_at=f"{sessions[12]}T08:30:00Z")
    built = build_prospective_execution_snapshot(
        root, generation, source_data_snapshot_sha256=source.snapshot_sha256
    )
    event = next(
        row
        for row in built.snapshot.rows
        if row.date == sessions[5] and row.ticker == "000001.SZ"
    )
    assert event.is_suspended is True
    assert event.open_adj_hex is None


def test_suspension_snapshot_must_postdate_end_market_completion(execution_root) -> None:
    root, generation, source, sessions, _checkpoint = execution_root
    empty = pd.DataFrame(
        {
            "ticker": pd.Series(dtype="string"),
            "date": pd.Series(dtype="datetime64[ns]"),
            "suspend_type": pd.Series(dtype="string"),
            "suspend_timing": pd.Series(dtype="string"),
        }
    )
    _replace_suspensions(root, empty, completed_at=f"{sessions[12]}T07:59:59Z")
    with pytest.raises(ProspectiveExecutionDataError, match="predates holding-end"):
        build_prospective_execution_snapshot(
            root, generation, source_data_snapshot_sha256=source.snapshot_sha256
        )


def test_immutable_suspension_source_survives_canonical_overwrite(execution_root) -> None:
    root, generation, source, _sessions, _checkpoint = execution_root
    built = build_prospective_execution_snapshot(
        root, generation, source_data_snapshot_sha256=source.snapshot_sha256
    )
    canonical = root / "runtime/data/top500/suspensions.parquet"
    canonical.write_bytes(b"later mutable replacement")

    loaded = load_prospective_execution_snapshot(built.directory, generation)
    assert loaded.snapshot.snapshot_sha256 == built.snapshot.snapshot_sha256


def test_benchmark_endpoint_coverage_below_frozen_95_percent_is_rejected(
    execution_root,
) -> None:
    root, generation, source, sessions, _checkpoint = execution_root
    frame = pd.DataFrame(
        {
            "ticker": pd.Series(["000002.SZ"], dtype="string"),
            "date": pd.to_datetime([sessions[12]]),
            "suspend_type": pd.Series(["S"], dtype="string"),
            "suspend_timing": pd.Series([pd.NA], dtype="string"),
        }
    )
    _replace_suspensions(root, frame, completed_at=f"{sessions[12]}T08:30:00Z")
    with pytest.raises(ProspectiveExecutionDataError, match="95% minimum"):
        build_prospective_execution_snapshot(
            root, generation, source_data_snapshot_sha256=source.snapshot_sha256
        )


@pytest.mark.parametrize(
    ("event_field", "event_at_start"),
    [
        ("is_suspended", True),
        ("is_suspended", False),
        ("is_delisted", True),
        ("is_delisted", False),
    ],
)
def test_benchmark_endpoint_coverage_rejects_flagged_open_endpoint(
    event_field: str,
    event_at_start: bool,
) -> None:
    def endpoint(**overrides: object) -> SimpleNamespace:
        values: dict[str, object] = {
            "open_adj_hex": 100.0.hex(),
            "is_suspended": False,
            "is_delisted": False,
        }
        values.update(overrides)
        return SimpleNamespace(**values)

    clean = endpoint()
    flagged = endpoint(**{event_field: True})
    start, end = (flagged, clean) if event_at_start else (clean, flagged)

    assert execution_data._benchmark_endpoint_pair_complete(clean, clean) is True
    assert execution_data._benchmark_endpoint_pair_complete(start, end) is False

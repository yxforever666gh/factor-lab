#!/usr/bin/env python
"""Build and run the pre-registered 6.1 widened-opportunity-set experiment.

Selection is deliberately staged: train is built/evaluated first; validation
is not opened unless at least one challenger passes train.  Audit is a
separate invocation that requires a hash-verified winner-freeze artifact.
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = SCRIPT_PATH.parents[1]
SOURCE_ROOT = PROJECT_ROOT / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from factor_lab.data import (  # noqa: E402
    DailyOpportunitySetBuilder,
    RuntimeLayout,
    SparsePricingBuilder,
    audit_security_master,
    audit_suspensions_snapshot,
    load_data_config,
    load_security_master,
)
from factor_lab.data.catalog import sha256_file  # noqa: E402
from factor_lab.data.sources import (  # noqa: E402
    DAILY_STOCK_ST_CONTRACT_ID,
    DAILY_STOCK_ST_CUTOFF_VIEW_CONTRACT_ID,
    ENRICHMENT_DATASET_FIELDS,
    _daily_stock_st_partition_path,
    _daily_stock_st_partition_root_payload_sha256,
    _replace_atomic_with_windows_retry,
    _write_json_atomic,
    _write_parquet_atomic,
)
from factor_lab.release_integrity import (  # noqa: E402
    AUDIT_EVIDENCE_PATH,
    FROZEN_HISTORICAL_AUDIT,
    FROZEN_IMPLEMENTATION_PATHS,
    RELEASE_RESULT_PATH,
    RUNTIME_PATH,
    WINNER_FREEZE_PATH,
    verify_active_runtime,
    verify_historical_audit,
    verify_preselection_closure,
    verify_winner_freeze,
)
from factor_lab.portfolio.long_only import (  # noqa: E402
    LongOnlyCostConfig,
    LongOnlyPortfolioConfig,
    evaluate_long_only_portfolio,
)
from factor_lab.research.wide_universe import (  # noqa: E402
    CHALLENGER_IDS,
    CONTROL_ID,
    PhaseBounds,
    UNIVERSE_IDS,
    audit_rankings,
    build_target_decisions,
    candidate_gate,
    canonical_sha256,
    decisions_frame,
    select_winner,
    summarize_phase,
    target_maps,
)
from factor_lab.strategy import LowChurnStrategyConfig  # noqa: E402


DATA_START = pd.Timestamp("2016-07-07")
ANCHOR = pd.Timestamp("2017-01-03")
TRAIN_END = pd.Timestamp("2022-12-31")
VALIDATION_START = pd.Timestamp("2023-01-01")
VALIDATION_END = pd.Timestamp("2024-12-31")
AUDIT_START = pd.Timestamp("2025-01-01")
AUDIT_END = pd.Timestamp(FROZEN_HISTORICAL_AUDIT["physical_market_data_end"])
TRAIN_SUSPENSIONS = "runtime/data/wide-universe/train/suspensions.parquet"
TRAIN_SUSPENSION_METADATA = "runtime/data/wide-universe/train/suspensions.meta.json"
TRAIN_ST_CHECKPOINT = (
    "runtime/data/wide-universe/train/stock-st-isolated-checkpoint.json"
)
SELECTION_SUSPENSIONS = "runtime/data/wide-universe/selection/suspensions.parquet"
SELECTION_SUSPENSION_METADATA = (
    "runtime/data/wide-universe/selection/suspensions.meta.json"
)
SELECTION_ST_CHECKPOINT = (
    "runtime/data/wide-universe/selection/stock-st-isolated-checkpoint.json"
)
AUDIT_SUSPENSIONS = "runtime/data/wide-universe/audit/suspensions.parquet"
AUDIT_SUSPENSION_METADATA = "runtime/data/wide-universe/audit/suspensions.meta.json"
AUDIT_ST_CHECKPOINT = (
    "runtime/data/wide-universe/audit/stock-st-isolated-checkpoint.json"
)
WINNER_FREEZE = WINNER_FREEZE_PATH


def _selected_definition(candidate_id: str) -> dict[str, Any]:
    """Return the complete immutable definition admitted to historical audit."""

    return {
        "candidate_id": str(candidate_id),
        "signal": "fixed_core_defensive_weight_0.70",
        "portfolio": "Top10_exit25_10_offsets",
        "capital": 50_000_000.0,
        "max_adv_participation": 0.05,
        "costs_source": "configs/research.json",
    }


def _selected_definition_matches(value: Any, candidate_id: str) -> bool:
    """Require exact canonical equality, including keys and numeric encoding."""

    if not isinstance(value, Mapping):
        return False
    return canonical_sha256(value) == canonical_sha256(
        _selected_definition(candidate_id)
    )


def _path(value: str | Path) -> Path:
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (PROJECT_ROOT / path).resolve()


def _portable(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return str(path.resolve())


def _same_path_or_file(left: Path, right: Path) -> bool:
    left_resolved = left.resolve()
    right_resolved = right.resolve()
    if left_resolved == right_resolved:
        return True
    if left_resolved.exists() and right_resolved.exists():
        try:
            return os.path.samefile(left_resolved, right_resolved)
        except OSError:
            return False
    return False


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON object required: {path}")
    return value


def _payload_sha256(value: Mapping[str, Any]) -> str:
    payload = dict(value)
    payload.pop("payload_sha256", None)
    return canonical_sha256(payload)


def _git_text(*args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=PROJECT_ROOT,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ).stdout.strip()


def _write_json_create_only(path: Path, payload: Mapping[str, Any]) -> None:
    """Publish a JSON terminal artifact with an atomic exclusive create."""

    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (
        json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False, default=str)
        + "\n"
    ).encode("utf-8")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(path, flags, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        if descriptor >= 0:
            os.close(descriptor)
        path.unlink(missing_ok=True)
        raise


class SourceLedger:
    """Hash every admitted source once, then prove it did not change."""

    def __init__(self) -> None:
        self._records: dict[Path, dict[str, Any]] = {}

    def admit(self, path: Path, *, expected_sha256: str | None = None) -> None:
        resolved = path.resolve()
        if resolved in self._records:
            if expected_sha256 and self._records[resolved]["sha256"] != expected_sha256:
                raise ValueError(f"conflicting expected hashes for {resolved}")
            return
        if not resolved.is_file() or resolved.is_symlink():
            raise FileNotFoundError(f"source must be a regular file: {resolved}")
        digest = sha256_file(resolved)
        if expected_sha256 is not None and digest != expected_sha256:
            raise ValueError(f"source/checkpoint SHA mismatch: {resolved}")
        self._records[resolved] = {
            "path": _portable(resolved),
            "size_bytes": int(resolved.stat().st_size),
            "sha256": digest,
        }

    def verify_unchanged(self) -> None:
        for path, record in self._records.items():
            if (
                not path.is_file()
                or path.is_symlink()
                or path.stat().st_size != int(record["size_bytes"])
                or sha256_file(path) != record["sha256"]
            ):
                raise RuntimeError(f"source changed during stage build: {path}")

    def payload(self) -> dict[str, Any]:
        files = sorted(self._records.values(), key=lambda row: str(row["path"]))
        return {
            "file_count": len(files),
            "files": files,
            "payload_sha256": canonical_sha256(files),
        }


def _runtime_layout(config_path: Path) -> tuple[dict[str, Any], RuntimeLayout]:
    config = load_data_config(config_path)
    layout = RuntimeLayout.from_config(
        config, config_path=config_path, repo_root=PROJECT_ROOT
    )
    return config, layout


def _checkpoint(path: Path, ledger: SourceLedger) -> dict[str, Any]:
    ledger.admit(path)
    value = _read_json(path)
    if not isinstance(value.get("partitions"), Mapping):
        raise ValueError(f"checkpoint partitions missing: {path}")
    return value


def _stock_st_cutoff_view(
    path: Path,
    ledger: SourceLedger,
    *,
    layout: RuntimeLayout,
    stage: str,
    end: pd.Timestamp,
    calendar: pd.DatetimeIndex,
    calendar_contract: Mapping[str, Any],
    winner_freeze_payload_sha256: str | None,
) -> dict[str, Any]:
    """Load and independently audit one exact stage-specific ST allowlist."""

    view = _checkpoint(path, ledger)
    expected_fields = {
        "schema_version",
        "kind",
        "contract_id",
        "status",
        "source",
        "dataset",
        "stage",
        "start_date",
        "cutoff_date",
        "partition_count",
        "official_open_session_count",
        "official_calendar_records_sha256",
        "source_checkpoint_path",
        "source_checkpoint_sha256",
        "partition_root",
        "partition_root_payload_sha256",
        "partitions_payload_sha256",
        "partitions",
        "payload_sha256",
    }
    if stage == "audit":
        expected_fields.add("winner_freeze_payload_sha256")
    if set(view) != expected_fields:
        raise ValueError("stock_st cutoff view contains missing or unknown fields")
    expected_end = end.date().isoformat()
    if (
        view.get("schema_version") != 1
        or view.get("kind") != "factor_lab_daily_stock_st_cutoff_view"
        or view.get("contract_id") != DAILY_STOCK_ST_CUTOFF_VIEW_CONTRACT_ID
        or view.get("status") != "complete"
        or view.get("source") != "tushare"
        or view.get("dataset") != "stock_st"
        or view.get("stage") != stage
        or view.get("start_date") != DATA_START.date().isoformat()
        or view.get("cutoff_date") != expected_end
        or view.get("payload_sha256") != _payload_sha256(view)
        or view.get("official_calendar_records_sha256")
        != calendar_contract.get("records_sha256")
    ):
        raise ValueError("stock_st cutoff-view contract mismatch")
    if stage == "audit":
        if view.get("winner_freeze_payload_sha256") != winner_freeze_payload_sha256:
            raise ValueError("audit stock_st view is not bound to the winner freeze")
    elif winner_freeze_payload_sha256 is not None:
        raise ValueError("non-audit stage cannot bind a winner freeze")

    partitions = view["partitions"]
    expected_partition_root = (
        path.resolve().parent / "stock_st" / f"stage={stage}"
    ).resolve()
    expected_keys = {
        f"stock_st/trade_date={date.date().isoformat()}" for date in calendar
    }
    if (
        set(partitions) != expected_keys
        or int(view.get("partition_count") or -1) != len(expected_keys)
        or int(view.get("official_open_session_count") or -1) != len(expected_keys)
        or view.get("partitions_payload_sha256") != canonical_sha256(partitions)
        or Path(str(view.get("partition_root") or "")).resolve()
        != expected_partition_root
        or view.get("partition_root_payload_sha256")
        != _daily_stock_st_partition_root_payload_sha256(
            expected_partition_root, partitions
        )
    ):
        raise ValueError("stock_st cutoff view is not the exact official-session set")

    entry_fields = {
        "status",
        "contract_id",
        "source",
        "dataset",
        "trade_date",
        "path",
        "row_count",
        "size_bytes",
        "sha256",
        "endpoint_row_limit",
        "official_calendar_exchange",
        "official_calendar_session_sha256",
        "completed_at_utc",
    }
    expected_schema = tuple(ENRICHMENT_DATASET_FIELDS["stock_st"].split(","))
    seen_paths: set[Path] = set()
    for date in calendar:
        trade_date = date.date().isoformat()
        key = f"stock_st/trade_date={trade_date}"
        entry = partitions[key]
        if not isinstance(entry, Mapping) or set(entry) != entry_fields:
            raise ValueError(f"stock_st cutoff-view entry schema mismatch: {key}")
        expected_path = _daily_stock_st_partition_path(
            expected_partition_root, trade_date
        ).resolve()
        recorded_path = Path(str(entry.get("path") or "")).resolve()
        session_sha256 = (calendar_contract.get("session_sha256") or {}).get(
            trade_date
        )
        if (
            entry.get("status") != "complete"
            or entry.get("contract_id") != DAILY_STOCK_ST_CONTRACT_ID
            or entry.get("source") != "tushare"
            or entry.get("dataset") != "stock_st"
            or entry.get("trade_date") != trade_date
            or entry.get("endpoint_row_limit") != 1_000
            or entry.get("official_calendar_exchange") != "SSE"
            or entry.get("official_calendar_session_sha256") != session_sha256
            or recorded_path != expected_path
            or expected_path in seen_paths
            or not expected_path.is_file()
            or expected_path.is_symlink()
            or expected_path.stat().st_nlink != 1
            or not (0 < int(entry.get("row_count") or 0) < 1_000)
            or expected_path.stat().st_size != int(entry.get("size_bytes") or -1)
        ):
            raise ValueError(f"stock_st cutoff-view entry identity failed: {key}")
        seen_paths.add(expected_path)
        ledger.admit(expected_path, expected_sha256=str(entry.get("sha256") or ""))
        frame = pd.read_parquet(expected_path)
        if tuple(frame.columns) != expected_schema or len(frame) != int(entry["row_count"]):
            raise ValueError(f"stock_st cutoff-view Parquet schema/count failed: {key}")
        trade_dates = pd.to_datetime(
            frame["trade_date"].astype("string"), format="%Y%m%d", errors="coerce"
        )
        tickers = frame["ts_code"].astype("string").str.strip()
        if (
            trade_dates.isna().any()
            or not trade_dates.dt.normalize().eq(date).all()
            or tickers.isna().any()
            or tickers.eq("").any()
            or tickers.duplicated().any()
        ):
            raise ValueError(f"stock_st cutoff-view Parquet semantics failed: {key}")
    return view


def _load_official_calendar(
    layout: RuntimeLayout,
    raw_checkpoint: Mapping[str, Any],
    ledger: SourceLedger,
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> tuple[pd.DatetimeIndex, list[dict[str, Any]], dict[str, Any]]:
    selected_frames: list[pd.DataFrame] = []
    identities: list[dict[str, Any]] = []
    calendars = raw_checkpoint.get("calendars") or {}
    for content_sha256, raw_entry in calendars.items():
        if not isinstance(raw_entry, Mapping) or raw_entry.get("status") != "complete":
            continue
        entry_start = pd.Timestamp(str(raw_entry.get("start_date"))).normalize()
        entry_end = pd.Timestamp(str(raw_entry.get("end_date"))).normalize()
        if entry_end < start or entry_start > end:
            continue
        path = Path(str(raw_entry.get("path"))).resolve()
        expected = str(raw_entry.get("artifact_sha256") or "")
        if not expected or str(raw_entry.get("calendar_content_sha256")) != str(
            content_sha256
        ):
            raise ValueError("calendar checkpoint identity is incomplete")
        ledger.admit(path, expected_sha256=expected)
        frame = pd.read_parquet(path)
        frame["cal_date"] = pd.to_datetime(frame["cal_date"], errors="coerce").dt.normalize()
        selected_frames.append(frame)
        identities.append(
            {
                "calendar_content_sha256": str(content_sha256),
                "artifact_sha256": expected,
                "path": _portable(path),
                "start_date": str(raw_entry.get("start_date")),
                "end_date": str(raw_entry.get("end_date")),
            }
        )
    if not selected_frames:
        raise ValueError("no checkpointed official calendar covers the stage")
    calendar = pd.concat(selected_frames, ignore_index=True)
    calendar = calendar.loc[calendar["cal_date"].between(start, end)].copy()
    conflicts = calendar.groupby("cal_date", sort=False)["is_open"].nunique(dropna=False)
    if bool(conflicts.gt(1).any()):
        raise ValueError("official calendar artifacts disagree on is_open")
    calendar = calendar.drop_duplicates("cal_date").sort_values("cal_date")
    expected_dates = pd.date_range(start, end, freq="D")
    if not pd.DatetimeIndex(calendar["cal_date"]).equals(expected_dates):
        raise ValueError("official calendar does not cover every calendar day")
    is_open = calendar["is_open"].astype(bool)
    open_dates = pd.DatetimeIndex(calendar.loc[is_open, "cal_date"])
    if open_dates.empty:
        raise ValueError("official calendar contains no open sessions")
    records = [
        {
            "cal_date": row.cal_date.date().isoformat(),
            "exchange": str(row.exchange),
            "is_open": bool(row.is_open),
            "pretrade_date": (
                pd.Timestamp(row.pretrade_date).date().isoformat()
                if not pd.isna(row.pretrade_date)
                else None
            ),
        }
        for row in calendar[
            ["cal_date", "exchange", "is_open", "pretrade_date"]
        ].itertuples(index=False)
    ]
    open_records = {row["cal_date"]: row for row in records if row["is_open"]}
    contract = {
        "records_sha256": canonical_sha256(records),
        "session_sha256": {
            trade_date: canonical_sha256(record)
            for trade_date, record in open_records.items()
        },
    }
    return (
        open_dates,
        sorted(identities, key=lambda row: row["start_date"]),
        contract,
    )


def _partition_entry(
    checkpoint: Mapping[str, Any], dataset: str, date: pd.Timestamp
) -> Mapping[str, Any]:
    value = date.date().isoformat()
    key = (
        f"stock_st/trade_date={value}" if dataset == "stock_st" else f"{dataset}/{value}"
    )
    entry = (checkpoint.get("partitions") or {}).get(key)
    if not isinstance(entry, Mapping) or entry.get("status") != "complete":
        raise ValueError(f"missing complete checkpoint entry: {key}")
    return entry


def _read_partition(
    checkpoint: Mapping[str, Any],
    dataset: str,
    date: pd.Timestamp,
    columns: Sequence[str],
    ledger: SourceLedger,
) -> pd.DataFrame:
    entry = _partition_entry(checkpoint, dataset, date)
    path = Path(str(entry.get("path"))).resolve()
    expected = str(entry.get("sha256") or "")
    ledger.admit(path, expected_sha256=expected)
    frame = pd.read_parquet(path, columns=list(columns))
    if int(entry.get("row_count") or -1) != len(frame):
        raise ValueError(f"{dataset}/{date.date()} row count differs from checkpoint")
    return frame


def _filter_security_scope(master: pd.DataFrame) -> pd.DataFrame:
    exchange = master["exchange"].astype("string").str.upper()
    currency = master["curr_type"].astype("string").str.upper()
    ticker = master["ts_code"].astype("string").str.upper()
    selected = master.loc[
        exchange.isin(["SSE", "SZSE"])
        & currency.eq("CNY")
        & ticker.str.fullmatch(r"\d{6}\.(?:SH|SZ)", na=False)
    ].copy()
    if selected.empty or selected["ts_code"].duplicated().any():
        raise ValueError("SSE/SZSE CNY ordinary-share security master is invalid")
    return selected.reset_index(drop=True)


def _restrict_partition_to_security_scope(
    frame: pd.DataFrame,
    *,
    identifier: str,
    allowed_tickers: set[str],
    role: str,
    date: pd.Timestamp,
) -> tuple[pd.DataFrame, int]:
    """Ignore explicit BSE rows but fail on an unknown Shanghai/Shenzhen code."""

    codes = frame[identifier].astype("string").str.strip().str.upper()
    allowed = codes.isin(allowed_tickers)
    ignored_bj = ~allowed & codes.str.fullmatch(r"\d{6}\.BJ", na=False)
    unexpected = sorted(set(codes.loc[~allowed & ~ignored_bj].dropna().astype(str)))
    if unexpected:
        raise ValueError(
            f"{role}/{date.date()} contains codes absent from the security master: "
            f"{unexpected[:10]}"
        )
    selected = frame.loc[allowed].copy()
    return selected, int(ignored_bj.sum())


def _load_suspensions(
    path: Path,
    metadata_path: Path,
    ledger: SourceLedger,
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    audit = audit_suspensions_snapshot(
        path,
        metadata_path=metadata_path,
        requested_start=start.date().isoformat(),
        requested_end=end.date().isoformat(),
    )
    if audit.get("status") != "complete":
        raise ValueError(f"suspension snapshot failed audit: {audit}")
    query = dict(audit.get("query") or {})
    if query.get("start_date") != start.date().isoformat() or query.get(
        "end_date"
    ) != end.date().isoformat():
        raise ValueError("suspension snapshot must have the exact physical stage range")
    ledger.admit(path, expected_sha256=str(audit["hash"]))
    ledger.admit(metadata_path)
    frame = pd.read_parquet(path)
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.normalize()
    if frame["date"].isna().any():
        raise ValueError("suspensions contain invalid dates")
    return frame.loc[frame["date"].between(start, end)].copy()


def _stage_paths(root: Path, stage: str) -> dict[str, Path]:
    directory = root / stage
    directory.mkdir(parents=True, exist_ok=True)
    return {
        "root": directory,
        "rankings": directory / "rankings.parquet",
        "targets": directory / "targets.parquet",
        "carried_suspensions": directory / "carried-suspensions.parquet",
        "decisions": directory / "decisions.json",
        "pricing": directory / "sparse-pricing.parquet",
        "source_files": directory / "source-files.json",
        "manifest": directory / "manifest.json",
        "evaluation": directory / "evaluation.json",
    }


def _artifact(path: Path) -> dict[str, Any]:
    return {
        "path": _portable(path),
        "size_bytes": int(path.stat().st_size),
        "sha256": sha256_file(path),
    }


def _terminal_binding(path: Path, *, expected_path: str) -> dict[str, Any]:
    if _portable(path) != expected_path:
        raise ValueError(f"terminal artifact path differs: {expected_path}")
    value = _read_json(path)
    if value.get("payload_sha256") != _payload_sha256(value):
        raise ValueError(f"terminal artifact payload differs: {expected_path}")
    return {
        "path": expected_path,
        "file_sha256": sha256_file(path),
        "payload_sha256": value["payload_sha256"],
    }


def _stage_protocol(
    protocol_path: Path,
    amendment_path: Path,
    ledger: SourceLedger,
) -> dict[str, Any]:
    ledger.admit(protocol_path)
    protocol = _read_json(protocol_path)
    actual = _payload_sha256(protocol)
    if protocol.get("payload_sha256") != actual:
        raise ValueError("6.1 protocol payload hash is invalid")
    if protocol.get("protocol_id") != "factor-lab/6.1/widened-opportunity-set-v1":
        raise ValueError("unexpected widened-universe protocol id")
    ledger.admit(amendment_path)
    amendment = _read_json(amendment_path)
    if amendment.get("payload_sha256") != _payload_sha256(amendment):
        raise ValueError("6.1 protocol amendment payload hash is invalid")
    if amendment.get("protocol_id") != protocol.get("protocol_id"):
        raise ValueError("protocol amendment targets a different protocol")
    base = dict(amendment.get("base_protocol") or {})
    if (
        _path(str(base.get("path") or "")) != protocol_path.resolve()
        or str(base.get("file_sha256") or "") != sha256_file(protocol_path)
        or str(base.get("payload_sha256") or "") != protocol["payload_sha256"]
        or amendment.get("wide_return_evaluation_opened_before_freeze") is not False
    ):
        raise ValueError("protocol amendment does not bind the frozen base protocol")
    return {
        "protocol_id": protocol["protocol_id"],
        "base_payload_sha256": protocol["payload_sha256"],
        "amendment_id": amendment["amendment_id"],
        "amendment_payload_sha256": amendment["payload_sha256"],
    }


def _verify_release_closure(
    closure_path: Path,
    protocol_path: Path,
    amendment_path: Path,
) -> dict[str, Any]:
    return verify_preselection_closure(
        PROJECT_ROOT,
        closure_path=closure_path,
        protocol_path=protocol_path,
        amendment_path=amendment_path,
    )


def _ranking_rows(result: Any) -> list[pd.DataFrame]:
    rows: list[pd.DataFrame] = []
    for universe in result.universes:
        frame = universe.to_frame().rename(
            columns={
                "fixed_core_score": "score",
                "adv20_rmb": "universe_adv_20_rmb",
            }
        )
        frame.insert(0, "candidate_id", universe.name)
        frame.insert(1, "date", pd.Timestamp(result.signal_date))
        frame["universe_member_count"] = universe.member_count
        frame["finite_score_count"] = universe.finite_score_count
        frame["finite_score_coverage"] = universe.finite_score_coverage
        frame["base_eligible_count"] = result.base_eligible_count
        rows.append(frame)
    return rows


def build_stage(
    *,
    stage: str,
    candidates: Sequence[str],
    end_date: pd.Timestamp,
    config_path: Path,
    research_config_path: Path,
    protocol_path: Path,
    protocol_amendment_path: Path,
    release_closure_path: Path,
    work_root: Path,
    suspension_path: Path,
    suspension_metadata_path: Path,
    stock_st_checkpoint_path: Path,
    winner_freeze_payload_sha256: str | None = None,
) -> dict[str, Any]:
    """Build rankings, low-churn decisions, and sparse pricing through cutoff."""

    started = time.time()
    if config_path.resolve() != _path(FROZEN_IMPLEMENTATION_PATHS["data_config"]):
        raise ValueError("stage data config differs from the frozen implementation")
    if research_config_path.resolve() != _path(
        FROZEN_IMPLEMENTATION_PATHS["research_config"]
    ):
        raise ValueError("stage research config differs from the frozen implementation")
    candidate_ids = tuple(map(str, candidates))
    if (
        not candidate_ids
        or len(set(candidate_ids)) != len(candidate_ids)
        or CONTROL_ID not in candidate_ids
        or not set(candidate_ids).issubset(UNIVERSE_IDS)
    ):
        raise ValueError("stage candidates must be a unique frozen subset including control")
    paths = _stage_paths(work_root, stage)
    ledger = SourceLedger()
    config, layout = _runtime_layout(config_path)
    protocol = _stage_protocol(protocol_path, protocol_amendment_path, ledger)
    for implementation in (
        release_closure_path,
        protocol_amendment_path,
        _path(RUNTIME_PATH),
        *(
            _path(relative_path)
            for relative_path in FROZEN_IMPLEMENTATION_PATHS.values()
        ),
    ):
        ledger.admit(implementation)

    raw_checkpoint_path = layout.checkpoint_path.resolve()
    stock_st_checkpoint_path = stock_st_checkpoint_path.resolve()
    raw_checkpoint = _checkpoint(raw_checkpoint_path, ledger)
    calendar, calendar_inputs, calendar_contract = _load_official_calendar(
        layout,
        raw_checkpoint,
        ledger,
        start=DATA_START,
        end=end_date,
    )
    stock_st_checkpoint = _stock_st_cutoff_view(
        stock_st_checkpoint_path,
        ledger,
        layout=layout,
        stage=stage,
        end=end_date,
        calendar=calendar,
        calendar_contract=calendar_contract,
        winner_freeze_payload_sha256=winner_freeze_payload_sha256,
    )
    if ANCHOR not in calendar:
        raise ValueError("stage calendar lacks frozen anchor")

    master_audit = audit_security_master(layout)
    if master_audit.get("status") != "pass":
        raise ValueError(f"security master audit failed: {master_audit}")
    master = _filter_security_scope(load_security_master(layout))
    master_path = Path(master_audit["snapshot_path"]) / "part-000.parquet"
    master_manifest = Path(master_audit["snapshot_path"]) / "manifest.json"
    master_checkpoint = Path(master_audit["checkpoint_path"])
    for path in (master_path, master_manifest, master_checkpoint):
        ledger.admit(path)

    suspensions = _load_suspensions(
        suspension_path,
        suspension_metadata_path,
        ledger,
        start=DATA_START,
        end=end_date,
    )
    suspensions_by_date = {
        pd.Timestamp(date): group.copy()
        for date, group in suspensions.groupby("date", sort=False)
    }
    aliases = list((config.get("enrichment") or {}).get("security_code_aliases") or [])
    canonical_tickers = set(master["ts_code"].astype(str))
    vendor_tickers = {str(row["vendor_ts_code"]) for row in aliases}
    allowed_raw_tickers = canonical_tickers | vendor_tickers

    print(f"building {stage} rankings through {end_date.date()}", flush=True)
    builder = DailyOpportunitySetBuilder(
        calendar,
        master,
        aliases=aliases,
        universe_names=candidate_ids,
    )
    ranking_parts: list[pd.DataFrame] = []
    carried_suspension_rows: list[dict[str, Any]] = []
    inactive_stock_st_ignored_count = 0
    out_of_scope_bj_rows_ignored = {
        "daily": 0,
        "daily_basic": 0,
        "stock_st": 0,
        "suspensions": 0,
    }
    for index, date in enumerate(calendar):
        daily = _read_partition(
            raw_checkpoint,
            "daily",
            date,
            ("ts_code", "trade_date", "amount", "pct_chg"),
            ledger,
        )
        daily_basic = _read_partition(
            raw_checkpoint,
            "daily_basic",
            date,
            ("ts_code", "trade_date", "pe_ttm", "pb"),
            ledger,
        )
        stock_st = _read_partition(
            stock_st_checkpoint,
            "stock_st",
            date,
            ("ts_code", "trade_date", "name", "type", "type_name"),
            ledger,
        )
        daily, ignored = _restrict_partition_to_security_scope(
            daily,
            identifier="ts_code",
            allowed_tickers=allowed_raw_tickers,
            role="daily",
            date=date,
        )
        out_of_scope_bj_rows_ignored["daily"] += ignored
        daily_basic, ignored = _restrict_partition_to_security_scope(
            daily_basic,
            identifier="ts_code",
            allowed_tickers=allowed_raw_tickers,
            role="daily_basic",
            date=date,
        )
        out_of_scope_bj_rows_ignored["daily_basic"] += ignored
        stock_st, ignored = _restrict_partition_to_security_scope(
            stock_st,
            identifier="ts_code",
            allowed_tickers=allowed_raw_tickers,
            role="stock_st",
            date=date,
        )
        out_of_scope_bj_rows_ignored["stock_st"] += ignored
        suspension_day = suspensions_by_date.get(date)
        if suspension_day is not None:
            suspension_day, ignored = _restrict_partition_to_security_scope(
                suspension_day,
                identifier="ticker",
                allowed_tickers=allowed_raw_tickers,
                role="suspensions",
                date=date,
            )
            out_of_scope_bj_rows_ignored["suspensions"] += ignored
        result = builder.push_day(
            date,
            daily=daily,
            daily_basic=daily_basic,
            stock_st=stock_st,
            suspensions=suspension_day,
        )
        carried_suspension_rows.extend(
            {
                "date": pd.Timestamp(result.signal_date),
                "ticker": evidence.ticker,
                "provenance": "carried_prior_explicit_full_day_S",
                "source_suspend_date": pd.Timestamp(
                    evidence.source_suspend_date
                ),
                "official_session_age": evidence.official_session_age,
            }
            for evidence in result.carried_suspension_evidence
        )
        inactive_stock_st_ignored_count += result.inactive_stock_st_ignored_count
        if date >= ANCHOR:
            if not result.history_ready:
                raise ValueError("ranking emitted before exact ADV20 history")
            ranking_parts.extend(_ranking_rows(result))
        if index and index % 250 == 0:
            print(f"ranking sessions {index}/{len(calendar)}", flush=True)
    rankings = pd.concat(ranking_parts, ignore_index=True)
    rankings = audit_rankings(
        rankings, calendar, expected_universes=candidate_ids
    )
    coverage = {
        candidate_id: {
            "median": float(group.groupby("date")["finite_score_coverage"].first().median()),
            "q05": float(
                group.groupby("date")["finite_score_coverage"].first().quantile(
                    0.05, interpolation="linear"
                )
            ),
            "minimum_finite_score_count": int(
                group.groupby("date")["finite_score_count"].first().min()
            ),
        }
        for candidate_id, group in rankings.groupby("candidate_id", sort=True)
    }
    coverage_failures = {
        candidate_id: value
        for candidate_id, value in coverage.items()
        if value["median"] < 0.95
        or value["q05"] < 0.90
        or value["minimum_finite_score_count"] < 10
    }
    if coverage_failures:
        raise ValueError(f"finite-score data admission failed: {coverage_failures}")

    decisions = build_target_decisions(
        rankings, calendar, expected_universes=candidate_ids
    )
    targets = decisions_frame(decisions)
    carried_suspensions = pd.DataFrame(
        carried_suspension_rows,
        columns=(
            "date",
            "ticker",
            "provenance",
            "source_suspend_date",
            "official_session_age",
        ),
    )
    if not carried_suspensions.empty:
        carried_suspensions = carried_suspensions.sort_values(
            ["date", "ticker"], kind="mergesort"
        ).reset_index(drop=True)
        if carried_suspensions.duplicated(["date", "ticker"]).any():
            raise ValueError("carried-suspension provenance contains duplicate keys")
    target_union = set(targets["ticker"].astype(str))
    _write_parquet_atomic(paths["rankings"], rankings)
    _write_parquet_atomic(paths["targets"], targets)
    _write_parquet_atomic(paths["carried_suspensions"], carried_suspensions)
    _write_json_atomic(paths["decisions"], {"decisions": decisions})

    print(
        f"building {stage} sparse pricing for {len(target_union)} ever-targeted tickers",
        flush=True,
    )
    pricing_builder = SparsePricingBuilder(
        calendar, master, target_union, aliases=aliases
    )
    target_vendor_tickers = {
        str(row["vendor_ts_code"])
        for row in aliases
        if str(row["canonical_ts_code"]) in target_union
    }
    pricing_raw_tickers = target_union | target_vendor_tickers
    temporary_pricing = paths["pricing"].with_name(
        f".{paths['pricing'].name}.{os.getpid()}.tmp"
    )
    writer: pq.ParquetWriter | None = None
    pricing_rows = 0
    try:
        for index, date in enumerate(calendar):
            daily = _read_partition(
                raw_checkpoint,
                "daily",
                date,
                (
                    "ts_code",
                    "trade_date",
                    "open",
                    "high",
                    "low",
                    "pre_close",
                    "pct_chg",
                    "amount",
                ),
                ledger,
            )
            factors = _read_partition(
                raw_checkpoint,
                "adj_factor",
                date,
                ("ts_code", "trade_date", "adj_factor"),
                ledger,
            )
            stock_st = _read_partition(
                stock_st_checkpoint,
                "stock_st",
                date,
                ("ts_code", "trade_date", "name", "type", "type_name"),
                ledger,
            )
            daily = daily.loc[daily["ts_code"].astype(str).isin(pricing_raw_tickers)]
            factors = factors.loc[
                factors["ts_code"].astype(str).isin(pricing_raw_tickers)
            ]
            stock_st = stock_st.loc[
                stock_st["ts_code"].astype(str).isin(pricing_raw_tickers)
            ]
            suspension_day = suspensions_by_date.get(date)
            if suspension_day is not None:
                suspension_day = suspension_day.loc[
                    suspension_day["ticker"].astype(str).isin(pricing_raw_tickers)
                ]
            built = pricing_builder.push_day(
                date,
                daily=daily,
                adj_factor=factors,
                stock_st=stock_st,
                suspensions=suspension_day,
            )
            table = pa.Table.from_pandas(built.frame, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(
                    temporary_pricing,
                    table.schema,
                    compression="zstd",
                    use_dictionary=True,
                )
            writer.write_table(table)
            pricing_rows += len(built.frame)
            if index and index % 250 == 0:
                print(f"pricing sessions {index}/{len(calendar)}", flush=True)
        if writer is None:
            raise RuntimeError("sparse pricing writer received no sessions")
        writer.close()
        writer = None
        _replace_atomic_with_windows_retry(temporary_pricing, paths["pricing"])
    finally:
        if writer is not None:
            writer.close()
        temporary_pricing.unlink(missing_ok=True)

    ledger.verify_unchanged()
    source_payload = ledger.payload()
    _write_json_atomic(paths["source_files"], source_payload)
    artifacts = {
        name: _artifact(paths[name])
        for name in (
            "rankings",
            "targets",
            "carried_suspensions",
            "decisions",
            "pricing",
            "source_files",
        )
    }
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "kind": "factor_lab_wide_universe_stage_manifest",
        "stage": stage,
        "candidate_ids": list(candidate_ids),
        "status": "data_admission_passed",
        "protocol_id": protocol["protocol_id"],
        "protocol_payload_sha256": protocol["base_payload_sha256"],
        "protocol_amendment_id": protocol["amendment_id"],
        "protocol_amendment_payload_sha256": protocol[
            "amendment_payload_sha256"
        ],
        "physical_max_date": end_date.date().isoformat(),
        "physical_post_cutoff_market_rows": 0,
        "calendar_start": calendar[0].date().isoformat(),
        "calendar_end": calendar[-1].date().isoformat(),
        "official_session_count": len(calendar),
        "signal_session_count": int(rankings["date"].nunique()),
        "ranking_row_count": len(rankings),
        "target_row_count": len(targets),
        "ever_targeted_ticker_count": len(target_union),
        "sparse_pricing_row_count": pricing_rows,
        "finite_score_coverage": coverage,
        "carried_suspension_inference": {
            "provenance": "carried_prior_explicit_full_day_S",
            "ticker_day_count": len(carried_suspensions),
            "unique_ticker_count": int(
                carried_suspensions["ticker"].nunique()
                if not carried_suspensions.empty
                else 0
            ),
            "maximum_official_session_age": int(
                carried_suspensions["official_session_age"].max()
                if not carried_suspensions.empty
                else 0
            ),
            "same_day_raw_S_misreported_as_carry": False,
            "remaining_unexplained_missing_bar_count": 0,
        },
        "status_scope_diagnostics": {
            "inactive_stock_st_ignored_count": inactive_stock_st_ignored_count,
            "unknown_in_scope_security_count": 0,
            "out_of_scope_bj_rows_ignored": out_of_scope_bj_rows_ignored,
        },
        "calendar_inputs": calendar_inputs,
        "stock_st_cutoff_view": {
            "path": _portable(stock_st_checkpoint_path),
            "sha256": sha256_file(stock_st_checkpoint_path),
            "payload_sha256": stock_st_checkpoint["payload_sha256"],
            "source_checkpoint_sha256": stock_st_checkpoint[
                "source_checkpoint_sha256"
            ],
        },
        "security_master_audit": master_audit,
        "source_file_payload_sha256": source_payload["payload_sha256"],
        "source_file_count": source_payload["file_count"],
        "source_hashes_verified_before_and_after": True,
        "artifacts": artifacts,
        "git_commit": _git_text("rev-parse", "HEAD"),
        "elapsed_seconds": time.time() - started,
    }
    manifest["payload_sha256"] = _payload_sha256(manifest)
    _write_json_atomic(paths["manifest"], manifest)
    print(
        f"stage {stage} built payload={manifest['payload_sha256']} "
        f"seconds={manifest['elapsed_seconds']:.1f}",
        flush=True,
    )
    return manifest


def _load_stage(work_root: Path, stage: str) -> tuple[dict[str, Path], dict[str, Any]]:
    paths = _stage_paths(work_root, stage)
    manifest = _read_json(paths["manifest"])
    if manifest.get("payload_sha256") != _payload_sha256(manifest):
        raise ValueError(f"{stage} manifest payload hash mismatch")
    for name, identity in (manifest.get("artifacts") or {}).items():
        path = _path(identity["path"])
        if (
            path != paths[name].resolve()
            or not path.is_file()
            or path.stat().st_size != int(identity["size_bytes"])
            or sha256_file(path) != identity["sha256"]
        ):
            raise ValueError(f"{stage} artifact identity failed: {name}")
    return paths, manifest


def _portfolio_base(research_config_path: Path) -> dict[str, Any]:
    research = _read_json(research_config_path)
    costs = LongOnlyCostConfig(**dict(research["costs"]))
    return {
        "capital": 50_000_000.0,
        "holding_days": 10,
        "rebalance_every_days": 10,
        "periods_per_year": 25.2,
        "date_column": "date",
        "ticker_column": "ticker",
        "open_column": "open_adj",
        "price_basis": "adjusted_total_return",
        "price_source": "6.1_full_market_raw_open_times_contemporaneous_adj_factor",
        "lot_size": 0,
        "adv_column": "adv_20",
        "volatility_column": "volatility_20",
        "eligible_columns": ("eligible", "universe_member"),
        "limit_up_column": "is_one_price_limit_up",
        "limit_down_column": "is_one_price_limit_down",
        "max_stale_position_age_days": 21,
        "max_adv_participation": 0.05,
        "costs": costs,
    }


def _phase_payload_sha256(phase: Mapping[str, Any]) -> str:
    return canonical_sha256(dict(phase))


def _phase_trace_sha256(result: Any, bounds: PhaseBounds) -> dict[str, str]:
    """Hash the complete exact traces attributable to one frozen phase."""

    periods = [
        dict(row)
        for row in result.periods
        if pd.Timestamp(row["signal_date"]).normalize() >= bounds.start
        and pd.Timestamp(row["end_date"]).normalize() <= bounds.end
    ]
    start_dates = {str(row["start_date"]) for row in periods}
    trades = [
        dict(row)
        for row in result.trades
        if str(row.get("date")) in start_dates
    ]
    if periods:
        first_sequence = int(periods[0]["account_nav_path_start_sequence"])
        last_sequence = int(periods[-1]["account_nav_path_end_sequence"])
        daily_nav = [
            dict(row)
            for row in result.account_nav_path
            if first_sequence <= int(row["sequence"]) <= last_sequence
        ]
    else:
        daily_nav = []
    return {
        "period_trace_sha256": canonical_sha256(periods),
        "trade_trace_sha256": canonical_sha256(trades),
        "daily_nav_sha256": canonical_sha256(daily_nav),
    }


def _compact_exact_result(
    candidate_id: str,
    offset: int,
    result: Any,
    phase_bounds: Mapping[str, PhaseBounds],
) -> dict[str, Any]:
    phases = {}
    for name, bounds in phase_bounds.items():
        phase = summarize_phase(result, bounds)
        phase["exact_trace_sha256"] = _phase_trace_sha256(result, bounds)
        phase["payload_sha256"] = _phase_payload_sha256(phase)
        phases[name] = phase
    return {
        "candidate_id": candidate_id,
        "offset": offset,
        "status": result.status,
        "observations": result.observations,
        "period_trace_sha256": canonical_sha256(result.periods),
        "trade_trace_sha256": canonical_sha256(result.trades),
        "daily_nav_sha256": canonical_sha256(result.account_nav_path),
        "phases": phases,
    }


def _evaluation_phase_bounds(physical_max_date: Any) -> dict[str, PhaseBounds]:
    """Return only phases that can physically exist in the staged artifact."""

    physical_end = pd.Timestamp(physical_max_date).normalize()
    if physical_end < ANCHOR:
        raise ValueError("stage physical cutoff precedes the frozen anchor")
    bounds = {
        "train": PhaseBounds.from_values(ANCHOR, min(TRAIN_END, physical_end)),
    }
    if physical_end >= VALIDATION_START:
        bounds["validation"] = PhaseBounds.from_values(
            VALIDATION_START, min(VALIDATION_END, physical_end)
        )
    if physical_end >= AUDIT_START:
        bounds["audit"] = PhaseBounds.from_values(AUDIT_START, physical_end)
    return bounds


def evaluate_stage(
    *,
    stage: str,
    candidates: Sequence[str],
    work_root: Path,
    research_config_path: Path,
) -> dict[str, list[dict[str, Any]]]:
    paths, manifest = _load_stage(work_root, stage)
    rankings = pd.read_parquet(paths["rankings"])
    decisions_payload = _read_json(paths["decisions"])
    decisions = decisions_payload.get("decisions")
    if not isinstance(decisions, list):
        raise ValueError("stage decisions artifact is malformed")
    phase_bounds = _evaluation_phase_bounds(manifest["physical_max_date"])
    portfolio_base = _portfolio_base(research_config_path)
    output: dict[str, list[dict[str, Any]]] = {}
    run_root = paths["root"] / "exact-runs"
    run_root.mkdir(parents=True, exist_ok=True)
    for candidate_id in candidates:
        candidate_rankings = rankings.loc[
            rankings["candidate_id"].astype(str).eq(candidate_id)
        ].copy()
        if candidate_rankings.empty:
            raise ValueError(f"stage lacks candidate rankings: {candidate_id}")
        rows: list[dict[str, Any]] = []
        for offset in range(10):
            started = time.time()
            targets, audits = target_maps(decisions, candidate_id, offset)
            target_tickers = {
                ticker for weights in targets.values() for ticker in weights
            }
            pricing = pd.read_parquet(
                paths["pricing"],
                filters=[("ticker", "in", sorted({"__CALENDAR__", *target_tickers}))],
            )
            signal = candidate_rankings.loc[
                candidate_rankings["ticker"].astype(str).isin(target_tickers),
                ["date", "ticker", "score"],
            ].copy()
            config = LongOnlyPortfolioConfig(
                **portfolio_base,
                position_count=10,
                retention_buffer=15,
                target_weight=0.1,
                rebalance_offset_days=offset,
            )
            result = evaluate_long_only_portfolio(
                signal,
                "score",
                config,
                pricing_frame=pricing,
                target_weights_by_date=targets,
                optimization_audit_by_date=audits,
                require_optimized_targets=True,
            )
            if result.status != "ok":
                raise RuntimeError(
                    f"{stage}/{candidate_id}/offset{offset}: {result.reason}"
                )
            compact = _compact_exact_result(
                candidate_id, offset, result, phase_bounds
            )
            rows.append(compact)
            run_path = run_root / f"{candidate_id}-offset{offset}.json"
            full_payload = result.to_dict()
            full_payload["payload_sha256"] = canonical_sha256(full_payload)
            _write_json_atomic(run_path, full_payload)
            active_phase = "audit" if stage == "audit" else (
                "validation" if stage == "validation" else "train"
            )
            phase = compact["phases"][active_phase]
            print(
                f"exact {stage} {candidate_id} offset={offset} "
                f"obs={phase['observations']} cagr={phase['net_cagr']:.6f} "
                f"seconds={time.time() - started:.1f}",
                flush=True,
            )
            del result, pricing, signal
            gc.collect()
        output[candidate_id] = rows
    evaluation = {
        "schema_version": 1,
        "stage": stage,
        "manifest_payload_sha256": manifest["payload_sha256"],
        "candidates": list(candidates),
        "results": output,
    }
    evaluation["payload_sha256"] = _payload_sha256(evaluation)
    _write_json_atomic(paths["evaluation"], evaluation)
    return output


def _phases(
    results: Mapping[str, Sequence[Mapping[str, Any]]],
    candidate_id: str,
    phase: str,
) -> list[dict[str, Any]]:
    rows = list(results[candidate_id])
    if [int(row["offset"]) for row in rows] != list(range(10)):
        raise ValueError("exact results do not contain ordered offsets 0..9")
    return [dict(row["phases"][phase]) for row in rows]


def _phase_replay_digests(
    results: Mapping[str, Sequence[Mapping[str, Any]]], phase: str
) -> dict[str, list[str]]:
    return {
        candidate_id: [
            str(row["phases"][phase]["payload_sha256"])
            for row in rows
        ]
        for candidate_id, rows in results.items()
    }


def _strip_gate(gate: Mapping[str, Any]) -> dict[str, Any]:
    value = json.loads(json.dumps(gate, allow_nan=False, default=str))
    for phase in value.get("paired_phases", []):
        for key in ("signal_dates", "start_dates", "outcome_end_dates", "period_returns"):
            phase.pop(key, None)
    return value


def run_selection(args: argparse.Namespace) -> dict[str, Any]:
    if args.freeze_output.resolve() != _path(WINNER_FREEZE):
        raise ValueError("selection freeze path differs from the frozen repository path")
    if args.freeze_output.exists():
        raise FileExistsError("selection freeze is create-only")
    if _git_text("status", "--porcelain"):
        raise RuntimeError("selection requires a clean committed implementation")
    start_head = _git_text("rev-parse", "HEAD")
    closure = _verify_release_closure(
        args.release_closure, args.protocol, args.protocol_amendment
    )
    verify_active_runtime(PROJECT_ROOT)
    train_manifest = build_stage(
        stage="train",
        candidates=UNIVERSE_IDS,
        end_date=TRAIN_END,
        config_path=args.config,
        research_config_path=args.research_config,
        protocol_path=args.protocol,
        protocol_amendment_path=args.protocol_amendment,
        release_closure_path=args.release_closure,
        work_root=args.work_root,
        suspension_path=args.train_suspensions,
        suspension_metadata_path=args.train_suspension_metadata,
        stock_st_checkpoint_path=args.train_stock_st_checkpoint,
    )
    train_results = evaluate_stage(
        stage="train",
        candidates=UNIVERSE_IDS,
        work_root=args.work_root,
        research_config_path=args.research_config,
    )
    train_gates = {
        candidate_id: candidate_gate(
            _phases(train_results, candidate_id, "train"),
            _phases(train_results, CONTROL_ID, "train"),
        )
        for candidate_id in CHALLENGER_IDS
    }
    train_passers = [
        candidate_id for candidate_id in CHALLENGER_IDS if train_gates[candidate_id]["passed"]
    ]
    validation_manifest = None
    validation_results: dict[str, list[dict[str, Any]]] = {}
    validation_gates: dict[str, dict[str, Any]] = {}
    turnover: dict[str, float] = {}
    winner = None
    if train_passers:
        validation_manifest = build_stage(
            stage="validation",
            candidates=(CONTROL_ID, *train_passers),
            end_date=VALIDATION_END,
            config_path=args.config,
            research_config_path=args.research_config,
            protocol_path=args.protocol,
            protocol_amendment_path=args.protocol_amendment,
            release_closure_path=args.release_closure,
            work_root=args.work_root,
            suspension_path=args.suspensions,
            suspension_metadata_path=args.suspension_metadata,
            stock_st_checkpoint_path=args.stock_st_checkpoint,
        )
        validation_results = evaluate_stage(
            stage="validation",
            candidates=(CONTROL_ID, *train_passers),
            work_root=args.work_root,
            research_config_path=args.research_config,
        )
        train_replay = _phase_replay_digests(validation_results, "train")
        original_train = _phase_replay_digests(
            {key: train_results[key] for key in validation_results}, "train"
        )
        if train_replay != original_train:
            raise RuntimeError("validation build does not exactly replay train evidence")
        validation_gates = {
            candidate_id: candidate_gate(
                _phases(validation_results, candidate_id, "validation"),
                _phases(validation_results, CONTROL_ID, "validation"),
            )
            for candidate_id in train_passers
        }
        turnover = {
            candidate_id: float(
                np.median(
                    [
                        float(row["phases"]["validation"]["mean_turnover"])
                        for row in validation_results[candidate_id]
                    ]
                )
            )
            for candidate_id in train_passers
        }
        winner = select_winner(
            train_gates,
            validation_gates,
            turnover_by_candidate=turnover,
        )

    freeze: dict[str, Any] = {
        "schema_version": 1,
        "kind": "factor_lab_6_1_winner_freeze",
        "status": "selected_definition_frozen" if winner else "selected_null_frozen",
        "protocol_payload_sha256": train_manifest["protocol_payload_sha256"],
        "protocol_amendment_payload_sha256": train_manifest[
            "protocol_amendment_payload_sha256"
        ],
        "implementation_closure_payload_sha256": closure["payload_sha256"],
        "selected_candidate_id": winner,
        "runner_up_fallback_after_audit_fail": False,
        "train_passers": train_passers,
        "train_gates": {
            key: _strip_gate(value) for key, value in train_gates.items()
        },
        "validation_gates": {
            key: _strip_gate(value) for key, value in validation_gates.items()
        },
        "turnover_by_candidate": turnover,
        "train_manifest_payload_sha256": train_manifest["payload_sha256"],
        "validation_manifest_payload_sha256": (
            validation_manifest["payload_sha256"] if validation_manifest else None
        ),
        "selection_status_sources": {
            "train": {
                "suspensions": _artifact(args.train_suspensions),
                "suspension_metadata": _artifact(args.train_suspension_metadata),
                "stock_st_cutoff_view": _artifact(args.train_stock_st_checkpoint),
            },
            "validation": (
                {
                    "suspensions": _artifact(args.suspensions),
                    "suspension_metadata": _artifact(args.suspension_metadata),
                    "stock_st_cutoff_view": _artifact(args.stock_st_checkpoint),
                }
                if validation_manifest is not None
                else None
            ),
        },
        "train_phase_replay_sha256": _phase_replay_digests(train_results, "train"),
        "validation_phase_replay_sha256": (
            _phase_replay_digests(validation_results, "validation")
            if validation_results
            else {}
        ),
        "selected_definition": (
            _selected_definition(winner)
            if winner
            else None
        ),
        "audit_market_outcomes_opened": False,
        "selection_execution_commit": start_head,
    }
    freeze["payload_sha256"] = _payload_sha256(freeze)
    if _git_text("rev-parse", "HEAD") != start_head or _git_text(
        "status", "--porcelain"
    ):
        raise RuntimeError("tracked implementation changed during selection")
    _verify_release_closure(
        args.release_closure, args.protocol, args.protocol_amendment
    )
    verify_active_runtime(PROJECT_ROOT)
    _write_json_create_only(args.freeze_output, freeze)
    print(
        f"winner freeze selected={winner} payload={freeze['payload_sha256']}",
        flush=True,
    )
    return freeze


def run_audit(args: argparse.Namespace) -> dict[str, Any]:
    if args.freeze.resolve() != _path(WINNER_FREEZE):
        raise ValueError("audit freeze path differs from the frozen repository path")
    if args.audit_output.resolve() != _path(AUDIT_EVIDENCE_PATH):
        raise ValueError("audit result path differs from the frozen repository path")
    if args.audit_output.exists():
        raise FileExistsError("historical audit result is create-only")
    if _git_text("status", "--porcelain"):
        raise RuntimeError("audit requires a clean committed winner freeze")
    start_head = _git_text("rev-parse", "HEAD")
    closure = _verify_release_closure(
        args.release_closure, args.protocol, args.protocol_amendment
    )
    verify_active_runtime(PROJECT_ROOT)
    freeze = verify_winner_freeze(
        PROJECT_ROOT,
        preselection_closure=closure,
        freeze_path=args.freeze,
    )
    winner = freeze.get("selected_candidate_id")
    if not winner:
        raise ValueError("audit is forbidden because selection froze null")
    if winner not in CHALLENGER_IDS:
        raise ValueError("winner freeze contains an unknown challenger")
    if (
        freeze.get("status") != "selected_definition_frozen"
        or freeze.get("audit_market_outcomes_opened") is not False
        or not _selected_definition_matches(
            freeze.get("selected_definition"), str(winner)
        )
        or freeze.get("protocol_payload_sha256")
        != _read_json(args.protocol).get("payload_sha256")
        or freeze.get("protocol_amendment_payload_sha256")
        != _read_json(args.protocol_amendment).get("payload_sha256")
        or freeze.get("implementation_closure_payload_sha256")
        != closure.get("payload_sha256")
    ):
        raise ValueError("winner freeze does not bind the current frozen definition")
    frozen_status_paths = {
        _path(identity["path"])
        for stage_sources in (freeze.get("selection_status_sources") or {}).values()
        if isinstance(stage_sources, Mapping)
        for identity in stage_sources.values()
        if isinstance(identity, Mapping) and identity.get("path")
    }
    for audit_path in (
        args.suspensions,
        args.suspension_metadata,
        args.stock_st_checkpoint,
    ):
        if any(_same_path_or_file(audit_path, frozen) for frozen in frozen_status_paths):
            raise ValueError("audit reuses a frozen train/validation status source")
    freeze_commit = str(freeze.get("selection_execution_commit") or "")
    if not freeze_commit:
        raise ValueError("winner freeze lacks its implementation git commit")
    ancestor = subprocess.run(
        ["git", "merge-base", "--is-ancestor", freeze_commit, "HEAD"],
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if ancestor.returncode != 0:
        raise ValueError("winner-freeze implementation commit is not an ancestor")
    historical_closure = subprocess.run(
        [
            "git",
            "show",
            f"{freeze_commit}:{_portable(args.release_closure)}",
        ],
        cwd=PROJECT_ROOT,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ).stdout
    historical_payload = json.loads(historical_closure)
    if (
        not isinstance(historical_payload, dict)
        or _payload_sha256(historical_payload)
        != freeze.get("implementation_closure_payload_sha256")
    ):
        raise ValueError("winner freeze git commit lacks the bound implementation closure")
    audit_end = pd.Timestamp(args.audit_end).normalize()
    if audit_end != AUDIT_END:
        raise ValueError("audit_end differs from the pre-selection frozen cutoff")
    manifest = build_stage(
        stage="audit",
        candidates=(CONTROL_ID, str(winner)),
        end_date=audit_end,
        config_path=args.config,
        research_config_path=args.research_config,
        protocol_path=args.protocol,
        protocol_amendment_path=args.protocol_amendment,
        release_closure_path=args.release_closure,
        work_root=args.work_root,
        suspension_path=args.suspensions,
        suspension_metadata_path=args.suspension_metadata,
        stock_st_checkpoint_path=args.stock_st_checkpoint,
        winner_freeze_payload_sha256=str(freeze["payload_sha256"]),
    )
    results = evaluate_stage(
        stage="audit",
        candidates=(CONTROL_ID, str(winner)),
        work_root=args.work_root,
        research_config_path=args.research_config,
    )
    for phase, frozen_key in (
        ("train", "train_phase_replay_sha256"),
        ("validation", "validation_phase_replay_sha256"),
    ):
        actual = _phase_replay_digests(results, phase)
        frozen = dict(freeze.get(frozen_key) or {})
        expected = {key: frozen[key] for key in actual}
        if actual != expected:
            raise RuntimeError(f"audit build does not replay frozen {phase} evidence")
    gate = candidate_gate(
        _phases(results, str(winner), "audit"),
        _phases(results, CONTROL_ID, "audit"),
        minimum_observations=25,
    )
    evidence: dict[str, Any] = {
        "schema_version": 1,
        "kind": "factor_lab_6_1_historical_audit",
        "status": "historical_holdout_passed_requires_fresh_future"
        if gate["passed"]
        else "audit_falsified",
        "selected_candidate_id": winner,
        "winner_freeze_payload_sha256": freeze["payload_sha256"],
        "audit_manifest_payload_sha256": manifest["payload_sha256"],
        "audit_end": audit_end.date().isoformat(),
        "gate": _strip_gate(gate),
        "runner_up_fallback_used": False,
        "profit_claim_allowed": False,
        "audit_execution_commit": start_head,
    }
    evidence["payload_sha256"] = _payload_sha256(evidence)
    if _git_text("rev-parse", "HEAD") != start_head or _git_text(
        "status", "--porcelain"
    ):
        raise RuntimeError("tracked implementation changed during historical audit")
    _verify_release_closure(
        args.release_closure, args.protocol, args.protocol_amendment
    )
    verify_active_runtime(PROJECT_ROOT)
    verify_winner_freeze(
        PROJECT_ROOT,
        preselection_closure=closure,
        freeze_path=args.freeze,
    )
    _write_json_create_only(args.audit_output, evidence)
    print(
        f"audit passed={gate['passed']} payload={evidence['payload_sha256']}",
        flush=True,
    )
    return evidence


def run_finalize(args: argparse.Namespace) -> dict[str, Any]:
    """Create the unique tracked terminal result after selection or audit."""

    if args.result_output.resolve() != _path(RELEASE_RESULT_PATH):
        raise ValueError("terminal result path differs from the frozen repository path")
    if args.result_output.exists():
        raise FileExistsError("terminal result is create-only")
    if _git_text("status", "--porcelain"):
        raise RuntimeError("finalize requires clean committed terminal evidence")
    start_head = _git_text("rev-parse", "HEAD")
    closure = _verify_release_closure(
        args.release_closure, args.protocol, args.protocol_amendment
    )
    freeze_path = _path(WINNER_FREEZE)
    freeze = verify_winner_freeze(
        PROJECT_ROOT,
        preselection_closure=closure,
        freeze_path=freeze_path,
    )
    winner = freeze.get("selected_candidate_id")
    freeze_binding = _terminal_binding(
        freeze_path, expected_path=WINNER_FREEZE_PATH
    )
    if winner is None:
        if _path(AUDIT_EVIDENCE_PATH).exists():
            raise ValueError("null selection forbids a historical-audit artifact")
        status = "selection_falsified_no_candidate"
        audit_status = "not_opened"
        audit_binding = None
        claim = {
            "historical_evidence_class": "pre_registered_selection_falsified",
            "profit_claim_allowed": False,
            "fresh_future_evidence_required": True,
        }
    else:
        audit_path = _path(AUDIT_EVIDENCE_PATH)
        audit = verify_historical_audit(
            PROJECT_ROOT,
            preselection_closure=closure,
            winner_freeze=freeze,
            audit_path=audit_path,
        )
        status = str(audit["status"])
        audit_status = status
        audit_binding = _terminal_binding(
            audit_path, expected_path=AUDIT_EVIDENCE_PATH
        )
        claim = {
            "historical_evidence_class": status,
            "profit_claim_allowed": False,
            "fresh_future_evidence_required": True,
        }
    result: dict[str, Any] = {
        "schema_version": 1,
        "kind": "factor_lab_6_1_release_result",
        "release": "6.1",
        "status": status,
        "preselection_closure_payload_sha256": closure["payload_sha256"],
        "winner_freeze": freeze_binding,
        "selected_candidate_id": winner,
        "audit_status": audit_status,
        "audit": audit_binding,
        "runner_up_fallback_used": False,
        "claim_contract": claim,
    }
    result["payload_sha256"] = _payload_sha256(result)
    if _git_text("rev-parse", "HEAD") != start_head or _git_text(
        "status", "--porcelain"
    ):
        raise RuntimeError("tracked terminal evidence changed during finalize")
    _write_json_create_only(args.result_output, result)
    print(
        f"terminal result status={status} payload={result['payload_sha256']}",
        flush=True,
    )
    return result


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode", required=True, choices=("selection", "audit", "finalize")
    )
    parser.add_argument("--config", type=_path, default=_path("configs/data.json"))
    parser.add_argument(
        "--research-config", type=_path, default=_path("configs/research.json")
    )
    parser.add_argument(
        "--protocol", type=_path, default=_path("protocols/6.1-wide-universe.json")
    )
    parser.add_argument(
        "--protocol-amendment",
        type=_path,
        default=_path("protocols/6.1-wide-universe-amendment-1.json"),
    )
    parser.add_argument(
        "--release-closure",
        type=_path,
        default=_path("protocols/6.1-release.json"),
    )
    parser.add_argument(
        "--work-root", type=_path, default=_path("runtime/data/wide-universe")
    )
    parser.add_argument(
        "--train-suspensions",
        type=_path,
    )
    parser.add_argument(
        "--train-suspension-metadata",
        type=_path,
    )
    parser.add_argument(
        "--train-stock-st-checkpoint",
        type=_path,
    )
    parser.add_argument(
        "--suspensions",
        type=_path,
    )
    parser.add_argument(
        "--suspension-metadata",
        type=_path,
    )
    parser.add_argument(
        "--stock-st-checkpoint",
        type=_path,
    )
    parser.add_argument(
        "--freeze-output",
        type=_path,
        default=_path(WINNER_FREEZE),
    )
    parser.add_argument("--freeze", type=_path)
    parser.add_argument("--audit-end")
    parser.add_argument(
        "--audit-output",
        type=_path,
        default=_path(AUDIT_EVIDENCE_PATH),
    )
    parser.add_argument(
        "--result-output",
        type=_path,
        default=_path(RELEASE_RESULT_PATH),
    )
    args = parser.parse_args(argv)
    if args.mode == "audit" and (args.freeze is None or args.audit_end is None):
        parser.error("audit mode requires --freeze and --audit-end")
    if args.mode == "selection":
        if args.freeze_output.resolve() != _path(WINNER_FREEZE):
            parser.error("selection freeze must use the frozen repository path")
        args.train_suspensions = (
            args.train_suspensions or _path(TRAIN_SUSPENSIONS)
        )
        args.train_suspension_metadata = (
            args.train_suspension_metadata or _path(TRAIN_SUSPENSION_METADATA)
        )
        args.train_stock_st_checkpoint = (
            args.train_stock_st_checkpoint or _path(TRAIN_ST_CHECKPOINT)
        )
        args.suspensions = args.suspensions or _path(SELECTION_SUSPENSIONS)
        args.suspension_metadata = (
            args.suspension_metadata or _path(SELECTION_SUSPENSION_METADATA)
        )
        args.stock_st_checkpoint = (
            args.stock_st_checkpoint or _path(SELECTION_ST_CHECKPOINT)
        )
        for train_path, validation_path in (
            (args.train_suspensions, args.suspensions),
            (args.train_suspension_metadata, args.suspension_metadata),
            (args.train_stock_st_checkpoint, args.stock_st_checkpoint),
        ):
            if _same_path_or_file(train_path, validation_path):
                parser.error("train and validation status artifacts must be distinct")
    elif args.mode == "audit":
        if args.freeze.resolve() != _path(WINNER_FREEZE):
            parser.error("audit must use the frozen repository winner freeze")
        if args.audit_output.resolve() != _path(AUDIT_EVIDENCE_PATH):
            parser.error("audit must use the frozen repository result path")
        args.train_suspensions = None
        args.train_suspension_metadata = None
        args.train_stock_st_checkpoint = None
        args.suspensions = args.suspensions or _path(AUDIT_SUSPENSIONS)
        args.suspension_metadata = (
            args.suspension_metadata or _path(AUDIT_SUSPENSION_METADATA)
        )
        args.stock_st_checkpoint = (
            args.stock_st_checkpoint or _path(AUDIT_ST_CHECKPOINT)
        )
        selection_paths = {
            _path(TRAIN_SUSPENSIONS),
            _path(TRAIN_SUSPENSION_METADATA),
            _path(TRAIN_ST_CHECKPOINT),
            _path(SELECTION_SUSPENSIONS),
            _path(SELECTION_SUSPENSION_METADATA),
            _path(SELECTION_ST_CHECKPOINT),
        }
        if {
            args.suspensions,
            args.suspension_metadata,
            args.stock_st_checkpoint,
        } & selection_paths:
            parser.error("audit mode forbids selection status artifacts")
    else:
        if args.result_output.resolve() != _path(RELEASE_RESULT_PATH):
            parser.error("finalize must use the frozen repository result path")
        args.train_suspensions = None
        args.train_suspension_metadata = None
        args.train_stock_st_checkpoint = None
        args.suspensions = None
        args.suspension_metadata = None
        args.stock_st_checkpoint = None
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.mode == "selection":
        run_selection(args)
    elif args.mode == "audit":
        run_audit(args)
    else:
        run_finalize(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

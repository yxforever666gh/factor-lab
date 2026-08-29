"""Source-backed execution snapshots for the prospective 5.0 ledger.

This module is the I/O trust boundary around :mod:`factor_lab.prospective_execution`.
The evaluator in that module is deliberately pure; the builder here proves that
its calendar, adjusted opens, causal execution inputs, benchmark roster, and
event flags can be rebuilt from checkpointed project-local bytes.

The public loader never accepts a merely self-consistent ``snapshot.json``.  It
re-runs this builder against the bound decision input and source checkpoints and
requires byte-identical source and execution contracts.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time as wall_time, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
from typing import Any, Mapping, Sequence
from uuid import uuid4
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from ..prospective_execution import (
    ExecutionObservation,
    ExecutionSnapshot,
    HOLDING_DAYS,
    ProspectiveExecutionError,
    SleeveAccountState,
)
from ..prospective_targets import GenerationResult, InputSnapshot
from .catalog import DEFAULT_CONFIG_PATH, RuntimeLayout, load_data_config, sha256_file
from .prospective import (
    IMMUTABLE_SOURCE_RELATIVE_ROOT,
    _capture_immutable_artifact,
    _resolve_immutable_artifact,
    _write_verified,
    PROSPECTIVE_RELATIVE_ROOT,
    ProspectiveInputSnapshot,
    load_prospective_input_snapshot,
)
from .sources import DATASET_FIELDS, _call, _configured_tushare_client
from .suspensions import SUSPENSION_PAGE_SIZE, audit_suspensions_snapshot


SCHEMA_VERSION = 1
KIND = "prospective_execution_sources"
EXECUTION_RELATIVE_ROOT = PROSPECTIVE_RELATIVE_ROOT / "executions"
MINIMUM_BENCHMARK_COVERAGE_PPM = 950_000
SUSPENSION_FULL_START_DATE = "2017-01-01"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SUSPENSION_INTERVAL_RE = re.compile(
    r"(?P<start_hour>\d{1,2}):(?P<start_minute>\d{2})"
    r"\s*[-~至到]\s*"
    r"(?P<end_hour>\d{1,2}):(?P<end_minute>\d{2})"
)
_MARKET_OPEN_MINUTE = 9 * 60 + 30
_OFFICIAL_DELIST_FIELDS = ("ts_code", "list_status", "delist_date")
_OFFICIAL_DELIST_QUERY = {
    "list_status": "D",
    "fields": ",".join(_OFFICIAL_DELIST_FIELDS),
}
_OFFICIAL_DELIST_RESULT_LIMIT = 6_000
_OFFICIAL_DELIST_MINIMUM_ROWS = 200
_EXECUTION_SOURCE_KEYS = frozenset(
    {
        "schema_version",
        "kind",
        "protocol_release",
        "generation_result_sha256",
        "source_data_snapshot_sha256",
        "target_input_snapshot_sha256",
        "previous_account_state_sha256",
        "benchmark_tickers_sha256",
        "benchmark_coverage",
        "selected_market_max_date",
        "raw_checkpoint_path",
        "decision_input",
        "calendar",
        "raw_partitions",
        "suspensions",
        "delists",
    }
)


class ProspectiveExecutionDataError(ValueError):
    """Raised when execution evidence cannot be rebuilt without guessing."""


@dataclass(frozen=True)
class ProspectiveExecutionDataSnapshot:
    """A persisted pure snapshot plus its independently rebuildable sources."""

    snapshot: ExecutionSnapshot
    directory: Path
    snapshot_path: Path
    sources_path: Path
    source_contract: Mapping[str, Any]

    @property
    def snapshot_sha256(self) -> str:
        return self.snapshot.snapshot_sha256

    @property
    def execution_source_sha256(self) -> str:
        return self.snapshot.execution_source_sha256


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _require_sha(value: Any, *, label: str) -> str:
    result = str(value or "")
    if not _SHA256_RE.fullmatch(result):
        raise ProspectiveExecutionDataError(f"{label} must be a lowercase SHA-256")
    return result


def _date(value: Any, *, label: str) -> str:
    try:
        result = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise ProspectiveExecutionDataError(f"invalid {label}: {value!r}") from exc
    if pd.isna(result) or result.tzinfo is not None or result != result.normalize():
        raise ProspectiveExecutionDataError(f"{label} must be a timezone-free calendar date")
    return result.date().isoformat()


def _utc(value: Any, *, label: str) -> pd.Timestamp:
    try:
        result = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise ProspectiveExecutionDataError(f"invalid {label}: {value!r}") from exc
    if pd.isna(result) or result.tzinfo is None:
        raise ProspectiveExecutionDataError(f"{label} must include a timezone")
    return result.tz_convert("UTC")


def _utc_text(value: Any, *, label: str) -> str:
    resolved = _utc(value, label=label)
    if resolved != resolved.floor("s"):
        resolved = resolved.ceil("s")
    return resolved.strftime("%Y-%m-%dT%H:%M:%SZ")


def _relative(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError as exc:
        raise ProspectiveExecutionDataError(f"source escapes project root: {path}") from exc


def _resolved_source_path(value: Any, *, expected: Path, root: Path, label: str) -> Path:
    candidate = Path(str(value or expected))
    if not candidate.is_absolute():
        candidate = root / candidate
    candidate = candidate.resolve()
    if candidate != expected.resolve():
        raise ProspectiveExecutionDataError(f"{label} path is not canonical")
    _relative(candidate, root)
    return candidate


def _load_json(path: Path, *, label: str) -> dict[str, Any]:
    if not path.is_file():
        raise ProspectiveExecutionDataError(f"missing {label}: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ProspectiveExecutionDataError(f"unreadable {label}: {path}") from exc
    if not isinstance(value, dict):
        raise ProspectiveExecutionDataError(f"{label} must be a JSON object")
    return value


def _write_create_only(path: Path, payload: bytes) -> None:
    """Durably publish one immutable artifact without exposing partial bytes.

    The temporary file lives beside the destination so the final hard-link is
    both atomic and create-only on Windows and POSIX.  A concurrent winner is
    accepted only when its bytes are identical.  We deliberately do not use
    ``os.replace``: replacing an existing evidence file would violate the
    immutable execution-store contract.
    """

    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() or path.is_symlink():
        if not path.is_file() or path.is_symlink() or path.read_bytes() != payload:
            raise ProspectiveExecutionDataError(f"create-only artifact differs: {path}")
        return
    temporary = path.parent / (
        f".{path.name}.pending-{os.getpid()}-{uuid4().hex}.tmp"
    )
    binary_flag = getattr(os, "O_BINARY", 0)
    descriptor = os.open(
        temporary,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | binary_flag,
        0o600,
    )
    try:
        try:
            remaining = memoryview(payload)
            while remaining:
                written = os.write(descriptor, remaining)
                if written <= 0:
                    raise OSError("short write while publishing execution evidence")
                remaining = remaining[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        try:
            os.link(temporary, path)
        except FileExistsError:
            if (
                not path.is_file()
                or path.is_symlink()
                or path.read_bytes() != payload
            ):
                raise ProspectiveExecutionDataError(
                    f"create-only artifact raced with different bytes: {path}"
                )
        except OSError as exc:
            raise ProspectiveExecutionDataError(
                "filesystem cannot atomically publish create-only execution evidence"
            ) from exc
        if not path.is_file() or path.is_symlink() or path.read_bytes() != payload:
            raise ProspectiveExecutionDataError(
                f"create-only artifact winner bytes differ: {path}"
            )
        if os.name != "nt":
            directory_descriptor = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _generation(value: GenerationResult | Mapping[str, Any]) -> GenerationResult:
    if isinstance(value, GenerationResult):
        return value
    if isinstance(value, Mapping):
        try:
            return GenerationResult.from_mapping(value)
        except Exception as exc:
            raise ProspectiveExecutionDataError("invalid generation result") from exc
    raise ProspectiveExecutionDataError("generation_result must be a strict GenerationResult")


def _account_state(
    value: SleeveAccountState | Mapping[str, Any] | None,
) -> SleeveAccountState | None:
    if value is None or isinstance(value, SleeveAccountState):
        return value
    if isinstance(value, Mapping):
        try:
            return SleeveAccountState.from_mapping(value)
        except Exception as exc:
            raise ProspectiveExecutionDataError("invalid previous account state") from exc
    raise ProspectiveExecutionDataError("previous_account_state must be a strict state")


def _trade_deadline(trade_date: str) -> str:
    local = datetime.combine(
        datetime.strptime(trade_date, "%Y-%m-%d").date(),
        wall_time(hour=9, minute=15),
        tzinfo=ZoneInfo("Asia/Shanghai"),
    )
    return local.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _signal_close(signal_date: str) -> str:
    local = datetime.combine(
        datetime.strptime(signal_date, "%Y-%m-%d").date(),
        wall_time(hour=15),
        tzinfo=ZoneInfo("Asia/Shanghai"),
    )
    return local.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _verify_generation_input_binding(
    generation: GenerationResult,
    source: ProspectiveInputSnapshot,
    *,
    deadline: str,
) -> InputSnapshot:
    if source.signal_date != generation.signal_date or source.trade_date != generation.trade_date:
        raise ProspectiveExecutionDataError("generation and decision source dates differ")
    try:
        rebuilt = InputSnapshot(
            signal_date=source.signal_date,
            calendar_sessions=source.calendar_sessions,
            skipped_sessions=generation.skipped_sessions,
            rows=source.target_frame,
            source_data_snapshot_sha256=source.snapshot_sha256,
            target_rows_sha256=source.target_rows_sha256,
            input_sources_sha256=source.input_sources_sha256,
            membership_artifact_sha256=source.membership_artifact_sha256,
            # The source manifest remains content-addressed independently of
            # its receipt, but the target snapshot must hash the durable build
            # boundary.  Otherwise a bundle published after the admission
            # deadline could replay as if only its earlier inputs mattered.
            source_build_checkpoint_utc=source.build_completed_at_utc,
            max_available_at_utc=source.inputs_available_at_utc,
            information_cutoff_utc=source.build_completed_at_utc,
            signal_close_utc=_signal_close(source.signal_date),
            admission_deadline_utc=deadline,
        )
    except Exception as exc:
        raise ProspectiveExecutionDataError("decision source cannot rebuild the target input") from exc
    if rebuilt.snapshot_sha256 != generation.input_snapshot_sha256:
        raise ProspectiveExecutionDataError("generation is not bound to this decision source")
    if (
        generation.calendar_index >= len(rebuilt.calendar_sessions)
        or rebuilt.calendar_sessions[generation.calendar_index] != generation.signal_date
        or rebuilt.calendar_sessions[-1] != generation.trade_date
    ):
        raise ProspectiveExecutionDataError("generation absolute calendar index changed")
    return rebuilt


def _checkpoint(root: Path) -> tuple[Path, dict[str, Any]]:
    path = root / "runtime/data/raw/checkpoint.json"
    value = _load_json(path, label="raw checkpoint")
    if type(value.get("schema_version")) is not int or value.get("schema_version") != 1:
        raise ProspectiveExecutionDataError("unsupported raw checkpoint schema")
    if not isinstance(value.get("partitions"), Mapping) or not isinstance(
        value.get("calendars"), Mapping
    ):
        raise ProspectiveExecutionDataError("raw checkpoint lacks partitions or calendars")
    return path, value


def _calendar_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    required = {"exchange", "cal_date", "is_open", "pretrade_date"}
    if set(frame.columns) != required or len(frame.columns) != len(required):
        raise ProspectiveExecutionDataError("official calendar has a non-canonical schema")
    work = frame.loc[:, ["exchange", "cal_date", "is_open", "pretrade_date"]].copy()
    work["cal_date"] = pd.to_datetime(work["cal_date"], errors="coerce").dt.normalize()
    work["pretrade_date"] = pd.to_datetime(work["pretrade_date"], errors="coerce").dt.normalize()
    if work.empty or work["cal_date"].isna().any() or work["cal_date"].duplicated().any():
        raise ProspectiveExecutionDataError("official calendar has invalid or duplicate dates")
    work = work.sort_values("cal_date", kind="mergesort").reset_index(drop=True)
    expected = pd.date_range(work["cal_date"].iloc[0], work["cal_date"].iloc[-1], freq="D")
    if not pd.DatetimeIndex(work["cal_date"]).equals(expected):
        raise ProspectiveExecutionDataError("official calendar does not cover every calendar day")
    open_numeric = pd.to_numeric(work["is_open"], errors="coerce")
    open_text = work["is_open"].astype("string").str.strip().str.casefold()
    valid = open_numeric.isin([0, 1]) | open_text.isin(["false", "true"])
    if not bool(valid.all()):
        raise ProspectiveExecutionDataError("official calendar has invalid is_open values")
    work["is_open"] = open_numeric.eq(1) | open_text.eq("true")
    return [
        {
            "cal_date": row.cal_date.date().isoformat(),
            "exchange": str(row.exchange),
            "is_open": bool(row.is_open),
            "pretrade_date": (
                row.pretrade_date.date().isoformat() if not pd.isna(row.pretrade_date) else None
            ),
        }
        for row in work.itertuples(index=False)
    ]


def _calendar_candidate(
    root: Path,
    key: str,
    entry: Mapping[str, Any],
    *,
    deadline: pd.Timestamp,
    sealed: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any], pd.Timestamp] | None:
    if (not sealed and entry.get("status") != "complete") or str(
        entry.get("calendar_content_sha256", entry.get("checkpoint_key"))
    ) != key:
        return None
    completed = _utc(entry.get("completed_at_utc"), label="calendar completed_at_utc")
    if completed > deadline:
        return None
    expected = root / "runtime/data/raw/trade_cal" / f"calendar_sha256={key}" / "part-000.parquet"
    if sealed:
        if entry.get("path") != _relative(expected, root):
            raise ProspectiveExecutionDataError("sealed calendar origin path differs")
        try:
            path = _resolve_immutable_artifact(
                root, entry, sha_field="artifact_sha256"
            )
            manifest_path = _resolve_immutable_artifact(
                root,
                entry,
                sha_field="manifest_sha256",
                path_field="immutable_manifest_path",
                size_field="manifest_size_bytes",
                media_field="manifest_media_type",
            )
        except Exception as exc:
            raise ProspectiveExecutionDataError(
                "sealed calendar CAS binding is invalid"
            ) from exc
        immutable = {
            field: entry[field]
            for field in (
                "artifact_sha256",
                "immutable_path",
                "size_bytes",
                "media_type",
            )
        }
        immutable_manifest = {
            field: entry[field]
            for field in (
                "manifest_sha256",
                "immutable_manifest_path",
                "manifest_size_bytes",
                "manifest_media_type",
            )
        }
        origin_path_text = str(entry["path"])
        origin_manifest_text = str(entry["manifest_path"])
    else:
        origin_path = _resolved_source_path(
            entry.get("path"), expected=expected, root=root, label="calendar"
        )
        wanted = _require_sha(entry.get("artifact_sha256"), label="calendar artifact hash")
        manifest_expected = origin_path.with_name("manifest.json")
        origin_manifest = _resolved_source_path(
            entry.get("manifest_path"),
            expected=manifest_expected,
            root=root,
            label="calendar manifest",
        )
        try:
            path, immutable = _capture_immutable_artifact(
                root,
                origin_path,
                expected_sha256=wanted,
                sha_field="artifact_sha256",
            )
            manifest_path, immutable_manifest = _capture_immutable_artifact(
                root,
                origin_manifest,
                expected_sha256=str(entry.get("manifest_sha256") or ""),
                sha_field="manifest_sha256",
                path_field="immutable_manifest_path",
                size_field="manifest_size_bytes",
                media_field="manifest_media_type",
            )
        except Exception as exc:
            raise ProspectiveExecutionDataError(
                "official calendar artifact could not enter immutable CAS"
            ) from exc
        origin_path_text = _relative(origin_path, root)
        origin_manifest_text = _relative(origin_manifest, root)
    wanted = str(immutable["artifact_sha256"])
    try:
        frame = pd.read_parquet(path)
    except Exception as exc:
        raise ProspectiveExecutionDataError("unreadable official calendar artifact") from exc
    records = _calendar_records(frame)
    if _sha256_bytes(_canonical_json_bytes(records)) != key:
        raise ProspectiveExecutionDataError("official calendar content hash mismatch")
    open_count = sum(bool(row["is_open"]) for row in records)
    if (
        str(entry.get("source_start_date", entry.get("start_date")))
        != records[0]["cal_date"]
        or str(entry.get("source_end_date", entry.get("end_date")))
        != records[-1]["cal_date"]
        or int(entry.get("row_count") or -1) != len(records)
        or int(entry.get("open_day_count") or -1) != open_count
    ):
        raise ProspectiveExecutionDataError("official calendar checkpoint metadata mismatch")
    manifest_sha = str(immutable_manifest["manifest_sha256"])
    manifest = _load_json(manifest_path, label="calendar manifest")
    if (
        str(manifest.get("calendar_content_sha256")) != key
        or str(manifest.get("artifact_sha256")) != wanted
        or _require_sha(manifest.get("records_sha256"), label="calendar records hash") != key
    ):
        raise ProspectiveExecutionDataError("official calendar manifest binding mismatch")
    source = {
        "role": "official_calendar",
        "checkpoint_key": key,
        "calendar_content_sha256": key,
        "path": origin_path_text,
        **immutable,
        "manifest_path": origin_manifest_text,
        **immutable_manifest,
        "completed_at_utc": _utc_text(completed, label="calendar completion"),
        "source_start_date": records[0]["cal_date"],
        "source_end_date": records[-1]["cal_date"],
        "row_count": len(records),
        "open_day_count": open_count,
    }
    sealed_base = {
        field: value
        for field, value in entry.items()
        if field not in {"selected_max_date", "selected_open_sessions"}
    }
    if sealed and source != sealed_base:
        raise ProspectiveExecutionDataError("sealed calendar contract differs from CAS bytes")
    return records, source, completed


def _select_calendar(
    root: Path,
    checkpoint: Mapping[str, Any],
    generation: GenerationResult,
    decision_sessions: Sequence[str],
    *,
    deadline: pd.Timestamp,
    sealed_sources: Sequence[Mapping[str, Any]] | None = None,
    require_sealed_selection: bool = True,
) -> tuple[tuple[str, ...], tuple[str, ...], dict[str, Any], pd.Timestamp]:
    candidates: list[tuple[pd.Timestamp, str, list[dict[str, Any]], dict[str, Any]]] = []
    entries = (
        {
            str(row.get("checkpoint_key") or ""): row
            for row in sealed_sources
        }
        if sealed_sources is not None
        else checkpoint["calendars"]
    )
    for raw_key, raw_entry in entries.items():
        key = _require_sha(raw_key, label="calendar checkpoint key")
        if not isinstance(raw_entry, Mapping):
            raise ProspectiveExecutionDataError("calendar checkpoint entry must be an object")
        checked = _calendar_candidate(
            root,
            key,
            raw_entry,
            deadline=deadline,
            sealed=sealed_sources is not None,
        )
        if checked is not None:
            records, source, completed = checked
            candidates.append((completed, key, records, source))
    candidates.sort(key=lambda row: (row[0].value, row[1]))
    decision = tuple(_date(value, label="decision calendar session") for value in decision_sessions)
    for completed, key, records, source in candidates:
        opens = tuple(row["cal_date"] for row in records if row["is_open"])
        if not opens:
            continue
        source_start, source_end = records[0]["cal_date"], records[-1]["cal_date"]
        overlap_expected = tuple(
            value for value in decision if source_start <= value <= source_end
        )
        overlap_actual = tuple(value for value in opens if value <= decision[-1])
        if overlap_actual != overlap_expected:
            continue
        extension = tuple(value for value in opens if value > decision[-1])
        merged = decision + extension
        end_index = generation.calendar_index + HOLDING_DAYS + 1
        if end_index >= len(merged):
            continue
        if merged[generation.calendar_index] != generation.signal_date:
            continue
        if merged[generation.calendar_index + 1] != generation.trade_date:
            continue
        sessions = merged[: end_index + 1]
        window = sessions[generation.calendar_index + 1 : end_index + 1]
        if len(window) != HOLDING_DAYS + 1:
            continue
        selected = {
            **source,
            "selected_max_date": sessions[-1],
            "selected_open_sessions": list(value for value in opens if value <= sessions[-1]),
        }
        if sealed_sources is not None and require_sealed_selection:
            sealed_entry = entries[key]
            if selected != dict(sealed_entry):
                raise ProspectiveExecutionDataError(
                    "sealed calendar selection differs from its source contract"
                )
        return sessions, window, selected, completed
    raise ProspectiveExecutionDataError(
        "no checkpointed official calendar available before the trade deadline reaches holding end"
    )


def _read_partition(
    root: Path,
    checkpoint: Mapping[str, Any],
    *,
    dataset: str,
    trade_date: str,
    availability_cap: pd.Timestamp | None,
    sealed_source: Mapping[str, Any] | None = None,
) -> tuple[pd.DataFrame, dict[str, Any], pd.Timestamp]:
    key = f"{dataset}/{trade_date}"
    raw = sealed_source or checkpoint.get("partitions", {}).get(key)
    if not isinstance(raw, Mapping):
        raise ProspectiveExecutionDataError(f"missing checkpointed partition {key}")
    if (
        (sealed_source is None and raw.get("status") != "complete")
        or raw.get("dataset") != dataset
        or raw.get("trade_date") != trade_date
    ):
        raise ProspectiveExecutionDataError(f"partition checkpoint identity mismatch for {key}")
    completed = _utc(raw.get("completed_at_utc"), label=f"{key}.completed_at_utc")
    if availability_cap is not None and completed > availability_cap:
        raise ProspectiveExecutionDataError(f"partition {key} was unavailable at the requested cutoff")
    expected = (
        root
        / "runtime/data/raw"
        / dataset
        / f"trade_date={trade_date}"
        / "part-000.parquet"
    )
    if sealed_source is None:
        origin_path = _resolved_source_path(
            raw.get("path"), expected=expected, root=root, label=key
        )
        wanted = _require_sha(raw.get("sha256"), label=f"{key}.sha256")
        if origin_path.stat().st_size != int(raw.get("size_bytes") or -1):
            raise ProspectiveExecutionDataError(f"partition bytes differ from checkpoint for {key}")
        try:
            path, immutable = _capture_immutable_artifact(
                root, origin_path, expected_sha256=wanted
            )
        except Exception as exc:
            raise ProspectiveExecutionDataError(
                f"partition {key} could not enter immutable CAS"
            ) from exc
        origin_text = _relative(origin_path, root)
    else:
        if raw.get("path") != _relative(expected, root):
            raise ProspectiveExecutionDataError(
                f"sealed partition origin path differs for {key}"
            )
        try:
            path = _resolve_immutable_artifact(root, raw)
        except Exception as exc:
            raise ProspectiveExecutionDataError(
                f"sealed partition CAS binding differs for {key}"
            ) from exc
        immutable = {
            field: raw[field]
            for field in ("sha256", "immutable_path", "size_bytes", "media_type")
        }
        wanted = str(immutable["sha256"])
        origin_text = str(raw["path"])
    try:
        frame = pd.read_parquet(path)
    except Exception as exc:
        raise ProspectiveExecutionDataError(f"unreadable partition {key}") from exc
    required = {value for value in DATASET_FIELDS[dataset].split(",") if value}
    missing = sorted(required - set(frame.columns))
    if missing or frame.empty or int(raw.get("row_count") or -1) != len(frame):
        raise ProspectiveExecutionDataError(f"partition {key} schema/count mismatch: {missing}")
    tickers = frame["ts_code"].astype("string").str.strip()
    dates = pd.to_datetime(
        frame["trade_date"].astype("string").str.replace("-", "", regex=False),
        format="%Y%m%d",
        errors="coerce",
    ).dt.strftime("%Y-%m-%d")
    if (
        tickers.isna().any()
        or tickers.eq("").any()
        or dates.isna().any()
        or bool(dates.ne(trade_date).any())
        or bool(frame.assign(_ticker=tickers).duplicated(["_ticker", "trade_date"]).any())
    ):
        raise ProspectiveExecutionDataError(f"partition {key} contains invalid/future/duplicate rows")
    result = frame.copy()
    result["ts_code"] = tickers
    result["trade_date"] = dates
    if dataset == "daily":
        numeric = ["open", "high", "low", "close", "pre_close", "pct_chg", "amount"]
        converted = result[numeric].apply(pd.to_numeric, errors="coerce")
        if not bool(np.isfinite(converted).all().all()) or bool(
            (converted[["open", "high", "low", "close", "pre_close"]] <= 0).any().any()
        ):
            raise ProspectiveExecutionDataError(f"partition {key} contains invalid market values")
        expected_pct = (converted["close"] / converted["pre_close"] - 1.0) * 100.0
        if not bool(np.isclose(converted["pct_chg"], expected_pct, rtol=0.0, atol=0.02).all()):
            raise ProspectiveExecutionDataError(f"partition {key} pct_chg is inconsistent")
        result[numeric] = converted
    elif dataset == "adj_factor":
        factors = pd.to_numeric(result["adj_factor"], errors="coerce")
        if not bool(np.isfinite(factors).all()) or bool((factors <= 0).any()):
            raise ProspectiveExecutionDataError(f"partition {key} has invalid adjustment factors")
        result["adj_factor"] = factors
    source = {
        "role": "raw_partition",
        "checkpoint_key": key,
        "dataset": dataset,
        "trade_date": trade_date,
        "path": origin_text,
        **immutable,
        "row_count": int(len(result)),
        "completed_at_utc": _utc_text(completed, label=f"{key}.completion"),
    }
    if sealed_source is not None and source != dict(sealed_source):
        raise ProspectiveExecutionDataError(
            f"sealed partition contract differs from CAS bytes for {key}"
        )
    return result, source, completed


def _inspect_checkpoint_partition(
    root: Path,
    checkpoint: Mapping[str, Any],
    *,
    dataset: str,
    trade_date: str,
) -> pd.Timestamp | None:
    """Verify one mutable checkpoint origin without materialising CAS bytes."""

    key = f"{dataset}/{trade_date}"
    raw = checkpoint.get("partitions", {}).get(key)
    if raw is None:
        return None
    if not isinstance(raw, Mapping):
        raise ProspectiveExecutionDataError(
            f"partition checkpoint entry must be an object for {key}"
        )
    if (
        raw.get("status") != "complete"
        or raw.get("dataset") != dataset
        or raw.get("trade_date") != trade_date
    ):
        raise ProspectiveExecutionDataError(
            f"partition checkpoint identity mismatch for {key}"
        )
    completed = _utc(raw.get("completed_at_utc"), label=f"{key}.completed_at_utc")
    expected = (
        root
        / "runtime/data/raw"
        / dataset
        / f"trade_date={trade_date}"
        / "part-000.parquet"
    )
    if expected.is_symlink():
        raise ProspectiveExecutionDataError(f"partition path is a symlink for {key}")
    origin = _resolved_source_path(
        raw.get("path"), expected=expected, root=root, label=key
    )
    if not origin.is_file() or origin.is_symlink():
        raise ProspectiveExecutionDataError(f"missing canonical partition bytes for {key}")
    if (
        origin.stat().st_size != int(raw.get("size_bytes") or -1)
        or sha256_file(origin)
        != _require_sha(raw.get("sha256"), label=f"{key}.sha256")
    ):
        raise ProspectiveExecutionDataError(
            f"partition bytes differ from checkpoint for {key}"
        )
    try:
        frame = pd.read_parquet(origin)
    except Exception as exc:
        raise ProspectiveExecutionDataError(f"unreadable partition {key}") from exc
    required = {value for value in DATASET_FIELDS[dataset].split(",") if value}
    missing = sorted(required - set(frame.columns))
    if missing or frame.empty or int(raw.get("row_count") or -1) != len(frame):
        raise ProspectiveExecutionDataError(
            f"partition {key} schema/count mismatch: {missing}"
        )
    tickers = frame["ts_code"].astype("string").str.strip()
    dates = pd.to_datetime(
        frame["trade_date"].astype("string").str.replace("-", "", regex=False),
        format="%Y%m%d",
        errors="coerce",
    ).dt.strftime("%Y-%m-%d")
    if (
        tickers.isna().any()
        or tickers.eq("").any()
        or dates.isna().any()
        or bool(dates.ne(trade_date).any())
        or bool(frame.assign(_ticker=tickers).duplicated(["_ticker", "trade_date"]).any())
    ):
        raise ProspectiveExecutionDataError(
            f"partition {key} contains invalid/future/duplicate rows"
        )
    if dataset == "daily":
        numeric = ["open", "high", "low", "close", "pre_close", "pct_chg", "amount"]
        converted = frame[numeric].apply(pd.to_numeric, errors="coerce")
        if not bool(np.isfinite(converted).all().all()) or bool(
            (converted[["open", "high", "low", "close", "pre_close"]] <= 0)
            .any()
            .any()
        ):
            raise ProspectiveExecutionDataError(
                f"partition {key} contains invalid market values"
            )
        expected_pct = (converted["close"] / converted["pre_close"] - 1.0) * 100.0
        if not bool(
            np.isclose(
                converted["pct_chg"], expected_pct, rtol=0.0, atol=0.02
            ).all()
        ):
            raise ProspectiveExecutionDataError(
                f"partition {key} pct_chg is inconsistent"
            )
    elif dataset == "adj_factor":
        factors = pd.to_numeric(frame["adj_factor"], errors="coerce")
        if not bool(np.isfinite(factors).all()) or bool((factors <= 0).any()):
            raise ProspectiveExecutionDataError(
                f"partition {key} has invalid adjustment factors"
            )
    return completed


def inspect_prospective_execution_sources(
    project_root: str | Path,
    generation_result: GenerationResult | Mapping[str, Any],
    *,
    source_data_snapshot_sha256: str,
    observed_at_utc: str | pd.Timestamp,
) -> dict[str, Any]:
    """Read-only readiness for one sealed decision's i+11 source closure.

    The holding window comes only from the decision input's sealed official
    calendar CAS.  Mutable raw data is accepted only through byte-verified
    checkpoint entries, and suspensions must be a full-history capture that is
    no older than the holding-end market partitions.
    """

    root = Path(project_root).expanduser().resolve()
    generation = _generation(generation_result)
    observed = _utc(observed_at_utc, label="observed_at_utc")
    source_sha = _require_sha(
        source_data_snapshot_sha256, label="source_data_snapshot_sha256"
    )
    source_path = root / PROSPECTIVE_RELATIVE_ROOT / "inputs" / source_sha
    try:
        source = load_prospective_input_snapshot(source_path)
    except Exception as exc:
        raise ProspectiveExecutionDataError(
            "decision input failed independent source rebuild"
        ) from exc
    if source.snapshot_sha256 != source_sha:
        raise ProspectiveExecutionDataError("decision input path/hash mismatch")
    deadline_text = _trade_deadline(generation.trade_date)
    _verify_generation_input_binding(generation, source, deadline=deadline_text)
    calendar = source.manifest.get("calendar")
    sealed_values = calendar.get("sources") if isinstance(calendar, Mapping) else None
    if not isinstance(sealed_values, list) or not sealed_values or not all(
        isinstance(row, Mapping) for row in sealed_values
    ):
        raise ProspectiveExecutionDataError(
            "decision input lacks sealed official calendar sources"
        )
    # The input source adds an availability-basis label that is not part of the
    # execution source contract.  Every other field is verified byte-for-byte
    # by the sealed branch of ``_select_calendar``.
    sealed_calendars = [
        {
            "role": "official_calendar",
            "checkpoint_key": row.get("calendar_content_sha256"),
            **{
                key: value
                for key, value in row.items()
                if key != "availability_basis"
            },
        }
        for row in sealed_values
    ]
    _sessions, window, calendar_source, _calendar_completed = _select_calendar(
        root,
        {},
        generation,
        source.calendar_sessions,
        deadline=_utc(deadline_text, label="trade deadline"),
        sealed_sources=sealed_calendars,
        require_sealed_selection=False,
    )

    holding_end_close = pd.Timestamp(
        datetime.combine(
            datetime.strptime(window[-1], "%Y-%m-%d").date(),
            wall_time(hour=15),
            tzinfo=ZoneInfo("Asia/Shanghai"),
        )
    ).tz_convert("UTC")
    if observed < holding_end_close:
        return {
            "schema_version": 1,
            "status": "not_mature",
            "generation_result_sha256": generation.result_sha256,
            "source_data_snapshot_sha256": source_sha,
            "holding_start_date": window[0],
            "holding_end_date": window[-1],
            "holding_end_close_utc": _utc_text(
                holding_end_close, label="holding-end close"
            ),
            "calendar_source_sha256": str(
                calendar_source["calendar_content_sha256"]
            ),
            "required_datasets": ["daily", "adj_factor"],
            "required_partition_count": len(window) * 2,
            "missing_partition_keys": [],
            "future_partition_keys": [],
        }

    checkpoint_path = root / "runtime/data/raw/checkpoint.json"
    if checkpoint_path.is_symlink():
        raise ProspectiveExecutionDataError("raw checkpoint path is a symlink")
    if checkpoint_path.exists():
        _loaded_checkpoint_path, checkpoint = _checkpoint(root)
    else:
        checkpoint = {"schema_version": 1, "partitions": {}, "calendars": {}}
    missing_keys: list[str] = []
    future_keys: list[str] = []
    completions: dict[str, pd.Timestamp] = {}
    for session in window:
        for dataset in ("daily", "adj_factor"):
            key = f"{dataset}/{session}"
            completed = _inspect_checkpoint_partition(
                root,
                checkpoint,
                dataset=dataset,
                trade_date=session,
            )
            if completed is None:
                missing_keys.append(key)
            else:
                completions[key] = completed
                if completed > observed:
                    future_keys.append(key)

    base = {
        "schema_version": 1,
        "generation_result_sha256": generation.result_sha256,
        "source_data_snapshot_sha256": source_sha,
        "holding_start_date": window[0],
        "holding_end_date": window[-1],
        "calendar_source_sha256": str(calendar_source["calendar_content_sha256"]),
        "required_datasets": ["daily", "adj_factor"],
        "required_partition_count": len(window) * 2,
        "missing_partition_keys": missing_keys,
        "future_partition_keys": future_keys,
    }
    if missing_keys:
        return {**base, "status": "market_data_missing"}
    if future_keys:
        return {**base, "status": "waiting", "reason": "market_data_not_yet_available"}

    end_completed = max(
        completions[f"daily/{window[-1]}"],
        completions[f"adj_factor/{window[-1]}"],
    )
    base["holding_end_market_completed_at_utc"] = _utc_text(
        end_completed, label="holding-end market completion"
    )
    suspension_path = root / "runtime/data/top500/suspensions.parquet"
    suspension_metadata_path = suspension_path.with_name("suspensions.meta.json")
    suspension_present = suspension_path.exists() or suspension_path.is_symlink()
    metadata_present = (
        suspension_metadata_path.exists()
        or suspension_metadata_path.is_symlink()
    )
    if not suspension_present and not metadata_present:
        return {**base, "status": "suspensions_missing"}
    if suspension_present and not metadata_present:
        if suspension_path.is_symlink() or not suspension_path.is_file():
            raise ProspectiveExecutionDataError(
                "uncommitted suspension parquet is not a regular file"
            )
        # ``sync_suspensions --no-resume`` publishes parquet before its
        # metadata commit marker.  A crash in that narrow window leaves no
        # authoritative evidence and is safe to replace on the next action.
        return {
            **base,
            "status": "suspensions_missing",
            "recovery": "uncommitted_parquet_without_metadata",
        }
    if (
        suspension_path.is_symlink()
        or suspension_metadata_path.is_symlink()
        or not suspension_path.is_file()
        or not suspension_metadata_path.is_file()
    ):
        raise ProspectiveExecutionDataError(
            "suspension evidence is incomplete or uses a symlink"
        )
    try:
        suspension = audit_suspensions_snapshot(
            suspension_path,
            suspension_metadata_path,
        )
        suspension_metadata = _load_json(
            suspension_metadata_path, label="suspension metadata"
        )
    except Exception as exc:
        # The only recoverable two-file mismatch is the documented publish
        # window: an older, already-stale metadata marker can remain after the
        # new parquet was atomically installed.  Current/malformed metadata is
        # still an evidence conflict and fails closed.
        try:
            stale_metadata = _load_json(
                suspension_metadata_path, label="suspension metadata"
            )
            stale_query = stale_metadata.get("query")
            stale_file = stale_metadata.get("file")
            stale_retrieved = _utc(
                stale_metadata.get("retrieved_at_utc"),
                label="suspension retrieved_at_utc",
            )
            stale_start = _date(
                (
                    stale_query.get("start_date")
                    if isinstance(stale_query, Mapping)
                    else None
                ),
                label="suspension query start",
            )
            stale_end = _date(
                (
                    stale_query.get("end_date")
                    if isinstance(stale_query, Mapping)
                    else None
                ),
                label="suspension query end",
            )
            stale_sha = _require_sha(
                (
                    stale_file.get("sha256")
                    if isinstance(stale_file, Mapping)
                    else None
                ),
                label="stale suspension parquet hash",
            )
            stale_contract = (
                set(stale_metadata)
                == {
                    "schema_version",
                    "status",
                    "source",
                    "endpoint",
                    "query",
                    "retrieved_at_utc",
                    "rows",
                    "date",
                    "security",
                    "S",
                    "R",
                    "file",
                    "metadata_path",
                }
                and type(stale_metadata.get("schema_version")) is int
                and stale_metadata.get("schema_version") == 1
                and stale_metadata.get("status") == "complete"
                and stale_metadata.get("source") == "tushare"
                and stale_metadata.get("endpoint") == "suspend_d"
                and isinstance(stale_query, Mapping)
                and set(stale_query)
                == {"start_date", "end_date", "window", "limit"}
                and stale_query.get("window") == "calendar_year"
                and stale_query.get("limit") == SUSPENSION_PAGE_SIZE
                and isinstance(stale_file, Mapping)
                and set(stale_file) == {"path", "size_bytes", "sha256"}
                and type(stale_file.get("size_bytes")) is int
                and int(stale_file["size_bytes"]) >= 0
                and stale_sha == stale_file.get("sha256")
                and str(stale_file.get("path")) == str(suspension_path.resolve())
                and str(stale_metadata.get("metadata_path"))
                == str(suspension_metadata_path.resolve())
            )
            stale_by_coverage = stale_contract and (
                stale_start > SUSPENSION_FULL_START_DATE
                or stale_end < window[-1]
                or stale_retrieved < end_completed
            )
        except Exception:
            stale_by_coverage = False
        if stale_by_coverage:
            return {
                **base,
                "status": "suspensions_missing",
                "recovery": "parquet_published_before_stale_metadata_replace",
            }
        raise ProspectiveExecutionDataError(
            "authoritative suspension evidence is invalid"
        ) from exc
    query = suspension.get("query")
    if not isinstance(query, Mapping):
        raise ProspectiveExecutionDataError("suspension query contract is invalid")
    retrieved = _utc(
        suspension_metadata.get("retrieved_at_utc"),
        label="suspension retrieved_at_utc",
    )
    coverage_incomplete = (
        str(query.get("start_date")) > SUSPENSION_FULL_START_DATE
        or str(query.get("end_date")) < window[-1]
        or retrieved < end_completed
    )
    suspension_view = {
        "query_start_date": str(query.get("start_date")),
        "query_end_date": str(query.get("end_date")),
        "retrieved_at_utc": _utc_text(retrieved, label="suspension retrieval"),
        "sha256": str(suspension["hash"]),
    }
    if coverage_incomplete:
        return {
            **base,
            "status": "suspensions_missing",
            "suspensions": suspension_view,
        }
    if retrieved > observed:
        return {
            **base,
            "status": "waiting",
            "reason": "suspensions_not_yet_available",
            "suspensions": suspension_view,
        }
    return {
        **base,
        "status": "complete",
        "suspensions": suspension_view,
    }


def _suspension_class(value: Any) -> str:
    if value is None or pd.isna(value) or not str(value).strip():
        return "full_day"
    for match in _SUSPENSION_INTERVAL_RE.finditer(str(value)):
        values = [int(match.group(name)) for name in (
            "start_hour", "start_minute", "end_hour", "end_minute"
        )]
        start_hour, start_minute, end_hour, end_minute = values
        if not (0 <= start_hour <= 23 and 0 <= end_hour <= 23 and 0 <= start_minute <= 59 and 0 <= end_minute <= 59):
            continue
        if start_hour * 60 + start_minute <= _MARKET_OPEN_MINUTE <= end_hour * 60 + end_minute:
            return "open_intraday"
    return "ignored_after_open"


def _suspension_flags(
    frame: pd.DataFrame,
    *,
    sessions: Sequence[str],
    tickers: set[str],
    daily_presence: set[tuple[str, str]],
) -> set[tuple[str, str]]:
    events = frame.copy()
    events["ticker"] = events["ticker"].astype("string").str.strip()
    events["date"] = pd.to_datetime(events["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    events["suspend_type"] = events["suspend_type"].astype("string").str.strip().str.upper()
    events = events.loc[events["ticker"].isin(tickers)].copy()
    by_key: dict[tuple[str, str], list[tuple[str, str]]] = {}
    for row in events.itertuples(index=False):
        timing = None if pd.isna(row.suspend_timing) else row.suspend_timing
        by_key.setdefault((str(row.date), str(row.ticker)), []).append(
            (str(row.suspend_type), timing)
        )
    blocked: set[tuple[str, str]] = set()
    first_session = sessions[0]
    for ticker in sorted(tickers):
        active = False
        prior = events.loc[events["ticker"].eq(ticker) & events["date"].lt(first_session)]
        for _, group in prior.groupby("date", sort=True):
            types = set(group["suspend_type"])
            if "S" in types:
                active = True
            if "R" in types:
                active = False
        for session in sessions:
            rows = by_key.get((session, ticker), [])
            types = {kind for kind, _ in rows}
            block = active
            if "R" in types:
                active = False
                block = False
            s_classes = [_suspension_class(timing) for kind, timing in rows if kind == "S"]
            if s_classes:
                if any(value in {"full_day", "open_intraday"} for value in s_classes):
                    block = True
                active = True
                if "R" in types:
                    active = False
            if (session, ticker) not in daily_presence and "R" in types:
                block = True
            if block:
                blocked.add((session, ticker))
    return blocked


def _audit_suspension_frame(
    frame: pd.DataFrame,
    *,
    query_start: str,
    query_end: str,
    expected_stats: Mapping[str, Any],
) -> None:
    columns = ("ticker", "date", "suspend_type", "suspend_timing")
    if set(frame.columns) != set(columns) or len(frame.columns) != len(columns):
        raise ProspectiveExecutionDataError("suspension source has a non-canonical schema")
    work = frame.loc[:, list(columns)].copy()
    work["ticker"] = work["ticker"].astype("string").str.strip()
    work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.normalize()
    work["suspend_type"] = work["suspend_type"].astype("string").str.strip().str.upper()
    if (
        work["ticker"].isna().any()
        or work["ticker"].eq("").any()
        or work["date"].isna().any()
        or not bool(work["suspend_type"].isin({"S", "R"}).all())
        or bool(work.duplicated(list(columns)).any())
        or (
            len(work)
            and (
                bool(work["date"].lt(pd.Timestamp(query_start)).any())
                or bool(work["date"].gt(pd.Timestamp(query_end)).any())
            )
        )
    ):
        raise ProspectiveExecutionDataError("suspension source contains invalid rows")
    types = work["suspend_type"]
    stats = {
        "rows": int(len(work)),
        "date": {
            "min": work["date"].min().date().isoformat() if len(work) else None,
            "max": work["date"].max().date().isoformat() if len(work) else None,
        },
        "security": int(work["ticker"].nunique()),
        "S": int(types.eq("S").sum()),
        "R": int(types.eq("R").sum()),
    }
    if any(expected_stats.get(name) != value for name, value in stats.items()):
        raise ProspectiveExecutionDataError("suspension source statistics differ from manifest")


def _materialize_suspension_source(
    root: Path,
    *,
    start: str,
    end: str,
) -> str:
    """Promote the mutable canonical suspension file into an immutable source."""

    origin_path = root / "runtime/data/top500/suspensions.parquet"
    origin_metadata_path = origin_path.with_name("suspensions.meta.json")
    try:
        data_path, data_binding = _capture_immutable_artifact(root, origin_path)
        metadata_path, metadata_binding = _capture_immutable_artifact(
            root,
            origin_metadata_path,
            sha_field="metadata_sha256",
            path_field="immutable_metadata_path",
            size_field="metadata_size_bytes",
            media_field="metadata_media_type",
        )
    except Exception as exc:
        raise ProspectiveExecutionDataError("authoritative suspension evidence is unavailable") from exc
    metadata = _load_json(metadata_path, label="suspension metadata")
    required_metadata = {
        "schema_version",
        "status",
        "source",
        "endpoint",
        "query",
        "retrieved_at_utc",
        "rows",
        "date",
        "security",
        "S",
        "R",
        "file",
        "metadata_path",
    }
    query = metadata.get("query")
    file_info = metadata.get("file")
    if (
        set(metadata) != required_metadata
        or type(metadata.get("schema_version")) is not int
        or metadata.get("schema_version") != 1
        or metadata.get("status") != "complete"
        or metadata.get("source") != "tushare"
        or metadata.get("endpoint") != "suspend_d"
        or not isinstance(query, Mapping)
        or set(query) != {"start_date", "end_date", "window", "limit"}
        or query.get("window") != "calendar_year"
        or query.get("limit") != SUSPENSION_PAGE_SIZE
        or _date(query.get("start_date"), label="suspension query start")
        > SUSPENSION_FULL_START_DATE
        or _date(query.get("end_date"), label="suspension query end") < end
        or not isinstance(file_info, Mapping)
        or set(file_info) != {"path", "size_bytes", "sha256"}
        or file_info.get("path") != str(origin_path.resolve())
        or file_info.get("size_bytes") != data_binding["size_bytes"]
        or file_info.get("sha256") != data_binding["sha256"]
        or metadata.get("metadata_path") != str(origin_metadata_path.resolve())
    ):
        raise ProspectiveExecutionDataError("suspension metadata has a non-exact binding")
    try:
        frame = pd.read_parquet(data_path)
    except Exception as exc:
        raise ProspectiveExecutionDataError("unreadable immutable suspension data") from exc
    _audit_suspension_frame(
        frame,
        query_start=str(query["start_date"]),
        query_end=str(query["end_date"]),
        expected_stats=metadata,
    )
    data_sha = str(data_binding["sha256"])
    metadata_sha = str(metadata_binding["metadata_sha256"])
    manifest = {
        "schema_version": 1,
        "kind": "prospective_suspension_source",
        "source": "tushare",
        "endpoint": "suspend_d",
        "query": dict(query),
        "retrieved_at_utc": _utc_text(
            metadata.get("retrieved_at_utc"), label="suspension retrieved_at_utc"
        ),
        "rows": metadata["rows"],
        "date": metadata["date"],
        "security": metadata["security"],
        "S": metadata["S"],
        "R": metadata["R"],
        "data_file": "suspensions.parquet",
        "data_sha256": data_sha,
        "origin_metadata_file": "origin-metadata.json",
        "origin_metadata_sha256": metadata_sha,
        "origin_data_path": _relative(origin_path, root),
        "origin_metadata_path": _relative(origin_metadata_path, root),
    }
    manifest_bytes = _canonical_json_bytes(manifest)
    key = _sha256_bytes(manifest_bytes)
    directory = root / PROSPECTIVE_RELATIVE_ROOT / "suspensions" / key
    # Publish from already captured CAS bytes, never by re-reading the mutable
    # canonical files after their hashes were established.
    _write_create_only(directory / "suspensions.parquet", data_path.read_bytes())
    _write_create_only(directory / "origin-metadata.json", metadata_path.read_bytes())
    _write_create_only(directory / "manifest.json", manifest_bytes)
    return key


def _load_suspensions(
    root: Path,
    *,
    start: str,
    end: str,
    availability_cap: pd.Timestamp | None,
    minimum_completed_at: pd.Timestamp,
    artifact_sha256: str | None = None,
) -> tuple[pd.DataFrame, dict[str, Any], pd.Timestamp]:
    key = (
        _require_sha(artifact_sha256, label="suspension artifact key")
        if artifact_sha256 is not None
        else _materialize_suspension_source(root, start=start, end=end)
    )
    directory = root / PROSPECTIVE_RELATIVE_ROOT / "suspensions" / key
    manifest_path = directory / "manifest.json"
    manifest_bytes = manifest_path.read_bytes() if manifest_path.is_file() else b""
    if _sha256_bytes(manifest_bytes) != key:
        raise ProspectiveExecutionDataError("suspension source directory/manifest hash mismatch")
    manifest = _load_json(manifest_path, label="immutable suspension manifest")
    if manifest_bytes != _canonical_json_bytes(manifest):
        raise ProspectiveExecutionDataError("immutable suspension manifest is not canonical")
    required = {
        "schema_version",
        "kind",
        "source",
        "endpoint",
        "query",
        "retrieved_at_utc",
        "rows",
        "date",
        "security",
        "S",
        "R",
        "data_file",
        "data_sha256",
        "origin_metadata_file",
        "origin_metadata_sha256",
        "origin_data_path",
        "origin_metadata_path",
    }
    if (
        set(manifest) != required
        or type(manifest.get("schema_version")) is not int
        or manifest.get("schema_version") != 1
        or manifest.get("kind") != "prospective_suspension_source"
    ):
        raise ProspectiveExecutionDataError("immutable suspension manifest has a non-exact schema")
    query = manifest.get("query")
    if (
        not isinstance(query, Mapping)
        or str(query.get("start_date")) > SUSPENSION_FULL_START_DATE
        or str(query.get("end_date")) < end
    ):
        raise ProspectiveExecutionDataError("immutable suspension query does not cover holding window")
    completed = _utc(manifest.get("retrieved_at_utc"), label="suspension retrieved_at_utc")
    if availability_cap is not None and completed > availability_cap:
        raise ProspectiveExecutionDataError("suspension evidence was unavailable at requested cutoff")
    if completed < minimum_completed_at:
        raise ProspectiveExecutionDataError(
            "suspension snapshot predates holding-end market-data completion"
        )
    data_path = directory / str(manifest.get("data_file"))
    metadata_path = directory / str(manifest.get("origin_metadata_file"))
    data_sha = _require_sha(manifest.get("data_sha256"), label="suspension data hash")
    metadata_sha = _require_sha(
        manifest.get("origin_metadata_sha256"), label="suspension metadata hash"
    )
    if (
        data_path.parent != directory
        or metadata_path.parent != directory
        or not data_path.is_file()
        or not metadata_path.is_file()
        or sha256_file(data_path) != data_sha
        or sha256_file(metadata_path) != metadata_sha
    ):
        raise ProspectiveExecutionDataError("immutable suspension source bytes differ")
    try:
        frame = pd.read_parquet(data_path)
    except Exception as exc:
        raise ProspectiveExecutionDataError("unreadable immutable suspension data") from exc
    _audit_suspension_frame(
        frame,
        query_start=str(query["start_date"]),
        query_end=str(query["end_date"]),
        expected_stats=manifest,
    )
    selected = frame.loc[
        pd.to_datetime(frame["date"], errors="coerce").between(
            pd.Timestamp(start), pd.Timestamp(end)
        )
    ]
    source = {
        "role": "suspensions",
        "artifact_sha256": key,
        "path": _relative(directory, root),
        "manifest_path": _relative(manifest_path, root),
        "manifest_sha256": key,
        "data_sha256": data_sha,
        "origin_metadata_sha256": metadata_sha,
        "completed_at_utc": _utc_text(completed, label="suspension completion"),
        "source_query_start_date": str(query["start_date"]),
        "source_query_end_date": str(query["end_date"]),
        "selected_start_date": start,
        "selected_end_date": end,
        "selected_row_count": int(len(selected)),
    }
    return frame, source, completed


def _frozen_delist_dates(
    root: Path,
    source: ProspectiveInputSnapshot,
    tickers: set[str],
) -> tuple[dict[str, str], dict[str, Any]]:
    feature_sources = [
        item
        for item in source.manifest.get("inputs", ())
        if isinstance(item, Mapping) and item.get("role") == "canonical_features"
    ]
    if len(feature_sources) != 1:
        raise ProspectiveExecutionDataError("decision source must bind one frozen canonical feature file")
    item = feature_sources[0]
    expected = root / "runtime/data/top500/features.parquet"
    if item.get("path") != _relative(expected, root):
        raise ProspectiveExecutionDataError("frozen canonical features origin path differs")
    try:
        path = _resolve_immutable_artifact(root, item)
    except Exception as exc:
        raise ProspectiveExecutionDataError(
            "frozen canonical features CAS binding is invalid"
        ) from exc
    wanted = _require_sha(item.get("sha256"), label="canonical features hash")
    try:
        frame = pd.read_parquet(path, columns=["ticker", "delist_date"])
    except Exception as exc:
        raise ProspectiveExecutionDataError("delist evidence is unavailable in canonical features") from exc
    frame["ticker"] = frame["ticker"].astype("string").str.strip()
    frame["delist_date"] = pd.to_datetime(frame["delist_date"], errors="coerce").dt.strftime(
        "%Y-%m-%d"
    )
    selected = frame.loc[frame["ticker"].isin(tickers) & frame["delist_date"].notna()]
    dates: dict[str, str] = {}
    for ticker, group in selected.groupby("ticker", sort=True):
        values = sorted(set(group["delist_date"].astype(str)))
        if len(values) != 1:
            raise ProspectiveExecutionDataError(f"conflicting frozen delist dates for {ticker}")
        dates[str(ticker)] = values[0]
    return dates, {
        "role": "frozen_delist_projection",
        "path": str(item["path"]),
        "sha256": wanted,
        "immutable_path": str(item["immutable_path"]),
        "size_bytes": item["size_bytes"],
        "media_type": item["media_type"],
        "availability_basis": "decision_bound_pre_activation_frozen_canonical",
        "source_row_count": int(len(frame)),
        "selected_security_count": len(dates),
    }


def _official_delist_client(root: Path) -> Any:
    """Construct the configured official client without accepting caller data."""

    config_path = root / DEFAULT_CONFIG_PATH
    try:
        config = load_data_config(config_path)
        layout = RuntimeLayout.from_config(
            config,
            config_path=config_path,
            repo_root=root,
        )
        return _configured_tushare_client(dict(config.get("sync") or {}), layout)
    except Exception as exc:
        raise ProspectiveExecutionDataError(
            "official delist client could not be configured"
        ) from exc


def _official_retrieved_at_utc() -> str:
    return _utc_text(
        datetime.now(timezone.utc),
        label="official delist retrieval",
    )


def _normalise_official_delist_rows(frame: pd.DataFrame) -> list[dict[str, Any]]:
    missing = sorted(set(_OFFICIAL_DELIST_FIELDS) - set(frame.columns))
    if missing:
        raise ProspectiveExecutionDataError(
            f"official delist response lacks required fields: {missing}"
        )
    if len(frame) >= _OFFICIAL_DELIST_RESULT_LIMIT:
        raise ProspectiveExecutionDataError(
            "official delist response reached the provider result limit"
        )
    if len(frame) < _OFFICIAL_DELIST_MINIMUM_ROWS:
        raise ProspectiveExecutionDataError(
            "official delist response is implausibly empty"
        )
    selected = frame.loc[:, list(_OFFICIAL_DELIST_FIELDS)].copy()
    selected["ts_code"] = selected["ts_code"].astype("string").str.strip().str.upper()
    selected["list_status"] = (
        selected["list_status"].astype("string").str.strip().str.upper()
    )
    if selected["ts_code"].isna().any() or selected["ts_code"].eq("").any():
        raise ProspectiveExecutionDataError("official delist response has a blank ticker")
    if not selected["list_status"].eq("D").all():
        raise ProspectiveExecutionDataError(
            "official delist response contains a non-delisted status"
        )
    raw_dates = selected["delist_date"].astype("string").str.strip()
    blank = raw_dates.isna() | raw_dates.eq("") | raw_dates.str.upper().eq("NAN")
    compact = raw_dates.where(~blank)
    malformed = compact.notna() & ~compact.str.fullmatch(r"\d{8}")
    if malformed.any():
        raise ProspectiveExecutionDataError(
            "official delist response has a non-canonical delist_date"
        )
    parsed = pd.to_datetime(compact, format="%Y%m%d", errors="coerce")
    if (compact.notna() & parsed.isna()).any():
        raise ProspectiveExecutionDataError(
            "official delist response has an invalid delist_date"
        )
    selected["delist_date"] = parsed.dt.strftime("%Y-%m-%d")
    selected = selected.sort_values(
        ["ts_code", "delist_date"], na_position="last", kind="mergesort"
    ).reset_index(drop=True)
    if selected["ts_code"].duplicated().any():
        raise ProspectiveExecutionDataError(
            "official delist response has duplicate tickers"
        )
    return [
        {
            "ts_code": str(row.ts_code),
            "list_status": str(row.list_status),
            "delist_date": None if pd.isna(row.delist_date) else str(row.delist_date),
        }
        for row in selected.itertuples(index=False)
    ]


def _validate_official_delist_payload(value: Any) -> tuple[list[dict[str, Any]], str]:
    expected_keys = {
        "schema_version",
        "kind",
        "provider",
        "endpoint",
        "query",
        "retrieved_at_utc",
        "row_count",
        "rows",
    }
    if not isinstance(value, Mapping) or set(value) != expected_keys:
        raise ProspectiveExecutionDataError(
            "official delist artifact does not have the exact schema"
        )
    if type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise ProspectiveExecutionDataError("official delist artifact schema differs")
    if (
        value["kind"] != "official_delist_status"
        or value["provider"] != "tushare"
        or value["endpoint"] != "stock_basic"
        or value["query"] != _OFFICIAL_DELIST_QUERY
    ):
        raise ProspectiveExecutionDataError("official delist source identity differs")
    retrieved = _utc_text(value["retrieved_at_utc"], label="official delist retrieval")
    rows = value["rows"]
    if not isinstance(rows, list) or not all(isinstance(row, Mapping) for row in rows):
        raise ProspectiveExecutionDataError("official delist rows must be objects")
    if type(value["row_count"]) is not int or value["row_count"] != len(rows):
        raise ProspectiveExecutionDataError("official delist row count differs")
    rebuilt = pd.DataFrame(rows, columns=list(_OFFICIAL_DELIST_FIELDS))
    if rows:
        rebuilt["delist_date"] = rebuilt["delist_date"].map(
            lambda item: None if item is None else str(item).replace("-", "")
        )
    normalised = _normalise_official_delist_rows(rebuilt)
    if normalised != rows:
        raise ProspectiveExecutionDataError("official delist rows are not canonical")
    return normalised, retrieved


def _capture_official_delist_status(
    root: Path,
    *,
    sealed_source: Mapping[str, Any] | None,
) -> tuple[list[dict[str, Any]], dict[str, Any], pd.Timestamp]:
    if sealed_source is None:
        client = _official_delist_client(root)
        try:
            first_frame = _call(client, "stock_basic", **_OFFICIAL_DELIST_QUERY)
            second_frame = _call(client, "stock_basic", **_OFFICIAL_DELIST_QUERY)
        except Exception as exc:
            raise ProspectiveExecutionDataError(
                "official delist query failed"
            ) from exc
        first_rows = _normalise_official_delist_rows(first_frame)
        second_rows = _normalise_official_delist_rows(second_frame)
        if first_rows != second_rows:
            raise ProspectiveExecutionDataError(
                "independent official delist queries differ"
            )
        rows = first_rows
        retrieved = _official_retrieved_at_utc()
        payload = {
            "schema_version": 1,
            "kind": "official_delist_status",
            "provider": "tushare",
            "endpoint": "stock_basic",
            "query": dict(_OFFICIAL_DELIST_QUERY),
            "retrieved_at_utc": retrieved,
            "row_count": len(rows),
            "rows": rows,
        }
        payload_bytes = _canonical_json_bytes(payload)
        digest = _sha256_bytes(payload_bytes)
        relative = IMMUTABLE_SOURCE_RELATIVE_ROOT / f"sha256={digest}" / "artifact"
        path = root / relative
        _write_verified(path, payload_bytes)
        binding = {
            "role": "official_delist_status",
            "provider": "tushare",
            "endpoint": "stock_basic",
            "query": dict(_OFFICIAL_DELIST_QUERY),
            "sha256": digest,
            "immutable_path": relative.as_posix(),
            "size_bytes": len(payload_bytes),
            "media_type": "application/json",
            "retrieved_at_utc": retrieved,
            "source_row_count": len(rows),
        }
        return rows, binding, _utc(retrieved, label="official delist retrieval")

    expected_keys = {
        "role",
        "provider",
        "endpoint",
        "query",
        "sha256",
        "immutable_path",
        "size_bytes",
        "media_type",
        "retrieved_at_utc",
        "source_row_count",
        "selected_security_count",
    }
    if set(sealed_source) != expected_keys:
        raise ProspectiveExecutionDataError(
            "sealed official delist source does not have the exact schema"
        )
    if (
        sealed_source.get("role") != "official_delist_status"
        or sealed_source.get("provider") != "tushare"
        or sealed_source.get("endpoint") != "stock_basic"
        or sealed_source.get("query") != _OFFICIAL_DELIST_QUERY
        or sealed_source.get("media_type") != "application/json"
    ):
        raise ProspectiveExecutionDataError("sealed official delist source identity differs")
    try:
        path = _resolve_immutable_artifact(root, sealed_source)
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ProspectiveExecutionDataError(
            "sealed official delist artifact is invalid"
        ) from exc
    rows, retrieved = _validate_official_delist_payload(payload)
    if (
        sealed_source.get("retrieved_at_utc") != retrieved
        or type(sealed_source.get("source_row_count")) is not int
        or sealed_source.get("source_row_count") != len(rows)
    ):
        raise ProspectiveExecutionDataError("sealed official delist metadata differs")
    return rows, dict(sealed_source), _utc(retrieved, label="official delist retrieval")


def _delist_dates(
    root: Path,
    source: ProspectiveInputSnapshot,
    tickers: set[str],
    *,
    sealed_source: Mapping[str, Any] | None = None,
) -> tuple[dict[str, str], dict[str, Any], pd.Timestamp]:
    frozen_dates, frozen_source = _frozen_delist_dates(root, source, tickers)
    sealed_official: Mapping[str, Any] | None = None
    if sealed_source is not None:
        expected_keys = {"role", "frozen_projection", "official_stock_basic"}
        if set(sealed_source) != expected_keys or sealed_source.get("role") != "delists":
            raise ProspectiveExecutionDataError(
                "sealed combined delist source does not have the exact schema"
            )
        if sealed_source.get("frozen_projection") != frozen_source:
            raise ProspectiveExecutionDataError("sealed frozen delist projection differs")
        official_value = sealed_source.get("official_stock_basic")
        if not isinstance(official_value, Mapping):
            raise ProspectiveExecutionDataError("sealed official delist source is invalid")
        sealed_official = official_value
    official_rows, official_source, retrieved = _capture_official_delist_status(
        root,
        sealed_source=sealed_official,
    )
    official_dates = {
        str(row["ts_code"]): str(row["delist_date"])
        for row in official_rows
        if row["ts_code"] in tickers and row["delist_date"] is not None
    }
    for ticker, official_date in official_dates.items():
        frozen_date = frozen_dates.get(ticker)
        if frozen_date is not None and frozen_date != official_date:
            raise ProspectiveExecutionDataError(
                f"official/frozen delist dates conflict for {ticker}"
            )
    dates = {**frozen_dates, **official_dates}
    official_source["selected_security_count"] = len(official_dates)
    contract = {
        "role": "delists",
        "frozen_projection": frozen_source,
        "official_stock_basic": official_source,
    }
    if sealed_source is not None and contract != sealed_source:
        raise ProspectiveExecutionDataError("combined delist source differs from sealed bytes")
    return dates, contract, retrieved


def _float_hex(value: Any, *, label: str, positive: bool = False, nonnegative: bool = False) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ProspectiveExecutionDataError(f"invalid {label}") from exc
    if not math.isfinite(number) or (positive and number <= 0) or (nonnegative and number < 0):
        raise ProspectiveExecutionDataError(f"invalid {label}")
    return number.hex()


def _source_rows(source: ProspectiveInputSnapshot) -> pd.DataFrame:
    frame = source.frame.copy()
    required = {
        "ticker",
        "date",
        "eligible",
        "universe_member",
        "close",
        "close_adj",
        "adv_20",
        "volatility_20",
        "adj_factor",
        "adj_calibration_multiplier",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ProspectiveExecutionDataError(f"decision source lacks execution anchors: {missing}")
    frame["ticker"] = frame["ticker"].astype("string").str.strip()
    frame["date"] = frame["date"].astype("string")
    if frame["ticker"].eq("").any() or frame["ticker"].duplicated().any():
        raise ProspectiveExecutionDataError("decision source ticker roster is invalid")
    return frame


def _fallback_execution_inputs(
    root: Path,
    checkpoint: Mapping[str, Any],
    *,
    calendar_sessions: Sequence[str],
    signal_index: int,
    tickers: set[str],
    deadline: pd.Timestamp,
    sealed_sources: Mapping[str, Mapping[str, Any]] | None = None,
) -> tuple[dict[str, tuple[float, float, str]], list[dict[str, Any]]]:
    """Rebuild causal ADV/volatility for prior holdings outside current membership.

    Membership exits are normal.  They must not make an existing sleeve
    impossible to liquidate, but neither may a caller hand-fill execution
    inputs.  We therefore use the last 21 checkpointed observations no later
    than the signal and apply the same 20-observation definitions as the
    decision snapshot (RMB ADV; sample standard deviation of adjusted closes).
    """

    if not tickers:
        return {}, []
    history: dict[str, list[tuple[str, float, float]]] = {
        ticker: [] for ticker in tickers
    }
    sources: dict[str, dict[str, Any]] = {}
    for session in reversed(tuple(calendar_sessions[: signal_index + 1])):
        daily, daily_source, _ = _read_partition(
            root,
            checkpoint,
            dataset="daily",
            trade_date=session,
            availability_cap=deadline,
            sealed_source=(
                sealed_sources.get(f"daily/{session}")
                if sealed_sources is not None
                else None
            ),
        )
        adj, adj_source, _ = _read_partition(
            root,
            checkpoint,
            dataset="adj_factor",
            trade_date=session,
            availability_cap=deadline,
            sealed_source=(
                sealed_sources.get(f"adj_factor/{session}")
                if sealed_sources is not None
                else None
            ),
        )
        sources[daily_source["checkpoint_key"]] = daily_source
        sources[adj_source["checkpoint_key"]] = adj_source
        factors = adj.set_index("ts_code")["adj_factor"]
        for row in daily.loc[daily["ts_code"].isin(tickers)].itertuples(index=False):
            ticker = str(row.ts_code)
            if len(history[ticker]) >= 21 or ticker not in factors.index:
                continue
            factor = float(factors.loc[ticker])
            close = float(row.close) * factor
            amount_rmb = float(row.amount) * 1_000.0
            if not (
                math.isfinite(close)
                and close > 0
                and math.isfinite(amount_rmb)
                and amount_rmb > 0
            ):
                raise ProspectiveExecutionDataError(
                    f"invalid prior execution input observation for {ticker}/{session}"
                )
            history[ticker].append((session, close, amount_rmb))
        if all(len(values) >= 21 for values in history.values()):
            break
    result: dict[str, tuple[float, float, str]] = {}
    for ticker, reverse_rows in history.items():
        rows = sorted(reverse_rows)
        if len(rows) < 21:
            raise ProspectiveExecutionDataError(
                f"fewer than 21 checkpointed observations for prior holding {ticker}"
            )
        rows = rows[-21:]
        closes = pd.Series([row[1] for row in rows], dtype="float64")
        returns = closes.pct_change(fill_method=None).iloc[-20:]
        amounts = pd.Series([row[2] for row in rows[-20:]], dtype="float64")
        volatility = float(returns.std(ddof=1))
        adv = float(amounts.mean())
        if not (math.isfinite(volatility) and volatility >= 0 and math.isfinite(adv) and adv > 0):
            raise ProspectiveExecutionDataError(
                f"prior execution inputs are not finite for {ticker}"
            )
        result[ticker] = (adv, volatility, rows[-1][0])
    return result, list(sources.values())


def _build_observations(
    source: ProspectiveInputSnapshot,
    generation: GenerationResult,
    window: Sequence[str],
    daily_frames: Mapping[str, pd.DataFrame],
    adj_frames: Mapping[str, pd.DataFrame],
    suspension_frame: pd.DataFrame,
    delists: Mapping[str, str],
    previous: SleeveAccountState | None,
    fallback_execution_inputs: Mapping[str, tuple[float, float, str]],
) -> tuple[tuple[str, ...], tuple[ExecutionObservation, ...]]:
    decision = _source_rows(source)
    benchmark = tuple(
        sorted(
            decision.loc[
                decision["universe_member"].fillna(False).astype(bool)
                & decision["eligible"].fillna(False).astype(bool),
                "ticker",
            ].astype(str)
        )
    )
    if not benchmark or len(set(benchmark)) != len(benchmark):
        raise ProspectiveExecutionDataError("decision-derived benchmark roster is empty or duplicated")
    due_targets = set(generation.sleeve_plans[generation.due_offset]["targets_ppm"])
    prior_tickers: set[str] = set()
    if previous is not None:
        if previous.deployment_sha256 != generation.deployment_sha256:
            raise ProspectiveExecutionDataError("previous account uses a different deployment")
        if previous.offset != generation.due_offset:
            raise ProspectiveExecutionDataError("previous account is for another offset")
        prior_tickers = {row.ticker for row in previous.positions}
    row_tickers = set(benchmark) | due_targets | prior_tickers
    if not due_targets.issubset(set(decision["ticker"].astype(str))):
        raise ProspectiveExecutionDataError("generated targets are absent from decision-time target data")

    daily_by_key: dict[tuple[str, str], pd.Series] = {}
    adj_by_key: dict[tuple[str, str], float] = {}
    for session in window:
        daily = daily_frames[session]
        adj = adj_frames[session]
        for _, row in daily.loc[daily["ts_code"].isin(row_tickers)].iterrows():
            daily_by_key[(session, str(row["ts_code"]))] = row
        for _, row in adj.loc[adj["ts_code"].isin(row_tickers)].iterrows():
            adj_by_key[(session, str(row["ts_code"]))] = float(row["adj_factor"])
    presence = set(daily_by_key)
    blocked = _suspension_flags(
        suspension_frame,
        sessions=window,
        tickers=row_tickers,
        daily_presence=presence,
    )
    decision_by_ticker = decision.set_index("ticker", drop=False)
    prior_positions = {row.ticker: row for row in previous.positions} if previous is not None else {}
    rows: list[ExecutionObservation] = []
    start = window[0]
    for ticker in sorted(row_tickers):
        anchor = decision_by_ticker.loc[ticker] if ticker in decision_by_ticker.index else None
        previous_raw: float | None = None
        previous_adjusted: float | None = None
        calibration: float | None = None
        if anchor is not None:
            previous_raw = float(anchor["close"])
            previous_adjusted = float(anchor["close_adj"])
            calibration = float(anchor["adj_calibration_multiplier"])
            anchor_factor = float(anchor["adj_factor"])
            if not (
                math.isfinite(previous_raw)
                and previous_raw > 0
                and math.isfinite(previous_adjusted)
                and previous_adjusted > 0
                and math.isfinite(calibration)
                and calibration > 0
                and math.isfinite(anchor_factor)
                and anchor_factor > 0
                and math.isclose(
                    previous_adjusted,
                    previous_raw * anchor_factor * calibration,
                    rel_tol=0.005,
                    abs_tol=1e-10,
                )
            ):
                raise ProspectiveExecutionDataError(f"invalid decision price anchor for {ticker}")
        for session in window:
            daily = daily_by_key.get((session, ticker))
            is_delisted = ticker in delists and session >= delists[ticker]
            is_suspended = (session, ticker) in blocked and not is_delisted
            open_adj: float | None = None
            if daily is not None and not is_delisted:
                factor = adj_by_key.get((session, ticker))
                if factor is None or not math.isfinite(factor) or factor <= 0:
                    raise ProspectiveExecutionDataError(
                        f"daily observation lacks a positive adj_factor for {ticker}/{session}"
                    )
                raw_open = float(daily["open"])
                raw_close = float(daily["close"])
                raw_pre_close = float(daily["pre_close"])
                if previous_raw is None or previous_adjusted is None:
                    position = prior_positions.get(ticker)
                    if position is None:
                        raise ProspectiveExecutionDataError(f"no adjusted-price anchor for {ticker}")
                    state_price = float.fromhex(position.last_price_hex)
                    # At a shared, tradable boundary the sealed prior outcome
                    # provides this exact open.  If the boundary was suspended,
                    # the first later raw row is anchored through its adjusted
                    # pre-close to the carried last normal mark.
                    if session == start:
                        effective = state_price / raw_open
                        open_adj = state_price
                    else:
                        effective = state_price / raw_pre_close
                        open_adj = raw_open * effective
                    previous_adjusted = raw_close * effective
                    previous_raw = raw_close
                    calibration = effective / factor
                else:
                    if calibration is None:
                        raise ProspectiveExecutionDataError(
                            f"missing adjustment calibration for {ticker}/{session}"
                        )
                    effective = factor * calibration
                    adjusted_pre_close = raw_pre_close * effective
                    if not math.isclose(
                        adjusted_pre_close,
                        previous_adjusted,
                        rel_tol=0.005,
                        abs_tol=0.02 * effective,
                    ):
                        raise ProspectiveExecutionDataError(
                            f"adjusted pre-close continuity failed for {ticker}/{session}"
                        )
                    adjusted_close = raw_close * effective
                    open_adj = raw_open * effective
                    previous_adjusted = adjusted_close
                    previous_raw = raw_close
                if is_suspended:
                    open_adj = None
            elif daily is None and not (is_suspended or is_delisted):
                raise ProspectiveExecutionDataError(
                    f"missing daily row without suspension/delist proof for {ticker}/{session}"
                )
            if session == start:
                if anchor is not None:
                    adv_value = anchor["adv_20"]
                    volatility_value = anchor["volatility_20"]
                    input_date: str | None = generation.signal_date
                elif ticker in fallback_execution_inputs:
                    adv_value, volatility_value, input_date = fallback_execution_inputs[ticker]
                else:
                    raise ProspectiveExecutionDataError(
                        f"prior-only security {ticker} lacks checkpointed causal ADV/volatility"
                    )
                adv = _float_hex(adv_value, label=f"{ticker}.adv_20", positive=True)
                volatility = _float_hex(
                    volatility_value, label=f"{ticker}.volatility_20", nonnegative=True
                )
            else:
                adv = None
                volatility = None
                input_date = None
            limit_up = False
            limit_down = False
            if daily is not None:
                one_price = math.isclose(
                    float(daily["high"]), float(daily["low"]), rel_tol=0.0, abs_tol=1e-12
                )
                limit_up = one_price and float(daily["pct_chg"]) > 0
                limit_down = one_price and float(daily["pct_chg"]) < 0
            rows.append(
                ExecutionObservation(
                    date=session,
                    ticker=ticker,
                    open_adj_hex=(open_adj.hex() if open_adj is not None else None),
                    adv_20_asof_hex=adv,
                    volatility_20_asof_hex=volatility,
                    execution_input_date=input_date,
                    is_one_price_limit_up=limit_up,
                    is_one_price_limit_down=limit_down,
                    is_suspended=is_suspended,
                    is_delisted=is_delisted,
                )
            )
    return benchmark, tuple(rows)


def _benchmark_endpoint_pair_complete(
    start: ExecutionObservation,
    end: ExecutionObservation,
) -> bool:
    return (
        start.open_adj_hex is not None
        and end.open_adj_hex is not None
        and not start.is_suspended
        and not end.is_suspended
        and not start.is_delisted
        and not end.is_delisted
    )


def _matching_execution_store_entries(
    root: Path,
    generation: GenerationResult,
    *,
    source_data_snapshot_sha256: str,
    previous_account_state_sha256: str | None,
) -> list[tuple[Path, dict[str, Any], bool]]:
    """Find exact complete/source-first entries and reject malformed store rows."""

    execution_root = root / EXECUTION_RELATIVE_ROOT
    if not execution_root.exists():
        return []
    if execution_root.is_symlink() or not execution_root.is_dir():
        raise ProspectiveExecutionDataError("execution store is not a canonical directory")
    matches: list[tuple[Path, dict[str, Any], bool]] = []
    for directory in sorted(execution_root.iterdir(), key=lambda path: path.name):
        if directory.is_symlink() or not directory.is_dir() or not _SHA256_RE.fullmatch(
            directory.name
        ):
            raise ProspectiveExecutionDataError(
                f"execution store contains an invalid entry: {directory}"
            )
        snapshot_path = directory / "snapshot.json"
        sources_path = directory / "sources.json"
        snapshot_present = snapshot_path.exists() or snapshot_path.is_symlink()
        sources_present = sources_path.exists() or sources_path.is_symlink()
        # A process may die after creating the content-addressed directory but
        # before linking its first complete file.  With no claimed identity it
        # is an uncommitted partial, not evidence and not a candidate.
        if not snapshot_present and not sources_present:
            continue
        if snapshot_present and not sources_present:
            raise ProspectiveExecutionDataError(
                f"execution bundle published snapshot before sources: {directory}"
            )
        if sources_path.is_symlink() or not sources_path.is_file():
            raise ProspectiveExecutionDataError(
                f"execution source contract is not a regular file: {sources_path}"
            )
        sources_raw = sources_path.read_bytes()
        try:
            sources = json.loads(sources_raw.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ProspectiveExecutionDataError(
                f"execution source contract is unreadable: {sources_path}"
            ) from exc
        if (
            not isinstance(sources, dict)
            or sources_raw != _canonical_json_bytes(sources)
            or set(sources) != _EXECUTION_SOURCE_KEYS
        ):
            raise ProspectiveExecutionDataError(
                f"execution source contract is malformed: {sources_path}"
            )
        identity = (
            sources.get("generation_result_sha256"),
            sources.get("source_data_snapshot_sha256"),
            sources.get("previous_account_state_sha256"),
        )
        expected = (
            generation.result_sha256,
            source_data_snapshot_sha256,
            previous_account_state_sha256,
        )
        if snapshot_present:
            if snapshot_path.is_symlink() or not snapshot_path.is_file():
                raise ProspectiveExecutionDataError(
                    f"execution snapshot is not a regular file: {snapshot_path}"
                )
            # Structural validation applies to every committed execution, not
            # only the current decision, so unrelated corruption cannot hide.
            _load_snapshot_files(directory)
        if identity == expected:
            matches.append((directory, sources, snapshot_present))
    return matches


def build_prospective_execution_snapshot(
    project_root: str | Path,
    generation_result: GenerationResult | Mapping[str, Any],
    *,
    source_data_snapshot_sha256: str,
    previous_account_state: SleeveAccountState | Mapping[str, Any] | None = None,
    available_at_utc: str | pd.Timestamp | None = None,
    _materialize: bool = True,
    _suspension_artifact_sha256: str | None = None,
    _sealed_source_contract: Mapping[str, Any] | None = None,
    _resume_existing: bool = True,
) -> ProspectiveExecutionDataSnapshot:
    """Build one create-only execution/outcome source snapshot.

    ``source_data_snapshot_sha256`` is taken from the already sealed decision
    plan.  A benchmark roster is never accepted as an argument: it is derived
    from that snapshot's decision-time ``universe_member`` rows.
    """

    root = Path(project_root).expanduser().resolve()
    generation = _generation(generation_result)
    previous = _account_state(previous_account_state)
    sealed_contract = (
        dict(_sealed_source_contract)
        if isinstance(_sealed_source_contract, Mapping)
        else None
    )
    if _sealed_source_contract is not None and sealed_contract is None:
        raise ProspectiveExecutionDataError("sealed execution source contract must be an object")
    source_sha = _require_sha(source_data_snapshot_sha256, label="source_data_snapshot_sha256")
    cap = _utc(available_at_utc, label="available_at_utc") if available_at_utc is not None else None
    deadline_text = _trade_deadline(generation.trade_date)
    deadline = _utc(deadline_text, label="trade deadline")
    source_path = root / PROSPECTIVE_RELATIVE_ROOT / "inputs" / source_sha
    try:
        source = load_prospective_input_snapshot(source_path)
    except Exception as exc:
        raise ProspectiveExecutionDataError("decision input failed independent source rebuild") from exc
    if source.snapshot_sha256 != source_sha:
        raise ProspectiveExecutionDataError("decision input path/hash mismatch")
    _verify_generation_input_binding(generation, source, deadline=deadline_text)
    source_inputs_available = _utc(
        source.inputs_available_at_utc, label="decision inputs availability"
    )
    source_build_completed = _utc(
        source.build_completed_at_utc, label="decision build completion"
    )
    if not source_inputs_available <= source_build_completed <= deadline:
        raise ProspectiveExecutionDataError(
            "decision input bundle was not durably published by the trade deadline"
        )

    if sealed_contract is None and _materialize and _resume_existing:
        previous_sha = previous.state_sha256 if previous is not None else None
        matches = _matching_execution_store_entries(
            root,
            generation,
            source_data_snapshot_sha256=source_sha,
            previous_account_state_sha256=previous_sha,
        )
        if len(matches) > 1:
            raise ProspectiveExecutionDataError(
                "multiple execution bundles match one sealed decision"
            )
        if matches:
            directory, existing_contract, snapshot_present = matches[0]
            existing_suspensions = existing_contract.get("suspensions")
            if not isinstance(existing_suspensions, Mapping):
                raise ProspectiveExecutionDataError(
                    "partial execution source lacks its suspension binding"
                )
            rebuilt = build_prospective_execution_snapshot(
                root,
                generation,
                source_data_snapshot_sha256=source_sha,
                previous_account_state=previous,
                _materialize=False,
                _suspension_artifact_sha256=str(
                    existing_suspensions.get("artifact_sha256") or ""
                ),
                _sealed_source_contract=existing_contract,
                _resume_existing=False,
            )
            if rebuilt.directory != directory:
                raise ProspectiveExecutionDataError(
                    "partial execution directory differs from sealed source rebuild"
                )
            source_bytes = _canonical_json_bytes(rebuilt.source_contract)
            snapshot_bytes = _canonical_json_bytes(rebuilt.snapshot.to_dict())
            _write_create_only(rebuilt.sources_path, source_bytes)
            _write_create_only(rebuilt.snapshot_path, snapshot_bytes)
            if snapshot_present:
                _load_snapshot_files(directory)
            return rebuilt

    checkpoint_path = root / "runtime/data/raw/checkpoint.json"
    sealed_calendar_sources: list[Mapping[str, Any]] | None = None
    sealed_raw_by_key: dict[str, Mapping[str, Any]] | None = None
    if sealed_contract is None:
        loaded_checkpoint_path, checkpoint = _checkpoint(root)
        if loaded_checkpoint_path.resolve() != checkpoint_path.resolve():
            raise ProspectiveExecutionDataError("raw checkpoint path is not canonical")
    else:
        if sealed_contract.get("raw_checkpoint_path") != _relative(checkpoint_path, root):
            raise ProspectiveExecutionDataError("sealed raw checkpoint diagnostic path differs")
        calendar_value = sealed_contract.get("calendar")
        raw_values = sealed_contract.get("raw_partitions")
        if not isinstance(calendar_value, Mapping) or not isinstance(raw_values, list) or not all(
            isinstance(row, Mapping) for row in raw_values
        ):
            raise ProspectiveExecutionDataError("sealed calendar/raw source contracts are invalid")
        sealed_calendar_sources = [calendar_value]
        sealed_raw_by_key = {
            str(row.get("checkpoint_key") or ""): row for row in raw_values
        }
        if (
            len(sealed_raw_by_key) != len(raw_values)
            or "" in sealed_raw_by_key
        ):
            raise ProspectiveExecutionDataError("sealed raw partition keys are invalid or duplicated")
        checkpoint = {}
    sessions, window, calendar_source, calendar_completed = _select_calendar(
        root,
        checkpoint,
        generation,
        source.calendar_sessions,
        deadline=deadline,
        sealed_sources=sealed_calendar_sources,
    )
    raw_sources: dict[str, dict[str, Any]] = {}
    raw_completed: dict[str, list[pd.Timestamp]] = {session: [] for session in window}
    daily_frames: dict[str, pd.DataFrame] = {}
    adj_frames: dict[str, pd.DataFrame] = {}
    for session in window:
        # daily_basic is already bound in the sealed decision snapshot.  It has
        # no execution/outcome field, so requiring it again for every holding
        # session would add failure modes without contributing evidence.
        for dataset in ("daily", "adj_factor"):
            frame, item, completed = _read_partition(
                root,
                checkpoint,
                dataset=dataset,
                trade_date=session,
                availability_cap=cap,
                sealed_source=(
                    sealed_raw_by_key.get(f"{dataset}/{session}")
                    if sealed_raw_by_key is not None
                    else None
                ),
            )
            raw_sources[item["checkpoint_key"]] = item
            raw_completed[session].append(completed)
            if dataset == "daily":
                daily_frames[session] = frame
            elif dataset == "adj_factor":
                adj_frames[session] = frame
    start_completed = max(raw_completed[window[0]])
    end_completed = max(raw_completed[window[-1]])
    if start_completed > end_completed:
        raise ProspectiveExecutionDataError("raw checkpoint completion order contradicts the holding window")
    decision_frame = _source_rows(source)
    decision_tickers = set(decision_frame["ticker"].astype(str))
    prior_tickers = (
        {row.ticker for row in previous.positions} if previous is not None else set()
    )
    fallback_inputs, fallback_sources = _fallback_execution_inputs(
        root,
        checkpoint,
        calendar_sessions=sessions,
        signal_index=generation.calendar_index,
        tickers=prior_tickers - decision_tickers,
        deadline=deadline,
        sealed_sources=sealed_raw_by_key,
    )
    for item in fallback_sources:
        raw_sources[item["checkpoint_key"]] = item
    suspension_artifact = _suspension_artifact_sha256
    if sealed_contract is not None:
        sealed_suspensions = sealed_contract.get("suspensions")
        if not isinstance(sealed_suspensions, Mapping):
            raise ProspectiveExecutionDataError("sealed suspension source contract is invalid")
        contract_artifact = _require_sha(
            sealed_suspensions.get("artifact_sha256"),
            label="sealed suspension artifact key",
        )
        if suspension_artifact is not None and suspension_artifact != contract_artifact:
            raise ProspectiveExecutionDataError("sealed suspension artifact keys differ")
        suspension_artifact = contract_artifact
    suspension_frame, suspension_source, suspension_completed = _load_suspensions(
        root,
        start=window[0],
        end=window[-1],
        availability_cap=cap,
        minimum_completed_at=end_completed,
        artifact_sha256=suspension_artifact,
    )
    candidate_tickers = set(decision_frame["ticker"].astype(str))
    candidate_tickers.update(generation.sleeve_plans[generation.due_offset]["targets_ppm"])
    if previous is not None:
        candidate_tickers.update(row.ticker for row in previous.positions)
    sealed_delists: Mapping[str, Any] | None = None
    if sealed_contract is not None:
        sealed_delist_value = sealed_contract.get("delists")
        if not isinstance(sealed_delist_value, Mapping):
            raise ProspectiveExecutionDataError("sealed delist source contract is invalid")
        sealed_delists = sealed_delist_value
    delists, delist_source, delist_completed = _delist_dates(
        root,
        source,
        candidate_tickers,
        sealed_source=sealed_delists,
    )
    benchmark, observations = _build_observations(
        source,
        generation,
        window,
        daily_frames,
        adj_frames,
        suspension_frame,
        delists,
        previous,
        fallback_inputs,
    )
    benchmark_sha = _sha256_bytes(_canonical_json_bytes(list(benchmark)))
    endpoints = {(row.date, row.ticker): row for row in observations}
    benchmark_complete = sum(
        _benchmark_endpoint_pair_complete(
            endpoints[(window[0], ticker)],
            endpoints[(window[-1], ticker)],
        )
        for ticker in benchmark
    )
    benchmark_coverage_ppm = benchmark_complete * 1_000_000 // len(benchmark)
    if benchmark_coverage_ppm < MINIMUM_BENCHMARK_COVERAGE_PPM:
        raise ProspectiveExecutionDataError(
            "decision-time benchmark endpoint coverage is below the frozen 95% minimum"
        )
    observation_available = max(
        suspension_completed,
        delist_completed,
        *(value for values in raw_completed.values() for value in values),
    )
    if cap is not None and observation_available > cap:
        raise ProspectiveExecutionDataError("execution evidence exceeds requested availability cutoff")
    source_contract: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "kind": KIND,
        "protocol_release": "5.0",
        "generation_result_sha256": generation.result_sha256,
        "source_data_snapshot_sha256": source.snapshot_sha256,
        "target_input_snapshot_sha256": generation.input_snapshot_sha256,
        "previous_account_state_sha256": previous.state_sha256 if previous is not None else None,
        "benchmark_tickers_sha256": benchmark_sha,
        "benchmark_coverage": {
            "expected_count": len(benchmark),
            "complete_count": benchmark_complete,
            "coverage_ppm": benchmark_coverage_ppm,
            "minimum_coverage_ppm": MINIMUM_BENCHMARK_COVERAGE_PPM,
        },
        "selected_market_max_date": window[-1],
        "raw_checkpoint_path": _relative(checkpoint_path, root),
        "decision_input": {
            "path": _relative(source.directory, root),
            "snapshot_sha256": source.snapshot_sha256,
            "inputs_available_at_utc": source.inputs_available_at_utc,
            "build_completed_at_utc": source.build_completed_at_utc,
        },
        "calendar": calendar_source,
        "raw_partitions": sorted(
            raw_sources.values(), key=lambda row: (row["trade_date"], row["dataset"])
        ),
        "suspensions": suspension_source,
        "delists": delist_source,
    }
    if sealed_contract is not None and source_contract != sealed_contract:
        raise ProspectiveExecutionDataError(
            "execution source contract differs from immutable source rebuild"
        )
    source_bytes = _canonical_json_bytes(source_contract)
    source_contract_sha = _sha256_bytes(source_bytes)
    try:
        snapshot = ExecutionSnapshot(
            generation_result_sha256=generation.result_sha256,
            execution_source_sha256=source_contract_sha,
            official_calendar_sha256=_sha256_bytes(
                _canonical_json_bytes(
                    {
                        "schema_version": 1,
                        "anchor": sessions[0],
                        "count": len(sessions),
                        "sessions": list(sessions),
                    }
                )
            ),
            signal_date=generation.signal_date,
            holding_start_date=window[0],
            holding_end_date=window[-1],
            calendar_sessions=sessions,
            benchmark_tickers=benchmark,
            benchmark_tickers_sha256=benchmark_sha,
            rows=observations,
            calendar_available_at_utc=_utc_text(
                calendar_completed, label="calendar availability"
            ),
            decision_inputs_available_at_utc=_utc_text(
                source_inputs_available, label="decision availability"
            ),
            trade_deadline_utc=deadline_text,
            start_open_available_at_utc=_utc_text(
                start_completed, label="start availability"
            ),
            end_open_available_at_utc=_utc_text(end_completed, label="end availability"),
            observation_available_at_utc=_utc_text(
                observation_available, label="observation availability"
            ),
        )
    except ProspectiveExecutionError as exc:
        raise ProspectiveExecutionDataError("pure execution snapshot rejected rebuilt evidence") from exc
    directory = root / EXECUTION_RELATIVE_ROOT / snapshot.snapshot_sha256
    snapshot_path = directory / "snapshot.json"
    sources_path = directory / "sources.json"
    if _materialize:
        _write_create_only(sources_path, source_bytes)
        # ``snapshot.json`` is the bundle commit marker.  Publishing it last
        # means a crash can leave only a source-first partial, which the next
        # invocation can deterministically rebuild without another provider
        # call or a second execution identity.
        _write_create_only(snapshot_path, _canonical_json_bytes(snapshot.to_dict()))
    return ProspectiveExecutionDataSnapshot(
        snapshot=snapshot,
        directory=directory,
        snapshot_path=snapshot_path,
        sources_path=sources_path,
        source_contract=source_contract,
    )


def _load_snapshot_files(path: str | Path) -> ProspectiveExecutionDataSnapshot:
    directory = Path(path).expanduser().resolve()
    snapshot_path = directory / "snapshot.json"
    sources_path = directory / "sources.json"
    snapshot_raw = snapshot_path.read_bytes() if snapshot_path.is_file() else b""
    sources_raw = sources_path.read_bytes() if sources_path.is_file() else b""
    try:
        snapshot_value = json.loads(snapshot_raw.decode("utf-8"))
        sources_value = json.loads(sources_raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ProspectiveExecutionDataError("execution bundle contains unreadable JSON") from exc
    if (
        not isinstance(snapshot_value, dict)
        or snapshot_raw != _canonical_json_bytes(snapshot_value)
        or not isinstance(sources_value, dict)
        or sources_raw != _canonical_json_bytes(sources_value)
    ):
        raise ProspectiveExecutionDataError("execution bundle JSON is not canonical")
    try:
        snapshot = ExecutionSnapshot.from_mapping(snapshot_value)
    except Exception as exc:
        raise ProspectiveExecutionDataError("execution snapshot contract is invalid") from exc
    if directory.name != snapshot.snapshot_sha256 or not _SHA256_RE.fullmatch(directory.name):
        raise ProspectiveExecutionDataError("execution directory does not match snapshot hash")
    if _sha256_bytes(sources_raw) != snapshot.execution_source_sha256:
        raise ProspectiveExecutionDataError("execution source contract hash mismatch")
    if (
        set(sources_value) != _EXECUTION_SOURCE_KEYS
        or len(sources_value) != len(_EXECUTION_SOURCE_KEYS)
    ):
        raise ProspectiveExecutionDataError("execution source contract has a non-exact schema")
    coverage = sources_value.get("benchmark_coverage")
    decision_input = sources_value.get("decision_input")
    suspensions = sources_value.get("suspensions")
    if (
        not isinstance(coverage, Mapping)
        or set(coverage)
        != {
            "expected_count",
            "complete_count",
            "coverage_ppm",
            "minimum_coverage_ppm",
        }
        or not isinstance(decision_input, Mapping)
        or set(decision_input)
        != {
            "path",
            "snapshot_sha256",
            "inputs_available_at_utc",
            "build_completed_at_utc",
        }
        or not isinstance(suspensions, Mapping)
        or "artifact_sha256" not in suspensions
        or not isinstance(sources_value.get("raw_partitions"), list)
    ):
        raise ProspectiveExecutionDataError("execution source nested contracts are invalid")
    endpoints = {(row.date, row.ticker): row for row in snapshot.rows}
    complete = sum(
        _benchmark_endpoint_pair_complete(
            endpoints[(snapshot.holding_start_date, ticker)],
            endpoints[(snapshot.holding_end_date, ticker)],
        )
        for ticker in snapshot.benchmark_tickers
    )
    expected_coverage = complete * 1_000_000 // len(snapshot.benchmark_tickers)
    if (
        type(sources_value["schema_version"]) is not int
        or sources_value["schema_version"] != SCHEMA_VERSION
        or sources_value["kind"] != KIND
        or sources_value["generation_result_sha256"] != snapshot.generation_result_sha256
        or sources_value["benchmark_tickers_sha256"] != snapshot.benchmark_tickers_sha256
        or sources_value["selected_market_max_date"] != snapshot.holding_end_date
        or decision_input.get("snapshot_sha256")
        != sources_value["source_data_snapshot_sha256"]
        or coverage.get("expected_count") != len(snapshot.benchmark_tickers)
        or coverage.get("complete_count") != complete
        or coverage.get("coverage_ppm") != expected_coverage
        or coverage.get("minimum_coverage_ppm") != MINIMUM_BENCHMARK_COVERAGE_PPM
        or expected_coverage < MINIMUM_BENCHMARK_COVERAGE_PPM
    ):
        raise ProspectiveExecutionDataError("execution source contract bindings differ")
    return ProspectiveExecutionDataSnapshot(
        snapshot=snapshot,
        directory=directory,
        snapshot_path=snapshot_path,
        sources_path=sources_path,
        source_contract=sources_value,
    )


def load_prospective_execution_snapshot(
    path: str | Path,
    generation_result: GenerationResult | Mapping[str, Any],
    *,
    previous_account_state: SleeveAccountState | Mapping[str, Any] | None = None,
) -> ProspectiveExecutionDataSnapshot:
    """Load and independently rebuild a source-backed execution snapshot."""

    loaded = _load_snapshot_files(path)
    generation = _generation(generation_result)
    previous = _account_state(previous_account_state)
    try:
        project_root = loaded.directory.parents[4]
    except IndexError as exc:
        raise ProspectiveExecutionDataError("execution snapshot is outside canonical layout") from exc
    expected = (
        project_root / EXECUTION_RELATIVE_ROOT / loaded.snapshot.snapshot_sha256
    ).resolve()
    if loaded.directory != expected:
        raise ProspectiveExecutionDataError("execution snapshot is outside canonical store")
    if generation.result_sha256 != loaded.snapshot.generation_result_sha256:
        raise ProspectiveExecutionDataError("loader received a different generation result")
    expected_previous = loaded.source_contract["previous_account_state_sha256"]
    actual_previous = previous.state_sha256 if previous is not None else None
    if expected_previous != actual_previous:
        raise ProspectiveExecutionDataError("loader received a different previous account state")
    rebuilt = build_prospective_execution_snapshot(
        project_root,
        generation,
        source_data_snapshot_sha256=str(
            loaded.source_contract["source_data_snapshot_sha256"]
        ),
        previous_account_state=previous,
        available_at_utc=loaded.snapshot.observation_available_at_utc,
        _materialize=False,
        _suspension_artifact_sha256=str(
            loaded.source_contract["suspensions"]["artifact_sha256"]
        ),
        _sealed_source_contract=loaded.source_contract,
    )
    if (
        rebuilt.snapshot.snapshot_sha256 != loaded.snapshot.snapshot_sha256
        or _canonical_json_bytes(rebuilt.source_contract)
        != loaded.sources_path.read_bytes()
        or _canonical_json_bytes(rebuilt.snapshot.to_dict())
        != loaded.snapshot_path.read_bytes()
    ):
        raise ProspectiveExecutionDataError(
            "execution snapshot differs from independent source rebuild"
        )
    return loaded


__all__ = [
    "EXECUTION_RELATIVE_ROOT",
    "MINIMUM_BENCHMARK_COVERAGE_PPM",
    "SUSPENSION_FULL_START_DATE",
    "ProspectiveExecutionDataError",
    "ProspectiveExecutionDataSnapshot",
    "build_prospective_execution_snapshot",
    "inspect_prospective_execution_sources",
    "load_prospective_execution_snapshot",
]

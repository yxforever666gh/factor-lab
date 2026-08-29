"""Zero-write readiness inspection for the prospective research route.

This module is deliberately an observer, not a builder.  It reads the frozen
ledger snapshot, content-addressed calendar, raw checkpoints, and already
sealed membership artifacts without acquiring a lock, creating a cache, or
materialising CAS bytes.  Callers that require authoritative ledger replay
must perform that replay separately before acting on this report.
"""

from __future__ import annotations

from datetime import date, datetime, time, timezone
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Mapping, Sequence
import unicodedata
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow.parquet as pq

from .sources import (
    PROVIDER_COMPLETION_CONTRACT_ID,
    PROVIDER_COMPLETION_DATASETS,
    provider_completion_required,
    validate_provider_completion_evidence,
)


SCHEMA_VERSION = 2
CONTRACT_ID = "factor-lab/prospective-readiness/5.9"
LEDGER_ID = "factor-lab/prospective/5.0"
FROZEN_BRIDGE_END = "2026-08-21"
CANONICAL_CALENDAR_ANCHOR = "2017-01-03"
CANONICAL_CALENDAR_COUNT = 2340
CANONICAL_CALENDAR_SHA256 = (
    "49b71c0b4482569d56b00cca8d468c3ec417379ac2b03e2d3afea32e312ef67f"
)
LIQUIDITY_SESSION_COUNT = 60
EXPECTED_MEMBERSHIP_SIZE = 500
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_RECORD_RE = re.compile(
    r"^(?P<sequence>[0-9]{16})-(?P<kind>[a-z0-9_]+)-(?P<sha>[0-9a-f]{64})\.json$"
)
_SNAPSHOT_RE = re.compile(r"^(?P<sequence>[0-9]{16})-(?P<sha>[0-9a-f]{64})\.json$")
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_UTC = timezone.utc

_PARTITION_COLUMNS = {
    "daily": {
        "ts_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "pct_chg",
        "amount",
    },
    "daily_basic": {"ts_code", "trade_date", "pe_ttm", "pb"},
    "adj_factor": {"ts_code", "trade_date", "adj_factor"},
}
_REFERENCE_COLUMNS = {"trade_date", "ts_code", "name", "industry", "list_date"}


class _InspectionError(ValueError):
    def __init__(self, code: str, component: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.component = component


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


def _sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(chunk_size):
            digest.update(block)
    return digest.hexdigest()


def _unique_pairs(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for raw_key, value in pairs:
        key = unicodedata.normalize("NFC", raw_key)
        if key in result:
            raise ValueError(f"duplicate JSON key: {key!r}")
        result[key] = value
    return result


def _reject_float(token: str) -> Any:
    raise ValueError(f"floating-point JSON token is forbidden: {token}")


def _reject_constant(token: str) -> Any:
    raise ValueError(f"non-finite JSON token is forbidden: {token}")


def _load_json(
    path: Path,
    *,
    canonical: bool = False,
    allow_finite_floats: bool = False,
) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
        options: dict[str, Any] = {"object_pairs_hook": _unique_pairs}
        if canonical:
            options["parse_constant"] = _reject_constant
            if not allow_finite_floats:
                options["parse_float"] = _reject_float
        value = json.loads(raw.decode("utf-8", errors="strict"), **options)
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        raise _InspectionError(
            "JSON_UNREADABLE", "filesystem", f"unreadable JSON object: {path}"
        ) from exc
    if not isinstance(value, dict):
        raise _InspectionError(
            "JSON_SCHEMA_INVALID", "filesystem", f"JSON value is not an object: {path}"
        )
    if canonical and _canonical_json_bytes(value) != raw:
        raise _InspectionError(
            "JSON_NOT_CANONICAL", "ledger", f"JSON bytes are not canonical: {path}"
        )
    return value


def _utc(value: datetime | str, *, label: str) -> datetime:
    try:
        parsed = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise _InspectionError(
            "TIMESTAMP_INVALID", "timing", f"{label} is not an ISO-8601 timestamp"
        ) from exc
    if parsed.tzinfo is None:
        raise _InspectionError(
            "TIMESTAMP_INVALID", "timing", f"{label} must include a UTC offset"
        )
    return parsed.tz_convert("UTC").to_pydatetime()


def _utc_text(value: datetime) -> str:
    return value.astimezone(_UTC).isoformat().replace("+00:00", "Z")


def _date_text(value: Any, *, label: str) -> str:
    try:
        parsed = date.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise _InspectionError("DATE_INVALID", "calendar", f"{label} is not YYYY-MM-DD") from exc
    text = parsed.isoformat()
    if text != str(value):
        raise _InspectionError("DATE_INVALID", "calendar", f"{label} is not canonical")
    return text


def _relative(path: Path, root: Path) -> str:
    return path.resolve().relative_to(root.resolve()).as_posix()


def _exact_file(recorded: Any, expected: Path, *, label: str) -> Path:
    path = Path(str(recorded or expected))
    try:
        resolved = path.expanduser().resolve()
    except OSError as exc:
        raise _InspectionError("PATH_INVALID", "filesystem", f"{label} path is invalid") from exc
    if resolved != expected.resolve():
        raise _InspectionError(
            "PATH_NOT_CANONICAL", "filesystem", f"{label} path is not canonical"
        )
    if path.is_symlink() or not resolved.is_file():
        raise _InspectionError("FILE_MISSING", "filesystem", f"missing regular {label}")
    return resolved


def _calendar_prefix_sha256(sessions: Sequence[str]) -> str:
    return _sha256_bytes(
        _canonical_json_bytes(
            {
                "schema_version": 1,
                "anchor": sessions[0],
                "count": len(sessions),
                "sessions": list(sessions),
            }
        )
    )


def _parquet_unique_dates(path: Path, column: str) -> list[str]:
    try:
        parquet = pq.ParquetFile(path)
        if column not in parquet.schema.names:
            raise ValueError(f"missing {column}")
        values: set[str] = set()
        for batch in parquet.iter_batches(columns=[column], batch_size=262_144):
            parsed = pd.to_datetime(batch.column(0).to_pandas(), errors="coerce")
            if parsed.isna().any():
                raise ValueError(f"invalid {column}")
            values.update(parsed.dt.strftime("%Y-%m-%d").tolist())
    except Exception as exc:
        raise _InspectionError(
            "PARQUET_INVALID", "frozen_data", f"cannot read {column} from {path}"
        ) from exc
    return sorted(values)


def _tree_token(root: Path, ledger_root: Path) -> str:
    selected = [
        ledger_root / "records",
        ledger_root / "snapshots",
        ledger_root / "membership",
        ledger_root / "inputs",
        ledger_root / "source-artifacts",
        root / "runtime/data/raw/checkpoint.json",
        root / "runtime/data/raw/enrichment-checkpoint.json",
        root / "runtime/data/raw/trade_cal",
        root / "runtime/data/raw/daily",
        root / "runtime/data/raw/daily_basic",
        root / "runtime/data/raw/adj_factor",
        root / "runtime/data/raw/bak_basic",
        root / "runtime/data/top500/execution.parquet",
        root / "runtime/data/top500/features.parquet",
    ]
    rows: list[tuple[str, str, int, int]] = []
    for selected_path in selected:
        if not selected_path.exists() and not selected_path.is_symlink():
            rows.append((str(selected_path), "missing", 0, 0))
            continue
        paths = [selected_path]
        if selected_path.is_dir() and not selected_path.is_symlink():
            paths.extend(sorted(selected_path.rglob("*"), key=lambda item: str(item)))
        for path in paths:
            stat = path.lstat()
            kind = "link" if path.is_symlink() else "dir" if path.is_dir() else "file"
            rows.append((str(path), kind, int(stat.st_size), int(stat.st_mtime_ns)))
    return _sha256_bytes(_canonical_json_bytes(rows))


def _issue(
    code: str,
    severity: str,
    component: str,
    message: str,
    *,
    retryable: bool,
    details: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "code": code,
        "severity": severity,
        "component": component,
        "retryable": retryable,
        "message": message,
        "details": dict(details or {}),
    }


def _ledger_view(
    root: Path, ledger_root: Path, ledger_id: str
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    records_root = ledger_root / "records"
    snapshots_root = ledger_root / "snapshots"
    if not records_root.is_dir() or not snapshots_root.is_dir():
        raise _InspectionError("LEDGER_MISSING", "ledger", "prospective ledger is missing")
    paths = sorted(records_root.glob("*.json"))
    if not paths:
        raise _InspectionError("LEDGER_EMPTY", "ledger", "prospective ledger has no records")
    previous: str | None = None
    records: list[dict[str, Any]] = []
    last_signal: str | None = None
    last_index: int | None = None
    prospective_epoch_tlog: str | None = None
    for expected_sequence, path in enumerate(paths, start=1):
        match = _RECORD_RE.fullmatch(path.name)
        if path.is_symlink() or match is None:
            raise _InspectionError("LEDGER_RECORD_INVALID", "ledger", f"invalid record filename: {path.name}")
        raw = path.read_bytes()
        digest = _sha256_bytes(raw)
        record = _load_json(path, canonical=True)
        if (
            int(match.group("sequence")) != expected_sequence
            or record.get("sequence") != expected_sequence
            or match.group("sha") != digest
            or record.get("kind") != match.group("kind")
            or record.get("ledger_id") != ledger_id
            or record.get("previous_record_sha256") != previous
        ):
            raise _InspectionError(
                "LEDGER_CHAIN_INVALID", "ledger", f"record chain differs at sequence {expected_sequence}"
            )
        if record.get("kind") == "decision":
            payload = record.get("payload")
            plan = payload.get("plan") if isinstance(payload, Mapping) else None
            route = plan.get("route_target_plan") if isinstance(plan, Mapping) else None
            if not isinstance(route, Mapping):
                raise _InspectionError("LEDGER_DECISION_INVALID", "ledger", "decision lacks route target plan")
            signal = _date_text(route.get("signal_date"), label="decision signal_date")
            index = route.get("calendar_index")
            if type(index) is not int or index < CANONICAL_CALENDAR_COUNT:
                raise _InspectionError("LEDGER_DECISION_INVALID", "ledger", "decision calendar index is invalid")
            if last_index is not None and index <= last_index:
                raise _InspectionError("LEDGER_DECISION_INVALID", "ledger", "decision calendar indices are not increasing")
            last_signal, last_index = signal, index
        if record.get("kind") == "attestation_receipt":
            payload = record.get("payload")
            if (
                isinstance(payload, Mapping)
                and payload.get("purpose") == "implementation_upgrade_canary"
            ):
                candidate_tlog = payload.get("verified_tlog_timestamp_utc")
                if not isinstance(candidate_tlog, str):
                    raise _InspectionError(
                        "LEDGER_RECEIPT_INVALID",
                        "ledger",
                        "implementation canary lacks a trusted TLog timestamp",
                    )
                normalized_tlog = _utc_text(
                    _utc(candidate_tlog, label="prospective epoch TLog timestamp")
                )
                if prospective_epoch_tlog is None:
                    prospective_epoch_tlog = normalized_tlog
        records.append(record)
        previous = digest

    snapshot_paths = sorted(snapshots_root.glob("*.json"))
    if not snapshot_paths:
        raise _InspectionError("LEDGER_SNAPSHOT_MISSING", "ledger", "prospective ledger has no snapshot")
    parsed_snapshots: list[tuple[int, str, Path]] = []
    for path in snapshot_paths:
        match = _SNAPSHOT_RE.fullmatch(path.name)
        if path.is_symlink() or match is None:
            raise _InspectionError("LEDGER_SNAPSHOT_INVALID", "ledger", f"invalid snapshot filename: {path.name}")
        parsed_snapshots.append((int(match.group("sequence")), match.group("sha"), path))
    highest_sequence = max(row[0] for row in parsed_snapshots)
    latest = [row for row in parsed_snapshots if row[0] == highest_sequence]
    if len(latest) != 1:
        raise _InspectionError(
            "LEDGER_SNAPSHOT_AMBIGUOUS",
            "ledger",
            "multiple snapshots claim the latest ledger sequence",
        )
    sequence, snapshot_sha, snapshot_path = latest[0]
    raw_snapshot = snapshot_path.read_bytes()
    snapshot = _load_json(snapshot_path, canonical=True)
    if (
        _sha256_bytes(raw_snapshot) != snapshot_sha
        or sequence != len(records)
        or snapshot.get("head_sequence") != len(records)
        or snapshot.get("head_record_sha256") != previous
        or snapshot.get("ledger_id") != ledger_id
        or snapshot.get("integrity_valid") is not True
    ):
        raise _InspectionError("LEDGER_SNAPSHOT_INVALID", "ledger", "latest snapshot does not bind the ledger head")
    tlog_text = snapshot.get("implementation_trusted_tlog_timestamp_utc")
    if not isinstance(tlog_text, str):
        raise _InspectionError("IMPLEMENTATION_NOT_TRUSTED", "ledger", "active implementation has no trusted TLog time")
    _utc(tlog_text, label="implementation TLog timestamp")
    if prospective_epoch_tlog is None:
        # Compatibility for early/synthetic ledgers whose canonical snapshot
        # predates explicit receipt-chain inspection.  Authoritative replay in
        # prospective_ledger still derives the epoch from the real records.
        prospective_epoch_tlog = _utc_text(
            _utc(tlog_text, label="prospective epoch TLog timestamp")
        )
    view = {
        "root": _relative(ledger_root, root),
        "ledger_id": ledger_id,
        "head_sequence": len(records),
        "head_record_sha256": previous,
        "snapshot_sha256": snapshot_sha,
        "phase": snapshot.get("phase"),
        "decision_generation_ready": snapshot.get("decision_generation_ready") is True,
        "decision_count": snapshot.get("decision_count"),
        "open_decision_count": snapshot.get("open_decision_count"),
        "implementation_trusted_tlog_timestamp_utc": _utc_text(
            _utc(tlog_text, label="implementation TLog timestamp")
        ),
        "prospective_epoch_tlog_timestamp_utc": prospective_epoch_tlog,
        "last_decision_signal_date": last_signal,
        "last_decision_calendar_index": last_index,
        "observer_validation_scope": "canonical_record_chain_and_snapshot_binding",
    }
    return view, records


def _calendar_records(frame: pd.DataFrame, *, exchange: str) -> list[dict[str, Any]]:
    required = {"cal_date", "is_open"}
    if not required.issubset(frame.columns):
        raise ValueError("calendar columns are incomplete")
    work = frame.copy()
    work["cal_date"] = pd.to_datetime(
        work["cal_date"].astype("string").str.replace("-", "", regex=False),
        format="%Y%m%d",
        errors="coerce",
    )
    if work.empty or work["cal_date"].isna().any() or work.duplicated("cal_date").any():
        raise ValueError("calendar dates are invalid")
    numeric = pd.to_numeric(work["is_open"], errors="coerce")
    textual = work["is_open"].astype("string").str.strip().str.casefold()
    valid = numeric.isin([0, 1]) | textual.isin(["false", "true"])
    if not bool(valid.all()):
        raise ValueError("calendar open flags are invalid")
    work["is_open"] = numeric.eq(1) | textual.eq("true")
    if "exchange" not in work:
        work["exchange"] = exchange
    work["exchange"] = work["exchange"].astype("string").fillna(exchange).str.strip()
    if bool(work["exchange"].ne(exchange).any()):
        raise ValueError("calendar exchange differs")
    if "pretrade_date" not in work:
        work["pretrade_date"] = pd.NaT
    work["pretrade_date"] = pd.to_datetime(
        work["pretrade_date"].astype("string").str.replace("-", "", regex=False),
        format="%Y%m%d",
        errors="coerce",
    )
    work = work.sort_values("cal_date", kind="mergesort").reset_index(drop=True)
    expected = pd.date_range(work["cal_date"].iloc[0], work["cal_date"].iloc[-1], freq="D")
    if not pd.DatetimeIndex(work["cal_date"]).equals(expected):
        raise ValueError("calendar has a natural-date gap")
    return [
        {
            "cal_date": row.cal_date.date().isoformat(),
            "exchange": str(row.exchange),
            "is_open": bool(row.is_open),
            "pretrade_date": None if pd.isna(row.pretrade_date) else row.pretrade_date.date().isoformat(),
        }
        for row in work.itertuples(index=False)
    ]


def _verified_calendars(
    root: Path, checkpoint: Mapping[str, Any], observed: datetime
) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    raw = checkpoint.get("calendars")
    if not isinstance(raw, Mapping):
        raise _InspectionError("CALENDAR_CHECKPOINT_INVALID", "calendar", "checkpoint calendars is not a mapping")
    available: list[dict[str, Any]] = []
    invalid: list[str] = []
    future: list[str] = []
    for key, raw_entry in sorted(raw.items()):
        if not _SHA256_RE.fullmatch(str(key)) or not isinstance(raw_entry, Mapping):
            invalid.append(str(key))
            continue
        entry = dict(raw_entry)
        try:
            if entry.get("status") != "complete" or entry.get("calendar_content_sha256") != key:
                raise ValueError("identity")
            completed = _utc(entry.get("completed_at_utc"), label="calendar completed_at_utc")
            if completed > observed:
                future.append(str(key))
                continue
            expected = root / "runtime/data/raw/trade_cal" / f"calendar_sha256={key}" / "part-000.parquet"
            path = _exact_file(entry.get("path"), expected, label="calendar artifact")
            expected_manifest = expected.with_name("manifest.json")
            manifest_path = _exact_file(entry.get("manifest_path"), expected_manifest, label="calendar manifest")
            if (
                _sha256_file(path) != entry.get("artifact_sha256")
                or path.stat().st_size <= 0
                or _sha256_file(manifest_path) != entry.get("manifest_sha256")
            ):
                raise ValueError("artifact hash")
            manifest = _load_json(manifest_path)
            if (
                manifest.get("schema_version") != 1
                or manifest.get("artifact_sha256") != entry.get("artifact_sha256")
                or manifest.get("calendar_content_sha256") != key
                or manifest.get("records_sha256") != key
            ):
                raise ValueError("manifest binding")
            records = _calendar_records(pd.read_parquet(path), exchange=str(entry.get("exchange") or "SSE"))
            if (
                _sha256_bytes(_canonical_json_bytes(records)) != key
                or len(records) != entry.get("row_count")
                or records[0]["cal_date"] != entry.get("start_date")
                or records[-1]["cal_date"] != entry.get("end_date")
                or sum(bool(row["is_open"]) for row in records) != entry.get("open_day_count")
            ):
                raise ValueError("logical content")
            available.append(
                {
                    "content_sha256": key,
                    "artifact_sha256": entry.get("artifact_sha256"),
                    "completed_at_utc": _utc_text(completed),
                    "start_date": records[0]["cal_date"],
                    "end_date": records[-1]["cal_date"],
                    "records": records,
                    "open_dates": [row["cal_date"] for row in records if row["is_open"]],
                }
            )
        except Exception:
            invalid.append(str(key))
    return available, invalid, future


def _conflicting_calendar_sources(
    calendars: Sequence[Mapping[str, Any]],
) -> list[str]:
    chosen: dict[str, tuple[Mapping[str, Any], str]] = {}
    conflicts: set[str] = set()
    for calendar in calendars:
        digest = str(calendar["content_sha256"])
        for row in calendar["records"]:
            trade_date = str(row["cal_date"])
            prior = chosen.get(trade_date)
            if prior is not None and dict(prior[0]) != dict(row):
                conflicts.update({prior[1], digest})
            elif prior is None:
                chosen[trade_date] = (row, digest)
    return sorted(conflicts)


def _sealed_calendar_view(
    root: Path,
    source: Mapping[str, Any],
    observed: datetime,
) -> dict[str, Any]:
    """Verify one calendar source already sealed into an authoritative artifact.

    Signal and membership builders copy every source byte into the prospective
    content-addressed store.  Once a complete artifact has been independently
    replayed, those immutable bytes outrank the mutable raw checkpoint for
    readiness.  This helper reads only the canonical CAS paths.
    """

    from .prospective import _resolve_immutable_artifact

    if source.get("role") != "official_trade_calendar":
        raise ValueError("calendar source role")
    content_sha = source.get("calendar_content_sha256")
    if not isinstance(content_sha, str) or not _SHA256_RE.fullmatch(content_sha):
        raise ValueError("calendar content hash")
    completed = _utc(
        source.get("completed_at_utc"), label="sealed calendar completed_at_utc"
    )
    if completed > observed:
        raise ValueError("calendar source is not yet available")
    artifact_sha_field = (
        "artifact_sha256" if "artifact_sha256" in source else "sha256"
    )
    path = _resolve_immutable_artifact(
        root,
        source,
        sha_field=artifact_sha_field,
    )
    manifest_path = _resolve_immutable_artifact(
        root,
        source,
        sha_field="manifest_sha256",
        path_field="immutable_manifest_path",
        size_field="manifest_size_bytes",
        media_field="manifest_media_type",
    )
    artifact_sha = str(source[artifact_sha_field])
    manifest = _load_json(manifest_path)
    if (
        manifest.get("schema_version") != 1
        or manifest.get("artifact_sha256") != artifact_sha
        or manifest.get("calendar_content_sha256") != content_sha
        or manifest.get("records_sha256") != content_sha
    ):
        raise ValueError("sealed calendar manifest binding")
    exchange = str(source.get("exchange") or "SSE")
    records = _calendar_records(pd.read_parquet(path), exchange=exchange)
    start = str(source.get("source_start_date") or source.get("start_date") or "")
    end = str(source.get("source_end_date") or source.get("end_date") or "")
    if (
        _sha256_bytes(_canonical_json_bytes(records)) != content_sha
        or records[0]["cal_date"] != start
        or records[-1]["cal_date"] != end
    ):
        raise ValueError("sealed calendar logical content")
    if "row_count" in source and len(records) != source.get("row_count"):
        raise ValueError("sealed calendar row count")
    open_count = sum(bool(row["is_open"]) for row in records)
    if "open_day_count" in source and open_count != source.get("open_day_count"):
        raise ValueError("sealed calendar open-day count")
    return {
        "content_sha256": content_sha,
        "artifact_sha256": artifact_sha,
        "completed_at_utc": _utc_text(completed),
        "start_date": records[0]["cal_date"],
        "end_date": records[-1]["cal_date"],
        "records": records,
        "open_dates": [row["cal_date"] for row in records if row["is_open"]],
    }


def _sealed_artifact_calendars(
    root: Path, observed: datetime
) -> list[dict[str, Any]]:
    """Recover calendar authority from fully replayed membership/input CAS."""

    from .prospective import load_prospective_input_snapshot
    from .prospective_membership import load_prospective_membership_snapshot

    sources: list[Mapping[str, Any]] = []
    input_root = root / "runtime/prospective/5.0/inputs"
    if input_root.is_dir():
        for directory in sorted(input_root.iterdir(), key=lambda item: item.name):
            if directory.is_symlink() or not directory.is_dir():
                continue
            try:
                loaded = load_prospective_input_snapshot(directory)
                if max(
                    _utc(
                        loaded.inputs_available_at_utc,
                        label="sealed input inputs_available_at_utc",
                    ),
                    _utc(
                        loaded.build_completed_at_utc,
                        label="sealed input build_completed_at_utc",
                    ),
                ) > observed:
                    continue
                calendar = loaded.manifest.get("calendar")
                rows = calendar.get("sources") if isinstance(calendar, Mapping) else None
                if isinstance(rows, list) and all(
                    isinstance(row, Mapping) for row in rows
                ):
                    sources.extend(rows)
            except Exception:
                continue

    membership_root = root / "runtime/prospective/5.0/membership"
    if membership_root.is_dir():
        for artifact_path in sorted(
            membership_root.glob("*/*/membership.parquet"),
            key=lambda item: item.as_posix(),
        ):
            if artifact_path.is_symlink():
                continue
            try:
                loaded = load_prospective_membership_snapshot(
                    artifact_path,
                    project_root=root,
                    available_at_utc=_utc_text(observed),
                )
                rows = loaded.manifest.get("input_sources")
                if isinstance(rows, list):
                    sources.extend(
                        row
                        for row in rows
                        if isinstance(row, Mapping)
                        and row.get("role") == "official_trade_calendar"
                    )
            except Exception:
                continue

    verified: dict[str, dict[str, Any]] = {}
    for source in sources:
        try:
            calendar = _sealed_calendar_view(root, source, observed)
        except Exception:
            continue
        digest = str(calendar["content_sha256"])
        prior = verified.get(digest)
        if prior is not None and prior != calendar:
            continue
        verified[digest] = calendar
    return [verified[key] for key in sorted(verified)]


def _signal_close(signal: str) -> datetime:
    local = datetime.combine(date.fromisoformat(signal), time(15, 0), tzinfo=_SHANGHAI)
    return local.astimezone(_UTC)


def _admission_deadline(entry: str) -> datetime:
    local = datetime.combine(date.fromisoformat(entry), time(9, 15), tzinfo=_SHANGHAI)
    return local.astimezone(_UTC)


def _candidate_from_calendar(
    calendar: Mapping[str, Any], ledger: Mapping[str, Any]
) -> tuple[dict[str, Any], list[str]]:
    post_bridge = [value for value in calendar["open_dates"] if value > FROZEN_BRIDGE_END]
    if ledger["last_decision_signal_date"] is None:
        tlog = _utc(
            ledger["prospective_epoch_tlog_timestamp_utc"],
            label="prospective epoch TLog timestamp",
        )
        signal = next((value for value in post_bridge if _signal_close(value) > tlog), None)
        if signal is None:
            raise _InspectionError("CALENDAR_HORIZON_INSUFFICIENT", "calendar", "calendar cannot derive the first post-TLog signal")
        position = post_bridge.index(signal)
        index = CANONICAL_CALENDAR_COUNT + position
        skipped = post_bridge[:position]
    else:
        last_signal = str(ledger["last_decision_signal_date"])
        signal = next((value for value in post_bridge if value > last_signal), None)
        if signal is None:
            raise _InspectionError("CALENDAR_HORIZON_INSUFFICIENT", "calendar", "calendar cannot derive the signal after the last decision")
        index = int(ledger["last_decision_calendar_index"]) + 1
        skipped = []
    position = post_bridge.index(signal)
    if position + 11 >= len(post_bridge):
        raise _InspectionError("CALENDAR_HORIZON_INSUFFICIENT", "calendar", "calendar does not cover signal i+11")
    entry = post_bridge[position + 1]
    checkpoint = post_bridge[position + 11]
    month = date.fromisoformat(entry).strftime("%Y-%m")
    month_start = date.fromisoformat(f"{month}-01")
    next_month = date(month_start.year + (month_start.month == 12), month_start.month % 12 + 1, 1)
    month_end = (pd.Timestamp(next_month) - pd.Timedelta(days=1)).date().isoformat()
    month_opens = [
        value
        for value in calendar["open_dates"]
        if month_start.isoformat() <= value <= month_end
    ]
    prior_opens = [
        value for value in calendar["open_dates"] if value < month_start.isoformat()
    ]
    if not month_opens or not prior_opens:
        raise _InspectionError(
            "CALENDAR_HORIZON_INSUFFICIENT",
            "calendar",
            "calendar cannot derive the monthly membership interval",
        )
    return (
        {
            "signal_date": signal,
            "signal_close_utc": _utc_text(_signal_close(signal)),
            "entry_date": entry,
            "i_plus_11_date": checkpoint,
            "admission_deadline_utc": _utc_text(_admission_deadline(entry)),
            "calendar_index": index,
            "due_offset": index % 10,
            "membership_month": month,
            "membership_as_of_date": prior_opens[-1],
            "membership_effective_start_date": month_opens[0],
            "membership_effective_end_date": month_opens[-1],
            "membership_calendar_end_date": month_end,
            "initial_skipped_sessions": skipped,
        },
        post_bridge,
    )


def _partition_result(
    root: Path,
    checkpoint: Mapping[str, Any],
    dataset: str,
    trade_date: str,
    observed: datetime,
) -> dict[str, Any]:
    entries = checkpoint.get("partitions")
    key = f"{dataset}/{trade_date}"
    entry = entries.get(key) if isinstance(entries, Mapping) else None
    result = {
        "key": key,
        "date": trade_date,
        "status": "missing",
        "completed_at_utc": None,
        "provider_completion_contract_id": None,
    }
    if isinstance(entry, Mapping) and entry.get("status") == "reconciling":
        result["status"] = "reconcile"
        return result
    if not isinstance(entry, Mapping) or entry.get("status") != "complete":
        return result
    try:
        if entry.get("dataset") != dataset or entry.get("trade_date") != trade_date:
            raise ValueError("identity")
        completed = _utc(entry.get("completed_at_utc"), label=f"{key} completed_at_utc")
        result["completed_at_utc"] = _utc_text(completed)
        # A same-session market partition cannot truthfully be available
        # before that session's official close.  Treat such checkpoint claims
        # as invalid evidence, not as an especially fast data feed.
        if completed < _signal_close(trade_date):
            raise ValueError("completion precedes market close")
        if completed > observed:
            result["status"] = "not_yet_available"
            return result
        expected = root / "runtime/data/raw" / dataset / f"trade_date={trade_date}" / "part-000.parquet"
        path = _exact_file(entry.get("path"), expected, label=key)
        expected_sha = str(entry.get("sha256") or "")
        if (
            not _SHA256_RE.fullmatch(expected_sha)
            or entry.get("size_bytes") != path.stat().st_size
            or _sha256_file(path) != expected_sha
        ):
            raise ValueError("bytes")
        parquet = pq.ParquetFile(path)
        if parquet.metadata.num_rows != entry.get("row_count") or parquet.metadata.num_rows <= 0:
            raise ValueError("rows")
        required = _PARTITION_COLUMNS[dataset]
        if not required.issubset(parquet.schema.names):
            raise ValueError("columns")
        frame = parquet.read().to_pandas()
        dates = pd.to_datetime(
            frame["trade_date"].astype("string").str.replace("-", "", regex=False),
            format="%Y%m%d",
            errors="coerce",
        ).dt.strftime("%Y-%m-%d")
        tickers = frame["ts_code"].astype("string").str.strip()
        if dates.isna().any() or bool(dates.ne(trade_date).any()) or tickers.isna().any() or tickers.eq("").any() or tickers.duplicated().any():
            raise ValueError("contents")
        try:
            completion_evidence = validate_provider_completion_evidence(
                entry,
                frame,
                dataset=dataset,
                trade_date=trade_date,
                required_datasets=PROVIDER_COMPLETION_DATASETS,
            )
        except ValueError as exc:
            if str(exc) == "provider completion evidence is missing":
                result["status"] = "reconcile"
                return result
            raise
        if completion_evidence is not None:
            evidence_sha = str(
                completion_evidence.get("evidence_sha256") or ""
            )
            for sibling_dataset in PROVIDER_COMPLETION_DATASETS:
                sibling = entries.get(f"{sibling_dataset}/{trade_date}")
                if (
                    not isinstance(sibling, Mapping)
                    or sibling.get("status") == "reconciling"
                    or sibling.get("status") != "complete"
                    or "provider_completion" not in sibling
                ):
                    result["status"] = "reconcile"
                    return result
                sibling_evidence = sibling.get("provider_completion")
                if (
                    not isinstance(sibling_evidence, Mapping)
                    or sibling_evidence.get("evidence_sha256") != evidence_sha
                ):
                    raise ValueError("provider completion bundles differ")
            ticker_sets: dict[str, set[str]] = {}
            for sibling_dataset in PROVIDER_COMPLETION_DATASETS:
                sibling_path = (
                    root
                    / "runtime/data/raw"
                    / sibling_dataset
                    / f"trade_date={trade_date}"
                    / "part-000.parquet"
                )
                sibling_frame = pq.ParquetFile(sibling_path).read(
                    columns=["ts_code"]
                ).to_pandas()
                ticker_sets[sibling_dataset] = set(
                    sibling_frame["ts_code"].astype("string").str.strip()
                )
            if (
                ticker_sets["daily"] != ticker_sets["daily_basic"]
                or not ticker_sets["daily"].issubset(
                    ticker_sets["adj_factor"]
                )
            ):
                raise ValueError("provider completion universe relation differs")
            result["provider_completion_contract_id"] = (
                PROVIDER_COMPLETION_CONTRACT_ID
            )
        result["status"] = "complete"
        return result
    except Exception:
        result["status"] = "invalid"
        return result


def _coverage(
    results: Mapping[tuple[str, str], Mapping[str, Any]],
    dataset: str,
    dates: Sequence[str],
) -> dict[str, Any]:
    statuses = {value: str(results[(dataset, value)]["status"]) for value in dates}
    return {
        "dataset": dataset,
        "required_count": len(dates),
        "complete_count": sum(value == "complete" for value in statuses.values()),
        "required_dates": list(dates),
        "missing_dates": [key for key, value in statuses.items() if value == "missing"],
        "not_yet_available_dates": [key for key, value in statuses.items() if value == "not_yet_available"],
        "reconcile_dates": [key for key, value in statuses.items() if value == "reconcile"],
        "invalid_dates": [key for key, value in statuses.items() if value == "invalid"],
        "complete": not statuses
        or all(value == "complete" for value in statuses.values()),
    }


def _reference_view(
    root: Path, checkpoint: Mapping[str, Any], as_of: str, observed: datetime
) -> dict[str, Any]:
    key = f"bak_basic/trade_date={as_of}"
    entries = checkpoint.get("partitions")
    entry = entries.get(key) if isinstance(entries, Mapping) else None
    result = {
        "status": "missing",
        "checkpoint_key": key,
        "as_of_date": as_of,
        "completed_at_utc": None,
        "row_count": None,
        "artifact_sha256": None,
    }
    if not isinstance(entry, Mapping) or entry.get("status") != "complete":
        return result
    try:
        if (
            entry.get("dataset") != "bak_basic"
            or entry.get("trade_date") != as_of
            or entry.get("request_trade_date") != as_of
            or entry.get("source_trade_date") != as_of
            or entry.get("capture_contract_id")
            != "factor-lab/exact-bak-basic-raw/1"
            or entry.get("capture_mode") != "exact_only"
            or entry.get("fallback_used") is not False
            or entry.get("fields")
            != "trade_date,ts_code,name,industry,list_date"
            or entry.get("exact_source_required") is not True
            or type(entry.get("stability_sample_count")) is not int
            or int(entry["stability_sample_count"]) < 2
        ):
            raise ValueError("identity")
        completed = _utc(entry.get("completed_at_utc"), label=f"{key} completed_at_utc")
        result["completed_at_utc"] = _utc_text(completed)
        if completed < _signal_close(as_of):
            raise ValueError("completion precedes as-of market close")
        if completed > observed:
            result["status"] = "not_yet_available"
            return result
        expected = root / "runtime/data/raw/bak_basic" / f"trade_date={as_of}" / "part-000.parquet"
        path = _exact_file(entry.get("path"), expected, label="exact-as-of bak_basic")
        expected_sha = str(entry.get("sha256") or "")
        if (
            not _SHA256_RE.fullmatch(expected_sha)
            or entry.get("size_bytes") != path.stat().st_size
            or _sha256_file(path) != expected_sha
        ):
            raise ValueError("bytes")
        parquet = pq.ParquetFile(path)
        if (
            parquet.metadata.num_rows != entry.get("row_count")
            or parquet.metadata.num_rows <= 0
            or not _REFERENCE_COLUMNS.issubset(parquet.schema.names)
        ):
            raise ValueError("schema")
        if "source_trade_date" not in parquet.schema.names:
            raise ValueError("exact source evidence")
        frame = parquet.read(
            columns=["trade_date", "source_trade_date", "ts_code"]
        ).to_pandas()
        dates = pd.to_datetime(
            frame["trade_date"].astype("string").str.replace("-", "", regex=False),
            format="%Y%m%d",
            errors="coerce",
        ).dt.strftime("%Y-%m-%d")
        tickers = frame["ts_code"].astype("string").str.strip()
        source_dates = pd.to_datetime(
            frame["source_trade_date"]
            .astype("string")
            .str.replace("-", "", regex=False),
            format="%Y%m%d",
            errors="coerce",
        ).dt.strftime("%Y-%m-%d")
        if (
            dates.isna().any()
            or bool(dates.ne(as_of).any())
            or source_dates.isna().any()
            or bool(source_dates.ne(as_of).any())
            or tickers.isna().any()
            or tickers.eq("").any()
            or tickers.duplicated().any()
        ):
            raise ValueError("contents")
        raw_checkpoint = _load_json(root / "runtime/data/raw/checkpoint.json")
        raw_partitions = raw_checkpoint.get("partitions")
        daily_key = f"daily/{as_of}"
        daily_entry = (
            raw_partitions.get(daily_key)
            if isinstance(raw_partitions, Mapping)
            else None
        )
        if (
            not isinstance(daily_entry, Mapping)
            or entry.get("daily_partition_sha256") != daily_entry.get("sha256")
            or type(entry.get("daily_ticker_count")) is not int
            or type(entry.get("covered_ticker_count")) is not int
            or entry.get("daily_ticker_count") != entry.get("covered_ticker_count")
        ):
            raise ValueError("daily universe binding")
        daily_path = root / "runtime/data/raw/daily" / f"trade_date={as_of}" / "part-000.parquet"
        daily = pq.ParquetFile(daily_path).read(columns=["ts_code"]).to_pandas()
        daily_tickers = set(daily["ts_code"].astype("string").str.strip())
        if (
            len(daily_tickers) != entry.get("daily_ticker_count")
            or entry.get("reference_ticker_count") != len(set(tickers))
            or entry.get("reference_ticker_count") != parquet.metadata.num_rows
            or not daily_tickers.issubset(set(tickers))
        ):
            raise ValueError("daily universe coverage")
        result.update(
            status="complete",
            row_count=int(parquet.metadata.num_rows),
            artifact_sha256=expected_sha,
        )
        return result
    except Exception:
        result["status"] = "invalid"
        return result


def _membership_view(
    root: Path, candidate: Mapping[str, Any], observed: datetime
) -> dict[str, Any]:
    from .prospective_membership import load_prospective_membership_snapshot

    month = str(candidate["membership_month"])
    month_root = root / "runtime/prospective/5.0/membership" / month
    result = {
        "status": "not_built",
        "membership_month": month,
        "artifact_sha256": None,
        "manifest_sha256": None,
        "completed_at_utc": None,
        "row_count": None,
        "candidate_count": 0,
        "path": None,
    }
    if not month_root.is_dir():
        return result
    manifests = sorted(month_root.glob("*/manifest.json"))
    result["candidate_count"] = len(manifests)
    valid: list[dict[str, Any]] = []
    for manifest_path in manifests:
        try:
            directory_sha = manifest_path.parent.name
            if not _SHA256_RE.fullmatch(directory_sha) or manifest_path.is_symlink():
                raise ValueError("path")
            artifact_path = _exact_file(
                None, manifest_path.parent / "membership.parquet", label="membership artifact"
            )
            manifest = _load_json(manifest_path, canonical=True)
            completed = _utc(manifest.get("completed_at_utc"), label="membership completed_at_utc")
            if completed > observed:
                continue
            if (
                manifest.get("membership_month") != month
                or manifest.get("as_of_date") != candidate["membership_as_of_date"]
                or manifest.get("effective_start_date") != candidate["membership_effective_start_date"]
                or manifest.get("effective_end_date")
                != candidate["membership_effective_end_date"]
            ):
                raise ValueError("contract")
            verified = load_prospective_membership_snapshot(
                artifact_path,
                project_root=root,
                available_at_utc=_utc_text(observed),
            )
            if (
                verified.membership_month != month
                or verified.as_of_date != candidate["membership_as_of_date"]
                or verified.artifact_sha256 != directory_sha
                or verified.membership_path.resolve() != artifact_path
                or verified.manifest_path.resolve() != manifest_path.resolve()
                or dict(verified.manifest) != manifest
                or len(verified.frame) != EXPECTED_MEMBERSHIP_SIZE
            ):
                raise ValueError("authoritative replay identity")
            valid.append(
                {
                    "artifact_sha256": directory_sha,
                    "manifest_sha256": _sha256_file(manifest_path),
                    "completed_at_utc": _utc_text(completed),
                    "row_count": int(len(verified.frame)),
                    "path": str(artifact_path.resolve()),
                }
            )
        except Exception:
            result["status"] = "invalid"
    if len(valid) > 1:
        result["status"] = "ambiguous"
        return result
    if len(valid) == 1 and result["status"] != "invalid":
        result.update(status="complete", **valid[0])
    elif len(valid) == 1:
        result["candidate_count"] = len(manifests)
    return result


def _input_snapshot_view(
    root: Path,
    candidate: Mapping[str, Any],
    membership: Mapping[str, Any],
    observed: datetime,
) -> dict[str, Any]:
    from .prospective import load_prospective_input_snapshot

    input_root = root / "runtime/prospective/5.0/inputs"
    result = {
        "status": "not_built",
        "signal_date": candidate["signal_date"],
        "trade_date": candidate["entry_date"],
        "snapshot_sha256": None,
        "membership_artifact_sha256": None,
        "inputs_available_at_utc": None,
        "build_completed_at_utc": None,
        "candidate_count": 0,
        "directory": None,
    }
    if not input_root.is_dir():
        return result

    valid: list[dict[str, Any]] = []
    future_count = 0
    invalid = False
    for directory in sorted(input_root.iterdir(), key=lambda item: item.name):
        if directory.is_symlink() or not directory.is_dir():
            continue
        manifest_path = directory / "manifest.json"
        if not manifest_path.is_file() or manifest_path.is_symlink():
            continue
        claimed = False
        try:
            # Signal manifests intentionally contain finite IEEE-754 values
            # (for example ``signal_coverage_ratio``).  They remain
            # byte-canonical and reject NaN/Infinity, while authoritative
            # ledger records continue to use the no-float parser above.
            manifest = _load_json(
                manifest_path,
                canonical=True,
                allow_finite_floats=True,
            )
            if manifest.get("signal_date") != candidate["signal_date"]:
                continue
            claimed = True
            result["candidate_count"] += 1
            if not _SHA256_RE.fullmatch(directory.name):
                raise ValueError("snapshot path")
            loaded = load_prospective_input_snapshot(directory)
            inputs_available = _utc(
                loaded.inputs_available_at_utc,
                label="input snapshot inputs_available_at_utc",
            )
            built = _utc(
                loaded.build_completed_at_utc,
                label="input snapshot build_completed_at_utc",
            )
            signal_close = _utc(candidate["signal_close_utc"], label="signal close")
            deadline = _utc(
                candidate["admission_deadline_utc"], label="admission deadline"
            )
            membership_sha = membership.get("artifact_sha256")
            if (
                loaded.signal_date != candidate["signal_date"]
                or loaded.trade_date != candidate["entry_date"]
                or loaded.snapshot_sha256 != directory.name
                or dict(loaded.manifest) != manifest
                or membership.get("status") != "complete"
                or not isinstance(membership_sha, str)
                or loaded.membership_artifact_sha256 != membership_sha
                or not signal_close < inputs_available <= built < deadline
            ):
                raise ValueError("snapshot contract")
            if built > observed or inputs_available > observed:
                future_count += 1
                continue
            valid.append(
                {
                    "snapshot_sha256": loaded.snapshot_sha256,
                    "membership_artifact_sha256": loaded.membership_artifact_sha256,
                    "inputs_available_at_utc": _utc_text(inputs_available),
                    "build_completed_at_utc": _utc_text(built),
                    "directory": str(directory.resolve()),
                }
            )
        except Exception:
            if claimed:
                invalid = True

    if invalid:
        result["status"] = "invalid"
    elif len(valid) > 1:
        result["status"] = "ambiguous"
    elif len(valid) == 1:
        result.update(status="complete", **valid[0])
    elif future_count:
        result["status"] = "not_yet_available"
    return result


def _empty_report(
    observed: datetime,
    clock_source: str,
    ledger_root_text: str,
    ledger_id: str,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "prospective_readiness",
        "contract_id": CONTRACT_ID,
        "observed_at_utc": _utc_text(observed),
        "clock_source": clock_source,
        "stable_view": True,
        "status": "blocked",
        "reason": "inspection_incomplete",
        "ready": False,
        "next_action": "none",
        "action": None,
        "ready_for": {
            "membership_build": False,
            "input_build": False,
            "decision_admission": False,
        },
        "ledger": {
            "root": ledger_root_text,
            "ledger_id": ledger_id,
            "head_sequence": None,
            "head_record_sha256": None,
            "snapshot_sha256": None,
            "phase": None,
            "decision_generation_ready": False,
            "decision_count": None,
            "open_decision_count": None,
            "implementation_trusted_tlog_timestamp_utc": None,
            "prospective_epoch_tlog_timestamp_utc": None,
            "last_decision_signal_date": None,
            "last_decision_calendar_index": None,
            "observer_validation_scope": "canonical_record_chain_and_snapshot_binding",
        },
        "candidate": {
            "signal_date": None,
            "signal_close_utc": None,
            "entry_date": None,
            "i_plus_11_date": None,
            "admission_deadline_utc": None,
            "calendar_index": None,
            "due_offset": None,
            "membership_month": None,
            "membership_as_of_date": None,
            "membership_effective_start_date": None,
            "membership_effective_end_date": None,
            "membership_calendar_end_date": None,
            "initial_skipped_sessions": [],
        },
        "calendar": {
            "status": "unknown",
            "content_sha256": None,
            "artifact_sha256": None,
            "completed_at_utc": None,
            "start_date": None,
            "end_date": None,
            "full_membership_month": False,
            "prefix_contiguous": False,
            "covers_i_plus_11": False,
        },
        "coverage": {
            name: {
                "dataset": dataset,
                "required_count": 0,
                "complete_count": 0,
                "required_dates": [],
                "missing_dates": [],
                "not_yet_available_dates": [],
                "invalid_dates": [],
                "complete": False,
            }
            for name, dataset in (
                ("liquidity_daily", "daily"),
                ("signal_daily", "daily"),
                ("signal_daily_basic", "daily_basic"),
                ("signal_adj_factor", "adj_factor"),
            )
        },
        "reference": {
            "status": "unknown",
            "checkpoint_key": None,
            "as_of_date": None,
            "completed_at_utc": None,
            "row_count": None,
            "artifact_sha256": None,
        },
        "membership": {
            "status": "unknown",
            "membership_month": None,
            "artifact_sha256": None,
            "manifest_sha256": None,
            "completed_at_utc": None,
            "row_count": None,
            "candidate_count": 0,
            "path": None,
        },
        "input_snapshot": {
            "status": "unknown",
            "signal_date": None,
            "trade_date": None,
            "snapshot_sha256": None,
            "membership_artifact_sha256": None,
            "inputs_available_at_utc": None,
            "build_completed_at_utc": None,
            "candidate_count": 0,
            "directory": None,
        },
        "target_replay": {
            "status": "not_run",
            "result_sha256": None,
            "deployment_sha256": None,
            "generator_id": None,
        },
        "timing": {
            "phase": "unknown",
            "before_signal_close": None,
            "admission_deadline_missed": None,
            "seconds_until_signal_close": None,
            "seconds_until_admission_deadline": None,
        },
        "issues": [],
    }


def inspect_prospective_readiness(
    project_root: str | Path,
    *,
    observed_at_utc: datetime | str | None = None,
    ledger_root: str | Path | None = None,
    ledger_id: str = LEDGER_ID,
) -> dict[str, Any]:
    """Return a stable, JSON-serialisable readiness report without writing.

    The ledger validation performed here is intentionally structural.  A
    command that authorises mutation must first run the ledger's authoritative
    replay/audit, then use this observer only for prospective data readiness.
    """

    root = Path(project_root).expanduser().resolve()
    resolved_ledger = (
        (root / "runtime/prospective/5.0")
        if ledger_root is None
        else Path(ledger_root).expanduser().resolve()
    )
    try:
        ledger_text = _relative(resolved_ledger, root)
    except ValueError:
        ledger_text = str(resolved_ledger)
    if observed_at_utc is None:
        observed = datetime.now(_UTC).replace(microsecond=0)
        clock_source = "local_system_clock_untrusted"
    else:
        observed = _utc(observed_at_utc, label="observed_at_utc")
        clock_source = "caller_supplied"
    report = _empty_report(observed, clock_source, ledger_text, ledger_id)
    before = _tree_token(root, resolved_ledger)
    issues: list[dict[str, Any]] = report["issues"]
    try:
        ledger, _ = _ledger_view(root, resolved_ledger, ledger_id)
        report["ledger"] = ledger
        if not ledger["decision_generation_ready"]:
            issues.append(
                _issue(
                    "LEDGER_NOT_READY",
                    "error",
                    "ledger",
                    "ledger has no attested active implementation",
                    retryable=False,
                )
            )
        raw_checkpoint_path = root / "runtime/data/raw/checkpoint.json"
        raw_checkpoint: Mapping[str, Any] | None = None
        sealed_calendars = _sealed_artifact_calendars(root, observed)
        calendars = list(sealed_calendars)
        live_calendars: list[dict[str, Any]] = []
        invalid_calendars: list[str] = []
        future_calendars: list[str] = []
        if raw_checkpoint_path.is_file():
            try:
                candidate_checkpoint = _load_json(raw_checkpoint_path)
                if (
                    candidate_checkpoint.get("schema_version") != 1
                    or not isinstance(
                        candidate_checkpoint.get("partitions"), Mapping
                    )
                ):
                    raise ValueError("raw checkpoint schema")
                raw_checkpoint = candidate_checkpoint
                (
                    live_calendars,
                    invalid_calendars,
                    future_calendars,
                ) = _verified_calendars(root, raw_checkpoint, observed)
            except Exception as exc:
                raw_checkpoint = None
                if not sealed_calendars:
                    raise _InspectionError(
                        "RAW_CHECKPOINT_INVALID",
                        "raw_checkpoint",
                        "raw checkpoint schema or calendar evidence is invalid",
                    ) from exc
            else:
                merged: dict[str, dict[str, Any]] = {
                    str(calendar["content_sha256"]): calendar
                    for calendar in live_calendars
                }
                # Prefer an independently replayed sealed view when the same
                # logical calendar exists in both authorities; its CAS bytes
                # are immune to later checkpoint/origin replacement.
                merged.update(
                    {
                        str(calendar["content_sha256"]): calendar
                        for calendar in sealed_calendars
                    }
                )
                calendars = [merged[key] for key in sorted(merged)]
        elif not sealed_calendars:
            raise _InspectionError(
                "RAW_CHECKPOINT_MISSING",
                "raw_checkpoint",
                "raw checkpoint is missing and no sealed calendar authority exists",
            )
        if not calendars:
            severity = "error" if invalid_calendars else "wait"
            code = "CALENDAR_INVALID" if invalid_calendars else "CALENDAR_NOT_AVAILABLE"
            issues.append(
                _issue(
                    code,
                    severity,
                    "calendar",
                    "no verified official calendar is available at the observation time",
                    retryable=not invalid_calendars,
                    details={"invalid_keys": invalid_calendars, "future_keys": future_calendars},
                )
            )
            raise _InspectionError("CANDIDATE_UNAVAILABLE", "calendar", "candidate cannot be derived without a calendar")

        derivations: list[tuple[dict[str, Any], dict[str, Any], list[str]]] = []
        for calendar in calendars:
            try:
                candidate, post_bridge = _candidate_from_calendar(calendar, ledger)
                derivations.append((calendar, candidate, post_bridge))
            except _InspectionError:
                continue
        if not derivations:
            raise _InspectionError("CALENDAR_HORIZON_INSUFFICIENT", "calendar", "verified calendars cannot derive signal, entry, and i+11")
        qualified = [
            row
            for row in derivations
            if row[0]["start_date"] <= (pd.Timestamp(FROZEN_BRIDGE_END) + pd.Timedelta(days=1)).date().isoformat()
            and row[0]["end_date"] >= row[1]["membership_calendar_end_date"]
        ]
        pool = qualified or derivations
        pool.sort(key=lambda row: (row[0]["completed_at_utc"], row[0]["content_sha256"]))
        calendar, candidate, post_bridge = pool[0]
        report["candidate"] = candidate

        close = _utc(candidate["signal_close_utc"], label="signal close")
        deadline = _utc(candidate["admission_deadline_utc"], label="admission deadline")
        before_close = observed < close
        missed = (
            observed >= deadline
            and ledger["last_decision_signal_date"] != candidate["signal_date"]
        )
        timing_phase = (
            "before_signal_close"
            if before_close
            else "deadline_missed"
            if missed
            else "admission_open"
        )
        report["timing"] = {
            "phase": timing_phase,
            "before_signal_close": before_close,
            "admission_deadline_missed": missed,
            "seconds_until_signal_close": int((close - observed).total_seconds()),
            "seconds_until_admission_deadline": int(
                (deadline - observed).total_seconds()
            ),
        }
        if before_close:
            issues.append(
                _issue(
                    "BEFORE_SIGNAL_CLOSE",
                    "wait",
                    "timing",
                    "signal session has not closed",
                    retryable=True,
                )
            )
        if missed:
            issues.append(
                _issue(
                    "ADMISSION_DEADLINE_MISSED",
                    "fatal",
                    "timing",
                    "the immutable admission deadline passed without a sealed decision",
                    retryable=False,
                )
            )

        full_month = calendar["end_date"] >= candidate[
            "membership_calendar_end_date"
        ] and calendar["start_date"] <= f"{candidate['membership_month']}-01"
        prefix_contiguous = calendar["start_date"] <= (pd.Timestamp(FROZEN_BRIDGE_END) + pd.Timedelta(days=1)).date().isoformat()
        covers_i11 = calendar["end_date"] >= candidate["i_plus_11_date"]
        report["calendar"] = {
            "status": "complete" if full_month and prefix_contiguous and covers_i11 else "incomplete",
            "content_sha256": calendar["content_sha256"],
            "artifact_sha256": calendar["artifact_sha256"],
            "completed_at_utc": calendar["completed_at_utc"],
            "start_date": calendar["start_date"],
            "end_date": calendar["end_date"],
            "full_membership_month": full_month,
            "prefix_contiguous": prefix_contiguous,
            "covers_i_plus_11": covers_i11,
        }
        if not prefix_contiguous:
            issues.append(_issue("CALENDAR_PREFIX_GAP", "wait", "calendar", "official calendar cannot join the frozen prefix", retryable=True))
        if not full_month:
            issues.append(_issue("CALENDAR_MONTH_INCOMPLETE", "wait", "calendar", "official calendar does not cover the complete membership month", retryable=True))
        if not covers_i11:
            issues.append(_issue("CALENDAR_I11_INCOMPLETE", "wait", "calendar", "official calendar does not reach signal i+11", retryable=True))

        membership = _membership_view(root, candidate, observed)
        report["membership"] = membership
        if membership["status"] in {"invalid", "ambiguous"}:
            issues.append(_issue("MEMBERSHIP_INVALID", "error", "membership", "monthly membership artifacts are invalid or ambiguous", retryable=False, details={"candidate_count": membership["candidate_count"]}))

        input_snapshot = _input_snapshot_view(root, candidate, membership, observed)
        report["input_snapshot"] = input_snapshot
        if input_snapshot["status"] in {"invalid", "ambiguous"}:
            issues.append(
                _issue(
                    "INPUT_SNAPSHOT_INVALID",
                    "error",
                    "input_snapshot",
                    "candidate input snapshots are invalid or ambiguous",
                    retryable=False,
                    details={"candidate_count": input_snapshot["candidate_count"]},
                )
            )
        elif input_snapshot["status"] == "not_yet_available":
            issues.append(
                _issue(
                    "INPUT_SNAPSHOT_NOT_YET_AVAILABLE",
                    "wait",
                    "input_snapshot",
                    "candidate input snapshot is later than the observation time",
                    retryable=True,
                )
            )

        stage = (
            "admission"
            if input_snapshot["status"] == "complete"
            else "input_build"
            if membership["status"] == "complete"
            else "membership_build"
            if membership["status"] == "not_built"
            else "invalid"
        )
        liquidity_dates: list[str] = []
        signal_daily_dates: list[str] = []
        post_bridge_signal_dates: list[str] = []
        partition_results: dict[tuple[str, str], dict[str, Any]] = {}

        if stage in {"membership_build", "input_build"}:
            if raw_checkpoint is None:
                code = (
                    "RAW_CHECKPOINT_INVALID"
                    if raw_checkpoint_path.is_file()
                    else "RAW_CHECKPOINT_MISSING"
                )
                raise _InspectionError(
                    code,
                    "raw_checkpoint",
                    "a valid live raw checkpoint is required to build the missing artifact",
                )
            if invalid_calendars:
                raise _InspectionError(
                    "CALENDAR_INVALID",
                    "calendar",
                    "live calendar checkpoint contains invalid entries required by builders",
                )
            conflicting_calendars = _conflicting_calendar_sources(live_calendars)
            if conflicting_calendars:
                raise _InspectionError(
                    "CALENDAR_CONFLICT",
                    "calendar",
                    "live calendar sources conflict on one or more natural dates",
                )
            live_build_options: list[tuple[dict[str, Any], list[str]]] = []
            for live_calendar in live_calendars:
                try:
                    live_candidate, live_post_bridge = _candidate_from_calendar(
                        live_calendar, ledger
                    )
                except _InspectionError:
                    continue
                if (
                    live_candidate == candidate
                    and live_calendar["start_date"]
                    <= (
                        pd.Timestamp(FROZEN_BRIDGE_END) + pd.Timedelta(days=1)
                    ).date().isoformat()
                    and live_calendar["end_date"]
                    >= candidate["membership_calendar_end_date"]
                ):
                    live_build_options.append((live_calendar, live_post_bridge))
            if not live_build_options:
                raise _InspectionError(
                    "CALENDAR_BUILD_HORIZON_INSUFFICIENT",
                    "calendar",
                    "live calendar cannot reproduce the complete candidate build horizon",
                )
            live_build_options.sort(
                key=lambda row: (
                    row[0]["completed_at_utc"],
                    row[0]["content_sha256"],
                )
            )
            calendar, post_bridge = live_build_options[0]
            report["calendar"] = {
                "status": "complete",
                "content_sha256": calendar["content_sha256"],
                "artifact_sha256": calendar["artifact_sha256"],
                "completed_at_utc": calendar["completed_at_utc"],
                "start_date": calendar["start_date"],
                "end_date": calendar["end_date"],
                "full_membership_month": True,
                "prefix_contiguous": True,
                "covers_i_plus_11": True,
            }
            execution_path = _exact_file(
                root / "runtime/data/top500/execution.parquet",
                root / "runtime/data/top500/execution.parquet",
                label="frozen execution",
            )
            frozen_sessions = _parquet_unique_dates(execution_path, "date")
            if (
                len(frozen_sessions) != CANONICAL_CALENDAR_COUNT
                or frozen_sessions[0] != CANONICAL_CALENDAR_ANCHOR
                or frozen_sessions[-1] != FROZEN_BRIDGE_END
                or _calendar_prefix_sha256(frozen_sessions)
                != CANONICAL_CALENDAR_SHA256
            ):
                raise _InspectionError(
                    "FROZEN_CALENDAR_INVALID",
                    "frozen_data",
                    "frozen execution calendar contract differs",
                )
            complete_sessions = [*frozen_sessions, *post_bridge]
            if candidate["signal_date"] not in complete_sessions:
                raise _InspectionError(
                    "CANDIDATE_CALENDAR_INVALID",
                    "calendar",
                    "signal is outside the complete calendar",
                )
            signal_index = complete_sessions.index(candidate["signal_date"])
            if signal_index != candidate["calendar_index"]:
                raise _InspectionError(
                    "CANDIDATE_INDEX_INVALID",
                    "calendar",
                    "derived calendar index differs",
                )

            if stage == "membership_build":
                month_start = f"{candidate['membership_month']}-01"
                prior_sessions = [
                    value for value in complete_sessions if value < month_start
                ]
                liquidity_dates = prior_sessions[-LIQUIDITY_SESSION_COUNT:]
                if (
                    len(liquidity_dates) != LIQUIDITY_SESSION_COUNT
                    or liquidity_dates[-1] != candidate["membership_as_of_date"]
                ):
                    raise _InspectionError(
                        "LIQUIDITY_WINDOW_INVALID",
                        "calendar",
                        "calendar cannot prove the prior 60-session window",
                    )
            else:
                features_path = _exact_file(
                    root / "runtime/data/top500/features.parquet",
                    root / "runtime/data/top500/features.parquet",
                    label="frozen features",
                )
                feature_dates = _parquet_unique_dates(features_path, "date")
                if not feature_dates or feature_dates[-1] >= FROZEN_BRIDGE_END:
                    raise _InspectionError(
                        "FEATURE_CUTOFF_INVALID",
                        "frozen_data",
                        "frozen feature cutoff is invalid",
                    )
                cutoff = feature_dates[-1]
                signal_daily_dates = [
                    value
                    for value in complete_sessions
                    if cutoff <= value <= candidate["signal_date"]
                ]
                post_bridge_signal_dates = [
                    value for value in post_bridge if value <= candidate["signal_date"]
                ]

            required_pairs = {
                *(("daily", value) for value in liquidity_dates),
                *(("daily", value) for value in signal_daily_dates),
                *(("daily_basic", value) for value in post_bridge_signal_dates),
                *(("adj_factor", value) for value in post_bridge_signal_dates),
            }
            partition_results = {
                pair: _partition_result(
                    root, raw_checkpoint, pair[0], pair[1], observed
                )
                for pair in sorted(required_pairs)
            }

        report["coverage"] = {
            "liquidity_daily": _coverage(
                partition_results, "daily", liquidity_dates
            ),
            "signal_daily": _coverage(
                partition_results, "daily", signal_daily_dates
            ),
            "signal_daily_basic": _coverage(
                partition_results, "daily_basic", post_bridge_signal_dates
            ),
            "signal_adj_factor": _coverage(
                partition_results, "adj_factor", post_bridge_signal_dates
            ),
        }
        relevant_coverage = (
            (("daily", ("liquidity_daily",)),)
            if stage == "membership_build"
            else (
                ("daily", ("signal_daily",)),
                ("daily_basic", ("signal_daily_basic",)),
                ("adj_factor", ("signal_adj_factor",)),
            )
            if stage == "input_build"
            else ()
        )
        for dataset, names in relevant_coverage:
            missing = sorted(
                {
                    value
                    for name in names
                    for value in report["coverage"][name]["missing_dates"]
                }
            )
            future = sorted(
                {
                    value
                    for name in names
                    for value in report["coverage"][name][
                        "not_yet_available_dates"
                    ]
                }
            )
            reconcile = sorted(
                {
                    value
                    for name in names
                    for value in report["coverage"][name][
                        "reconcile_dates"
                    ]
                }
            )
            invalid = sorted(
                {
                    value
                    for name in names
                    for value in report["coverage"][name]["invalid_dates"]
                }
            )
            code_prefix = dataset.upper()
            if missing or future or reconcile:
                issues.append(
                    _issue(
                        f"{code_prefix}_PARTITION_MISSING",
                        "wait",
                        "raw_partitions",
                        f"{dataset} coverage is not complete",
                        retryable=True,
                        details={
                            "missing_dates": missing,
                            "not_yet_available_dates": future,
                            "reconcile_dates": reconcile,
                        },
                    )
                )
            if invalid:
                issues.append(
                    _issue(
                        f"{code_prefix}_PARTITION_INVALID",
                        "error",
                        "raw_partitions",
                        f"{dataset} partition evidence is invalid",
                        retryable=False,
                        details={"invalid_dates": invalid},
                    )
                )

        if stage == "membership_build":
            enrichment_path = root / "runtime/data/raw/enrichment-checkpoint.json"
            enrichment = (
                _load_json(enrichment_path)
                if enrichment_path.is_file()
                else {"schema_version": 1, "partitions": {}}
            )
            if enrichment.get("schema_version") != 1 or not isinstance(
                enrichment.get("partitions"), Mapping
            ):
                raise _InspectionError(
                    "REFERENCE_CHECKPOINT_INVALID",
                    "reference",
                    "enrichment checkpoint schema is invalid",
                )
            reference = _reference_view(
                root, enrichment, candidate["membership_as_of_date"], observed
            )
            report["reference"] = reference
            if reference["status"] in {"missing", "not_yet_available"}:
                issues.append(
                    _issue(
                        "REFERENCE_MISSING",
                        "wait",
                        "reference",
                        "exact-as-of bak_basic reference is not available",
                        retryable=True,
                        details={"checkpoint_key": reference["checkpoint_key"]},
                    )
                )
            elif reference["status"] != "complete":
                issues.append(
                    _issue(
                        "REFERENCE_INVALID",
                        "error",
                        "reference",
                        "exact-as-of bak_basic reference evidence is invalid",
                        retryable=False,
                    )
                )
        elif stage in {"input_build", "admission"}:
            report["reference"].update(
                status="satisfied_by_authoritative_replay",
                as_of_date=candidate["membership_as_of_date"],
            )

        calendar_complete = report["calendar"]["status"] == "complete"
        membership_prerequisites = (
            report["coverage"]["liquidity_daily"]["complete"]
            and report["reference"]["status"] == "complete"
        )
        input_prerequisites = all(
            report["coverage"][name]["complete"]
            for name in (
                "signal_daily",
                "signal_daily_basic",
                "signal_adj_factor",
            )
        )
        no_error = not any(item["severity"] in {"error", "fatal"} for item in issues)
        report["ready_for"] = {
            "membership_build": bool(
                not before_close
                and not missed
                and calendar_complete
                and membership_prerequisites
                and no_error
                and membership["status"] == "not_built"
            ),
            "input_build": bool(
                not before_close
                and not missed
                and calendar_complete
                and input_prerequisites
                and no_error
                and membership["status"] == "complete"
                and input_snapshot["status"] == "not_built"
            ),
            # Only the authoritative ledger wrapper may open this gate after
            # replaying the active published target-generator capsule.
            "decision_admission": False,
        }
    except _InspectionError as exc:
        if not any(item["code"] == exc.code for item in issues):
            retryable_codes = {
                "RAW_CHECKPOINT_MISSING",
                "CALENDAR_NOT_AVAILABLE",
                "CANDIDATE_UNAVAILABLE",
                "CALENDAR_HORIZON_INSUFFICIENT",
                "CALENDAR_BUILD_HORIZON_INSUFFICIENT",
                "LIQUIDITY_WINDOW_INVALID",
            }
            retryable = exc.code in retryable_codes
            issues.append(
                _issue(
                    exc.code,
                    "wait" if retryable else "error",
                    exc.component,
                    str(exc),
                    retryable=retryable,
                )
            )
    except Exception as exc:  # Defensive: a readiness command must fail closed.
        issues.append(_issue("INSPECTION_FAILED", "error", "observer", f"readiness inspection failed: {type(exc).__name__}", retryable=False))

    after = _tree_token(root, resolved_ledger)
    if before != after:
        report["stable_view"] = False
        issues.append(_issue("UNSTABLE_VIEW", "error", "filesystem", "observed files changed during inspection", retryable=True))

    fatal = any(item["severity"] == "fatal" for item in issues)
    error = any(item["severity"] == "error" for item in issues)
    codes = {item["code"] for item in issues}
    missing_market_dates = sorted(
        {
            value
            for coverage in report["coverage"].values()
            for value in (
                list(coverage.get("missing_dates", []))
                + list(coverage.get("reconcile_dates", []))
            )
        }
    )
    future_market_dates = sorted(
        {
            value
            for coverage in report["coverage"].values()
            for value in coverage.get("not_yet_available_dates", [])
        }
    )
    candidate = report.get("candidate")
    if fatal:
        report.update(status="terminal", reason="admission_deadline_missed", ready=False, next_action="none")
    elif error:
        report.update(status="blocked", reason="evidence_invalid", ready=False, next_action="none")
    elif (
        codes
        & {
            "RAW_CHECKPOINT_MISSING",
            "CALENDAR_NOT_AVAILABLE",
            "CALENDAR_HORIZON_INSUFFICIENT",
            "CANDIDATE_UNAVAILABLE",
        }
        and observed.astimezone(_SHANGHAI).date()
        >= (pd.Timestamp(FROZEN_BRIDGE_END) + pd.Timedelta(days=1)).date()
    ):
        local_observed = observed.astimezone(_SHANGHAI)
        completed_day = local_observed.date()
        if local_observed.time() < time(16, 0):
            completed_day -= pd.Timedelta(days=1).to_pytimedelta()
        bridge_start = (
            pd.Timestamp(FROZEN_BRIDGE_END) + pd.Timedelta(days=1)
        ).date()
        end_day = max(bridge_start, completed_day)
        start_day = max(bridge_start, end_day - pd.Timedelta(days=31).to_pytimedelta())
        horizon_anchor = pd.Timestamp(end_day) + pd.Timedelta(days=62)
        calendar_end = horizon_anchor.to_period("M").end_time.date().isoformat()
        start_date = start_day.isoformat()
        end_date = end_day.isoformat()
        report.update(
            status="ready",
            reason="calendar_bootstrap_ready",
            ready=True,
            next_action="sync_market_data",
            action={
                "command": "data sync",
                "arguments": {
                    "start_date": start_date,
                    "end_date": end_date,
                    "calendar_end_date": calendar_end,
                    "datasets": ["daily", "daily_basic", "adj_factor"],
                    "resume": True,
                },
                "argv": [
                    "data",
                    "sync",
                    "--from",
                    start_date,
                    "--to",
                    end_date,
                    "--calendar-to",
                    calendar_end,
                    "--dataset",
                    "daily",
                    "--dataset",
                    "daily_basic",
                    "--dataset",
                    "adj_factor",
                    "--resume",
                ],
            },
        )
    elif (
        "BEFORE_SIGNAL_CLOSE" not in codes
        and codes
        & {
            "CALENDAR_PREFIX_GAP",
            "CALENDAR_MONTH_INCOMPLETE",
            "CALENDAR_BUILD_HORIZON_INSUFFICIENT",
        }
        and isinstance(candidate, Mapping)
        and isinstance(candidate.get("signal_date"), str)
        and isinstance(candidate.get("membership_calendar_end_date"), str)
    ):
        signal_date = str(candidate["signal_date"])
        start_date = (
            (pd.Timestamp(FROZEN_BRIDGE_END) + pd.Timedelta(days=1))
            .date()
            .isoformat()
            if "CALENDAR_PREFIX_GAP" in codes
            else signal_date
        )
        calendar_end = str(candidate["membership_calendar_end_date"])
        report.update(
            status="ready",
            reason="calendar_sync_ready",
            ready=True,
            next_action="sync_market_data",
            action={
                "command": "data sync",
                "arguments": {
                    "start_date": start_date,
                    "end_date": signal_date,
                    "calendar_end_date": calendar_end,
                    "datasets": ["daily", "daily_basic", "adj_factor"],
                    "resume": True,
                },
                "argv": [
                    "data",
                    "sync",
                    "--from",
                    start_date,
                    "--to",
                    signal_date,
                    "--calendar-to",
                    calendar_end,
                    "--dataset",
                    "daily",
                    "--dataset",
                    "daily_basic",
                    "--dataset",
                    "adj_factor",
                    "--resume",
                ],
            },
        )
    elif (
        "BEFORE_SIGNAL_CLOSE" not in codes
        and missing_market_dates
        and not future_market_dates
        and isinstance(candidate, Mapping)
        and isinstance(candidate.get("signal_date"), str)
        and isinstance(candidate.get("membership_calendar_end_date"), str)
    ):
        start_date = missing_market_dates[0]
        end_date = str(candidate["signal_date"])
        calendar_end = str(candidate["membership_calendar_end_date"])
        report.update(
            status="ready",
            reason="market_data_sync_ready",
            ready=True,
            next_action="sync_market_data",
            action={
                "command": "data sync",
                "arguments": {
                    "start_date": start_date,
                    "end_date": end_date,
                    "calendar_end_date": calendar_end,
                    "datasets": ["daily", "daily_basic", "adj_factor"],
                    "resume": True,
                },
                "argv": [
                    "data",
                    "sync",
                    "--from",
                    start_date,
                    "--to",
                    end_date,
                    "--calendar-to",
                    calendar_end,
                    "--dataset",
                    "daily",
                    "--dataset",
                    "daily_basic",
                    "--dataset",
                    "adj_factor",
                    "--resume",
                ],
            },
        )
    elif (
        "BEFORE_SIGNAL_CLOSE" not in codes
        and report["reference"].get("status") == "missing"
        and isinstance(candidate, Mapping)
        and isinstance(candidate.get("membership_as_of_date"), str)
    ):
        trade_date = str(candidate["membership_as_of_date"])
        report.update(
            status="ready",
            reason="reference_sync_ready",
            ready=True,
            next_action="sync_reference",
            action={
                "command": "data reference",
                "arguments": {"trade_date": trade_date},
                "argv": ["data", "reference", "--trade-date", trade_date],
            },
        )
    elif report["ready_for"]["input_build"]:
        signal_date = str(candidate["signal_date"])
        membership_path = str(report["membership"]["path"])
        report.update(
            status="ready",
            reason="input_build_ready",
            ready=True,
            next_action="build_input",
            action={
                "command": "prospective input",
                "arguments": {
                    "signal_date": signal_date,
                    "membership_snapshot": membership_path,
                },
                "argv": [
                    "prospective",
                    "input",
                    "--signal-date",
                    signal_date,
                    "--membership-snapshot",
                    membership_path,
                ],
            },
        )
    elif report["ready_for"]["membership_build"]:
        month = str(candidate["membership_month"])
        report.update(
            status="ready",
            reason="membership_build_ready",
            ready=True,
            next_action="build_membership",
            action={
                "command": "prospective membership",
                "arguments": {"month": month},
                "argv": ["prospective", "membership", "--month", month],
            },
        )
    else:
        reason = (
            "before_signal_close"
            if "BEFORE_SIGNAL_CLOSE" in codes
            else "authoritative_target_replay_required"
            if report["input_snapshot"]["status"] == "complete"
            else "data_incomplete"
        )
        report.update(status="waiting", reason=reason, ready=False, next_action="wait")
    report["issues"] = sorted(
        issues,
        key=lambda row: (
            {"fatal": 0, "error": 1, "wait": 2}.get(str(row["severity"]), 9),
            str(row["code"]),
        ),
    )
    return report


def prospective_readiness_exit_code(report: Mapping[str, Any]) -> int:
    """Map the stable readiness status onto a small automation exit code."""

    return {"ready": 0, "waiting": 2, "blocked": 3, "terminal": 4}.get(
        str(report.get("status")), 1
    )


__all__ = [
    "CONTRACT_ID",
    "SCHEMA_VERSION",
    "inspect_prospective_readiness",
    "prospective_readiness_exit_code",
]

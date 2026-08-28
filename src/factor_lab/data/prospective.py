"""Deterministic, point-in-time input snapshots for the 5.0 live route.

The historical canonical store is immutable.  Dates after its feature cutoff
are reconstructed from the pre-activation per-security bridge through
2026-08-21 and append-only, checkpointed raw partitions thereafter.  File
mtimes are never used as evidence of availability.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
from typing import Any, Mapping, Sequence
from uuid import uuid4

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from .catalog import sha256_file
from .sources import turnover_amount_to_rmb


SCHEMA_VERSION = 1
EXPECTED_MEMBERSHIP_SIZE = 500
FROZEN_BRIDGE_END = pd.Timestamp("2026-08-21")
CANONICAL_CALENDAR_ANCHOR = "2017-01-03"
CANONICAL_CALENDAR_COUNT = 2340
CANONICAL_CALENDAR_SHA256 = (
    "49b71c0b4482569d56b00cca8d468c3ec417379ac2b03e2d3afea32e312ef67f"
)
PROSPECTIVE_RELATIVE_ROOT = Path("runtime/prospective/5.0")
IMMUTABLE_SOURCE_RELATIVE_ROOT = PROSPECTIVE_RELATIVE_ROOT / "source-artifacts"
_SHA256 = re.compile(r"[0-9a-f]{64}")
_SUPPLEMENT_DATASETS = ("akshare_hfq", "tushare_daily_basic")
_PRICE_COLUMNS = ("open_hfq", "high_hfq", "low_hfq", "close_hfq")
_BASIC_COLUMNS = ("pe_ttm", "pb")


class ProspectiveDataError(ValueError):
    """Raised when an input cannot be proven complete and point-in-time safe."""


@dataclass(frozen=True)
class ProspectiveInputSnapshot:
    """One verified signal-date snapshot and its content-addressed artifacts."""

    signal_date: str
    trade_date: str
    snapshot_sha256: str
    directory: Path
    manifest_path: Path
    rows_path: Path
    build_receipt_path: Path
    build_completed_at_utc: str
    inputs_available_at_utc: str
    frame: pd.DataFrame
    manifest: Mapping[str, Any]

    @property
    def target_adapter(self) -> Mapping[str, Any]:
        adapter = self.manifest.get("target_adapter")
        if not isinstance(adapter, Mapping):
            raise ProspectiveDataError("snapshot has no verified target adapter")
        return adapter

    @property
    def target_rows_sha256(self) -> str:
        return str(self.target_adapter["target_rows_sha256"])

    @property
    def input_sources_sha256(self) -> str:
        return str(self.target_adapter["input_sources_sha256"])

    @property
    def membership_artifact_sha256(self) -> str:
        return str(self.target_adapter["membership_artifact_sha256"])

    @property
    def calendar_sessions(self) -> tuple[str, ...]:
        return tuple(str(value) for value in self.target_adapter["calendar_sessions"])

    @property
    def target_frame(self) -> pd.DataFrame:
        columns = [str(value) for value in self.target_adapter["columns"]]
        return self.frame.loc[:, columns].copy()


def _canonical_json_bytes(payload: Any) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _load_json(path: Path, *, label: str) -> dict[str, Any]:
    if not path.is_file():
        raise ProspectiveDataError(f"missing {label}: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProspectiveDataError(f"unreadable {label}: {path}") from exc
    if not isinstance(payload, dict):
        raise ProspectiveDataError(f"{label} must be a JSON object")
    return payload


def _require_schema_version(
    value: Mapping[str, Any], *, label: str, expected: int = 1
) -> None:
    version = value.get("schema_version")
    if type(version) is not int or version != expected:
        raise ProspectiveDataError(f"unsupported {label} schema")


def _normalise_date(value: str | pd.Timestamp, *, label: str) -> pd.Timestamp:
    try:
        result = pd.Timestamp(value).normalize()
    except (TypeError, ValueError) as exc:
        raise ProspectiveDataError(f"invalid {label}: {value!r}") from exc
    if pd.isna(result) or getattr(result, "tzinfo", None) is not None:
        raise ProspectiveDataError(f"{label} must be a timezone-naive calendar date")
    return result


def _utc_timestamp(value: Any, *, label: str) -> pd.Timestamp:
    try:
        result = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise ProspectiveDataError(f"invalid {label}: {value!r}") from exc
    if pd.isna(result) or result.tzinfo is None:
        raise ProspectiveDataError(f"{label} must include a timezone")
    return result.tz_convert("UTC")


def _utc_text(value: pd.Timestamp) -> str:
    resolved = value.tz_convert("UTC")
    # Pure target/execution contracts require whole seconds.  Availability is
    # an upper bound, so discarding a fractional second would claim evidence
    # existed earlier than it really did.  Ceiling preserves causality.
    if resolved != resolved.floor("s"):
        resolved = resolved.ceil("s")
    return resolved.strftime("%Y-%m-%dT%H:%M:%SZ")


def _require_sha(value: Any, *, label: str) -> str:
    result = str(value or "")
    if not _SHA256.fullmatch(result):
        raise ProspectiveDataError(f"{label} must be a lowercase SHA-256")
    return result


def _under(path: Path, root: Path, *, label: str) -> Path:
    resolved = path.expanduser().resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError as exc:
        raise ProspectiveDataError(f"{label} escapes project runtime: {resolved}") from exc
    return resolved


def _relative(path: Path, project_root: Path) -> str:
    return path.resolve().relative_to(project_root.resolve()).as_posix()


def _artifact_path(raw_value: Any, *, expected: Path, root: Path, label: str) -> Path:
    candidate = Path(str(raw_value or expected))
    if not candidate.is_absolute():
        candidate = root / candidate
    resolved = _under(candidate, root, label=label)
    if resolved != expected.resolve():
        raise ProspectiveDataError(f"{label} path does not match its canonical location")
    return resolved


def _checked_hash(path: Path, expected: Any, *, label: str) -> str:
    wanted = _require_sha(expected, label=f"{label}.sha256")
    if not path.is_file():
        raise ProspectiveDataError(f"missing {label}: {path}")
    actual = sha256_file(path)
    if actual != wanted:
        raise ProspectiveDataError(f"{label} hash mismatch")
    return actual


def _json_value(value: Any) -> Any:
    if value is None or value is pd.NA or pd.isna(value):
        return None
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (int, np.integer)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        number = float(value)
        return number.hex() if math.isfinite(number) else None
    if isinstance(value, (pd.Timestamp, datetime)):
        return pd.Timestamp(value).isoformat()
    return str(value)


def _frame_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return [
        {column: _json_value(value) for column, value in row.items()}
        for row in frame.to_dict(orient="records")
    ]


def _calendar_prefix_sha256(sessions: Sequence[str]) -> str:
    if not sessions:
        raise ProspectiveDataError("canonical execution calendar is empty")
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


def _canonical_execution_calendar(
    project_root: Path,
    *,
    sealed_source: Mapping[str, Any] | None = None,
) -> tuple[list[pd.Timestamp], dict[str, Any]]:
    origin_path = project_root / "runtime/data/top500/execution.parquet"
    if sealed_source is None:
        path, immutable = _capture_immutable_artifact(project_root, origin_path)
        source: dict[str, Any] = {
            "role": "canonical_execution_calendar",
            "path": _relative(origin_path, project_root),
            **immutable,
            "availability_basis": "pre_activation_frozen_canonical",
        }
    else:
        source = dict(sealed_source)
        if (
            source.get("role") != "canonical_execution_calendar"
            or source.get("path") != _relative(origin_path, project_root)
        ):
            raise ProspectiveDataError("sealed canonical execution identity differs")
        path = _resolve_immutable_artifact(project_root, source)
    raw_dates = pd.to_datetime(
        pd.read_parquet(path, columns=["date"])["date"], errors="coerce"
    )
    if raw_dates.isna().any():
        raise ProspectiveDataError("canonical execution contains invalid dates")
    sessions = sorted(raw_dates.dt.strftime("%Y-%m-%d").unique().tolist())
    prefix_sha = _calendar_prefix_sha256(sessions)
    if (
        len(sessions) != CANONICAL_CALENDAR_COUNT
        or sessions[0] != CANONICAL_CALENDAR_ANCHOR
        or sessions[-1] != FROZEN_BRIDGE_END.date().isoformat()
        or prefix_sha != CANONICAL_CALENDAR_SHA256
    ):
        raise ProspectiveDataError("frozen canonical execution calendar contract mismatch")
    derived = {
        "calendar_anchor": sessions[0],
        "calendar_last_session": sessions[-1],
        "calendar_session_count": len(sessions),
        "calendar_prefix_sha256": prefix_sha,
    }
    if sealed_source is not None:
        for key, value in derived.items():
            if source.get(key) != value:
                raise ProspectiveDataError(
                    f"sealed canonical execution {key} differs from CAS bytes"
                )
    source.update(derived)
    return [pd.Timestamp(value) for value in sessions], source


def _write_verified(path: Path, payload: bytes) -> None:
    """Create one immutable file, accepting only an idempotent byte replay."""

    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if not path.is_file() or path.read_bytes() != payload:
            raise ProspectiveDataError(f"content-address collision at {path}")
        return
    temporary = path.parent / f".pending-{os.getpid()}-{uuid4().hex}.tmp"
    try:
        with temporary.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if not path.is_file() or path.read_bytes() != payload:
                raise ProspectiveDataError(f"content-address collision at {path}")
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _media_type(path: Path) -> str:
    suffix = path.suffix.casefold()
    if suffix == ".parquet":
        return "application/vnd.apache.parquet"
    if suffix == ".json":
        return "application/json"
    return "application/octet-stream"


def _capture_immutable_artifact(
    project_root: Path,
    origin_path: Path,
    *,
    expected_sha256: str | None = None,
    sha_field: str = "sha256",
    path_field: str = "immutable_path",
    size_field: str = "size_bytes",
    media_field: str = "media_type",
) -> tuple[Path, dict[str, Any]]:
    """Read origin once, publish exact bytes to CAS, and return the CAS path.

    Callers must parse ``cas_path`` rather than ``origin_path``.  This closes
    the origin hash/parse TOCTOU window and makes later checkpoint refreshes
    irrelevant to the sealed bundle.
    """

    origin = _under(origin_path, project_root, label="source artifact")
    if not origin.is_file():
        raise ProspectiveDataError(f"missing source artifact: {origin}")
    payload = origin.read_bytes()
    digest = _sha256_bytes(payload)
    if expected_sha256 is not None and digest != _require_sha(
        expected_sha256, label=sha_field
    ):
        raise ProspectiveDataError(f"source artifact {sha_field} mismatch")
    relative = (
        IMMUTABLE_SOURCE_RELATIVE_ROOT
        / f"sha256={digest}"
        / "artifact"
    )
    target = project_root / relative
    _write_verified(target, payload)
    return target, {
        sha_field: digest,
        path_field: relative.as_posix(),
        size_field: len(payload),
        media_field: _media_type(origin),
    }


def _resolve_immutable_artifact(
    project_root: Path,
    source: Mapping[str, Any],
    *,
    sha_field: str = "sha256",
    path_field: str = "immutable_path",
    size_field: str = "size_bytes",
    media_field: str = "media_type",
) -> Path:
    digest = _require_sha(source.get(sha_field), label=sha_field)
    expected_relative = (
        IMMUTABLE_SOURCE_RELATIVE_ROOT
        / f"sha256={digest}"
        / "artifact"
    )
    if source.get(path_field) != expected_relative.as_posix():
        raise ProspectiveDataError(f"{path_field} is not the canonical CAS path")
    size = source.get(size_field)
    if type(size) is not int or size < 0:
        raise ProspectiveDataError(f"{size_field} must be a non-negative integer")
    media_type = source.get(media_field)
    if media_type not in {
        "application/vnd.apache.parquet",
        "application/json",
        "application/octet-stream",
    }:
        raise ProspectiveDataError(f"unsupported {media_field}")
    path = (project_root / expected_relative).resolve()
    if (
        not path.is_file()
        or path.stat().st_size != size
        or sha256_file(path) != digest
    ):
        raise ProspectiveDataError("immutable source artifact bytes differ from contract")
    return path


def _membership_source(
    project_root: Path,
    signal_date: pd.Timestamp,
    trade_date: pd.Timestamp,
    *,
    membership_snapshot_path: str | Path | None,
    availability_cap: pd.Timestamp | None,
    sealed_source: Mapping[str, Any] | None = None,
) -> tuple[pd.DataFrame, dict[str, Any], list[pd.Timestamp]]:
    month = trade_date.strftime("%Y-%m")
    canonical_path = project_root / "runtime/data/top500/membership.parquet"
    availability: list[pd.Timestamp] = []
    if sealed_source is not None:
        source = dict(sealed_source)
        if source.get("role") != "membership":
            raise ProspectiveDataError("sealed membership source role differs")
        kind = source.get("kind")
        if kind == "frozen_canonical":
            expected_origin = canonical_path
        elif kind == "content_addressed_monthly_snapshot":
            expected_origin = _under(
                project_root / str(source.get("path") or ""),
                project_root / PROSPECTIVE_RELATIVE_ROOT / "membership",
                label="sealed membership origin",
            )
            if (
                expected_origin.name != "membership.parquet"
                or expected_origin.parent.name
                != _require_sha(source.get("sha256"), label="membership artifact hash")
            ):
                raise ProspectiveDataError("sealed monthly membership path/hash differs")
        else:
            raise ProspectiveDataError("sealed membership kind is unsupported")
        if source.get("path") != _relative(expected_origin, project_root):
            raise ProspectiveDataError("sealed membership origin path differs")
        path = _resolve_immutable_artifact(project_root, source)
        if kind == "content_addressed_monthly_snapshot":
            if source.get("manifest_path") != _relative(
                expected_origin.with_name("manifest.json"), project_root
            ):
                raise ProspectiveDataError("sealed membership manifest origin path differs")
            manifest_path = _resolve_immutable_artifact(
                project_root,
                source,
                sha_field="manifest_sha256",
                path_field="immutable_manifest_path",
                size_field="manifest_size_bytes",
                media_field="manifest_media_type",
            )
            manifest = _load_json(manifest_path, label="immutable membership manifest")
            _require_schema_version(manifest, label="immutable membership manifest")
            if (
                str(manifest.get("membership_month") or "") != month
                or _require_sha(
                    manifest.get("artifact_sha256"), label="membership manifest artifact hash"
                )
                != source.get("sha256")
            ):
                raise ProspectiveDataError("sealed membership manifest identity differs")
            completed = _utc_timestamp(
                manifest.get("completed_at_utc"),
                label="membership completed_at_utc",
            )
            if source.get("completed_at_utc") != _utc_text(completed):
                raise ProspectiveDataError("sealed membership completion differs")
            if availability_cap is not None and completed > availability_cap:
                raise ProspectiveDataError("membership was not available by the requested cutoff")
            try:
                from .prospective_membership import (
                    load_prospective_membership_snapshot,
                )

                verified_membership = load_prospective_membership_snapshot(
                    expected_origin,
                    project_root=project_root,
                    available_at_utc=availability_cap,
                    _sealed_artifact_source=source,
                )
            except Exception as exc:
                raise ProspectiveDataError(
                    "sealed monthly membership failed full immutable source replay"
                ) from exc
            if (
                verified_membership.artifact_sha256 != source.get("sha256")
                or verified_membership.manifest != manifest
            ):
                raise ProspectiveDataError(
                    "sealed monthly membership source replay identity differs"
                )
            availability.append(completed)
        frame = pd.read_parquet(path)
    elif membership_snapshot_path is None:
        path, immutable = _capture_immutable_artifact(project_root, canonical_path)
        frame = pd.read_parquet(path)
        source = {
            "role": "membership",
            "kind": "frozen_canonical",
            "path": _relative(canonical_path, project_root),
            **immutable,
            "availability_basis": "pre_activation_frozen_canonical",
        }
    else:
        requested_membership_path = Path(membership_snapshot_path)
        if not requested_membership_path.is_absolute():
            requested_membership_path = project_root / requested_membership_path
        path = _under(
            requested_membership_path,
            project_root / PROSPECTIVE_RELATIVE_ROOT / "membership",
            label="membership snapshot",
        )
        if path.suffix.casefold() != ".parquet" or not path.is_file():
            raise ProspectiveDataError("membership snapshot must be an existing Parquet file")
        # Admission is stricter than a self-consistent artifact/manifest pair:
        # the monthly builder must independently replay its frozen rule from
        # immutable source CAS before these bytes may enter a signal snapshot.
        try:
            from .prospective_membership import load_prospective_membership_snapshot

            verified_membership = load_prospective_membership_snapshot(
                path,
                project_root=project_root,
                available_at_utc=availability_cap,
            )
        except Exception as exc:
            raise ProspectiveDataError(
                "monthly membership failed immutable source replay"
            ) from exc
        path = verified_membership.membership_path
        manifest_path = verified_membership.manifest_path
        origin_artifact_sha = _require_sha(
            verified_membership.artifact_sha256,
            label="membership snapshot artifact hash",
        )
        immutable_path, immutable = _capture_immutable_artifact(
            project_root,
            path,
            expected_sha256=origin_artifact_sha,
        )
        immutable_manifest_path, immutable_manifest = _capture_immutable_artifact(
            project_root,
            manifest_path,
            sha_field="manifest_sha256",
            path_field="immutable_manifest_path",
            size_field="manifest_size_bytes",
            media_field="manifest_media_type",
        )
        manifest = _load_json(
            immutable_manifest_path, label="immutable membership snapshot manifest"
        )
        _require_schema_version(
            manifest, label="immutable membership snapshot manifest"
        )
        if str(manifest.get("membership_month") or "") != month:
            raise ProspectiveDataError("membership snapshot month does not match signal date")
        if _require_sha(
            manifest.get("artifact_sha256"), label="membership artifact hash"
        ) != origin_artifact_sha:
            raise ProspectiveDataError("membership manifest artifact hash mismatch")
        completed = _utc_timestamp(
            manifest.get("completed_at_utc"),
            label="membership completed_at_utc",
        )
        if availability_cap is not None and completed > availability_cap:
            raise ProspectiveDataError("membership was not available by the requested cutoff")
        availability.append(completed)
        frame = verified_membership.frame.copy()
        if len(frame) != int(manifest.get("row_count") or -1):
            raise ProspectiveDataError("membership manifest row count mismatch")
        source = {
            "role": "membership",
            "kind": "content_addressed_monthly_snapshot",
            "path": _relative(path, project_root),
            **immutable,
            "manifest_path": _relative(manifest_path, project_root),
            **immutable_manifest,
            "completed_at_utc": _utc_text(completed),
            "availability_basis": "manifest_completed_at_utc",
        }

    required = {"ts_code", "membership_month"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ProspectiveDataError(f"membership missing columns: {missing}")
    selected = frame.loc[frame["membership_month"].astype("string").eq(month)].copy()
    selected["ts_code"] = selected["ts_code"].astype("string").str.strip()
    if len(selected) != EXPECTED_MEMBERSHIP_SIZE:
        if membership_snapshot_path is None and month not in set(
            frame["membership_month"].astype("string")
        ):
            raise ProspectiveDataError(
                f"canonical membership has no {month}; provide a content-addressed monthly snapshot"
            )
        raise ProspectiveDataError(
            f"membership {month} must contain exactly {EXPECTED_MEMBERSHIP_SIZE} rows"
        )
    if selected["ts_code"].eq("").any() or selected["ts_code"].duplicated().any():
        raise ProspectiveDataError("membership contains blank or duplicate securities")
    if "as_of_date" in selected:
        as_of = pd.to_datetime(selected["as_of_date"], errors="coerce")
        if as_of.isna().any() or bool(as_of.gt(signal_date).any()):
            raise ProspectiveDataError("membership as_of_date exceeds the signal date")
    # These are effective-state dates, not evidence receipt timestamps.  A
    # month-end signal legitimately selects the membership effective at the
    # next open; actual availability remains proven by completed_at_utc and is
    # separately bounded by the next-trade admission deadline.
    for column in ("state_available_date", "effective_start_date"):
        if column not in selected:
            continue
        values = pd.to_datetime(selected[column], errors="coerce")
        if values.isna().any() or bool(values.gt(trade_date).any()):
            raise ProspectiveDataError(
                f"membership {column} exceeds the official trade date"
            )
    if "eligible" not in selected:
        selected["eligible"] = True
    selected["eligible"] = selected["eligible"].fillna(False).astype(bool)
    selected = selected.sort_values("ts_code", kind="mergesort").reset_index(drop=True)
    source.update(
        {
            "membership_month": month,
            "row_count": int(len(selected)),
            "selected_tickers_sha256": _sha256_bytes(
                _canonical_json_bytes(selected["ts_code"].tolist())
            ),
        }
    )
    if sealed_source is not None and source != dict(sealed_source):
        raise ProspectiveDataError("sealed membership contract differs from CAS bytes")
    if (
        sealed_source is not None
        and kind == "content_addressed_monthly_snapshot"
        and int(manifest.get("row_count") or -1) != len(frame)
    ):
        raise ProspectiveDataError("sealed membership manifest row count differs")
    return selected, source, availability


def _calendar_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    work = frame.copy()
    work["cal_date"] = pd.to_datetime(work["cal_date"], errors="coerce")
    work["pretrade_date"] = pd.to_datetime(work.get("pretrade_date"), errors="coerce")
    if work["cal_date"].isna().any():
        raise ProspectiveDataError("calendar artifact contains invalid dates")
    return [
        {
            "cal_date": row.cal_date.date().isoformat(),
            "exchange": str(row.exchange),
            "is_open": bool(row.is_open),
            "pretrade_date": (
                row.pretrade_date.date().isoformat()
                if not pd.isna(row.pretrade_date)
                else None
            ),
        }
        for row in work.sort_values("cal_date", kind="mergesort").itertuples(index=False)
    ]


def _official_calendar(
    project_root: Path,
    *,
    start_date: pd.Timestamp,
    signal_date: pd.Timestamp,
    checkpoint: Mapping[str, Any],
    availability_cap: pd.Timestamp | None,
    sealed_sources: Sequence[Mapping[str, Any]] | None = None,
) -> tuple[list[pd.Timestamp], pd.Timestamp, dict[str, Any], list[pd.Timestamp]]:
    raw_root = (project_root / "runtime/data/raw").resolve()
    entries = (
        {
            str(source.get("calendar_content_sha256") or ""): source
            for source in sealed_sources
        }
        if sealed_sources is not None
        else checkpoint.get("calendars")
    )
    if not isinstance(entries, Mapping) or not entries:
        raise ProspectiveDataError(
            "official trade calendar is missing; run data sync with calendar persistence"
        )
    candidates: list[tuple[pd.Timestamp, str, dict[str, Any], list[dict[str, Any]]]] = []
    for key, raw_entry in entries.items():
        if not isinstance(raw_entry, Mapping) or (
            sealed_sources is None and raw_entry.get("status") != "complete"
        ):
            continue
        entry_start = _normalise_date(
            raw_entry.get("source_start_date", raw_entry.get("start_date")),
            label="calendar start",
        )
        entry_end = _normalise_date(
            raw_entry.get("source_end_date", raw_entry.get("end_date")),
            label="calendar end",
        )
        if entry_end < start_date or entry_start > signal_date + pd.Timedelta(days=15):
            continue
        content_sha = _require_sha(
            raw_entry.get("calendar_content_sha256"), label="calendar content hash"
        )
        if str(key) != content_sha:
            raise ProspectiveDataError("calendar checkpoint key/hash mismatch")
        completed = _utc_timestamp(
            raw_entry.get("completed_at_utc"), label="calendar completed_at_utc"
        )
        if availability_cap is not None and completed > availability_cap:
            continue
        expected_path = raw_root / "trade_cal" / f"calendar_sha256={content_sha}" / "part-000.parquet"
        if sealed_sources is None:
            origin_path = _artifact_path(
                raw_entry.get("path"),
                expected=expected_path,
                root=raw_root,
                label="calendar artifact",
            )
            path, immutable = _capture_immutable_artifact(
                project_root,
                origin_path,
                expected_sha256=str(raw_entry.get("artifact_sha256") or ""),
                sha_field="artifact_sha256",
            )
            origin_manifest_path = origin_path.with_name("manifest.json")
            if not origin_manifest_path.is_file():
                raise ProspectiveDataError("calendar checkpoint lacks immutable manifest evidence")
            manifest_path, immutable_manifest = _capture_immutable_artifact(
                project_root,
                origin_manifest_path,
                expected_sha256=str(raw_entry.get("manifest_sha256") or ""),
                sha_field="manifest_sha256",
                path_field="immutable_manifest_path",
                size_field="manifest_size_bytes",
                media_field="manifest_media_type",
            )
            origin_path_text = _relative(origin_path, project_root)
            origin_manifest_text = _relative(origin_manifest_path, project_root)
        else:
            if raw_entry.get("path") != _relative(expected_path, project_root):
                raise ProspectiveDataError("sealed calendar origin path differs")
            path = _resolve_immutable_artifact(
                project_root, raw_entry, sha_field="artifact_sha256"
            )
            manifest_path = _resolve_immutable_artifact(
                project_root,
                raw_entry,
                sha_field="manifest_sha256",
                path_field="immutable_manifest_path",
                size_field="manifest_size_bytes",
                media_field="manifest_media_type",
            )
            immutable = {
                key: raw_entry[key]
                for key in (
                    "artifact_sha256",
                    "immutable_path",
                    "size_bytes",
                    "media_type",
                )
            }
            immutable_manifest = {
                key: raw_entry[key]
                for key in (
                    "manifest_sha256",
                    "immutable_manifest_path",
                    "manifest_size_bytes",
                    "manifest_media_type",
                )
            }
            origin_path_text = str(raw_entry["path"])
            origin_manifest_text = str(raw_entry["manifest_path"])
        artifact_sha = str(immutable["artifact_sha256"])
        frame = pd.read_parquet(path)
        required = {"exchange", "cal_date", "is_open", "pretrade_date"}
        if not required.issubset(frame.columns):
            raise ProspectiveDataError("calendar artifact schema is incomplete")
        records = _calendar_records(frame[list(required)])
        if _sha256_bytes(_canonical_json_bytes(records)) != content_sha:
            raise ProspectiveDataError("calendar logical content hash mismatch")
        if len(records) != int(raw_entry.get("row_count") or -1):
            raise ProspectiveDataError("calendar row count mismatch")
        manifest = _load_json(manifest_path, label="immutable calendar manifest")
        _require_schema_version(manifest, label="immutable calendar manifest")
        if (
            str(manifest.get("calendar_content_sha256") or "") != content_sha
            or str(manifest.get("artifact_sha256") or "") != artifact_sha
            or str(manifest.get("records_sha256") or "") != content_sha
        ):
            raise ProspectiveDataError("calendar immutable manifest binding mismatch")
        entry = {
            "calendar_content_sha256": content_sha,
            "path": origin_path_text,
            **immutable,
            "manifest_path": origin_manifest_text,
            **immutable_manifest,
            "completed_at_utc": _utc_text(completed),
            "availability_basis": "checkpoint_completed_at_utc",
            "source_start_date": records[0]["cal_date"],
            "source_end_date": records[-1]["cal_date"],
            "row_count": len(records),
            "open_day_count": sum(bool(row["is_open"]) for row in records),
        }
        if sealed_sources is not None and entry != dict(raw_entry):
            raise ProspectiveDataError("sealed calendar contract differs from CAS bytes")
        candidates.append((completed, content_sha, entry, records))
    if not candidates:
        raise ProspectiveDataError("no available official calendar artifact covers signal date")

    chosen: dict[str, tuple[dict[str, Any], tuple[pd.Timestamp, str]]] = {}
    for completed, content_sha, entry, records in candidates:
        priority = (completed, content_sha)
        for row in records:
            date = str(row["cal_date"])
            prior = chosen.get(date)
            if prior is not None and prior[0] != row:
                raise ProspectiveDataError(f"conflicting official calendar rows for {date}")
            if prior is None or priority < prior[1]:
                chosen[date] = (row, priority)

    signal_key = signal_date.date().isoformat()
    signal_row = chosen.get(signal_key)
    if signal_row is None or not bool(signal_row[0]["is_open"]):
        raise ProspectiveDataError("signal date is not a proven official open day")
    next_trade: pd.Timestamp | None = None
    cursor = signal_date + pd.Timedelta(days=1)
    while cursor <= signal_date + pd.Timedelta(days=15):
        row = chosen.get(cursor.date().isoformat())
        if row is None:
            raise ProspectiveDataError("official calendar has a gap after signal date")
        if bool(row[0]["is_open"]):
            next_trade = cursor
            break
        cursor += pd.Timedelta(days=1)
    if next_trade is None:
        raise ProspectiveDataError("official calendar lacks the next execution date")
    expected = pd.date_range(start_date, next_trade, freq="D")
    missing_dates = [date for date in expected if date.date().isoformat() not in chosen]
    if missing_dates:
        raise ProspectiveDataError("official calendar has a gap in the reconstruction interval")
    selected_rows = [chosen[date.date().isoformat()][0] for date in expected]
    open_dates = [
        pd.Timestamp(row["cal_date"]) for row in selected_rows if bool(row["is_open"])
    ]
    used_priorities = {chosen[row["cal_date"]][1] for row in selected_rows}
    used_sources = [
        entry
        for completed, content_sha, entry, _ in candidates
        if (completed, content_sha) in used_priorities
    ]
    availability = [priority[0] for priority in used_priorities]
    calendar_manifest = {
        "role": "official_trade_calendar",
        "exchange": "SSE",
        "start_date": start_date.date().isoformat(),
        "end_date": next_trade.date().isoformat(),
        "signal_is_open": True,
        "next_trade_date": next_trade.date().isoformat(),
        "calendar_sessions": [value.date().isoformat() for value in open_dates],
        "selected_rows_sha256": _sha256_bytes(_canonical_json_bytes(selected_rows)),
        "sources": sorted(used_sources, key=lambda row: row["calendar_content_sha256"]),
    }
    return open_dates, next_trade, calendar_manifest, availability


def _checkpoint_partition(
    project_root: Path,
    checkpoint: Mapping[str, Any],
    *,
    dataset: str,
    trade_date: pd.Timestamp,
    availability_cap: pd.Timestamp | None,
    sealed_source: Mapping[str, Any] | None = None,
) -> tuple[pd.DataFrame, dict[str, Any], pd.Timestamp]:
    date_text = trade_date.date().isoformat()
    key = f"{dataset}/{date_text}"
    entries = checkpoint.get("partitions")
    raw = sealed_source or (entries.get(key) if isinstance(entries, Mapping) else None)
    if not isinstance(raw, Mapping) or (
        sealed_source is None and raw.get("status") != "complete"
    ):
        raise ProspectiveDataError(f"missing complete raw partition {key}")
    if str(raw.get("dataset") or "") != dataset or str(raw.get("trade_date") or "") != date_text:
        raise ProspectiveDataError(f"raw checkpoint identity mismatch for {key}")
    completed = _utc_timestamp(
        raw.get("completed_at_utc"), label=f"{key} completed_at_utc"
    )
    if availability_cap is not None and completed > availability_cap:
        raise ProspectiveDataError(f"raw partition {key} was unavailable by the cutoff")
    raw_root = (project_root / "runtime/data/raw").resolve()
    expected = raw_root / dataset / f"trade_date={date_text}" / "part-000.parquet"
    if sealed_source is None:
        origin_path = _artifact_path(raw.get("path"), expected=expected, root=raw_root, label=key)
        path, immutable = _capture_immutable_artifact(
            project_root,
            origin_path,
            expected_sha256=str(raw.get("sha256") or ""),
        )
        if int(raw.get("size_bytes") or -1) != path.stat().st_size:
            raise ProspectiveDataError(f"raw partition size mismatch for {key}")
        origin_text = _relative(origin_path, project_root)
    else:
        if raw.get("path") != _relative(expected, project_root):
            raise ProspectiveDataError(f"sealed raw partition origin path differs for {key}")
        path = _resolve_immutable_artifact(project_root, raw)
        immutable = {
            field: raw[field]
            for field in ("sha256", "immutable_path", "size_bytes", "media_type")
        }
        origin_text = str(raw["path"])
    digest = str(immutable["sha256"])
    frame = pd.read_parquet(path)
    if len(frame) != int(raw.get("row_count") or -1) or frame.empty:
        raise ProspectiveDataError(f"raw partition row count mismatch for {key}")
    required = {"ts_code", "trade_date"}
    if not required.issubset(frame.columns):
        raise ProspectiveDataError(f"raw partition schema incomplete for {key}")
    parsed_dates = pd.to_datetime(
        frame["trade_date"].astype("string").str.replace("-", "", regex=False),
        format="%Y%m%d",
        errors="coerce",
    )
    if parsed_dates.isna().any() or bool(parsed_dates.ne(trade_date).any()):
        raise ProspectiveDataError(f"raw partition date mismatch for {key}")
    if frame["ts_code"].astype("string").duplicated().any():
        raise ProspectiveDataError(f"raw partition duplicate securities for {key}")
    frame = frame.copy()
    frame["trade_date"] = parsed_dates
    source = {
        "role": "raw_partition",
        "dataset": dataset,
        "trade_date": date_text,
        "path": origin_text,
        **immutable,
        "row_count": int(len(frame)),
        "completed_at_utc": _utc_text(completed),
        "availability_basis": "checkpoint_completed_at_utc",
    }
    if sealed_source is not None and source != dict(sealed_source):
        raise ProspectiveDataError(f"sealed raw partition contract differs for {key}")
    return frame, source, completed


def _supplement_files(
    project_root: Path,
    tickers: Sequence[str],
    *,
    minimum_date: pd.Timestamp,
    maximum_date: pd.Timestamp,
    sealed_sources: Sequence[Mapping[str, Any]] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, list[dict[str, Any]]]:
    root = project_root / "runtime/data/raw/supplements"
    checkpoint_path = root / "supplement_checkpoint.json"
    if sealed_sources is None:
        immutable_checkpoint_path, immutable_checkpoint = _capture_immutable_artifact(
            project_root, checkpoint_path
        )
        checkpoint = _load_json(
            immutable_checkpoint_path, label="immutable supplement checkpoint"
        )
        checkpoint_source = {
            "role": "supplement_checkpoint",
            "path": _relative(checkpoint_path, project_root),
            **immutable_checkpoint,
            "availability_basis": "pre_activation_frozen_bridge",
            "artifact_max_date": FROZEN_BRIDGE_END.date().isoformat(),
        }
        sealed_by_key: dict[tuple[str, str], Mapping[str, Any]] = {}
    else:
        checkpoint_rows = [
            row for row in sealed_sources if row.get("role") == "supplement_checkpoint"
        ]
        if len(checkpoint_rows) != 1:
            raise ProspectiveDataError("sealed supplement contract needs one checkpoint")
        checkpoint_source = dict(checkpoint_rows[0])
        immutable_checkpoint_path = _resolve_immutable_artifact(
            project_root, checkpoint_source
        )
        checkpoint = _load_json(
            immutable_checkpoint_path, label="sealed supplement checkpoint"
        )
        sealed_by_key = {
            (str(row.get("dataset")), str(row.get("ticker"))): row
            for row in sealed_sources
            if row.get("role") == "frozen_supplement"
        }
    sources: list[dict[str, Any]] = [checkpoint_source]
    frames: dict[str, list[pd.DataFrame]] = {name: [] for name in _SUPPLEMENT_DATASETS}
    for dataset in _SUPPLEMENT_DATASETS:
        entries = checkpoint.get(dataset)
        if not isinstance(entries, Mapping):
            raise ProspectiveDataError(f"supplement checkpoint lacks {dataset}")
        for ticker in tickers:
            raw = entries.get(ticker)
            if not isinstance(raw, Mapping) or raw.get("status") != "complete":
                raise ProspectiveDataError(f"missing frozen supplement {dataset}/{ticker}")
            expected = root / dataset / f"ticker={ticker}" / "history.parquet"
            if sealed_sources is None:
                origin_path = _artifact_path(
                    raw.get("path"),
                    expected=expected,
                    root=root,
                    label=f"{dataset}/{ticker}",
                )
                path, immutable = _capture_immutable_artifact(
                    project_root,
                    origin_path,
                    expected_sha256=str(raw.get("sha256") or ""),
                )
                origin_text = _relative(origin_path, project_root)
            else:
                sealed = sealed_by_key.get((dataset, ticker))
                if sealed is None:
                    raise ProspectiveDataError(
                        f"sealed supplement is missing {dataset}/{ticker}"
                    )
                if sealed.get("path") != _relative(expected, project_root):
                    raise ProspectiveDataError("sealed supplement origin path differs")
                path = _resolve_immutable_artifact(project_root, sealed)
                immutable = {
                    field: sealed[field]
                    for field in (
                        "sha256",
                        "immutable_path",
                        "size_bytes",
                        "media_type",
                    )
                }
                origin_text = str(sealed["path"])
            digest = str(immutable["sha256"])
            parquet = pq.ParquetFile(path)
            if parquet.metadata.num_rows != int(raw.get("rows") or -1):
                raise ProspectiveDataError(f"supplement row count mismatch for {dataset}/{ticker}")
            start = _normalise_date(raw.get("start_date"), label="supplement start")
            end = _normalise_date(raw.get("end_date"), label="supplement end")
            if end != FROZEN_BRIDGE_END or start > maximum_date:
                raise ProspectiveDataError(f"supplement date coverage mismatch for {dataset}/{ticker}")
            frame = pd.read_parquet(
                path,
                filters=[
                    ("trade_date", ">=", minimum_date),
                    ("trade_date", "<=", maximum_date),
                ],
            )
            if not frame.empty:
                frames[dataset].append(frame)
            source = {
                    "role": "frozen_supplement",
                    "dataset": dataset,
                    "ticker": ticker,
                    "path": origin_text,
                    **immutable,
                    "row_count": int(parquet.metadata.num_rows),
                    "artifact_start_date": start.date().isoformat(),
                    "artifact_end_date": end.date().isoformat(),
                    "selected_max_date": maximum_date.date().isoformat(),
                    "availability_basis": "pre_activation_frozen_bridge",
                }
            if sealed_sources is not None and source != dict(sealed):
                raise ProspectiveDataError(
                    f"sealed supplement contract differs for {dataset}/{ticker}"
                )
            sources.append(source)
    price = pd.concat(frames["akshare_hfq"], ignore_index=True)
    basic = pd.concat(frames["tushare_daily_basic"], ignore_index=True)
    for name, frame in (("akshare_hfq", price), ("tushare_daily_basic", basic)):
        required = {"ts_code", "trade_date"} | (
            set(_PRICE_COLUMNS) | {"amount_akshare", "price_source"}
            if name == "akshare_hfq"
            else set(_BASIC_COLUMNS)
        )
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ProspectiveDataError(f"{name} supplement missing columns: {missing}")
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
        if frame["trade_date"].isna().any() or frame.duplicated(["ts_code", "trade_date"]).any():
            raise ProspectiveDataError(f"{name} supplement has invalid or duplicate keys")
    return price, basic, sources


def _close_enough(left: pd.Series, right: pd.Series, *, rtol: float, atol: float) -> bool:
    a = pd.to_numeric(left, errors="coerce").to_numpy(dtype=float)
    b = pd.to_numeric(right, errors="coerce").to_numpy(dtype=float)
    return bool(np.isclose(a, b, rtol=rtol, atol=atol, equal_nan=True).all())


def _calibrate_canonical_boundary(
    canonical: pd.DataFrame,
    price: pd.DataFrame,
    basic: pd.DataFrame,
    *,
    cutoff: pd.Timestamp,
) -> dict[str, Any]:
    tickers = sorted(canonical["ticker"].astype("string").unique().tolist())
    boundary_price = price.loc[
        price["trade_date"].eq(cutoff) & price["ts_code"].astype("string").isin(tickers)
    ].copy()
    boundary_basic = basic.loc[
        basic["trade_date"].eq(cutoff) & basic["ts_code"].astype("string").isin(tickers)
    ].copy()
    merged = canonical.merge(
        boundary_price,
        left_on="ticker",
        right_on="ts_code",
        suffixes=("_canonical", "_bridge"),
        validate="one_to_one",
    ).merge(
        boundary_basic[["ts_code", *_BASIC_COLUMNS]],
        left_on="ticker",
        right_on="ts_code",
        suffixes=("", "_basic"),
        validate="one_to_one",
    )
    if len(merged) != len(canonical):
        raise ProspectiveDataError("bridge does not cover the canonical boundary universe")
    comparisons = [
        (f"{column}_canonical", f"{column}_bridge") for column in _PRICE_COLUMNS
    ] + [
        ("amount_akshare_canonical", "amount_akshare_bridge"),
        ("pe_ttm", "pe_ttm_basic"),
        ("pb", "pb_basic"),
    ]
    for left, right in comparisons:
        if not _close_enough(merged[left], merged[right], rtol=1e-12, atol=1e-12):
            raise ProspectiveDataError(f"bridge/canonical boundary calibration failed: {left}")

    history = price.loc[price["ts_code"].astype("string").isin(tickers)].copy()
    history = history.sort_values(["ts_code", "trade_date"], kind="mergesort")
    history["return_1d"] = history.groupby("ts_code", sort=False)["close_hfq"].pct_change(
        fill_method=None
    )
    history["volatility_20"] = history.groupby("ts_code", sort=False)["return_1d"].transform(
        lambda values: values.rolling(20, min_periods=20).std(ddof=1)
    )
    computed = history.loc[history["trade_date"].eq(cutoff), ["ts_code", "volatility_20"]]
    merged_vol = canonical.merge(computed, left_on="ticker", right_on="ts_code", validate="one_to_one")
    if not _close_enough(
        merged_vol["volatility_20_x"], merged_vol["volatility_20_y"], rtol=1e-12, atol=1e-12
    ):
        raise ProspectiveDataError("bridge volatility does not reproduce canonical ddof=1")
    expected_earnings = 1.0 / pd.to_numeric(canonical["pe_ttm"], errors="coerce")
    expected_book = 1.0 / pd.to_numeric(canonical["pb"], errors="coerce")
    if not _close_enough(canonical["earnings_yield"], expected_earnings, rtol=1e-12, atol=1e-12):
        raise ProspectiveDataError("canonical earnings_yield is not 1/pe_ttm")
    if not _close_enough(canonical["book_yield"], expected_book, rtol=1e-12, atol=1e-12):
        raise ProspectiveDataError("canonical book_yield is not 1/pb")
    return {
        "canonical_cutoff": cutoff.date().isoformat(),
        "ticker_count": int(len(canonical)),
        "ohlc_amount_basic_exact": True,
        "volatility_20_ddof": 1,
        "volatility_exact": True,
        "yield_formula_exact": True,
    }


def _raw_daily_audit(frame: pd.DataFrame) -> pd.DataFrame:
    required = {
        "ts_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "pct_chg",
        "amount",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ProspectiveDataError(f"daily partition missing columns: {missing}")
    result = frame.copy()
    for column in required - {"ts_code", "trade_date"}:
        result[column] = pd.to_numeric(result[column], errors="coerce")
    finite = np.isfinite(result[["open", "high", "low", "close", "pre_close", "amount"]])
    if not bool(finite.all().all()) or bool((result[["open", "high", "low", "close", "pre_close"]] <= 0).any().any()):
        raise ProspectiveDataError("daily partition contains invalid prices or amount")
    expected_pct = (result["close"] / result["pre_close"] - 1.0) * 100.0
    if not bool(np.isclose(result["pct_chg"], expected_pct, rtol=0.0, atol=0.02).all()):
        raise ProspectiveDataError("daily pct_chg is inconsistent with close/pre_close")
    return result


def _build_signal_frame(
    members: pd.DataFrame,
    price: pd.DataFrame,
    basic_bridge: pd.DataFrame,
    raw_daily: pd.DataFrame,
    raw_basic: pd.DataFrame,
    raw_adj: pd.DataFrame,
    *,
    signal_date: pd.Timestamp,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    member_set = set(members["ts_code"].astype(str))
    prices = price.loc[price["ts_code"].astype(str).isin(member_set)].copy()
    prices = prices.sort_values(["ts_code", "trade_date"], kind="mergesort")
    daily = _raw_daily_audit(raw_daily.loc[raw_daily["ts_code"].astype(str).isin(member_set)])
    daily = daily.sort_values(["ts_code", "trade_date"], kind="mergesort")

    bridge_daily = daily.loc[daily["trade_date"].le(FROZEN_BRIDGE_END)].copy()
    bridge_join = bridge_daily.merge(
        prices[["ts_code", "trade_date", *_PRICE_COLUMNS, "amount_akshare", "price_source"]],
        on=["ts_code", "trade_date"],
        how="inner",
        validate="one_to_one",
    )
    daily_bridge_keys = set(zip(bridge_daily["ts_code"], bridge_daily["trade_date"]))
    joined_bridge_keys = set(zip(bridge_join["ts_code"], bridge_join["trade_date"]))
    if daily_bridge_keys != joined_bridge_keys:
        raise ProspectiveDataError("bridge price coverage does not match raw daily observations")
    expected_amount = turnover_amount_to_rmb(bridge_join["amount"], source="tushare_daily")
    if not _close_enough(
        bridge_join["amount_akshare"], expected_amount, rtol=2e-8, atol=1.0
    ):
        raise ProspectiveDataError("bridge amount is not calibrated to raw daily RMB")
    close_multiplier = bridge_join["close_hfq"] / bridge_join["close"]
    for adjusted, raw in (("open_hfq", "open"), ("high_hfq", "high"), ("low_hfq", "low")):
        relative = (bridge_join[adjusted] / bridge_join[raw] / close_multiplier - 1.0).abs()
        if bool(relative.gt(0.005).any()):
            raise ProspectiveDataError("bridge OHLC adjustment is internally inconsistent")

    history = prices[["ts_code", "trade_date", *_PRICE_COLUMNS, "amount_akshare", "price_source"]].copy()
    history["open"] = np.nan
    history["high"] = np.nan
    history["low"] = np.nan
    history["close"] = np.nan
    history["pre_close"] = np.nan
    history["pct_chg"] = np.nan
    history["amount"] = np.nan
    history["amount_rmb"] = history["amount_akshare"]
    history["adj_factor"] = np.nan
    history["effective_adj_multiplier"] = np.nan
    history["adj_calibration_multiplier"] = np.nan
    history["adj_source"] = "bridge_implied_hfq_over_raw_close"
    if not bridge_join.empty:
        raw_values = bridge_join.set_index(["ts_code", "trade_date"])
        keys = pd.MultiIndex.from_frame(history[["ts_code", "trade_date"]])
        for column in ("open", "high", "low", "close", "pre_close", "pct_chg", "amount"):
            history[column] = raw_values[column].reindex(keys).to_numpy()
        history["effective_adj_multiplier"] = history["close_hfq"] / history["close"]

    future_daily = daily.loc[daily["trade_date"].gt(FROZEN_BRIDGE_END)].copy()
    future_adj = raw_adj.loc[raw_adj["ts_code"].astype(str).isin(member_set)].copy()
    if not future_daily.empty:
        if not {"ts_code", "trade_date", "adj_factor"}.issubset(future_adj.columns):
            raise ProspectiveDataError("future adj_factor partition schema is incomplete")
        future_adj["adj_factor"] = pd.to_numeric(future_adj["adj_factor"], errors="coerce")
        future = future_daily.merge(
            future_adj[["ts_code", "trade_date", "adj_factor"]],
            on=["ts_code", "trade_date"],
            how="left",
            validate="one_to_one",
        )
        if future["adj_factor"].isna().any() or bool((future["adj_factor"] <= 0).any()):
            raise ProspectiveDataError("future daily rows lack a positive adj_factor")
        built_rows: list[dict[str, Any]] = []
        bridge_last = history.sort_values("trade_date").groupby("ts_code", sort=False).tail(1)
        anchors = bridge_last.set_index("ts_code")
        for ticker, group in future.groupby("ts_code", sort=True):
            if ticker not in anchors.index:
                raise ProspectiveDataError(f"no frozen bridge price anchor for {ticker}")
            anchor = anchors.loc[ticker]
            previous_adjusted = float(anchor["close_hfq"])
            calibration: float | None = None
            for row in group.sort_values("trade_date", kind="mergesort").itertuples(index=False):
                if calibration is None:
                    # Calibrate Tushare's arbitrary adjustment-factor scale to
                    # the immutable HFQ bridge at the first new observation.
                    # Using pre_close also handles an ex-right first session.
                    denominator = float(row.pre_close) * float(row.adj_factor)
                    if not math.isfinite(denominator) or denominator <= 0:
                        raise ProspectiveDataError(
                            f"missing adjusted bridge anchor for {ticker}"
                        )
                    calibration = previous_adjusted / denominator
                adjusted_pre_close = (
                    float(row.pre_close) * float(row.adj_factor) * calibration
                )
                if not math.isclose(
                    adjusted_pre_close,
                    previous_adjusted,
                    rel_tol=0.005,
                    abs_tol=0.02,
                ):
                    raise ProspectiveDataError(
                        f"adjusted price continuity failed for {ticker}"
                    )
                effective = float(row.adj_factor) * calibration
                adjusted_close = float(row.close) * effective
                built_rows.append(
                    {
                        "ts_code": ticker,
                        "trade_date": row.trade_date,
                        "open_hfq": float(row.open) * effective,
                        "high_hfq": float(row.high) * effective,
                        "low_hfq": float(row.low) * effective,
                        "close_hfq": adjusted_close,
                        "amount_akshare": float(row.amount) * 1000.0,
                        "price_source": "tushare_raw_times_adj_factor_calibrated_to_bridge",
                        "open": float(row.open),
                        "high": float(row.high),
                        "low": float(row.low),
                        "close": float(row.close),
                        "pre_close": float(row.pre_close),
                        "pct_chg": float(row.pct_chg),
                        "amount": float(row.amount),
                        "amount_rmb": float(row.amount) * 1000.0,
                        "adj_factor": float(row.adj_factor),
                        "effective_adj_multiplier": effective,
                        "adj_calibration_multiplier": calibration,
                        "adj_source": "checkpointed_tushare_adj_factor_calibrated_to_bridge",
                    }
                )
                previous_adjusted = adjusted_close
        history = pd.concat([history, pd.DataFrame(built_rows)], ignore_index=True, sort=False)

    history = history.sort_values(["ts_code", "trade_date"], kind="mergesort")
    history["return_1d"] = history.groupby("ts_code", sort=False)["close_hfq"].pct_change(
        fill_method=None
    )
    history["volatility_20"] = history.groupby("ts_code", sort=False)["return_1d"].transform(
        lambda values: values.rolling(20, min_periods=20).std(ddof=1)
    )
    history["adv_20"] = history.groupby("ts_code", sort=False)["amount_rmb"].transform(
        lambda values: values.rolling(20, min_periods=20).mean()
    )
    signal_prices = history.loc[history["trade_date"].eq(signal_date)].copy()

    if signal_date <= FROZEN_BRIDGE_END:
        signal_basic = basic_bridge.loc[basic_bridge["trade_date"].eq(signal_date)].copy()
        basic_source = "pre_activation_frozen_tushare_daily_basic"
    else:
        signal_basic = raw_basic.loc[
            raw_basic["trade_date"].eq(signal_date)
            & raw_basic["ts_code"].astype(str).isin(member_set)
        ].copy()
        basic_source = "checkpointed_tushare_daily_basic"
    if not {"ts_code", "trade_date", *_BASIC_COLUMNS}.issubset(signal_basic.columns):
        raise ProspectiveDataError("signal daily_basic schema is incomplete")
    signal = signal_prices.merge(
        signal_basic[["ts_code", "trade_date", *_BASIC_COLUMNS]],
        on=["ts_code", "trade_date"],
        how="inner",
        validate="one_to_one",
    )
    observed_daily = set(
        daily.loc[daily["trade_date"].eq(signal_date), "ts_code"].astype(str)
    )
    if set(signal["ts_code"].astype(str)) != observed_daily:
        raise ProspectiveDataError("signal daily/basic/price security sets do not match")
    minimum_rows = max(10, math.ceil(EXPECTED_MEMBERSHIP_SIZE * 0.98))
    if len(signal) < minimum_rows:
        raise ProspectiveDataError("signal market-data coverage is below 98% of membership")

    signal["pe_ttm"] = pd.to_numeric(signal["pe_ttm"], errors="coerce")
    signal["pb"] = pd.to_numeric(signal["pb"], errors="coerce")
    signal["earnings_yield"] = 1.0 / signal["pe_ttm"]
    signal["book_yield"] = 1.0 / signal["pb"]
    member_fields = ["ts_code", "membership_month", "eligible"]
    signal = signal.merge(members[member_fields], on="ts_code", how="left", validate="one_to_one")
    valid = (
        signal[["earnings_yield", "book_yield", "volatility_20", "adv_20"]]
        .apply(pd.to_numeric, errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
        .notna()
        .all(axis=1)
    )
    signal["eligible"] = signal["eligible"].fillna(False).astype(bool) & valid
    signal["universe_member"] = True
    signal["is_one_price_limit_up"] = (
        np.isclose(signal["high"], signal["low"], equal_nan=False) & signal["pct_chg"].gt(0)
    )
    signal["is_one_price_limit_down"] = (
        np.isclose(signal["high"], signal["low"], equal_nan=False) & signal["pct_chg"].lt(0)
    )
    signal["ticker"] = signal["ts_code"].astype("string")
    signal["date"] = signal["trade_date"].dt.strftime("%Y-%m-%d")
    signal["open_adj"] = signal["open_hfq"]
    signal["close_adj"] = signal["close_hfq"]
    signal["basic_source"] = basic_source
    columns = [
        "ticker",
        "date",
        "membership_month",
        "universe_member",
        "eligible",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "pct_chg",
        "amount",
        "amount_rmb",
        "open_hfq",
        "high_hfq",
        "low_hfq",
        "close_hfq",
        "open_adj",
        "close_adj",
        "adj_factor",
        "effective_adj_multiplier",
        "adj_calibration_multiplier",
        "adj_source",
        "price_source",
        "basic_source",
        "pe_ttm",
        "pb",
        "earnings_yield",
        "book_yield",
        "return_1d",
        "volatility_20",
        "adv_20",
        "is_one_price_limit_up",
        "is_one_price_limit_down",
    ]
    result = signal[columns].sort_values("ticker", kind="mergesort").reset_index(drop=True)
    missing_members = sorted(member_set - set(result["ticker"].astype(str)))
    audit = {
        "membership_count": EXPECTED_MEMBERSHIP_SIZE,
        "signal_row_count": int(len(result)),
        "eligible_row_count": int(result["eligible"].sum()),
        "missing_signal_tickers": missing_members,
        "signal_coverage_ratio": round(len(result) / EXPECTED_MEMBERSHIP_SIZE, 8),
        "volatility_lookback": 20,
        "volatility_ddof": 1,
        "amount_unit": "RMB",
    }
    return result, audit


def build_prospective_input_snapshot(
    project_root: str | Path,
    signal_date: str | pd.Timestamp,
    *,
    available_at_utc: str | pd.Timestamp | None = None,
    membership_snapshot_path: str | Path | None = None,
    _materialize: bool = True,
    _sealed_manifest: Mapping[str, Any] | None = None,
) -> ProspectiveInputSnapshot:
    """Build and persist one deterministic, causally truncated signal snapshot.

    ``available_at_utc`` is an optional upper bound, not the trading deadline.
    The caller/ledger must separately prove that the resulting maximum input
    availability and the non-authoritative build receipt precede its deadline.
    """

    root = Path(project_root).expanduser().resolve()
    date = _normalise_date(signal_date, label="signal_date")
    cap = (
        _utc_timestamp(available_at_utc, label="available_at_utc")
        if available_at_utc is not None
        else None
    )
    sealed_inputs: list[Mapping[str, Any]] = []
    if _sealed_manifest is not None:
        raw_inputs = _sealed_manifest.get("inputs")
        if not isinstance(raw_inputs, list) or not all(
            isinstance(row, Mapping) for row in raw_inputs
        ):
            raise ProspectiveDataError("sealed signal manifest inputs are invalid")
        sealed_inputs = list(raw_inputs)

    def one_sealed(role: str) -> Mapping[str, Any] | None:
        rows = [row for row in sealed_inputs if row.get("role") == role]
        if _sealed_manifest is None:
            return None
        if len(rows) != 1:
            raise ProspectiveDataError(f"sealed signal manifest needs one {role}")
        return rows[0]

    features_origin = root / "runtime/data/top500/features.parquet"
    sealed_features = one_sealed("canonical_features")
    if sealed_features is None:
        features_path, immutable_features = _capture_immutable_artifact(
            root, features_origin
        )
        features_source: dict[str, Any] = {
            "role": "canonical_features",
            "path": _relative(features_origin, root),
            **immutable_features,
            "availability_basis": "pre_activation_frozen_canonical",
        }
    else:
        features_source = dict(sealed_features)
        if features_source.get("path") != _relative(features_origin, root):
            raise ProspectiveDataError("sealed canonical features origin path differs")
        features_path = _resolve_immutable_artifact(root, features_source)
    feature_dates = pd.read_parquet(features_path, columns=["date"])["date"]
    cutoff = pd.to_datetime(feature_dates, errors="coerce").max().normalize()
    if pd.isna(cutoff) or cutoff >= FROZEN_BRIDGE_END:
        raise ProspectiveDataError("canonical feature cutoff must precede the frozen bridge end")
    if date <= cutoff:
        raise ProspectiveDataError("prospective snapshots require a date after canonical cutoff")

    canonical_sessions, execution_source = _canonical_execution_calendar(
        root, sealed_source=one_sealed("canonical_execution_calendar")
    )
    raw_checkpoint_path = root / "runtime/data/raw/checkpoint.json"
    raw_checkpoint = (
        _load_json(raw_checkpoint_path, label="raw data checkpoint")
        if _sealed_manifest is None
        else {}
    )
    if _sealed_manifest is None:
        _require_schema_version(raw_checkpoint, label="raw data checkpoint")
    official_start = (
        cutoff
        if date <= FROZEN_BRIDGE_END
        else FROZEN_BRIDGE_END + pd.Timedelta(days=1)
    )
    open_dates, next_trade, calendar_manifest, calendar_availability = _official_calendar(
        root,
        start_date=official_start,
        signal_date=date,
        checkpoint=raw_checkpoint,
        availability_cap=cap,
        sealed_sources=(
            _sealed_manifest.get("calendar", {}).get("sources")
            if _sealed_manifest is not None
            else None
        ),
    )
    members, membership_source, membership_availability = _membership_source(
        root,
        date,
        next_trade,
        membership_snapshot_path=membership_snapshot_path,
        availability_cap=cap,
        sealed_source=one_sealed("membership"),
    )

    canonical_columns = [
        "ticker",
        "date",
        *_PRICE_COLUMNS,
        "amount_akshare",
        "pe_ttm",
        "pb",
        "earnings_yield",
        "book_yield",
        "volatility_20",
    ]
    canonical = pd.read_parquet(
        features_path,
        columns=canonical_columns,
        filters=[("date", "=", cutoff)],
    )
    if canonical.empty or canonical["ticker"].astype("string").duplicated().any():
        raise ProspectiveDataError("canonical boundary is empty or duplicated")
    canonical_tickers = set(canonical["ticker"].astype("string"))
    current_tickers = set(members["ts_code"].astype("string"))
    supplement_tickers = sorted(canonical_tickers | current_tickers)
    lookback_start = cutoff - pd.Timedelta(days=90)
    selected_bridge_end = min(date, FROZEN_BRIDGE_END)
    bridge_price, bridge_basic, supplement_sources = _supplement_files(
        root,
        supplement_tickers,
        minimum_date=lookback_start,
        maximum_date=selected_bridge_end,
        sealed_sources=(sealed_inputs if _sealed_manifest is not None else None),
    )
    calibration = _calibrate_canonical_boundary(
        canonical,
        bridge_price,
        bridge_basic,
        cutoff=cutoff,
    )

    bridge_daily_dates = [
        value
        for value in canonical_sessions
        if cutoff <= value <= min(date, FROZEN_BRIDGE_END)
    ]
    official_input_dates = [value for value in open_dates if value <= date]
    required_open_dates = sorted(set(bridge_daily_dates + official_input_dates))
    future_open_dates = [value for value in required_open_dates if value > FROZEN_BRIDGE_END]
    raw_sources: list[dict[str, Any]] = []
    raw_availability: list[pd.Timestamp] = []
    daily_frames: list[pd.DataFrame] = []
    basic_frames: list[pd.DataFrame] = []
    adj_frames: list[pd.DataFrame] = []
    sealed_raw_by_key = {
        (str(row.get("dataset")), str(row.get("trade_date"))): row
        for row in sealed_inputs
        if row.get("role") == "raw_partition"
    }
    for open_date in required_open_dates:
        daily, source, completed = _checkpoint_partition(
            root,
            raw_checkpoint,
            dataset="daily",
            trade_date=open_date,
            availability_cap=cap,
            sealed_source=sealed_raw_by_key.get(
                ("daily", open_date.date().isoformat())
            ) if _sealed_manifest is not None else None,
        )
        daily_frames.append(daily)
        raw_sources.append(source)
        raw_availability.append(completed)
        if open_date <= FROZEN_BRIDGE_END:
            continue
        for dataset, collection in (("daily_basic", basic_frames), ("adj_factor", adj_frames)):
            frame, source, completed = _checkpoint_partition(
                root,
                raw_checkpoint,
                dataset=dataset,
                trade_date=open_date,
                availability_cap=cap,
                sealed_source=sealed_raw_by_key.get(
                    (dataset, open_date.date().isoformat())
                ) if _sealed_manifest is not None else None,
            )
            collection.append(frame)
            raw_sources.append(source)
            raw_availability.append(completed)
    if date > FROZEN_BRIDGE_END and not future_open_dates:
        raise ProspectiveDataError("calendar has no future raw interval for signal date")
    raw_daily = pd.concat(daily_frames, ignore_index=True)
    raw_basic = (
        pd.concat(basic_frames, ignore_index=True)
        if basic_frames
        else bridge_basic.iloc[0:0].copy()
    )
    raw_adj = (
        pd.concat(adj_frames, ignore_index=True)
        if adj_frames
        else pd.DataFrame(columns=["ts_code", "trade_date", "adj_factor"])
    )
    frame, signal_audit = _build_signal_frame(
        members,
        bridge_price,
        bridge_basic,
        raw_daily,
        raw_basic,
        raw_adj,
        signal_date=date,
    )

    availability_values = membership_availability + calendar_availability + raw_availability
    if not availability_values:
        raise ProspectiveDataError("snapshot has no checkpointed availability evidence")
    inputs_available = max(availability_values)
    if cap is not None and inputs_available > cap:
        raise ProspectiveDataError("snapshot inputs exceed requested availability cutoff")
    rows = _frame_records(frame)
    rows_bytes = _canonical_json_bytes(rows)
    rows_sha = _sha256_bytes(rows_bytes)
    float_columns = [
        column
        for column in frame.columns
        if pd.api.types.is_float_dtype(frame[column].dtype)
    ]
    target_columns = [
        "date",
        "ticker",
        "eligible",
        "universe_member",
        "earnings_yield",
        "pb",
        "book_yield",
        "volatility_20",
    ]
    target_rows = _frame_records(frame[target_columns])
    target_rows_sha = _sha256_bytes(_canonical_json_bytes(target_rows))
    features_source["selected_max_date"] = cutoff.date().isoformat()
    if sealed_features is not None and features_source != dict(sealed_features):
        raise ProspectiveDataError("sealed canonical features contract differs from CAS bytes")
    input_sources = [
        features_source,
        execution_source,
        membership_source,
        *supplement_sources,
        *sorted(raw_sources, key=lambda row: (row["trade_date"], row["dataset"])),
    ]
    if date > FROZEN_BRIDGE_END:
        extension = [value for value in open_dates if value > FROZEN_BRIDGE_END]
        complete_calendar_sessions = [*canonical_sessions, *extension]
        if complete_calendar_sessions[-1] != next_trade:
            raise ProspectiveDataError("official calendar extension does not end at next trade")
    else:
        complete_calendar_sessions = [
            value for value in canonical_sessions if value <= next_trade
        ]
    complete_calendar_text = [value.date().isoformat() for value in complete_calendar_sessions]
    input_sources_sha = _sha256_bytes(_canonical_json_bytes(input_sources))
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "protocol_release": "5.0",
        "kind": "prospective_signal_input_snapshot",
        "signal_date": date.date().isoformat(),
        "official_trade_date": next_trade.date().isoformat(),
        "canonical_cutoff": cutoff.date().isoformat(),
        "frozen_bridge_end": FROZEN_BRIDGE_END.date().isoformat(),
        "selected_data_max_date": date.date().isoformat(),
        "inputs_available_at_utc": _utc_text(inputs_available),
        "time_semantics": {
            "signal_date_basis": "official_SSE_trade_cal_open_day",
            "raw_and_calendar_availability": "checkpoint_completed_at_utc",
            "supplement_availability": "pre_activation_frozen_bridge",
            "membership_availability": membership_source["availability_basis"],
            "build_completed_at": "non_authoritative_build_receipt_not_part_of_snapshot_hash",
            "deadline_enforcement": "caller_or_prospective_ledger",
            "note": (
                "inputs_available_at_utc may be later than signal close; this module proves "
                "availability but does not prove the pre-trade deadline"
            ),
        },
        "calendar": calendar_manifest,
        "canonical_bridge_calibration": calibration,
        "signal_audit": signal_audit,
        "rows": {
            "path": "rows.json",
            "sha256": rows_sha,
            "row_count": int(len(frame)),
            "columns": list(frame.columns),
            "float_columns": float_columns,
            "float_encoding": "python_float_hex_ieee754_binary64",
            "sort": ["ticker"],
        },
        "target_adapter": {
            "columns": target_columns,
            "target_rows_sha256": target_rows_sha,
            "input_sources_sha256": input_sources_sha,
            "membership_artifact_sha256": membership_source["sha256"],
            "calendar_sessions": complete_calendar_text,
            "calendar_next_trade_date": next_trade.date().isoformat(),
            "calendar_sessions_sha256": _calendar_prefix_sha256(
                complete_calendar_text
            ),
            "canonical_calendar_prefix_sha256": execution_source[
                "calendar_prefix_sha256"
            ],
            "rows_selection": "rows.json projected to columns in this exact order",
        },
        "inputs": input_sources,
        "input_sources_sha256": input_sources_sha,
    }
    manifest_bytes = _canonical_json_bytes(manifest)
    snapshot_sha = _sha256_bytes(manifest_bytes)
    directory = root / PROSPECTIVE_RELATIVE_ROOT / "inputs" / snapshot_sha
    rows_path = directory / "rows.json"
    manifest_path = directory / "manifest.json"
    receipt_path = directory / "build-receipt.json"
    if not _materialize:
        # The public verifier uses this pure branch when independently
        # rebuilding provenance.  A source mutation must be reported without
        # first creating a second, newly content-addressed bundle as a side
        # effect of an audit/read operation.
        return ProspectiveInputSnapshot(
            signal_date=date.date().isoformat(),
            trade_date=next_trade.date().isoformat(),
            snapshot_sha256=snapshot_sha,
            directory=directory,
            manifest_path=manifest_path,
            rows_path=rows_path,
            build_receipt_path=receipt_path,
            build_completed_at_utc=_utc_text(inputs_available),
            inputs_available_at_utc=_utc_text(inputs_available),
            frame=frame,
            manifest=manifest,
        )
    _write_verified(rows_path, rows_bytes)
    _write_verified(manifest_path, manifest_bytes)
    if not receipt_path.is_file():
        built_at = pd.Timestamp(datetime.now(timezone.utc))
        receipt = {
            "schema_version": 1,
            "snapshot_sha256": snapshot_sha,
            "build_completed_at_utc": _utc_text(built_at),
            "authoritative_for_snapshot_hash": False,
        }
        _write_verified(receipt_path, _canonical_json_bytes(receipt))
    # Return only through the same strict loader used by later decisions.  In
    # particular, an already-existing receipt must satisfy the full canonical
    # schema rather than receiving a weaker fast-path check during rebuild.
    return _load_prospective_input_snapshot_files(directory)


def _load_prospective_input_snapshot_files(path: str | Path) -> ProspectiveInputSnapshot:
    """Verify the bundle bytes themselves without trusting external sources."""

    directory = Path(path).expanduser().resolve()
    manifest_path = directory / "manifest.json"
    rows_path = directory / "rows.json"
    receipt_path = directory / "build-receipt.json"
    manifest_bytes = manifest_path.read_bytes() if manifest_path.is_file() else b""
    snapshot_sha = _sha256_bytes(manifest_bytes)
    if not _SHA256.fullmatch(directory.name) or directory.name != snapshot_sha:
        raise ProspectiveDataError("snapshot directory does not match manifest SHA-256")
    manifest = _load_json(manifest_path, label="snapshot manifest")
    if manifest_bytes != _canonical_json_bytes(manifest):
        raise ProspectiveDataError("snapshot manifest is not canonical JSON")
    if (
        type(manifest.get("schema_version")) is not int
        or manifest.get("schema_version") != SCHEMA_VERSION
        or manifest.get("kind") != "prospective_signal_input_snapshot"
    ):
        raise ProspectiveDataError("snapshot manifest schema/kind mismatch")
    rows_bytes = rows_path.read_bytes() if rows_path.is_file() else b""
    if _sha256_bytes(rows_bytes) != str((manifest.get("rows") or {}).get("sha256") or ""):
        raise ProspectiveDataError("snapshot rows hash mismatch")
    rows = json.loads(rows_bytes.decode("utf-8"))
    if rows_bytes != _canonical_json_bytes(rows) or not isinstance(rows, list):
        raise ProspectiveDataError("snapshot rows are not canonical JSON records")
    rows_contract = manifest.get("rows") or {}
    columns = rows_contract.get("columns")
    if (
        not isinstance(columns, list)
        or len(columns) != len(set(columns))
        or any(not isinstance(column, str) for column in columns)
        or int(rows_contract.get("row_count") or -1) != len(rows)
        or any(not isinstance(row, dict) or set(row) != set(columns) for row in rows)
    ):
        raise ProspectiveDataError("snapshot rows schema/count does not match manifest")
    tickers = [str(row.get("ticker") or "") for row in rows]
    if tickers != sorted(tickers) or len(tickers) != len(set(tickers)):
        raise ProspectiveDataError("snapshot rows are not uniquely sorted by ticker")
    adapter = manifest.get("target_adapter") or {}
    target_columns = adapter.get("columns")
    if not isinstance(target_columns, list) or not set(target_columns).issubset(columns):
        raise ProspectiveDataError("snapshot target adapter columns are invalid")
    encoded_target_rows = [
        {column: row[column] for column in target_columns} for row in rows
    ]
    if _sha256_bytes(_canonical_json_bytes(encoded_target_rows)) != str(
        adapter.get("target_rows_sha256") or ""
    ):
        raise ProspectiveDataError("snapshot target rows binding mismatch")
    inputs = manifest.get("inputs")
    if not isinstance(inputs, list) or _sha256_bytes(_canonical_json_bytes(inputs)) != str(
        manifest.get("input_sources_sha256") or ""
    ):
        raise ProspectiveDataError("snapshot input sources binding mismatch")
    if adapter.get("input_sources_sha256") != manifest.get("input_sources_sha256"):
        raise ProspectiveDataError("snapshot target adapter input binding mismatch")
    membership_sources = [
        source
        for source in inputs
        if isinstance(source, Mapping) and source.get("role") == "membership"
    ]
    if (
        len(membership_sources) != 1
        or adapter.get("membership_artifact_sha256")
        != membership_sources[0].get("sha256")
    ):
        raise ProspectiveDataError("snapshot target adapter membership binding mismatch")
    sessions = adapter.get("calendar_sessions")
    if not isinstance(sessions, list) or _calendar_prefix_sha256(sessions) != str(
        adapter.get("calendar_sessions_sha256") or ""
    ):
        raise ProspectiveDataError("snapshot complete calendar binding mismatch")
    float_columns = set((manifest.get("rows") or {}).get("float_columns") or ())
    for row in rows:
        for column in float_columns:
            value = row.get(column)
            if isinstance(value, str) and value.casefold().startswith(("0x", "-0x")):
                row[column] = float.fromhex(value)
    frame = pd.DataFrame(rows, columns=columns)
    receipt = _load_json(receipt_path, label="snapshot build receipt")
    if set(receipt) != {
        "schema_version",
        "snapshot_sha256",
        "build_completed_at_utc",
        "authoritative_for_snapshot_hash",
    }:
        raise ProspectiveDataError("snapshot build receipt does not have the exact schema")
    if (
        type(receipt.get("schema_version")) is not int
        or receipt.get("schema_version") != 1
        or str(receipt.get("snapshot_sha256") or "") != snapshot_sha
        or receipt.get("authoritative_for_snapshot_hash") is not False
        or receipt_path.read_bytes() != _canonical_json_bytes(receipt)
    ):
        raise ProspectiveDataError("snapshot build receipt binding mismatch")
    built = _utc_timestamp(receipt.get("build_completed_at_utc"), label="build completed")
    return ProspectiveInputSnapshot(
        signal_date=str(manifest["signal_date"]),
        trade_date=str(manifest["official_trade_date"]),
        snapshot_sha256=snapshot_sha,
        directory=directory,
        manifest_path=manifest_path,
        rows_path=rows_path,
        build_receipt_path=receipt_path,
        build_completed_at_utc=_utc_text(built),
        inputs_available_at_utc=str(manifest["inputs_available_at_utc"]),
        frame=frame,
        manifest=manifest,
    )


def load_prospective_input_snapshot(path: str | Path) -> ProspectiveInputSnapshot:
    """Purely read and rebuild a bundle from its sealed immutable source CAS."""

    loaded = _load_prospective_input_snapshot_files(path)
    directory = loaded.directory
    try:
        project_root = directory.parents[4]
    except IndexError as exc:
        raise ProspectiveDataError("snapshot is outside the canonical project layout") from exc
    expected = (
        project_root / PROSPECTIVE_RELATIVE_ROOT / "inputs" / loaded.snapshot_sha256
    ).resolve()
    if directory != expected:
        raise ProspectiveDataError("snapshot is outside the canonical project input store")

    rebuilt = build_prospective_input_snapshot(
        project_root,
        loaded.signal_date,
        available_at_utc=loaded.inputs_available_at_utc,
        _materialize=False,
        _sealed_manifest=loaded.manifest,
    )
    if rebuilt.snapshot_sha256 != loaded.snapshot_sha256:
        raise ProspectiveDataError(
            "snapshot does not match an independent point-in-time source rebuild"
        )
    return loaded


__all__ = [
    "EXPECTED_MEMBERSHIP_SIZE",
    "FROZEN_BRIDGE_END",
    "ProspectiveDataError",
    "ProspectiveInputSnapshot",
    "build_prospective_input_snapshot",
    "load_prospective_input_snapshot",
]

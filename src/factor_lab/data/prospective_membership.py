"""Causal, content-addressed monthly Top-500 membership snapshots.

This module defines the forward-only 5.2 membership rule.  It deliberately
does not claim to reproduce the retired historical membership builder.  For a
membership month it ranks securities using only the 60 official sessions
ending on the last open day before the month starts.  Selection is purely a
liquidity operation; point-in-time reference checks affect ``eligible`` but
never remove or replace one of the 500 selected securities.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
import re
import tempfile
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from .catalog import DEFAULT_CONFIG_PATH, sha256_file
from .prospective import (
    IMMUTABLE_SOURCE_RELATIVE_ROOT,
    ProspectiveDataError,
    _bundle_lock,
    _capture_immutable_artifact,
    _fsync_directory,
    _now_utc,
    _publication_upper_bound,
    _resolve_immutable_artifact,
    _verify_publication_upper_bound,
    _wait_through_publication_upper_bound,
    _write_verified,
)
from .sources import (
    ENRICHMENT_DATASET_FIELDS,
    EXACT_REFERENCE_CONTRACT_ID,
    _normalise_trade_calendar,
    turnover_amount_to_rmb,
)


SCHEMA_VERSION = 1
RULE_ID = "prospective_top500_membership_5_2"
RULE_EFFECTIVE_MONTH = "2026-09"
EXPECTED_MEMBERSHIP_SIZE = 500
LIQUIDITY_SESSION_COUNT = 60
MINIMUM_LIQUIDITY_OBSERVATIONS = 20
MINIMUM_REFERENCE_COVERAGE = 0.99
MINIMUM_PROVIDER_REFERENCE_COVERAGE_PPM = 1_000_000
PROSPECTIVE_MEMBERSHIP_ROOT = Path("runtime/prospective/5.0/membership")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_MONTH = re.compile(r"\d{4}-(?:0[1-9]|1[0-2])")


class ProspectiveMembershipError(ValueError):
    """Raised when a monthly membership cannot be proven causal and complete."""


@dataclass(frozen=True)
class ProspectiveMembershipSnapshot:
    membership_month: str
    as_of_date: str
    artifact_sha256: str
    directory: Path
    membership_path: Path
    manifest_path: Path
    source_contract_path: Path
    reference_raw_path: Path
    completed_at_utc: str
    frame: pd.DataFrame
    manifest: Mapping[str, Any]


@dataclass(frozen=True)
class _CalendarSelection:
    as_of_date: pd.Timestamp
    effective_start_date: pd.Timestamp
    effective_end_date: pd.Timestamp
    liquidity_sessions: tuple[pd.Timestamp, ...]
    sources: tuple[Mapping[str, Any], ...]


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


def _utc(value: Any, *, label: str) -> pd.Timestamp:
    try:
        result = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise ProspectiveMembershipError(f"invalid {label}: {value!r}") from exc
    if pd.isna(result) or result.tzinfo is None:
        raise ProspectiveMembershipError(f"{label} must include a timezone")
    return result.tz_convert("UTC")


def _utc_text(value: pd.Timestamp) -> str:
    resolved = value.tz_convert("UTC")
    if resolved != resolved.floor("s"):
        resolved = resolved.ceil("s")
    return resolved.strftime("%Y-%m-%dT%H:%M:%SZ")


def _capture_json_bytes(
    root: Path,
    payload: bytes,
    *,
    sha_field: str,
    path_field: str,
    size_field: str,
    media_field: str,
) -> dict[str, Any]:
    """Publish already-captured JSON bytes into the shared immutable CAS."""

    digest = _sha256_bytes(payload)
    relative = IMMUTABLE_SOURCE_RELATIVE_ROOT / f"sha256={digest}" / "artifact"
    _write_verified(root / relative, payload)
    return {
        sha_field: digest,
        path_field: relative.as_posix(),
        size_field: len(payload),
        media_field: "application/json",
    }


def _month(value: Any) -> pd.Period:
    text = str(value or "")
    if not _MONTH.fullmatch(text):
        raise ProspectiveMembershipError("membership_month must use YYYY-MM")
    result = pd.Period(text, freq="M")
    if text < RULE_EFFECTIVE_MONTH:
        raise ProspectiveMembershipError(
            f"{RULE_ID} is forward-only from {RULE_EFFECTIVE_MONTH}"
        )
    return result


def _require_sha(value: Any, *, label: str) -> str:
    result = str(value or "")
    if not _SHA256.fullmatch(result):
        raise ProspectiveMembershipError(f"{label} must be a lowercase SHA-256")
    return result


def _relative(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError as exc:
        raise ProspectiveMembershipError(
            f"source escapes project root: {path}"
        ) from exc


def _under(path: Path, root: Path, *, label: str) -> Path:
    result = path.expanduser().resolve()
    try:
        result.relative_to(root.resolve())
    except ValueError as exc:
        raise ProspectiveMembershipError(f"{label} escapes its canonical root") from exc
    return result


def _load_json(path: Path, *, label: str) -> dict[str, Any]:
    if not path.is_file():
        raise ProspectiveMembershipError(f"missing {label}: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProspectiveMembershipError(f"unreadable {label}: {path}") from exc
    if not isinstance(value, dict):
        raise ProspectiveMembershipError(f"{label} must be a JSON object")
    return value


def _require_schema_version(
    value: Mapping[str, Any], *, label: str, expected: int = 1
) -> None:
    version = value.get("schema_version")
    if type(version) is not int or version != expected:
        raise ProspectiveMembershipError(f"unsupported {label} schema")


def _json_value(value: Any) -> Any:
    if value is None or value is pd.NA:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
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


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return [
        {str(column): _json_value(value) for column, value in row.items()}
        for row in frame.to_dict(orient="records")
    ]


def _reference_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    """Preserve a vendor response as ordinary canonical JSON values.

    Output-frame digests use hexadecimal binary64 strings, but raw reference
    columns may themselves legitimately be numeric (including ``list_date``).
    Keeping numbers as numbers makes the captured response directly reusable.
    """

    def convert(value: Any) -> Any:
        if value is None or value is pd.NA:
            return None
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
        if isinstance(value, (bool, np.bool_)):
            return bool(value)
        if isinstance(value, (int, np.integer)):
            return int(value)
        if isinstance(value, (float, np.floating)):
            number = float(value)
            return number if math.isfinite(number) else None
        if isinstance(value, (pd.Timestamp, datetime)):
            return pd.Timestamp(value).isoformat()
        return str(value)

    return [
        {str(column): convert(value) for column, value in row.items()}
        for row in frame.to_dict(orient="records")
    ]


def _compact_date_text(values: pd.Series) -> pd.Series:
    return (
        values.astype("string")
        .str.strip()
        .str.replace("-", "", regex=False)
        .str.replace(r"\.0$", "", regex=True)
    )


def _source_contract() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "rule_id": RULE_ID,
        "effective_from_membership_month": RULE_EFFECTIVE_MONTH,
        "historical_equivalence_claimed": False,
        "membership_size": EXPECTED_MEMBERSHIP_SIZE,
        "calendar_rule": (
            "as_of is the last official open session strictly before the first "
            "calendar day of membership_month"
        ),
        "liquidity_rule": {
            "source": "checkpointed_tushare_daily.amount",
            "source_unit": "thousand_RMB",
            "target_unit": "RMB",
            "official_session_count": LIQUIDITY_SESSION_COUNT,
            "statistic": "median_positive_amount_RMB",
            "minimum_positive_observations": MINIMUM_LIQUIDITY_OBSERVATIONS,
            "ranking": ["median_amount_60d descending", "ts_code ascending"],
        },
        "selection_and_eligibility_are_separate": True,
        "source_replay": {
            "store": "runtime/prospective/5.0/source-artifacts/sha256=<sha256>/artifact",
            "capture_before_parse": True,
            "public_loader_reads_live_checkpoint_or_origin": False,
            "availability_timestamp_rounding": "ceil_to_next_whole_second",
        },
        "eligibility_rule": {
            "reference": "stable_exact_as_of_checkpointed_tushare_bak_basic",
            "minimum_selected_reference_coverage": MINIMUM_REFERENCE_COVERAGE,
            "provider_universe_basis": "exact_as_of_official_daily_tickers",
            "minimum_provider_universe_coverage_ppm": (
                MINIMUM_PROVIDER_REFERENCE_COVERAGE_PPM
            ),
            "fail_if": [
                "reference_missing",
                "name_has_A_share_ST_prefix",
                "list_date_missing_or_after_as_of",
                "delist_date_on_or_before_as_of_when_present",
                "list_status_is_delisted_when_present",
            ],
            "eligibility_never_changes_membership_ranking": True,
        },
        "create_only": True,
        "artifact_address": "sha256(membership.parquet bytes)",
    }


def _checkpoint_entry_digest(entry: Mapping[str, Any]) -> str:
    selected = {
        key: entry.get(key)
        for key in (
            "status",
            "dataset",
            "trade_date",
            "request_trade_date",
            "source_trade_date",
            "capture_contract_id",
            "capture_mode",
            "fallback_used",
            "fields",
            "exact_source_required",
            "stability_sample_count",
            "daily_partition_sha256",
            "daily_ticker_count",
            "covered_ticker_count",
            "reference_ticker_count",
            "path",
            "row_count",
            "size_bytes",
            "sha256",
            "completed_at_utc",
            "exchange",
            "start_date",
            "end_date",
            "open_day_count",
            "artifact_sha256",
            "calendar_content_sha256",
            "manifest_path",
            "manifest_sha256",
        )
        if key in entry
    }
    return _sha256_bytes(_canonical_json_bytes(selected))


def _normalise_calendar(frame: pd.DataFrame, *, source_label: str) -> pd.DataFrame:
    required = {"cal_date", "is_open"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ProspectiveMembershipError(
            f"{source_label} calendar missing columns: {missing}"
        )
    work = frame.copy()
    work["cal_date"] = pd.to_datetime(
        work["cal_date"].astype("string").str.replace("-", "", regex=False),
        format="%Y%m%d",
        errors="coerce",
    )
    if work.empty or work["cal_date"].isna().any():
        raise ProspectiveMembershipError(f"{source_label} calendar has invalid dates")
    if bool(work.duplicated("cal_date").any()):
        raise ProspectiveMembershipError(f"{source_label} calendar has duplicate dates")
    numeric = pd.to_numeric(work["is_open"], errors="coerce")
    textual = work["is_open"].astype("string").str.strip().str.casefold()
    valid = numeric.isin([0, 1]) | textual.isin(["false", "true"])
    if not bool(valid.all()):
        raise ProspectiveMembershipError(
            f"{source_label} calendar has invalid open flags"
        )
    work["is_open"] = numeric.eq(1) | textual.eq("true")
    work = work[["cal_date", "is_open"]].sort_values("cal_date", kind="mergesort")
    expected = pd.date_range(
        work["cal_date"].iloc[0], work["cal_date"].iloc[-1], freq="D"
    )
    if not pd.DatetimeIndex(work["cal_date"]).equals(expected):
        raise ProspectiveMembershipError(
            f"{source_label} calendar does not cover every calendar day"
        )
    return work.reset_index(drop=True)


def _verified_calendar_candidates(
    root: Path,
    *,
    available_at: pd.Timestamp,
) -> list[tuple[pd.DataFrame, dict[str, Any]]]:
    checkpoint_path = root / "runtime/data/raw/checkpoint.json"
    checkpoint = _load_json(checkpoint_path, label="market-data checkpoint")
    _require_schema_version(checkpoint, label="market-data checkpoint")
    calendars = checkpoint.get("calendars")
    if not isinstance(calendars, Mapping) or not calendars:
        raise ProspectiveMembershipError(
            "checkpoint has no content-addressed official trade calendar; run data sync "
            "with calendar coverage through the membership month end"
        )
    result: list[tuple[pd.DataFrame, dict[str, Any]]] = []
    calendar_root = root / "runtime/data/raw/trade_cal"
    for content_sha, raw_entry in calendars.items():
        if not isinstance(raw_entry, Mapping) or raw_entry.get("status") != "complete":
            continue
        if not _SHA256.fullmatch(str(content_sha)):
            continue
        entry = dict(raw_entry)
        try:
            completed = _utc(
                entry.get("completed_at_utc"), label="calendar completed_at_utc"
            )
        except ProspectiveMembershipError:
            continue
        if completed > available_at:
            continue
        artifact_sha = _require_sha(
            entry.get("artifact_sha256"), label="calendar artifact SHA"
        )
        canonical_path = (
            calendar_root / f"calendar_sha256={content_sha}" / "part-000.parquet"
        ).resolve()
        path = _under(
            Path(str(entry.get("path") or canonical_path)),
            calendar_root,
            label="calendar",
        )
        if path != canonical_path or not path.is_file():
            continue
        manifest_path = path.parent / "manifest.json"
        if not manifest_path.is_file():
            continue
        try:
            immutable_path, immutable = _capture_immutable_artifact(
                root,
                path,
                expected_sha256=artifact_sha,
            )
            immutable_manifest_path, immutable_manifest = _capture_immutable_artifact(
                root,
                manifest_path,
                expected_sha256=_require_sha(
                    entry.get("manifest_sha256"), label="calendar manifest SHA"
                ),
                sha_field="manifest_sha256",
                path_field="immutable_manifest_path",
                size_field="manifest_size_bytes",
                media_field="manifest_media_type",
            )
        except Exception:
            continue
        immutable_manifest_value = _load_json(
            immutable_manifest_path, label="immutable calendar manifest"
        )
        _require_schema_version(
            immutable_manifest_value, label="immutable calendar manifest"
        )
        if str(immutable_manifest_value.get("calendar_content_sha256") or "") != str(
            content_sha
        ):
            continue
        raw_calendar = pd.read_parquet(immutable_path)
        exchange = str(entry.get("exchange") or "SSE")
        try:
            _, _, rebuilt_content_sha = _normalise_trade_calendar(
                raw_calendar,
                exchange=exchange,
                start_date=str(entry.get("start_date")),
                end_date=str(entry.get("end_date")),
            )
        except ValueError:
            continue
        if rebuilt_content_sha != str(content_sha) or rebuilt_content_sha != str(
            entry.get("calendar_content_sha256")
        ):
            continue
        calendar = _normalise_calendar(raw_calendar, source_label="official")
        if len(calendar) != int(entry.get("row_count") or -1):
            continue
        if calendar["cal_date"].iloc[0].date().isoformat() != str(
            entry.get("start_date")
        ):
            continue
        if calendar["cal_date"].iloc[-1].date().isoformat() != str(
            entry.get("end_date")
        ):
            continue
        source = {
            "role": "official_trade_calendar",
            "path": _relative(path, root),
            **immutable,
            "manifest_path": _relative(manifest_path, root),
            **immutable_manifest,
            "calendar_content_sha256": _require_sha(
                entry.get("calendar_content_sha256"),
                label="calendar content SHA",
            ),
            "start_date": str(entry.get("start_date")),
            "end_date": str(entry.get("end_date")),
            "exchange": exchange,
            "completed_at_utc": _utc_text(completed),
            "checkpoint_key": str(content_sha),
            "checkpoint_entry_sha256": _checkpoint_entry_digest(entry),
            "availability_basis": "checkpoint.completed_at_utc",
            "calendar_is_non_market_schedule": True,
        }
        result.append((calendar, source))
    if not result:
        raise ProspectiveMembershipError(
            "no verified official calendar was available by cutoff"
        )
    return result


def _frozen_execution_sessions(
    root: Path,
) -> tuple[list[pd.Timestamp], dict[str, Any]] | None:
    path = root / "runtime/data/top500/execution.parquet"
    if not path.is_file():
        return None
    try:
        immutable_path, immutable = _capture_immutable_artifact(root, path)
        values = pd.to_datetime(
            pd.read_parquet(immutable_path, columns=["date"])["date"], errors="coerce"
        )
    except Exception as exc:
        raise ProspectiveMembershipError(
            "unreadable frozen execution session prefix"
        ) from exc
    if values.empty or values.isna().any():
        raise ProspectiveMembershipError(
            "frozen execution session prefix has invalid dates"
        )
    sessions = [pd.Timestamp(value) for value in sorted(values.dt.normalize().unique())]
    return sessions, {
        "role": "frozen_execution_session_prefix",
        "path": _relative(path, root),
        **immutable,
        "first_session": sessions[0].date().isoformat(),
        "last_session": sessions[-1].date().isoformat(),
        "availability_basis": "pre_activation_frozen_canonical",
    }


def _calendar_selection(
    root: Path,
    period: pd.Period,
    *,
    available_at: pd.Timestamp,
) -> _CalendarSelection:
    month_first = period.start_time.normalize()
    month_end = period.end_time.normalize()
    frozen = _frozen_execution_sessions(root)
    candidates = _verified_calendar_candidates(root, available_at=available_at)
    qualified: list[tuple[pd.Timestamp, str, _CalendarSelection]] = []
    for calendar, calendar_source in candidates:
        start = pd.Timestamp(calendar["cal_date"].iloc[0])
        end = pd.Timestamp(calendar["cal_date"].iloc[-1])
        if start > month_first or end < month_end:
            continue
        month_opens = calendar.loc[
            calendar["is_open"] & calendar["cal_date"].between(month_first, month_end),
            "cal_date",
        ].tolist()
        prior_opens = calendar.loc[
            calendar["is_open"] & calendar["cal_date"].lt(month_first), "cal_date"
        ].tolist()
        sources: list[Mapping[str, Any]] = [calendar_source]
        sessions = [pd.Timestamp(value) for value in prior_opens]
        if len(sessions) < LIQUIDITY_SESSION_COUNT:
            if frozen is None:
                continue
            frozen_sessions, frozen_source = frozen
            frozen_last = frozen_sessions[-1]
            if start > frozen_last + pd.Timedelta(days=1):
                continue
            sessions = sorted(
                {
                    *[value for value in frozen_sessions if value < month_first],
                    *sessions,
                }
            )
            sources.append(frozen_source)
        if len(sessions) < LIQUIDITY_SESSION_COUNT or not month_opens:
            continue
        window = tuple(sessions[-LIQUIDITY_SESSION_COUNT:])
        as_of = window[-1]
        # The complete daily calendar interval proves that there was no later
        # official open day between as_of and the month boundary.
        if as_of != max(pd.Timestamp(value) for value in prior_opens):
            continue
        selection = _CalendarSelection(
            as_of_date=as_of,
            effective_start_date=pd.Timestamp(month_opens[0]),
            effective_end_date=pd.Timestamp(month_opens[-1]),
            liquidity_sessions=window,
            sources=tuple(sources),
        )
        qualified.append(
            (
                _utc(
                    calendar_source["completed_at_utc"],
                    label="calendar completed_at_utc",
                ),
                str(calendar_source["calendar_content_sha256"]),
                selection,
            )
        )
    if not qualified:
        raise ProspectiveMembershipError(
            "no verified calendar proves the prior 60 sessions and the full membership month"
        )
    qualified.sort(key=lambda item: (item[0], item[1]))
    return qualified[0][2]


def _daily_sources(
    root: Path,
    sessions: Sequence[pd.Timestamp],
    *,
    available_at: pd.Timestamp,
    sealed_sources: Sequence[Mapping[str, Any]] | None = None,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    if sealed_sources is None:
        checkpoint_path = root / "runtime/data/raw/checkpoint.json"
        checkpoint = _load_json(checkpoint_path, label="market-data checkpoint")
        _require_schema_version(checkpoint, label="market-data checkpoint")
        entries = checkpoint.get("partitions")
        if not isinstance(entries, Mapping):
            raise ProspectiveMembershipError(
                "market-data checkpoint partitions must be a mapping"
            )
        sealed_by_date: dict[str, Mapping[str, Any]] = {}
    else:
        entries = {}
        sealed_by_date = {
            str(source.get("trade_date") or ""): source
            for source in sealed_sources
        }
        if len(sealed_by_date) != len(sealed_sources) or "" in sealed_by_date:
            raise ProspectiveMembershipError(
                "recorded daily source dates are missing or duplicated"
            )
    frames: list[pd.DataFrame] = []
    sources: list[dict[str, Any]] = []
    raw_root = root / "runtime/data/raw/daily"
    for session in sessions:
        date = session.date().isoformat()
        key = f"daily/{date}"
        if sealed_sources is None:
            raw_entry = entries.get(key)
            if not isinstance(raw_entry, Mapping) or raw_entry.get("status") != "complete":
                raise ProspectiveMembershipError(
                    f"missing complete checkpoint entry for {key}"
                )
            entry = dict(raw_entry)
        else:
            raw_entry = sealed_by_date.get(date)
            if (
                not isinstance(raw_entry, Mapping)
                or raw_entry.get("role") != "liquidity_daily_partition"
                or raw_entry.get("dataset") != "daily"
                or raw_entry.get("checkpoint_key") != key
            ):
                raise ProspectiveMembershipError(
                    f"recorded daily source identity differs for {key}"
                )
            entry = dict(raw_entry)
        completed = _utc(
            entry.get("completed_at_utc"), label=f"{key} completed_at_utc"
        )
        if completed > available_at:
            raise ProspectiveMembershipError(f"{key} was not available by cutoff")
        expected = raw_root / f"trade_date={date}" / "part-000.parquet"
        if sealed_sources is None:
            path = _under(Path(str(entry.get("path") or expected)), raw_root, label=key)
            if path != expected.resolve() or not path.is_file():
                raise ProspectiveMembershipError(f"{key} path is not canonical")
            expected_sha = _require_sha(entry.get("sha256"), label=f"{key} SHA")
            if int(entry.get("size_bytes") or -1) != path.stat().st_size:
                raise ProspectiveMembershipError(f"{key} size mismatch")
            try:
                immutable_path, immutable = _capture_immutable_artifact(
                    root,
                    path,
                    expected_sha256=expected_sha,
                )
            except Exception as exc:
                raise ProspectiveMembershipError(
                    f"{key} could not enter immutable source CAS"
                ) from exc
        else:
            if entry.get("path") != _relative(expected, root):
                raise ProspectiveMembershipError(
                    f"recorded daily origin identity differs for {key}"
                )
            try:
                immutable_path = _resolve_immutable_artifact(root, entry)
            except Exception as exc:
                raise ProspectiveMembershipError(
                    f"recorded daily CAS binding differs for {key}"
                ) from exc
            path = expected
            immutable = {
                field: entry[field]
                for field in ("sha256", "immutable_path", "size_bytes", "media_type")
            }
        frame = pd.read_parquet(immutable_path)
        required = {"ts_code", "trade_date", "amount"}
        missing = sorted(required - set(frame.columns))
        if frame.empty or missing:
            raise ProspectiveMembershipError(f"{key} missing daily fields: {missing}")
        dates = pd.to_datetime(
            frame["trade_date"].astype("string").str.replace("-", "", regex=False),
            format="%Y%m%d",
            errors="coerce",
        ).dt.normalize()
        if dates.isna().any() or bool(dates.ne(session).any()):
            raise ProspectiveMembershipError(f"{key} contains mismatched dates")
        tickers = frame["ts_code"].astype("string").str.strip()
        if tickers.isna().any() or tickers.eq("").any() or tickers.duplicated().any():
            raise ProspectiveMembershipError(f"{key} has blank or duplicate securities")
        if len(frame) != int(entry.get("row_count") or -1):
            raise ProspectiveMembershipError(f"{key} row-count mismatch")
        amount = turnover_amount_to_rmb(frame["amount"], source="tushare_daily")
        if bool(np.isinf(amount.to_numpy(dtype=float, na_value=np.nan)).any()):
            raise ProspectiveMembershipError(f"{key} contains infinite amount")
        frames.append(
            pd.DataFrame(
                {"ts_code": tickers, "trade_date": session, "amount_rmb": amount}
            )
        )
        source = {
            "role": "liquidity_daily_partition",
            "dataset": "daily",
            "trade_date": date,
            "path": _relative(path, root),
            **immutable,
            "row_count": int(len(frame)),
            "size_bytes": int(immutable_path.stat().st_size),
            "completed_at_utc": _utc_text(completed),
            "checkpoint_key": key,
            "checkpoint_entry_sha256": _checkpoint_entry_digest(entry),
            "availability_basis": "checkpoint.completed_at_utc",
        }
        if sealed_sources is not None:
            # The checkpoint entry itself is deliberately not re-read.  The
            # sealed source contract and exact CAS bytes are the replay input.
            if source["sha256"] != entry.get("sha256") or source[
                "row_count"
            ] != entry.get("row_count"):
                raise ProspectiveMembershipError(
                    f"recorded daily source derivation differs for {key}"
                )
            source = dict(entry)
        sources.append(source)
    return pd.concat(frames, ignore_index=True), sources


def _rank_members(daily: pd.DataFrame) -> pd.DataFrame:
    work = daily.copy()
    work["amount_rmb"] = pd.to_numeric(work["amount_rmb"], errors="coerce")
    work = work.loc[work["amount_rmb"].gt(0) & np.isfinite(work["amount_rmb"])].copy()
    grouped = work.groupby("ts_code", sort=False, observed=True)["amount_rmb"]
    liquidity = grouped.agg(
        median_amount_60d="median",
        liquidity_observations="count",
    ).reset_index()
    liquidity = liquidity.loc[
        liquidity["liquidity_observations"].ge(MINIMUM_LIQUIDITY_OBSERVATIONS)
    ].copy()
    liquidity["ts_code"] = liquidity["ts_code"].astype("string")
    liquidity = liquidity.sort_values(
        ["median_amount_60d", "ts_code"],
        ascending=[False, True],
        kind="mergesort",
    ).reset_index(drop=True)
    if len(liquidity) < EXPECTED_MEMBERSHIP_SIZE:
        raise ProspectiveMembershipError(
            "fewer than 500 securities meet the frozen liquidity observation minimum"
        )
    selected = liquidity.iloc[:EXPECTED_MEMBERSHIP_SIZE].copy()
    selected["liquidity_rank"] = np.arange(
        1, EXPECTED_MEMBERSHIP_SIZE + 1, dtype=np.int32
    )
    selected["liquidity_observations"] = selected["liquidity_observations"].astype(
        "int64"
    )
    return selected


def _normalise_reference(frame: pd.DataFrame, *, as_of: pd.Timestamp) -> pd.DataFrame:
    required = {"ts_code", "name", "industry", "list_date"}
    missing = sorted(required - set(frame.columns))
    if frame.empty or missing:
        raise ProspectiveMembershipError(
            f"bak_basic response missing fields: {missing}"
        )
    work = frame.copy()
    work["ts_code"] = work["ts_code"].astype("string").str.strip()
    if work["ts_code"].isna().any() or work["ts_code"].eq("").any():
        raise ProspectiveMembershipError("bak_basic response contains blank securities")
    if work["ts_code"].duplicated().any():
        raise ProspectiveMembershipError(
            "bak_basic response contains duplicate securities"
        )
    if "source_trade_date" in work:
        source_dates = pd.to_datetime(
            _compact_date_text(work["source_trade_date"]),
            format="%Y%m%d",
            errors="coerce",
        )
        if source_dates.isna().any() or bool(source_dates.ne(as_of).any()):
            raise ProspectiveMembershipError(
                "bak_basic source_trade_date is not the exact as_of date"
            )
    if "trade_date" in work:
        dates = pd.to_datetime(
            _compact_date_text(work["trade_date"]),
            format="%Y%m%d",
            errors="coerce",
        ).dt.normalize()
        if dates.isna().any() or bool(dates.ne(as_of).any()):
            raise ProspectiveMembershipError(
                "bak_basic trade_date is not the exact as_of date"
            )
    return work.sort_values("ts_code", kind="mergesort").reset_index(drop=True)


def _provider_reference_coverage(
    reference: pd.DataFrame,
    daily: pd.DataFrame,
    *,
    as_of: pd.Timestamp,
) -> dict[str, int]:
    as_of_rows = daily.loc[
        pd.to_datetime(daily["trade_date"], errors="coerce").dt.normalize().eq(as_of)
    ]
    expected = set(as_of_rows["ts_code"].astype("string").str.strip())
    expected.discard("")
    if not expected:
        raise ProspectiveMembershipError(
            "exact-as-of official daily universe is empty"
        )
    observed = set(reference["ts_code"].astype("string").str.strip())
    covered = len(expected & observed)
    coverage_ppm = covered * 1_000_000 // len(expected)
    if coverage_ppm < MINIMUM_PROVIDER_REFERENCE_COVERAGE_PPM:
        missing = sorted(expected - observed)
        raise ProspectiveMembershipError(
            "bak_basic provider-universe coverage "
            f"{coverage_ppm}ppm is below "
            f"{MINIMUM_PROVIDER_REFERENCE_COVERAGE_PPM}ppm; "
            f"missing exact-as-of daily tickers include {missing[:5]}"
        )
    return {
        "as_of_daily_ticker_count": len(expected),
        "reference_ticker_count": len(observed),
        "covered_ticker_count": covered,
        "coverage_ppm": coverage_ppm,
        "minimum_coverage_ppm": MINIMUM_PROVIDER_REFERENCE_COVERAGE_PPM,
    }


def _reference_source(
    root: Path,
    as_of: pd.Timestamp,
    *,
    client: Any | None,
    available_at: pd.Timestamp | None,
    config_path: Path,
) -> tuple[pd.DataFrame, dict[str, Any], bytes]:
    date = as_of.date().isoformat()
    expected = (
        root / "runtime/data/raw/bak_basic" / f"trade_date={date}" / "part-000.parquet"
    )
    checkpoint_path = root / "runtime/data/raw/enrichment-checkpoint.json"
    entry: Mapping[str, Any] | None = None
    if checkpoint_path.is_file():
        checkpoint = _load_json(checkpoint_path, label="enrichment checkpoint")
        _require_schema_version(checkpoint, label="enrichment checkpoint")
        partitions = checkpoint.get("partitions")
        if isinstance(partitions, Mapping):
            candidate = partitions.get(f"bak_basic/trade_date={date}")
            if isinstance(candidate, Mapping) and candidate.get("status") == "complete":
                entry = candidate
    if entry is not None:
        if (
            entry.get("dataset") != "bak_basic"
            or entry.get("trade_date") != date
            or entry.get("request_trade_date") != date
            or entry.get("source_trade_date") != date
            or entry.get("capture_contract_id")
            != EXACT_REFERENCE_CONTRACT_ID
            or entry.get("capture_mode") != "exact_only"
            or entry.get("fallback_used") is not False
            or entry.get("fields") != ENRICHMENT_DATASET_FIELDS["bak_basic"]
            or entry.get("exact_source_required") is not True
            or type(entry.get("stability_sample_count")) is not int
            or int(entry["stability_sample_count"]) < 2
            or type(entry.get("daily_ticker_count")) is not int
            or type(entry.get("covered_ticker_count")) is not int
            or entry.get("daily_ticker_count")
            != entry.get("covered_ticker_count")
            or type(entry.get("reference_ticker_count")) is not int
            or int(entry["reference_ticker_count"]) <= 0
        ):
            raise ProspectiveMembershipError(
                "bak_basic checkpoint is not a stable exact-as-of capture"
            )
        _require_sha(
            entry.get("daily_partition_sha256"),
            label="bak_basic bound daily partition SHA",
        )
        completed = _utc(
            entry.get("completed_at_utc"), label="bak_basic completed_at_utc"
        )
        if available_at is not None and completed > available_at:
            raise ProspectiveMembershipError(
                "bak_basic reference was not available by cutoff"
            )
        path = _under(
            Path(str(entry.get("path") or expected)),
            root / "runtime/data/raw/bak_basic",
            label="bak_basic reference",
        )
        if path != expected.resolve() or not path.is_file():
            raise ProspectiveMembershipError(
                "bak_basic checkpoint path is not canonical"
            )
        source_sha = _require_sha(entry.get("sha256"), label="bak_basic source SHA")
        if int(entry.get("size_bytes") or -1) != path.stat().st_size:
            raise ProspectiveMembershipError("bak_basic source size mismatch")
        try:
            immutable_path, immutable = _capture_immutable_artifact(
                root,
                path,
                expected_sha256=source_sha,
                sha_field="source_sha256",
                path_field="immutable_source_path",
                size_field="source_size_bytes",
                media_field="source_media_type",
            )
        except Exception as exc:
            raise ProspectiveMembershipError(
                "bak_basic source could not enter immutable source CAS"
            ) from exc
        raw = pd.read_parquet(immutable_path)
        if len(raw) != int(entry.get("row_count") or -1):
            raise ProspectiveMembershipError("bak_basic source row-count mismatch")
        if len(raw) != int(entry["reference_ticker_count"]):
            raise ProspectiveMembershipError("bak_basic reference ticker count mismatch")
        source = {
            "role": "point_in_time_reference",
            "kind": "checkpointed_bak_basic",
            "endpoint": "bak_basic",
            "request_trade_date": date,
            "source_path": _relative(path, root),
            **immutable,
            "source_row_count": int(len(raw)),
            "completed_at_utc": _utc_text(completed),
            "checkpoint_key": f"bak_basic/trade_date={date}",
            "checkpoint_entry_sha256": _checkpoint_entry_digest(entry),
            "availability_basis": "checkpoint.completed_at_utc",
            "capture_contract_id": entry["capture_contract_id"],
            "capture_mode": entry["capture_mode"],
            "fallback_used": entry["fallback_used"],
            "source_trade_date": entry["source_trade_date"],
            "stability_sample_count": entry["stability_sample_count"],
            "daily_partition_sha256": entry["daily_partition_sha256"],
            "daily_ticker_count": entry["daily_ticker_count"],
            "covered_ticker_count": entry["covered_ticker_count"],
        }
    else:
        raise ProspectiveMembershipError(
            "stable exact-as-of checkpointed bak_basic reference is required; "
            "run data reference first"
        )
    normalised = _normalise_reference(raw, as_of=as_of)
    raw_payload = {
        "schema_version": 1,
        "endpoint": "bak_basic",
        "request": {
            "trade_date": date.replace("-", ""),
            "fields": ENRICHMENT_DATASET_FIELDS["bak_basic"],
        },
        "columns": sorted(str(column) for column in normalised.columns),
        "records": _reference_records(normalised.loc[:, sorted(normalised.columns)]),
    }
    raw_bytes = _canonical_json_bytes(raw_payload)
    source.update(
        _capture_json_bytes(
            root,
            raw_bytes,
            sha_field="captured_response_sha256",
            path_field="immutable_response_path",
            size_field="response_size_bytes",
            media_field="response_media_type",
        )
    )
    source["captured_response_row_count"] = int(len(normalised))
    return normalised, source, raw_bytes


def _name_has_st_marker(values: pd.Series) -> pd.Series:
    return values.astype("string").str.match(r"^(?:S\*?|\*)?ST", case=False, na=False)


def _build_membership_frame(
    selected: pd.DataFrame,
    reference: pd.DataFrame,
    *,
    period: pd.Period,
    calendar: _CalendarSelection,
) -> pd.DataFrame:
    ref = reference.copy()
    ref["vendor_ts_code_pit"] = ref["ts_code"].astype("string")
    joined = selected.merge(
        ref.drop(columns=["ts_code"]),
        left_on="ts_code",
        right_on="vendor_ts_code_pit",
        how="left",
        validate="one_to_one",
        indicator=True,
    )
    reference_verified = joined["_merge"].eq("both")
    coverage = float(reference_verified.mean())
    if coverage < MINIMUM_REFERENCE_COVERAGE:
        raise ProspectiveMembershipError(
            f"bak_basic selected-member coverage {coverage:.2%} is below "
            f"{MINIMUM_REFERENCE_COVERAGE:.2%}"
        )
    names = joined["name"].astype("string")
    industries = joined["industry"].astype("string")
    list_dates = pd.to_datetime(
        _compact_date_text(joined["list_date"]),
        format="%Y%m%d",
        errors="coerce",
    )
    listed = list_dates.notna() & list_dates.le(calendar.as_of_date)
    is_st = _name_has_st_marker(names)
    if "delist_date" in joined:
        delist_dates = pd.to_datetime(
            _compact_date_text(joined["delist_date"]),
            format="%Y%m%d",
            errors="coerce",
        )
        not_delisted = delist_dates.isna() | delist_dates.gt(calendar.as_of_date)
    else:
        not_delisted = pd.Series(True, index=joined.index)
    if "list_status" in joined:
        status = joined["list_status"].astype("string").str.strip().str.upper()
        not_delisted &= ~status.isin(["D", "DELISTED", "退市"])
    eligible = reference_verified & listed & not_delisted & ~is_st

    reasons = pd.Series("eligible", index=joined.index, dtype="string")
    reasons.loc[~reference_verified] = "reference_missing"
    reasons.loc[reference_verified & ~listed] = "not_listed_at_as_of"
    reasons.loc[reference_verified & listed & ~not_delisted] = "delisted_at_as_of"
    reasons.loc[reference_verified & listed & not_delisted & is_st] = "st_name_marker"
    result = pd.DataFrame(
        {
            "ts_code": joined["ts_code"].astype("string"),
            "as_of_date": calendar.as_of_date,
            "historical_st_known": reference_verified.astype(bool),
            "is_st_at_asof": is_st.astype(bool),
            "st_filter_status": np.where(
                reference_verified,
                "future_5_2_bak_basic_name_verified",
                "future_5_2_reference_missing_excluded",
            ),
            "median_amount_60d": joined["median_amount_60d"].astype(float),
            "liquidity_observations": joined["liquidity_observations"].astype("int64"),
            "liquidity_rank": joined["liquidity_rank"].astype("int32"),
            "membership_month": str(period),
            "effective_start_date": calendar.effective_start_date,
            "effective_end_date": calendar.effective_end_date,
            "liquidity_window_start": calendar.liquidity_sessions[0],
            "liquidity_window_end": calendar.liquidity_sessions[-1],
            "state_as_of_date": calendar.as_of_date,
            "state_available_date": calendar.effective_start_date,
            "industry_pit": industries,
            "name_pit": names,
            "st_known_pit": reference_verified.astype(bool),
            "is_st_pit": is_st.astype(bool),
            "st_type_pit": pd.Series(
                np.where(is_st, "NAME_MARKER", None), dtype="string"
            ),
            "st_type_name_pit": pd.Series(
                np.where(is_st, "名称风险警示", None), dtype="string"
            ),
            "reference_verified_pit": reference_verified.astype(bool),
            "eligible_pre_pit": True,
            "eligible": eligible.astype(bool),
            "vendor_ts_code_pit": joined["vendor_ts_code_pit"].astype("string"),
            "security_alias_applied_pit": False,
            "security_alias_source": pd.Series(
                pd.NA, index=joined.index, dtype="string"
            ),
            "eligibility_reason": reasons,
        }
    )
    if len(result) != EXPECTED_MEMBERSHIP_SIZE:
        raise AssertionError("membership selection changed row count")
    return result.sort_values("liquidity_rank", kind="mergesort").reset_index(drop=True)


def _write_create_only(path: Path, payload: bytes) -> None:
    try:
        _write_verified(path, payload)
    except ProspectiveDataError as exc:
        raise ProspectiveMembershipError(f"create-only collision at {path}") from exc


def _manifest_core(manifest: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value for key, value in manifest.items() if key != "manifest_core_sha256"
    }


def _resolve_project_and_path(
    snapshot: str | Path,
    *,
    project_root: str | Path | None,
) -> tuple[Path, Path]:
    path = Path(snapshot).expanduser().resolve()
    if path.is_dir():
        path = path / "membership.parquet"
    if project_root is not None:
        return Path(project_root).expanduser().resolve(), path
    parts = path.parts
    marker = ("runtime", "prospective", "5.0", "membership")
    lowered = tuple(part.casefold() for part in parts)
    marker_lower = tuple(part.casefold() for part in marker)
    for index in range(len(parts) - len(marker) + 1):
        if lowered[index : index + len(marker)] == marker_lower:
            return Path(*parts[:index]).resolve(), path
    raise ProspectiveMembershipError("cannot infer project root from membership path")


def build_prospective_membership_snapshot(
    project_root: str | Path,
    membership_month: str,
    *,
    client: Any | None = None,
    available_at_utc: str | pd.Timestamp | None = None,
    config_path: str | Path | None = None,
) -> ProspectiveMembershipSnapshot:
    """Build one create-only future monthly membership snapshot.

    ``available_at_utc`` is an optional hard cutoff for both source evidence
    and durable bundle completion.  The manifest records the actual
    conservative publication bound, not the caller's cutoff.  Production
    callers normally omit it; tests and witnessed replays may supply one.
    """

    root = Path(project_root).expanduser().resolve()
    period = _month(membership_month)
    resolved_config_path = (
        root / DEFAULT_CONFIG_PATH
        if config_path is None
        else Path(config_path).expanduser()
    )
    if not resolved_config_path.is_absolute():
        resolved_config_path = root / resolved_config_path
    resolved_config_path = resolved_config_path.resolve()
    if config_path is None:
        resolved_config_path = _under(
            resolved_config_path,
            root,
            label="membership data config",
        )
    if not resolved_config_path.is_file():
        raise ProspectiveMembershipError(
            f"missing membership data config: {resolved_config_path}"
        )
    explicit_cap = (
        None
        if available_at_utc is None
        else _utc(available_at_utc, label="available_at_utc")
    )
    source_cutoff = explicit_cap or _now_utc()
    calendar = _calendar_selection(root, period, available_at=source_cutoff)
    if calendar.as_of_date >= period.start_time.normalize():
        raise ProspectiveMembershipError("membership as_of is not strictly pre-month")
    daily, daily_sources = _daily_sources(
        root, calendar.liquidity_sessions, available_at=source_cutoff
    )
    if pd.Timestamp(daily["trade_date"].max()) > calendar.as_of_date:
        raise ProspectiveMembershipError("future daily data entered membership build")
    selected = _rank_members(daily)
    reference, reference_source, reference_bytes = _reference_source(
        root,
        calendar.as_of_date,
        client=client,
        available_at=explicit_cap,
        config_path=resolved_config_path,
    )
    provider_coverage = _provider_reference_coverage(
        reference,
        daily,
        as_of=calendar.as_of_date,
    )
    as_of_daily_sources = [
        source
        for source in daily_sources
        if source.get("trade_date") == calendar.as_of_date.date().isoformat()
    ]
    if len(as_of_daily_sources) != 1:
        raise ProspectiveMembershipError(
            "exact-as-of daily source binding is missing or duplicated"
        )
    as_of_daily_source = as_of_daily_sources[0]
    if (
        reference_source.get("daily_partition_sha256")
        != as_of_daily_source.get("sha256")
        or reference_source.get("daily_ticker_count")
        != provider_coverage["as_of_daily_ticker_count"]
        or reference_source.get("covered_ticker_count")
        != provider_coverage["covered_ticker_count"]
        or reference_source.get("source_row_count")
        != provider_coverage["reference_ticker_count"]
    ):
        raise ProspectiveMembershipError(
            "bak_basic checkpoint daily-universe binding differs from exact source bytes"
        )
    reference_source["provider_universe_coverage"] = provider_coverage
    frame = _build_membership_frame(
        selected,
        reference,
        period=period,
        calendar=calendar,
    )
    completed_candidates: list[pd.Timestamp] = []
    for source in (*calendar.sources, *daily_sources, reference_source):
        if source.get("completed_at_utc") is not None:
            completed_candidates.append(
                _utc(source["completed_at_utc"], label="source completed_at_utc")
            )
    if not completed_candidates:
        raise ProspectiveMembershipError(
            "membership has no checkpointed source completion evidence"
        )

    month_root = root / PROSPECTIVE_MEMBERSHIP_ROOT / str(period)
    month_root_was_missing = not month_root.exists()
    month_root.mkdir(parents=True, exist_ok=True)
    if month_root_was_missing:
        _fsync_directory(month_root.parent)
    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=month_root, suffix=".membership.parquet.tmp", delete=False
        ) as handle:
            temporary_name = handle.name
        temporary = Path(temporary_name)
        frame.to_parquet(temporary, index=False)
        membership_bytes = temporary.read_bytes()
    finally:
        if temporary_name is not None:
            Path(temporary_name).unlink(missing_ok=True)
    artifact_sha = _sha256_bytes(membership_bytes)
    directory = month_root / artifact_sha
    membership_path = directory / "membership.parquet"
    reference_path = directory / "bak-basic-raw.json"
    source_contract_path = directory / "source-contract.json"
    manifest_path = directory / "manifest.json"
    contract = _source_contract()
    contract_bytes = _canonical_json_bytes(contract)
    output_records_sha = _sha256_bytes(_canonical_json_bytes(_records(frame)))
    input_sources = [*calendar.sources, *daily_sources, reference_source]
    input_sources_sha = _sha256_bytes(_canonical_json_bytes(input_sources))
    lock_path = month_root / f".{artifact_sha}.lock"
    with _bundle_lock(lock_path):
        # The manifest is published last and is the only commit marker.  Once
        # present it is never deleted or rewritten; an idempotent builder keeps
        # the original completion evidence.
        if manifest_path.is_file():
            loaded = load_prospective_membership_snapshot(
                membership_path,
                project_root=root,
                available_at_utc=explicit_cap,
            )
            if explicit_cap is None:
                _wait_through_publication_upper_bound(
                    _utc(
                        loaded.completed_at_utc,
                        label="existing membership completed_at_utc",
                    )
                )
            return loaded

        _write_create_only(membership_path, membership_bytes)
        _write_create_only(reference_path, reference_bytes)
        _write_create_only(source_contract_path, contract_bytes)

        publication_bound = _publication_upper_bound()
        completed_at = max(publication_bound, *completed_candidates)
        if explicit_cap is not None and completed_at > explicit_cap:
            raise ProspectiveMembershipError(
                "membership sources/build completed after the requested cutoff"
            )
        manifest: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "rule_id": RULE_ID,
            "membership_month": str(period),
            "as_of_date": calendar.as_of_date.date().isoformat(),
            "effective_start_date": calendar.effective_start_date.date().isoformat(),
            "effective_end_date": calendar.effective_end_date.date().isoformat(),
            "liquidity_window_start": calendar.liquidity_sessions[0].date().isoformat(),
            "liquidity_window_end": calendar.liquidity_sessions[-1].date().isoformat(),
            "liquidity_session_count": len(calendar.liquidity_sessions),
            "minimum_liquidity_observations": MINIMUM_LIQUIDITY_OBSERVATIONS,
            "row_count": int(len(frame)),
            "eligible_count": int(frame["eligible"].sum()),
            "artifact_path": _relative(membership_path, root),
            "artifact_sha256": artifact_sha,
            "output_records_sha256": output_records_sha,
            "completed_at_utc": _utc_text(completed_at),
            "source_contract_path": _relative(source_contract_path, root),
            "source_contract_sha256": _sha256_bytes(contract_bytes),
            "reference_raw_path": _relative(reference_path, root),
            "reference_raw_sha256": _sha256_bytes(reference_bytes),
            "input_sources": input_sources,
            "input_sources_sha256": input_sources_sha,
            "selected_tickers_sha256": _sha256_bytes(
                _canonical_json_bytes(frame["ts_code"].astype(str).tolist())
            ),
            "amount_unit": "RMB",
            "historical_equivalence_claimed": False,
        }
        manifest["manifest_core_sha256"] = _sha256_bytes(
            _canonical_json_bytes(_manifest_core(manifest))
        )
        manifest_bytes = _canonical_json_bytes(manifest)
        _write_create_only(manifest_path, manifest_bytes)
        try:
            _verify_publication_upper_bound(
                completed_at, label="membership bundle"
            )
        except ProspectiveDataError as exc:
            raise ProspectiveMembershipError(str(exc)) from exc
        _wait_through_publication_upper_bound(publication_bound)
        return load_prospective_membership_snapshot(
            membership_path,
            project_root=root,
            available_at_utc=explicit_cap or completed_at,
        )


def _verify_recorded_source_file(
    root: Path,
    source: Mapping[str, Any],
    *,
    available_at: pd.Timestamp,
) -> None:
    completed_text = source.get("completed_at_utc")
    if (
        completed_text is not None
        and _utc(completed_text, label="source completed_at_utc") > available_at
    ):
        raise ProspectiveMembershipError(
            "snapshot contains a source unavailable by cutoff"
        )
    role = str(source.get("role") or "")
    if role == "frozen_execution_session_prefix":
        expected = root / "runtime/data/top500/execution.parquet"
        if source.get("path") != _relative(expected, root):
            raise ProspectiveMembershipError("frozen execution origin identity changed")
        try:
            _resolve_immutable_artifact(root, source)
        except Exception as exc:
            raise ProspectiveMembershipError(
                "frozen execution immutable source mutation detected"
            ) from exc
    elif role == "official_trade_calendar":
        key = _require_sha(source.get("checkpoint_key"), label="calendar checkpoint key")
        expected = (
            root
            / "runtime/data/raw/trade_cal"
            / f"calendar_sha256={key}"
            / "part-000.parquet"
        )
        if (
            source.get("path") != _relative(expected, root)
            or source.get("manifest_path")
            != _relative(expected.with_name("manifest.json"), root)
        ):
            raise ProspectiveMembershipError("official calendar origin identity changed")
        try:
            _resolve_immutable_artifact(root, source)
            _resolve_immutable_artifact(
                root,
                source,
                sha_field="manifest_sha256",
                path_field="immutable_manifest_path",
                size_field="manifest_size_bytes",
                media_field="manifest_media_type",
            )
        except Exception as exc:
            raise ProspectiveMembershipError(
                "official calendar immutable source mutation detected"
            ) from exc
    elif role == "liquidity_daily_partition":
        date = str(source.get("trade_date") or "")
        expected = (
            root / "runtime/data/raw/daily" / f"trade_date={date}" / "part-000.parquet"
        )
        if source.get("path") != _relative(expected, root):
            raise ProspectiveMembershipError("daily source origin identity changed")
        try:
            _resolve_immutable_artifact(root, source)
        except Exception as exc:
            raise ProspectiveMembershipError(
                f"daily immutable source mutation detected for {date}"
            ) from exc
    elif (
        role == "point_in_time_reference"
        and source.get("kind") == "checkpointed_bak_basic"
    ):
        date = str(source.get("request_trade_date") or "")
        expected = (
            root
            / "runtime/data/raw/bak_basic"
            / f"trade_date={date}"
            / "part-000.parquet"
        )
        if source.get("source_path") != _relative(expected, root):
            raise ProspectiveMembershipError("bak_basic origin identity changed")
        try:
            _resolve_immutable_artifact(
                root,
                source,
                sha_field="source_sha256",
                path_field="immutable_source_path",
                size_field="source_size_bytes",
                media_field="source_media_type",
            )
        except Exception as exc:
            raise ProspectiveMembershipError(
                "bak_basic immutable source mutation detected"
            ) from exc
    elif role == "point_in_time_reference" and source.get("kind") != (
        "captured_bak_basic_response"
    ):
        raise ProspectiveMembershipError("unsupported point-in-time reference kind")
    elif role != "point_in_time_reference":
        raise ProspectiveMembershipError(f"unsupported membership source role: {role}")

    if role == "point_in_time_reference":
        try:
            _resolve_immutable_artifact(
                root,
                source,
                sha_field="captured_response_sha256",
                path_field="immutable_response_path",
                size_field="response_size_bytes",
                media_field="response_media_type",
            )
        except Exception as exc:
            raise ProspectiveMembershipError(
                "captured reference immutable source mutation detected"
            ) from exc


def _verify_calendar_rebuild(
    root: Path,
    *,
    period: pd.Period,
    sources: Sequence[Mapping[str, Any]],
    expected_sessions: Sequence[pd.Timestamp],
    expected_as_of: pd.Timestamp,
    expected_effective_start: pd.Timestamp,
    expected_effective_end: pd.Timestamp,
) -> None:
    official = [
        source for source in sources if source.get("role") == "official_trade_calendar"
    ]
    frozen = [
        source
        for source in sources
        if source.get("role") == "frozen_execution_session_prefix"
    ]
    if len(official) != 1 or len(frozen) > 1:
        raise ProspectiveMembershipError(
            "membership calendar-source cardinality is invalid"
    )
    calendar_source = official[0]
    try:
        path = _resolve_immutable_artifact(root, calendar_source)
        manifest_path = _resolve_immutable_artifact(
            root,
            calendar_source,
            sha_field="manifest_sha256",
            path_field="immutable_manifest_path",
            size_field="manifest_size_bytes",
            media_field="manifest_media_type",
        )
    except Exception as exc:
        raise ProspectiveMembershipError(
            "official calendar CAS binding no longer validates"
        ) from exc
    calendar_manifest = _load_json(
        manifest_path, label="immutable official calendar manifest"
    )
    _require_schema_version(
        calendar_manifest, label="immutable official calendar manifest"
    )
    if str(calendar_manifest.get("calendar_content_sha256") or "") != str(
        calendar_source.get("calendar_content_sha256") or ""
    ):
        raise ProspectiveMembershipError(
            "official calendar manifest/content binding differs"
        )
    raw = pd.read_parquet(path)
    exchange = str(calendar_source.get("exchange") or "SSE")
    try:
        normalised, _, content_sha = _normalise_trade_calendar(
            raw,
            exchange=exchange,
            start_date=str(calendar_source.get("start_date")),
            end_date=str(calendar_source.get("end_date")),
        )
    except ValueError as exc:
        raise ProspectiveMembershipError(
            "official calendar source no longer validates"
        ) from exc
    if content_sha != _require_sha(
        calendar_source.get("calendar_content_sha256"), label="calendar content SHA"
    ):
        raise ProspectiveMembershipError("official calendar content digest mismatch")
    month_first = period.start_time.normalize()
    month_end = period.end_time.normalize()
    dates = normalised["cal_date"]
    if dates.iloc[0] > month_first or dates.iloc[-1] < month_end:
        raise ProspectiveMembershipError(
            "official calendar no longer covers membership month"
        )
    prior_opens = [
        pd.Timestamp(value)
        for value in normalised.loc[
            normalised["is_open"] & dates.lt(month_first), "cal_date"
        ]
    ]
    month_opens = [
        pd.Timestamp(value)
        for value in normalised.loc[
            normalised["is_open"] & dates.between(month_first, month_end), "cal_date"
        ]
    ]
    if not prior_opens or not month_opens:
        raise ProspectiveMembershipError(
            "official calendar has no required open sessions"
        )
    sessions = list(prior_opens)
    if len(sessions) < LIQUIDITY_SESSION_COUNT:
        if len(frozen) != 1:
            raise ProspectiveMembershipError("calendar needs a frozen session prefix")
        try:
            frozen_path = _resolve_immutable_artifact(root, frozen[0])
        except Exception as exc:
            raise ProspectiveMembershipError(
                "frozen execution CAS binding no longer validates"
            ) from exc
        values = pd.to_datetime(
            pd.read_parquet(frozen_path, columns=["date"])["date"], errors="coerce"
        )
        if values.empty or values.isna().any():
            raise ProspectiveMembershipError(
                "frozen execution session prefix is invalid"
            )
        frozen_sessions = [
            pd.Timestamp(value) for value in sorted(values.dt.normalize().unique())
        ]
        if normalised["cal_date"].iloc[0] > frozen_sessions[-1] + pd.Timedelta(days=1):
            raise ProspectiveMembershipError(
                "calendar and frozen session prefix have a gap"
            )
        sessions = sorted(
            {
                *[value for value in frozen_sessions if value < month_first],
                *sessions,
            }
        )
    elif frozen:
        raise ProspectiveMembershipError(
            "unnecessary frozen session prefix changes the contract"
        )
    rebuilt_window = tuple(sessions[-LIQUIDITY_SESSION_COUNT:])
    if rebuilt_window != tuple(expected_sessions):
        raise ProspectiveMembershipError(
            "official calendar rebuild changed liquidity sessions"
        )
    if prior_opens[-1] != expected_as_of:
        raise ProspectiveMembershipError("official calendar rebuild changed as_of")
    if (
        month_opens[0] != expected_effective_start
        or month_opens[-1] != expected_effective_end
    ):
        raise ProspectiveMembershipError(
            "official calendar rebuild changed effective interval"
        )


def load_prospective_membership_snapshot(
    snapshot: str | Path,
    *,
    project_root: str | Path | None = None,
    available_at_utc: str | pd.Timestamp | None = None,
    _sealed_artifact_source: Mapping[str, Any] | None = None,
) -> ProspectiveMembershipSnapshot:
    """Load, source-verify, and independently rebuild a membership snapshot."""

    root, membership_path = _resolve_project_and_path(
        snapshot, project_root=project_root
    )
    membership_root = root / PROSPECTIVE_MEMBERSHIP_ROOT
    membership_path = _under(
        membership_path, membership_root, label="membership snapshot"
    )
    if membership_path.name != "membership.parquet":
        raise ProspectiveMembershipError("membership snapshot path is not canonical")
    manifest_path = membership_path.parent / "manifest.json"
    if _sealed_artifact_source is None:
        if not membership_path.is_file():
            raise ProspectiveMembershipError("membership snapshot path is not canonical")
        membership_read_path = membership_path
        manifest_read_path = manifest_path
    else:
        source = dict(_sealed_artifact_source)
        if (
            source.get("role") != "membership"
            or source.get("kind") != "content_addressed_monthly_snapshot"
            or source.get("path") != _relative(membership_path, root)
            or source.get("manifest_path") != _relative(manifest_path, root)
        ):
            raise ProspectiveMembershipError(
                "sealed membership artifact identity differs"
            )
        try:
            membership_read_path = _resolve_immutable_artifact(root, source)
            manifest_read_path = _resolve_immutable_artifact(
                root,
                source,
                sha_field="manifest_sha256",
                path_field="immutable_manifest_path",
                size_field="manifest_size_bytes",
                media_field="manifest_media_type",
            )
        except Exception as exc:
            raise ProspectiveMembershipError(
                "sealed membership artifact CAS binding is invalid"
            ) from exc
    artifact_sha = sha256_file(membership_read_path)
    if membership_path.parent.name != artifact_sha:
        raise ProspectiveMembershipError(
            "membership directory does not match artifact SHA"
        )
    month_text = membership_path.parent.parent.name
    period = _month(month_text)
    contract_path = membership_path.parent / "source-contract.json"
    reference_path = membership_path.parent / "bak-basic-raw.json"
    manifest = _load_json(manifest_read_path, label="membership manifest")
    _require_schema_version(
        manifest,
        label="membership manifest",
        expected=SCHEMA_VERSION,
    )
    if (
        manifest.get("rule_id") != RULE_ID
        or manifest.get("membership_month") != month_text
    ):
        raise ProspectiveMembershipError("membership manifest rule/month mismatch")
    if (
        _require_sha(manifest.get("artifact_sha256"), label="membership artifact SHA")
        != artifact_sha
    ):
        raise ProspectiveMembershipError("membership artifact hash mismatch")
    expected_relative_paths = {
        "artifact_path": _relative(membership_path, root),
        "source_contract_path": _relative(contract_path, root),
        "reference_raw_path": _relative(reference_path, root),
    }
    if any(
        manifest.get(key) != value for key, value in expected_relative_paths.items()
    ):
        raise ProspectiveMembershipError(
            "membership manifest contains a noncanonical path"
        )
    expected_core_sha = _sha256_bytes(_canonical_json_bytes(_manifest_core(manifest)))
    if (
        _require_sha(manifest.get("manifest_core_sha256"), label="manifest core SHA")
        != expected_core_sha
    ):
        raise ProspectiveMembershipError("membership manifest core hash mismatch")
    contract_bytes = _canonical_json_bytes(_source_contract())
    if _sha256_bytes(contract_bytes) != _require_sha(
        manifest.get("source_contract_sha256"), label="source contract SHA"
    ):
        raise ProspectiveMembershipError("membership source contract hash mismatch")
    try:
        contract = json.loads(contract_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProspectiveMembershipError(
            "unreadable membership source contract"
        ) from exc
    if not isinstance(contract, Mapping):
        raise ProspectiveMembershipError("membership source contract must be an object")
    _require_schema_version(contract, label="membership source contract")
    if contract != _source_contract():
        raise ProspectiveMembershipError(
            "membership source contract is not the frozen 5.2 rule"
        )
    completed = _utc(
        manifest.get("completed_at_utc"), label="membership completed_at_utc"
    )
    cap = (
        completed
        if available_at_utc is None
        else _utc(available_at_utc, label="available_at_utc")
    )
    if completed > cap:
        raise ProspectiveMembershipError(
            "membership was not available by requested cutoff"
        )
    input_sources = manifest.get("input_sources")
    if not isinstance(input_sources, list):
        raise ProspectiveMembershipError("membership input_sources must be a list")
    if _sha256_bytes(_canonical_json_bytes(input_sources)) != _require_sha(
        manifest.get("input_sources_sha256"), label="input sources SHA"
    ):
        raise ProspectiveMembershipError("membership input-source digest mismatch")
    for source in input_sources:
        if not isinstance(source, Mapping):
            raise ProspectiveMembershipError("membership source must be an object")
        _verify_recorded_source_file(
            root,
            source,
            available_at=completed,
        )

    # Independently recover the exact source window from the recorded daily
    # partitions rather than trusting the serialized output rows.
    daily_sources = [
        source
        for source in input_sources
        if source.get("role") == "liquidity_daily_partition"
    ]
    if len(daily_sources) != LIQUIDITY_SESSION_COUNT:
        raise ProspectiveMembershipError(
            "membership must bind exactly 60 daily partitions"
        )
    daily_sources.sort(key=lambda source: str(source.get("trade_date")))
    sessions = tuple(
        pd.Timestamp(str(source["trade_date"])) for source in daily_sources
    )
    as_of = pd.Timestamp(str(manifest.get("as_of_date")))
    if sessions[-1] != as_of or any(value > as_of for value in sessions):
        raise ProspectiveMembershipError("membership daily source window is not causal")
    effective_start = pd.Timestamp(str(manifest.get("effective_start_date")))
    effective_end = pd.Timestamp(str(manifest.get("effective_end_date")))
    _verify_calendar_rebuild(
        root,
        period=period,
        sources=input_sources,
        expected_sessions=sessions,
        expected_as_of=as_of,
        expected_effective_start=effective_start,
        expected_effective_end=effective_end,
    )
    daily, rebuilt_daily_sources = _daily_sources(
        root,
        sessions,
        available_at=completed,
        sealed_sources=daily_sources,
    )
    if [
        (
            source["sha256"],
            source["checkpoint_entry_sha256"],
            source["completed_at_utc"],
        )
        for source in rebuilt_daily_sources
    ] != [
        (
            source["sha256"],
            source["checkpoint_entry_sha256"],
            source["completed_at_utc"],
        )
        for source in daily_sources
    ]:
        raise ProspectiveMembershipError("daily checkpoint bindings changed")
    selected = _rank_members(daily)
    reference_sources = [
        source
        for source in input_sources
        if source.get("role") == "point_in_time_reference"
    ]
    if len(reference_sources) != 1:
        raise ProspectiveMembershipError("membership needs one reference source")
    reference_source = reference_sources[0]
    try:
        immutable_reference_path = _resolve_immutable_artifact(
            root,
            reference_source,
            sha_field="captured_response_sha256",
            path_field="immutable_response_path",
            size_field="response_size_bytes",
            media_field="response_media_type",
        )
    except Exception as exc:
        raise ProspectiveMembershipError(
            "captured reference CAS binding is invalid"
        ) from exc
    reference_bytes = immutable_reference_path.read_bytes()
    if _sha256_bytes(reference_bytes) != _require_sha(
        manifest.get("reference_raw_sha256"), label="reference raw SHA"
    ):
        raise ProspectiveMembershipError(
            "captured reference CAS response mutation detected"
        )
    try:
        reference_payload = json.loads(reference_bytes.decode("utf-8"))
        reference_records = reference_payload["records"]
    except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError) as exc:
        raise ProspectiveMembershipError(
            "unreadable captured reference response"
        ) from exc
    if not isinstance(reference_records, list):
        raise ProspectiveMembershipError("captured reference records must be a list")
    if not isinstance(reference_payload, Mapping):
        raise ProspectiveMembershipError("captured reference response must be an object")
    _require_schema_version(reference_payload, label="captured reference response")
    reference_request = reference_payload.get("request")
    if (
        reference_payload.get("endpoint") != "bak_basic"
        or not isinstance(reference_request, Mapping)
        or reference_request.get("trade_date") != as_of.strftime("%Y%m%d")
        or reference_request.get("fields") != ENRICHMENT_DATASET_FIELDS["bak_basic"]
    ):
        raise ProspectiveMembershipError(
            "captured reference request is not exact-as_of"
        )
    if reference_source.get("captured_response_sha256") != manifest.get(
        "reference_raw_sha256"
    ):
        raise ProspectiveMembershipError("captured reference source binding mismatch")
    if (
        reference_source.get("kind") != "checkpointed_bak_basic"
        or reference_source.get("capture_contract_id")
        != EXACT_REFERENCE_CONTRACT_ID
        or reference_source.get("capture_mode") != "exact_only"
        or reference_source.get("fallback_used") is not False
        or reference_source.get("source_trade_date")
        != as_of.date().isoformat()
        or type(reference_source.get("stability_sample_count")) is not int
        or int(reference_source["stability_sample_count"]) < 2
        or reference_source.get("daily_ticker_count")
        != reference_source.get("covered_ticker_count")
    ):
        raise ProspectiveMembershipError(
            "membership reference is not a stable checkpointed exact capture"
        )
    try:
        immutable_source_path = _resolve_immutable_artifact(
            root,
            reference_source,
            sha_field="source_sha256",
            path_field="immutable_source_path",
            size_field="source_size_bytes",
            media_field="source_media_type",
        )
        source_frame = pd.read_parquet(immutable_source_path)
    except Exception as exc:
        raise ProspectiveMembershipError(
            "checkpointed bak_basic CAS bytes are unreadable"
        ) from exc
    if len(source_frame) != int(reference_source.get("source_row_count") or -1):
        raise ProspectiveMembershipError("checkpointed bak_basic CAS row count differs")
    rebuilt_reference = _normalise_reference(source_frame, as_of=as_of)
    rebuilt_payload = {
        "schema_version": 1,
        "endpoint": "bak_basic",
        "request": {
            "trade_date": as_of.strftime("%Y%m%d"),
            "fields": ENRICHMENT_DATASET_FIELDS["bak_basic"],
        },
        "columns": sorted(str(column) for column in rebuilt_reference.columns),
        "records": _reference_records(
            rebuilt_reference.loc[:, sorted(rebuilt_reference.columns)]
        ),
    }
    if _canonical_json_bytes(rebuilt_payload) != reference_bytes:
        raise ProspectiveMembershipError(
            "checkpointed bak_basic CAS does not rebuild captured response"
        )
    reference = _normalise_reference(pd.DataFrame(reference_records), as_of=as_of)
    recorded_coverage = reference_source.get("provider_universe_coverage")
    rebuilt_coverage = _provider_reference_coverage(
        reference,
        daily,
        as_of=as_of,
    )
    as_of_daily_sources = [
        source
        for source in input_sources
        if source.get("role") == "liquidity_daily_partition"
        and source.get("trade_date") == as_of.date().isoformat()
    ]
    if len(as_of_daily_sources) != 1:
        raise ProspectiveMembershipError(
            "sealed exact-as-of daily source binding is missing or duplicated"
        )
    if (
        reference_source.get("daily_partition_sha256")
        != as_of_daily_sources[0].get("sha256")
        or reference_source.get("daily_ticker_count")
        != rebuilt_coverage["as_of_daily_ticker_count"]
        or reference_source.get("covered_ticker_count")
        != rebuilt_coverage["covered_ticker_count"]
        or reference_source.get("source_row_count")
        != rebuilt_coverage["reference_ticker_count"]
    ):
        raise ProspectiveMembershipError(
            "sealed bak_basic daily-universe binding differs from source replay"
        )
    if recorded_coverage != rebuilt_coverage:
        raise ProspectiveMembershipError(
            "captured reference provider-universe coverage differs"
        )
    calendar = _CalendarSelection(
        as_of_date=as_of,
        effective_start_date=effective_start,
        effective_end_date=effective_end,
        liquidity_sessions=sessions,
        sources=tuple(
            source
            for source in input_sources
            if source.get("role")
            in {"official_trade_calendar", "frozen_execution_session_prefix"}
        ),
    )
    rebuilt = _build_membership_frame(
        selected,
        reference,
        period=period,
        calendar=calendar,
    )
    rebuilt_records_sha = _sha256_bytes(_canonical_json_bytes(_records(rebuilt)))
    if rebuilt_records_sha != _require_sha(
        manifest.get("output_records_sha256"), label="output records SHA"
    ):
        raise ProspectiveMembershipError("membership source rebuild mismatch")
    frame = pd.read_parquet(membership_read_path)
    if len(frame) != EXPECTED_MEMBERSHIP_SIZE or len(frame) != int(
        manifest.get("row_count") or -1
    ):
        raise ProspectiveMembershipError("membership row count is not exactly 500")
    if _sha256_bytes(_canonical_json_bytes(_records(frame))) != rebuilt_records_sha:
        raise ProspectiveMembershipError(
            "membership Parquet rows do not match source rebuild"
        )
    if frame["ts_code"].astype("string").duplicated().any():
        raise ProspectiveMembershipError("membership contains duplicate securities")
    if _sha256_bytes(
        _canonical_json_bytes(frame["ts_code"].astype(str).tolist())
    ) != _require_sha(
        manifest.get("selected_tickers_sha256"), label="selected tickers SHA"
    ):
        raise ProspectiveMembershipError("membership selected-ticker digest mismatch")
    return ProspectiveMembershipSnapshot(
        membership_month=month_text,
        as_of_date=as_of.date().isoformat(),
        artifact_sha256=artifact_sha,
        directory=membership_path.parent,
        membership_path=membership_path,
        manifest_path=manifest_path,
        source_contract_path=contract_path,
        reference_raw_path=reference_path,
        completed_at_utc=_utc_text(completed),
        frame=frame,
        manifest=manifest,
    )


__all__ = [
    "EXPECTED_MEMBERSHIP_SIZE",
    "LIQUIDITY_SESSION_COUNT",
    "MINIMUM_LIQUIDITY_OBSERVATIONS",
    "ProspectiveMembershipError",
    "ProspectiveMembershipSnapshot",
    "RULE_ID",
    "build_prospective_membership_snapshot",
    "load_prospective_membership_snapshot",
]

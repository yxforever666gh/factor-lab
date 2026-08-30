"""Content-addressed, all-status Tushare security-master snapshots.

The security master is deliberately separate from the daily-market checkpoint.
Every published snapshot contains listed, delisted, and (when supplied by the
provider) paused-listing securities.  Downstream research code may apply a PIT
exchange/currency filter, but this ingestion layer never turns the archive into
a current-``L`` universe.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Mapping, Sequence

import pandas as pd

from .catalog import DEFAULT_CONFIG_PATH, RuntimeLayout, load_data_config, sha256_file
from .sources import (
    MarketDataClient,
    _call,
    _checkpoint_lock,
    _configured_tushare_client,
    _write_checkpoint_with_conservative_completion,
    _write_json_atomic,
    _write_parquet_atomic,
)


SECURITY_MASTER_CONTRACT_ID = "factor-lab/tushare-stock-basic-security-master/1"
SECURITY_MASTER_FIELDS = (
    "ts_code",
    "symbol",
    "name",
    "area",
    "industry",
    "fullname",
    "enname",
    "cnspell",
    "market",
    "exchange",
    "curr_type",
    "list_status",
    "list_date",
    "delist_date",
    "is_hs",
)
SECURITY_MASTER_FIELD_ARGUMENT = ",".join(SECURITY_MASTER_FIELDS)
SECURITY_MASTER_LIST_STATUSES = ("L", "D", "P")

# Tushare documents ``P`` as the paused-listing state.  There are legitimately
# dates on which no security has that status.  It must still be queried and
# recorded in the manifest.  Empty ``L`` or ``D`` responses are treated as an
# incomplete/permission-limited provider response because the A-share archive
# is known to contain both states.
SECURITY_MASTER_OPTIONAL_EMPTY_STATUSES = frozenset({"P"})


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def security_master_root(layout: RuntimeLayout) -> Path:
    """Return the dedicated raw reference root for security-master snapshots."""

    return layout.raw_root / "reference" / "stock_basic"


def security_master_checkpoint_path(layout: RuntimeLayout) -> Path:
    """Return the checkpoint that is independent of daily-market ingestion."""

    return security_master_root(layout) / "security-master-checkpoint.json"


def security_master_snapshot_path(layout: RuntimeLayout, snapshot_sha256: str) -> Path:
    if not re.fullmatch(r"[0-9a-f]{64}", str(snapshot_sha256)):
        raise ValueError("snapshot_sha256 must be a lowercase SHA-256 digest")
    return security_master_root(layout) / f"snapshot_sha256={snapshot_sha256}"


def _empty_master() -> pd.DataFrame:
    columns: dict[str, pd.Series] = {
        name: pd.Series(dtype="datetime64[ns]" if name in {"list_date", "delist_date"} else "string")
        for name in SECURITY_MASTER_FIELDS
    }
    return pd.DataFrame(columns)


def _vendor_date(values: pd.Series) -> tuple[pd.Series, pd.Series]:
    text = values.astype("string").str.strip().str.replace("-", "", regex=False)
    missing = text.isna() | text.eq("")
    parsed = pd.to_datetime(text.mask(missing), format="%Y%m%d", errors="coerce")
    return parsed.dt.normalize(), missing


def _normalize_status_response(
    frame: pd.DataFrame,
    *,
    query_status: str,
    observation_date: str,
) -> pd.DataFrame:
    if query_status not in SECURITY_MASTER_LIST_STATUSES:
        raise ValueError(f"unsupported stock_basic list_status: {query_status!r}")
    if frame.empty:
        if query_status not in SECURITY_MASTER_OPTIONAL_EMPTY_STATUSES:
            raise ValueError(
                f"stock_basic list_status={query_status} returned no rows"
            )
        return _empty_master()

    missing_columns = sorted(set(SECURITY_MASTER_FIELDS) - set(frame.columns))
    if missing_columns:
        raise ValueError(
            f"stock_basic list_status={query_status} missing columns: {missing_columns}"
        )
    work = frame.loc[:, list(SECURITY_MASTER_FIELDS)].copy()
    text_columns = [
        name for name in SECURITY_MASTER_FIELDS if name not in {"list_date", "delist_date"}
    ]
    for name in text_columns:
        work[name] = work[name].astype("string").str.strip().replace("", pd.NA)
    for name in ("ts_code", "exchange", "curr_type", "is_hs"):
        work[name] = work[name].str.upper()

    tickers = work["ts_code"]
    # Tushare retains a small number of historical delisting identifiers with
    # a leading ``T`` (for example ``T600018.SH``).  They are vendor identities,
    # not aliases for the currently listed six-digit security, so preserve them
    # byte-for-byte and permit them only in the independently queried D state.
    ticker_pattern = (
        r"(?:\d{6}|T\d{6})\.(?:SH|SZ|BJ)"
        if query_status == "D"
        else r"\d{6}\.(?:SH|SZ|BJ)"
    )
    if tickers.isna().any() or not tickers.str.fullmatch(
        ticker_pattern, na=False
    ).all():
        raise ValueError("stock_basic response contains an invalid or empty ts_code")
    if work["list_status"].isna().any() or work["list_status"].ne(query_status).any():
        returned = sorted(work["list_status"].dropna().astype(str).unique())
        raise ValueError(
            "stock_basic query/response list_status mismatch: "
            f"queried {query_status}, returned {returned}"
        )

    list_dates, list_missing = _vendor_date(work["list_date"])
    if list_missing.any() or list_dates.isna().any():
        raise ValueError("stock_basic response contains an invalid or empty list_date")
    delist_dates, delist_missing = _vendor_date(work["delist_date"])
    if bool((~delist_missing & delist_dates.isna()).any()):
        raise ValueError("stock_basic response contains an invalid delist_date")
    work["list_date"] = list_dates
    work["delist_date"] = delist_dates

    is_delisted = work["list_status"].eq("D")
    if bool((is_delisted & work["delist_date"].isna()).any()):
        raise ValueError("delisted securities require a valid delist_date")
    if bool(
        (
            is_delisted
            & work["delist_date"].notna()
            & work["delist_date"].lt(work["list_date"])
        ).any()
    ):
        raise ValueError("delist_date must be on or after list_date")

    observed = pd.Timestamp(observation_date).normalize()
    listed_with_past_delist = (
        work["list_status"].eq("L")
        & work["delist_date"].notna()
        & work["delist_date"].lt(observed)
    )
    if bool(listed_with_past_delist.any()):
        raise ValueError("listed securities cannot have a past delist_date")
    return work.loc[:, list(SECURITY_MASTER_FIELDS)]


def normalize_security_master_responses(
    responses: Mapping[str, pd.DataFrame],
    *,
    observation_date: str,
) -> pd.DataFrame:
    """Normalize all three independently queried ``stock_basic`` states."""

    missing = sorted(set(SECURITY_MASTER_LIST_STATUSES) - set(responses))
    extra = sorted(set(responses) - set(SECURITY_MASTER_LIST_STATUSES))
    if missing or extra:
        raise ValueError(
            f"security-master responses require exactly L,D,P; missing={missing}, extra={extra}"
        )
    frames = [
        _normalize_status_response(
            responses[status],
            query_status=status,
            observation_date=observation_date,
        )
        for status in SECURITY_MASTER_LIST_STATUSES
    ]
    combined = pd.concat(frames, ignore_index=True)
    if combined.empty:
        raise ValueError("security master contains no rows")
    if bool(combined["ts_code"].duplicated(keep=False).any()):
        duplicates = sorted(
            combined.loc[
                combined["ts_code"].duplicated(keep=False), "ts_code"
            ].astype(str).unique()
        )
        raise ValueError(f"security master contains duplicate ts_code values: {duplicates}")
    return combined.sort_values("ts_code", kind="mergesort").reset_index(drop=True)[
        list(SECURITY_MASTER_FIELDS)
    ]


def _canonical_scalar(value: Any) -> Any:
    if value is None or bool(pd.isna(value)):
        return None
    if isinstance(value, (pd.Timestamp, datetime)):
        return pd.Timestamp(value).strftime("%Y-%m-%d")
    item = value.item() if hasattr(value, "item") else value
    return item if isinstance(item, (str, int, float, bool)) else str(item)


def security_master_content_sha256(frame: pd.DataFrame) -> str:
    """Hash canonical logical rows, independently of Parquet byte encoding."""

    if list(frame.columns) != list(SECURITY_MASTER_FIELDS):
        raise ValueError("security master has a non-canonical column order")
    records = [
        [_canonical_scalar(value) for value in row]
        for row in frame.itertuples(index=False, name=None)
    ]
    payload = {
        "contract_id": SECURITY_MASTER_CONTRACT_ID,
        "columns": list(SECURITY_MASTER_FIELDS),
        "records": records,
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _read_security_checkpoint(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {
            "schema_version": 1,
            "contract_id": SECURITY_MASTER_CONTRACT_ID,
            "current_snapshot_sha256": None,
            "snapshots": {},
        }
    if path.is_symlink():
        raise ValueError("security-master checkpoint must not be a symlink")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"unreadable security-master checkpoint: {path}") from exc
    if (
        payload.get("schema_version") != 1
        or payload.get("contract_id") != SECURITY_MASTER_CONTRACT_ID
        or not isinstance(payload.get("snapshots"), Mapping)
    ):
        raise ValueError("security-master checkpoint has an invalid contract")
    current = payload.get("current_snapshot_sha256")
    if current is not None and not re.fullmatch(r"[0-9a-f]{64}", str(current)):
        raise ValueError("security-master checkpoint has an invalid current snapshot")
    return payload


def _status_counts(frame: pd.DataFrame) -> dict[str, int]:
    values = frame["list_status"].astype("string")
    return {status: int(values.eq(status).sum()) for status in SECURITY_MASTER_LIST_STATUSES}


def _snapshot_validation(
    layout: RuntimeLayout,
    snapshot_sha256: str,
    entry: Any,
) -> tuple[list[str], dict[str, Any] | None, pd.DataFrame | None]:
    issues: list[str] = []
    if not re.fullmatch(r"[0-9a-f]{64}", str(snapshot_sha256)):
        return ["invalid_snapshot_sha256"], None, None
    directory = security_master_snapshot_path(layout, snapshot_sha256)
    part = directory / "part-000.parquet"
    manifest_path = directory / "manifest.json"
    if not isinstance(entry, Mapping) or entry.get("status") != "complete":
        issues.append("checkpoint_entry_invalid")
        return issues, None, None
    expected_entry = {
        "snapshot_path": directory,
        "parquet_path": part,
        "manifest_path": manifest_path,
    }
    for name, expected in expected_entry.items():
        try:
            actual = Path(str(entry.get(name) or "")).expanduser().resolve()
        except (OSError, RuntimeError, ValueError):
            issues.append(f"checkpoint_{name}_invalid")
            continue
        if actual != expected.expanduser().resolve():
            issues.append(f"checkpoint_{name}_mismatch")
    if entry.get("snapshot_sha256") != snapshot_sha256:
        issues.append("checkpoint_snapshot_sha256_mismatch")
    if directory.is_symlink() or not directory.is_dir():
        issues.append("snapshot_directory_missing_or_symlink")
        return sorted(set(issues)), None, None
    if part.is_symlink() or not part.is_file():
        issues.append("parquet_missing_or_symlink")
    if manifest_path.is_symlink() or not manifest_path.is_file():
        issues.append("manifest_missing_or_symlink")
    try:
        entries = {path.name for path in directory.iterdir()}
        if entries != {"part-000.parquet", "manifest.json"}:
            issues.append("snapshot_directory_has_unexpected_entries")
    except OSError:
        issues.append("snapshot_directory_unreadable")
    if issues:
        return sorted(set(issues)), None, None

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ["manifest_unreadable"], None, None
    if not isinstance(manifest, Mapping):
        return ["manifest_contract_invalid"], None, None
    if sha256_file(manifest_path) != entry.get("manifest_sha256"):
        issues.append("manifest_sha256_mismatch")
    if sha256_file(part) != entry.get("parquet_sha256"):
        issues.append("parquet_sha256_mismatch")
    try:
        frame = pd.read_parquet(part)
    except (OSError, ValueError):
        issues.append("parquet_unreadable")
        return sorted(set(issues)), manifest, None

    if list(frame.columns) != list(SECURITY_MASTER_FIELDS):
        issues.append("parquet_schema_mismatch")
        return sorted(set(issues)), manifest, frame
    if manifest.get("schema_version") != 1:
        issues.append("manifest_schema_version_mismatch")
    if manifest.get("contract_id") != SECURITY_MASTER_CONTRACT_ID:
        issues.append("manifest_contract_id_mismatch")
    if manifest.get("source") != "tushare" or manifest.get("endpoint") != "stock_basic":
        issues.append("manifest_source_mismatch")
    if manifest.get("fields") != list(SECURITY_MASTER_FIELDS):
        issues.append("manifest_fields_mismatch")
    if manifest.get("snapshot_sha256") != snapshot_sha256:
        issues.append("manifest_snapshot_sha256_mismatch")
    if manifest.get("row_count") != len(frame) or entry.get("row_count") != len(frame):
        issues.append("row_count_mismatch")
    file_info = manifest.get("file")
    if not isinstance(file_info, Mapping):
        issues.append("manifest_file_invalid")
    else:
        if file_info.get("path") != "part-000.parquet":
            issues.append("manifest_file_path_mismatch")
        if file_info.get("sha256") != sha256_file(part):
            issues.append("manifest_file_sha256_mismatch")
        if file_info.get("size_bytes") != part.stat().st_size:
            issues.append("manifest_file_size_mismatch")
    observation_date = str(manifest.get("observation_date") or "")
    try:
        parsed_observation = pd.Timestamp(observation_date).normalize()
        if pd.isna(parsed_observation) or parsed_observation.strftime("%Y-%m-%d") != observation_date:
            raise ValueError
    except (TypeError, ValueError):
        issues.append("manifest_observation_date_invalid")
        parsed_observation = None

    queries = manifest.get("queries")
    if not isinstance(queries, Mapping) or set(queries) != set(SECURITY_MASTER_LIST_STATUSES):
        issues.append("manifest_queries_invalid")
    else:
        counts = _status_counts(frame)
        for status in SECURITY_MASTER_LIST_STATUSES:
            query = queries.get(status)
            if not isinstance(query, Mapping) or query.get("list_status") != status:
                issues.append(f"manifest_query_{status}_invalid")
                continue
            if query.get("row_count") != counts[status]:
                issues.append(f"manifest_query_{status}_row_count_mismatch")
            if query.get("empty_allowed") is not (
                status in SECURITY_MASTER_OPTIONAL_EMPTY_STATUSES
            ):
                issues.append(f"manifest_query_{status}_empty_rule_mismatch")
        if manifest.get("status_counts") != counts:
            issues.append("manifest_status_counts_mismatch")

    if parsed_observation is not None:
        try:
            responses = {
                status: frame.loc[frame["list_status"].eq(status)].copy()
                for status in SECURITY_MASTER_LIST_STATUSES
            }
            normalized = normalize_security_master_responses(
                responses, observation_date=observation_date
            )
            if security_master_content_sha256(normalized) != snapshot_sha256:
                issues.append("logical_content_sha256_mismatch")
            if not frame.reset_index(drop=True).equals(normalized):
                # Dtype changes induced by a non-canonical writer are material:
                # the stored artifact itself, not just its values, is the contract.
                issues.append("parquet_not_canonical")
        except (TypeError, ValueError):
            issues.append("parquet_semantics_invalid")
    if entry.get("parquet_size_bytes") != part.stat().st_size:
        issues.append("checkpoint_parquet_size_mismatch")
    if manifest.get("status_counts") != entry.get("status_counts"):
        issues.append("checkpoint_status_counts_mismatch")
    return sorted(set(issues)), manifest, frame


def audit_security_master(
    layout: RuntimeLayout,
    *,
    snapshot_sha256: str | None = None,
) -> dict[str, Any]:
    """Audit checkpoint identity, both file hashes, and all-status semantics."""

    checkpoint_path = security_master_checkpoint_path(layout)
    if not checkpoint_path.is_file():
        return {
            "schema_version": 1,
            "status": "missing",
            "issues": ["checkpoint_missing"],
            "checkpoint_path": str(checkpoint_path),
        }
    try:
        checkpoint = _read_security_checkpoint(checkpoint_path)
    except ValueError as exc:
        return {
            "schema_version": 1,
            "status": "fail",
            "issues": ["checkpoint_invalid"],
            "error": str(exc),
            "checkpoint_path": str(checkpoint_path),
        }
    selected = snapshot_sha256 or checkpoint.get("current_snapshot_sha256")
    if selected is None:
        return {
            "schema_version": 1,
            "status": "fail",
            "issues": ["current_snapshot_missing"],
            "checkpoint_path": str(checkpoint_path),
        }
    snapshots = checkpoint.get("snapshots") or {}
    entry = snapshots.get(selected)
    issues, manifest, _frame = _snapshot_validation(layout, str(selected), entry)
    return {
        "schema_version": 1,
        "status": "pass" if not issues else "fail",
        "issues": issues,
        "checkpoint_path": str(checkpoint_path),
        "snapshot_sha256": str(selected),
        "snapshot_path": str(security_master_snapshot_path(layout, str(selected))),
        "row_count": manifest.get("row_count") if isinstance(manifest, Mapping) else None,
        "status_counts": (
            manifest.get("status_counts") if isinstance(manifest, Mapping) else None
        ),
    }


def load_security_master(
    layout: RuntimeLayout,
    *,
    snapshot_sha256: str | None = None,
    verify: bool = True,
) -> pd.DataFrame:
    """Load the current or named all-status snapshot, hash-verifying by default."""

    checkpoint = _read_security_checkpoint(security_master_checkpoint_path(layout))
    selected = snapshot_sha256 or checkpoint.get("current_snapshot_sha256")
    if selected is None:
        raise ValueError("security-master checkpoint has no current snapshot")
    entry = (checkpoint.get("snapshots") or {}).get(selected)
    if verify:
        issues, _manifest, frame = _snapshot_validation(layout, str(selected), entry)
        if issues or frame is None:
            raise ValueError(f"security-master snapshot audit failed: {issues}")
        return frame.copy()
    path = security_master_snapshot_path(layout, str(selected)) / "part-000.parquet"
    return pd.read_parquet(path)


def _resume_result(layout: RuntimeLayout, snapshot_sha256: str) -> dict[str, Any]:
    loaded = load_security_master(layout, snapshot_sha256=snapshot_sha256)
    return {
        "schema_version": 1,
        "status": "complete",
        "source": "tushare",
        "endpoint": "stock_basic",
        "snapshot_sha256": snapshot_sha256,
        "snapshot_path": str(security_master_snapshot_path(layout, snapshot_sha256)),
        "checkpoint_path": str(security_master_checkpoint_path(layout)),
        "row_count": int(len(loaded)),
        "status_counts": _status_counts(loaded),
        "completed_before": 1,
        "completed_this_run": 0,
        "resumed": True,
    }


def sync_security_master(
    layout: RuntimeLayout | None = None,
    *,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    client: MarketDataClient | Any | None = None,
    resume: bool = True,
) -> dict[str, Any]:
    """Fetch and atomically publish a complete L/D/P security master.

    ``resume=True`` performs no provider calls when the checkpoint's current
    snapshot, manifest, and Parquet bytes all verify.  A corrupted artifact is
    never trusted: the three provider states are fetched again and the
    content-addressed directory is repaired only when their semantics pass.
    Use ``resume=False`` to observe a new provider snapshot; unchanged logical
    content reuses the existing immutable snapshot.
    """

    config = load_data_config(config_path)
    resolved_layout = layout or RuntimeLayout.from_config(config, config_path=config_path)
    resolved_layout.ensure_directories()
    root = security_master_root(resolved_layout)
    checkpoint_path = security_master_checkpoint_path(resolved_layout)
    root.mkdir(parents=True, exist_ok=True)

    with _checkpoint_lock(checkpoint_path):
        checkpoint = _read_security_checkpoint(checkpoint_path)
        current = checkpoint.get("current_snapshot_sha256")
        if resume and current is not None:
            issues, _manifest, _frame = _snapshot_validation(
                resolved_layout,
                str(current),
                (checkpoint.get("snapshots") or {}).get(current),
            )
            if not issues:
                return _resume_result(resolved_layout, str(current))

    resolved_client = client or _configured_tushare_client(
        dict(config.get("sync") or {}), resolved_layout
    )
    observed = _now_utc()
    if observed.tzinfo is None:
        raise ValueError("security-master observation clock must be timezone-aware")
    observed = observed.astimezone(timezone.utc)
    observation_date = observed.date().isoformat()
    responses: dict[str, pd.DataFrame] = {}
    for status in SECURITY_MASTER_LIST_STATUSES:
        responses[status] = _call(
            resolved_client,
            "stock_basic",
            exchange="",
            list_status=status,
            fields=SECURITY_MASTER_FIELD_ARGUMENT,
        )
    frame = normalize_security_master_responses(
        responses, observation_date=observation_date
    )
    content_sha256 = security_master_content_sha256(frame)
    directory = security_master_snapshot_path(resolved_layout, content_sha256)
    part = directory / "part-000.parquet"
    manifest_path = directory / "manifest.json"
    counts = _status_counts(frame)

    with _checkpoint_lock(checkpoint_path):
        latest = _read_security_checkpoint(checkpoint_path)
        latest_current = latest.get("current_snapshot_sha256")
        if resume and latest_current is not None:
            issues, _manifest, _frame = _snapshot_validation(
                resolved_layout,
                str(latest_current),
                (latest.get("snapshots") or {}).get(latest_current),
            )
            if not issues:
                return _resume_result(resolved_layout, str(latest_current))

        existing_entry = (latest.get("snapshots") or {}).get(content_sha256)
        existing_issues, _existing_manifest, _existing_frame = _snapshot_validation(
            resolved_layout, content_sha256, existing_entry
        )
        reused_snapshot = not existing_issues
        if not reused_snapshot:
            _write_parquet_atomic(part, frame)
            manifest = {
                "schema_version": 1,
                "contract_id": SECURITY_MASTER_CONTRACT_ID,
                "source": "tushare",
                "endpoint": "stock_basic",
                "fields": list(SECURITY_MASTER_FIELDS),
                "observed_at_utc": observed.isoformat(),
                "observation_date": observation_date,
                "snapshot_sha256": content_sha256,
                "row_count": int(len(frame)),
                "status_counts": counts,
                "queries": {
                    status: {
                        "list_status": status,
                        "row_count": int(len(responses[status])),
                        "empty_allowed": (
                            status in SECURITY_MASTER_OPTIONAL_EMPTY_STATUSES
                        ),
                    }
                    for status in SECURITY_MASTER_LIST_STATUSES
                },
                "selection_contract": {
                    "all_statuses_queried": list(SECURITY_MASTER_LIST_STATUSES),
                    "includes_historical_delisted": True,
                    "current_listed_only": False,
                    "exchange_filter_applied": False,
                    "currency_filter_applied": False,
                },
                "file": {
                    "path": "part-000.parquet",
                    "size_bytes": int(part.stat().st_size),
                    "sha256": sha256_file(part),
                },
            }
            _write_json_atomic(manifest_path, manifest)

        entry_without_completion = {
            "status": "complete",
            "snapshot_sha256": content_sha256,
            "snapshot_path": str(directory),
            "parquet_path": str(part),
            "parquet_size_bytes": int(part.stat().st_size),
            "parquet_sha256": sha256_file(part),
            "manifest_path": str(manifest_path),
            "manifest_sha256": sha256_file(manifest_path),
            "row_count": int(len(frame)),
            "status_counts": counts,
        }

        def checkpoint_payload(completed_at_utc: str) -> Mapping[str, Any]:
            entry = {
                **entry_without_completion,
                "completed_at_utc": completed_at_utc,
            }
            return {
                **dict(latest),
                "schema_version": 1,
                "contract_id": SECURITY_MASTER_CONTRACT_ID,
                "current_snapshot_sha256": content_sha256,
                "snapshots": {
                    **dict(latest.get("snapshots") or {}),
                    content_sha256: entry,
                },
            }

        published = _write_checkpoint_with_conservative_completion(
            checkpoint_path, checkpoint_payload
        )
        entry = published["snapshots"][content_sha256]
        issues, _manifest, _loaded = _snapshot_validation(
            resolved_layout, content_sha256, entry
        )
        if issues:
            raise ValueError(f"published security-master snapshot failed audit: {issues}")

    return {
        "schema_version": 1,
        "status": "complete",
        "source": "tushare",
        "endpoint": "stock_basic",
        "snapshot_sha256": content_sha256,
        "snapshot_path": str(directory),
        "checkpoint_path": str(checkpoint_path),
        "row_count": int(len(frame)),
        "status_counts": counts,
        "completed_before": 0,
        "completed_this_run": 1,
        "resumed": False,
        "reused_snapshot": reused_snapshot,
    }


__all__: Sequence[str] = (
    "SECURITY_MASTER_CONTRACT_ID",
    "SECURITY_MASTER_FIELDS",
    "SECURITY_MASTER_FIELD_ARGUMENT",
    "SECURITY_MASTER_LIST_STATUSES",
    "SECURITY_MASTER_OPTIONAL_EMPTY_STATUSES",
    "audit_security_master",
    "load_security_master",
    "normalize_security_master_responses",
    "security_master_checkpoint_path",
    "security_master_content_sha256",
    "security_master_root",
    "security_master_snapshot_path",
    "sync_security_master",
)

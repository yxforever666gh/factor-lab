"""Minimal PIT ingestion and pure features for Tushare ``report_rc``."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import time
from typing import Any, Callable, Mapping, Protocol, Sequence
import uuid
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from .catalog import DEFAULT_CONFIG_PATH, RuntimeLayout, load_data_config, sha256_file
from .sources import TushareClient


REPORT_RC_PAGE_LIMIT = 3_000
REPORT_RC_MAX_PAGES = 100
REPORT_RC_PARTITION_SCHEMA = "factor-lab/report-rc-partition/3"
REPORT_RC_TIMEZONE = ZoneInfo("Asia/Shanghai")
REPORT_RC_FIELDS = (
    "ts_code",
    "name",
    "report_date",
    "report_title",
    "report_type",
    "classify",
    "org_name",
    "author_name",
    "quarter",
    "op_rt",
    "op_pr",
    "tp",
    "np",
    "eps",
    "rating",
    "create_time",
)
REPORT_IDENTITY = (
    "ts_code",
    "report_date",
    "report_title",
    "report_type",
    "classify",
    "org_name",
    "author_name",
    "quarter",
)
NUMERIC_FIELDS = ("op_rt", "op_pr", "tp", "np", "eps")
TEXT_FIELDS = tuple(
    name for name in REPORT_RC_FIELDS if name not in {*NUMERIC_FIELDS, "report_date", "create_time"}
)
NORMALIZED_COLUMNS = (*REPORT_RC_FIELDS, "source_row_sha256")
FEATURE_KEYS = ("ts_code", "quarter", "org_name")
TARGET_FISCAL_YEAR_OFFSETS = (0, 1)
FORBIDDEN_FEATURE_COLUMN = re.compile(
    r"(?:^|_)(?:price|return|label|forward)(?:_|$)", re.IGNORECASE
)


class AnalystDataClient(Protocol):
    def query(self, endpoint: str, **kwargs: Any) -> pd.DataFrame: ...


def _vendor_date(values: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(values):
        return pd.to_datetime(values, errors="coerce").dt.normalize()
    text = values.astype("string").str.strip().str.replace("-", "", regex=False)
    parsed = pd.to_datetime(text, format="%Y%m%d", errors="coerce")
    return parsed.dt.normalize()


def _canonical_scalar(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, (np.integer, np.floating)):
        return value.item()
    return value


def _row_hash(row: pd.Series) -> str:
    payload = [_canonical_scalar(row[name]) for name in REPORT_RC_FIELDS]
    encoded = json.dumps(
        payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _page_hash(frame: pd.DataFrame) -> str:
    rows = "\n".join(sorted(frame.get("source_row_sha256", pd.Series(dtype="string"))))
    return hashlib.sha256(rows.encode("ascii")).hexdigest()


def normalize_analyst_reports(
    frame: pd.DataFrame,
    *,
    expected_report_date: str | None = None,
) -> pd.DataFrame:
    """Normalize one response and reject ambiguous duplicate identities."""

    missing = sorted(set(REPORT_RC_FIELDS) - set(frame.columns))
    if missing:
        raise ValueError(f"report_rc response missing columns: {missing}")
    work = frame.loc[:, REPORT_RC_FIELDS].copy()
    if work.empty:
        return pd.DataFrame(columns=NORMALIZED_COLUMNS)
    for name in TEXT_FIELDS:
        work[name] = work[name].astype("string").str.strip()
    work["ts_code"] = work["ts_code"].str.upper()
    if work["ts_code"].isna().any() or not work["ts_code"].str.fullmatch(
        r"\d{6}\.(?:SH|SZ|BJ)"
    ).all():
        raise ValueError("report_rc response contains an invalid ts_code")
    if work["org_name"].isna().any() or work["org_name"].eq("").any():
        raise ValueError("report_rc response contains an empty broker")
    work["report_date"] = _vendor_date(work["report_date"])
    if work["report_date"].isna().any():
        raise ValueError("report_rc response contains an invalid report_date")
    if expected_report_date is not None:
        expected = pd.Timestamp(expected_report_date).normalize()
        if work["report_date"].ne(expected).any():
            raise ValueError("report_rc response contains an unexpected report_date")
    work["quarter"] = work["quarter"].str.upper()
    work.loc[~work["quarter"].str.fullmatch(r"\d{4}Q[1-4]", na=False), "quarter"] = pd.NA
    work["create_time"] = pd.to_datetime(work["create_time"], errors="coerce")
    for name in NUMERIC_FIELDS:
        work[name] = pd.to_numeric(work[name], errors="coerce").astype("float64")
    work["source_row_sha256"] = work.apply(_row_hash, axis=1)
    work = work.drop_duplicates("source_row_sha256", keep="first")
    if work.duplicated(list(REPORT_IDENTITY), keep=False).any():
        raise ValueError("report_rc response contains conflicting duplicate identities")
    return work.sort_values(
        [*REPORT_IDENTITY, "source_row_sha256"], kind="mergesort", na_position="last"
    ).reset_index(drop=True)[list(NORMALIZED_COLUMNS)]


def _open_sessions(calendar: pd.DataFrame) -> pd.DatetimeIndex:
    required = {"cal_date", "is_open"}
    missing = sorted(required - set(calendar.columns))
    if missing:
        raise ValueError(f"trading calendar missing columns: {missing}")
    dates = _vendor_date(calendar["cal_date"])
    numeric = pd.to_numeric(calendar["is_open"], errors="coerce")
    textual = calendar["is_open"].astype("string").str.casefold()
    valid = numeric.isin([0, 1]) | textual.isin(["true", "false"])
    if dates.isna().any() or not valid.all():
        raise ValueError("trading calendar contains invalid values")
    opened = dates[numeric.eq(1) | textual.eq("true")]
    sessions = pd.DatetimeIndex(opened.drop_duplicates().sort_values())
    if sessions.empty:
        raise ValueError("trading calendar has no open sessions")
    return sessions


def assign_report_availability(
    reports: pd.DataFrame,
    trading_calendar: pd.DataFrame,
) -> pd.DataFrame:
    """Assign the first official open session strictly after ``report_date``.

    ``create_time`` is deliberately retained but never read by this function.
    """

    if "report_date" not in reports:
        raise ValueError("reports require report_date")
    work = reports.copy()
    dates = _vendor_date(work["report_date"])
    if dates.isna().any():
        raise ValueError("reports contain invalid report_date values")
    sessions = _open_sessions(trading_calendar)
    positions = sessions.searchsorted(pd.DatetimeIndex(dates), side="right")
    if bool((positions >= len(sessions)).any()):
        raise ValueError("trading calendar does not extend beyond every report_date")
    work["report_date"] = dates
    work["available_session"] = sessions.take(positions).to_numpy()
    return work


def _latest_state(work: pd.DataFrame, session: pd.Timestamp) -> pd.DataFrame:
    eligible = work.loc[work["available_session"].le(session)].copy()
    if eligible.empty:
        return eligible
    eligible = eligible.sort_values(
        [*FEATURE_KEYS, "available_session", "report_date"],
        kind="mergesort",
    )
    return eligible.drop_duplicates(list(FEATURE_KEYS), keep="last")


def _collapse_same_day_broker_states(work: pd.DataFrame) -> pd.DataFrame:
    """Collapse same-broker same-day reports without using vendor lineage.

    Tushare does not expose an original intraday publication timestamp and its
    ``create_time`` may be backfilled years later.  Multiple reports with the
    same economic state date therefore use a frozen median, never a content
    hash or ``create_time`` ordering rule.
    """

    group_keys = [*FEATURE_KEYS, "available_session", "report_date"]
    records: list[dict[str, Any]] = []
    for keys, group in work.groupby(group_keys, dropna=False, sort=True):
        record = dict(zip(group_keys, keys))
        for metric in ("eps", "np"):
            values = pd.to_numeric(group[metric], errors="coerce").dropna()
            record[metric] = float(values.median()) if not values.empty else np.nan
        lineage = "\n".join(sorted(group["source_row_sha256"].astype(str)))
        record["source_row_sha256"] = hashlib.sha256(
            lineage.encode("ascii")
        ).hexdigest()
        records.append(record)
    return pd.DataFrame(records, columns=[*group_keys, "eps", "np", "source_row_sha256"])


def _feature_output_columns(horizons: Sequence[int]) -> list[str]:
    columns = [
        "signal_date",
        "ts_code",
        "quarter",
        "fiscal_horizon",
        "eps_broker_count",
        "np_broker_count",
    ]
    for horizon in horizons:
        for metric in ("eps", "np"):
            columns.extend(
                [
                    f"{metric}_consensus_revision_{horizon}d",
                    f"{metric}_revision_breadth_{horizon}d",
                    f"{metric}_paired_brokers_{horizon}d",
                ]
            )
    return columns


def _symmetric_change(current: float, previous: float) -> float:
    return 2.0 * (current - previous) / (abs(current) + abs(previous) + 1e-12)


def build_analyst_revision_features(
    reports: pd.DataFrame,
    trading_calendar: pd.DataFrame,
    *,
    signal_sessions: Sequence[str | pd.Timestamp] | None = None,
    horizons: Sequence[int] = (20, 60),
    min_brokers: int = 3,
) -> pd.DataFrame:
    """Build PIT consensus revisions without reading any price or return data."""

    forbidden = sorted(name for name in reports if FORBIDDEN_FEATURE_COLUMN.search(name))
    if forbidden:
        raise ValueError(f"analyst features reject price/return/label columns: {forbidden}")
    required = {
        "ts_code",
        "quarter",
        "org_name",
        "report_date",
        "available_session",
        "source_row_sha256",
        "eps",
        "np",
    }
    missing = sorted(required - set(reports.columns))
    if missing:
        raise ValueError(f"analyst reports missing feature columns: {missing}")
    if min_brokers < 1:
        raise ValueError("min_brokers must be positive")
    resolved_horizons = tuple(sorted({int(value) for value in horizons}))
    if not resolved_horizons or resolved_horizons[0] <= 0:
        raise ValueError("horizons must contain positive trading-session counts")

    sessions = _open_sessions(trading_calendar)
    signals = (
        pd.DatetimeIndex(pd.to_datetime(list(signal_sessions))).normalize()
        if signal_sessions is not None
        else pd.DatetimeIndex(pd.to_datetime(reports["available_session"].dropna().unique()))
        .normalize()
        .sort_values()
    )
    if not signals.isin(sessions).all():
        raise ValueError("every signal session must be an official open session")
    work = reports.loc[reports["quarter"].notna()].copy()
    work["report_date"] = pd.to_datetime(work["report_date"], errors="coerce").dt.normalize()
    work["available_session"] = pd.to_datetime(
        work["available_session"], errors="coerce"
    ).dt.normalize()
    if work[["report_date", "available_session"]].isna().any().any():
        raise ValueError("analyst reports contain invalid PIT dates")
    source_hashes = work["source_row_sha256"].astype("string")
    if source_hashes.isna().any() or not source_hashes.str.fullmatch(
        r"[0-9a-f]{64}"
    ).all():
        raise ValueError("analyst reports contain invalid source row hashes")
    expected_positions = sessions.searchsorted(
        pd.DatetimeIndex(work["report_date"]), side="right"
    )
    if bool((expected_positions >= len(sessions)).any()):
        raise ValueError("trading calendar does not extend beyond every report_date")
    expected_availability = sessions.take(expected_positions)
    if not np.array_equal(
        work["available_session"].to_numpy(dtype="datetime64[ns]"),
        expected_availability.to_numpy(dtype="datetime64[ns]"),
    ):
        raise ValueError(
            "available_session must be the first official open session strictly "
            "after report_date"
        )
    for name in ("eps", "np"):
        work[name] = pd.to_numeric(work[name], errors="coerce")
    work = _collapse_same_day_broker_states(work)

    outputs: list[pd.DataFrame] = []
    for signal in signals:
        current = _latest_state(work, signal)
        fiscal_quarters = {
            f"{signal.year + offset}Q4": f"FY{offset}"
            for offset in TARGET_FISCAL_YEAR_OFFSETS
        }
        current = current.loc[current["quarter"].isin(fiscal_quarters)].copy()
        if current.empty:
            continue
        base = current[["ts_code", "quarter"]].drop_duplicates().copy()
        base.insert(0, "signal_date", signal)
        base["fiscal_horizon"] = base["quarter"].map(fiscal_quarters)
        for metric in ("eps", "np"):
            counts = current.groupby(["ts_code", "quarter"])[metric].count()
            base = base.merge(
                counts.rename(f"{metric}_broker_count").reset_index(),
                on=["ts_code", "quarter"],
                how="left",
            )
        signal_position = int(sessions.get_loc(signal))
        for horizon in resolved_horizons:
            for metric in ("eps", "np"):
                base[f"{metric}_consensus_revision_{horizon}d"] = np.nan
                base[f"{metric}_revision_breadth_{horizon}d"] = np.nan
                base[f"{metric}_paired_brokers_{horizon}d"] = np.nan
            if signal_position < horizon:
                continue
            previous = _latest_state(work, sessions[signal_position - horizon])
            for metric in ("eps", "np"):
                paired = current[[*FEATURE_KEYS, metric]].merge(
                    previous[[*FEATURE_KEYS, metric]],
                    on=list(FEATURE_KEYS),
                    how="inner",
                    suffixes=("_new", "_old"),
                ).dropna(subset=[f"{metric}_new", f"{metric}_old"])
                records: list[dict[str, Any]] = []
                for (ticker, quarter), group in paired.groupby(["ts_code", "quarter"]):
                    count = len(group)
                    record: dict[str, Any] = {
                        "ts_code": ticker,
                        "quarter": quarter,
                        f"{metric}_paired_brokers_{horizon}d": float(count),
                        f"{metric}_consensus_revision_{horizon}d": np.nan,
                        f"{metric}_revision_breadth_{horizon}d": np.nan,
                    }
                    if count >= min_brokers:
                        new = float(group[f"{metric}_new"].median())
                        old = float(group[f"{metric}_old"].median())
                        deltas = group[f"{metric}_new"] - group[f"{metric}_old"]
                        record[f"{metric}_consensus_revision_{horizon}d"] = (
                            _symmetric_change(new, old)
                        )
                        record[f"{metric}_revision_breadth_{horizon}d"] = float(
                            np.sign(deltas).sum() / count
                        )
                    records.append(record)
                if records:
                    updates = pd.DataFrame(records)
                    columns = [
                        f"{metric}_paired_brokers_{horizon}d",
                        f"{metric}_consensus_revision_{horizon}d",
                        f"{metric}_revision_breadth_{horizon}d",
                    ]
                    base = base.drop(columns=columns).merge(
                        updates, on=["ts_code", "quarter"], how="left"
                    )
        outputs.append(base)
    if not outputs:
        return pd.DataFrame(columns=_feature_output_columns(resolved_horizons))
    return pd.concat(outputs, ignore_index=True)[
        _feature_output_columns(resolved_horizons)
    ].sort_values(
        ["signal_date", "ts_code", "quarter"], kind="mergesort"
    ).reset_index(drop=True)


def _configured_client(config: Mapping[str, Any], layout: RuntimeLayout) -> TushareClient:
    sync = dict(config.get("sync") or {})
    token: str | None = None
    configured = str(sync.get("token_file") or "").strip()
    if configured:
        path = Path(configured).expanduser()
        path = path if path.is_absolute() else layout.repo_root / path
        if path.is_file():
            token = path.read_text(encoding="utf-8").strip() or None
    return TushareClient(token=token, token_env=str(sync.get("token_env") or "TUSHARE_TOKEN"))


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _valid_partition(directory: Path, report_date: str) -> bool:
    if directory.is_symlink() or not directory.is_dir():
        return False
    part = directory / "part-000.parquet"
    manifest = directory / "manifest.json"
    page_root = directory / "pages"
    if not part.is_file() or not manifest.is_file() or part.is_symlink() or manifest.is_symlink():
        return False
    if page_root.is_symlink() or not page_root.is_dir():
        return False
    try:
        payload = json.loads(manifest.read_text(encoding="utf-8"))
        pages = payload.get("pages")
        if not bool(
            payload.get("schema_version") == REPORT_RC_PARTITION_SCHEMA
            and payload.get("endpoint") == "report_rc"
            and payload.get("fields") == list(REPORT_RC_FIELDS)
            and payload.get("report_date") == report_date
            and payload.get("page_limit") == REPORT_RC_PAGE_LIMIT
            and isinstance(pages, list)
            and payload.get("page_count") == len(pages)
            and 1 <= len(pages) <= REPORT_RC_MAX_PAGES
            and payload.get("provider_row_count")
            == sum(page["row_count"] for page in pages)
        ):
            return False
        expected_page_files: set[str] = set()
        page_frames: list[pd.DataFrame] = []
        observed_row_hashes: set[str] = set()
        for index, page in enumerate(pages):
            relative_page = f"pages/page-{index:03d}.parquet"
            if not bool(
                page.get("page_index") == index
                and page.get("offset") == index * REPORT_RC_PAGE_LIMIT
                and isinstance(page.get("row_count"), int)
                and 0 <= page["row_count"] <= REPORT_RC_PAGE_LIMIT
                and isinstance(page.get("normalized_row_count"), int)
                and 0 <= page["normalized_row_count"] <= page["row_count"]
                and page.get("path") == relative_page
                and re.fullmatch(
                    r"[0-9a-f]{64}", str(page.get("content_sha256"))
                )
                and re.fullmatch(
                    r"[0-9a-f]{64}", str(page.get("file_sha256"))
                )
            ):
                return False
            page_path = directory / relative_page
            expected_page_files.add(page_path.name)
            if not page_path.is_file() or page_path.is_symlink():
                return False
            if sha256_file(page_path) != page["file_sha256"]:
                return False
            page_frame = pd.read_parquet(page_path)
            if list(page_frame.columns) != list(NORMALIZED_COLUMNS):
                return False
            if len(page_frame) != page["normalized_row_count"]:
                return False
            if _page_hash(page_frame) != page["content_sha256"]:
                return False
            normalized_page = normalize_analyst_reports(
                page_frame, expected_report_date=report_date
            )
            if _page_hash(normalized_page) != page["content_sha256"]:
                return False
            hashes = set(normalized_page["source_row_sha256"].astype(str))
            if observed_row_hashes.intersection(hashes):
                return False
            observed_row_hashes.update(hashes)
            page_frames.append(normalized_page)
        if any(page["row_count"] != REPORT_RC_PAGE_LIMIT for page in pages[:-1]):
            return False
        if pages[-1]["row_count"] >= REPORT_RC_PAGE_LIMIT:
            return False
        page_entries = list(page_root.iterdir())
        if any(not path.is_file() or path.is_symlink() for path in page_entries):
            return False
        if {path.name for path in page_entries} != expected_page_files:
            return False
        combined_pages = [frame for frame in page_frames if not frame.empty]
        combined = (
            normalize_analyst_reports(
                pd.concat(combined_pages, ignore_index=True),
                expected_report_date=report_date,
            )
            if combined_pages
            else pd.DataFrame(columns=NORMALIZED_COLUMNS)
        )
        final_frame = pd.read_parquet(part)
        if list(final_frame.columns) != list(NORMALIZED_COLUMNS):
            return False
        normalized_final = normalize_analyst_reports(
            final_frame, expected_report_date=report_date
        )
        return bool(
            payload.get("row_count") == len(final_frame) == len(combined)
            and payload.get("sha256") == sha256_file(part)
            and _page_hash(normalized_final) == _page_hash(combined)
        )
    except (OSError, TypeError, ValueError, KeyError):
        return False


def _publish_partition(
    dataset_root: Path,
    report_date: str,
    frame: pd.DataFrame,
    retrieved_at_utc: str,
    pages: Sequence[Mapping[str, Any]],
    page_frames: Sequence[pd.DataFrame],
) -> None:
    final = dataset_root / f"report_date={report_date}"
    staging = dataset_root / f".report_date={report_date}.{uuid.uuid4().hex}.tmp"
    staging.mkdir(parents=True)
    try:
        if len(pages) != len(page_frames):
            raise ValueError("page manifest and page frame counts differ")
        part = staging / "part-000.parquet"
        frame.to_parquet(part, index=False)
        with part.open("r+b") as handle:
            os.fsync(handle.fileno())
        page_root = staging / "pages"
        page_root.mkdir()
        published_pages: list[dict[str, Any]] = []
        for index, (page, page_frame) in enumerate(zip(pages, page_frames)):
            page_path = page_root / f"page-{index:03d}.parquet"
            page_frame.to_parquet(page_path, index=False)
            with page_path.open("r+b") as handle:
                os.fsync(handle.fileno())
            published_pages.append(
                {
                    **dict(page),
                    "normalized_row_count": len(page_frame),
                    "path": f"pages/{page_path.name}",
                    "file_sha256": sha256_file(page_path),
                }
            )
        manifest = {
            "schema_version": REPORT_RC_PARTITION_SCHEMA,
            "endpoint": "report_rc",
            "report_date": report_date,
            "retrieved_at_utc": retrieved_at_utc,
            "fields": list(REPORT_RC_FIELDS),
            "row_count": len(frame),
            "sha256": sha256_file(part),
            "page_limit": REPORT_RC_PAGE_LIMIT,
            "page_count": len(published_pages),
            "provider_row_count": sum(
                int(page["row_count"]) for page in published_pages
            ),
            "pages": published_pages,
        }
        manifest_path = staging / "manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        with manifest_path.open("r+b") as handle:
            os.fsync(handle.fileno())
        staging.replace(final)
    finally:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)


def sync_analyst_reports(
    start_date: str,
    end_date: str,
    *,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    raw_root: str | Path | None = None,
    client: AnalystDataClient | None = None,
    request_rate_per_minute: float = 1.0,
    resume: bool = True,
    sleep_fn: Callable[[float], None] = time.sleep,
    monotonic_fn: Callable[[], float] = time.monotonic,
    now_utc_fn: Callable[[], datetime] = _now_utc,
    max_pages_per_date: int = REPORT_RC_MAX_PAGES,
) -> dict[str, Any]:
    """Synchronize immutable daily partitions using verified limit/offset pages."""

    start, end = pd.Timestamp(start_date).normalize(), pd.Timestamp(end_date).normalize()
    if pd.isna(start) or pd.isna(end) or start > end:
        raise ValueError("start_date and end_date must form a valid interval")
    if request_rate_per_minute < 0:
        raise ValueError("request_rate_per_minute cannot be negative")
    if not 1 <= max_pages_per_date <= REPORT_RC_MAX_PAGES:
        raise ValueError(
            f"max_pages_per_date must be between 1 and {REPORT_RC_MAX_PAGES}"
        )
    config = load_data_config(config_path)
    layout = RuntimeLayout.from_config(config, config_path=config_path)
    resolved_root = Path(raw_root).expanduser().resolve() if raw_root else layout.raw_root
    dataset_root = resolved_root / "report_rc"
    dataset_root.mkdir(parents=True, exist_ok=True)
    resolved_client = client or _configured_client(config, layout)
    dates = [value.date().isoformat() for value in pd.date_range(start, end, freq="D")]
    completed_before = completed_this_run = 0
    last_request: float | None = None
    minimum_interval = 60.0 / request_rate_per_minute if request_rate_per_minute else 0.0

    for report_date in dates:
        observed_now = now_utc_fn()
        if observed_now.tzinfo is None:
            raise ValueError("now_utc_fn must return a timezone-aware datetime")
        shanghai_today = observed_now.astimezone(REPORT_RC_TIMEZONE).date()
        if pd.Timestamp(report_date).date() >= shanghai_today:
            return {
                "status": "blocked",
                "reason": "report_date_not_mature",
                "report_date": report_date,
                "maturity_rule": "report_date_before_current_Asia_Shanghai_date",
                "observed_at_utc": observed_now.astimezone(timezone.utc).isoformat(),
                "completed_before": completed_before,
                "completed_this_run": completed_this_run,
            }
        directory = dataset_root / f"report_date={report_date}"
        if directory.exists():
            if resume and _valid_partition(directory, report_date):
                completed_before += 1
                continue
            return {
                "status": "blocked",
                "reason": "local_partition_conflict",
                "report_date": report_date,
                "completed_before": completed_before,
                "completed_this_run": completed_this_run,
            }
        page_frames: list[pd.DataFrame] = []
        page_manifest: list[dict[str, Any]] = []
        page_hashes: set[str] = set()
        source_row_hashes: set[str] = set()
        pagination_complete = False
        for page_index in range(max_pages_per_date):
            if last_request is not None and minimum_interval:
                remaining = minimum_interval - (monotonic_fn() - last_request)
                if remaining > 0:
                    sleep_fn(remaining)
            offset = page_index * REPORT_RC_PAGE_LIMIT
            try:
                response = resolved_client.query(
                    "report_rc",
                    report_date=report_date.replace("-", ""),
                    fields=",".join(REPORT_RC_FIELDS),
                    limit=REPORT_RC_PAGE_LIMIT,
                    offset=offset,
                )
            except Exception as exc:
                return {
                    "status": "blocked",
                    "reason": (
                        "provider_pagination_unsupported"
                        if isinstance(exc, TypeError)
                        else "provider_request_failed"
                    ),
                    "report_date": report_date,
                    "page_index": page_index,
                    "offset": offset,
                    "error_type": type(exc).__name__,
                    "completed_before": completed_before,
                    "completed_this_run": completed_this_run,
                }
            last_request = monotonic_fn()
            if not isinstance(response, pd.DataFrame):
                return {
                    "status": "blocked",
                    "reason": "provider_response_not_dataframe",
                    "report_date": report_date,
                    "page_index": page_index,
                }
            if len(response) > REPORT_RC_PAGE_LIMIT:
                return {
                    "status": "blocked",
                    "reason": "provider_page_exceeds_documented_limit",
                    "report_date": report_date,
                    "page_index": page_index,
                    "row_count": len(response),
                }
            try:
                normalized_page = (
                    normalize_analyst_reports(response, expected_report_date=report_date)
                    if len(response)
                    else pd.DataFrame(columns=NORMALIZED_COLUMNS)
                )
            except (TypeError, ValueError) as exc:
                return {
                    "status": "blocked",
                    "reason": "provider_schema_invalid",
                    "report_date": report_date,
                    "page_index": page_index,
                    "error_type": type(exc).__name__,
                }
            content_sha256 = _page_hash(normalized_page)
            if content_sha256 in page_hashes:
                return {
                    "status": "blocked",
                    "reason": "provider_repeated_page",
                    "report_date": report_date,
                    "page_index": page_index,
                    "offset": offset,
                }
            current_row_hashes = set(
                normalized_page["source_row_sha256"].astype(str)
            )
            if source_row_hashes.intersection(current_row_hashes):
                return {
                    "status": "blocked",
                    "reason": "provider_cross_page_duplicate",
                    "report_date": report_date,
                    "page_index": page_index,
                    "offset": offset,
                }
            page_hashes.add(content_sha256)
            source_row_hashes.update(current_row_hashes)
            page_frames.append(normalized_page)
            page_manifest.append(
                {
                    "page_index": page_index,
                    "offset": offset,
                    "row_count": len(response),
                    "content_sha256": content_sha256,
                }
            )
            if len(response) < REPORT_RC_PAGE_LIMIT:
                pagination_complete = True
                break
        if not pagination_complete:
            return {
                "status": "blocked",
                "reason": "provider_pagination_max_pages",
                "report_date": report_date,
                "page_count": len(page_manifest),
            }
        try:
            nonempty_pages = [frame for frame in page_frames if not frame.empty]
            combined = (
                pd.concat(nonempty_pages, ignore_index=True)
                if nonempty_pages
                else pd.DataFrame(columns=NORMALIZED_COLUMNS)
            )
            normalized = normalize_analyst_reports(
                combined,
                expected_report_date=report_date,
            )
        except (TypeError, ValueError) as exc:
            return {
                "status": "blocked",
                "reason": (
                    "provider_cross_page_identity_conflict"
                    if "conflicting duplicate identities" in str(exc)
                    else "provider_schema_invalid"
                ),
                "report_date": report_date,
                "error_type": type(exc).__name__,
                "completed_before": completed_before,
                "completed_this_run": completed_this_run,
            }
        retrieved_at = now_utc_fn().astimezone(timezone.utc).isoformat()
        _publish_partition(
            dataset_root,
            report_date,
            normalized,
            retrieved_at,
            page_manifest,
            page_frames,
        )
        completed_this_run += 1
    return {
        "status": "complete",
        "dataset": "report_rc",
        "start_date": start.date().isoformat(),
        "end_date": end.date().isoformat(),
        "partition_count": len(dates),
        "completed_before": completed_before,
        "completed_this_run": completed_this_run,
        "remaining_partition_count": 0,
    }

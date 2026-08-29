"""Small, resumable raw-market synchronisation adapters."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, time as wall_time, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import time
from typing import Any, Callable, Iterator, Mapping, Protocol, Sequence
import uuid
from zoneinfo import ZoneInfo

import pandas as pd

from .catalog import DEFAULT_CONFIG_PATH, RuntimeLayout, load_data_config, sha256_file


DATASET_FIELDS = {
    "daily": "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount",
    "daily_basic": (
        "ts_code,trade_date,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,"
        "ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv"
    ),
    "adj_factor": "ts_code,trade_date,adj_factor",
}

# Prospective raw partitions after the frozen bridge are not considered
# provider-complete merely because Tushare returned a non-empty frame.  The
# documented endpoint windows end no later than 17:00 Asia/Shanghai for this
# bundle, but that is not a provider completeness guarantee.  17:10 is a
# protocol-chosen conservative observation gate, followed by independent
# stable sampling and cross-endpoint universe checks.
PROVIDER_COMPLETION_CONTRACT_ID = (
    "factor-lab/tushare-eod-stable-bundle/1"
)
PROVIDER_COMPLETION_CUTOVER_DATE = "2026-08-21"
PROVIDER_COMPLETION_TIMEZONE = "Asia/Shanghai"
PROVIDER_COMPLETION_NOT_BEFORE_LOCAL_TIME = "17:10:00"
PROVIDER_COMPLETION_MINIMUM_SAMPLES = 2
PROVIDER_COMPLETION_DATASETS = ("daily", "daily_basic", "adj_factor")

# These endpoints use a different partition axis from the daily market files:
# financial indicators are fetched once per report quarter, while the two
# reference tables are fetched at the exact ``as_of_date`` already frozen in
# the monthly Top-500 membership file.
ENRICHMENT_DATASET_FIELDS = {
    "fina_indicator_vip": (
        "ts_code,ann_date,end_date,update_flag,eps,bps,ocfps,roe,roe_dt,roa,roic,"
        "ocf_to_profit,q_ocf_to_sales,ocf_to_debt,grossprofit_margin,netprofit_margin,"
        "debt_to_assets,current_ratio,q_sales_yoy,q_netprofit_yoy,dt_netprofit_yoy,"
        "or_yoy,ocf_yoy"
    ),
    "bak_basic": "trade_date,ts_code,name,industry,list_date",
    "stock_st": "ts_code,name,trade_date,type,type_name",
}
EXACT_REFERENCE_CONTRACT_ID = "factor-lab/exact-bak-basic-raw/1"

AMOUNT_TO_RMB_MULTIPLIERS = {
    "tushare_daily": 1000.0,
    "akshare": 1.0,
    "turnover_estimate_rmb": 1.0,
}


def turnover_amount_to_rmb(values: pd.Series, *, source: str) -> pd.Series:
    """Normalize a vendor turnover-amount column to RMB explicitly.

    Tushare ``daily.amount`` is reported in thousands of RMB.  AkShare
    turnover amount and the ``circ_mv * turnover_rate`` fallback are already
    RMB.  Keeping this conversion source-aware prevents a repeat of the
    retired builder's blanket x1000 bug.
    """

    if source not in AMOUNT_TO_RMB_MULTIPLIERS:
        raise ValueError(f"unknown turnover amount source: {source}")
    return pd.to_numeric(values, errors="coerce") * AMOUNT_TO_RMB_MULTIPLIERS[source]


class MarketDataClient(Protocol):
    def query(self, endpoint: str, **kwargs: Any) -> pd.DataFrame: ...


class TushareClient:
    """Lazy Tushare Pro wrapper so status/audit commands need no data extra."""

    def __init__(self, token: str | None = None, *, token_env: str = "TUSHARE_TOKEN") -> None:
        resolved_token = token or os.environ.get(token_env)
        if not resolved_token:
            raise RuntimeError(f"missing Tushare token in {token_env}")
        try:
            import tushare as ts
        except ImportError as exc:  # pragma: no cover - depends on optional extra
            raise RuntimeError("Tushare support requires the data optional dependency") from exc
        self._pro = ts.pro_api(resolved_token)

    def query(self, endpoint: str, **kwargs: Any) -> pd.DataFrame:
        target = getattr(self._pro, endpoint, None)
        value = target(**kwargs) if callable(target) else self._pro.query(endpoint, **kwargs)
        if value is None:
            return pd.DataFrame()
        if not isinstance(value, pd.DataFrame):
            raise TypeError(f"Tushare endpoint {endpoint!r} did not return a DataFrame")
        return value.copy()


def _configured_tushare_client(
    sync_config: Mapping[str, Any],
    layout: RuntimeLayout,
) -> TushareClient:
    token: str | None = None
    configured_file = str(sync_config.get("token_file") or "").strip()
    if configured_file:
        token_path = Path(configured_file).expanduser()
        if not token_path.is_absolute():
            token_path = layout.repo_root / token_path
        if token_path.is_file():
            token = token_path.read_text(encoding="utf-8").strip() or None
    return TushareClient(
        token=token,
        token_env=str(sync_config.get("token_env") or "TUSHARE_TOKEN"),
    )


def _call(client: Any, endpoint: str, **kwargs: Any) -> pd.DataFrame:
    target = getattr(client, endpoint, None)
    if callable(target):
        value = target(**kwargs)
    else:
        query = getattr(client, "query", None)
        if not callable(query):
            raise TypeError(f"data client has no {endpoint!r} endpoint or query method")
        value = query(endpoint, **kwargs)
    if value is None:
        return pd.DataFrame()
    if not isinstance(value, pd.DataFrame):
        raise TypeError(f"data endpoint {endpoint!r} did not return a DataFrame")
    return value.copy()


def _date(value: str) -> str:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(value)):
        raise ValueError(f"date must use YYYY-MM-DD: {value!r}")
    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        raise ValueError(f"invalid date: {value!r}")
    return timestamp.strftime("%Y-%m-%d")


def _compact(value: str) -> str:
    return _date(value).replace("-", "")


def _now_utc() -> datetime:
    """Return the local observation clock used by provider sampling."""

    return datetime.now(timezone.utc)


def provider_completion_required(trade_date: str) -> bool:
    """Whether a raw session is inside the prospective provider-guard era."""

    return _date(trade_date) > PROVIDER_COMPLETION_CUTOVER_DATE


def provider_completion_not_before_utc(trade_date: str) -> datetime:
    """Return the protocol observation gate for one Shanghai session.

    This timestamp is an engineering policy boundary, not a claim that the
    provider guarantees complete data at or after this instant.
    """

    local = datetime.combine(
        pd.Timestamp(_date(trade_date)).date(),
        wall_time.fromisoformat(PROVIDER_COMPLETION_NOT_BEFORE_LOCAL_TIME),
        tzinfo=ZoneInfo(PROVIDER_COMPLETION_TIMEZONE),
    )
    return local.astimezone(timezone.utc)


def _provider_reconciling_entry_valid(
    entry: Any,
    *,
    dataset: str,
    trade_date: str,
    expected_path: Path,
) -> bool:
    """Validate the durable pre-publish marker used for crash recovery."""

    if not isinstance(entry, Mapping) or entry.get("status") != "reconciling":
        return False
    try:
        recorded_path = Path(str(entry.get("path") or "")).expanduser().resolve()
    except (OSError, RuntimeError, ValueError):
        return False
    reconciliation = entry.get("reconciliation")
    if not isinstance(reconciliation, Mapping):
        return False
    attempts = reconciliation.get("attempts")
    resume_count = reconciliation.get("resume_count")
    target_bundle = str(entry.get("target_bundle_content_sha256") or "")
    if (
        entry.get("dataset") != dataset
        or entry.get("trade_date") != _date(trade_date)
        or recorded_path != expected_path.expanduser().resolve()
        or entry.get("reconcile_contract_id")
        != PROVIDER_COMPLETION_CONTRACT_ID
        or reconciliation.get("contract_id")
        != PROVIDER_COMPLETION_CONTRACT_ID
        or not re.fullmatch(r"[0-9a-f]{64}", target_bundle)
        or type(resume_count) is not int
        or int(resume_count) < 0
        or not isinstance(attempts, list)
        or len(attempts) != int(resume_count) + 1
        or not all(isinstance(value, Mapping) for value in attempts)
    ):
        return False
    if any(
        value.get("target_bundle_content_sha256") != target_bundle
        or not re.fullmatch(
            r"[0-9a-f]{64}",
            str(value.get("published_evidence_sha256") or ""),
        )
        for value in attempts
    ):
        return False
    try:
        started_values = [
            _completed_at_utc(
                value.get("started_at_utc"),
                label="provider reconciliation attempt started_at_utc",
            )
            for value in attempts
        ]
    except ValueError:
        return False
    return (
        started_values == sorted(started_values)
        and reconciliation.get("started_at_utc")
        == attempts[0].get("started_at_utc")
        and entry.get("reconcile_started_at_utc")
        == reconciliation.get("started_at_utc")
    )


def _partition_path(raw_root: Path, dataset: str, trade_date: str) -> Path:
    return raw_root / dataset / f"trade_date={_date(trade_date)}" / "part-000.parquet"


def enrichment_partition_path(raw_root: Path, dataset: str, value: str) -> Path:
    """Return the canonical raw path for one enrichment partition."""

    if dataset not in ENRICHMENT_DATASET_FIELDS:
        raise ValueError(f"unsupported enrichment dataset: {dataset}")
    axis = "period" if dataset == "fina_indicator_vip" else "trade_date"
    return raw_root / dataset / f"{axis}={_date(value)}" / "part-000.parquet"


def _read_checkpoint(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"schema_version": 1, "partitions": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"unreadable data checkpoint: {path}") from exc
    if not isinstance(payload.get("partitions"), Mapping):
        raise ValueError("data checkpoint partitions must be a mapping")
    return payload


def _fsync_directory(directory: Path) -> None:
    """Durably publish a directory entry when the platform supports it.

    POSIX filesystems are expected to support directory ``fsync`` and failures
    remain fatal.  Windows does not expose a portable directory handle through
    ``os.open``; only that platform has the deliberately bounded compatibility
    fallback.
    """

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(directory, flags)
    except OSError:
        if os.name == "nt":
            return
        raise
    try:
        try:
            os.fsync(descriptor)
        except OSError:
            if os.name != "nt":
                raise
    finally:
        os.close(descriptor)


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp"
    )
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        with temporary.open("r+b") as handle:
            os.fsync(handle.fileno())
        temporary.replace(path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _write_parquet_atomic(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp"
    )
    try:
        frame.to_parquet(temporary, index=False)
        with temporary.open("r+b") as handle:
            os.fsync(handle.fileno())
        temporary.replace(path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


@contextmanager
def _checkpoint_lock(
    checkpoint_path: Path, *, timeout_seconds: float = 15.0
) -> Iterator[None]:
    """Serialize checkpoint read/merge/write cycles across processes.

    The reference synchronizer is intentionally independent of the historical
    enrichment planner.  Both may nevertheless target the same checkpoint, so
    a byte-range lock prevents a concurrent exact capture from losing an
    already-published partition entry.
    """

    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = checkpoint_path.with_suffix(checkpoint_path.suffix + ".lock")
    descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        if os.fstat(descriptor).st_size == 0:
            os.write(descriptor, b"\0")
            os.fsync(descriptor)
        started = time.monotonic()
        if os.name == "nt":
            import msvcrt

            while True:
                try:
                    os.lseek(descriptor, 0, os.SEEK_SET)
                    msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
                    break
                except OSError:
                    if time.monotonic() - started >= timeout_seconds:
                        raise TimeoutError(
                            f"timed out acquiring data checkpoint lock: {lock_path}"
                        )
                    time.sleep(0.05)
            try:
                yield
            finally:
                os.lseek(descriptor, 0, os.SEEK_SET)
                msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            while True:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() - started >= timeout_seconds:
                        raise TimeoutError(
                            f"timed out acquiring data checkpoint lock: {lock_path}"
                        )
                    time.sleep(0.05)
            try:
                yield
            finally:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


@contextmanager
def _raw_reference_checkpoint_locks(
    raw_checkpoint_path: Path,
    reference_checkpoint_path: Path,
) -> Iterator[None]:
    """Lock exact-reference inputs in the one permitted cross-file order."""

    raw = raw_checkpoint_path.expanduser().resolve()
    reference = reference_checkpoint_path.expanduser().resolve()
    with _checkpoint_lock(raw):
        if reference == raw:
            yield
        else:
            with _checkpoint_lock(reference):
                yield


def _canonical_json_bytes(payload: Any) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _checkpoint_value_sha256(value: Any) -> str:
    """Return a stable CAS token for one checkpoint value, including null."""

    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _write_checkpoint_with_conservative_completion(
    checkpoint_path: Path,
    payload_factory: Callable[[str], Mapping[str, Any]],
    *,
    minimum_completion_utc: datetime | None = None,
) -> dict[str, Any]:
    """Publish a checkpoint whose completion time bounds the commit from above.

    The candidate timestamp is rounded into the future.  If atomic replace and
    directory durability cross that candidate, the checkpoint is immediately
    rewritten with a wider bound.  A successful writer waits through the bound
    before returning, so its own strict validation never observes future-dated
    evidence.
    """

    base_quantum_ns = 10_000_000
    attempt = 0
    while True:
        quantum_ns = base_quantum_ns * (2 ** min(attempt, 11))
        now_ns = time.time_ns()
        candidate_ns = ((now_ns // quantum_ns) + 2) * quantum_ns
        seconds, nanoseconds = divmod(candidate_ns, 1_000_000_000)
        candidate = datetime.fromtimestamp(seconds, timezone.utc).replace(
            microsecond=nanoseconds // 1_000
        )
        if (
            minimum_completion_utc is not None
            and candidate < minimum_completion_utc.astimezone(timezone.utc)
        ):
            raise ValueError(
                "checkpoint completion clock precedes its provider evidence"
            )
        completed_at = candidate.isoformat()
        payload = dict(payload_factory(completed_at))
        _write_json_atomic(checkpoint_path, payload)
        published_at = datetime.now(timezone.utc)
        if published_at <= candidate:
            while True:
                remaining = (candidate - datetime.now(timezone.utc)).total_seconds()
                if remaining <= 0:
                    return payload
                time.sleep(min(remaining, 0.05))
        attempt += 1


def _normalise_trade_calendar(
    calendar: pd.DataFrame,
    *,
    exchange: str,
    start_date: str,
    end_date: str,
) -> tuple[pd.DataFrame, list[dict[str, Any]], str]:
    """Return one complete, deterministic official-calendar interval.

    The raw API response is deliberately not trusted as a clock. Availability
    is recorded separately in the checkpoint when this normalised artifact is
    persisted.
    """

    required = {"cal_date", "is_open"}
    missing = sorted(required - set(calendar.columns))
    if missing:
        raise ValueError(f"trade_cal response missing columns: {missing}")
    work = calendar.copy()
    work["cal_date"] = pd.to_datetime(
        work["cal_date"].astype("string").str.replace("-", "", regex=False),
        format="%Y%m%d",
        errors="coerce",
    )
    work = work.loc[
        work["cal_date"].between(pd.Timestamp(start_date), pd.Timestamp(end_date))
    ].copy()
    if work.empty or work["cal_date"].isna().any():
        raise ValueError("trade calendar contains invalid or no in-range dates")
    if bool(work.duplicated("cal_date").any()):
        raise ValueError("trade calendar contains duplicate calendar dates")
    expected_dates = pd.date_range(start_date, end_date, freq="D")
    actual_dates = pd.DatetimeIndex(work["cal_date"].sort_values())
    if not actual_dates.equals(expected_dates):
        raise ValueError("trade calendar does not cover every requested calendar date")

    numeric_open = pd.to_numeric(work["is_open"], errors="coerce")
    textual_open = work["is_open"].astype("string").str.strip().str.casefold()
    valid_open = numeric_open.isin([0, 1]) | textual_open.isin(["false", "true"])
    if not bool(valid_open.all()):
        raise ValueError("trade calendar contains invalid is_open values")
    work["is_open"] = numeric_open.eq(1) | textual_open.eq("true")
    if "exchange" not in work:
        work["exchange"] = exchange
    work["exchange"] = work["exchange"].astype("string").fillna(exchange).str.strip()
    if bool(work["exchange"].ne(exchange).any()):
        raise ValueError("trade calendar contains an unexpected exchange")
    if "pretrade_date" not in work:
        work["pretrade_date"] = pd.NaT
    work["pretrade_date"] = pd.to_datetime(
        work["pretrade_date"].astype("string").str.replace("-", "", regex=False),
        format="%Y%m%d",
        errors="coerce",
    )
    work = work[["exchange", "cal_date", "is_open", "pretrade_date"]].sort_values(
        "cal_date", kind="mergesort"
    )
    work = work.reset_index(drop=True)
    records = [
        {
            "cal_date": value.cal_date.date().isoformat(),
            "exchange": str(value.exchange),
            "is_open": bool(value.is_open),
            "pretrade_date": (
                value.pretrade_date.date().isoformat()
                if not pd.isna(value.pretrade_date)
                else None
            ),
        }
        for value in work.itertuples(index=False)
    ]
    content_sha256 = hashlib.sha256(_canonical_json_bytes(records)).hexdigest()
    return work, records, content_sha256


def _persist_trade_calendar(
    calendar: pd.DataFrame,
    *,
    raw_root: Path,
    checkpoint_path: Path,
    baseline_calendars: Mapping[str, Any],
    resume: bool,
    exchange: str,
    start_date: str,
    end_date: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Persist an append-only, content-addressed official calendar artifact."""

    normalised, records, content_sha256 = _normalise_trade_calendar(
        calendar,
        exchange=exchange,
        start_date=start_date,
        end_date=end_date,
    )
    artifact_dir = raw_root / "trade_cal" / f"calendar_sha256={content_sha256}"
    artifact_path = artifact_dir / "part-000.parquet"
    manifest_path = artifact_dir / "manifest.json"
    with _checkpoint_lock(checkpoint_path):
        latest = _read_checkpoint(checkpoint_path)
        calendars = dict(latest.get("calendars") or {})
        prior = calendars.get(content_sha256)
        prior_valid = (
            isinstance(prior, Mapping)
            and prior.get("status") == "complete"
            and artifact_path.is_file()
            and str(prior.get("artifact_sha256") or "")
            == sha256_file(artifact_path)
            and str(prior.get("calendar_content_sha256") or "")
            == content_sha256
            and str(prior.get("completed_at_utc") or "").strip() != ""
        )
        baseline = baseline_calendars.get(content_sha256)
        changed_since_start = _checkpoint_value_sha256(
            prior
        ) != _checkpoint_value_sha256(baseline)
        if resume and prior_valid:
            return dict(latest), dict(prior)
        elif not resume and changed_since_start:
            if not prior_valid:
                raise ValueError(
                    "trade calendar checkpoint changed during no-resume refresh"
                )
            return dict(latest), dict(prior)

        _write_parquet_atomic(artifact_path, normalised)
        entry_without_completion = {
            "status": "complete",
            "exchange": exchange,
            "start_date": start_date,
            "end_date": end_date,
            "row_count": int(len(normalised)),
            "open_day_count": int(normalised["is_open"].sum()),
            "path": str(artifact_path),
            "artifact_sha256": sha256_file(artifact_path),
            "calendar_content_sha256": content_sha256,
        }
        records_sha256 = hashlib.sha256(
            _canonical_json_bytes(records)
        ).hexdigest()

        def calendar_checkpoint_payload(
            completed_at_utc: str,
        ) -> Mapping[str, Any]:
            published_entry = {
                **entry_without_completion,
                "completed_at_utc": completed_at_utc,
            }
            _write_json_atomic(
                manifest_path,
                {
                    "schema_version": 1,
                    **published_entry,
                    "records_sha256": records_sha256,
                },
            )
            published_entry["manifest_path"] = str(manifest_path)
            published_entry["manifest_sha256"] = sha256_file(manifest_path)
            return {
                **dict(latest),
                "schema_version": int(latest.get("schema_version") or 1),
                "partitions": dict(latest.get("partitions") or {}),
                "calendars": {
                    **calendars,
                    content_sha256: published_entry,
                },
            }

        updated = _write_checkpoint_with_conservative_completion(
            checkpoint_path,
            calendar_checkpoint_payload,
        )
        entry = dict(updated["calendars"][content_sha256])
    return updated, entry


def _checkpoint_entry_is_valid(entry: Any, path: Path, *, verify_hash: bool) -> bool:
    if (
        not isinstance(entry, Mapping)
        or entry.get("status") != "complete"
        or not path.is_file()
        or path.is_symlink()
    ):
        return False
    try:
        recorded_path = Path(str(entry.get("path") or "")).expanduser().resolve()
    except (OSError, RuntimeError, ValueError):
        return False
    if recorded_path != path.expanduser().resolve():
        return False
    if int(entry.get("row_count") or 0) <= 0 or int(entry.get("size_bytes") or -1) != path.stat().st_size:
        return False
    expected_hash = str(entry.get("sha256") or "")
    if not re.fullmatch(r"[0-9a-f]{64}", expected_hash):
        return False
    return not verify_hash or sha256_file(path) == expected_hash


def _audit_partition(frame: pd.DataFrame, dataset: str, trade_date: str) -> None:
    required = {field.strip() for field in DATASET_FIELDS[dataset].split(",") if field.strip()}
    missing = sorted(required - set(frame.columns))
    if frame.empty:
        raise ValueError(f"{dataset}/{trade_date} returned no rows")
    if missing:
        raise ValueError(f"{dataset}/{trade_date} missing columns: {missing}")
    compact_dates = frame["trade_date"].astype("string").str.replace("-", "", regex=False)
    dates = pd.to_datetime(compact_dates, format="%Y%m%d", errors="coerce").dt.strftime(
        "%Y-%m-%d"
    )
    if dates.isna().any() or bool((dates != trade_date).any()):
        raise ValueError(f"{dataset}/{trade_date} contains mismatched trade dates")
    if bool(frame.duplicated(["ts_code", "trade_date"]).any()):
        raise ValueError(f"{dataset}/{trade_date} contains duplicate securities")


def _canonical_sample_scalar(value: Any) -> Any:
    if value is None or bool(pd.isna(value)):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    item = value.item() if hasattr(value, "item") else value
    if isinstance(item, float):
        if not math.isfinite(item):
            raise ValueError("provider sample contains a non-finite number")
        return float(item)
    if isinstance(item, (bool, int, str)):
        return item
    return str(item)


def provider_partition_fingerprint(
    frame: pd.DataFrame,
    dataset: str,
    trade_date: str,
) -> dict[str, Any]:
    """Canonical content/ticker evidence for one provider response.

    Row order and provider column order do not affect the digest.  All returned
    columns are bound, while downstream dataset-specific schema checks remain
    responsible for their stronger numeric contracts.
    """

    if dataset not in DATASET_FIELDS:
        raise ValueError(f"unsupported provider-completion dataset: {dataset}")
    _audit_partition(frame, dataset, _date(trade_date))
    work = frame.copy()
    work["ts_code"] = work["ts_code"].astype("string").str.strip()
    if work["ts_code"].isna().any() or work["ts_code"].eq("").any():
        raise ValueError(f"{dataset}/{trade_date} contains blank securities")
    parsed_dates = pd.to_datetime(
        work["trade_date"].astype("string").str.replace("-", "", regex=False),
        format="%Y%m%d",
        errors="coerce",
    )
    if parsed_dates.isna().any():
        raise ValueError(f"{dataset}/{trade_date} contains invalid trade dates")
    work["trade_date"] = parsed_dates.dt.strftime("%Y%m%d")
    columns = sorted(str(column) for column in work.columns)
    work = work.loc[:, columns].sort_values(
        ["ts_code", "trade_date"], kind="mergesort"
    ).reset_index(drop=True)
    records = [
        {
            column: _canonical_sample_scalar(value)
            for column, value in zip(columns, row, strict=True)
        }
        for row in work.itertuples(index=False, name=None)
    ]
    tickers = sorted(work["ts_code"].astype(str).tolist())
    return {
        "content_sha256": hashlib.sha256(
            _canonical_json_bytes(records)
        ).hexdigest(),
        "row_count": len(records),
        "ticker_count": len(tickers),
        "ticker_set_sha256": hashlib.sha256(
            _canonical_json_bytes(tickers)
        ).hexdigest(),
        "tickers": tickers,
    }


def _build_provider_completion_evidence(
    trade_date: str,
    samples: Sequence[Mapping[str, pd.DataFrame]],
    *,
    observations: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Build auditable stable-sampling evidence for one endpoint bundle."""

    date_text = _date(trade_date)
    if len(samples) < PROVIDER_COMPLETION_MINIMUM_SAMPLES:
        raise ValueError("provider completion requires at least two samples")
    dataset_set = tuple(sorted(samples[0]))
    if dataset_set != tuple(sorted(PROVIDER_COMPLETION_DATASETS)):
        raise ValueError("provider completion sample dataset set is invalid")
    if any(tuple(sorted(sample)) != dataset_set for sample in samples):
        raise ValueError("provider completion samples use different dataset sets")
    not_before = provider_completion_not_before_utc(date_text)
    if len(observations) != len(samples):
        raise ValueError("provider sample observations differ from sample count")

    sample_rows: list[dict[str, Any]] = []
    ticker_sets: list[dict[str, set[str]]] = []
    request_ids: set[str] = set()
    previous_completed: datetime | None = None
    for index, (sample, observation) in enumerate(
        zip(samples, observations, strict=True), start=1
    ):
        if not isinstance(observation, Mapping):
            raise ValueError("provider_sample_observation_invalid")
        request_id = str(observation.get("request_id") or "")
        try:
            started = _completed_at_utc(
                observation.get("request_started_at_utc"),
                label="provider sample request_started_at_utc",
            )
            completed = _completed_at_utc(
                observation.get("response_completed_at_utc"),
                label="provider sample response_completed_at_utc",
            )
        except ValueError as exc:
            raise ValueError("provider_sample_observation_invalid") from exc
        if (
            not request_id
            or request_id in request_ids
            or started < not_before
            or completed < started
            or (
                previous_completed is not None
                and started < previous_completed
            )
        ):
            raise ValueError("provider_sample_observation_invalid")
        request_ids.add(request_id)
        previous_completed = completed
        datasets: dict[str, dict[str, Any]] = {}
        sets: dict[str, set[str]] = {}
        for dataset in dataset_set:
            fingerprint = provider_partition_fingerprint(
                sample[dataset], dataset, date_text
            )
            if dataset in {"daily", "daily_basic"} and int(
                fingerprint["row_count"]
            ) >= 6_000:
                raise ValueError("provider_response_at_row_limit")
            sets[dataset] = set(fingerprint.pop("tickers"))
            datasets[dataset] = fingerprint
        sample_rows.append(
            {
                "sample_index": index,
                "request_id": request_id,
                "request_started_at_utc": started.isoformat(),
                "response_completed_at_utc": completed.isoformat(),
                "datasets": datasets,
            }
        )
        ticker_sets.append(sets)

    canonical_first = sample_rows[0]["datasets"]
    if any(row["datasets"] != canonical_first for row in sample_rows[1:]):
        raise ValueError("provider_response_unstable")
    daily_equals_basic = (
        "daily" not in dataset_set
        or "daily_basic" not in dataset_set
        or ticker_sets[0]["daily"] == ticker_sets[0]["daily_basic"]
    )
    daily_subset_adj = (
        "daily" not in dataset_set
        or "adj_factor" not in dataset_set
        or ticker_sets[0]["daily"].issubset(ticker_sets[0]["adj_factor"])
    )
    if not daily_equals_basic or not daily_subset_adj:
        raise ValueError("provider_universe_incomplete")
    bundle_payload = {
        "trade_date": date_text,
        "datasets": canonical_first,
    }
    completed_values = [
        _completed_at_utc(
            row["response_completed_at_utc"],
            label="provider sample response_completed_at_utc",
        )
        for row in sample_rows
    ]
    evidence = {
        "contract_id": PROVIDER_COMPLETION_CONTRACT_ID,
        "source": "tushare",
        "trade_date": date_text,
        "timezone": PROVIDER_COMPLETION_TIMEZONE,
        "not_before_local_time": PROVIDER_COMPLETION_NOT_BEFORE_LOCAL_TIME,
        "not_before_utc": not_before.isoformat(),
        "not_before_basis": (
            "protocol_chosen_conservative_gate_not_provider_completeness_sla"
        ),
        "sample_count": len(sample_rows),
        "minimum_sample_count": PROVIDER_COMPLETION_MINIMUM_SAMPLES,
        "datasets": list(dataset_set),
        "bundle_content_sha256": hashlib.sha256(
            _canonical_json_bytes(bundle_payload)
        ).hexdigest(),
        "stable_at_utc": max(completed_values).isoformat(),
        "relations": {
            "daily_equals_daily_basic": daily_equals_basic,
            "daily_subset_of_adj_factor": daily_subset_adj,
        },
        "samples": sample_rows,
    }
    evidence["evidence_sha256"] = hashlib.sha256(
        _canonical_json_bytes(evidence)
    ).hexdigest()
    return evidence


def validate_provider_completion_evidence(
    entry: Mapping[str, Any],
    frame: pd.DataFrame,
    *,
    dataset: str,
    trade_date: str,
    required_datasets: Sequence[str],
) -> dict[str, Any] | None:
    """Validate one checkpoint/sealed source against its stable bundle proof.

    Historical frozen-bridge inputs remain compatible and return ``None``.
    Every later partition must bind the exact canonical bytes to at least two
    independently recorded samples from the required endpoint bundle.
    """

    date_text = _date(trade_date)
    if not provider_completion_required(date_text):
        return None
    if (
        entry.get("dataset") != dataset
        or entry.get("trade_date") != date_text
        or type(entry.get("row_count")) is not int
        or int(entry["row_count"]) != len(frame)
    ):
        raise ValueError("provider completion checkpoint identity is invalid")
    if "provider_completion" not in entry:
        raise ValueError("provider completion evidence is missing")
    evidence = entry["provider_completion"]
    if not isinstance(evidence, Mapping):
        raise ValueError("provider completion evidence is malformed")
    required = set(required_datasets)
    datasets = evidence.get("datasets")
    samples = evidence.get("samples")
    if (
        evidence.get("contract_id") != PROVIDER_COMPLETION_CONTRACT_ID
        or evidence.get("source") != "tushare"
        or evidence.get("trade_date") != date_text
        or evidence.get("timezone") != PROVIDER_COMPLETION_TIMEZONE
        or evidence.get("not_before_local_time")
        != PROVIDER_COMPLETION_NOT_BEFORE_LOCAL_TIME
        or evidence.get("not_before_basis")
        != "protocol_chosen_conservative_gate_not_provider_completeness_sla"
        or type(evidence.get("sample_count")) is not int
        or int(evidence["sample_count"]) < PROVIDER_COMPLETION_MINIMUM_SAMPLES
        or evidence.get("minimum_sample_count")
        != PROVIDER_COMPLETION_MINIMUM_SAMPLES
        or not isinstance(datasets, list)
        or datasets != sorted(set(str(value) for value in datasets))
        or set(datasets) != required
        or not isinstance(samples, list)
        or len(samples) != evidence["sample_count"]
    ):
        raise ValueError("provider completion evidence identity is invalid")
    evidence_without_sha = {
        key: value for key, value in evidence.items() if key != "evidence_sha256"
    }
    if evidence.get("evidence_sha256") != hashlib.sha256(
        _canonical_json_bytes(evidence_without_sha)
    ).hexdigest():
        raise ValueError("provider completion evidence digest is invalid")
    not_before = provider_completion_not_before_utc(date_text)
    recorded_not_before = _completed_at_utc(
        evidence.get("not_before_utc"), label="provider completion not_before_utc"
    )
    if recorded_not_before != not_before:
        raise ValueError("provider completion gate differs from protocol")
    request_ids: set[str] = set()
    first_datasets: Mapping[str, Any] | None = None
    response_times: list[datetime] = []
    previous_completed: datetime | None = None
    for index, row in enumerate(samples, start=1):
        if not isinstance(row, Mapping) or row.get("sample_index") != index:
            raise ValueError("provider completion samples are not canonical")
        request_id = str(row.get("request_id") or "")
        if not request_id or request_id in request_ids:
            raise ValueError("provider completion requests are not independent")
        request_ids.add(request_id)
        started = _completed_at_utc(
            row.get("request_started_at_utc"),
            label="provider sample request_started_at_utc",
        )
        completed = _completed_at_utc(
            row.get("response_completed_at_utc"),
            label="provider sample response_completed_at_utc",
        )
        if (
            started < not_before
            or completed < started
            or (
                previous_completed is not None
                and started < previous_completed
            )
        ):
            raise ValueError("provider sample precedes completion gate")
        previous_completed = completed
        response_times.append(completed)
        row_datasets = row.get("datasets")
        if not isinstance(row_datasets, Mapping) or set(row_datasets) != set(datasets):
            raise ValueError("provider sample dataset evidence is invalid")
        if first_datasets is None:
            first_datasets = row_datasets
        elif row_datasets != first_datasets:
            raise ValueError("provider response samples are unstable")
    assert first_datasets is not None
    fingerprint = provider_partition_fingerprint(frame, dataset, date_text)
    fingerprint.pop("tickers")
    if dataset in {"daily", "daily_basic"} and int(
        fingerprint["row_count"]
    ) >= 6_000:
        raise ValueError("provider response reached ambiguous row limit")
    if first_datasets.get(dataset) != fingerprint:
        raise ValueError("partition differs from provider completion sample")
    expected_bundle = hashlib.sha256(
        _canonical_json_bytes(
            {"trade_date": date_text, "datasets": dict(first_datasets)}
        )
    ).hexdigest()
    if evidence.get("bundle_content_sha256") != expected_bundle:
        raise ValueError("provider completion bundle digest is invalid")
    relations = evidence.get("relations")
    if not isinstance(relations, Mapping):
        raise ValueError("provider completion universe relations are missing")
    if {"daily", "daily_basic"}.issubset(required) and (
        relations.get("daily_equals_daily_basic") is not True
        or first_datasets["daily"].get("ticker_set_sha256")
        != first_datasets["daily_basic"].get("ticker_set_sha256")
        or first_datasets["daily"].get("ticker_count")
        != first_datasets["daily_basic"].get("ticker_count")
    ):
        raise ValueError("daily and daily_basic provider universes differ")
    if {"daily", "adj_factor"}.issubset(required) and (
        relations.get("daily_subset_of_adj_factor") is not True
    ):
        raise ValueError("daily provider universe is not covered by adj_factor")
    stable = _completed_at_utc(
        evidence.get("stable_at_utc"), label="provider completion stable_at_utc"
    )
    if stable != max(response_times):
        raise ValueError("provider completion stable timestamp is invalid")
    published = _completed_at_utc(
        entry.get("completed_at_utc"), label="partition completed_at_utc"
    )
    if published < stable:
        raise ValueError("partition completion precedes stable provider evidence")
    return dict(evidence)


def audit_enrichment_partition(frame: pd.DataFrame, dataset: str, value: str) -> None:
    """Fail closed on malformed quarterly or month-end source responses."""

    if dataset not in ENRICHMENT_DATASET_FIELDS:
        raise ValueError(f"unsupported enrichment dataset: {dataset}")
    partition_date = _date(value)
    required = {
        field.strip()
        for field in ENRICHMENT_DATASET_FIELDS[dataset].split(",")
        if field.strip()
    }
    missing = sorted(required - set(frame.columns))
    if frame.empty:
        raise ValueError(f"{dataset}/{partition_date} returned no rows")
    if missing:
        raise ValueError(f"{dataset}/{partition_date} missing columns: {missing}")
    if frame["ts_code"].isna().any() or frame["ts_code"].astype("string").str.strip().eq("").any():
        raise ValueError(f"{dataset}/{partition_date} contains blank securities")

    date_column = "end_date" if dataset == "fina_indicator_vip" else "trade_date"
    dates = pd.to_datetime(
        frame[date_column].astype("string").str.replace("-", "", regex=False),
        format="%Y%m%d",
        errors="coerce",
    ).dt.strftime("%Y-%m-%d")
    if dates.isna().any() or bool((dates != partition_date).any()):
        raise ValueError(f"{dataset}/{partition_date} contains mismatched {date_column}")

    if dataset == "fina_indicator_vip":
        announcements = pd.to_datetime(
            frame["ann_date"].astype("string").str.replace("-", "", regex=False),
            format="%Y%m%d",
            errors="coerce",
        )
        report_dates = pd.to_datetime(dates, errors="coerce")
        if announcements.isna().any() or bool((announcements < report_dates).any()):
            raise ValueError(
                f"{dataset}/{partition_date} contains invalid announcement dates"
            )
        return

    if bool(frame.duplicated(["ts_code", "trade_date"]).any()):
        raise ValueError(f"{dataset}/{partition_date} contains duplicate securities")


def _quarantine_early_financial_announcements(
    frame: pd.DataFrame,
    *,
    maximum_ratio: float = 0.001,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Separate impossible pre-period announcements from an otherwise valid partition.

    Tushare occasionally returns a stale ``update_flag=0`` row whose
    ``ann_date`` precedes the requested report period.  Such a row can never
    be made point-in-time safe.  It is retained as raw quarantine evidence,
    while a material vendor-wide defect still fails closed.
    """

    if not {"ann_date", "end_date"}.issubset(frame.columns) or frame.empty:
        return frame, frame.iloc[0:0].copy()
    announcements = pd.to_datetime(
        frame["ann_date"].astype("string").str.replace("-", "", regex=False),
        format="%Y%m%d",
        errors="coerce",
    )
    periods = pd.to_datetime(
        frame["end_date"].astype("string").str.replace("-", "", regex=False),
        format="%Y%m%d",
        errors="coerce",
    )
    early = announcements.notna() & periods.notna() & announcements.lt(periods)
    quarantined = frame.loc[early].copy()
    if len(quarantined) / len(frame) > maximum_ratio:
        raise ValueError(
            "fina_indicator_vip early-announcement quarantine exceeds "
            f"{maximum_ratio:.2%}: {len(quarantined)}/{len(frame)}"
        )
    return frame.loc[~early].copy(), quarantined


def _quarter_partitions(
    start_date: str,
    end_date: str,
    *,
    lookback_quarters: int,
) -> list[str]:
    start_period = pd.Timestamp(start_date).to_period("Q") - max(0, int(lookback_quarters))
    end_timestamp = pd.Timestamp(end_date)
    periods = pd.period_range(start=start_period, end=end_timestamp.to_period("Q"), freq="Q")
    return [
        period.end_time.normalize().strftime("%Y-%m-%d")
        for period in periods
        if period.end_time.normalize() <= end_timestamp
    ]


def _membership_as_of_dates(
    membership_path: Path,
    start_date: str,
    end_date: str,
) -> list[str]:
    if not membership_path.is_file():
        raise FileNotFoundError(f"missing membership file: {membership_path}")
    membership = pd.read_parquet(
        membership_path,
        columns=[
            "membership_month",
            "as_of_date",
            "effective_start_date",
            "effective_end_date",
        ],
    )
    required = {
        "membership_month",
        "as_of_date",
        "effective_start_date",
        "effective_end_date",
    }
    if not required.issubset(membership.columns):
        raise ValueError("membership file lacks month-end as-of columns")
    starts = pd.to_datetime(membership["effective_start_date"], errors="coerce")
    ends = pd.to_datetime(membership["effective_end_date"], errors="coerce")
    as_of = pd.to_datetime(membership["as_of_date"], errors="coerce")
    if starts.isna().any() or ends.isna().any() or as_of.isna().any():
        raise ValueError("membership contains invalid as-of/effective dates")
    selected = membership.loc[
        ends.ge(pd.Timestamp(start_date)) & starts.le(pd.Timestamp(end_date))
    ].copy()
    if selected.empty:
        raise ValueError("membership has no months overlapping the requested range")
    month_counts = selected.groupby("membership_month", observed=True)["as_of_date"].nunique()
    if bool(month_counts.ne(1).any()):
        raise ValueError("each membership month must have exactly one as_of_date")
    return sorted(as_of.loc[selected.index].dt.strftime("%Y-%m-%d").unique().tolist())


def sync_enrichment(
    start_date: str,
    end_date: str,
    *,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    layout: RuntimeLayout | None = None,
    client: MarketDataClient | Any | None = None,
    datasets: Sequence[str] | None = None,
    resume: bool = True,
    max_partitions: int | None = None,
    financial_lookback_quarters: int = 2,
) -> dict[str, Any]:
    """Resume quarterly financial and membership month-end reference pulls.

    ``start_date`` and ``end_date`` bound the effective membership months.  Two
    earlier report quarters are included by default so the first research date
    can see the latest report that had already been announced.
    """

    start = _date(start_date)
    end = _date(end_date)
    if end < start:
        raise ValueError("end_date must be on or after start_date")
    config = load_data_config(config_path)
    resolved_layout = layout or RuntimeLayout.from_config(config, config_path=config_path)
    resolved_layout.ensure_directories()
    enrichment_config = dict(config.get("enrichment") or {})
    fundamentals_config = dict(config.get("fundamentals") or enrichment_config)
    reference_config = dict(config.get("reference_snapshots") or enrichment_config)
    sync_config = dict(config.get("sync") or {})
    # ``stock_st`` is intentionally opt-in: many otherwise valid Tushare
    # accounts cannot call it.  ``bak_basic.name`` remains a conservative,
    # auditable monthly ST-name fallback in the enrichment builder.
    selected_datasets = tuple(
        datasets
        or enrichment_config.get("datasets")
        or ("fina_indicator_vip", "bak_basic")
    )
    unknown = sorted(set(selected_datasets) - set(ENRICHMENT_DATASET_FIELDS))
    if unknown:
        raise ValueError(f"unsupported enrichment datasets: {unknown}")
    resolved_client = client or _configured_tushare_client(sync_config, resolved_layout)
    checkpoint_paths = {
        "fina_indicator_vip": resolved_layout.raw_root
        / str(
            fundamentals_config.get("checkpoint_file")
            or "fundamentals-checkpoint.json"
        ),
        "bak_basic": resolved_layout.raw_root
        / str(
            reference_config.get("checkpoint_file")
            or "reference-snapshots-checkpoint.json"
        ),
        "stock_st": resolved_layout.raw_root
        / str(
            reference_config.get("checkpoint_file")
            or "reference-snapshots-checkpoint.json"
        ),
    }
    checkpoint_payloads: dict[Path, dict[str, Any]] = {}
    for path in sorted(set(checkpoint_paths.values()), key=str):
        with _checkpoint_lock(path):
            checkpoint_payloads[path] = _read_checkpoint(path)
    entries_by_path = {
        path: dict(payload.get("partitions") or {})
        for path, payload in checkpoint_payloads.items()
    }

    configured_start_period = str(fundamentals_config.get("start_period") or "").strip()
    if configured_start_period:
        parsed_start_period = pd.to_datetime(
            configured_start_period.replace("-", ""), format="%Y%m%d", errors="coerce"
        )
        if (
            pd.isna(parsed_start_period)
            or parsed_start_period
            != parsed_start_period.to_period("Q").end_time.normalize()
        ):
            raise ValueError("fundamentals.start_period must be a calendar quarter end")
        quarter_dates = [
            period.end_time.normalize().strftime("%Y-%m-%d")
            for period in pd.period_range(
                start=parsed_start_period.to_period("Q"),
                end=pd.Timestamp(end).to_period("Q"),
                freq="Q",
            )
            if period.end_time.normalize() <= pd.Timestamp(end)
        ]
    else:
        quarter_dates = _quarter_partitions(
            start,
            end,
            lookback_quarters=int(
                enrichment_config.get(
                    "financial_lookback_quarters", financial_lookback_quarters
                )
            ),
        )
    as_of_dates = _membership_as_of_dates(resolved_layout.membership_path, start, end)
    planned: list[tuple[str, str, Path, str, Path]] = []
    for dataset in selected_datasets:
        values = quarter_dates if dataset == "fina_indicator_vip" else as_of_dates
        axis = "period" if dataset == "fina_indicator_vip" else "trade_date"
        for value in values:
            path = enrichment_partition_path(resolved_layout.raw_root, dataset, value)
            planned.append(
                (
                    dataset,
                    value,
                    path,
                    f"{dataset}/{axis}={value}",
                    checkpoint_paths[dataset],
                )
            )

    pending: list[tuple[str, str, Path, str, Path, str]] = []
    completed_before = 0
    for dataset, value, path, key, checkpoint_path in planned:
        dataset_config = (
            fundamentals_config if dataset == "fina_indicator_vip" else reference_config
        )
        verify_hash = bool(
            dataset_config.get(
                "verify_hashes_on_resume",
                sync_config.get("verify_hashes_on_resume", True),
            )
        )
        if resume and _checkpoint_entry_is_valid(
            entries_by_path[checkpoint_path].get(key),
            path,
            verify_hash=verify_hash,
        ):
            completed_before += 1
        else:
            pending.append(
                (
                    dataset,
                    value,
                    path,
                    key,
                    checkpoint_path,
                    _checkpoint_value_sha256(
                        entries_by_path[checkpoint_path].get(key)
                    ),
                )
            )
    requested_count = (
        len(pending)
        if max_partitions is None
        else min(len(pending), max(0, int(max_partitions)))
    )
    completed_now = 0
    for (
        dataset,
        value,
        path,
        key,
        checkpoint_path,
        baseline_sha256,
    ) in pending[:requested_count]:
        argument = "period" if dataset == "fina_indicator_vip" else "trade_date"
        source_value = value
        frame = _call(
            resolved_client,
            dataset,
            **{
                argument: _compact(value),
                "fields": ENRICHMENT_DATASET_FIELDS[dataset],
            },
        )
        # The historical bak_basic archive has isolated missing trading days.
        # A prior snapshot is PIT-safe; a later one is not.  Search a bounded
        # calendar window backwards and preserve the vendor date explicitly.
        if dataset == "bak_basic" and frame.empty:
            for lag_days in range(1, 11):
                fallback = pd.Timestamp(value) - pd.Timedelta(days=lag_days)
                source_value = fallback.strftime("%Y-%m-%d")
                frame = _call(
                    resolved_client,
                    dataset,
                    trade_date=_compact(source_value),
                    fields=ENRICHMENT_DATASET_FIELDS[dataset],
                )
                if not frame.empty:
                    break
        if dataset == "bak_basic" and not frame.empty:
            frame = frame.copy()
            frame["source_trade_date"] = frame["trade_date"]
            frame["trade_date"] = _compact(value)
        quarantine_path: Path | None = None
        candidate_quarantine: Path | None = None
        quarantined = frame.iloc[0:0].copy()
        if dataset == "fina_indicator_vip":
            frame, quarantined = _quarantine_early_financial_announcements(frame)
            candidate_quarantine = path.with_name("part-000.quarantine.parquet")
            if not quarantined.empty:
                quarantine_path = candidate_quarantine
        audit_enrichment_partition(frame, dataset, value)
        with _checkpoint_lock(checkpoint_path):
            latest = _read_checkpoint(checkpoint_path)
            entries = dict(latest.get("partitions") or {})
            concurrent = entries.get(key)
            concurrent_valid = _checkpoint_entry_is_valid(
                concurrent, path, verify_hash=True
            )
            changed_since_start = (
                _checkpoint_value_sha256(concurrent) != baseline_sha256
            )
            publish = not (resume and concurrent_valid)
            if not resume and changed_since_start:
                if not concurrent_valid:
                    raise ValueError(
                        f"{key} checkpoint changed during no-resume refresh"
                    )
                publish = False
            if publish:
                if candidate_quarantine is not None:
                    if quarantine_path is not None:
                        _write_parquet_atomic(quarantine_path, quarantined)
                    elif candidate_quarantine.exists():
                        candidate_quarantine.unlink()
                        _fsync_directory(candidate_quarantine.parent)
                _write_parquet_atomic(path, frame)
                entry_without_completion = {
                    "status": "complete",
                    "dataset": dataset,
                    argument: value,
                    "source_trade_date": (
                        source_value if dataset == "bak_basic" else None
                    ),
                    "path": str(path),
                    "row_count": int(len(frame)),
                    "size_bytes": int(path.stat().st_size),
                    "sha256": sha256_file(path),
                    "quarantine_row_count": int(len(quarantined)),
                    "quarantine_path": (
                        str(quarantine_path) if quarantine_path else None
                    ),
                    "quarantine_sha256": (
                        sha256_file(quarantine_path) if quarantine_path else None
                    ),
                }

                def enrichment_checkpoint_payload(
                    completed_at_utc: str,
                ) -> Mapping[str, Any]:
                    published_entry = {
                        **entry_without_completion,
                        "completed_at_utc": completed_at_utc,
                    }
                    return {
                        **dict(latest),
                        "schema_version": 1,
                        "partitions": {**entries, key: published_entry},
                    }

                published_checkpoint = (
                    _write_checkpoint_with_conservative_completion(
                        checkpoint_path,
                        enrichment_checkpoint_payload,
                    )
                )
                entries = dict(published_checkpoint["partitions"])
            entries_by_path[checkpoint_path] = entries
        completed_now += 1
        dataset_config = (
            fundamentals_config if dataset == "fina_indicator_vip" else reference_config
        )
        rate = max(
            0.0,
            float(
                dataset_config.get(
                    "request_rate_per_minute",
                    sync_config.get("request_rate_per_minute") or 0.0,
                )
            ),
        )
        if rate and completed_now < requested_count:
            time.sleep(60.0 / rate)

    return {
        "schema_version": 1,
        "status": "complete" if requested_count == len(pending) else "partial",
        "source": "tushare",
        "start_date": start,
        "end_date": end,
        "datasets": list(selected_datasets),
        "quarter_partition_count": len(quarter_dates),
        "membership_month_end_count": len(as_of_dates),
        "partition_count": len(planned),
        "completed_before": completed_before,
        "completed_this_run": completed_now,
        "remaining_partition_count": len(pending) - completed_now,
        "checkpoint_paths": sorted(
            str(path) for path in {checkpoint_paths[name] for name in selected_datasets}
        ),
        "raw_root": str(resolved_layout.raw_root),
    }


def _reference_checkpoint_path(
    config: Mapping[str, Any], layout: RuntimeLayout
) -> Path:
    enrichment = dict(config.get("enrichment") or {})
    reference = dict(config.get("reference_snapshots") or enrichment)
    return layout.raw_root / str(
        reference.get("checkpoint_file") or "enrichment-checkpoint.json"
    )


def _market_close_utc(trade_date: str) -> datetime:
    local = datetime.combine(
        pd.Timestamp(_date(trade_date)).date(),
        wall_time(hour=15),
        tzinfo=ZoneInfo("Asia/Shanghai"),
    )
    return local.astimezone(timezone.utc)


def _completed_at_utc(value: Any, *, label: str) -> datetime:
    try:
        parsed = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{label} must include a UTC offset")
    return parsed.tz_convert("UTC").to_pydatetime()


def _exact_daily_universe(
    layout: RuntimeLayout, trade_date: str
) -> tuple[set[str], dict[str, Any]]:
    """Load a hash-verified exact daily universe from the raw checkpoint."""

    checkpoint = _read_checkpoint(layout.checkpoint_path)
    entries = checkpoint.get("partitions")
    key = f"daily/{trade_date}"
    entry = entries.get(key) if isinstance(entries, Mapping) else None
    expected = _partition_path(layout.raw_root, "daily", trade_date)
    if (
        not isinstance(entry, Mapping)
        or entry.get("status") != "complete"
        or entry.get("dataset") != "daily"
        or entry.get("trade_date") != trade_date
        or Path(str(entry.get("path") or "")).expanduser().resolve()
        != expected.resolve()
        or not _checkpoint_entry_is_valid(entry, expected, verify_hash=True)
    ):
        raise ValueError(
            f"exact reference requires a valid checkpointed {key} partition"
        )
    completed = _completed_at_utc(
        entry.get("completed_at_utc"), label=f"{key} completed_at_utc"
    )
    if completed < _market_close_utc(trade_date):
        raise ValueError(f"{key} checkpoint completion precedes market close")
    if completed > datetime.now(timezone.utc):
        raise ValueError(f"{key} checkpoint completion is in the future")
    frame = pd.read_parquet(expected)
    _audit_partition(frame, "daily", trade_date)
    validate_provider_completion_evidence(
        entry,
        frame,
        dataset="daily",
        trade_date=trade_date,
        required_datasets=PROVIDER_COMPLETION_DATASETS,
    )
    if type(entry.get("row_count")) is not int or int(entry["row_count"]) != len(
        frame
    ):
        raise ValueError(f"{key} checkpoint row count differs from its artifact")
    ticker_values = frame["ts_code"].astype("string").str.strip()
    if ticker_values.isna().any() or ticker_values.eq("").any():
        raise ValueError("exact daily universe contains blank securities")
    tickers = set(ticker_values)
    tickers.discard("")
    if not tickers:
        raise ValueError("exact daily universe is empty")
    return tickers, dict(entry)


def _normalise_exact_bak_basic(
    frame: pd.DataFrame, *, trade_date: str
) -> pd.DataFrame:
    """Canonicalize one exact response without historical prior-day fallback."""

    audit_enrichment_partition(frame, "bak_basic", trade_date)
    columns = ENRICHMENT_DATASET_FIELDS["bak_basic"].split(",")
    work = frame.loc[:, columns].copy()
    work["ts_code"] = work["ts_code"].astype("string").str.strip()
    source_dates = work["trade_date"].astype("string").str.replace(
        "-", "", regex=False
    )
    work["source_trade_date"] = source_dates
    work["trade_date"] = _compact(trade_date)
    return work.sort_values("ts_code", kind="mergesort").reset_index(drop=True)


def _validate_exact_reference_entry(
    entry: Any,
    *,
    path: Path,
    trade_date: str,
    daily_tickers: set[str],
    daily_sha256: str,
) -> dict[str, Any]:
    if (
        not isinstance(entry, Mapping)
        or entry.get("status") != "complete"
        or entry.get("dataset") != "bak_basic"
        or entry.get("trade_date") != trade_date
        or entry.get("request_trade_date") != trade_date
        or entry.get("source_trade_date") != trade_date
        or entry.get("capture_contract_id") != EXACT_REFERENCE_CONTRACT_ID
        or entry.get("capture_mode") != "exact_only"
        or entry.get("fallback_used") is not False
        or entry.get("fields") != ENRICHMENT_DATASET_FIELDS["bak_basic"]
        or entry.get("exact_source_required") is not True
        or type(entry.get("stability_sample_count")) is not int
        or int(entry["stability_sample_count"]) < 2
        or entry.get("daily_partition_sha256") != daily_sha256
        or entry.get("daily_ticker_count") != len(daily_tickers)
        or entry.get("covered_ticker_count") != len(daily_tickers)
        or Path(str(entry.get("path") or "")).expanduser().resolve()
        != path.resolve()
        or path.is_symlink()
        or not _checkpoint_entry_is_valid(entry, path, verify_hash=True)
    ):
        raise ValueError("exact reference checkpoint identity or bytes are invalid")
    completed = _completed_at_utc(
        entry.get("completed_at_utc"),
        label=f"bak_basic/trade_date={trade_date} completed_at_utc",
    )
    if completed < _market_close_utc(trade_date):
        raise ValueError("exact reference checkpoint completion precedes market close")
    if completed > datetime.now(timezone.utc):
        raise ValueError("exact reference checkpoint completion is in the future")
    frame = pd.read_parquet(path)
    audit_enrichment_partition(frame, "bak_basic", trade_date)
    if "source_trade_date" not in frame.columns:
        raise ValueError("exact reference lacks source_trade_date evidence")
    source_dates = frame["source_trade_date"].astype("string").str.replace(
        "-", "", regex=False
    )
    if not bool(source_dates.eq(_compact(trade_date)).all()):
        raise ValueError("exact reference used a non-exact provider snapshot")
    reference_tickers = set(frame["ts_code"].astype("string").str.strip())
    if frame["ts_code"].isna().any() or "" in reference_tickers:
        raise ValueError("exact reference contains blank securities")
    missing = sorted(daily_tickers - reference_tickers)
    if entry.get("reference_ticker_count") != len(reference_tickers):
        raise ValueError("exact reference ticker count differs from checkpoint")
    if missing:
        raise ValueError(
            "exact reference does not cover the complete daily universe; "
            f"missing tickers include {missing[:5]}"
        )
    return {
        "entry": dict(entry),
        "daily_ticker_count": len(daily_tickers),
        "reference_ticker_count": len(reference_tickers),
        "covered_ticker_count": len(daily_tickers),
    }


def sync_exact_reference(
    trade_date: str,
    *,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    layout: RuntimeLayout | None = None,
    client: MarketDataClient | Any | None = None,
    stability_samples: int = 2,
) -> dict[str, Any]:
    """Capture one exact, stable, full-universe ``bak_basic`` partition.

    This operation is deliberately raw-only: it never reads the frozen
    historical membership planner and never applies enrichment to canonical
    Top-500 files.  Unlike the historical archive synchronizer, it forbids a
    prior-day fallback because a prospective monthly membership must bind the
    provider's exact as-of response.  The checkpoint ``completed_at_utc`` is a
    conservative publication-completion bound: it follows partition durability
    and is never earlier than checkpoint replace plus directory durability.
    """

    as_of = _date(trade_date)
    if type(stability_samples) is not int or stability_samples < 2:
        raise ValueError("stability_samples must be an integer of at least 2")
    captured = datetime.now(timezone.utc)
    if captured < _market_close_utc(as_of):
        return {
            "schema_version": 1,
            "status": "waiting",
            "reason": "before_market_close",
            "source": "tushare",
            "dataset": "bak_basic",
            "trade_date": as_of,
            "exact_source_required": True,
            "completed_before": 0,
            "completed_this_run": 0,
        }

    config = load_data_config(config_path)
    resolved_layout = layout or RuntimeLayout.from_config(
        config, config_path=config_path
    )
    resolved_layout.ensure_directories()
    checkpoint_path = _reference_checkpoint_path(config, resolved_layout)
    key = f"bak_basic/trade_date={as_of}"
    path = enrichment_partition_path(resolved_layout.raw_root, "bak_basic", as_of)
    raw_checkpoint_path = resolved_layout.checkpoint_path

    with _raw_reference_checkpoint_locks(raw_checkpoint_path, checkpoint_path):
        daily_tickers, daily_entry = _exact_daily_universe(
            resolved_layout, as_of
        )
        checkpoint = _read_checkpoint(checkpoint_path)
        partitions = checkpoint.get("partitions")
        existing = partitions.get(key) if isinstance(partitions, Mapping) else None
        if existing is not None:
            verified = _validate_exact_reference_entry(
                existing,
                path=path,
                trade_date=as_of,
                daily_tickers=daily_tickers,
                daily_sha256=str(daily_entry["sha256"]),
            )
            return {
                "schema_version": 1,
                "status": "complete",
                "source": "tushare",
                "dataset": "bak_basic",
                "trade_date": as_of,
                "exact_source_required": True,
                "stability_sample_count": int(
                    existing.get("stability_sample_count") or 1
                ),
                "completed_before": 1,
                "completed_this_run": 0,
                "checkpoint_path": str(checkpoint_path),
                "partition_path": str(path),
                **{name: verified[name] for name in (
                    "daily_ticker_count",
                    "reference_ticker_count",
                    "covered_ticker_count",
                )},
            }

    sync_config = dict(config.get("sync") or {})
    reference_config = dict(
        config.get("reference_snapshots") or config.get("enrichment") or {}
    )
    resolved_client = client or _configured_tushare_client(
        sync_config, resolved_layout
    )
    samples: list[pd.DataFrame] = []
    rate = max(
        0.0,
        float(
            reference_config.get(
                "request_rate_per_minute",
                sync_config.get("request_rate_per_minute") or 0.0,
            )
        ),
    )
    for index in range(stability_samples):
        raw = _call(
            resolved_client,
            "bak_basic",
            trade_date=_compact(as_of),
            fields=ENRICHMENT_DATASET_FIELDS["bak_basic"],
        )
        if raw.empty:
            return {
                "schema_version": 1,
                "status": "waiting",
                "reason": "provider_empty",
                "source": "tushare",
                "dataset": "bak_basic",
                "trade_date": as_of,
                "exact_source_required": True,
                "completed_before": 0,
                "completed_this_run": 0,
                "checkpoint_path": str(checkpoint_path),
                "partition_path": str(path),
            }
        sample = _normalise_exact_bak_basic(raw, trade_date=as_of)
        missing = sorted(
            daily_tickers
            - set(sample["ts_code"].astype("string").str.strip())
        )
        if missing:
            return {
                "schema_version": 1,
                "status": "waiting",
                "reason": "provider_universe_incomplete",
                "source": "tushare",
                "dataset": "bak_basic",
                "trade_date": as_of,
                "exact_source_required": True,
                "missing_tickers": missing[:5],
                "completed_before": 0,
                "completed_this_run": 0,
                "checkpoint_path": str(checkpoint_path),
                "partition_path": str(path),
            }
        if samples and not sample.equals(samples[0]):
            return {
                "schema_version": 1,
                "status": "waiting",
                "reason": "provider_response_unstable",
                "source": "tushare",
                "dataset": "bak_basic",
                "trade_date": as_of,
                "exact_source_required": True,
                "completed_before": 0,
                "completed_this_run": 0,
                "checkpoint_path": str(checkpoint_path),
                "partition_path": str(path),
            }
        samples.append(sample)
        if rate and index + 1 < stability_samples:
            time.sleep(60.0 / rate)

    frame = samples[0]
    initial_daily_sha256 = str(daily_entry["sha256"])
    with _raw_reference_checkpoint_locks(raw_checkpoint_path, checkpoint_path):
        latest_daily_tickers, latest_daily_entry = _exact_daily_universe(
            resolved_layout, as_of
        )
        latest_daily_sha256 = str(latest_daily_entry["sha256"])
        if (
            latest_daily_sha256 != initial_daily_sha256
            or len(latest_daily_tickers) != len(daily_tickers)
            or latest_daily_tickers != daily_tickers
        ):
            return {
                "schema_version": 1,
                "status": "waiting",
                "reason": "daily_universe_changed_during_capture",
                "source": "tushare",
                "dataset": "bak_basic",
                "trade_date": as_of,
                "exact_source_required": True,
                "initial_daily_partition_sha256": initial_daily_sha256,
                "current_daily_partition_sha256": latest_daily_sha256,
                "completed_before": 0,
                "completed_this_run": 0,
                "checkpoint_path": str(checkpoint_path),
                "partition_path": str(path),
            }
        daily_tickers = latest_daily_tickers
        daily_entry = latest_daily_entry
        checkpoint = _read_checkpoint(checkpoint_path)
        entries = dict(checkpoint.get("partitions") or {})
        concurrent = entries.get(key)
        if concurrent is not None:
            verified = _validate_exact_reference_entry(
                concurrent,
                path=path,
                trade_date=as_of,
                daily_tickers=daily_tickers,
                daily_sha256=str(daily_entry["sha256"]),
            )
            entry = dict(concurrent)
            completed_before = 1
            completed_this_run = 0
        else:
            _write_parquet_atomic(path, frame)
            partition_size = int(path.stat().st_size)
            partition_sha256 = sha256_file(path)
            entry_without_completion = {
                "status": "complete",
                "dataset": "bak_basic",
                "trade_date": as_of,
                "request_trade_date": as_of,
                "source_trade_date": as_of,
                "capture_contract_id": EXACT_REFERENCE_CONTRACT_ID,
                "capture_mode": "exact_only",
                "fallback_used": False,
                "fields": ENRICHMENT_DATASET_FIELDS["bak_basic"],
                "path": str(path),
                "row_count": int(len(frame)),
                "size_bytes": partition_size,
                "sha256": partition_sha256,
                "quarantine_row_count": 0,
                "quarantine_path": None,
                "quarantine_sha256": None,
                "exact_source_required": True,
                "stability_sample_count": stability_samples,
                "daily_partition_sha256": str(daily_entry["sha256"]),
                "daily_ticker_count": len(daily_tickers),
                "covered_ticker_count": len(daily_tickers),
                "reference_ticker_count": int(len(frame)),
            }

            def exact_checkpoint_payload(
                completed_at_utc: str,
            ) -> Mapping[str, Any]:
                published_entry = {
                    **entry_without_completion,
                    "completed_at_utc": completed_at_utc,
                }
                return {
                    **dict(checkpoint),
                    "schema_version": 1,
                    "partitions": {**entries, key: published_entry},
                }

            published_checkpoint = _write_checkpoint_with_conservative_completion(
                checkpoint_path,
                exact_checkpoint_payload,
            )
            entry = dict(published_checkpoint["partitions"][key])
            verified = _validate_exact_reference_entry(
                entry,
                path=path,
                trade_date=as_of,
                daily_tickers=daily_tickers,
                daily_sha256=str(daily_entry["sha256"]),
            )
            completed_before = 0
            completed_this_run = 1

    return {
        "schema_version": 1,
        "status": "complete",
        "source": "tushare",
        "dataset": "bak_basic",
        "trade_date": as_of,
        "exact_source_required": True,
        "stability_sample_count": int(
            entry.get("stability_sample_count") or stability_samples
        ),
        "completed_before": completed_before,
        "completed_this_run": completed_this_run,
        "checkpoint_path": str(checkpoint_path),
        "partition_path": str(path),
        **{name: verified[name] for name in (
            "daily_ticker_count",
            "reference_ticker_count",
            "covered_ticker_count",
        )},
    }


def _provider_bundle_checkpoint_valid(
    entries: Mapping[str, Any],
    *,
    raw_root: Path,
    trade_date: str,
    datasets: Sequence[str],
    required_evidence_datasets: Sequence[str] | None = None,
    verify_hash: bool,
) -> tuple[bool, str | None]:
    """Verify all selected entries and their shared completion evidence."""

    bundle_sha: str | None = None
    evidence_sha: str | None = None
    selected = tuple(sorted(set(datasets)))
    required = tuple(
        sorted(set(required_evidence_datasets or selected))
    )
    for dataset in selected:
        key = f"{dataset}/{trade_date}"
        path = _partition_path(raw_root, dataset, trade_date)
        entry = entries.get(key)
        if not _checkpoint_entry_is_valid(
            entry, path, verify_hash=verify_hash
        ):
            return False, None
        try:
            frame = pd.read_parquet(path)
            evidence = validate_provider_completion_evidence(
                entry,
                frame,
                dataset=dataset,
                trade_date=trade_date,
                required_datasets=required,
            )
        except (OSError, TypeError, ValueError):
            return False, None
        if evidence is None:
            return False, None
        current = str(evidence.get("bundle_content_sha256") or "")
        if not re.fullmatch(r"[0-9a-f]{64}", current):
            return False, None
        if bundle_sha is None:
            bundle_sha = current
        elif current != bundle_sha:
            return False, None
        current_evidence = str(evidence.get("evidence_sha256") or "")
        if not re.fullmatch(r"[0-9a-f]{64}", current_evidence):
            return False, None
        if evidence_sha is None:
            evidence_sha = current_evidence
        elif current_evidence != evidence_sha:
            return False, None
    return True, bundle_sha


def _provider_waiting_result(
    *,
    reason: str,
    dataset: str,
    trade_date: str,
    start: str,
    end: str,
    calendar_end: str,
    completed_before: int,
    completed_now: int,
    remaining: int,
    layout: RuntimeLayout,
    calendar_entry: Mapping[str, Any],
    details: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "status": "waiting",
        "reason": reason,
        "source": "tushare",
        "dataset": dataset,
        "trade_date": trade_date,
        "partition_request_start_date": start,
        "partition_request_end_date": end,
        "calendar_start_date": start,
        "calendar_end_date": calendar_end,
        "completed_before": completed_before,
        "completed_this_run": completed_now,
        "remaining_partition_count": remaining,
        "checkpoint_path": str(layout.checkpoint_path),
        "calendar_path": str(calendar_entry["path"]),
        "calendar_content_sha256": str(
            calendar_entry["calendar_content_sha256"]
        ),
        "calendar_artifact_sha256": str(calendar_entry["artifact_sha256"]),
        "calendar_completed_at_utc": str(calendar_entry["completed_at_utc"]),
        "raw_root": str(layout.raw_root),
        **dict(details or {}),
    }


def sync_data(
    start_date: str,
    end_date: str,
    *,
    calendar_end_date: str | None = None,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    layout: RuntimeLayout | None = None,
    client: MarketDataClient | Any | None = None,
    datasets: Sequence[str] | None = None,
    resume: bool = True,
    max_partitions: int | None = None,
) -> dict[str, Any]:
    """Synchronise full-market daily Parquet partitions with a local checkpoint."""

    start = _date(start_date)
    end = _date(end_date)
    if end < start:
        raise ValueError("end_date must be on or after start_date")
    calendar_end = _date(calendar_end_date or end)
    if calendar_end < end:
        raise ValueError("calendar_end_date must be on or after end_date")
    config = load_data_config(config_path)
    resolved_layout = layout or RuntimeLayout.from_config(config, config_path=config_path)
    resolved_layout.ensure_directories()
    sync_config = dict(config.get("sync") or {})
    selected_datasets = tuple(datasets or sync_config.get("datasets") or DATASET_FIELDS)
    unknown = sorted(set(selected_datasets) - set(DATASET_FIELDS))
    if unknown:
        raise ValueError(f"unsupported datasets: {unknown}")
    resolved_client = client or _configured_tushare_client(sync_config, resolved_layout)
    exchange = str(sync_config.get("exchange") or "SSE")
    with _checkpoint_lock(resolved_layout.checkpoint_path):
        baseline_checkpoint = _read_checkpoint(resolved_layout.checkpoint_path)
    baseline_partitions = dict(baseline_checkpoint.get("partitions") or {})
    baseline_calendars = dict(baseline_checkpoint.get("calendars") or {})
    calendar = _call(
        resolved_client,
        "trade_cal",
        exchange=exchange,
        start_date=_compact(start),
        end_date=_compact(calendar_end),
        fields="exchange,cal_date,is_open,pretrade_date",
    )
    if calendar.empty:
        return {
            "schema_version": 1,
            "status": "waiting",
            "reason": "provider_empty",
            "source": "tushare",
            "dataset": "trade_cal",
            "partition_request_start_date": start,
            "partition_request_end_date": end,
            "calendar_start_date": start,
            "calendar_end_date": calendar_end,
            "completed_before": 0,
            "completed_this_run": 0,
            "checkpoint_path": str(resolved_layout.checkpoint_path),
            "raw_root": str(resolved_layout.raw_root),
        }
    normalised_calendar, _, _ = _normalise_trade_calendar(
        calendar,
        exchange=exchange,
        start_date=start,
        end_date=calendar_end,
    )
    dates = normalised_calendar.loc[
        normalised_calendar["is_open"]
        & normalised_calendar["cal_date"].le(pd.Timestamp(end)),
        "cal_date",
    ].dt.strftime("%Y-%m-%d").tolist()
    if not dates:
        raise ValueError("trade calendar contains no open dates")

    checkpoint, calendar_entry = _persist_trade_calendar(
        calendar,
        raw_root=resolved_layout.raw_root,
        checkpoint_path=resolved_layout.checkpoint_path,
        baseline_calendars=baseline_calendars,
        resume=resume,
        exchange=exchange,
        start_date=start,
        end_date=calendar_end,
    )
    entries = dict(checkpoint.get("partitions") or {})
    verify_hash = bool(sync_config.get("verify_hashes_on_resume", False))
    pending_work: list[tuple[str, Any, int]] = []
    completed_before = 0
    for trade_date in dates:
        if provider_completion_required(trade_date):
            valid, _bundle_sha = _provider_bundle_checkpoint_valid(
                entries,
                raw_root=resolved_layout.raw_root,
                trade_date=trade_date,
                datasets=selected_datasets,
                required_evidence_datasets=PROVIDER_COMPLETION_DATASETS,
                verify_hash=True,
            )
            if resume and valid:
                completed_before += len(selected_datasets)
            else:
                pending_work.append(
                    (
                        "guarded_bundle",
                        (trade_date, tuple(sorted(selected_datasets))),
                        len(selected_datasets),
                    )
                )
            continue
        for dataset in selected_datasets:
            key = f"{dataset}/{trade_date}"
            path = _partition_path(resolved_layout.raw_root, dataset, trade_date)
            if resume and _checkpoint_entry_is_valid(
                entries.get(key), path, verify_hash=verify_hash
            ):
                completed_before += 1
            else:
                pending_work.append(
                    (
                        "historical_partition",
                        (
                            dataset,
                            trade_date,
                            path,
                            key,
                            _checkpoint_value_sha256(
                                baseline_partitions.get(key)
                            ),
                        ),
                        1,
                    )
                )

    pending_partition_count = sum(item[2] for item in pending_work)
    budget = (
        pending_partition_count
        if max_partitions is None
        else max(0, int(max_partitions))
    )
    requested_work: list[tuple[str, Any, int]] = []
    requested_count = 0
    for item in pending_work:
        cost = item[2]
        if requested_count + cost > budget:
            break
        requested_work.append(item)
        requested_count += cost

    rate = max(
        0.0, float(sync_config.get("request_rate_per_minute") or 0.0)
    )
    delay = 60.0 / rate if rate else 0.0
    sample_count = int(
        sync_config.get(
            "provider_completion_stability_samples",
            PROVIDER_COMPLETION_MINIMUM_SAMPLES,
        )
    )
    if sample_count < PROVIDER_COMPLETION_MINIMUM_SAMPLES:
        raise ValueError(
            "sync.provider_completion_stability_samples must be at least 2"
        )
    completed_now = 0
    completed_concurrently = 0
    request_number = 0
    for kind, payload, cost in requested_work:
        if kind == "historical_partition":
            dataset, trade_date, path, key, baseline_sha256 = payload
            frame = _call(
                resolved_client,
                dataset,
                trade_date=_compact(trade_date),
                fields=DATASET_FIELDS[dataset],
            )
            request_number += 1
            if frame.empty:
                return _provider_waiting_result(
                    reason="provider_empty",
                    dataset=dataset,
                    trade_date=trade_date,
                    start=start,
                    end=end,
                    calendar_end=calendar_end,
                    completed_before=completed_before,
                    completed_now=completed_now,
                    remaining=pending_partition_count - completed_now,
                    layout=resolved_layout,
                    calendar_entry=calendar_entry,
                )
            _audit_partition(frame, dataset, trade_date)
            with _checkpoint_lock(resolved_layout.checkpoint_path):
                latest = _read_checkpoint(resolved_layout.checkpoint_path)
                latest_entries = dict(latest.get("partitions") or {})
                concurrent = latest_entries.get(key)
                concurrent_valid = _checkpoint_entry_is_valid(
                    concurrent,
                    path,
                    verify_hash=verify_hash if resume else True,
                )
                changed_since_start = (
                    _checkpoint_value_sha256(concurrent) != baseline_sha256
                )
                publish = not (resume and concurrent_valid)
                if not resume and changed_since_start:
                    if not concurrent_valid:
                        raise ValueError(
                            f"{key} checkpoint changed during no-resume refresh"
                        )
                    publish = False
                if publish:
                    _write_parquet_atomic(path, frame)
                    entry_without_completion = {
                        "status": "complete",
                        "dataset": dataset,
                        "trade_date": trade_date,
                        "path": str(path),
                        "row_count": int(len(frame)),
                        "size_bytes": int(path.stat().st_size),
                        "sha256": sha256_file(path),
                    }

                    def raw_checkpoint_payload(
                        completed_at_utc: str,
                    ) -> Mapping[str, Any]:
                        published_entry = {
                            **entry_without_completion,
                            "completed_at_utc": completed_at_utc,
                        }
                        return {
                            **dict(latest),
                            "schema_version": int(
                                latest.get("schema_version") or 1
                            ),
                            "partitions": {
                                **latest_entries,
                                key: published_entry,
                            },
                            "calendars": dict(
                                latest.get("calendars") or {}
                            ),
                        }

                    _write_checkpoint_with_conservative_completion(
                        resolved_layout.checkpoint_path,
                        raw_checkpoint_payload,
                    )
            completed_now += 1
            if delay and request_number < requested_count * sample_count:
                time.sleep(delay)
            continue

        trade_date, publish_datasets = payload
        sample_datasets = PROVIDER_COMPLETION_DATASETS
        observed = _now_utc()
        not_before = provider_completion_not_before_utc(trade_date)
        if observed < not_before:
            return _provider_waiting_result(
                reason="before_provider_completion_gate",
                dataset="provider_completion_bundle",
                trade_date=trade_date,
                start=start,
                end=end,
                calendar_end=calendar_end,
                completed_before=completed_before,
                completed_now=completed_now,
                remaining=pending_partition_count - completed_now,
                layout=resolved_layout,
                calendar_entry=calendar_entry,
                details={
                    "provider_completion_contract_id": (
                        PROVIDER_COMPLETION_CONTRACT_ID
                    ),
                    "provider_completion_not_before_utc": (
                        not_before.isoformat()
                    ),
                    "provider_completion_not_before_basis": (
                        "protocol_chosen_conservative_gate_not_provider_completeness_sla"
                    ),
                },
            )
        samples: list[dict[str, pd.DataFrame]] = []
        observations: list[dict[str, Any]] = []
        for sample_index in range(sample_count):
            started = _now_utc()
            sample: dict[str, pd.DataFrame] = {}
            request_id = uuid.uuid4().hex
            for dataset in sample_datasets:
                frame = _call(
                    resolved_client,
                    dataset,
                    trade_date=_compact(trade_date),
                    fields=DATASET_FIELDS[dataset],
                )
                request_number += 1
                if frame.empty:
                    return _provider_waiting_result(
                        reason="provider_empty",
                        dataset=dataset,
                        trade_date=trade_date,
                        start=start,
                        end=end,
                        calendar_end=calendar_end,
                        completed_before=completed_before,
                        completed_now=completed_now,
                        remaining=pending_partition_count - completed_now,
                        layout=resolved_layout,
                        calendar_entry=calendar_entry,
                        details={
                            "provider_completion_sample_index": sample_index + 1
                        },
                    )
                _audit_partition(frame, dataset, trade_date)
                sample[dataset] = frame
                if delay:
                    time.sleep(delay)
            completed = _now_utc()
            samples.append(sample)
            observations.append(
                {
                    "request_id": request_id,
                    "request_started_at_utc": started.isoformat(),
                    "response_completed_at_utc": completed.isoformat(),
                }
            )
        try:
            evidence = _build_provider_completion_evidence(
                trade_date, samples, observations=observations
            )
        except ValueError as exc:
            reason = str(exc)
            if reason not in {
                "provider_sample_observation_invalid",
                "provider_response_unstable",
                "provider_universe_incomplete",
                "provider_response_at_row_limit",
            }:
                raise
            return _provider_waiting_result(
                reason=reason,
                dataset="provider_completion_bundle",
                trade_date=trade_date,
                start=start,
                end=end,
                calendar_end=calendar_end,
                completed_before=completed_before,
                completed_now=completed_now,
                remaining=pending_partition_count - completed_now,
                layout=resolved_layout,
                calendar_entry=calendar_entry,
                details={
                    "provider_completion_contract_id": (
                        PROVIDER_COMPLETION_CONTRACT_ID
                    ),
                    "provider_completion_sample_count": sample_count,
                },
            )

        live_bundle_sha = str(evidence["bundle_content_sha256"])
        with _checkpoint_lock(resolved_layout.checkpoint_path):
            latest = _read_checkpoint(resolved_layout.checkpoint_path)
            latest_entries = dict(latest.get("partitions") or {})
            protected_evidence: Mapping[str, Any] | None = None
            protected_evidence_sha: str | None = None
            datasets_to_publish: list[str] = []
            conflict: str | None = None
            selected_for_publish = set(publish_datasets)
            for dataset in PROVIDER_COMPLETION_DATASETS:
                key = f"{dataset}/{trade_date}"
                prior = latest_entries.get(key)
                if prior is None:
                    if dataset in selected_for_publish:
                        datasets_to_publish.append(dataset)
                    continue
                if isinstance(prior, Mapping) and prior.get("status") == "reconciling":
                    expected_path = _partition_path(
                        resolved_layout.raw_root, dataset, trade_date
                    )
                    if (
                        not _provider_reconciling_entry_valid(
                            prior,
                            dataset=dataset,
                            trade_date=trade_date,
                            expected_path=expected_path,
                        )
                        or prior.get("target_bundle_content_sha256")
                        != live_bundle_sha
                    ):
                        conflict = (
                            "reconciling checkpoint state is malformed or its "
                            f"provider bundle changed for {key}"
                        )
                        break
                    if dataset in selected_for_publish:
                        datasets_to_publish.append(dataset)
                    continue
                if (
                    isinstance(prior, Mapping)
                    and prior.get("status") == "complete"
                    and "provider_completion" not in prior
                ):
                    if dataset in selected_for_publish:
                        datasets_to_publish.append(dataset)
                    continue
                if not isinstance(prior, Mapping) or prior.get("status") != "complete":
                    conflict = f"unsupported checkpoint state for {key}"
                    break
                path = _partition_path(
                    resolved_layout.raw_root, dataset, trade_date
                )
                if not _checkpoint_entry_is_valid(
                    prior, path, verify_hash=True
                ):
                    conflict = f"guarded checkpoint bytes are invalid for {key}"
                    break
                try:
                    prior_frame = pd.read_parquet(path)
                    prior_evidence = validate_provider_completion_evidence(
                        prior,
                        prior_frame,
                        dataset=dataset,
                        trade_date=trade_date,
                        required_datasets=PROVIDER_COMPLETION_DATASETS,
                    )
                except (OSError, TypeError, ValueError) as exc:
                    conflict = (
                        f"guarded checkpoint evidence is invalid for {key}: "
                        f"{type(exc).__name__}"
                    )
                    break
                assert prior_evidence is not None
                prior_bundle_sha = str(
                    prior_evidence["bundle_content_sha256"]
                )
                current_evidence_sha = str(prior_evidence["evidence_sha256"])
                if prior_bundle_sha != live_bundle_sha:
                    conflict = (
                        "stable provider revision conflicts with an already "
                        f"guarded partition bundle for {trade_date}"
                    )
                    break
                if protected_evidence_sha is None:
                    protected_evidence = prior_evidence
                    protected_evidence_sha = current_evidence_sha
                elif protected_evidence_sha != current_evidence_sha:
                    conflict = (
                        f"guarded partition entries bind mixed proofs for {trade_date}"
                    )
                    break
            if conflict is not None:
                blocked = _provider_waiting_result(
                    reason="provider_revision_conflict",
                    dataset="provider_completion_bundle",
                    trade_date=trade_date,
                    start=start,
                    end=end,
                    calendar_end=calendar_end,
                    completed_before=completed_before,
                    completed_now=completed_now,
                    remaining=pending_partition_count - completed_now,
                    layout=resolved_layout,
                    calendar_entry=calendar_entry,
                    details={
                        "error": conflict,
                        "live_bundle_content_sha256": live_bundle_sha,
                    },
                )
                blocked["status"] = "blocked"
                return blocked
            if not datasets_to_publish:
                completed_concurrently += cost
                continue

            publication_evidence = dict(protected_evidence or evidence)
            stable_at = _completed_at_utc(
                publication_evidence["stable_at_utc"],
                label="provider completion stable_at_utc",
            )
            reconcile_started_at = _now_utc()
            if reconcile_started_at < stable_at:
                return _provider_waiting_result(
                    reason="provider_sample_observation_invalid",
                    dataset="provider_completion_bundle",
                    trade_date=trade_date,
                    start=start,
                    end=end,
                    calendar_end=calendar_end,
                    completed_before=completed_before,
                    completed_now=completed_now,
                    remaining=pending_partition_count - completed_now,
                    layout=resolved_layout,
                    calendar_entry=calendar_entry,
                    details={
                        "error": "publication clock precedes stable sample"
                    },
                )
            reconcile_started = reconcile_started_at.isoformat()
            reconciling_entries = dict(latest_entries)
            reconciliation_by_dataset: dict[str, dict[str, Any]] = {}
            for dataset in datasets_to_publish:
                key = f"{dataset}/{trade_date}"
                prior = latest_entries.get(key)
                attempt = {
                    "started_at_utc": reconcile_started,
                    "target_bundle_content_sha256": live_bundle_sha,
                    "published_evidence_sha256": publication_evidence[
                        "evidence_sha256"
                    ],
                }
                existing_reconciliation = (
                    prior.get("reconciliation")
                    if isinstance(prior, Mapping)
                    and prior.get("status") == "reconciling"
                    else None
                )
                if isinstance(existing_reconciliation, Mapping):
                    reconciliation = dict(existing_reconciliation)
                    raw_attempts = reconciliation.get("attempts")
                    attempts = (
                        [dict(value) for value in raw_attempts]
                        if isinstance(raw_attempts, list)
                        and all(isinstance(value, Mapping) for value in raw_attempts)
                        else []
                    )
                    attempts.append(attempt)
                    reconciliation["attempts"] = attempts
                    reconciliation["resume_count"] = int(
                        reconciliation.get("resume_count") or 0
                    ) + 1
                    reconciliation["last_resumed_at_utc"] = reconcile_started
                    reconciliation["published_evidence_sha256"] = (
                        publication_evidence["evidence_sha256"]
                    )
                else:
                    reconciliation = {
                        "contract_id": PROVIDER_COMPLETION_CONTRACT_ID,
                        "started_at_utc": reconcile_started,
                        "reason": (
                            "legacy_or_incomplete_provider_completion_evidence"
                        ),
                        "previous_checkpoint_entry_sha256": (
                            _checkpoint_value_sha256(prior)
                        ),
                        "previous_status": (
                            prior.get("status")
                            if isinstance(prior, Mapping)
                            else None
                        ),
                        "previous_artifact_sha256": (
                            prior.get("sha256")
                            if isinstance(prior, Mapping)
                            else None
                        ),
                        "published_evidence_sha256": publication_evidence[
                            "evidence_sha256"
                        ],
                        "attempts": [attempt],
                        "resume_count": 0,
                    }
                reconciliation_by_dataset[dataset] = reconciliation
                reconciling_entries[key] = {
                    "status": "reconciling",
                    "dataset": dataset,
                    "trade_date": trade_date,
                    "path": str(
                        _partition_path(
                            resolved_layout.raw_root, dataset, trade_date
                        )
                    ),
                    "reconcile_contract_id": (
                        PROVIDER_COMPLETION_CONTRACT_ID
                    ),
                    "reconcile_started_at_utc": reconciliation[
                        "started_at_utc"
                    ],
                    "reconcile_resumed_at_utc": (
                        reconcile_started
                        if existing_reconciliation is not None
                        else None
                    ),
                    "target_bundle_content_sha256": live_bundle_sha,
                    "reconciliation": reconciliation,
                }
            reconciling_checkpoint = {
                **dict(latest),
                "schema_version": int(latest.get("schema_version") or 1),
                "partitions": reconciling_entries,
                "calendars": dict(latest.get("calendars") or {}),
            }
            _write_json_atomic(
                resolved_layout.checkpoint_path, reconciling_checkpoint
            )

            published_without_completion: dict[str, dict[str, Any]] = {}
            for dataset in datasets_to_publish:
                frame = samples[-1][dataset]
                path = _partition_path(
                    resolved_layout.raw_root, dataset, trade_date
                )
                _write_parquet_atomic(path, frame)
                published_without_completion[dataset] = {
                    "status": "complete",
                    "dataset": dataset,
                    "trade_date": trade_date,
                    "path": str(path),
                    "row_count": int(len(frame)),
                    "size_bytes": int(path.stat().st_size),
                    "sha256": sha256_file(path),
                    "provider_completion": publication_evidence,
                    "reconciliation": reconciliation_by_dataset[dataset],
                }

            def guarded_checkpoint_payload(
                completed_at_utc: str,
            ) -> Mapping[str, Any]:
                complete_entries = dict(reconciling_entries)
                for dataset in datasets_to_publish:
                    key = f"{dataset}/{trade_date}"
                    complete_entries[key] = {
                        **published_without_completion[dataset],
                        "completed_at_utc": completed_at_utc,
                    }
                return {
                    **dict(reconciling_checkpoint),
                    "schema_version": int(
                        reconciling_checkpoint.get("schema_version") or 1
                    ),
                    "partitions": complete_entries,
                    "calendars": dict(
                        reconciling_checkpoint.get("calendars") or {}
                    ),
                }

            published_checkpoint = _write_checkpoint_with_conservative_completion(
                resolved_layout.checkpoint_path,
                guarded_checkpoint_payload,
                minimum_completion_utc=stable_at,
            )
            published_entries = dict(published_checkpoint["partitions"])
            valid, bundle_sha = _provider_bundle_checkpoint_valid(
                published_entries,
                raw_root=resolved_layout.raw_root,
                trade_date=trade_date,
                datasets=publish_datasets,
                required_evidence_datasets=PROVIDER_COMPLETION_DATASETS,
                verify_hash=True,
            )
            if not valid or bundle_sha != live_bundle_sha:
                raise ValueError(
                    f"provider completion durable publish failed for {trade_date}"
                )
        completed_now += cost
    return {
        "schema_version": 1,
        "status": (
            "complete"
            if pending_partition_count - completed_now - completed_concurrently == 0
            else "partial"
        ),
        "source": "tushare",
        "start_date": dates[0],
        "end_date": dates[-1],
        "partition_request_start_date": start,
        "partition_request_end_date": end,
        "calendar_start_date": start,
        "calendar_end_date": calendar_end,
        "open_day_count": len(dates),
        "dataset_count": len(selected_datasets),
        "partition_count": len(dates) * len(selected_datasets),
        "completed_before": completed_before,
        "completed_this_run": completed_now,
        "completed_concurrently": completed_concurrently,
        "remaining_partition_count": (
            pending_partition_count - completed_now - completed_concurrently
        ),
        "checkpoint_path": str(resolved_layout.checkpoint_path),
        "calendar_path": str(calendar_entry["path"]),
        "calendar_content_sha256": str(
            calendar_entry["calendar_content_sha256"]
        ),
        "calendar_artifact_sha256": str(calendar_entry["artifact_sha256"]),
        "calendar_completed_at_utc": str(calendar_entry["completed_at_utc"]),
        "raw_root": str(resolved_layout.raw_root),
    }


__all__ = [
    "AMOUNT_TO_RMB_MULTIPLIERS",
    "DATASET_FIELDS",
    "ENRICHMENT_DATASET_FIELDS",
    "EXACT_REFERENCE_CONTRACT_ID",
    "MarketDataClient",
    "PROVIDER_COMPLETION_CONTRACT_ID",
    "PROVIDER_COMPLETION_CUTOVER_DATE",
    "PROVIDER_COMPLETION_DATASETS",
    "PROVIDER_COMPLETION_MINIMUM_SAMPLES",
    "PROVIDER_COMPLETION_NOT_BEFORE_LOCAL_TIME",
    "PROVIDER_COMPLETION_TIMEZONE",
    "TushareClient",
    "audit_enrichment_partition",
    "enrichment_partition_path",
    "provider_completion_not_before_utc",
    "provider_completion_required",
    "provider_partition_fingerprint",
    "sync_data",
    "sync_enrichment",
    "sync_exact_reference",
    "turnover_amount_to_rmb",
    "validate_provider_completion_evidence",
]

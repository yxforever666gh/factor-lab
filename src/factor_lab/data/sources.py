"""Small, resumable raw-market synchronisation adapters."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import time
from typing import Any, Mapping, Protocol, Sequence

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


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8"
    )
    temporary.replace(path)


def _write_parquet_atomic(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.to_parquet(temporary, index=False)
    temporary.replace(path)


def _checkpoint_entry_is_valid(entry: Any, path: Path, *, verify_hash: bool) -> bool:
    if not isinstance(entry, Mapping) or entry.get("status") != "complete" or not path.is_file():
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
    checkpoint_paths = {
        "fina_indicator_vip": resolved_layout.raw_root
        / str(fundamentals_config.get("checkpoint_file") or "fundamentals-checkpoint.json"),
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
    checkpoint_payloads = {
        path: (
            _read_checkpoint(path)
            if resume
            else {"schema_version": 1, "partitions": {}}
        )
        for path in set(checkpoint_paths.values())
    }
    entries_by_path = {
        path: dict(payload.get("partitions") or {})
        for path, payload in checkpoint_payloads.items()
    }

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

    pending: list[tuple[str, str, Path, str, Path]] = []
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
            pending.append((dataset, value, path, key, checkpoint_path))
    requested_count = (
        len(pending)
        if max_partitions is None
        else min(len(pending), max(0, int(max_partitions)))
    )
    completed_now = 0
    for dataset, value, path, key, checkpoint_path in pending[:requested_count]:
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
        quarantined = frame.iloc[0:0].copy()
        if dataset == "fina_indicator_vip":
            frame, quarantined = _quarantine_early_financial_announcements(frame)
            candidate_quarantine = path.with_name("part-000.quarantine.parquet")
            if not quarantined.empty:
                quarantine_path = candidate_quarantine
                _write_parquet_atomic(quarantine_path, quarantined)
            else:
                candidate_quarantine.unlink(missing_ok=True)
        audit_enrichment_partition(frame, dataset, value)
        _write_parquet_atomic(path, frame)
        entries = entries_by_path[checkpoint_path]
        entries[key] = {
            "status": "complete",
            "dataset": dataset,
            argument: value,
            "source_trade_date": source_value if dataset == "bak_basic" else None,
            "path": str(path),
            "row_count": int(len(frame)),
            "size_bytes": int(path.stat().st_size),
            "sha256": sha256_file(path),
            "quarantine_row_count": int(len(quarantined)),
            "quarantine_path": str(quarantine_path) if quarantine_path else None,
            "quarantine_sha256": sha256_file(quarantine_path) if quarantine_path else None,
            "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        _write_json_atomic(
            checkpoint_path,
            {"schema_version": 1, "partitions": entries},
        )
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


def sync_data(
    start_date: str,
    end_date: str,
    *,
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
    config = load_data_config(config_path)
    resolved_layout = layout or RuntimeLayout.from_config(config, config_path=config_path)
    resolved_layout.ensure_directories()
    sync_config = dict(config.get("sync") or {})
    selected_datasets = tuple(datasets or sync_config.get("datasets") or DATASET_FIELDS)
    unknown = sorted(set(selected_datasets) - set(DATASET_FIELDS))
    if unknown:
        raise ValueError(f"unsupported datasets: {unknown}")
    resolved_client = client or _configured_tushare_client(sync_config, resolved_layout)
    calendar = _call(
        resolved_client,
        "trade_cal",
        exchange=str(sync_config.get("exchange") or "SSE"),
        start_date=_compact(start),
        end_date=_compact(end),
        fields="exchange,cal_date,is_open,pretrade_date",
    )
    if not {"cal_date", "is_open"}.issubset(calendar.columns):
        raise ValueError("trade_cal response requires cal_date and is_open")
    open_flag = pd.to_numeric(calendar["is_open"], errors="coerce").eq(1)
    open_flag |= calendar["is_open"].astype(str).str.lower().eq("true")
    dates = (
        pd.to_datetime(calendar.loc[open_flag, "cal_date"], errors="coerce")
        .dropna()
        .dt.strftime("%Y-%m-%d")
        .drop_duplicates()
        .sort_values()
        .tolist()
    )
    if not dates:
        raise ValueError("trade calendar contains no open dates")

    checkpoint = _read_checkpoint(resolved_layout.checkpoint_path) if resume else {
        "schema_version": 1,
        "partitions": {},
    }
    entries = dict(checkpoint.get("partitions") or {})
    verify_hash = bool(sync_config.get("verify_hashes_on_resume", False))
    pending: list[tuple[str, str, Path, str]] = []
    completed_before = 0
    for trade_date in dates:
        for dataset in selected_datasets:
            key = f"{dataset}/{trade_date}"
            path = _partition_path(resolved_layout.raw_root, dataset, trade_date)
            if resume and _checkpoint_entry_is_valid(entries.get(key), path, verify_hash=verify_hash):
                completed_before += 1
            else:
                pending.append((dataset, trade_date, path, key))
    requested_count = len(pending) if max_partitions is None else min(len(pending), max(0, max_partitions))
    rate = max(0.0, float(sync_config.get("request_rate_per_minute") or 0.0))
    delay = 60.0 / rate if rate else 0.0
    completed_now = 0
    for dataset, trade_date, path, key in pending[:requested_count]:
        frame = _call(
            resolved_client,
            dataset,
            trade_date=_compact(trade_date),
            fields=DATASET_FIELDS[dataset],
        )
        _audit_partition(frame, dataset, trade_date)
        _write_parquet_atomic(path, frame)
        entries[key] = {
            "status": "complete",
            "dataset": dataset,
            "trade_date": trade_date,
            "path": str(path),
            "row_count": int(len(frame)),
            "size_bytes": int(path.stat().st_size),
            "sha256": sha256_file(path),
            "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        checkpoint = {"schema_version": 1, "partitions": entries}
        _write_json_atomic(resolved_layout.checkpoint_path, checkpoint)
        completed_now += 1
        if delay and completed_now < requested_count:
            time.sleep(delay)
    return {
        "schema_version": 1,
        "status": "complete" if requested_count == len(pending) else "partial",
        "source": "tushare",
        "start_date": dates[0],
        "end_date": dates[-1],
        "open_day_count": len(dates),
        "dataset_count": len(selected_datasets),
        "partition_count": len(dates) * len(selected_datasets),
        "completed_before": completed_before,
        "completed_this_run": completed_now,
        "remaining_partition_count": len(pending) - completed_now,
        "checkpoint_path": str(resolved_layout.checkpoint_path),
        "raw_root": str(resolved_layout.raw_root),
    }


__all__ = [
    "AMOUNT_TO_RMB_MULTIPLIERS",
    "DATASET_FIELDS",
    "ENRICHMENT_DATASET_FIELDS",
    "MarketDataClient",
    "TushareClient",
    "audit_enrichment_partition",
    "enrichment_partition_path",
    "sync_data",
    "sync_enrichment",
    "turnover_amount_to_rmb",
]

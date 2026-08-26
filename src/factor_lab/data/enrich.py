"""Point-in-time financial and month-end reference enrichment.

The implementation deliberately stays local and small: raw Tushare Parquet
partitions are audited, canonical files are rewritten in bounded Arrow batches,
and a recoverable three-file transaction keeps features, execution pricing and
membership on the same reference epoch.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .catalog import DEFAULT_CONFIG_PATH, RuntimeLayout, load_data_config, sha256_file
from .sources import audit_enrichment_partition, enrichment_partition_path


FINANCIAL_FIELD_MAP = {
    "eps": "eps_pit",
    "bps": "bps_pit",
    "ocfps": "ocfps_pit",
    "roe": "roe_pit",
    "roe_dt": "roe_dt_pit",
    "roa": "roa_pit",
    "roic": "roic_pit",
    "ocf_to_profit": "ocf_to_profit_pit",
    "q_ocf_to_sales": "q_ocf_to_sales_pit",
    "ocf_to_debt": "ocf_to_debt_pit",
    "grossprofit_margin": "grossprofit_margin_pit",
    "netprofit_margin": "netprofit_margin_pit",
    "debt_to_assets": "debt_to_assets_pit",
    "current_ratio": "current_ratio_pit",
    "q_sales_yoy": "q_sales_yoy_pit",
    "q_netprofit_yoy": "q_netprofit_yoy_pit",
    "dt_netprofit_yoy": "dt_netprofit_yoy_pit",
    "or_yoy": "or_yoy_pit",
    "ocf_yoy": "ocf_yoy_pit",
}

FUNDAMENTAL_COLUMNS = (
    "fundamental_roic",
    "fundamental_q_ocf_to_sales",
    "fundamental_debt_to_assets",
    "fundamental_age_days",
)

FINANCIAL_METADATA_COLUMNS = (
    "financial_report_period",
    "financial_ann_date",
    "financial_available_date",
    "financial_update_flag",
    "financial_pit_valid",
)

REFERENCE_COLUMNS = (
    "state_as_of_date",
    "state_available_date",
    "vendor_ts_code_pit",
    "security_alias_applied_pit",
    "security_alias_source",
    "industry_pit",
    "name_pit",
    "st_known_pit",
    "is_st_pit",
    "st_type_pit",
    "st_type_name_pit",
    "st_filter_status",
    "reference_verified_pit",
)

_OUTPUT_TYPES: dict[str, pa.DataType] = {
    **{
        name: pa.timestamp("ns")
        for name in (
            "financial_report_period",
            "financial_ann_date",
            "financial_available_date",
            "state_as_of_date",
            "state_available_date",
        )
    },
    **{
        name: pa.string()
        for name in (
            "financial_update_flag",
            "vendor_ts_code_pit",
            "security_alias_source",
            "industry_pit",
            "name_pit",
            "st_type_pit",
            "st_type_name_pit",
            "st_filter_status",
        )
    },
    **{
        name: pa.float64()
        for name in (*FINANCIAL_FIELD_MAP.values(), *FUNDAMENTAL_COLUMNS)
    },
    **{
        name: pa.bool_()
        for name in (
            "financial_pit_valid",
            "st_known_pit",
            "is_st_pit",
            "reference_verified_pit",
            "security_alias_applied_pit",
            "eligible_pre_pit",
            "eligible",
            "historical_st_known",
            "is_st_at_asof",
        )
    },
}


def _parse_vendor_date(values: pd.Series) -> pd.Series:
    text = values.astype("string").str.replace("-", "", regex=False)
    return pd.to_datetime(text, format="%Y%m%d", errors="coerce")


def _json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    temporary.replace(path)


def _replace_table_columns(table: pa.Table, updates: Mapping[str, Any]) -> pa.Table:
    out = table
    for name, values in updates.items():
        array = (
            values
            if isinstance(values, pa.Array)
            else pa.array(values, type=_OUTPUT_TYPES.get(name), from_pandas=True)
        )
        index = out.schema.get_field_index(name)
        if index >= 0:
            out = out.set_column(index, pa.field(name, array.type), array)
        else:
            out = out.append_column(name, array)
    return out


def _parquet_dates(path: Path, column: str = "date") -> set[pd.Timestamp]:
    dates: set[pd.Timestamp] = set()
    parquet = pq.ParquetFile(path)
    if column not in parquet.schema_arrow.names:
        raise ValueError(f"{path.name} lacks {column}")
    for batch in parquet.iter_batches(batch_size=500_000, columns=[column]):
        values = pd.to_datetime(batch.column(0).to_pandas(), errors="coerce").dropna()
        dates.update(pd.Timestamp(value).normalize() for value in values.unique())
    return dates


def canonical_trading_dates(layout: RuntimeLayout) -> pd.DatetimeIndex:
    """Build the accepted local trading calendar without loading market panels."""

    dates = _parquet_dates(layout.features_path) | _parquet_dates(layout.execution_path)
    daily_root = layout.raw_root / "daily"
    if daily_root.is_dir():
        for path in daily_root.glob("trade_date=*"):
            match = re.fullmatch(r"trade_date=(\d{4}-\d{2}-\d{2})", path.name)
            if match:
                dates.add(pd.Timestamp(match.group(1)))
    if not dates:
        raise ValueError("canonical trading calendar is empty")
    return pd.DatetimeIndex(sorted(dates))


def prepare_financial_pit(
    rows: pd.DataFrame,
    trading_dates: Iterable[pd.Timestamp],
) -> pd.DataFrame:
    """Normalize financial events and apply strict next-session availability.

    Exact vendor duplicates are collapsed.  Conflicting values with the same
    ticker, report period and announcement date are rejected because the
    vendor does not provide a historical revision timestamp that could make a
    deterministic PIT choice safe.
    """

    required = {"ts_code", "ann_date", "end_date", "update_flag", *FINANCIAL_FIELD_MAP}
    missing = sorted(required - set(rows.columns))
    if missing:
        raise ValueError(f"financial rows missing columns: {missing}")
    if rows.empty:
        raise ValueError("financial rows are empty")
    calendar = pd.DatetimeIndex(pd.to_datetime(list(trading_dates))).dropna().sort_values().unique()
    if calendar.empty:
        raise ValueError("trading_dates are empty")

    frame = rows[list(required)].copy()
    frame["ts_code"] = frame["ts_code"].astype("string").str.strip()
    frame["ann_date"] = _parse_vendor_date(frame["ann_date"])
    frame["end_date"] = _parse_vendor_date(frame["end_date"])
    if frame[["ts_code", "ann_date", "end_date"]].isna().any().any():
        raise ValueError("financial rows contain invalid identifiers or dates")
    if bool((frame["ann_date"] < frame["end_date"]).any()):
        raise ValueError("financial announcement predates its report period")

    metric_columns = list(FINANCIAL_FIELD_MAP)
    for column in metric_columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["update_flag"] = frame["update_flag"].astype("string")
    frame["_update_priority"] = frame["update_flag"].str.upper().isin({"1", "Y", "TRUE"})
    frame = frame.drop_duplicates(
        ["ts_code", "ann_date", "end_date", "update_flag", *metric_columns],
        keep="last",
    ).reset_index(drop=True)
    revision_keys = ["ts_code", "ann_date", "end_date"]
    frame["_metric_hash"] = pd.util.hash_pandas_object(
        frame[metric_columns], index=False
    ).to_numpy()
    revision_groups = frame.groupby(
        revision_keys, observed=True, dropna=False, sort=False
    )
    distinct_values = revision_groups["_metric_hash"].transform("nunique")
    updated_count = revision_groups["_update_priority"].transform("sum")
    ambiguous_mask = distinct_values.gt(1) & updated_count.ne(1)
    if bool(ambiguous_mask.any()):
        ambiguous = (
            frame.loc[ambiguous_mask, revision_keys]
            .drop_duplicates()
            .head(3)
            .astype(str)
            .agg("/".join, axis=1)
            .tolist()
        )
        raise ValueError(
            "ambiguous financial revisions at identical announcement time: "
            f"{ambiguous}"
        )
    frame = (
        frame.sort_values([*revision_keys, "_update_priority"], kind="mergesort")
        .drop_duplicates(revision_keys, keep="last")
        .reset_index(drop=True)
    )

    ann_values = frame["ann_date"].to_numpy(dtype="datetime64[ns]")
    calendar_values = calendar.to_numpy(dtype="datetime64[ns]")
    positions = np.searchsorted(calendar_values, ann_values, side="right")
    has_next_session = positions < len(calendar_values)
    frame = frame.loc[has_next_session].copy()
    frame["financial_available_date"] = calendar_values[positions[has_next_session]]
    if frame.empty:
        raise ValueError("no financial announcement has a following trading session")
    if bool((frame["financial_available_date"] <= frame["ann_date"]).any()):
        raise AssertionError("financial availability must be strictly after announcement")

    # Build a compact state-change timeline.  A late revision to an older
    # period never displaces a newer report that was already available.
    frame = frame.sort_values(
        [
            "ts_code",
            "financial_available_date",
            "end_date",
            "ann_date",
            "_update_priority",
        ],
        kind="mergesort",
    )
    selected: list[int] = []
    for _, group in frame.groupby("ts_code", sort=False, observed=True):
        current_period = pd.Timestamp.min
        for index, row in group.iterrows():
            report_period = pd.Timestamp(row["end_date"])
            if report_period >= current_period:
                selected.append(index)
                current_period = report_period
    timeline = frame.loc[selected].copy()
    timeline = timeline.rename(
        columns={
            "ts_code": "ticker",
            "end_date": "financial_report_period",
            "ann_date": "financial_ann_date",
            "update_flag": "financial_update_flag",
            **FINANCIAL_FIELD_MAP,
        }
    )
    timeline["financial_pit_valid"] = True
    timeline["fundamental_roic"] = timeline["roic_pit"]
    timeline["fundamental_q_ocf_to_sales"] = timeline["q_ocf_to_sales_pit"]
    timeline["fundamental_debt_to_assets"] = timeline["debt_to_assets_pit"]
    keep = [
        "ticker",
        *FINANCIAL_METADATA_COLUMNS,
        *FINANCIAL_FIELD_MAP.values(),
        *FUNDAMENTAL_COLUMNS[:-1],
    ]
    return timeline[keep].sort_values(
        ["ticker", "financial_available_date", "financial_report_period"],
        kind="mergesort",
    ).reset_index(drop=True)


@dataclass(frozen=True)
class _FinancialIndex:
    timelines: Mapping[str, pd.DataFrame]

    @classmethod
    def from_frame(cls, frame: pd.DataFrame) -> "_FinancialIndex":
        return cls(
            {
                str(ticker): group.reset_index(drop=True)
                for ticker, group in frame.groupby("ticker", sort=False, observed=True)
            }
        )

    def lookup(self, tickers: pd.Series, dates: pd.Series) -> pd.DataFrame:
        size = len(tickers)
        result: dict[str, Any] = {
            "financial_report_period": np.full(size, np.datetime64("NaT"), dtype="datetime64[ns]"),
            "financial_ann_date": np.full(size, np.datetime64("NaT"), dtype="datetime64[ns]"),
            "financial_available_date": np.full(
                size, np.datetime64("NaT"), dtype="datetime64[ns]"
            ),
            "financial_update_flag": np.full(size, None, dtype=object),
            "financial_pit_valid": np.zeros(size, dtype=bool),
        }
        for output in FINANCIAL_FIELD_MAP.values():
            result[output] = np.full(size, np.nan, dtype=float)
        for output in FUNDAMENTAL_COLUMNS:
            result[output] = np.full(size, np.nan, dtype=float)

        ticker_values = tickers.astype("string").to_numpy(dtype=object)
        date_values = pd.to_datetime(dates, errors="coerce").to_numpy(dtype="datetime64[ns]")
        positions_by_ticker: dict[str, list[int]] = {}
        for position, ticker in enumerate(ticker_values):
            if ticker is not None and not pd.isna(ticker):
                positions_by_ticker.setdefault(str(ticker), []).append(position)
        for ticker, row_positions in positions_by_ticker.items():
            timeline = self.timelines.get(ticker)
            if timeline is None or timeline.empty:
                continue
            positions = np.asarray(row_positions, dtype=np.int64)
            available = timeline["financial_available_date"].to_numpy(dtype="datetime64[ns]")
            matches = np.searchsorted(available, date_values[positions], side="right") - 1
            valid = matches >= 0
            if not bool(valid.any()):
                continue
            target_positions = positions[valid]
            source_positions = matches[valid]
            for column in result:
                if column == "fundamental_age_days":
                    continue
                values = timeline[column].to_numpy()
                result[column][target_positions] = values[source_positions]
        report_period = result["financial_report_period"]
        valid_period = ~np.isnat(report_period) & ~np.isnat(date_values)
        result["fundamental_age_days"][valid_period] = (
            date_values[valid_period] - report_period[valid_period]
        ).astype("timedelta64[D]").astype(float)
        return pd.DataFrame(result)


def _name_has_st_marker(values: pd.Series) -> pd.Series:
    # Historical A-share risk-warning names use ST, *ST, SST or S*ST prefixes.
    return values.astype("string").str.match(
        r"^(?:S\*?|\*)?ST",
        case=False,
        na=False,
    )


def _resolve_vendor_code_aliases(
    members: pd.DataFrame,
    as_of_date: pd.Timestamp,
    aliases: Iterable[Mapping[str, Any]],
) -> pd.DataFrame:
    """Resolve only explicitly verified, interval-bounded security aliases."""

    resolved = members.copy()
    resolved["vendor_ts_code_pit"] = resolved["ts_code"].astype("string")
    resolved["security_alias_applied_pit"] = False
    resolved["security_alias_source"] = pd.Series(pd.NA, index=resolved.index, dtype="string")
    seen_intervals: dict[str, list[tuple[pd.Timestamp, pd.Timestamp]]] = {}
    for raw in aliases:
        alias = dict(raw)
        required = {
            "canonical_ts_code",
            "vendor_ts_code",
            "effective_from",
            "effective_to",
            "source",
        }
        missing = sorted(required - set(alias))
        if missing:
            raise ValueError(f"security code alias missing fields: {missing}")
        canonical = str(alias["canonical_ts_code"]).strip()
        vendor = str(alias["vendor_ts_code"]).strip()
        start = pd.Timestamp(str(alias["effective_from"]))
        end = pd.Timestamp(str(alias["effective_to"]))
        source = str(alias["source"]).strip()
        if not canonical or not vendor or canonical == vendor or not source:
            raise ValueError("security code alias identifiers/source must be explicit")
        if start >= end:
            raise ValueError(f"invalid security code alias interval for {canonical}")
        intervals = seen_intervals.setdefault(canonical, [])
        if any(start < prior_end and prior_start < end for prior_start, prior_end in intervals):
            raise ValueError(f"overlapping security code alias intervals for {canonical}")
        intervals.append((start, end))
        if not start <= as_of_date < end:
            continue
        mask = resolved["ts_code"].eq(canonical)
        resolved.loc[mask, "vendor_ts_code_pit"] = vendor
        resolved.loc[mask, "security_alias_applied_pit"] = True
        resolved.loc[mask, "security_alias_source"] = source
    return resolved


def build_monthly_reference_state(
    layout: RuntimeLayout,
    *,
    security_code_aliases: Iterable[Mapping[str, Any]] = (),
) -> pd.DataFrame:
    """Join every frozen member to its exact month-end industry/name/ST state."""

    membership = pd.read_parquet(layout.membership_path)
    required = {
        "ts_code",
        "membership_month",
        "as_of_date",
        "effective_start_date",
        "effective_end_date",
    }
    missing = sorted(required - set(membership.columns))
    if missing:
        raise ValueError(f"membership missing columns: {missing}")
    if bool(membership.duplicated(["ts_code", "membership_month"]).any()):
        raise ValueError("membership contains duplicate ticker/month keys")
    membership = membership.copy()
    membership["ts_code"] = membership["ts_code"].astype("string")
    membership["as_of_date"] = pd.to_datetime(membership["as_of_date"], errors="coerce")
    membership["effective_start_date"] = pd.to_datetime(
        membership["effective_start_date"], errors="coerce"
    )
    if membership[["as_of_date", "effective_start_date"]].isna().any().any():
        raise ValueError("membership contains invalid month-end dates")
    if bool((membership["effective_start_date"] <= membership["as_of_date"]).any()):
        raise ValueError("membership state must become effective after its as_of_date")

    states: list[pd.DataFrame] = []
    for month, members in membership.groupby("membership_month", sort=True, observed=True):
        as_of_values = members["as_of_date"].drop_duplicates()
        if len(as_of_values) != 1:
            raise ValueError(f"membership month {month} has multiple as_of dates")
        as_of = pd.Timestamp(as_of_values.iloc[0]).strftime("%Y-%m-%d")
        bak_path = enrichment_partition_path(layout.raw_root, "bak_basic", as_of)
        st_path = enrichment_partition_path(layout.raw_root, "stock_st", as_of)
        if not bak_path.is_file():
            raise FileNotFoundError(f"missing bak_basic month-end partition for {month}/{as_of}")
        bak = pd.read_parquet(bak_path)
        audit_enrichment_partition(bak, "bak_basic", as_of)
        bak = bak[["ts_code", "name", "industry", "list_date"]].copy()
        bak["ts_code"] = bak["ts_code"].astype("string")
        bak = bak.rename(columns={"ts_code": "vendor_ts_code_pit"})
        if st_path.is_file():
            st = pd.read_parquet(st_path)
            audit_enrichment_partition(st, "stock_st", as_of)
            st = st[["ts_code", "name", "type", "type_name"]].copy()
            st["ts_code"] = st["ts_code"].astype("string")
            st = st.rename(
                columns={
                    "ts_code": "vendor_ts_code_pit",
                    "name": "st_source_name",
                    "type": "st_type_pit",
                    "type_name": "st_type_name_pit",
                }
            )
            st_status = "monthly_stock_st_verified"
        else:
            st = pd.DataFrame(
                columns=[
                    "vendor_ts_code_pit",
                    "st_source_name",
                    "st_type_pit",
                    "st_type_name_pit",
                ]
            )
            st["vendor_ts_code_pit"] = st["vendor_ts_code_pit"].astype("string")
            st_status = "monthly_name_verified_daily_events_unavailable"
        selected = members[["ts_code", "membership_month", "as_of_date", "effective_start_date"]]
        selected = _resolve_vendor_code_aliases(
            selected,
            pd.Timestamp(as_of),
            security_code_aliases,
        )
        state = selected.merge(
            bak,
            on="vendor_ts_code_pit",
            how="left",
            validate="one_to_one",
            indicator=True,
        )
        state["reference_verified_pit"] = state["_merge"].eq("both")
        missing_bak = state.loc[~state["reference_verified_pit"], "ts_code"].astype(str).tolist()
        if len(missing_bak) / len(state) > 0.01:
            raise ValueError(
                f"bak_basic/{as_of} misses too many membership securities: "
                f"{len(missing_bak)}/{len(state)} {missing_bak[:5]}"
            )
        state = state.drop(columns="_merge").merge(
            st,
            on="vendor_ts_code_pit",
            how="left",
            validate="one_to_one",
        )
        listed_by_stock_st = state["st_type_pit"].notna()
        listed_by_name = _name_has_st_marker(state["name"])
        state["is_st_pit"] = (listed_by_stock_st | listed_by_name).astype(bool)
        name_only = listed_by_name & ~listed_by_stock_st
        state.loc[name_only, "st_type_pit"] = "NAME_MARKER"
        state.loc[name_only, "st_type_name_pit"] = "名称风险警示"
        state["state_as_of_date"] = state["as_of_date"]
        state["state_available_date"] = state["effective_start_date"]
        state["industry_pit"] = state["industry"].astype("string")
        state["name_pit"] = state["name"].astype("string")
        state["st_known_pit"] = state["reference_verified_pit"].astype(bool)
        state["st_filter_status"] = st_status
        state.loc[
            ~state["reference_verified_pit"], "st_filter_status"
        ] = "monthly_reference_missing_excluded"
        state["_state_present"] = True
        states.append(
            state[
                [
                    "ts_code",
                    "membership_month",
                    *REFERENCE_COLUMNS,
                    "_state_present",
                ]
            ]
        )
    result = pd.concat(states, ignore_index=True)
    if len(result) != len(membership):
        raise AssertionError("monthly reference state changed membership row count")
    return result


def _load_financial_rows(layout: RuntimeLayout) -> tuple[pd.DataFrame, list[Path]]:
    paths = sorted((layout.raw_root / "fina_indicator_vip").glob("period=*/part-000.parquet"))
    if not paths:
        raise FileNotFoundError("no fina_indicator_vip quarterly partitions are available")
    frames: list[pd.DataFrame] = []
    for path in paths:
        match = re.fullmatch(r"period=(\d{4}-\d{2}-\d{2})", path.parent.name)
        if not match:
            continue
        frame = pd.read_parquet(path)
        audit_enrichment_partition(frame, "fina_indicator_vip", match.group(1))
        frames.append(frame)
    if not frames:
        raise ValueError("no valid fina_indicator_vip partitions were found")
    return pd.concat(frames, ignore_index=True), paths


def _monthly_index(state: pd.DataFrame) -> pd.DataFrame:
    return state.set_index(["ts_code", "membership_month"], verify_integrity=True)


def _reference_lookup(
    index: pd.DataFrame,
    tickers: pd.Series,
    months: pd.Series,
) -> pd.DataFrame:
    keys = pd.MultiIndex.from_arrays(
        [tickers.astype("string"), months.astype("string")],
        names=["ts_code", "membership_month"],
    )
    return index.reindex(keys).reset_index(drop=True)


def _nullable_bool(values: pd.Series) -> pd.Series:
    """Convert reference joins to bool without object-fill downcast warnings."""

    return values.astype("boolean").fillna(False).astype(bool)


def _stage_features(
    source: Path,
    target: Path,
    monthly: pd.DataFrame,
    financial: _FinancialIndex,
    *,
    batch_size: int,
) -> dict[str, int]:
    parquet = pq.ParquetFile(source)
    required = {"ticker", "date", "membership_month", "eligible"}
    missing = sorted(required - set(parquet.schema_arrow.names))
    if missing:
        raise ValueError(f"features missing enrichment keys: {missing}")
    target.unlink(missing_ok=True)
    writer: pq.ParquetWriter | None = None
    row_count = valid_financial = excluded = industry_known = 0
    try:
        for batch in parquet.iter_batches(batch_size=batch_size):
            table = pa.Table.from_batches([batch])
            keys = table.select(["ticker", "date", "membership_month", "eligible"]).to_pandas()
            reference = _reference_lookup(monthly, keys["ticker"], keys["membership_month"])
            if reference["_state_present"].isna().any():
                raise ValueError("features contain ticker/month rows without audited reference state")
            financial_values = financial.lookup(keys["ticker"], keys["date"])
            base_eligible = (
                table["eligible_pre_pit"].to_pandas().fillna(False).astype(bool)
                if "eligible_pre_pit" in table.column_names
                else keys["eligible"].fillna(False).astype(bool)
            )
            is_st = _nullable_bool(reference["is_st_pit"])
            reference_verified = _nullable_bool(reference["reference_verified_pit"])
            new_eligible = base_eligible & reference_verified & ~is_st
            updates: dict[str, Any] = {
                **{column: reference[column] for column in REFERENCE_COLUMNS},
                **{column: financial_values[column] for column in financial_values.columns},
                "eligible_pre_pit": base_eligible,
                "eligible": new_eligible,
                "historical_st_known": _nullable_bool(reference["st_known_pit"]),
                "is_st_at_asof": is_st,
            }
            enriched = _replace_table_columns(table, updates)
            if writer is None:
                writer = pq.ParquetWriter(target, enriched.schema, compression="zstd")
            writer.write_table(enriched)
            row_count += len(keys)
            valid_financial += int(financial_values["financial_pit_valid"].sum())
            excluded += int((base_eligible & ~new_eligible).sum())
            industry_known += int(reference["industry_pit"].notna().sum())
    finally:
        if writer is not None:
            writer.close()
    if row_count != parquet.metadata.num_rows:
        target.unlink(missing_ok=True)
        raise RuntimeError("feature enrichment changed row count")
    return {
        "row_count": row_count,
        "financial_valid_row_count": valid_financial,
        "st_excluded_row_count": excluded,
        "industry_known_row_count": industry_known,
    }


def _stage_execution(
    source: Path,
    target: Path,
    monthly: pd.DataFrame,
    *,
    batch_size: int,
) -> dict[str, int]:
    parquet = pq.ParquetFile(source)
    required = {"ticker", "date", "eligible", "universe_member"}
    missing = sorted(required - set(parquet.schema_arrow.names))
    if missing:
        raise ValueError(f"execution missing enrichment keys: {missing}")
    target.unlink(missing_ok=True)
    writer: pq.ParquetWriter | None = None
    row_count = excluded = member_rows = 0
    try:
        for batch in parquet.iter_batches(batch_size=batch_size):
            table = pa.Table.from_batches([batch])
            keys = table.select(["ticker", "date", "eligible", "universe_member"]).to_pandas()
            months = pd.to_datetime(keys["date"], errors="coerce").dt.strftime("%Y-%m")
            reference = _reference_lookup(monthly, keys["ticker"], months)
            members = keys["universe_member"].fillna(False).astype(bool)
            if reference.loc[members, "_state_present"].isna().any():
                raise ValueError("execution contains member rows without audited reference state")
            base_eligible = (
                table["eligible_pre_pit"].to_pandas().fillna(False).astype(bool)
                if "eligible_pre_pit" in table.column_names
                else keys["eligible"].fillna(False).astype(bool)
            )
            is_st = _nullable_bool(reference["is_st_pit"])
            reference_verified = _nullable_bool(reference["reference_verified_pit"])
            new_eligible = base_eligible & ~(
                members & (is_st | ~reference_verified)
            )
            updates: dict[str, Any] = {
                **{column: reference[column] for column in REFERENCE_COLUMNS},
                "eligible_pre_pit": base_eligible,
                "eligible": new_eligible,
            }
            enriched = _replace_table_columns(table, updates)
            if writer is None:
                writer = pq.ParquetWriter(target, enriched.schema, compression="zstd")
            writer.write_table(enriched)
            row_count += len(keys)
            member_rows += int(members.sum())
            excluded += int((base_eligible & ~new_eligible).sum())
    finally:
        if writer is not None:
            writer.close()
    if row_count != parquet.metadata.num_rows:
        target.unlink(missing_ok=True)
        raise RuntimeError("execution enrichment changed row count")
    return {
        "row_count": row_count,
        "member_row_count": member_rows,
        "st_excluded_row_count": excluded,
    }


def _stage_membership(
    source: Path,
    target: Path,
    monthly_state: pd.DataFrame,
) -> dict[str, int]:
    membership = pd.read_parquet(source)
    reference = _reference_lookup(
        _monthly_index(monthly_state),
        membership["ts_code"],
        membership["membership_month"],
    )
    if reference["_state_present"].isna().any():
        raise ValueError("membership rows lack audited reference state")
    output = membership.copy()
    for column in REFERENCE_COLUMNS:
        output[column] = reference[column].to_numpy()
    output["historical_st_known"] = _nullable_bool(reference["st_known_pit"]).to_numpy()
    output["is_st_at_asof"] = reference["is_st_pit"].astype(bool).to_numpy()
    base_eligible = (
        output["eligible_pre_pit"].fillna(False).astype(bool)
        if "eligible_pre_pit" in output.columns
        else pd.Series(True, index=output.index)
    )
    output["eligible_pre_pit"] = base_eligible
    output["eligible"] = (
        base_eligible
        & _nullable_bool(reference["reference_verified_pit"]).to_numpy()
        & ~_nullable_bool(reference["is_st_pit"]).to_numpy()
    )
    target.unlink(missing_ok=True)
    output.to_parquet(target, index=False, compression="zstd")
    if pq.ParquetFile(target).metadata.num_rows != len(membership):
        target.unlink(missing_ok=True)
        raise RuntimeError("membership enrichment changed row count")
    return {
        "row_count": int(len(output)),
        "st_member_count": int(output["is_st_at_asof"].sum()),
        "industry_known_count": int(output["industry_pit"].notna().sum()),
        "reference_missing_count": int((~output["reference_verified_pit"]).sum()),
        "security_alias_applied_count": int(
            output["security_alias_applied_pit"].sum()
        ),
    }


def _recover_transaction(journal_path: Path) -> None:
    if not journal_path.is_file():
        return
    payload = json.loads(journal_path.read_text(encoding="utf-8"))
    for raw in reversed(payload.get("items") or []):
        item = dict(raw)
        target = Path(str(item["target"]))
        staged = Path(str(item["staged"]))
        backup = Path(str(item["backup"]))
        if backup.is_file():
            target.unlink(missing_ok=True)
            backup.replace(target)
        elif not bool(item.get("had_target")):
            target.unlink(missing_ok=True)
        staged.unlink(missing_ok=True)
    journal_path.unlink(missing_ok=True)


def _commit_transaction(items: list[dict[str, Any]], journal_path: Path) -> None:
    _recover_transaction(journal_path)
    for item in items:
        staged = Path(str(item["staged"]))
        target = Path(str(item["target"]))
        if not staged.is_file():
            raise FileNotFoundError(f"missing staged enrichment file: {staged}")
        item["had_target"] = target.is_file()
        item["staged_sha256"] = sha256_file(staged)
    _json_atomic(
        journal_path,
        {
            "schema_version": 1,
            "status": "prepared",
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "items": items,
        },
    )
    try:
        for item in items:
            target = Path(str(item["target"]))
            staged = Path(str(item["staged"]))
            backup = Path(str(item["backup"]))
            backup.unlink(missing_ok=True)
            if target.is_file():
                target.replace(backup)
            staged.replace(target)
            if sha256_file(target) != item["staged_sha256"]:
                raise RuntimeError(f"installed enrichment hash mismatch: {target}")
    except BaseException:
        _recover_transaction(journal_path)
        raise
    journal_path.unlink(missing_ok=True)
    for item in items:
        Path(str(item["backup"])).unlink(missing_ok=True)


def enrich_top500_store(
    *,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    layout: RuntimeLayout | None = None,
    batch_size: int = 100_000,
) -> dict[str, Any]:
    """Atomically enrich canonical features, execution and membership Parquet."""

    if int(batch_size) <= 0:
        raise ValueError("batch_size must be positive")
    config = load_data_config(config_path)
    resolved = layout or RuntimeLayout.from_config(config, config_path=config_path)
    resolved.ensure_directories()
    for path in (resolved.features_path, resolved.execution_path, resolved.membership_path):
        if not path.is_file():
            raise FileNotFoundError(f"missing canonical Parquet: {path}")
    journal_path = resolved.top500_root / "enrichment-transaction.json"
    _recover_transaction(journal_path)

    alias_config = (config.get("enrichment") or {}).get("security_code_aliases") or []
    monthly_state = build_monthly_reference_state(
        resolved,
        security_code_aliases=alias_config,
    )
    monthly = _monthly_index(monthly_state)
    financial_rows, financial_paths = _load_financial_rows(resolved)
    financial_timeline = prepare_financial_pit(
        financial_rows,
        canonical_trading_dates(resolved),
    )
    financial_index = _FinancialIndex.from_frame(financial_timeline)

    feature_stage = resolved.features_path.with_name("features.enrich.partial.parquet")
    execution_stage = resolved.execution_path.with_name("execution.enrich.partial.parquet")
    membership_stage = resolved.membership_path.with_name("membership.enrich.partial.parquet")
    manifest_path = resolved.top500_root / "enrichment-manifest.json"
    manifest_stage = manifest_path.with_name("enrichment-manifest.partial.json")
    stages = (feature_stage, execution_stage, membership_stage, manifest_stage)
    for path in stages:
        path.unlink(missing_ok=True)
    before = {
        "features": sha256_file(resolved.features_path),
        "execution": sha256_file(resolved.execution_path),
        "membership": sha256_file(resolved.membership_path),
    }
    try:
        membership_summary = _stage_membership(
            resolved.membership_path,
            membership_stage,
            monthly_state,
        )
        feature_summary = _stage_features(
            resolved.features_path,
            feature_stage,
            monthly,
            financial_index,
            batch_size=int(batch_size),
        )
        execution_summary = _stage_execution(
            resolved.execution_path,
            execution_stage,
            monthly,
            batch_size=int(batch_size),
        )
        after = {
            "features": sha256_file(feature_stage),
            "execution": sha256_file(execution_stage),
            "membership": sha256_file(membership_stage),
        }
        manifest = {
            "schema_version": 1,
            "status": "complete",
            "completed_at_utc": datetime.now(timezone.utc).isoformat(),
            "availability_rule": "financial announcements become usable on the next trading day",
            "before": before,
            "after": after,
            "financial_partition_count": len(financial_paths),
            "financial_event_count": int(len(financial_timeline)),
            "membership_month_count": int(monthly_state["membership_month"].nunique()),
            "security_code_alias_count": len(alias_config),
            "security_alias_applied_member_count": int(
                monthly_state["security_alias_applied_pit"].sum()
            ),
            "security_code_aliases": [dict(row) for row in alias_config],
            "features": feature_summary,
            "execution": execution_summary,
            "membership": membership_summary,
            "source_partitions": [
                {"path": str(path), "sha256": sha256_file(path)}
                for path in [
                    *financial_paths,
                    *sorted((resolved.raw_root / "bak_basic").glob("trade_date=*/part-000.parquet")),
                    *sorted((resolved.raw_root / "stock_st").glob("trade_date=*/part-000.parquet")),
                ]
            ],
        }
        _json_atomic(manifest_stage, manifest)
        items = [
            {
                "target": str(resolved.features_path),
                "staged": str(feature_stage),
                "backup": str(resolved.features_path.with_name("features.enrich.previous.parquet")),
            },
            {
                "target": str(resolved.execution_path),
                "staged": str(execution_stage),
                "backup": str(
                    resolved.execution_path.with_name("execution.enrich.previous.parquet")
                ),
            },
            {
                "target": str(resolved.membership_path),
                "staged": str(membership_stage),
                "backup": str(
                    resolved.membership_path.with_name("membership.enrich.previous.parquet")
                ),
            },
            {
                "target": str(manifest_path),
                "staged": str(manifest_stage),
                "backup": str(manifest_path.with_name("enrichment-manifest.previous.json")),
            },
        ]
        _commit_transaction(items, journal_path)
    except BaseException:
        for path in stages:
            path.unlink(missing_ok=True)
        raise
    return {**manifest, "manifest_path": str(manifest_path)}


__all__ = [
    "FINANCIAL_FIELD_MAP",
    "FINANCIAL_METADATA_COLUMNS",
    "FUNDAMENTAL_COLUMNS",
    "REFERENCE_COLUMNS",
    "build_monthly_reference_state",
    "canonical_trading_dates",
    "enrich_top500_store",
    "prepare_financial_pit",
]

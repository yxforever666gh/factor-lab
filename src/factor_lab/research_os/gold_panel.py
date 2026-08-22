"""Research-ready point-in-time Gold market panel.

This module is the only supported bridge from reconciled Silver observations to
historical research input.  It deliberately does not read the legacy
``expanded_*`` artifacts.  Every input file is bound to an accepted Silver
snapshot and the resulting parent closure is carried into the Gold manifest.

The panel contains features knowable at the decision cutoff and a separately
marked research label.  The label is never an execution input: its availability
is the availability of the future exit-open observation.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import tempfile
import warnings
from typing import Any, Iterable, Mapping, Protocol, Sequence
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from .contracts import (
    DataQualityStatus,
    DataSnapshotRef,
    LabelSpec,
    SnapshotTier,
    UniverseSpec,
)
from .data_quality import sha256_path
from .legacy_bronze_seed import FORBIDDEN_PROMOTION_TRUST_LABELS
from .snapshots import SNAPSHOT_SCHEMA_VERSION, verify_immutable_snapshot_manifest


GOLD_PANEL_SCHEMA_VERSION = "research-os/gold-panel/v1"
SHANGHAI = ZoneInfo("Asia/Shanghai")
DEFAULT_SILVER_SNAPSHOT_DISCOVERY_CAP = 50_000
_CATALOG_SNAPSHOT_PAGE_SIZE = 1_000

CORE_DAILY_DATASETS = ("daily", "daily_basic", "adj_factor", "trade_status")
REFERENCE_DATASETS = (
    "trade_calendar",
    "stock_basic",
    "historical_st",
    "industry_classification",
    "company_action",
)
DEFAULT_REQUIRED_DATASETS = (*CORE_DAILY_DATASETS, *REFERENCE_DATASETS)

_DATASET_ALIASES = {
    "suspend_d": "trade_status",
    "suspend": "trade_status",
    "suspension": "trade_status",
    "suspension_status": "trade_status",
    "stock_limit": "trade_status",
    "stk_limit": "trade_status",
    "namechange": "historical_st",
    "industry": "industry_classification",
    "stock_industry": "industry_classification",
    "dividend": "company_action",
    "corporate_action": "company_action",
    "trade_cal": "trade_calendar",
    "stock_basic_l": "stock_basic",
    "stock_basic_p": "stock_basic",
    "stock_basic_d": "stock_basic",
    "stock_basic_g": "stock_basic",
}

_SECURITY_MASTER_STATUS_DATASETS = {
    "stock_basic_l": "L",
    "stock_basic_p": "P",
    "stock_basic_d": "D",
    "stock_basic_g": "G",
}


class GoldPanelError(RuntimeError):
    """A fail-closed Gold construction or provenance error."""


class SnapshotCatalog(Protocol):
    def get_snapshot(self, snapshot_id: str): ...


@dataclass(frozen=True)
class SilverSnapshotInput:
    """One local Silver parquet bound to its catalog reference."""

    reference: DataSnapshotRef
    path: Path


@dataclass(frozen=True)
class GoldPanelBuildResult:
    panel: pd.DataFrame
    membership: pd.DataFrame
    audit: Mapping[str, Any]
    parent_snapshot_ids: tuple[str, ...]


@dataclass(frozen=True)
class GoldPanelArtifacts:
    panel_path: Path
    membership_path: Path
    audit_path: Path
    build_spec_path: Path
    parent_snapshot_ids: tuple[str, ...]
    audit: Mapping[str, Any]

    @property
    def manifest_paths(self) -> tuple[Path, ...]:
        return (
            self.panel_path,
            self.membership_path,
            self.audit_path,
            self.build_spec_path,
        )


def _canonical_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        dict(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")


def _manifest_identity(reference: DataSnapshotRef) -> str:
    manifest = dict(reference.manifest or {})
    try:
        payload = {
            "schema_version": manifest["schema_version"],
            "tier": manifest["tier"],
            "as_of": manifest["as_of"],
            "parent_snapshot_ids": list(manifest["parent_snapshot_ids"]),
            "environment_hashes": dict(manifest["environment_hashes"]),
            "quality_status": manifest["quality_status"],
            "trust_labels": list(manifest["trust_labels"]),
            "files": list(manifest["files"]),
        }
        if "trading_calendar" in manifest:
            payload["trading_calendar"] = dict(manifest["trading_calendar"])
    except (KeyError, TypeError, ValueError) as exc:
        raise GoldPanelError("Silver snapshot manifest structure is invalid") from exc
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def verify_silver_snapshot_input(
    item: SilverSnapshotInput,
    *,
    lake_root: str | Path,
) -> None:
    """Verify identity, quality and exact file membership for a Silver input."""

    reference = item.reference
    if reference.tier is not SnapshotTier.SILVER:
        raise GoldPanelError(f"Gold parent is not Silver: {reference.snapshot_id}")
    if reference.quality_status is not DataQualityStatus.ACCEPTED:
        raise GoldPanelError(f"Silver parent is not accepted: {reference.snapshot_id}")
    forbidden = sorted(
        set(map(str, reference.trust_labels)).intersection(
            FORBIDDEN_PROMOTION_TRUST_LABELS
        )
    )
    if forbidden:
        raise GoldPanelError(
            f"Silver parent carries non-promotable trust labels: {forbidden}"
        )
    if reference.manifest.get("schema_version") != SNAPSHOT_SCHEMA_VERSION:
        raise GoldPanelError("Silver parent lacks an immutable Research OS manifest")
    expected = _manifest_identity(reference)
    if not (
        expected
        == reference.snapshot_id
        == reference.content_hash
        == reference.manifest.get("snapshot_id")
    ):
        raise GoldPanelError("Silver reference and immutable manifest identity differ")
    if not reference.parent_snapshot_ids:
        raise GoldPanelError("Silver snapshot has no Bronze parents")
    verification = verify_immutable_snapshot_manifest(
        reference.manifest, base_dir=lake_root
    )
    if not verification["valid"]:
        raise GoldPanelError(
            f"Silver snapshot file verification failed: {verification['errors']}"
        )
    root = Path(lake_root).resolve()
    path = item.path.resolve()
    try:
        relative = path.relative_to(root).as_posix()
    except ValueError as exc:
        raise GoldPanelError(f"Silver input escapes lake root: {path}") from exc
    matches = [
        row
        for row in reference.manifest.get("files", ())
        if str(row.get("path") or "") == relative
        and str(row.get("sha256") or "") == sha256_path(path)
        and int(row.get("size_bytes", -1)) == path.stat().st_size
    ]
    if len(matches) != 1:
        raise GoldPanelError(
            f"Silver parquet is not uniquely bound to parent {reference.snapshot_id}"
        )


def verify_parent_snapshot_closure(
    inputs: Sequence[SilverSnapshotInput],
    *,
    catalog: SnapshotCatalog,
    lake_root: str | Path,
) -> tuple[str, ...]:
    """Verify every Silver file and every declared Bronze parent from the catalog."""

    if not inputs:
        raise GoldPanelError("Gold construction requires Silver snapshot inputs")
    snapshot_ids = [item.reference.snapshot_id for item in inputs]
    if len(snapshot_ids) != len(set(snapshot_ids)):
        raise GoldPanelError("duplicate Silver snapshot inputs would hide parent lineage")
    for item in inputs:
        verify_silver_snapshot_input(item, lake_root=lake_root)
        catalog_record = catalog.get_snapshot(item.reference.snapshot_id)
        if catalog_record is None:
            raise GoldPanelError(
                f"Silver parent is absent from the catalog: {item.reference.snapshot_id}"
            )
        if catalog_record.reference != item.reference:
            raise GoldPanelError("catalog Silver reference differs from supplied evidence")
        for parent_id in item.reference.parent_snapshot_ids:
            parent = catalog.get_snapshot(parent_id)
            if parent is None:
                raise GoldPanelError(f"missing Bronze ancestor: {parent_id}")
            if parent.reference.tier is not SnapshotTier.BRONZE:
                raise GoldPanelError(f"Silver ancestor is not Bronze: {parent_id}")
            if parent.reference.quality_status is not DataQualityStatus.ACCEPTED:
                raise GoldPanelError(f"Bronze ancestor is not accepted: {parent_id}")
            forbidden = sorted(
                set(map(str, parent.reference.trust_labels)).intersection(
                    FORBIDDEN_PROMOTION_TRUST_LABELS
                )
            )
            if forbidden:
                raise GoldPanelError(
                    "Bronze ancestor carries non-promotable trust labels: "
                    f"{forbidden}"
                )
            if not (
                parent.reference.snapshot_id
                == parent.reference.content_hash
                == _manifest_identity(parent.reference)
            ):
                raise GoldPanelError(f"Bronze ancestor identity is invalid: {parent_id}")
    return tuple(sorted(snapshot_ids))


def discover_cataloged_silver_inputs(
    catalog: Any,
    *,
    lake_root: str | Path,
    limit: int = DEFAULT_SILVER_SNAPSHOT_DISCOVERY_CAP,
) -> tuple[SilverSnapshotInput, ...]:
    """Find every accepted Silver parquet up to an explicit safety cap.

    Catalog queries remain bounded to 1,000 rows and advance with a stable
    keyset cursor.  ``limit`` is a total safety cap, not a query page size.  If
    the catalog proves another matching row exists beyond the cap, discovery
    fails closed instead of publishing a Gold snapshot from partial history.
    """

    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise GoldPanelError("Silver snapshot discovery limit must be a positive integer")

    records: list[Any] = []
    seen_snapshot_ids: set[str] = set()
    after = None
    while True:
        remaining = limit - len(records)
        page = catalog.list_snapshot_page(
            limit=min(_CATALOG_SNAPSHOT_PAGE_SIZE, remaining),
            quality_status=DataQualityStatus.ACCEPTED,
            tier=SnapshotTier.SILVER,
            after=after,
        )
        if not page.records and page.next_cursor is not None:
            raise GoldPanelError("Silver snapshot pagination did not make progress")
        for record in page.records:
            snapshot_id = record.reference.snapshot_id
            if snapshot_id in seen_snapshot_ids:
                raise GoldPanelError(
                    f"Silver snapshot pagination repeated catalog row: {snapshot_id}"
                )
            seen_snapshot_ids.add(snapshot_id)
            records.append(record)
        if page.next_cursor is None:
            break
        if len(records) >= limit:
            raise GoldPanelError(
                "Silver snapshot discovery safety cap was reached while the catalog "
                "still has additional accepted Silver snapshots"
            )
        after = page.next_cursor

    root = Path(lake_root).resolve()
    result: list[SilverSnapshotInput] = []
    for record in records:
        entries = list(record.reference.manifest.get("files") or ())
        parquet_entries = [
            row
            for row in entries
            if str(row.get("path") or "").lower().endswith((".parquet", ".pq"))
        ]
        if len(parquet_entries) != 1:
            raise GoldPanelError(
                f"Silver snapshot {record.reference.snapshot_id} must declare exactly one parquet"
            )
        stored = str(parquet_entries[0].get("path") or "")
        pure = PurePosixPath(stored)
        if pure.is_absolute() or ".." in pure.parts:
            raise GoldPanelError(f"unsafe Silver manifest path: {stored}")
        path = (root / Path(*pure.parts)).resolve()
        result.append(SilverSnapshotInput(reference=record.reference, path=path))
    return tuple(sorted(result, key=lambda item: item.reference.snapshot_id))


def _parse_aware(value: Any, *, name: str) -> pd.Timestamp:
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(parsed):
        raise GoldPanelError(f"invalid {name}: {value!r}")
    return pd.Timestamp(parsed)


def _event_dates(values: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(values, errors="coerce", utc=True)
    if parsed.isna().any():
        raise GoldPanelError(f"Silver event_time contains {int(parsed.isna().sum())} invalid rows")
    return parsed.dt.tz_convert(SHANGHAI).dt.tz_localize(None).dt.normalize()


def _date_values(values: pd.Series) -> pd.Series:
    """Parse ISO dates and provider-style YYYYMMDD values consistently."""

    text = values.astype("string").str.strip()
    compact = text.str.fullmatch(r"\d{8}", na=False)
    parsed = pd.to_datetime(text.where(~compact), errors="coerce")
    if compact.any():
        parsed.loc[compact] = pd.to_datetime(
            text.loc[compact], format="%Y%m%d", errors="coerce"
        )
    return parsed.dt.normalize()


def _normalise_silver(frame: pd.DataFrame, *, as_of: datetime) -> pd.DataFrame:
    required = {"dataset", "event_time", "available_at", "ingested_at"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise GoldPanelError(f"Silver frame is missing columns: {missing}")
    result = frame.copy()
    raw_dataset = result["dataset"].astype("string").str.strip()
    status_from_partition = raw_dataset.map(_SECURITY_MASTER_STATUS_DATASETS)
    if status_from_partition.notna().any():
        if "queried_list_status" not in result:
            result["queried_list_status"] = pd.NA
        result.loc[status_from_partition.notna(), "queried_list_status"] = (
            status_from_partition.loc[status_from_partition.notna()]
        )
        if "list_status" not in result:
            result["list_status"] = pd.NA
        result.loc[status_from_partition.notna(), "list_status"] = (
            result.loc[status_from_partition.notna(), "list_status"]
            .astype("string")
            .fillna(status_from_partition.loc[status_from_partition.notna()])
        )
    result["dataset"] = (
        raw_dataset.replace(_DATASET_ALIASES)
    )
    result["trade_date"] = _event_dates(result["event_time"])
    for column in ("available_at", "ingested_at"):
        result[column] = pd.to_datetime(result[column], errors="coerce", utc=True)
        if result[column].isna().any():
            raise GoldPanelError(
                f"Silver {column} contains {int(result[column].isna().sum())} invalid rows"
            )
    # ``stock_basic`` is a current-state endpoint.  The listing fact can be
    # eventized from list_date, and a delisting fact from delist_date, but a
    # present-day P/G status has no historical effective timestamp.  Never
    # backdate those final states to the listing date: they become visible no
    # earlier than this immutable vendor ingestion.
    security = result["dataset"].eq("stock_basic")
    if security.any():
        status = result.get("queried_list_status", result.get("list_status")).astype(
            "string"
        )
        unproved_current_state = security & status.isin({"P", "G"})
        result.loc[unproved_current_state, "available_at"] = result.loc[
            unproved_current_state, ["available_at", "ingested_at"]
        ].max(axis=1)
        if "delist_date" in result:
            delist = _date_values(result["delist_date"])
            delist_release = pd.to_datetime(delist, errors="coerce").dt.tz_localize(
                SHANGHAI
            ) + pd.Timedelta(days=1, hours=18, minutes=30)
            delist_release = delist_release.dt.tz_convert("UTC")
            delisted = security & status.eq("D") & delist.notna()
            if delisted.any():
                result.loc[delisted, "available_at"] = pd.concat(
                    [
                        result.loc[delisted, "available_at"],
                        delist_release.loc[delisted],
                    ],
                    axis=1,
                ).max(axis=1)
    freeze = _parse_aware(as_of, name="as_of")
    result = result.loc[
        (result["available_at"] <= freeze) & (result["ingested_at"] <= freeze)
    ].copy()
    if result.empty:
        raise GoldPanelError("no Silver observations were available by the Gold freeze")
    # Multiple accepted Silver snapshots may cover the same vendor partition
    # after a revision/backfill.  Resolve the system-time view explicitly:
    # later ``ingested_at`` wins at this Gold freeze.  Equal-time conflicting
    # revisions remain ambiguous and therefore block instead of silently
    # overwriting one another.  All source snapshots still remain Gold parents,
    # so the earlier Gold snapshots are reproducible and unchanged.
    if "entity_key" in result and result["entity_key"].notna().all():
        identity = result["entity_key"].astype(str)
    else:
        identifiers = [
            name
            for name in ("ts_code", "ticker", "symbol", "exchange")
            if name in result.columns
        ]
        if not identifiers:
            raise GoldPanelError("Silver rows have no entity identity for revision selection")
        identity = result[identifiers].astype("string").fillna("").agg("|".join, axis=1)
    result["_gold_entity_identity"] = identity
    keys = ["dataset", "_gold_entity_identity", "trade_date"]
    latest_ingest = result.groupby(keys, dropna=False)["ingested_at"].transform("max")
    candidates = result.loc[result["ingested_at"].eq(latest_ingest)].copy()
    same_time_duplicates = candidates.duplicated(keys, keep=False)
    if same_time_duplicates.any():
        technical = {*keys, "available_at", "ingested_at"}
        comparison_columns = [name for name in candidates if name not in technical]
        for _, group in candidates.loc[same_time_duplicates].groupby(keys, dropna=False):
            if len(group[comparison_columns].drop_duplicates()) != 1:
                raise GoldPanelError(
                    "same-time Silver revisions conflict for one dataset/entity/date"
                )
        candidates = candidates.drop_duplicates(keys, keep="last")
    superseded = len(result) - len(candidates)
    candidates = candidates.drop(columns=["_gold_entity_identity"]).reset_index(drop=True)
    candidates.attrs["silver_revision_rows_superseded"] = superseded
    return candidates


def _ticker_column(frame: pd.DataFrame) -> str:
    for name in ("ts_code", "ticker", "symbol", "stock_code"):
        if name in frame.columns:
            return name
    raise GoldPanelError("Silver security dataset has no ts_code/ticker identifier")


def _dataset(frame: pd.DataFrame, name: str) -> pd.DataFrame:
    return frame.loc[frame["dataset"].eq(name)].copy()


def _calendar_from_silver(frame: pd.DataFrame) -> pd.DatetimeIndex:
    rows = _dataset(frame, "trade_calendar")
    if rows.empty:
        raise GoldPanelError("required Silver dataset is missing: trade_calendar")
    if "is_open" in rows.columns:
        values = rows["is_open"]
        open_mask = pd.to_numeric(values, errors="coerce").eq(1)
        open_mask |= values.astype("string").str.lower().isin({"true", "open", "yes"})
        rows = rows.loc[open_mask]
    dates = pd.DatetimeIndex(rows["trade_date"].dropna().unique()).sort_values()
    if dates.empty:
        raise GoldPanelError("trade_calendar contains no open sessions")
    return dates


def _deduplicate_security_dataset(rows: pd.DataFrame, name: str) -> pd.DataFrame:
    if rows.empty:
        return rows
    ticker_col = _ticker_column(rows)
    if ticker_col != "ts_code":
        rows = rows.rename(columns={ticker_col: "ts_code"})
    rows["ts_code"] = rows["ts_code"].astype("string")
    duplicated = rows.duplicated(["ts_code", "trade_date"], keep=False)
    if duplicated.any():
        raise GoldPanelError(
            f"Silver {name} contains duplicate ticker/date rows: {int(duplicated.sum())}"
        )
    return rows


def _compose_trade_status(frame: pd.DataFrame) -> pd.DataFrame:
    """Collapse suspension and limit feeds into one daily execution contract.

    ``stk_limit`` is the market-wide spine; sparse suspension feeds overlay a
    blocked state.  Conflicting non-null price limits are never silently
    selected.  Reconciliation should normally catch them earlier, while this
    final guard also covers differently-shaped vendor identities.
    """

    inherited_attrs = dict(frame.attrs)
    rows = _dataset(frame, "trade_status")
    if rows.empty:
        return frame
    ticker_col = _ticker_column(rows)
    if ticker_col != "ts_code":
        rows = rows.rename(columns={ticker_col: "ts_code"})
    rows["ts_code"] = rows["ts_code"].astype("string")
    suspension_fields = tuple(
        name
        for name in (
            "suspend_type",
            "suspend_timing",
            "suspend_date",
            "resume_date",
            "suspend_start_time",
            "suspend_end_time",
        )
        if name in rows
    )

    def one_value(group: pd.DataFrame, column: str) -> Any:
        values = group[column].dropna()
        if values.empty:
            return pd.NA
        distinct = values.astype("string").drop_duplicates()
        if len(distinct) > 1:
            raise GoldPanelError(
                f"trade_status has conflicting {column} values for one ticker/date"
            )
        return values.iloc[-1]

    records: list[dict[str, Any]] = []
    for (ticker, trade_date), group in rows.groupby(
        ["ts_code", "trade_date"], sort=True, dropna=False
    ):
        explicit = pd.Series(False, index=group.index)
        if "is_suspended" in group:
            explicit |= group["is_suspended"].astype("boolean").fillna(False)
        if "trade_status" in group:
            explicit |= group["trade_status"].astype("string").str.lower().isin(
                {"suspended", "suspend", "停牌", "p"}
            )
        if suspension_fields:
            explicit |= group[list(suspension_fields)].notna().any(axis=1)
        record: dict[str, Any] = {
            "dataset": "trade_status",
            "ts_code": ticker,
            "trade_date": trade_date,
            "event_time": group["event_time"].max(),
            "available_at": group["available_at"].max(),
            "ingested_at": group["ingested_at"].max(),
            "is_suspended": bool(explicit.any()),
            "trade_status": "suspended" if explicit.any() else "trading",
            "source_evidence_count": pd.to_numeric(
                group.get("source_evidence_count"), errors="coerce"
            ).fillna(0).sum()
            if "source_evidence_count" in group
            else len(group),
        }
        for column in ("pre_close", "up_limit", "down_limit"):
            if column in group:
                record[column] = one_value(group, column)
        records.append(record)
    retained = frame.loc[~frame["dataset"].eq("trade_status")].copy()
    # Pandas 2.2 warns about the future dtype inference of the intentionally
    # sparse union schema.  Every value is normalized explicitly downstream,
    # so keep the stable behavior without leaking a library warning into every
    # daily production materialization.
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="The behavior of DataFrame concatenation with empty or all-NA entries is deprecated.*",
            category=FutureWarning,
        )
        result = pd.concat(
            [retained, pd.DataFrame.from_records(records)],
            ignore_index=True,
            sort=False,
        )
    result.attrs.update(inherited_attrs)
    return result


def _value_columns(rows: pd.DataFrame) -> list[str]:
    technical = {
        "dataset",
        "entity_key",
        "event_time",
        "trade_date",
        "available_at",
        "ingested_at",
        "source_evidence_count",
        "ts_code",
        "ticker",
        "symbol",
    }
    # Concatenated Silver partitions have the union of every dataset's schema.
    # A field that is entirely null inside this dataset belongs to another
    # dataset and must not be merged (otherwise pandas creates misleading
    # ``_x/_y`` columns and can silently replace the authoritative field).
    return [
        name
        for name in rows.columns
        if name not in technical
        and not name.startswith("entity_")
        and rows[name].notna().any()
    ]


def _market_wide(frame: pd.DataFrame) -> pd.DataFrame:
    keys: pd.DataFrame | None = None
    prepared: dict[str, pd.DataFrame] = {}
    for name in CORE_DAILY_DATASETS:
        rows = _deduplicate_security_dataset(_dataset(frame, name), name)
        if rows.empty:
            raise GoldPanelError(f"required Silver dataset is missing: {name}")
        current_keys = rows[["ts_code", "trade_date"]]
        keys = current_keys if keys is None else pd.concat([keys, current_keys], ignore_index=True)
        values = _value_columns(rows)
        keep = ["ts_code", "trade_date", *values, "available_at", "ingested_at"]
        part = rows[keep].copy()
        part = part.rename(
            columns={
                "available_at": f"{name}_available_at",
                "ingested_at": f"{name}_ingested_at",
                **{
                    column: column
                    if column not in {item for prior in prepared.values() for item in prior.columns}
                    else f"{name}_{column}"
                    for column in values
                },
            }
        )
        prepared[name] = part
    assert keys is not None
    panel = keys.drop_duplicates().sort_values(["trade_date", "ts_code"])
    for name in CORE_DAILY_DATASETS:
        panel = panel.merge(
            prepared[name], on=["ts_code", "trade_date"], how="left", validate="one_to_one"
        )
    return panel.reset_index(drop=True)


def _require_dataset_session_coverage(
    silver: pd.DataFrame,
    calendar: pd.DatetimeIndex,
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> dict[str, Any]:
    expected = calendar[(calendar >= start) & (calendar <= end)]
    if expected.empty:
        raise GoldPanelError("analysis range contains no open sessions")
    coverage: dict[str, Any] = {}
    for name in CORE_DAILY_DATASETS:
        actual = pd.DatetimeIndex(_dataset(silver, name)["trade_date"].unique())
        missing = expected.difference(actual)
        coverage[name] = {
            "expected_sessions": len(expected),
            "present_sessions": len(expected.intersection(actual)),
            "missing_sessions": [item.date().isoformat() for item in missing[:10]],
        }
        if len(missing):
            raise GoldPanelError(
                f"{name} omits {len(missing)} required open sessions; first={missing[0].date()}"
            )
    return coverage


def _stock_metadata(frame: pd.DataFrame) -> pd.DataFrame:
    rows = _dataset(frame, "stock_basic")
    if rows.empty:
        raise GoldPanelError("required Silver dataset is missing: stock_basic")
    ticker_col = _ticker_column(rows)
    if ticker_col != "ts_code":
        rows = rows.rename(columns={ticker_col: "ts_code"})
    required = {"ts_code", "list_date", "list_status"}
    missing = sorted(required - set(rows.columns))
    if missing:
        raise GoldPanelError(f"stock_basic is missing fields: {missing}")
    status_evidence = "queried_list_status" if "queried_list_status" in rows else "list_status"
    statuses = set(rows[status_evidence].dropna().astype(str))
    missing_statuses = sorted({"L", "P", "D"} - statuses)
    if missing_statuses:
        raise GoldPanelError(
            f"stock_basic does not prove L/P/D query scope; missing={missing_statuses}"
        )
    rows = rows.sort_values(["ts_code", "available_at", "ingested_at"])
    rows = rows.drop_duplicates("ts_code", keep="last").copy()
    rows["list_date"] = _date_values(rows["list_date"])
    if "delist_date" not in rows:
        rows["delist_date"] = pd.NaT
    rows["delist_date"] = _date_values(rows["delist_date"])
    if rows["list_date"].isna().any():
        raise GoldPanelError("stock_basic contains invalid list_date")
    code = rows["ts_code"].astype("string")
    stem = code.str.split(".").str[0]
    ordinary = (
        (code.str.endswith(".SH") & stem.str.fullmatch(r"6\d{5}"))
        | (code.str.endswith(".SZ") & stem.str.fullmatch(r"[03]\d{5}"))
    )
    return rows.loc[ordinary].reset_index(drop=True)


def _historical_st(frame: pd.DataFrame) -> pd.DataFrame:
    rows = _dataset(frame, "historical_st")
    if rows.empty:
        raise GoldPanelError("historical ST is empty or unavailable")
    ticker_col = _ticker_column(rows)
    if ticker_col != "ts_code":
        rows = rows.rename(columns={ticker_col: "ts_code"})
    if {"start_date", "end_date"} <= set(rows.columns) and rows[
        "start_date"
    ].notna().any():
        result = rows[["ts_code", "start_date", "end_date", "available_at"]].copy()
    elif "trade_date" in rows.columns:
        # Tushare stock_st is a daily membership table.  Singleton closed
        # intervals preserve the availability timestamp of every observation;
        # compressing consecutive days would incorrectly make the interval end
        # visible at its start.
        result = rows[["ts_code", "trade_date", "available_at"]].copy()
        result["start_date"] = result["trade_date"]
        result["end_date"] = result["trade_date"]
        result = result.drop(columns="trade_date")
    else:
        raise GoldPanelError(
            "historical ST requires start/end intervals or daily trade_date membership"
        )
    result["start_date"] = _date_values(result["start_date"])
    result["end_date"] = _date_values(result["end_date"])
    if result["start_date"].isna().any():
        raise GoldPanelError("historical ST contains invalid start_date")
    return result


def _is_st_as_of(history: pd.DataFrame, tickers: pd.Series, as_of: pd.Timestamp) -> pd.Series:
    known = history.loc[pd.to_datetime(history["available_at"], utc=True) <= _close_cutoff(as_of)]
    intervals: dict[str, list[tuple[pd.Timestamp, pd.Timestamp | pd.NaT]]] = {
        str(code): list(zip(group["start_date"], group["end_date"]))
        for code, group in known.groupby("ts_code")
    }
    return tickers.astype(str).map(
        lambda code: any(
            start <= as_of and (pd.isna(end) or as_of <= end)
            for start, end in intervals.get(code, ())
        )
    )


def _close_cutoff(day: pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(day.date()).tz_localize(SHANGHAI) + pd.Timedelta(hours=23, minutes=59)


def _build_membership(
    market: pd.DataFrame,
    metadata: pd.DataFrame,
    st_history: pd.DataFrame,
    calendar: pd.DatetimeIndex,
    *,
    universe: UniverseSpec,
    analysis_start: pd.Timestamp,
    analysis_end: pd.Timestamp,
    amount_multiplier: float,
) -> tuple[pd.DataFrame, pd.DataFrame, list[dict[str, Any]]]:
    if universe.mode != "monthly_liquid_top_n" or not universe.point_in_time:
        raise GoldPanelError("only PIT monthly_liquid_top_n is supported")
    amount = pd.to_numeric(market.get("amount"), errors="coerce") * amount_multiplier
    liquidity_source = market[["ts_code", "trade_date"]].copy()
    liquidity_source["amount_cny"] = amount
    periods = pd.period_range(analysis_start, analysis_end, freq="M")
    flags: list[pd.DataFrame] = []
    members: list[pd.DataFrame] = []
    audits: list[dict[str, Any]] = []
    for period in periods:
        month_dates = calendar[
            (calendar >= max(analysis_start, period.start_time))
            & (calendar <= min(analysis_end, period.end_time))
        ]
        if month_dates.empty:
            continue
        prior = calendar[calendar < period.start_time]
        if len(prior) < universe.liquidity_lookback_sessions:
            raise GoldPanelError(
                f"{period} has only {len(prior)} prior sessions; "
                f"{universe.liquidity_lookback_sessions} required"
            )
        as_of = prior[-1]
        window_dates = prior[-universe.liquidity_lookback_sessions :]
        listed = metadata.loc[
            (metadata["list_date"] <= as_of)
            & (metadata["delist_date"].isna() | (metadata["delist_date"] >= as_of))
            & ((as_of - metadata["list_date"]).dt.days >= universe.minimum_listing_days)
        ].copy()
        listed["is_st_at_asof"] = _is_st_as_of(st_history, listed["ts_code"], as_of)
        listed = listed.loc[~listed["is_st_at_asof"]]
        liquidity = (
            liquidity_source.loc[liquidity_source["trade_date"].isin(window_dates)]
            .groupby("ts_code", as_index=False)["amount_cny"]
            .agg(median_amount_60d="median", liquidity_observations="count")
        )
        eligible = listed.merge(liquidity, on="ts_code", how="left")
        eligible_flag = (
            eligible["median_amount_60d"].notna()
            & eligible["liquidity_observations"].fillna(0).ge(
                universe.minimum_liquidity_observations
            )
        )
        eligible = eligible.loc[eligible_flag].copy()
        eligible = eligible.sort_values(
            ["median_amount_60d", "ts_code"],
            ascending=[False, True],
            kind="mergesort",
        )
        eligible["liquidity_rank"] = np.arange(1, len(eligible) + 1)
        eligible["universe_member"] = eligible["liquidity_rank"] <= universe.target_size
        eligible["eligible"] = True
        eligible["membership_month"] = str(period)
        eligible["membership_as_of"] = as_of
        eligible["liquidity_window_start"] = window_dates[0]
        eligible["liquidity_window_end"] = window_dates[-1]
        flag_columns = [
            "ts_code",
            "membership_month",
            "eligible",
            "universe_member",
            "liquidity_rank",
            "median_amount_60d",
            "liquidity_observations",
            "membership_as_of",
            "liquidity_window_start",
            "liquidity_window_end",
        ]
        flags.append(eligible[flag_columns])
        selected = eligible.loc[eligible["universe_member"], flag_columns].copy()
        if len(selected) != universe.target_size:
            raise GoldPanelError(
                f"{period} selected {len(selected)} securities; target is {universe.target_size}"
            )
        members.append(selected)
        audits.append(
            {
                "membership_month": str(period),
                "as_of_date": as_of.date().isoformat(),
                "eligible_count": len(eligible),
                "selected_count": len(selected),
                "liquidity_window_start": window_dates[0].date().isoformat(),
                "liquidity_window_end": window_dates[-1].date().isoformat(),
            }
        )
    if not members:
        raise GoldPanelError("membership builder produced no months")
    return (
        pd.concat(flags, ignore_index=True),
        pd.concat(members, ignore_index=True),
        audits,
    )


def _overlay_industry(panel: pd.DataFrame, silver: pd.DataFrame) -> pd.DataFrame:
    rows = _dataset(silver, "industry_classification")
    if rows.empty:
        raise GoldPanelError("required Silver dataset is missing: industry_classification")
    ticker_col = _ticker_column(rows)
    if ticker_col != "ts_code":
        rows = rows.rename(columns={ticker_col: "ts_code"})
    if "industry" not in rows.columns:
        raise GoldPanelError("industry_classification has no industry field")
    effective = "effective_from" if "effective_from" in rows else "trade_date"
    history = rows[["ts_code", "industry", effective, "available_at"]].copy()
    history = history.rename(columns={effective: "industry_effective_from"})
    history["industry_effective_from"] = _date_values(
        history["industry_effective_from"]
    )
    history["industry_available_at"] = pd.to_datetime(
        history.pop("available_at"), errors="coerce", utc=True
    )
    if history[["industry_effective_from", "industry_available_at"]].isna().any().any():
        raise GoldPanelError("industry history has invalid PIT timestamps")
    history["industry_usable_at"] = history["industry_available_at"]

    result_parts: list[pd.DataFrame] = []
    by_ticker = {str(code): group for code, group in history.groupby("ts_code")}
    for code, current in panel.groupby("ts_code", sort=False):
        candidates = by_ticker.get(str(code))
        current = current.sort_values("decision_cutoff").copy()
        if candidates is None:
            current["industry"] = pd.NA
            current["industry_effective_from"] = pd.NaT
            current["industry_available_at"] = pd.NaT
            result_parts.append(current)
            continue
        candidates = candidates.sort_values("industry_usable_at")
        overlay = pd.merge_asof(
            current,
            candidates.drop(columns=["ts_code"]),
            left_on="decision_cutoff",
            right_on="industry_usable_at",
            direction="backward",
            allow_exact_matches=True,
        )
        invalid_effective = overlay["industry_effective_from"] > overlay["trade_date"]
        overlay.loc[
            invalid_effective,
            ["industry", "industry_effective_from", "industry_available_at"],
        ] = [pd.NA, pd.NaT, pd.NaT]
        result_parts.append(overlay.drop(columns=["industry_usable_at"]))
    return pd.concat(result_parts, ignore_index=True).sort_values(
        ["trade_date", "ts_code"]
    ).reset_index(drop=True)


def _overlay_company_actions(panel: pd.DataFrame, silver: pd.DataFrame) -> pd.DataFrame:
    result = panel.copy()
    actions = _dataset(silver, "company_action")
    action_fields: list[str] = []
    if not actions.empty:
        ticker_col = _ticker_column(actions)
        if ticker_col != "ts_code":
            actions = actions.rename(columns={ticker_col: "ts_code"})
        if "ex_date" in actions:
            effective = _date_values(actions["ex_date"])
            actions["action_effective_date"] = effective.fillna(actions["trade_date"])
        else:
            actions["action_effective_date"] = actions["trade_date"]
        if actions.duplicated(["ts_code", "action_effective_date"]).any():
            raise GoldPanelError(
                "company_action contains ambiguous duplicate ticker/effective-date rows"
            )
        action_fields = _value_columns(actions)
        action_fields = [
            name
            for name in action_fields
            if name not in {"action_effective_date"}
        ]
        keep = ["ts_code", "action_effective_date", *action_fields, "available_at"]
        actions = actions[keep].rename(
            columns={
                "action_effective_date": "trade_date",
                "available_at": "company_action_available_at",
            }
        )
        result = result.merge(
            actions,
            on=["ts_code", "trade_date"],
            how="left",
            validate="one_to_one",
        )
        unavailable = result["company_action_available_at"].gt(result["decision_cutoff"])
        result.loc[unavailable, action_fields] = pd.NA
        result.loc[unavailable, "company_action_available_at"] = pd.NaT
    adj = pd.to_numeric(result["adj_factor"], errors="coerce")
    prior = adj.groupby(result["ts_code"], sort=False).shift(1)
    result["adj_factor_change"] = adj / prior - 1.0
    explicit = pd.Series(False, index=result.index)
    if action_fields:
        explicit = result[action_fields].notna().any(axis=1)
    result["has_company_action"] = explicit | result["adj_factor_change"].abs().gt(1e-12)
    return result


def _add_market_features(
    panel: pd.DataFrame,
    *,
    amount_multiplier: float,
    market_cap_multiplier: float,
) -> pd.DataFrame:
    required = {"open", "high", "low", "close", "adj_factor", "amount", "total_mv"}
    missing = sorted(required - set(panel.columns))
    if missing:
        raise GoldPanelError(f"Gold core fields are missing: {missing}")
    result = panel.sort_values(["ts_code", "trade_date"]).copy()
    factor = pd.to_numeric(result["adj_factor"], errors="coerce").where(lambda item: item.gt(0))
    for name in ("open", "close"):
        raw = pd.to_numeric(result[name], errors="coerce").where(lambda item: item.gt(0))
        result[f"{name}_adj"] = raw * factor
    result["amount_cny"] = pd.to_numeric(result["amount"], errors="coerce") * amount_multiplier
    result["adv_20"] = (
        result.groupby("ts_code", sort=False)["amount_cny"]
        .rolling(20, min_periods=20)
        .mean()
        .reset_index(level=0, drop=True)
    )
    returns = result.groupby("ts_code", sort=False)["close_adj"].pct_change(fill_method=None)
    result["volatility_20"] = (
        returns.groupby(result["ts_code"], sort=False)
        .rolling(20, min_periods=20)
        .std(ddof=1)
        .reset_index(level=0, drop=True)
        * np.sqrt(252.0)
    )
    result["market_cap_cny"] = pd.to_numeric(result["total_mv"], errors="coerce") * market_cap_multiplier
    result["log_market_cap"] = np.log(result["market_cap_cny"].where(lambda item: item.gt(0)))
    result["size_available_at"] = result["daily_basic_available_at"]

    status = result.get("trade_status", pd.Series(pd.NA, index=result.index)).astype("string").str.lower()
    explicit_suspended = status.isin({"suspended", "suspend", "停牌", "p"})
    if "is_suspended" in result:
        explicit_suspended |= result["is_suspended"].astype("boolean").fillna(False)
    missing_bar = result[["open", "high", "low", "close"]].isna().all(axis=1)
    result["is_suspended"] = explicit_suspended | missing_bar
    one_price = (
        pd.to_numeric(result["high"], errors="coerce")
        .eq(pd.to_numeric(result["low"], errors="coerce"))
        & ~result["is_suspended"]
    )
    close = pd.to_numeric(result["close"], errors="coerce")
    pre_close = pd.to_numeric(result.get("pre_close"), errors="coerce")
    if "up_limit" in result:
        is_up = one_price & close.ge(pd.to_numeric(result["up_limit"], errors="coerce"))
    else:
        is_up = one_price & close.gt(pre_close)
    if "down_limit" in result:
        is_down = one_price & close.le(pd.to_numeric(result["down_limit"], errors="coerce"))
    else:
        is_down = one_price & close.lt(pre_close)
    result["is_one_price_limit_up"] = is_up.fillna(False)
    result["is_one_price_limit_down"] = is_down.fillna(False)
    result["can_buy"] = ~(result["is_suspended"] | result["is_one_price_limit_up"])
    result["can_sell"] = ~(result["is_suspended"] | result["is_one_price_limit_down"])
    availability_columns = [
        name for name in result if name.endswith("_available_at") and name != "company_action_available_at"
    ]
    result["decision_cutoff"] = result[availability_columns].max(axis=1)
    return result


def _add_research_label(
    panel: pd.DataFrame,
    calendar: pd.DatetimeIndex,
    label: LabelSpec,
    *,
    as_of: datetime,
) -> pd.DataFrame:
    if label.kind != "forward_open_to_open" or label.price_adjustment != "post":
        raise GoldPanelError("Gold panel v1 supports only post-adjusted forward open labels")
    dates = pd.DataFrame({"trade_date": calendar})
    entry_offset = label.entry_delay_sessions
    exit_offset = label.entry_delay_sessions + label.horizon_sessions
    dates["label_entry_date"] = dates["trade_date"].shift(-entry_offset)
    dates["label_exit_date"] = dates["trade_date"].shift(-exit_offset)
    result = panel.merge(dates, on="trade_date", how="left", validate="many_to_one")
    lookup = panel[["ts_code", "trade_date", "open_adj", "daily_available_at"]].copy()
    entry = lookup.rename(
        columns={
            "trade_date": "label_entry_date",
            "open_adj": "label_entry_open_adj",
            "daily_available_at": "label_entry_available_at",
        }
    )
    exit_rows = lookup.rename(
        columns={
            "trade_date": "label_exit_date",
            "open_adj": "label_exit_open_adj",
            "daily_available_at": "label_exit_available_at",
        }
    )
    result = result.merge(entry, on=["ts_code", "label_entry_date"], how="left", validate="many_to_one")
    result = result.merge(exit_rows, on=["ts_code", "label_exit_date"], how="left", validate="many_to_one")
    both_label_observations = result[
        ["label_entry_available_at", "label_exit_available_at"]
    ].notna().all(axis=1)
    result["label_available_at"] = result[
        ["label_entry_available_at", "label_exit_available_at"]
    ].max(axis=1).where(both_label_observations)
    freeze = _parse_aware(as_of, name="as_of")
    entry_price = pd.to_numeric(result["label_entry_open_adj"], errors="coerce")
    exit_price = pd.to_numeric(result["label_exit_open_adj"], errors="coerce")
    valid = (
        entry_price.gt(0)
        & exit_price.gt(0)
        & result["label_available_at"].notna()
        & result["label_available_at"].le(freeze)
        & result["label_available_at"].gt(result["decision_cutoff"])
    )
    result["forward_return_5d_open"] = np.where(valid, exit_price / entry_price - 1.0, np.nan)
    result["label_id"] = label.label_id
    result["label_is_research_only"] = True
    return result


def build_research_gold_panel(
    silver: pd.DataFrame,
    *,
    analysis_start: str | pd.Timestamp,
    analysis_end: str | pd.Timestamp,
    as_of: datetime,
    universe: UniverseSpec | None = None,
    label: LabelSpec | None = None,
    required_datasets: Sequence[str] = DEFAULT_REQUIRED_DATASETS,
    amount_unit_multiplier: float = 1_000.0,
    market_cap_unit_multiplier: float = 10_000.0,
    parent_snapshot_ids: Sequence[str] = (),
) -> GoldPanelBuildResult:
    """Build a deterministic PIT research panel from reconciled Silver rows."""

    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise GoldPanelError("as_of must include a timezone")
    universe = universe or UniverseSpec()
    label = label or LabelSpec()
    if amount_unit_multiplier <= 0 or market_cap_unit_multiplier <= 0:
        raise GoldPanelError("unit multipliers must be positive")
    normalised = _compose_trade_status(_normalise_silver(silver, as_of=as_of))
    available_datasets = set(normalised["dataset"].dropna().astype(str))
    canonical_required = {
        _DATASET_ALIASES.get(str(name), str(name)) for name in required_datasets
    }
    missing_datasets = sorted(canonical_required - available_datasets)
    if missing_datasets:
        if "historical_st" in missing_datasets:
            raise GoldPanelError("historical ST is empty or unavailable")
        raise GoldPanelError(f"required Silver datasets are missing: {missing_datasets}")
    calendar = _calendar_from_silver(normalised)
    start = pd.Timestamp(analysis_start).normalize()
    end = pd.Timestamp(analysis_end).normalize()
    if start > end:
        raise GoldPanelError("analysis_start cannot be after analysis_end")
    prior = calendar[calendar < start]
    if len(prior) < universe.liquidity_lookback_sessions:
        raise GoldPanelError(
            f"Gold history needs {universe.liquidity_lookback_sessions} warm-up sessions"
        )
    fetch_start = prior[-universe.liquidity_lookback_sessions]
    dataset_coverage = _require_dataset_session_coverage(
        normalised, calendar, start=fetch_start, end=end
    )
    metadata = _stock_metadata(normalised)
    st_history = _historical_st(normalised)
    market = _market_wide(normalised)
    ordinary_codes = set(metadata["ts_code"].astype(str))
    market = market.loc[market["ts_code"].astype(str).isin(ordinary_codes)].copy()
    market = _add_market_features(
        market,
        amount_multiplier=amount_unit_multiplier,
        market_cap_multiplier=market_cap_unit_multiplier,
    )
    market = _overlay_company_actions(market, normalised)
    market = _overlay_industry(market, normalised)
    flags, membership, membership_audit = _build_membership(
        market,
        metadata,
        st_history,
        calendar,
        universe=universe,
        analysis_start=start,
        analysis_end=end,
        amount_multiplier=amount_unit_multiplier,
    )
    market["membership_month"] = market["trade_date"].dt.to_period("M").astype(str)
    market = market.merge(
        flags,
        on=["ts_code", "membership_month"],
        how="left",
        validate="many_to_one",
    )
    market["eligible"] = market["eligible"].astype("boolean").fillna(False).astype(bool)
    market["universe_member"] = (
        market["universe_member"].astype("boolean").fillna(False).astype(bool)
    )
    market = _add_research_label(market, calendar, label, as_of=as_of)
    output = market.loc[market["trade_date"].between(start, end)].copy()
    if output.empty:
        raise GoldPanelError("Gold panel contains no analysis rows")
    core = ["open_adj", "close_adj", "adj_factor", "amount_cny"]
    coverage = {name: float(output[name].notna().mean()) for name in core}
    low_core = {name: value for name, value in coverage.items() if value < 0.95}
    if low_core:
        raise GoldPanelError(f"Gold core coverage is below 95%: {low_core}")
    member_rows = output.loc[output["universe_member"]]
    if member_rows.empty:
        raise GoldPanelError("Gold panel has no universe members")
    member_industry_coverage = float(member_rows["industry"].notna().mean())
    member_size_coverage = float(member_rows["log_market_cap"].notna().mean())
    if member_industry_coverage < 1.0:
        raise GoldPanelError(
            f"PIT industry coverage for universe members is {member_industry_coverage:.6f}; 1.0 required"
        )
    if member_size_coverage < 0.95:
        raise GoldPanelError(
            f"PIT size coverage for universe members is {member_size_coverage:.6f}; 0.95 required"
        )
    member_count = output.groupby("trade_date")["universe_member"].transform("sum")
    output["benchmark_weight"] = np.where(
        output["universe_member"] & member_count.gt(0), 1.0 / member_count, 0.0
    )
    benchmark = (
        output.loc[output["universe_member"]]
        .groupby("trade_date")["forward_return_5d_open"]
        .agg(benchmark_forward_return_5d_open=lambda values: values.mean() if values.notna().all() else np.nan,
             benchmark_labeled_count="count")
        .reset_index()
    )
    benchmark["benchmark_member_count"] = universe.target_size
    output = output.merge(benchmark, on="trade_date", how="left", validate="many_to_one")
    output = output.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)
    if output.duplicated(["ts_code", "trade_date"]).any():
        raise GoldPanelError("Gold panel contains duplicate ticker/date rows")
    if (output["liquidity_window_end"].dropna() >= output.loc[output["liquidity_window_end"].notna(), "trade_date"]).any():
        raise GoldPanelError("universe membership uses current or future liquidity")
    parent_ids = tuple(sorted(map(str, parent_snapshot_ids)))
    if parent_ids and len(parent_ids) != len(set(parent_ids)):
        raise GoldPanelError("Gold parent snapshot ids must be unique")
    calendar_sessions = tuple(item.date().isoformat() for item in calendar)
    calendar_hash = hashlib.sha256(
        "\n".join(calendar_sessions).encode("ascii")
    ).hexdigest()
    audit = {
        "schema_version": GOLD_PANEL_SCHEMA_VERSION,
        "status": "pass",
        "analysis_start": start.date().isoformat(),
        "analysis_end": end.date().isoformat(),
        "as_of": _parse_aware(as_of, name="as_of").isoformat(),
        "row_count": len(output),
        "ticker_count": int(output["ts_code"].nunique()),
        "parent_snapshot_ids": list(parent_ids),
        "required_datasets": list(map(str, required_datasets)),
        "silver_revision_rows_superseded": int(
            normalised.attrs.get("silver_revision_rows_superseded", 0)
        ),
        "dataset_coverage": dataset_coverage,
        "core_coverage": coverage,
        "member_industry_coverage": member_industry_coverage,
        "member_size_coverage": member_size_coverage,
        "membership": membership_audit,
        "universe_spec": universe.model_dump(mode="json"),
        "label_spec": label.model_dump(mode="json"),
        "label_semantics": {
            "research_only": True,
            "availability_column": "label_available_at",
            "entry": "next open",
            "exit": "sixth open",
        },
        "trading_calendar": {
            "source": "reconciled_silver:trade_calendar",
            "quality_status": "accepted",
            "sessions": list(calendar_sessions),
            "content_hash": calendar_hash,
        },
    }
    output.attrs["research_os_parent_snapshot_ids"] = parent_ids
    output.attrs["research_os_gold_panel_schema"] = GOLD_PANEL_SCHEMA_VERSION
    return GoldPanelBuildResult(
        panel=output,
        membership=membership.sort_values(["membership_month", "liquidity_rank"]).reset_index(drop=True),
        audit=audit,
        parent_snapshot_ids=parent_ids,
    )


def read_verified_silver_inputs(
    inputs: Sequence[SilverSnapshotInput],
    *,
    catalog: SnapshotCatalog,
    lake_root: str | Path,
) -> tuple[pd.DataFrame, tuple[str, ...]]:
    parent_ids = verify_parent_snapshot_closure(
        inputs, catalog=catalog, lake_root=lake_root
    )
    frames = [pd.read_parquet(item.path) for item in inputs]
    if not frames:
        raise GoldPanelError("no verified Silver frames")
    return pd.concat(frames, ignore_index=True, sort=False), parent_ids


def write_gold_panel_artifacts(
    result: GoldPanelBuildResult,
    *,
    output_dir: str | Path,
) -> GoldPanelArtifacts:
    """Write immutable-by-content build inputs used by the Gold manifest."""

    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    panel_path = root / "research_gold_panel.parquet"
    membership_path = root / "monthly_membership.parquet"
    audit_path = root / "gold_panel_audit.json"
    build_spec_path = root / "gold_panel_build_spec.json"
    _write_parquet_once(panel_path, result.panel)
    _write_parquet_once(membership_path, result.membership)
    _write_bytes_once(audit_path, _canonical_bytes(result.audit))
    build_spec = {
        "schema_version": GOLD_PANEL_SCHEMA_VERSION,
        "parent_snapshot_ids": list(result.parent_snapshot_ids),
        "universe_spec": result.audit["universe_spec"],
        "label_spec": result.audit["label_spec"],
        "required_datasets": result.audit["required_datasets"],
        "analysis_start": result.audit["analysis_start"],
        "analysis_end": result.audit["analysis_end"],
        "as_of": result.audit["as_of"],
    }
    _write_bytes_once(build_spec_path, _canonical_bytes(build_spec))
    return GoldPanelArtifacts(
        panel_path=panel_path,
        membership_path=membership_path,
        audit_path=audit_path,
        build_spec_path=build_spec_path,
        parent_snapshot_ids=result.parent_snapshot_ids,
        audit=result.audit,
    )


def load_gold_research_panel(
    reference: DataSnapshotRef,
    *,
    lake_root: str | Path,
) -> pd.DataFrame:
    """Load the authoritative panel and attach the verifier's source binding."""

    if reference.tier is not SnapshotTier.GOLD:
        raise GoldPanelError("research panel loader requires a Gold reference")
    if reference.quality_status is not DataQualityStatus.ACCEPTED:
        raise GoldPanelError("research Gold reference is not accepted")
    if _manifest_identity(reference) != reference.snapshot_id:
        raise GoldPanelError("research Gold reference identity is invalid")
    verification = verify_immutable_snapshot_manifest(
        reference.manifest, base_dir=lake_root
    )
    if not verification["valid"]:
        raise GoldPanelError(
            f"research Gold manifest verification failed: {verification['errors']}"
        )
    entries = [
        row
        for row in reference.manifest.get("files", ())
        if PurePosixPath(str(row.get("path") or "")).name
        == "research_gold_panel.parquet"
    ]
    if len(entries) != 1:
        raise GoldPanelError("Gold manifest does not uniquely identify the research panel")
    pure = PurePosixPath(str(entries[0]["path"]))
    if pure.is_absolute() or ".." in pure.parts:
        raise GoldPanelError("unsafe research panel manifest path")
    root = Path(lake_root).resolve()
    path = (root / Path(*pure.parts)).resolve()
    frame = pd.read_parquet(path)
    frame.attrs["research_os_source_path"] = str(path)
    frame.attrs["research_os_source_sha256"] = sha256_path(path)
    from .snapshots import verify_snapshot_frame_binding

    verify_snapshot_frame_binding(reference, frame)
    return frame


def _write_bytes_once(path: Path, payload: bytes) -> None:
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o444)
    except FileExistsError:
        if path.read_bytes() != payload:
            raise GoldPanelError(f"immutable Gold artifact already differs: {path}")
        return
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


def _write_parquet_once(path: Path, frame: pd.DataFrame) -> None:
    if path.exists():
        existing = pd.read_parquet(path)
        if list(existing.columns) != list(frame.columns) or not existing.equals(frame):
            raise GoldPanelError(f"immutable Gold parquet already differs: {path}")
        return
    with tempfile.NamedTemporaryFile(
        prefix=f".{path.stem}.", suffix=".parquet", dir=path.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
    try:
        frame.to_parquet(temporary, index=False)
        try:
            os.link(temporary, path)
        except FileExistsError:
            existing = pd.read_parquet(path)
            if list(existing.columns) != list(frame.columns) or not existing.equals(frame):
                raise GoldPanelError(f"concurrent Gold parquet differs: {path}")
    finally:
        temporary.unlink(missing_ok=True)


class ResearchGoldPanelService:
    """Catalog-backed historical Gold assembly used by Dagster/application services."""

    def __init__(self, catalog: Any, *, lake_root: str | Path) -> None:
        self.catalog = catalog
        self.lake_root = Path(lake_root).resolve()

    def build(
        self,
        *,
        inputs: Sequence[SilverSnapshotInput] | None = None,
        output_dir: str | Path,
        analysis_start: str,
        analysis_end: str,
        as_of: datetime,
        universe: UniverseSpec | None = None,
        label: LabelSpec | None = None,
        required_datasets: Sequence[str] = DEFAULT_REQUIRED_DATASETS,
        amount_unit_multiplier: float = 1_000.0,
        market_cap_unit_multiplier: float = 10_000.0,
        snapshot_limit: int = DEFAULT_SILVER_SNAPSHOT_DISCOVERY_CAP,
    ) -> GoldPanelArtifacts:
        selected = tuple(inputs) if inputs is not None else discover_cataloged_silver_inputs(
            self.catalog, lake_root=self.lake_root, limit=snapshot_limit
        )
        silver, parent_ids = read_verified_silver_inputs(
            selected, catalog=self.catalog, lake_root=self.lake_root
        )
        result = build_research_gold_panel(
            silver,
            analysis_start=analysis_start,
            analysis_end=analysis_end,
            as_of=as_of,
            universe=universe,
            label=label,
            required_datasets=required_datasets,
            amount_unit_multiplier=amount_unit_multiplier,
            market_cap_unit_multiplier=market_cap_unit_multiplier,
            parent_snapshot_ids=parent_ids,
        )
        return write_gold_panel_artifacts(result, output_dir=output_dir)


__all__ = [
    "CORE_DAILY_DATASETS",
    "DEFAULT_SILVER_SNAPSHOT_DISCOVERY_CAP",
    "DEFAULT_REQUIRED_DATASETS",
    "GOLD_PANEL_SCHEMA_VERSION",
    "GoldPanelArtifacts",
    "GoldPanelBuildResult",
    "GoldPanelError",
    "ResearchGoldPanelService",
    "SilverSnapshotInput",
    "build_research_gold_panel",
    "discover_cataloged_silver_inputs",
    "load_gold_research_panel",
    "read_verified_silver_inputs",
    "verify_parent_snapshot_closure",
    "verify_silver_snapshot_input",
    "write_gold_panel_artifacts",
]

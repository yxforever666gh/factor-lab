"""Bitemporal Silver observations and point-in-time views."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from typing import Any, Callable, Mapping, Sequence

import pandas as pd

from .data_sources import SourceBatch


CANONICAL_COLUMNS = (
    "dataset",
    "entity_key",
    "field",
    "value",
    "event_time",
    "available_at",
    "ingested_at",
    "vendor_revision",
    "source_id",
    "source_priority",
    "nullable",
    "unit",
    "adjustment",
    "lineage",
)


class BitemporalValidationError(ValueError):
    pass


@dataclass(frozen=True)
class CanonicalizationSpec:
    entity_columns: tuple[str, ...]
    event_time_column: str
    available_at_column: str | None = None
    vendor_revision_column: str | None = None
    value_columns: tuple[str, ...] = ()
    allows_pre_event_availability: bool = False

    def __post_init__(self) -> None:
        if not self.entity_columns:
            raise ValueError("entity_columns must be non-empty")
        if not self.event_time_column:
            raise ValueError("event_time_column is required")


def _utc_series(values: pd.Series, *, name: str) -> pd.Series:
    parsed = pd.to_datetime(values, errors="coerce", utc=True)
    if parsed.isna().any():
        bad_count = int(parsed.isna().sum())
        raise BitemporalValidationError(f"{name} contains {bad_count} invalid timestamps")
    return parsed


def _utc_timestamp(value: Any, *, name: str) -> pd.Timestamp:
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(parsed):
        raise BitemporalValidationError(f"invalid {name}: {value!r}")
    return pd.Timestamp(parsed)


def _entity_key(row: pd.Series, columns: Sequence[str]) -> str:
    payload = {name: None if pd.isna(row[name]) else str(row[name]) for name in columns}
    if any(value is None for value in payload.values()):
        raise BitemporalValidationError(f"entity key contains nulls: {payload}")
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def canonicalize_batch(
    batch: SourceBatch,
    spec: CanonicalizationSpec,
    *,
    availability_resolver: Callable[[pd.Series], Any] | None = None,
) -> pd.DataFrame:
    """Convert a contracted wide Bronze batch into canonical long Silver rows.

    Availability must be supplied by a provider column or an explicit resolver;
    defaulting it to event time would create an undetectable point-in-time leak.
    """

    frame = batch.frame.copy()
    required = {*spec.entity_columns, spec.event_time_column}
    if spec.available_at_column:
        required.add(spec.available_at_column)
    if spec.vendor_revision_column:
        required.add(spec.vendor_revision_column)
    missing = sorted(required - set(frame.columns))
    if missing:
        raise BitemporalValidationError(f"canonicalization columns missing: {missing}")
    if spec.available_at_column is None and availability_resolver is None:
        raise BitemporalValidationError(
            "available_at requires a source column or explicit availability_resolver"
        )

    value_columns = list(spec.value_columns)
    if not value_columns:
        excluded = {
            *spec.entity_columns,
            spec.event_time_column,
            spec.available_at_column,
            spec.vendor_revision_column,
        }
        value_columns = [name for name in frame.columns if name not in excluded]
    unknown = sorted(set(value_columns) - set(frame.columns))
    if unknown:
        raise BitemporalValidationError(f"value columns missing: {unknown}")
    if not value_columns:
        raise BitemporalValidationError("canonicalization has no value columns")

    event_time = _utc_series(frame[spec.event_time_column], name="event_time")
    if spec.available_at_column:
        available_at = _utc_series(frame[spec.available_at_column], name="available_at")
    else:
        resolved = frame.apply(availability_resolver, axis=1)
        available_at = _utc_series(resolved, name="available_at")
    ingested_at = _utc_timestamp(batch.ingested_at, name="ingested_at")
    future_availability = available_at > ingested_at
    before_event = available_at < event_time
    if future_availability.any() or (
        before_event.any() and not spec.allows_pre_event_availability
    ):
        raise BitemporalValidationError(
            "temporal ordering requires available_at <= ingested_at and, except "
            "for declared future-event authorities, event_time <= available_at; "
            f"before_event={int(before_event.sum())}, future_availability={int(future_availability.sum())}"
        )

    entity_keys = frame.apply(lambda row: _entity_key(row, spec.entity_columns), axis=1)
    field_map = batch.contract.field_map
    parts: list[pd.DataFrame] = []
    lineage_json = json.dumps(
        dict(batch.lineage), ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str
    )
    for column in value_columns:
        contract = field_map.get(column)
        if contract is None:
            raise BitemporalValidationError(
                f"value column {column!r} is absent from source contract"
            )
        if spec.vendor_revision_column:
            revisions = frame[spec.vendor_revision_column].astype("string").fillna(batch.vendor_revision)
        else:
            revisions = pd.Series(batch.vendor_revision, index=frame.index, dtype="string")
        parts.append(
            pd.DataFrame(
                {
                    "dataset": batch.dataset,
                    "entity_key": entity_keys,
                    "field": column,
                    # Canonical values are heterogeneous by design.  An
                    # explicit object dtype also keeps an all-null optional
                    # field from participating in pandas' deprecated concat
                    # dtype inference path.
                    "value": frame[column].astype("object"),
                    "event_time": event_time,
                    "available_at": available_at,
                    "ingested_at": ingested_at,
                    "vendor_revision": revisions,
                    "source_id": batch.source_id,
                    "source_priority": batch.source_priority,
                    "nullable": bool(contract.nullable),
                    "unit": contract.unit,
                    "adjustment": contract.adjustment,
                    "lineage": lineage_json,
                }
            )
        )
    result = pd.concat(parts, ignore_index=True)
    validate_bitemporal_frame(
        result,
        allows_pre_event_availability=spec.allows_pre_event_availability,
    )
    return result.loc[:, list(CANONICAL_COLUMNS)]


def validate_bitemporal_frame(
    frame: pd.DataFrame,
    *,
    allow_null_values: bool = True,
    allows_pre_event_availability: bool = False,
) -> None:
    missing = sorted(set(CANONICAL_COLUMNS) - set(frame.columns))
    if missing:
        raise BitemporalValidationError(f"canonical frame missing columns: {missing}")
    if frame.empty:
        raise BitemporalValidationError("canonical frame contains no observations")
    required_non_null = [
        "dataset",
        "entity_key",
        "field",
        "event_time",
        "available_at",
        "ingested_at",
        "vendor_revision",
        "source_id",
        "source_priority",
        "nullable",
    ]
    if not allow_null_values:
        required_non_null.append("value")
    nulls = {name: int(frame[name].isna().sum()) for name in required_non_null if frame[name].isna().any()}
    if nulls:
        raise BitemporalValidationError(f"canonical frame contains required nulls: {nulls}")
    event_time = _utc_series(frame["event_time"], name="event_time")
    available_at = _utc_series(frame["available_at"], name="available_at")
    ingested_at = _utc_series(frame["ingested_at"], name="ingested_at")
    before_event = available_at < event_time
    before_available = ingested_at < available_at
    if (
        before_event.any() and not allows_pre_event_availability
    ) or before_available.any():
        raise BitemporalValidationError(
            "canonical temporal ordering invalid: "
            f"available_before_event={int(before_event.sum())}, "
            f"ingested_before_available={int(before_available.sum())}"
        )
    priority = pd.to_numeric(frame["source_priority"], errors="coerce")
    if priority.isna().any() or priority.lt(0).any():
        raise BitemporalValidationError("source_priority must contain non-negative numbers")


def point_in_time_view(
    frame: pd.DataFrame,
    *,
    decision_cutoff: Any,
    system_cutoff: Any | None = None,
) -> pd.DataFrame:
    """Return observations knowable at both the valid-time and system-time cutoffs.

    Vendor corrections ingested after ``system_cutoff`` are excluded.  Within a
    source, only the latest ingested version per event is retained; cross-source
    conflicts intentionally remain for the reconciliation stage.
    """

    validate_bitemporal_frame(frame)
    decision = _utc_timestamp(decision_cutoff, name="decision_cutoff")
    system = _utc_timestamp(system_cutoff or datetime.now().astimezone(), name="system_cutoff")
    result = frame.copy()
    result["event_time"] = pd.to_datetime(result["event_time"], utc=True)
    result["available_at"] = pd.to_datetime(result["available_at"], utc=True)
    result["ingested_at"] = pd.to_datetime(result["ingested_at"], utc=True)
    result = result.loc[
        (result["event_time"] <= decision)
        & (result["available_at"] <= decision)
        & (result["ingested_at"] <= system)
    ].copy()
    if result.empty:
        return result.reset_index(drop=True)
    keys = ["dataset", "entity_key", "field", "event_time", "source_id"]
    result = result.sort_values([*keys, "ingested_at", "vendor_revision"])
    result = result.drop_duplicates(keys, keep="last")
    return result.sort_values(["dataset", "entity_key", "field", "event_time", "source_priority"]).reset_index(drop=True)


__all__ = [
    "BitemporalValidationError",
    "CANONICAL_COLUMNS",
    "CanonicalizationSpec",
    "canonicalize_batch",
    "point_in_time_view",
    "validate_bitemporal_frame",
]

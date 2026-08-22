"""Deterministic, auditable multi-source observation reconciliation."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from typing import Any, Mapping

import numpy as np
import pandas as pd

from .bitemporal import CANONICAL_COLUMNS


RECONCILIATION_KEY = ("dataset", "entity_key", "field", "event_time")
RECONCILIATION_EVALUATOR_SCHEMA = "research-os/data-reconciliation/v1"


def production_comparison_policies() -> dict[str, "ComparisonPolicy"]:
    """Reviewed field-level tolerances for A-share source reconciliation."""

    policies: dict[str, ComparisonPolicy] = {}
    for field in ("open", "high", "low", "close", "pre_close", "price"):
        policies[field] = ComparisonPolicy(absolute_tolerance=0.01)
    for field in ("vol", "volume", "amount", "turnover"):
        policies[field] = ComparisonPolicy(relative_tolerance=0.01)
    for field in (
        "adj_factor",
        "cash_dividend",
        "stock_dividend_ratio",
        "split_ratio",
    ):
        policies[field] = ComparisonPolicy(
            absolute_tolerance=1e-8,
            relative_tolerance=1e-8,
        )
    for field in ("is_open", "trade_status", "suspend_status", "st_status"):
        policies[field] = ComparisonPolicy(case_sensitive=False)
    return policies


@dataclass(frozen=True)
class ComparisonPolicy:
    absolute_tolerance: float = 0.0
    relative_tolerance: float = 0.0
    case_sensitive: bool = True

    def __post_init__(self) -> None:
        if self.absolute_tolerance < 0 or self.relative_tolerance < 0:
            raise ValueError("comparison tolerances must be non-negative")


@dataclass(frozen=True)
class ReconciliationResult:
    accepted: pd.DataFrame
    disputed: pd.DataFrame
    quarantined: pd.DataFrame
    audit: dict[str, Any]

    @property
    def promotion_allowed(self) -> bool:
        return self.audit["disputed_group_count"] == 0 and self.audit["quarantined_row_count"] == 0


def _empty_result_frame(extra_columns: tuple[str, ...]) -> pd.DataFrame:
    return pd.DataFrame(columns=[*CANONICAL_COLUMNS, *extra_columns])


def _canonical_digest_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "source_id": str(row["source_id"]),
        "priority": int(row["source_priority"]),
        "value": None if pd.isna(row["value"]) else str(row["value"]),
        "nullable": bool(row["nullable"]),
        "unit": None if pd.isna(row.get("unit")) else str(row.get("unit")),
        "adjustment": None
        if pd.isna(row.get("adjustment"))
        else str(row.get("adjustment")),
        "vendor_revision": str(row["vendor_revision"]),
        "ingested_at": pd.Timestamp(row["ingested_at"]).isoformat(),
    }


def _canonical_rows_digest(rows: list[dict[str, Any]]) -> str:
    payload = json.dumps(rows, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _canonical_group_digest(group: pd.DataFrame) -> str:
    rows = [
        _canonical_digest_row(row)
        for row in group.sort_values(
            ["source_priority", "source_id", "ingested_at"]
        ).to_dict("records")
    ]
    return _canonical_rows_digest(rows)


def _single_source_unique_group_mask(frame: pd.DataFrame) -> pd.Series:
    """Identify groups that never need source/revision arbitration.

    This mask is intentionally computed before same-source revisions are
    superseded. A group with duplicate revisions therefore remains on the
    audited slow path even when only its latest row survives.
    """

    return ~frame.duplicated(list(RECONCILIATION_KEY), keep=False)


def _reconcile_single_source_unique_groups(
    frame: pd.DataFrame,
    *,
    policies: Mapping[str, ComparisonPolicy],
    default_policy: ComparisonPolicy,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Reconcile singleton groups without constructing pandas groups per row."""

    if frame.empty:
        return frame.copy(), frame.copy()

    records = frame.to_dict("records")
    digests = [
        _canonical_rows_digest([_canonical_digest_row(row)]) for row in records
    ]
    compared_sources = [
        json.dumps([str(row["source_id"])], separators=(",", ":"))
        for row in records
    ]
    equivalent = np.fromiter(
        (
            _equivalent(
                row["value"],
                row["value"],
                policies.get(str(row["field"]), default_policy),
            )
            for row in records
        ),
        dtype=bool,
        count=len(records),
    )

    accepted_positions = np.flatnonzero(equivalent)
    accepted_records: list[dict[str, Any]] = []
    for position in accepted_positions:
        accepted_row = dict(records[position])
        accepted_row["reconciliation_status"] = "accepted"
        accepted_row["reconciliation_id"] = digests[position]
        accepted_row["compared_sources"] = compared_sources[position]
        accepted_row["evidence_count"] = 1
        accepted_records.append(accepted_row)
    accepted = pd.DataFrame(accepted_records)

    disputed_positions = np.flatnonzero(~equivalent)
    disputed = frame.iloc[disputed_positions].copy()
    disputed["reconciliation_status"] = "disputed"
    disputed["reconciliation_id"] = [digests[position] for position in disputed_positions]
    disputed["compared_sources"] = [
        compared_sources[position] for position in disputed_positions
    ]
    disputed["dispute_reason"] = "value_conflict"
    return accepted, disputed


def _equivalent(left: Any, right: Any, policy: ComparisonPolicy) -> bool:
    if pd.isna(left) or pd.isna(right):
        return False
    if isinstance(left, (int, float, np.number)) and isinstance(right, (int, float, np.number)):
        left_number = float(left)
        right_number = float(right)
        if not math.isfinite(left_number) or not math.isfinite(right_number):
            return False
        return math.isclose(
            left_number,
            right_number,
            rel_tol=policy.relative_tolerance,
            abs_tol=policy.absolute_tolerance,
        )
    left_text = str(left).strip()
    right_text = str(right).strip()
    if not policy.case_sensitive:
        left_text = left_text.casefold()
        right_text = right_text.casefold()
    return left_text == right_text


def _quarantine_invalid_rows(
    frame: pd.DataFrame,
    *,
    allows_pre_event_availability: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    result = frame.copy()
    reasons = pd.Series("", index=result.index, dtype="string")
    required = [
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
    null_mask = result[required].isna().any(axis=1)
    reasons.loc[null_mask] = "required_metadata_missing"
    nullable_values = result["nullable"].map(
        lambda value: value
        if isinstance(value, (bool, np.bool_))
        else str(value).strip().lower() in {"1", "true", "yes"}
    )
    invalid_nullable = ~result["nullable"].map(
        lambda value: isinstance(value, (bool, np.bool_))
        or str(value).strip().lower() in {"0", "1", "false", "true", "no", "yes"}
    )
    reasons.loc[(reasons == "") & invalid_nullable] = "invalid_nullable_contract"
    result["nullable"] = nullable_values.astype(bool)
    required_value_missing = result["value"].isna() & ~result["nullable"]
    reasons.loc[(reasons == "") & required_value_missing] = "required_value_missing"
    for column in ("event_time", "available_at", "ingested_at"):
        result[column] = pd.to_datetime(result[column], errors="coerce", utc=True)
    invalid_time = result[["event_time", "available_at", "ingested_at"]].isna().any(axis=1)
    reasons.loc[(reasons == "") & invalid_time] = "invalid_timestamp"
    temporal_order = result["ingested_at"] < result["available_at"]
    if not allows_pre_event_availability:
        temporal_order = temporal_order | (
            result["available_at"] < result["event_time"]
        )
    reasons.loc[(reasons == "") & temporal_order.fillna(False)] = "invalid_temporal_order"
    priority = pd.to_numeric(result["source_priority"], errors="coerce")
    invalid_priority = priority.isna() | priority.lt(0)
    reasons.loc[(reasons == "") & invalid_priority] = "invalid_source_priority"
    result["source_priority"] = priority
    optional_missing_mask = (
        (reasons == "") & result["value"].isna() & result["nullable"]
    )
    optional_missing = result.loc[optional_missing_mask].copy()
    bad = reasons != ""
    quarantined = result.loc[bad].copy()
    quarantined["reconciliation_status"] = "quarantined"
    quarantined["quarantine_reason"] = reasons.loc[bad]
    valid = result.loc[~bad & ~optional_missing_mask].copy()
    return valid, quarantined, optional_missing


def reconcile_observations(
    observations: pd.DataFrame,
    *,
    policies: Mapping[str, ComparisonPolicy] | None = None,
    default_policy: ComparisonPolicy = ComparisonPolicy(),
    allows_pre_event_availability: bool = False,
) -> ReconciliationResult:
    """Reconcile Silver observations without silently overwriting disagreements.

    Lower ``source_priority`` values are authoritative.  Priority chooses among
    equivalent observations only; it never resolves a material conflict.
    """

    missing = sorted(set(CANONICAL_COLUMNS) - set(observations.columns))
    if missing:
        raise ValueError(f"observations missing canonical columns: {missing}")
    policies = dict(policies or {})
    valid, quarantined, optional_missing = _quarantine_invalid_rows(
        observations,
        allows_pre_event_availability=allows_pre_event_availability,
    )

    singleton_marker = "__reconciliation_single_source_unique__"
    while singleton_marker in valid.columns:
        singleton_marker += "_"
    valid[singleton_marker] = _single_source_unique_group_mask(valid)
    source_keys = [*RECONCILIATION_KEY, "source_id"]
    valid = valid.sort_values([*source_keys, "ingested_at", "vendor_revision"])
    superseded_count = int(valid.duplicated(source_keys, keep="last").sum())
    valid = valid.drop_duplicates(source_keys, keep="last")
    singleton_mask = valid.pop(singleton_marker).astype(bool)
    singleton_valid = valid.loc[singleton_mask]
    grouped_valid = valid.loc[~singleton_mask]

    fast_accepted, fast_disputed = _reconcile_single_source_unique_groups(
        singleton_valid,
        policies=policies,
        default_policy=default_policy,
    )

    accepted_rows: list[pd.Series] = []
    disputed_parts: list[pd.DataFrame] = []
    accepted_group_count = int(len(fast_accepted))
    disputed_group_count = int(len(fast_disputed))
    for _, group in grouped_valid.groupby(
        list(RECONCILIATION_KEY), sort=True, dropna=False
    ):
        group = group.sort_values(["source_priority", "source_id"]).copy()
        digest = _canonical_group_digest(group)
        sources = json.dumps(group["source_id"].astype(str).tolist(), separators=(",", ":"))
        units = {None if pd.isna(value) else str(value) for value in group["unit"]}
        adjustments = {
            None if pd.isna(value) else str(value) for value in group["adjustment"]
        }
        nullable_contracts = {bool(value) for value in group["nullable"]}
        chosen = group.iloc[0].copy()
        policy = policies.get(str(chosen["field"]), default_policy)
        equivalent = all(_equivalent(chosen["value"], value, policy) for value in group["value"])
        comparable = (
            len(units) == 1
            and len(adjustments) == 1
            and len(nullable_contracts) == 1
        )
        if equivalent and comparable:
            chosen["reconciliation_status"] = "accepted"
            chosen["reconciliation_id"] = digest
            chosen["compared_sources"] = sources
            chosen["evidence_count"] = int(len(group))
            accepted_rows.append(chosen)
            accepted_group_count += 1
        else:
            disputed = group.copy()
            disputed["reconciliation_status"] = "disputed"
            disputed["reconciliation_id"] = digest
            disputed["compared_sources"] = sources
            if len(nullable_contracts) != 1:
                reason = "nullable_contract_mismatch"
            elif not comparable:
                reason = "unit_or_adjustment_mismatch"
            else:
                reason = "value_conflict"
            disputed["dispute_reason"] = reason
            disputed_parts.append(disputed)
            disputed_group_count += 1

    accepted_parts: list[pd.DataFrame] = []
    if not fast_accepted.empty:
        accepted_parts.append(fast_accepted)
    if accepted_rows:
        accepted_parts.append(pd.DataFrame(accepted_rows))
    if accepted_parts:
        accepted = pd.concat(accepted_parts, ignore_index=True)
        accepted = accepted.sort_values(
            list(RECONCILIATION_KEY), kind="mergesort"
        ).reset_index(drop=True)
    else:
        accepted = _empty_result_frame(
            ("reconciliation_status", "reconciliation_id", "compared_sources", "evidence_count")
        )
    if not fast_disputed.empty:
        disputed_parts.insert(0, fast_disputed)
    if disputed_parts:
        disputed = pd.concat(disputed_parts, ignore_index=True)
        disputed = disputed.sort_values(
            [*RECONCILIATION_KEY, "source_priority", "source_id"],
            kind="mergesort",
        ).reset_index(drop=True)
    else:
        disputed = _empty_result_frame(
            ("reconciliation_status", "reconciliation_id", "compared_sources", "dispute_reason")
        )
    if quarantined.empty:
        quarantined = _empty_result_frame(("reconciliation_status", "quarantine_reason"))
    else:
        quarantined = quarantined.reset_index(drop=True)

    audit = {
        "schema_version": RECONCILIATION_EVALUATOR_SCHEMA,
        "input_row_count": int(len(observations)),
        "latest_source_row_count": int(len(valid)),
        "superseded_row_count": superseded_count,
        "accepted_group_count": accepted_group_count,
        "disputed_group_count": disputed_group_count,
        "quarantined_row_count": int(len(quarantined)),
        "nullable_missing_row_count": int(len(optional_missing)),
        "status": "blocked" if disputed_group_count or len(quarantined) else "pass",
    }
    return ReconciliationResult(
        accepted=accepted,
        disputed=disputed,
        quarantined=quarantined,
        audit=audit,
    )


__all__ = [
    "ComparisonPolicy",
    "RECONCILIATION_EVALUATOR_SCHEMA",
    "RECONCILIATION_KEY",
    "ReconciliationResult",
    "production_comparison_policies",
    "reconcile_observations",
]

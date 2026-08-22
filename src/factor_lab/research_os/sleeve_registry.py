"""Content-addressed initial Sleeve registry and PIT-safe return clustering.

The registry is deliberately separate from lifecycle promotion.  An entry in
the roster is a preregistered research mechanism, not evidence that the
mechanism passed an authoritative experiment or belongs in the Champion.

Clustering consumes only cryptographically bound active-return evidence at an
explicit knowledge cutoff.  Representatives are selected by a frozen
priority/coverage rule; realised performance is never used to pick a winner.
"""

from __future__ import annotations

import json
import math
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Literal, Mapping, Sequence

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .catalog import CatalogConflict, ResearchCatalog, RunRecord
from .contracts import SignalFieldSpec, SleeveSpec
from .dsl import (
    Availability,
    DecisionPoint,
    DslValidationError,
    FieldRole,
    FieldSpec,
    ValueType,
    compile_factor_graph,
    factor_graph_from_spec,
)
from .fingerprint import FINGERPRINT_DOMAIN, canonical_json, content_fingerprint


ROSTER_SCHEMA_VERSION = "research-os/sleeve-roster/v1"
CLUSTER_SCHEMA_VERSION = "research-os/sleeve-clusters/v1"
INITIAL_ROSTER_NAME = "a-share-initial-mechanism-sleeves-v1"
INITIAL_SLEEVE_IDS = (
    "value_quality_v1",
    "low_risk_defensive_v1",
    "medium_term_trend_v1",
    "reversal_liquidity_v1",
)
DEFAULT_CORRELATION_THRESHOLD = 0.70
MINIMUM_COMMON_OBSERVATIONS = 60


class SleeveRegistryError(ValueError):
    """Raised when a roster or its typed signal contract is not admissible."""


class SleeveClusteringError(ValueError):
    """Raised when active-return evidence cannot be clustered safely."""


class _FrozenModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        validate_default=True,
    )


def _dsl_fields(registry: Sequence[SignalFieldSpec]) -> tuple[FieldSpec, ...]:
    fields: list[FieldSpec] = []
    for item in registry:
        if item.role != "feature":
            raise SleeveRegistryError(
                f"Sleeve signal field {item.name!r} must have role='feature'"
            )
        if not item.available_at_column:
            raise SleeveRegistryError(
                f"Sleeve signal field {item.name!r} has no PIT available_at binding"
            )
        try:
            fields.append(
                FieldSpec(
                    name=item.name,
                    value_type=ValueType(item.value_type),
                    role=FieldRole(item.role),
                    availability=Availability(item.availability),
                    minimum_lag_sessions=item.minimum_lag_sessions,
                    available_at_column=item.available_at_column,
                )
            )
        except ValueError as exc:
            raise SleeveRegistryError(
                f"Sleeve signal field {item.name!r} has an unsupported DSL contract"
            ) from exc
    return tuple(fields)


def validate_registered_sleeve(sleeve: SleeveSpec | Mapping[str, Any]) -> SleeveSpec:
    """Validate the non-negotiable contract of a research-only Sleeve entry."""

    try:
        spec = sleeve if isinstance(sleeve, SleeveSpec) else SleeveSpec.model_validate(sleeve)
    except Exception as exc:
        raise SleeveRegistryError("invalid SleeveSpec") from exc
    if not spec.long_only:
        raise SleeveRegistryError(f"Sleeve {spec.sleeve_id!r} must be long-only")
    if not math.isclose(spec.maximum_weight, 0.35, rel_tol=0.0, abs_tol=1e-12):
        raise SleeveRegistryError(
            f"Sleeve {spec.sleeve_id!r} must freeze maximum_weight at 0.35"
        )
    if not spec.cluster_id:
        raise SleeveRegistryError(f"Sleeve {spec.sleeve_id!r} has no cluster_id")
    if len(spec.factor_ids) != len(set(spec.factor_ids)):
        raise SleeveRegistryError(f"Sleeve {spec.sleeve_id!r} repeats factor_ids")
    if not spec.signal_field_registry:
        raise SleeveRegistryError(
            f"Sleeve {spec.sleeve_id!r} has no signal_field_registry"
        )
    try:
        graph = factor_graph_from_spec({"expression": spec.signal_expression})
        compiled = compile_factor_graph(
            graph,
            _dsl_fields(spec.signal_field_registry),
            decision_point=DecisionPoint.AFTER_CLOSE,
        )
    except (DslValidationError, ValueError) as exc:
        raise SleeveRegistryError(
            f"Sleeve {spec.sleeve_id!r} does not carry a valid typed PIT DSL: {exc}"
        ) from exc
    registered = {item.name for item in spec.signal_field_registry}
    referenced = set(compiled.field_lags)
    if registered != referenced:
        raise SleeveRegistryError(
            f"Sleeve {spec.sleeve_id!r} registry/DSL fields differ: "
            f"registered_only={sorted(registered - referenced)}, "
            f"dsl_only={sorted(referenced - registered)}"
        )
    return spec


class SleeveRosterEntry(_FrozenModel):
    sleeve: SleeveSpec
    representative_priority: int = Field(ge=0)
    registration_status: Literal["registered_research_only"] = (
        "registered_research_only"
    )

    @model_validator(mode="after")
    def _validate_sleeve(self) -> "SleeveRosterEntry":
        validate_registered_sleeve(self.sleeve)
        return self


def _roster_payload(
    *,
    roster_name: str,
    entries: Sequence[SleeveRosterEntry],
    promotion_policy: str,
) -> dict[str, Any]:
    return {
        "schema_version": ROSTER_SCHEMA_VERSION,
        "roster_name": roster_name,
        "promotion_policy": promotion_policy,
        "entries": [entry.model_dump(mode="python") for entry in entries],
    }


class SleeveRosterManifest(_FrozenModel):
    schema_version: Literal["research-os/sleeve-roster/v1"] = ROSTER_SCHEMA_VERSION
    roster_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    roster_name: str = Field(min_length=1, max_length=160)
    promotion_policy: Literal["authoritative_experiment_only"] = (
        "authoritative_experiment_only"
    )
    entries: tuple[SleeveRosterEntry, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_identity(self) -> "SleeveRosterManifest":
        sleeve_ids = [entry.sleeve.sleeve_id for entry in self.entries]
        if sleeve_ids != sorted(sleeve_ids):
            raise ValueError("roster entries must be sorted by sleeve_id")
        if len(sleeve_ids) != len(set(sleeve_ids)):
            raise ValueError("roster sleeve_ids must be unique")
        if self.roster_name == INITIAL_ROSTER_NAME and tuple(sleeve_ids) != tuple(
            sorted(INITIAL_SLEEVE_IDS)
        ):
            raise ValueError(
                "initial mechanism roster must contain exactly its four frozen Sleeves"
            )
        expected = content_fingerprint(
            _roster_payload(
                roster_name=self.roster_name,
                entries=self.entries,
                promotion_policy=self.promotion_policy,
            ),
            domain=f"{FINGERPRINT_DOMAIN}/sleeve-roster",
        )
        if self.roster_id != expected:
            raise ValueError("roster_id does not match roster content")
        return self

    def by_sleeve_id(self) -> dict[str, SleeveRosterEntry]:
        return {entry.sleeve.sleeve_id: entry for entry in self.entries}


def build_sleeve_roster_manifest(
    entries: Iterable[SleeveRosterEntry | Mapping[str, Any]],
    *,
    roster_name: str = INITIAL_ROSTER_NAME,
    promotion_policy: Literal["authoritative_experiment_only"] = (
        "authoritative_experiment_only"
    ),
) -> SleeveRosterManifest:
    """Build a deterministic, content-addressed research-only roster."""

    try:
        parsed = tuple(
            sorted(
                (
                    entry
                    if isinstance(entry, SleeveRosterEntry)
                    else SleeveRosterEntry.model_validate(entry)
                    for entry in entries
                ),
                key=lambda item: item.sleeve.sleeve_id,
            )
        )
    except Exception as exc:
        if isinstance(exc, SleeveRegistryError):
            raise
        raise SleeveRegistryError("invalid Sleeve roster entry") from exc
    if not parsed:
        raise SleeveRegistryError("Sleeve roster must not be empty")
    if roster_name == INITIAL_ROSTER_NAME:
        observed = tuple(entry.sleeve.sleeve_id for entry in parsed)
        if observed != tuple(sorted(INITIAL_SLEEVE_IDS)):
            raise SleeveRegistryError(
                "initial mechanism roster must contain exactly its four frozen Sleeves"
            )
    payload = _roster_payload(
        roster_name=roster_name,
        entries=parsed,
        promotion_policy=promotion_policy,
    )
    roster_id = content_fingerprint(
        payload,
        domain=f"{FINGERPRINT_DOMAIN}/sleeve-roster",
    )
    try:
        return SleeveRosterManifest(
            roster_id=roster_id,
            roster_name=roster_name,
            promotion_policy=promotion_policy,
            entries=parsed,
        )
    except Exception as exc:
        raise SleeveRegistryError("invalid Sleeve roster manifest") from exc


def load_sleeve_roster(path: str | Path) -> SleeveRosterManifest:
    """Load a JSON source document and derive its content-addressed manifest."""

    source = Path(path)
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SleeveRegistryError(f"cannot read Sleeve roster JSON: {source}") from exc
    if not isinstance(payload, Mapping):
        raise SleeveRegistryError("Sleeve roster JSON must be an object")
    if payload.get("schema_version") != ROSTER_SCHEMA_VERSION:
        raise SleeveRegistryError("unsupported Sleeve roster schema_version")
    allowed = {"schema_version", "roster_name", "promotion_policy", "entries"}
    unknown = sorted(set(payload) - allowed)
    if unknown:
        raise SleeveRegistryError(f"unknown Sleeve roster fields: {unknown}")
    manifest = build_sleeve_roster_manifest(
        payload.get("entries", ()),
        roster_name=str(payload.get("roster_name") or ""),
        promotion_policy=str(
            payload.get("promotion_policy") or "authoritative_experiment_only"
        ),
    )
    return manifest


def _assert_same_registry_run(existing: RunRecord, expected: RunRecord) -> None:
    if (
        existing.run_type != expected.run_type
        or existing.status != expected.status
        or existing.input_fingerprint != expected.input_fingerprint
        or canonical_json(existing.metadata) != canonical_json(expected.metadata)
        or existing.error != expected.error
    ):
        raise CatalogConflict(
            f"Sleeve registry run identity collision for {expected.run_id!r}"
        )


def _persist_registry_manifest(
    catalog: ResearchCatalog,
    *,
    identity: str,
    run_type: str,
    authority: str,
    manifest: Mapping[str, Any],
    recorded_at: datetime | None,
) -> RunRecord:
    timestamp = recorded_at or catalog.database_now()
    expected = RunRecord(
        run_id=f"{run_type}_{identity}",
        run_type=run_type,
        status="completed",
        input_fingerprint=identity,
        started_at=timestamp,
        completed_at=timestamp,
        metadata={"authority": authority, "manifest": dict(manifest)},
    )
    existing = catalog.get_run(expected.run_id)
    if existing is not None:
        _assert_same_registry_run(existing, expected)
        return existing
    claimed, won = catalog.claim_run(expected)
    if not won:
        _assert_same_registry_run(claimed, expected)
    return claimed


def persist_sleeve_roster(
    catalog: ResearchCatalog,
    roster: SleeveRosterManifest,
    *,
    recorded_at: datetime | None = None,
) -> RunRecord:
    """Persist a content-addressed research roster without promotion authority."""

    return _persist_registry_manifest(
        catalog,
        identity=roster.roster_id,
        run_type="sleeve_roster",
        authority="registered_research_only_no_promotion",
        manifest=roster.model_dump(mode="json"),
        recorded_at=recorded_at,
    )


class ActiveReturnObservation(_FrozenModel):
    session: date
    active_return: float
    available_at: datetime

    @field_validator("active_return")
    @classmethod
    def _finite_return(cls, value: float) -> float:
        if not math.isfinite(value):
            raise ValueError("active_return must be finite")
        return float(value)

    @field_validator("available_at")
    @classmethod
    def _aware_available_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("available_at must include a timezone")
        return value.astimezone(timezone.utc)


class ActiveReturnSourceRef(_FrozenModel):
    source_id: str = Field(min_length=1, max_length=240)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    as_of: datetime
    source_kind: Literal["authoritative_experiment", "shadow_ledger"]

    @field_validator("as_of")
    @classmethod
    def _aware_as_of(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("source as_of must include a timezone")
        return value.astimezone(timezone.utc)


def _series_payload(
    *,
    sleeve_id: str,
    source: ActiveReturnSourceRef,
    observations: Sequence[ActiveReturnObservation],
) -> dict[str, Any]:
    return {
        "sleeve_id": sleeve_id,
        "source": source.model_dump(mode="python"),
        "observations": [row.model_dump(mode="python") for row in observations],
    }


class SleeveActiveReturnSeries(_FrozenModel):
    sleeve_id: str = Field(min_length=1, max_length=160)
    source: ActiveReturnSourceRef
    observations: tuple[ActiveReturnObservation, ...] = Field(min_length=1)
    series_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def _validate_binding(self) -> "SleeveActiveReturnSeries":
        sessions = [row.session for row in self.observations]
        if sessions != sorted(sessions):
            raise ValueError("active-return observations must be sorted by session")
        if len(sessions) != len(set(sessions)):
            raise ValueError("active-return observations contain duplicate sessions")
        for row in self.observations:
            if row.session > self.source.as_of.date():
                raise ValueError("active-return session is after source as_of")
            if row.available_at.date() < row.session:
                raise ValueError("active return is marked available before its session")
            if row.available_at > self.source.as_of:
                raise ValueError("active return was not available at source as_of")
        expected = content_fingerprint(
            _series_payload(
                sleeve_id=self.sleeve_id,
                source=self.source,
                observations=self.observations,
            ),
            domain=f"{FINGERPRINT_DOMAIN}/sleeve-active-return-series",
        )
        if self.series_content_hash != expected:
            raise ValueError("series_content_hash does not match observations/source")
        return self


def build_active_return_series(
    *,
    sleeve_id: str,
    source_id: str,
    source_content_hash: str,
    source_as_of: datetime,
    source_kind: Literal["authoritative_experiment", "shadow_ledger"],
    observations: Iterable[ActiveReturnObservation | Mapping[str, Any]],
) -> SleeveActiveReturnSeries:
    """Bind an extracted active-return series to its immutable source result."""

    try:
        source = ActiveReturnSourceRef(
            source_id=source_id,
            content_hash=source_content_hash,
            as_of=source_as_of,
            source_kind=source_kind,
        )
        parsed = tuple(
            sorted(
                (
                    row
                    if isinstance(row, ActiveReturnObservation)
                    else ActiveReturnObservation.model_validate(row)
                    for row in observations
                ),
                key=lambda row: row.session,
            )
        )
    except Exception as exc:
        raise SleeveClusteringError("invalid active-return source or observation") from exc
    if not parsed:
        raise SleeveClusteringError("active-return observations must not be empty")
    digest = content_fingerprint(
        _series_payload(sleeve_id=sleeve_id, source=source, observations=parsed),
        domain=f"{FINGERPRINT_DOMAIN}/sleeve-active-return-series",
    )
    try:
        return SleeveActiveReturnSeries(
            sleeve_id=sleeve_id,
            source=source,
            observations=parsed,
            series_content_hash=digest,
        )
    except Exception as exc:
        raise SleeveClusteringError("active-return series failed PIT/content binding") from exc


class ClusterSourceBinding(_FrozenModel):
    sleeve_id: str = Field(min_length=1, max_length=160)
    source_id: str = Field(min_length=1, max_length=240)
    source_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_as_of: datetime
    series_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    observation_count: int = Field(ge=1)

    @field_validator("source_as_of")
    @classmethod
    def _aware_source_as_of(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("source_as_of must include a timezone")
        return value.astimezone(timezone.utc)


class CorrelationMatrixRow(_FrozenModel):
    sleeve_id: str
    correlations: tuple[float, ...]

    @field_validator("correlations")
    @classmethod
    def _finite_correlations(cls, values: tuple[float, ...]) -> tuple[float, ...]:
        if any(not math.isfinite(value) for value in values):
            raise ValueError("correlation matrix contains non-finite values")
        if any(abs(value) > 1.0 + 1e-12 for value in values):
            raise ValueError("correlation matrix values must be in [-1, 1]")
        return values


class SleeveCorrelationCluster(_FrozenModel):
    cluster_id: str = Field(pattern=r"^active-corr-[0-9a-f]{16}$")
    members: tuple[str, ...] = Field(min_length=1)
    representative_sleeve_id: str
    representative_rule: Literal["declared_priority_then_coverage_then_id"] = (
        "declared_priority_then_coverage_then_id"
    )

    @model_validator(mode="after")
    def _member_invariants(self) -> "SleeveCorrelationCluster":
        if self.members != tuple(sorted(self.members)):
            raise ValueError("cluster members must be sorted")
        if self.representative_sleeve_id not in self.members:
            raise ValueError("cluster representative must be a member")
        return self


def _cluster_manifest_payload(
    *,
    roster_id: str,
    as_of: datetime,
    correlation_threshold: float,
    minimum_common_observations: int,
    aligned_observations: int,
    common_start: date,
    common_end: date,
    source_bindings: Sequence[ClusterSourceBinding],
    sleeve_order: Sequence[str],
    correlation_matrix: Sequence[CorrelationMatrixRow],
    clusters: Sequence[SleeveCorrelationCluster],
) -> dict[str, Any]:
    return {
        "schema_version": CLUSTER_SCHEMA_VERSION,
        "roster_id": roster_id,
        "as_of": as_of,
        "method": "absolute_pearson_threshold_connected_components",
        "correlation_threshold": correlation_threshold,
        "minimum_common_observations": minimum_common_observations,
        "aligned_observations": aligned_observations,
        "common_start": common_start,
        "common_end": common_end,
        "source_bindings": [row.model_dump(mode="python") for row in source_bindings],
        "sleeve_order": list(sleeve_order),
        "correlation_matrix": [row.model_dump(mode="python") for row in correlation_matrix],
        "clusters": [row.model_dump(mode="python") for row in clusters],
        "promotion_effect": "none_research_dedup_only",
    }


class SleeveClusterManifest(_FrozenModel):
    schema_version: Literal["research-os/sleeve-clusters/v1"] = CLUSTER_SCHEMA_VERSION
    cluster_manifest_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    roster_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    as_of: datetime
    method: Literal["absolute_pearson_threshold_connected_components"] = (
        "absolute_pearson_threshold_connected_components"
    )
    correlation_threshold: float = Field(gt=0.0, lt=1.0)
    minimum_common_observations: int = Field(ge=60)
    aligned_observations: int = Field(ge=60)
    common_start: date
    common_end: date
    source_bindings: tuple[ClusterSourceBinding, ...]
    sleeve_order: tuple[str, ...]
    correlation_matrix: tuple[CorrelationMatrixRow, ...]
    clusters: tuple[SleeveCorrelationCluster, ...]
    promotion_effect: Literal["none_research_dedup_only"] = (
        "none_research_dedup_only"
    )

    @field_validator("as_of")
    @classmethod
    def _aware_as_of(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("cluster as_of must include a timezone")
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def _validate_identity(self) -> "SleeveClusterManifest":
        if self.sleeve_order != tuple(sorted(self.sleeve_order)):
            raise ValueError("sleeve_order must be sorted")
        if len(self.correlation_matrix) != len(self.sleeve_order):
            raise ValueError("correlation matrix row count is inconsistent")
        if tuple(row.sleeve_id for row in self.correlation_matrix) != self.sleeve_order:
            raise ValueError("correlation matrix row order is inconsistent")
        if tuple(row.sleeve_id for row in self.source_bindings) != self.sleeve_order:
            raise ValueError("source bindings must match sleeve_order")
        matrix_values: list[tuple[float, ...]] = []
        for row in self.correlation_matrix:
            if len(row.correlations) != len(self.sleeve_order):
                raise ValueError("correlation matrix column count is inconsistent")
            matrix_values.append(row.correlations)
        matrix = np.asarray(matrix_values, dtype=float)
        if not np.allclose(matrix, matrix.T, rtol=0.0, atol=1e-12):
            raise ValueError("correlation matrix must be symmetric")
        if not np.allclose(np.diag(matrix), 1.0, rtol=0.0, atol=1e-12):
            raise ValueError("correlation matrix diagonal must equal one")
        members = [member for cluster in self.clusters for member in cluster.members]
        if sorted(members) != list(self.sleeve_order) or len(members) != len(
            set(members)
        ):
            raise ValueError("clusters must partition sleeve_order exactly once")
        if tuple(cluster.cluster_id for cluster in self.clusters) != tuple(
            sorted(cluster.cluster_id for cluster in self.clusters)
        ):
            raise ValueError("clusters must be sorted by cluster_id")
        if self.common_start > self.common_end:
            raise ValueError("common active-return window is reversed")
        if self.common_end > self.as_of.date():
            raise ValueError("common active-return window extends beyond as_of")
        if self.aligned_observations < self.minimum_common_observations:
            raise ValueError("aligned observations are below the frozen minimum")
        if any(binding.source_as_of > self.as_of for binding in self.source_bindings):
            raise ValueError("cluster source is newer than as_of")
        expected = content_fingerprint(
            _cluster_manifest_payload(
                roster_id=self.roster_id,
                as_of=self.as_of,
                correlation_threshold=self.correlation_threshold,
                minimum_common_observations=self.minimum_common_observations,
                aligned_observations=self.aligned_observations,
                common_start=self.common_start,
                common_end=self.common_end,
                source_bindings=self.source_bindings,
                sleeve_order=self.sleeve_order,
                correlation_matrix=self.correlation_matrix,
                clusters=self.clusters,
            ),
            domain=f"{FINGERPRINT_DOMAIN}/sleeve-correlation-clusters",
        )
        if self.cluster_manifest_id != expected:
            raise ValueError("cluster_manifest_id does not match cluster content")
        return self

    @property
    def representatives(self) -> dict[str, str]:
        return {
            cluster.cluster_id: cluster.representative_sleeve_id
            for cluster in self.clusters
        }


def persist_sleeve_cluster_manifest(
    catalog: ResearchCatalog,
    manifest: SleeveClusterManifest,
    *,
    recorded_at: datetime | None = None,
) -> RunRecord:
    """Persist PIT-bound clustering evidence for research de-duplication only."""

    return _persist_registry_manifest(
        catalog,
        identity=manifest.cluster_manifest_id,
        run_type="sleeve_cluster_manifest",
        authority="research_dedup_only_no_promotion",
        manifest=manifest.model_dump(mode="json"),
        recorded_at=recorded_at,
    )


def _validate_evidence(
    evidence: Iterable[SleeveActiveReturnSeries | Mapping[str, Any]],
    *,
    roster: SleeveRosterManifest,
    as_of: datetime,
) -> tuple[SleeveActiveReturnSeries, ...]:
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise SleeveClusteringError("clustering as_of must include a timezone")
    cutoff = as_of.astimezone(timezone.utc)
    parsed: list[SleeveActiveReturnSeries] = []
    try:
        for item in evidence:
            parsed.append(
                item
                if isinstance(item, SleeveActiveReturnSeries)
                else SleeveActiveReturnSeries.model_validate(item)
            )
    except Exception as exc:
        raise SleeveClusteringError("active-return evidence is not content-bound") from exc
    parsed.sort(key=lambda item: item.sleeve_id)
    ids = [item.sleeve_id for item in parsed]
    expected = sorted(roster.by_sleeve_id())
    if len(ids) != len(set(ids)):
        raise SleeveClusteringError("duplicate active-return evidence for a Sleeve")
    if ids != expected:
        raise SleeveClusteringError(
            f"active-return evidence must match roster: missing={sorted(set(expected)-set(ids))}, "
            f"extra={sorted(set(ids)-set(expected))}"
        )
    for item in parsed:
        if item.source.as_of > cutoff:
            raise SleeveClusteringError(
                f"source {item.source.source_id!r} is newer than clustering as_of"
            )
        for row in item.observations:
            if row.session > cutoff.date() or row.available_at > cutoff:
                raise SleeveClusteringError(
                    f"Sleeve {item.sleeve_id!r} contains future evidence"
                )
    return tuple(parsed)


def _connected_components(
    order: Sequence[str], matrix: np.ndarray, threshold: float
) -> tuple[tuple[str, ...], ...]:
    adjacency: dict[str, set[str]] = {sleeve_id: set() for sleeve_id in order}
    for left_index, left in enumerate(order):
        for right_index in range(left_index + 1, len(order)):
            right = order[right_index]
            if abs(float(matrix[left_index, right_index])) >= threshold:
                adjacency[left].add(right)
                adjacency[right].add(left)
    components: list[tuple[str, ...]] = []
    unseen = set(order)
    while unseen:
        root = min(unseen)
        stack = [root]
        members: set[str] = set()
        while stack:
            current = stack.pop()
            if current in members:
                continue
            members.add(current)
            stack.extend(sorted(adjacency[current] - members, reverse=True))
        unseen -= members
        components.append(tuple(sorted(members)))
    return tuple(sorted(components))


def cluster_sleeve_active_returns(
    evidence: Iterable[SleeveActiveReturnSeries | Mapping[str, Any]],
    *,
    roster: SleeveRosterManifest,
    as_of: datetime,
    correlation_threshold: float = DEFAULT_CORRELATION_THRESHOLD,
    minimum_common_observations: int = MINIMUM_COMMON_OBSERVATIONS,
) -> SleeveClusterManifest:
    """Cluster aligned Sleeve active returns without looking at performance.

    Every roster Sleeve must have a bound source and at least 60 observations
    in the all-Sleeve intersection.  No imputation or pairwise sample changing
    is allowed, so a clustering decision can be exactly replayed.
    """

    if not 0.0 < correlation_threshold < 1.0:
        raise SleeveClusteringError("correlation_threshold must be in (0, 1)")
    if minimum_common_observations < MINIMUM_COMMON_OBSERVATIONS:
        raise SleeveClusteringError("minimum_common_observations cannot be below 60")
    cutoff = as_of.astimezone(timezone.utc) if as_of.tzinfo else as_of
    series = _validate_evidence(evidence, roster=roster, as_of=as_of)
    frames: list[pd.Series] = []
    for item in series:
        values = pd.Series(
            {row.session: row.active_return for row in item.observations},
            name=item.sleeve_id,
            dtype=float,
        )
        frames.append(values)
    aligned = pd.concat(frames, axis=1, join="inner").sort_index()
    if len(aligned) < minimum_common_observations:
        raise SleeveClusteringError(
            f"only {len(aligned)} common active-return observations; "
            f"{minimum_common_observations} required"
        )
    if aligned.isna().any().any() or not np.isfinite(aligned.to_numpy(dtype=float)).all():
        raise SleeveClusteringError("aligned active returns contain missing/non-finite values")
    raw_correlation = aligned.corr(method="pearson", min_periods=minimum_common_observations)
    if raw_correlation.isna().any().any():
        raise SleeveClusteringError(
            "active-return correlation is undefined (for example a constant series)"
        )
    order = tuple(sorted(aligned.columns.astype(str)))
    matrix = raw_correlation.loc[list(order), list(order)].to_numpy(dtype=float)
    matrix = np.round(matrix, decimals=12)
    np.fill_diagonal(matrix, 1.0)

    entry_by_id = roster.by_sleeve_id()
    coverage = {item.sleeve_id: len(item.observations) for item in series}
    components = _connected_components(order, matrix, correlation_threshold)
    clusters: list[SleeveCorrelationCluster] = []
    for members in components:
        representative = min(
            members,
            key=lambda sleeve_id: (
                entry_by_id[sleeve_id].representative_priority,
                -coverage[sleeve_id],
                sleeve_id,
            ),
        )
        cluster_hash = content_fingerprint(
            {
                "members": members,
                "method": "absolute_pearson_threshold_connected_components",
                "correlation_threshold": correlation_threshold,
            },
            domain=f"{FINGERPRINT_DOMAIN}/active-return-cluster",
        )
        clusters.append(
            SleeveCorrelationCluster(
                cluster_id=f"active-corr-{cluster_hash[:16]}",
                members=members,
                representative_sleeve_id=representative,
            )
        )
    clusters.sort(key=lambda item: item.cluster_id)

    bindings = tuple(
        ClusterSourceBinding(
            sleeve_id=item.sleeve_id,
            source_id=item.source.source_id,
            source_content_hash=item.source.content_hash,
            source_as_of=item.source.as_of,
            series_content_hash=item.series_content_hash,
            observation_count=len(item.observations),
        )
        for item in series
    )
    correlation_rows = tuple(
        CorrelationMatrixRow(
            sleeve_id=sleeve_id,
            correlations=tuple(float(value) for value in matrix[index]),
        )
        for index, sleeve_id in enumerate(order)
    )
    payload = _cluster_manifest_payload(
        roster_id=roster.roster_id,
        as_of=cutoff,
        correlation_threshold=float(correlation_threshold),
        minimum_common_observations=minimum_common_observations,
        aligned_observations=len(aligned),
        common_start=aligned.index.min(),
        common_end=aligned.index.max(),
        source_bindings=bindings,
        sleeve_order=order,
        correlation_matrix=correlation_rows,
        clusters=clusters,
    )
    manifest_id = content_fingerprint(
        payload,
        domain=f"{FINGERPRINT_DOMAIN}/sleeve-correlation-clusters",
    )
    try:
        return SleeveClusterManifest(
            cluster_manifest_id=manifest_id,
            roster_id=roster.roster_id,
            as_of=cutoff,
            correlation_threshold=float(correlation_threshold),
            minimum_common_observations=minimum_common_observations,
            aligned_observations=len(aligned),
            common_start=aligned.index.min(),
            common_end=aligned.index.max(),
            source_bindings=bindings,
            sleeve_order=order,
            correlation_matrix=correlation_rows,
            clusters=tuple(clusters),
        )
    except Exception as exc:
        raise SleeveClusteringError("failed to construct cluster manifest") from exc


__all__ = [
    "ActiveReturnObservation",
    "ActiveReturnSourceRef",
    "CLUSTER_SCHEMA_VERSION",
    "ClusterSourceBinding",
    "CorrelationMatrixRow",
    "DEFAULT_CORRELATION_THRESHOLD",
    "INITIAL_ROSTER_NAME",
    "INITIAL_SLEEVE_IDS",
    "MINIMUM_COMMON_OBSERVATIONS",
    "ROSTER_SCHEMA_VERSION",
    "SleeveActiveReturnSeries",
    "SleeveClusterManifest",
    "SleeveClusteringError",
    "SleeveCorrelationCluster",
    "SleeveRegistryError",
    "SleeveRosterEntry",
    "SleeveRosterManifest",
    "build_active_return_series",
    "build_sleeve_roster_manifest",
    "cluster_sleeve_active_returns",
    "load_sleeve_roster",
    "persist_sleeve_cluster_manifest",
    "persist_sleeve_roster",
    "validate_registered_sleeve",
]

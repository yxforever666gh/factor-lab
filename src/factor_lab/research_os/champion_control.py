"""Authoritative Champion allocation and point-in-time stock targets.

This module is deliberately catalog-first.  A Sleeve cannot affect the shadow
portfolio merely because it appears in a configuration file: it must have a
fingerprinted :class:`~factor_lab.research_os.contracts.SleeveSpec`, one
authoritative ``promoted_to_shadow`` result, and an append-only lifecycle
state.  The resulting allocation and stock target are themselves stored as
content-addressed ``ros_runs`` projections.

The stock target path accepts only a manifest-bound Gold frame and evaluates
the registered Sleeve DSL at the latest decision date.  There is intentionally
no JSON/file-input shortcut in this module.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timezone
from math import isfinite
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd

from .catalog import ResearchCatalog, RunRecord
from .champion import (
    ChampionChallengeDecision,
    ChampionChallengePolicy,
    evaluate_challenger,
)
from .contracts import (
    DataQualityStatus,
    DataSnapshotRef,
    ExperimentStatus,
    LifecycleState,
    SnapshotTier,
)
from .dsl import (
    Availability,
    DecisionPoint,
    EvaluationContext,
    FieldRole,
    FieldSpec,
    ValueType,
    evaluate_factor_graph,
)
from .fingerprint import canonical_json, content_fingerprint
from .field_safety import is_forward_derived_field
from .risk_optimizer import StockOptimizationPolicy, optimize_stock_weights
from .shadow_authority import (
    ShadowAuthorityError,
    ShadowEvidenceAuthority,
    ShadowRole,
    ShadowRoleBinding,
)
from .sleeve_registry import SleeveRosterManifest
from .snapshots import verify_snapshot_frame_binding


ALLOCATION_SCHEMA_VERSION = "research-os/champion-allocation/v1"
STOCK_TARGET_SCHEMA_VERSION = "research-os/champion-stock-target/v1"
ALLOCATION_RUN_TYPE = "champion_projection"
STOCK_TARGET_RUN_TYPE = "champion_stock_target"
ADAPTIVE_APPROVAL_RUN_TYPE = "champion_adaptive_approval"

_RESULT_DOMAIN = "factor-lab/research-os/v1/authoritative-result"
_ALLOCATION_DOMAIN = "factor-lab/research-os/v1/champion-allocation"
_STOCK_TARGET_DOMAIN = "factor-lab/research-os/v1/champion-stock-target"
_ADAPTIVE_APPROVAL_DOMAIN = "factor-lab/research-os/v1/champion-adaptive-approval"
_BLOCKING_TRUST_LABELS = frozenset(
    {
        "st_history_unverified",
        "legacy_untrusted_data",
        "legacy_execution_regression_only",
        "disputed",
        "quarantined",
    }
)
_HEALTHY_STATES = frozenset(
    {LifecycleState.ACTIVE, LifecycleState.REDUCED, LifecycleState.PROBATION}
)
_REQUIRED_CHALLENGER_CHECKS = frozenset(
    {
        "historical_data",
        "active_information_ratio",
        "drawdown_not_materially_worse",
        "positive_outer_years",
        "shadow_observation",
        "shadow_excess_positive",
    }
)
_CHALLENGE_AUTHORITY_DOMAIN = "factor-lab/research-os/v1/challenge-authority"


class ChampionControlError(RuntimeError):
    """A fail-closed Champion projection or target error."""


class ChampionProjectionUnavailable(ChampionControlError):
    """No trustworthy catalog projection is available for the requested date."""


class ChampionStockTargetUnavailable(ChampionControlError):
    """A current, Gold-bound stock target cannot be produced."""


def _aware(value: datetime, *, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must include a timezone")
    return value


def _float_map(values: Mapping[str, Any]) -> dict[str, float]:
    result: dict[str, float] = {}
    for key, raw in values.items():
        value = float(raw)
        if not isfinite(value) or value < -1e-12:
            raise ValueError(f"invalid non-negative weight for {key!r}")
        if value > 1e-12:
            result[str(key)] = max(0.0, value)
    return dict(sorted(result.items()))


def _allocation_payload(
    sleeve_weights: Mapping[str, float],
    *,
    benchmark_weight: float,
    cash_weight: float,
    reason: str,
) -> dict[str, Any]:
    sleeves = _float_map(sleeve_weights)
    benchmark = float(benchmark_weight)
    cash = float(cash_weight)
    values = (*sleeves.values(), benchmark, cash)
    if any(not isfinite(item) or item < -1e-12 for item in values):
        raise ValueError("allocation weights must be finite and non-negative")
    total = sum(sleeves.values()) + benchmark + cash
    if abs(total - 1.0) > 1e-8:
        raise ValueError(f"allocation must sum to one, observed {total:.12f}")
    return {
        "sleeve_weights": sleeves,
        "benchmark_weight": max(0.0, benchmark),
        "cash_weight": max(0.0, cash),
        "total_weight": total,
        "reason": str(reason),
    }


@dataclass(frozen=True)
class AuthoritativeSleeveEvidence:
    sleeve_id: str
    cluster_id: str
    experiment_id: str
    experiment_fingerprint: str
    result_id: str
    result_hash: str
    result_completed_at: datetime
    research_snapshot_id: str
    lifecycle_state: str
    maximum_weight: float

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["result_completed_at"] = self.result_completed_at.isoformat()
        return payload

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "AuthoritativeSleeveEvidence":
        return cls(
            sleeve_id=str(payload["sleeve_id"]),
            cluster_id=str(payload["cluster_id"]),
            experiment_id=str(payload["experiment_id"]),
            experiment_fingerprint=str(payload["experiment_fingerprint"]),
            result_id=str(payload["result_id"]),
            result_hash=str(payload["result_hash"]),
            result_completed_at=datetime.fromisoformat(
                str(payload["result_completed_at"]).replace("Z", "+00:00")
            ),
            research_snapshot_id=str(payload["research_snapshot_id"]),
            lifecycle_state=LifecycleState(str(payload["lifecycle_state"])).value,
            maximum_weight=float(payload["maximum_weight"]),
        )


@dataclass(frozen=True)
class ChampionAllocationProjection:
    projection_id: str
    generated_at: datetime
    data_snapshot_id: str
    data_snapshot_hash: str
    data_quality_ok: bool
    candidates: tuple[AuthoritativeSleeveEvidence, ...]
    static_allocation: Mapping[str, Any]
    state_overlay: Mapping[str, Any]
    model_allocation: Mapping[str, Any]
    effective_allocation: Mapping[str, Any]
    previous_projection_id: str | None = None
    schema_version: str = ALLOCATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _aware(self.generated_at, name="generated_at")
        if self.schema_version != ALLOCATION_SCHEMA_VERSION:
            raise ValueError("unsupported Champion allocation schema")
        for name, payload in (
            ("static_allocation", self.static_allocation),
            ("model_allocation", self.model_allocation),
            ("effective_allocation", self.effective_allocation),
        ):
            checked = _allocation_payload(
                dict(payload.get("sleeve_weights") or {}),
                benchmark_weight=float(payload.get("benchmark_weight", 0.0)),
                cash_weight=float(payload.get("cash_weight", 0.0)),
                reason=str(payload.get("reason") or ""),
            )
            if canonical_json(checked) != canonical_json(dict(payload)):
                raise ValueError(f"{name} is not canonical")
        clusters = [item.cluster_id for item in self.candidates]
        if len(clusters) != len(set(clusters)):
            raise ValueError("Champion projection has multiple representatives per cluster")
        expected = content_fingerprint(
            self.content_payload(), domain=_ALLOCATION_DOMAIN
        )
        if self.projection_id != expected:
            raise ValueError("Champion allocation projection identity differs from content")

    def content_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "generated_at": self.generated_at.isoformat(),
            "data_snapshot_id": self.data_snapshot_id,
            "data_snapshot_hash": self.data_snapshot_hash,
            "data_quality_ok": bool(self.data_quality_ok),
            "candidates": [item.to_dict() for item in self.candidates],
            "static_allocation": dict(self.static_allocation),
            "state_overlay": dict(self.state_overlay),
            "model_allocation": dict(self.model_allocation),
            "effective_allocation": dict(self.effective_allocation),
            "previous_projection_id": self.previous_projection_id,
        }

    def to_dict(self) -> dict[str, Any]:
        return {"projection_id": self.projection_id, **self.content_payload()}

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "ChampionAllocationProjection":
        return cls(
            projection_id=str(payload["projection_id"]),
            generated_at=datetime.fromisoformat(
                str(payload["generated_at"]).replace("Z", "+00:00")
            ),
            data_snapshot_id=str(payload["data_snapshot_id"]),
            data_snapshot_hash=str(payload["data_snapshot_hash"]),
            data_quality_ok=bool(payload["data_quality_ok"]),
            candidates=tuple(
                AuthoritativeSleeveEvidence.from_dict(item)
                for item in payload.get("candidates", ())
            ),
            static_allocation=dict(payload["static_allocation"]),
            state_overlay=dict(payload["state_overlay"]),
            model_allocation=dict(payload["model_allocation"]),
            effective_allocation=dict(payload["effective_allocation"]),
            previous_projection_id=(
                None
                if payload.get("previous_projection_id") is None
                else str(payload["previous_projection_id"])
            ),
            schema_version=str(payload["schema_version"]),
        )


@dataclass(frozen=True)
class ChampionStockTarget:
    target_id: str
    generated_at: datetime
    decision_date: date
    allocation_projection_id: str
    gold_snapshot_id: str
    gold_snapshot_hash: str
    lifecycle_states: Mapping[str, str]
    effective_allocation: Mapping[str, Any]
    target_weights: Mapping[str, float]
    component_audit: Mapping[str, Any]
    cash_weight: float
    supersedes_target_id: str | None = None
    schema_version: str = STOCK_TARGET_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _aware(self.generated_at, name="generated_at")
        if self.schema_version != STOCK_TARGET_SCHEMA_VERSION:
            raise ValueError("unsupported Champion stock-target schema")
        weights = _float_map(self.target_weights)
        if any(weight > 0.02 + 1e-12 for weight in weights.values()):
            raise ValueError("Champion stock target exceeds the 2% stock cap")
        if (
            not isfinite(float(self.cash_weight))
            or not 0.0 <= float(self.cash_weight) <= 1.0
            or abs(sum(weights.values()) + float(self.cash_weight) - 1.0) > 1e-8
        ):
            raise ValueError("Champion stock weights plus cash must sum to one")
        expected = content_fingerprint(
            self.content_payload(), domain=_STOCK_TARGET_DOMAIN
        )
        if self.target_id != expected:
            raise ValueError("Champion stock-target identity differs from content")

    def content_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "generated_at": self.generated_at.isoformat(),
            "decision_date": self.decision_date.isoformat(),
            "allocation_projection_id": self.allocation_projection_id,
            "gold_snapshot_id": self.gold_snapshot_id,
            "gold_snapshot_hash": self.gold_snapshot_hash,
            "lifecycle_states": dict(sorted(self.lifecycle_states.items())),
            "effective_allocation": dict(self.effective_allocation),
            "target_weights": _float_map(self.target_weights),
            "component_audit": dict(self.component_audit),
            "cash_weight": float(self.cash_weight),
            "supersedes_target_id": self.supersedes_target_id,
        }

    def to_dict(self) -> dict[str, Any]:
        return {"target_id": self.target_id, **self.content_payload()}

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "ChampionStockTarget":
        return cls(
            target_id=str(payload["target_id"]),
            generated_at=datetime.fromisoformat(
                str(payload["generated_at"]).replace("Z", "+00:00")
            ),
            decision_date=date.fromisoformat(str(payload["decision_date"])),
            allocation_projection_id=str(payload["allocation_projection_id"]),
            gold_snapshot_id=str(payload["gold_snapshot_id"]),
            gold_snapshot_hash=str(payload["gold_snapshot_hash"]),
            lifecycle_states={
                str(key): LifecycleState(str(value)).value
                for key, value in dict(payload["lifecycle_states"]).items()
            },
            effective_allocation=dict(payload["effective_allocation"]),
            target_weights=_float_map(dict(payload["target_weights"])),
            component_audit=dict(payload["component_audit"]),
            cash_weight=float(payload["cash_weight"]),
            supersedes_target_id=(
                None
                if payload.get("supersedes_target_id") is None
                else str(payload["supersedes_target_id"])
            ),
            schema_version=str(payload["schema_version"]),
        )


def _verified_result_hash(
    *, experiment_id: str, outcome: str, metrics: Mapping[str, Any], artifact_uri: str | None
) -> str:
    return content_fingerprint(
        {
            "experiment_id": experiment_id,
            "outcome": outcome,
            "metrics": dict(metrics),
            "artifact_uri": artifact_uri,
        },
        domain=_RESULT_DOMAIN,
    )


def _validate_promoted_spec(spec: Any) -> None:
    sleeve = spec.sleeve
    if sleeve is None:
        raise ChampionControlError("Champion candidates must be SleeveSpec experiments")
    if not sleeve.cluster_id:
        raise ChampionControlError(
            f"promoted Sleeve {sleeve.sleeve_id!r} has no fingerprint-bound cluster_id"
        )
    portfolio = spec.portfolio
    universe = spec.universe
    violations: list[str] = []
    if not sleeve.long_only or portfolio.mode != "long_only":
        violations.append("long_only_required")
    if universe.mode != "monthly_liquid_top_n" or universe.target_size != 500:
        violations.append("pit_top_500_required")
    if not universe.point_in_time or universe.membership_lag_months < 1:
        violations.append("point_in_time_membership_required")
    if abs(portfolio.capital - 50_000_000.0) > 1e-6:
        violations.append("capital_must_equal_50000000")
    if portfolio.rebalance_sessions != 5:
        violations.append("weekly_rebalance_required")
    if not 50 <= portfolio.target_position_count <= 100:
        violations.append("position_count_must_be_50_to_100")
    if portfolio.maximum_stock_weight > 0.02 + 1e-12:
        violations.append("maximum_stock_weight_above_2pct")
    if portfolio.maximum_adv_participation > 0.05 + 1e-12:
        violations.append("adv_participation_above_5pct")
    if portfolio.benchmark != "eligible_universe_equal_weight":
        violations.append("pit_equal_weight_benchmark_required")
    if violations:
        raise ChampionControlError(
            f"promoted Sleeve {sleeve.sleeve_id!r} violates the production contract: "
            + ",".join(violations)
        )


def _static_cluster_weights(
    candidates: Iterable[AuthoritativeSleeveEvidence],
) -> dict[str, Any]:
    rows = list(candidates)
    if not rows:
        return _allocation_payload(
            {}, benchmark_weight=0.5, cash_weight=0.5, reason="no_authoritative_sleeves"
        )
    clusters: dict[str, list[AuthoritativeSleeveEvidence]] = {}
    for item in rows:
        clusters.setdefault(item.cluster_id, []).append(item)
    cluster_weight = 1.0 / len(clusters)
    weights: dict[str, float] = {}
    for members in clusters.values():
        member_weight = cluster_weight / len(members)
        for item in members:
            weights[item.sleeve_id] = min(
                member_weight, item.maximum_weight, 0.35
            )
    residual = max(0.0, 1.0 - sum(weights.values()))
    return _allocation_payload(
        weights,
        benchmark_weight=residual,
        cash_weight=0.0,
        reason="cluster_balanced_authoritative_static_champion",
    )


def _model_overlay(
    static: Mapping[str, Any],
    candidates: Iterable[AuthoritativeSleeveEvidence],
    scores: Mapping[str, float],
    *,
    previous: ChampionAllocationProjection | None,
    adaptive_fraction: float,
    max_monthly_change: float,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not 0.0 <= adaptive_fraction <= 0.25:
        raise ValueError("adaptive_fraction must be in [0, 0.25]")
    if not 0.0 < max_monthly_change <= 0.05:
        raise ValueError("max_monthly_change must be in (0, 0.05]")
    rows = {item.sleeve_id: item for item in candidates}
    unknown = sorted(set(scores) - set(rows))
    if unknown:
        raise ChampionControlError(
            f"state overlay names non-authoritative Sleeves: {unknown}"
        )
    positive = {
        key: float(value)
        for key, value in scores.items()
        if isfinite(float(value)) and float(value) > 0.0
    }
    score_total = sum(positive.values())
    adaptive = (
        {key: value / score_total for key, value in positive.items()}
        if score_total > 0
        else {}
    )
    static_weights = _float_map(dict(static.get("sleeve_weights") or {}))
    if adaptive:
        desired = {
            sleeve_id: (1.0 - adaptive_fraction)
            * static_weights.get(sleeve_id, 0.0)
            + adaptive_fraction * adaptive.get(sleeve_id, 0.0)
            for sleeve_id in rows
        }
        reason = "75pct_static_25pct_state_conditioned"
    else:
        desired = dict(static_weights)
        reason = "static_champion_no_positive_overlay"

    previous_weights: dict[str, float] = {}
    if previous is not None:
        previous_weights = _float_map(
            dict(previous.model_allocation.get("sleeve_weights") or {})
        )
        for sleeve_id in rows:
            prior = previous_weights.get(sleeve_id, 0.0)
            desired[sleeve_id] = min(
                max(desired.get(sleeve_id, 0.0), prior - max_monthly_change),
                prior + max_monthly_change,
            )
    bounded = {
        sleeve_id: min(
            max(float(desired.get(sleeve_id, 0.0)), 0.0),
            rows[sleeve_id].maximum_weight,
            0.35,
        )
        for sleeve_id in rows
        if float(desired.get(sleeve_id, 0.0)) > 1e-12
    }
    if previous is not None:
        for sleeve_id, value in bounded.items():
            if abs(value - previous_weights.get(sleeve_id, 0.0)) > max_monthly_change + 1e-12:
                raise ChampionControlError("monthly Sleeve change exceeded 5 percentage points")
    residual = max(0.0, 1.0 - sum(bounded.values()))
    allocation = _allocation_payload(
        bounded,
        benchmark_weight=residual,
        cash_weight=0.0,
        reason=reason,
    )
    overlay = {
        "adaptive_fraction": adaptive_fraction if adaptive else 0.0,
        "scores": dict(sorted((str(k), float(v)) for k, v in scores.items())),
        "normalized_positive_weights": dict(sorted(adaptive.items())),
        "maximum_monthly_change": max_monthly_change,
        "previous_model_weights": previous_weights,
    }
    return allocation, overlay


def _effective_health_allocation(
    model_allocation: Mapping[str, Any],
    states: Mapping[str, LifecycleState],
    *,
    data_quality_ok: bool,
    previous_effective: Mapping[str, float] | None = None,
) -> dict[str, Any]:
    if not data_quality_ok or any(
        state is LifecycleState.FROZEN_DATA for state in states.values()
    ):
        return _allocation_payload(
            {}, benchmark_weight=0.0, cash_weight=1.0, reason="data_integrity_failure"
        )
    model = _float_map(dict(model_allocation.get("sleeve_weights") or {}))
    previous = _float_map(dict(previous_effective or {}))
    retained: dict[str, float] = {}
    for sleeve_id, target in model.items():
        state = states.get(sleeve_id, LifecycleState.DORMANT)
        if state is LifecycleState.ACTIVE:
            retained[sleeve_id] = target
        elif state is LifecycleState.REDUCED:
            retained[sleeve_id] = target * 0.5
        elif state is LifecycleState.PROBATION:
            retained[sleeve_id] = min(
                target, previous.get(sleeve_id, 0.0) + 0.05
            )
    retained = {key: value for key, value in retained.items() if value > 1e-12}
    retained_total = sum(retained.values())
    if retained_total <= 1e-12:
        return _allocation_payload(
            {}, benchmark_weight=0.5, cash_weight=0.5, reason="no_healthy_sleeve"
        )
    return _allocation_payload(
        retained,
        benchmark_weight=max(0.0, 1.0 - retained_total),
        cash_weight=0.0,
        reason="lifecycle_degradation_moves_removed_weight_to_benchmark",
    )


class AuthoritativeChampionControl:
    """Build and persist Champion projections solely from catalog truth."""

    def __init__(
        self,
        catalog: ResearchCatalog,
        *,
        shadow_authority: ShadowEvidenceAuthority | None = None,
        legacy_shadow_evidence: bool | None = None,
    ) -> None:
        self.catalog = catalog
        self.shadow_authority = shadow_authority
        inferred_legacy = (
            getattr(catalog, "_backend", None).__class__.__name__ == "_SQLiteCatalog"
        )
        self.legacy_shadow_evidence = (
            inferred_legacy
            if legacy_shadow_evidence is None
            else bool(legacy_shadow_evidence)
        )

    def authoritative_sleeves(self) -> tuple[AuthoritativeSleeveEvidence, ...]:
        experiments = self.catalog.list_experiments(limit=1_000)
        if len(experiments) >= 1_000:
            raise ChampionControlError(
                "experiment catalog reached the non-paginated safety limit"
            )
        selected: dict[str, tuple[Any, Any]] = {}
        for experiment in experiments:
            if experiment.status is not ExperimentStatus.COMPLETED:
                continue
            if experiment.spec.sleeve is None:
                continue
            result = self.catalog.get_authoritative_result(experiment.experiment_id)
            if result is None or not result.authoritative:
                continue
            if result.outcome != "promoted_to_shadow":
                continue
            if experiment.fingerprint != experiment.spec.fingerprint():
                raise ChampionControlError("catalog experiment fingerprint is corrupt")
            research_snapshot = self.catalog.get_snapshot(
                experiment.spec.snapshot.snapshot_id
            )
            if (
                research_snapshot is None
                or research_snapshot.reference != experiment.spec.snapshot
                or research_snapshot.reference.tier is not SnapshotTier.GOLD
                or research_snapshot.reference.quality_status
                is not DataQualityStatus.ACCEPTED
                or _BLOCKING_TRUST_LABELS
                & set(research_snapshot.reference.trust_labels)
            ):
                raise ChampionControlError(
                    "promoted Sleeve result is not bound to an accepted Gold snapshot"
                )
            expected_result_hash = _verified_result_hash(
                experiment_id=experiment.experiment_id,
                outcome=result.outcome,
                metrics=result.metrics,
                artifact_uri=result.artifact_uri,
            )
            if result.result_hash != expected_result_hash:
                raise ChampionControlError("authoritative result hash is corrupt")
            _validate_promoted_spec(experiment.spec)
            sleeve_id = experiment.spec.sleeve.sleeve_id
            existing = selected.get(sleeve_id)
            key = (result.completed_at, experiment.experiment_id)
            if existing is None or key > (existing[1].completed_at, existing[0].experiment_id):
                selected[sleeve_id] = (experiment, result)

        evidence: list[AuthoritativeSleeveEvidence] = []
        for sleeve_id, (experiment, result) in sorted(selected.items()):
            sleeve = experiment.spec.sleeve
            assert sleeve is not None and sleeve.cluster_id is not None
            state = self.catalog.latest_lifecycle_state(sleeve_id)
            if state is None:
                raise ChampionControlError(
                    f"promoted Sleeve {sleeve_id!r} has no lifecycle evidence"
                )
            evidence.append(
                AuthoritativeSleeveEvidence(
                    sleeve_id=sleeve_id,
                    cluster_id=sleeve.cluster_id,
                    experiment_id=experiment.experiment_id,
                    experiment_fingerprint=experiment.fingerprint,
                    result_id=result.result_id,
                    result_hash=result.result_hash,
                    result_completed_at=result.completed_at,
                    research_snapshot_id=experiment.spec.snapshot.snapshot_id,
                    lifecycle_state=state.value,
                    maximum_weight=min(float(sleeve.maximum_weight), 0.35),
                )
            )
        # One authoritative representative per correlated cluster.  Choosing
        # the newest promoted result is deterministic and bound to its result
        # fingerprint; it avoids silently multiplying exposure because several
        # near-duplicate Sleeves happened to survive in the same cluster.
        representatives: dict[str, AuthoritativeSleeveEvidence] = {}
        for item in evidence:
            previous = representatives.get(item.cluster_id)
            if previous is None or (
                item.result_completed_at,
                item.experiment_id,
            ) > (
                previous.result_completed_at,
                previous.experiment_id,
            ):
                representatives[item.cluster_id] = item
        return tuple(
            sorted(representatives.values(), key=lambda item: item.sleeve_id)
        )

    def _historical_result_returns(
        self,
        experiment_id: str,
        *,
        promoted_binding: ShadowRoleBinding | None = None,
    ) -> tuple[pd.Series, dict[str, Any]]:
        experiment = self.catalog.get_experiment(str(experiment_id))
        result = self.catalog.get_authoritative_result(str(experiment_id))
        if (
            experiment is None
            or experiment.status is not ExperimentStatus.COMPLETED
            or result is None
            or not result.authoritative
        ):
            raise ChampionControlError(
                f"historical source {experiment_id!r} is not an authoritative completed experiment"
            )
        if promoted_binding is not None:
            expected_role = promoted_binding.role
            if expected_role not in {ShadowRole.CHAMPION, ShadowRole.CHALLENGER}:
                raise ChampionControlError(
                    "historical comparison requires an active Champion/Challenger role"
                )
            if (
                not promoted_binding.active
                or promoted_binding.experiment_id != experiment.experiment_id
                or not promoted_binding.sleeve_id
                or experiment.spec.sleeve is None
                or experiment.spec.factor is not None
                or experiment.spec.sleeve.sleeve_id != promoted_binding.sleeve_id
                or result.outcome != "promoted_to_shadow"
            ):
                raise ChampionControlError(
                    f"historical {expected_role.value} is not backed by a promoted authoritative Sleeve"
                )
        if experiment.fingerprint != experiment.spec.fingerprint():
            raise ChampionControlError("historical experiment fingerprint is corrupt")
        expected_hash = _verified_result_hash(
            experiment_id=experiment.experiment_id,
            outcome=result.outcome,
            metrics=result.metrics,
            artifact_uri=result.artifact_uri,
        )
        if result.result_hash != expected_hash:
            raise ChampionControlError("historical authoritative result hash is corrupt")
        if promoted_binding is not None:
            roster_id = str(
                promoted_binding.metadata.get("roster_manifest_id") or ""
            ).strip()
            roster_run = (
                None
                if not roster_id
                else self.catalog.get_run(f"sleeve_roster_{roster_id}")
            )
            roster_payload = (
                None
                if roster_run is None
                else roster_run.metadata.get("manifest")
            )
            try:
                roster = (
                    None
                    if not isinstance(roster_payload, Mapping)
                    else SleeveRosterManifest.model_validate(roster_payload)
                )
            except Exception as exc:
                raise ChampionControlError(
                    f"historical {promoted_binding.role.value} roster manifest is invalid"
                ) from exc
            if (
                not roster_id
                or roster_run is None
                or roster_run.run_type != "sleeve_roster"
                or roster_run.status != "completed"
                or roster_run.input_fingerprint != roster_id
                or roster is None
                or roster.roster_id != roster_id
                or roster.by_sleeve_id().get(str(promoted_binding.sleeve_id)) is None
                or roster.by_sleeve_id()[str(promoted_binding.sleeve_id)].sleeve
                != experiment.spec.sleeve
                or str(promoted_binding.metadata.get("result_id") or "")
                != result.result_id
                or str(promoted_binding.metadata.get("result_hash") or "")
                != result.result_hash
                or promoted_binding.bound_at < result.completed_at
            ):
                raise ChampionControlError(
                    f"historical {promoted_binding.role.value} role differs from its immutable roster/result authority"
                )
            research_snapshot = self.catalog.get_snapshot(
                experiment.spec.snapshot.snapshot_id
            )
            if (
                research_snapshot is None
                or research_snapshot.reference != experiment.spec.snapshot
                or research_snapshot.reference.tier is not SnapshotTier.GOLD
                or research_snapshot.reference.quality_status
                is not DataQualityStatus.ACCEPTED
                or _BLOCKING_TRUST_LABELS
                & set(research_snapshot.reference.trust_labels)
            ):
                raise ChampionControlError(
                    f"historical {promoted_binding.role.value} result is not bound to accepted Gold"
                )
        folds = result.metrics.get("fold_results")
        if not isinstance(folds, (list, tuple)):
            raise ChampionControlError(
                "historical authoritative result has no stitched outer-OOS periods"
            )
        rows: list[tuple[pd.Timestamp, float]] = []
        for fold in folds:
            if not isinstance(fold, Mapping):
                raise ChampionControlError("historical outer fold payload is malformed")
            for period in fold.get("periods") or ():
                if not isinstance(period, Mapping) or "net_return" not in period:
                    raise ChampionControlError("historical outer period payload is malformed")
                raw_date = period.get("start_date") or period.get("signal_date")
                if not raw_date:
                    raise ChampionControlError("historical outer period has no session date")
                stamp = pd.Timestamp(raw_date).normalize()
                value = float(period["net_return"])
                if not isfinite(value) or value <= -1.0:
                    raise ChampionControlError("historical outer return is invalid")
                rows.append((stamp, value))
        if not rows:
            raise ChampionControlError("historical authoritative result has no OOS returns")
        rows.sort(key=lambda item: item[0])
        index = pd.DatetimeIndex(item[0] for item in rows)
        if index.has_duplicates:
            raise ChampionControlError("historical outer-OOS sessions are duplicated")
        return (
            pd.Series([item[1] for item in rows], index=index, dtype=float),
            {
                "experiment_id": experiment.experiment_id,
                "experiment_fingerprint": experiment.fingerprint,
                "result_id": result.result_id,
                "result_hash": result.result_hash,
                "result_completed_at": result.completed_at.isoformat(),
                "observation_count": len(rows),
                "first_session": rows[0][0].date().isoformat(),
                "last_session": rows[-1][0].date().isoformat(),
            },
        )

    def _shadow_projection_returns(
        self, account_id: str, *, first_forward_session: date
    ) -> tuple[pd.Series, dict[str, Any]]:
        account = self.catalog.get_shadow_account(str(account_id))
        if account is None:
            raise ChampionControlError(f"shadow account {account_id!r} is not cataloged")
        if not self.catalog.verify_shadow_chain(account.account_id):
            raise ChampionControlError(
                f"shadow account {account.account_id!r} has a corrupt event chain"
            )
        all_projections = self.catalog.list_shadow_events_by_type(
            account_id=account.account_id,
            event_type="account_projected",
            limit=1_000,
        )
        if len(all_projections) >= 1_000:
            raise ChampionControlError(
                "shadow projection history reached the non-paginated safety limit"
            )
        if any(event.occurred_at.date() < first_forward_session for event in all_projections):
            raise ChampionControlError(
                "forward-comparison shadow account contains pre-freeze NAV projections"
            )
        since = datetime.combine(first_forward_session, time.min, tzinfo=timezone.utc)
        events = self.catalog.list_shadow_events_by_type(
            account_id=account.account_id,
            event_type="account_projected",
            since=since,
            limit=1_000,
        )
        events.sort(key=lambda item: item.sequence_number)
        rows: list[tuple[pd.Timestamp, float, str, int]] = []
        observed_days: set[date] = set()
        prior_nav = float(account.initial_capital)
        if not isfinite(prior_nav) or prior_nav <= 0:
            raise ChampionControlError("shadow account initial capital is invalid")
        for event in events:
            session = event.occurred_at.date()
            if session < first_forward_session:
                raise ChampionControlError("pre-freeze shadow event entered forward evidence")
            if session in observed_days:
                raise ChampionControlError("shadow account has duplicate NAV projections per session")
            observed_days.add(session)
            state = event.payload.get("account_state")
            if not isinstance(state, Mapping):
                raise ChampionControlError("shadow NAV projection payload is malformed")
            nav = float(state.get("nav") or 0.0)
            if not isfinite(nav) or nav <= 0:
                raise ChampionControlError("shadow NAV projection is invalid")
            session_return = nav / prior_nav - 1.0
            if not isfinite(session_return) or session_return <= -1.0:
                raise ChampionControlError("shadow NAV return is invalid")
            rows.append(
                (pd.Timestamp(session), session_return, event.event_hash, event.sequence_number)
            )
            prior_nav = nav
        if not rows:
            raise ChampionControlError("shadow account has no post-freeze NAV projections")
        return (
            pd.Series(
                [item[1] for item in rows],
                index=pd.DatetimeIndex(item[0] for item in rows),
                dtype=float,
            ),
            {
                "account_id": account.account_id,
                "account_chain_tip": account.last_event_hash,
                "account_last_sequence": account.last_event_sequence,
                "projection_chain_tip": rows[-1][2],
                "projection_last_sequence": rows[-1][3],
                "projection_count": len(rows),
                "first_session": rows[0][0].date().isoformat(),
                "last_session": rows[-1][0].date().isoformat(),
            },
        )

    @staticmethod
    def _policy_payload(policy: ChampionChallengePolicy) -> dict[str, Any]:
        return {
            "periods_per_year": float(policy.periods_per_year),
            "min_active_information_ratio": float(policy.min_active_information_ratio),
            "max_drawdown_deterioration": float(policy.max_drawdown_deterioration),
            "min_positive_outer_years": int(policy.min_positive_outer_years),
            "min_shadow_sessions": int(policy.min_shadow_sessions),
        }

    def _authoritative_challenge_inputs(
        self,
        *,
        historical_challenger_experiment_id: str,
        shadow_challenger_account_id: str,
        shadow_champion_account_id: str,
        policy: ChampionChallengePolicy,
        through: date | None = None,
    ) -> tuple[pd.Series, pd.Series, pd.Series, pd.Series, dict[str, Any]]:
        epoch = self.catalog.get_evidence_epoch()
        if (
            epoch is None
            or epoch.first_forward_session is None
            or epoch.evidence_window_hash is None
        ):
            raise ChampionControlError(
                "Challenger evidence requires an activated architecture evidence epoch"
            )
        formal_window = None
        if self.shadow_authority is not None:
            try:
                formal_window = self.shadow_authority.aligned_forward_window(
                    champion_account_id=shadow_champion_account_id,
                    challenger_account_id=shadow_challenger_account_id,
                    minimum_sessions=max(60, int(policy.min_shadow_sessions)),
                    through=through,
                )
            except ShadowAuthorityError as exc:
                raise ChampionControlError(
                    f"formal Challenger shadow evidence was rejected: {exc}"
                ) from exc
            if (
                formal_window.epoch_id != epoch.epoch_id
                or formal_window.epoch_hash != epoch.epoch_hash
                or formal_window.evidence_window_hash
                != epoch.evidence_window_hash
                or formal_window.first_forward_session
                != epoch.first_forward_session
            ):
                raise ChampionControlError(
                    "formal shadow authority differs from the catalog evidence epoch"
                )
            challenger_binding = formal_window.challenger_binding
            if not (
                challenger_binding.active
                and challenger_binding.account_id == shadow_challenger_account_id
                and challenger_binding.experiment_id
                == historical_challenger_experiment_id
                and challenger_binding.role_key
                == historical_challenger_experiment_id
                and challenger_binding.sleeve_id is not None
            ):
                raise ChampionControlError(
                    "historical Challenger and forward shadow binding are not one authority"
                )
            champion_binding = formal_window.champion_binding
            if not (
                champion_binding.active
                and champion_binding.account_id == shadow_champion_account_id
                and champion_binding.experiment_id is not None
                and champion_binding.sleeve_id is not None
            ):
                raise ChampionControlError(
                    "static Champion forward role lacks its immutable historical Sleeve definition"
                )
            if not formal_window.data_quality_ok:
                raise ChampionControlError(
                    "open data incidents freeze Challenger forward evidence"
                )
            shadow_challenger = formal_window.challenger_series()
            shadow_champion = formal_window.champion_series()
            challenger_shadow = {
                "account_id": shadow_challenger_account_id,
                "role_binding_id": formal_window.challenger_binding.binding_id,
                "role_binding_hash": formal_window.challenger_binding.binding_hash,
                "projection_count": formal_window.observed_sessions,
                "first_session": formal_window.sessions[0].isoformat(),
                "last_session": formal_window.sessions[-1].isoformat(),
                "session_hashes": list(formal_window.challenger_session_hashes),
                "forward_evidence_hash": formal_window.evidence_hash,
            }
            champion_shadow = {
                "account_id": shadow_champion_account_id,
                "role_binding_id": formal_window.champion_binding.binding_id,
                "role_binding_hash": formal_window.champion_binding.binding_hash,
                "projection_count": formal_window.observed_sessions,
                "first_session": formal_window.sessions[0].isoformat(),
                "last_session": formal_window.sessions[-1].isoformat(),
                "session_hashes": list(formal_window.champion_session_hashes),
                "forward_evidence_hash": formal_window.evidence_hash,
            }
            historical_challenger, challenger_history = self._historical_result_returns(
                historical_challenger_experiment_id,
                promoted_binding=challenger_binding,
            )
            historical_champion, champion_history = self._historical_result_returns(
                str(champion_binding.experiment_id),
                promoted_binding=champion_binding,
            )
        else:
            # There is no immutable role/roster identity from which to derive a
            # historical Champion in the legacy event-only path.  Continuing
            # here would re-introduce the caller-selected completed-result
            # splice this control exists to prevent.
            raise ChampionControlError(
                "Challenger approval requires formal Champion role/roster authority"
            )
        if shadow_challenger_account_id == shadow_champion_account_id:
            raise ChampionControlError("Challenger and Champion need distinct shadow accounts")
        if not shadow_challenger.index.equals(shadow_champion.index):
            raise ChampionControlError(
                "Challenger and Champion shadow NAV sessions are not exactly aligned"
            )
        session_count = len(shadow_challenger)
        if session_count < 60:
            raise ChampionControlError(
                f"Challenger approval requires 60 common new sessions; found {session_count}"
            )
        session_pairs = [
            {
                "session": stamp.date().isoformat(),
                "challenger_return": float(shadow_challenger.loc[stamp]),
                "champion_return": float(shadow_champion.loc[stamp]),
            }
            for stamp in shadow_challenger.index
        ]
        authority = {
            "schema_version": "research-os/challenge-authority/v1",
            "epoch": {
                "epoch_id": epoch.epoch_id,
                "epoch_hash": epoch.epoch_hash,
                "evidence_window_hash": epoch.evidence_window_hash,
                "first_forward_session": epoch.first_forward_session.isoformat(),
                "calendar_snapshot_id": epoch.calendar_snapshot_id,
                "calendar_snapshot_hash": epoch.calendar_snapshot_hash,
                "activated_at": (
                    None if epoch.activated_at is None else epoch.activated_at.isoformat()
                ),
            },
            "historical_challenger": challenger_history,
            "historical_champion": champion_history,
            "shadow_challenger": challenger_shadow,
            "shadow_champion": champion_shadow,
            "session_range": {
                "first": shadow_challenger.index[0].date().isoformat(),
                "last": shadow_challenger.index[-1].date().isoformat(),
                "count": session_count,
                "aligned_returns_hash": content_fingerprint(
                    session_pairs,
                    domain="factor-lab/research-os/v1/aligned-shadow-returns",
                ),
            },
            "policy": self._policy_payload(policy),
        }
        if formal_window is not None:
            authority["formal_shadow_window"] = formal_window.authority_metadata()
        authority_hash = content_fingerprint(
            authority, domain=_CHALLENGE_AUTHORITY_DOMAIN
        )
        authority["authority_hash"] = authority_hash
        return (
            historical_challenger,
            historical_champion,
            shadow_challenger,
            shadow_champion,
            authority,
        )

    def evaluate_authoritative_challenger(
        self,
        *,
        historical_challenger_experiment_id: str,
        shadow_challenger_account_id: str,
        shadow_champion_account_id: str,
        policy: ChampionChallengePolicy | None = None,
        through: date | None = None,
    ) -> tuple[ChampionChallengeDecision, dict[str, Any]]:
        cfg = policy or ChampionChallengePolicy()
        (
            historical_challenger,
            historical_champion,
            shadow_challenger,
            shadow_champion,
            authority,
        ) = self._authoritative_challenge_inputs(
            historical_challenger_experiment_id=historical_challenger_experiment_id,
            shadow_challenger_account_id=shadow_challenger_account_id,
            shadow_champion_account_id=shadow_champion_account_id,
            policy=cfg,
            through=through,
        )
        decision = evaluate_challenger(
            historical_challenger,
            historical_champion,
            shadow_challenger_returns=shadow_challenger,
            shadow_champion_returns=shadow_champion,
            policy=cfg,
        )
        return decision, authority

    def _verify_challenge_authority(
        self, authority: Mapping[str, Any]
    ) -> ChampionChallengeDecision:
        raw = dict(authority)
        claimed_hash = str(raw.pop("authority_hash", ""))
        if content_fingerprint(raw, domain=_CHALLENGE_AUTHORITY_DOMAIN) != claimed_hash:
            raise ChampionControlError("Challenger authority fingerprint is corrupt")
        try:
            policy = ChampionChallengePolicy(**dict(raw["policy"]))
            decision, current = self.evaluate_authoritative_challenger(
                historical_challenger_experiment_id=str(
                    raw["historical_challenger"]["experiment_id"]
                ),
                shadow_challenger_account_id=str(raw["shadow_challenger"]["account_id"]),
                shadow_champion_account_id=str(raw["shadow_champion"]["account_id"]),
                policy=policy,
                through=date.fromisoformat(str(raw["session_range"]["last"])),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ChampionControlError("Challenger authority payload is malformed") from exc
        if canonical_json(current) != canonical_json(dict(authority)):
            raise ChampionControlError(
                "Challenger authority sources changed after approval"
            )
        return decision

    def _approved_adaptive_scores(
        self,
        scores: Mapping[str, float],
        *,
        approval_run_id: str | None,
    ) -> None:
        if not scores:
            return
        if not approval_run_id:
            raise ChampionControlError(
                "state-adaptive weights require a persisted Challenger approval"
            )
        run = self.catalog.get_run(str(approval_run_id))
        if (
            run is None
            or run.status != "completed"
            or run.run_type != ADAPTIVE_APPROVAL_RUN_TYPE
        ):
            raise ChampionControlError("adaptive approval run is absent or not authoritative")
        approval = run.metadata.get("approval")
        if not isinstance(approval, Mapping):
            raise ChampionControlError("adaptive approval payload is malformed")
        expected_hash = content_fingerprint(
            {key: value for key, value in approval.items() if key != "approval_hash"},
            domain=_ADAPTIVE_APPROVAL_DOMAIN,
        )
        if (
            str(approval.get("approval_hash") or "") != expected_hash
            or run.input_fingerprint != expected_hash
        ):
            raise ChampionControlError("adaptive approval fingerprint is corrupt")
        checks = approval.get("checks")
        metrics = approval.get("metrics")
        authority = approval.get("authority")
        if not isinstance(authority, Mapping):
            raise ChampionControlError(
                "adaptive approval has no authoritative experiment/shadow provenance"
            )
        current_decision = self._verify_challenge_authority(authority)
        expected_scores_hash = content_fingerprint(
            dict(sorted((str(key), float(value)) for key, value in scores.items())),
            domain="factor-lab/research-os/v1/adaptive-score-proposal",
        )
        if (
            approval.get("decision") != "challenger_research_recovered"
            or approval.get("fallback") != "challenger"
            or not isinstance(checks, Mapping)
            or not _REQUIRED_CHALLENGER_CHECKS.issubset(checks)
            or not all(bool(checks[key]) for key in _REQUIRED_CHALLENGER_CHECKS)
            or not isinstance(metrics, Mapping)
            or int(metrics.get("shadow_sessions") or 0) < 60
            or approval.get("adaptive_scores_hash") != expected_scores_hash
            or canonical_json(current_decision.to_dict())
            != canonical_json(
                {
                    "decision": approval.get("decision"),
                    "checks": checks,
                    "metrics": metrics,
                    "fallback": approval.get("fallback"),
                }
            )
        ):
            raise ChampionControlError(
                "adaptive overlay lacks stitched-OOS and 60-session shadow approval"
            )

    def persist_adaptive_approval(
        self,
        decision: Mapping[str, Any],
        adaptive_scores: Mapping[str, float],
        *,
        authority: Mapping[str, Any],
        generated_at: datetime,
        source_partition: str,
    ) -> RunRecord:
        """Persist the deterministic gate that alone can authorize an overlay."""

        generated_at = _aware(generated_at, name="generated_at")
        authoritative_decision = self._verify_challenge_authority(authority)
        if canonical_json(authoritative_decision.to_dict()) != canonical_json(
            dict(decision)
        ):
            raise ChampionControlError(
                "caller Challenger decision differs from authoritative catalog evidence"
            )
        checks = dict(authoritative_decision.checks)
        metrics = dict(authoritative_decision.metrics)
        try:
            last_shadow_session = date.fromisoformat(
                str(authority["session_range"]["last"])
            )
            source_times = (
                datetime.fromisoformat(
                    str(authority["historical_challenger"]["result_completed_at"])
                    .replace("Z", "+00:00")
                ),
                datetime.fromisoformat(
                    str(authority["historical_champion"]["result_completed_at"])
                    .replace("Z", "+00:00")
                ),
                datetime.fromisoformat(
                    str(authority["epoch"]["activated_at"]).replace("Z", "+00:00")
                ),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ChampionControlError("Challenger authority session range is malformed") from exc
        if generated_at.date() < last_shadow_session:
            raise ChampionControlError(
                "adaptive approval timestamp predates its shadow evidence"
            )
        if any(_aware(value, name="authority timestamp") > generated_at for value in source_times):
            raise ChampionControlError(
                "adaptive approval timestamp predates its catalog authority"
            )
        if (
            authoritative_decision.decision != "challenger_research_recovered"
            or authoritative_decision.fallback != "challenger"
            or not _REQUIRED_CHALLENGER_CHECKS.issubset(checks)
            or not all(bool(checks[key]) for key in _REQUIRED_CHALLENGER_CHECKS)
            or int(metrics.get("shadow_sessions") or 0) < 60
        ):
            raise ChampionControlError(
                "only a fully passed Challenger comparison can approve adaptive weights"
            )
        score_payload = dict(
            sorted((str(key), float(value)) for key, value in adaptive_scores.items())
        )
        if not score_payload or any(not isfinite(value) for value in score_payload.values()):
            raise ChampionControlError("adaptive approval requires finite proposed scores")
        approval = {
            "schema_version": "research-os/champion-adaptive-approval/v1",
            "generated_at": generated_at.isoformat(),
            "source_partition": str(source_partition),
            "decision": authoritative_decision.decision,
            "fallback": authoritative_decision.fallback,
            "checks": checks,
            "metrics": metrics,
            "authority": dict(authority),
            "adaptive_scores_hash": content_fingerprint(
                score_payload,
                domain="factor-lab/research-os/v1/adaptive-score-proposal",
            ),
        }
        approval_hash = content_fingerprint(
            approval, domain=_ADAPTIVE_APPROVAL_DOMAIN
        )
        approval["approval_hash"] = approval_hash
        run = RunRecord(
            run_id=f"adapt_{approval_hash[:32]}",
            run_type=ADAPTIVE_APPROVAL_RUN_TYPE,
            status="completed",
            input_fingerprint=approval_hash,
            started_at=generated_at,
            completed_at=generated_at,
            metadata={"approval": approval},
        )
        existing = self.catalog.get_run(run.run_id)
        if existing is not None:
            if (
                existing.status != run.status
                or existing.run_type != run.run_type
                or existing.input_fingerprint != run.input_fingerprint
                or canonical_json(existing.metadata) != canonical_json(run.metadata)
            ):
                raise ChampionControlError("adaptive approval run identity collision")
            return existing
        stored, created = self.catalog.claim_run(run)
        if not created:
            raise ChampionControlError("adaptive approval was concurrently changed")
        return stored

    def build_allocation(
        self,
        *,
        data_snapshot_id: str,
        generated_at: datetime,
        adaptive_scores: Mapping[str, float] | None = None,
        previous: ChampionAllocationProjection | None = None,
        adaptive_fraction: float = 0.25,
        max_monthly_change: float = 0.05,
        operational_data_failure: bool = False,
        adaptive_approval_run_id: str | None = None,
    ) -> ChampionAllocationProjection:
        generated_at = _aware(generated_at, name="generated_at")
        snapshot_record = self.catalog.get_snapshot(data_snapshot_id)
        if snapshot_record is None:
            raise ChampionProjectionUnavailable(
                f"data snapshot {data_snapshot_id!r} is not registered"
            )
        snapshot = snapshot_record.reference
        data_quality_ok = (
            not operational_data_failure
            and snapshot.tier is SnapshotTier.GOLD
            and snapshot.quality_status is DataQualityStatus.ACCEPTED
            and not (_BLOCKING_TRUST_LABELS & set(snapshot.trust_labels))
        )
        candidates = self.authoritative_sleeves()
        self._approved_adaptive_scores(
            adaptive_scores or {}, approval_run_id=adaptive_approval_run_id
        )
        static = _static_cluster_weights(candidates)
        model, overlay = _model_overlay(
            static,
            candidates,
            adaptive_scores or {},
            previous=previous,
            adaptive_fraction=adaptive_fraction,
            max_monthly_change=max_monthly_change,
        )
        states = {
            item.sleeve_id: LifecycleState(item.lifecycle_state)
            for item in candidates
        }
        previous_effective = (
            None
            if previous is None
            else dict(previous.effective_allocation.get("sleeve_weights") or {})
        )
        effective = _effective_health_allocation(
            model,
            states,
            data_quality_ok=data_quality_ok,
            previous_effective=previous_effective,
        )
        content = {
            "schema_version": ALLOCATION_SCHEMA_VERSION,
            "generated_at": generated_at.isoformat(),
            "data_snapshot_id": snapshot.snapshot_id,
            "data_snapshot_hash": snapshot.content_hash,
            "data_quality_ok": data_quality_ok,
            "candidates": [item.to_dict() for item in candidates],
            "static_allocation": static,
            "state_overlay": overlay,
            "model_allocation": model,
            "effective_allocation": effective,
            "previous_projection_id": (
                None if previous is None else previous.projection_id
            ),
        }
        projection_id = content_fingerprint(content, domain=_ALLOCATION_DOMAIN)
        return ChampionAllocationProjection(
            projection_id=projection_id,
            generated_at=generated_at,
            data_snapshot_id=snapshot.snapshot_id,
            data_snapshot_hash=snapshot.content_hash,
            data_quality_ok=data_quality_ok,
            candidates=candidates,
            static_allocation=static,
            state_overlay=overlay,
            model_allocation=model,
            effective_allocation=effective,
            previous_projection_id=None if previous is None else previous.projection_id,
        )

    def persist_allocation(
        self, projection: ChampionAllocationProjection
    ) -> RunRecord:
        metadata = {"projection": projection.to_dict()}
        run = RunRecord(
            run_id=f"champ_{projection.projection_id[:32]}",
            run_type=ALLOCATION_RUN_TYPE,
            status="completed",
            input_fingerprint=projection.projection_id,
            metadata=metadata,
            started_at=projection.generated_at,
            completed_at=projection.generated_at,
        )
        existing = self.catalog.get_run(run.run_id)
        if existing is not None:
            if (
                existing.status != "completed"
                or existing.input_fingerprint != projection.projection_id
                or canonical_json(existing.metadata) != canonical_json(metadata)
            ):
                raise ChampionControlError("Champion projection run identity collision")
            return existing
        stored, created = self.catalog.claim_run(run)
        if not created:
            raise ChampionControlError("Champion projection was concurrently changed")
        return stored

    def latest_allocation(
        self, *, through: datetime | None = None
    ) -> ChampionAllocationProjection | None:
        cutoff = None if through is None else _aware(through, name="through")
        rows = self.catalog.list_runs(
            limit=1_000, status="completed", run_type=ALLOCATION_RUN_TYPE
        )
        if len(rows) >= 1_000:
            raise ChampionControlError("Champion projection listing reached safety limit")
        for row in rows:
            raw = row.metadata.get("projection")
            if not isinstance(raw, Mapping):
                raise ChampionControlError("Champion run has no typed projection")
            projection = ChampionAllocationProjection.from_dict(raw)
            if row.input_fingerprint != projection.projection_id:
                raise ChampionControlError("Champion run fingerprint differs from projection")
            if cutoff is None or projection.generated_at <= cutoff:
                return projection
        return None

    def _assert_persisted_allocation(
        self, projection: ChampionAllocationProjection
    ) -> None:
        run = self.catalog.get_run(f"champ_{projection.projection_id[:32]}")
        if run is None or run.status != "completed":
            raise ChampionProjectionUnavailable(
                "stock targets require a persisted authoritative Champion projection"
            )
        raw = run.metadata.get("projection")
        if not isinstance(raw, Mapping):
            raise ChampionControlError("persisted Champion projection is malformed")
        persisted = ChampionAllocationProjection.from_dict(raw)
        if persisted.to_dict() != projection.to_dict():
            raise ChampionControlError("in-memory Champion differs from catalog truth")

    def _current_evidence(
        self, projection: ChampionAllocationProjection
    ) -> tuple[dict[str, Any], dict[str, LifecycleState]]:
        experiments: dict[str, Any] = {}
        states: dict[str, LifecycleState] = {}
        for evidence in projection.candidates:
            experiment = self.catalog.get_experiment(evidence.experiment_id)
            result = self.catalog.get_authoritative_result(evidence.experiment_id)
            if (
                experiment is None
                or result is None
                or experiment.spec.sleeve is None
                or experiment.fingerprint != evidence.experiment_fingerprint
                or result.result_id != evidence.result_id
                or result.result_hash != evidence.result_hash
                or result.outcome != "promoted_to_shadow"
            ):
                raise ChampionControlError(
                    f"authoritative evidence changed for {evidence.sleeve_id!r}"
                )
            state = self.catalog.latest_lifecycle_state(evidence.sleeve_id)
            if state is None:
                raise ChampionControlError("current lifecycle evidence is missing")
            experiments[evidence.sleeve_id] = experiment
            states[evidence.sleeve_id] = state
        return experiments, states

    def build_stock_target(
        self,
        projection: ChampionAllocationProjection,
        *,
        gold_snapshot_id: str,
        gold_frame: pd.DataFrame,
        decision_date: date | str,
        generated_at: datetime,
        force_data_failure: bool = False,
        supersedes: ChampionStockTarget | None = None,
    ) -> ChampionStockTarget:
        """Evaluate registered Sleeve DSLs and aggregate a current desired target.

        The result is a desired end-state for the event-driven shadow engine,
        not an order list.  Tradeability, locked sells and actual costs remain
        the execution engine's responsibility.
        """

        self._assert_persisted_allocation(projection)
        generated_at = _aware(generated_at, name="generated_at")
        if projection.generated_at > generated_at:
            raise ChampionStockTargetUnavailable(
                "stock target cannot predate its Champion allocation projection"
            )
        decision = (
            decision_date
            if isinstance(decision_date, date) and not isinstance(decision_date, datetime)
            else pd.Timestamp(decision_date).date()
        )
        if supersedes is not None:
            if supersedes.decision_date != decision:
                raise ChampionStockTargetUnavailable(
                    "a stock target can supersede only the same decision date"
                )
            if supersedes.generated_at >= generated_at:
                raise ChampionStockTargetUnavailable(
                    "a superseding stock target must be generated later"
                )
        snapshot_record = self.catalog.get_snapshot(gold_snapshot_id)
        if snapshot_record is None:
            raise ChampionStockTargetUnavailable("Gold decision snapshot is unregistered")
        snapshot = snapshot_record.reference
        calendar = snapshot.manifest.get("trading_calendar")
        if isinstance(calendar, Mapping) and isinstance(
            calendar.get("sessions"), (list, tuple)
        ):
            try:
                sessions = tuple(
                    date.fromisoformat(str(item)) for item in calendar["sessions"]
                )
                decision_index = sessions.index(decision)
                next_session = sessions[decision_index + 1]
            except (ValueError, IndexError) as exc:
                raise ChampionStockTargetUnavailable(
                    "Gold calendar cannot prove the decision's next session"
                ) from exc
            # A-share close/open boundaries are expressed explicitly instead of
            # relying on the host timezone.
            from zoneinfo import ZoneInfo

            shanghai = ZoneInfo("Asia/Shanghai")
            minimum_cutoff = datetime.combine(
                decision, datetime.min.time().replace(hour=15), tzinfo=shanghai
            ).astimezone(timezone.utc)
            next_open = datetime.combine(
                next_session,
                datetime.min.time().replace(hour=9, minute=30),
                tzinfo=shanghai,
            ).astimezone(timezone.utc)
            if generated_at < minimum_cutoff or generated_at >= next_open:
                raise ChampionStockTargetUnavailable(
                    "target generated_at must be after decision close and before next open"
                )
        if snapshot.as_of.astimezone(timezone.utc) > generated_at.astimezone(timezone.utc):
            raise ChampionStockTargetUnavailable(
                "target cannot predate its Gold snapshot publication"
            )
        experiments, states = self._current_evidence(projection)
        quality_ok = (
            not force_data_failure
            and snapshot.tier is SnapshotTier.GOLD
            and snapshot.quality_status is DataQualityStatus.ACCEPTED
            and not (_BLOCKING_TRUST_LABELS & set(snapshot.trust_labels))
        )
        previous_effective = dict(
            projection.effective_allocation.get("sleeve_weights") or {}
        )
        effective = _effective_health_allocation(
            projection.model_allocation,
            states,
            data_quality_ok=quality_ok,
            previous_effective=previous_effective,
        )
        if float(effective["cash_weight"]) >= 1.0 - 1e-12:
            return self._new_stock_target(
                projection=projection,
                snapshot=snapshot,
                decision_date=decision,
                generated_at=generated_at,
                states=states,
                effective=effective,
                target_weights={},
                component_audit={
                    "status": "all_cash",
                    "reason": (
                        "current_gold_data_failure"
                        if force_data_failure
                        else effective["reason"]
                    ),
                    "source": "latest_lifecycle_and_cataloged_snapshot",
                },
                cash_weight=1.0,
                supersedes_target_id=(
                    None if supersedes is None else supersedes.target_id
                ),
            )

        if not quality_ok:
            raise ChampionStockTargetUnavailable("unaccepted Gold cannot fund risk")
        binding = verify_snapshot_frame_binding(snapshot, gold_frame)
        frame = self._normalise_gold_frame(gold_frame, decision)
        current = frame.loc[frame["date"].eq(pd.Timestamp(decision))].copy()
        members = current.loc[current["universe_member"].astype(bool)].copy()
        if len(members) != 500 or members["ticker"].nunique() != 500:
            raise ChampionStockTargetUnavailable(
                "current PIT equal-weight universe does not contain exactly 500 securities"
            )
        benchmark = pd.Series(
            pd.to_numeric(members["benchmark_weight"], errors="coerce").to_numpy(),
            index=members["ticker"].astype(str),
            dtype=float,
        )
        if benchmark.isna().any() or abs(float(benchmark.sum()) - 1.0) > 1e-8:
            raise ChampionStockTargetUnavailable("current benchmark weights are invalid")

        component_targets: dict[str, dict[str, float]] = {}
        component_audit: dict[str, Any] = {
            "status": "ok",
            "snapshot_binding": asdict(binding),
            "source": "authoritative_result_plus_latest_gold_dsl",
            "sleeves": {},
        }
        for sleeve_id, allocation_weight in dict(
            effective.get("sleeve_weights") or {}
        ).items():
            experiment = experiments.get(sleeve_id)
            if experiment is None:
                raise ChampionStockTargetUnavailable(
                    f"effective Sleeve {sleeve_id!r} lacks authoritative evidence"
                )
            weights, audit = self._sleeve_stock_weights(
                experiment.spec, frame, members, benchmark, decision
            )
            component_targets[sleeve_id] = weights
            component_audit["sleeves"][sleeve_id] = {
                "allocation_weight": float(allocation_weight),
                **audit,
            }

        aggregate: dict[str, float] = {}
        for sleeve_id, weights in component_targets.items():
            sleeve_weight = float(effective["sleeve_weights"][sleeve_id])
            for ticker, weight in weights.items():
                aggregate[ticker] = aggregate.get(ticker, 0.0) + sleeve_weight * weight
        benchmark_allocation = float(effective["benchmark_weight"])
        for ticker, weight in benchmark.items():
            aggregate[str(ticker)] = aggregate.get(str(ticker), 0.0) + benchmark_allocation * float(weight)

        capital = 50_000_000.0
        maximum_stock_weight = min(
            [
                float(experiments[item.sleeve_id].spec.portfolio.maximum_stock_weight)
                for item in projection.candidates
            ]
            or [0.02]
        )
        maximum_adv = min(
            [
                float(
                    experiments[item.sleeve_id].spec.portfolio.maximum_adv_participation
                )
                for item in projection.candidates
            ]
            or [0.05]
        )
        adv = pd.Series(
            pd.to_numeric(members["adv_20"], errors="coerce").to_numpy(),
            index=members["ticker"].astype(str),
            dtype=float,
        )
        if adv.isna().any() or (adv <= 0).any():
            raise ChampionStockTargetUnavailable("current ADV evidence is incomplete")
        clipped: dict[str, float] = {}
        capacity_clipped: dict[str, float] = {}
        for ticker, raw_weight in aggregate.items():
            cap = min(
                maximum_stock_weight,
                float(adv.get(ticker, 0.0)) * maximum_adv / capital,
            )
            weight = min(max(float(raw_weight), 0.0), max(cap, 0.0))
            if weight + 1e-12 < raw_weight:
                capacity_clipped[ticker] = raw_weight - weight
            if weight > 1e-12:
                clipped[ticker] = weight
        expected_invested = 1.0 - float(effective["cash_weight"])
        invested = sum(clipped.values())
        cash_weight = min(1.0, max(0.0, 1.0 - invested))
        if invested > expected_invested + 1e-8:
            raise ChampionStockTargetUnavailable("aggregated stock target exceeds allocation")
        component_audit.update(
            {
                "benchmark_allocation_weight": benchmark_allocation,
                "capacity_clipped_count": len(capacity_clipped),
                "capacity_unallocated_weight": sum(capacity_clipped.values()),
                "expected_invested_weight": expected_invested,
                "actual_invested_weight": invested,
                "maximum_stock_weight": maximum_stock_weight,
                "maximum_adv_participation": maximum_adv,
            }
        )
        return self._new_stock_target(
            projection=projection,
            snapshot=snapshot,
            decision_date=decision,
            generated_at=generated_at,
            states=states,
            effective=effective,
            target_weights=clipped,
            component_audit=component_audit,
            cash_weight=cash_weight,
            supersedes_target_id=(
                None if supersedes is None else supersedes.target_id
            ),
        )

    @staticmethod
    def _normalise_gold_frame(frame: pd.DataFrame, decision: date) -> pd.DataFrame:
        result = frame.copy()
        # Research Gold deliberately contains forward labels for historical
        # evaluation.  The live target path physically projects them away
        # before any Sleeve DSL or portfolio code receives the frame.  Field
        # registry validation below remains a second, independent guard.
        label_columns = [
            str(column)
            for column in result.columns
            if is_forward_derived_field(column, strict_target_segments=True)
        ]
        if label_columns:
            result = result.drop(columns=label_columns)
        if "ticker" not in result and "ts_code" in result:
            result["ticker"] = result["ts_code"].astype(str)
        if "date" not in result and "trade_date" in result:
            result["date"] = result["trade_date"]
        if "decision_time" not in result and "decision_cutoff" in result:
            result["decision_time"] = result["decision_cutoff"]
        required = {
            "ticker",
            "date",
            "universe_member",
            "benchmark_weight",
            "adv_20",
            "close_adj",
            "industry",
            "industry_available_at",
            "log_market_cap",
            "size_available_at",
        }
        missing = sorted(required - set(result.columns))
        if missing:
            raise ChampionStockTargetUnavailable(
                f"Gold stock-target fields are missing: {missing}"
            )
        result["ticker"] = result["ticker"].astype(str)
        result["date"] = pd.to_datetime(result["date"], errors="raise").dt.tz_localize(None).dt.normalize()
        if result.duplicated(["ticker", "date"]).any():
            raise ChampionStockTargetUnavailable("Gold target frame has duplicate ticker/date rows")
        latest = result["date"].max()
        if pd.isna(latest) or latest.date() != decision:
            raise ChampionStockTargetUnavailable(
                "decision_date must be the latest complete Gold session"
            )
        return result.sort_values(["ticker", "date"]).reset_index(drop=True)

    @staticmethod
    def _field_specs(spec: Any) -> tuple[FieldSpec, ...]:
        sleeve = spec.sleeve
        assert sleeve is not None
        if sleeve.signal_expression is None or not sleeve.signal_field_registry:
            raise ChampionStockTargetUnavailable(
                f"Sleeve {sleeve.sleeve_id!r} has no registered DSL/field registry"
            )
        fields: list[FieldSpec] = []
        for item in sleeve.signal_field_registry:
            if item.role == "label" or "forward" in item.name.lower() or "next_return" in item.name.lower():
                raise ChampionStockTargetUnavailable("live Sleeve DSL cannot use label fields")
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
                raise ChampionStockTargetUnavailable(
                    f"unsupported live signal field contract for {item.name!r}"
                ) from exc
        return tuple(fields)

    def _sleeve_stock_weights(
        self,
        spec: Any,
        frame: pd.DataFrame,
        members: pd.DataFrame,
        benchmark: pd.Series,
        decision: date,
    ) -> tuple[dict[str, float], dict[str, Any]]:
        sleeve = spec.sleeve
        assert sleeve is not None
        signal = evaluate_factor_graph(
            sleeve.signal_expression,
            frame,
            self._field_specs(spec),
            context=EvaluationContext(decision_point=DecisionPoint.AFTER_CLOSE),
        )
        current_mask = frame["date"].eq(pd.Timestamp(decision)) & frame[
            "universe_member"
        ].astype(bool)
        scores = pd.Series(
            pd.to_numeric(signal.loc[current_mask], errors="coerce").to_numpy(),
            index=frame.loc[current_mask, "ticker"].astype(str),
            dtype=float,
        )
        if scores.notna().sum() < spec.portfolio.minimum_position_count:
            raise ChampionStockTargetUnavailable(
                f"Sleeve {sleeve.sleeve_id!r} has insufficient current signal coverage"
            )
        history = frame.loc[frame["date"] <= pd.Timestamp(decision), ["date", "ticker", "close_adj"]].copy()
        history["return"] = history.groupby("ticker", sort=False)["close_adj"].pct_change(fill_method=None)
        returns = history.pivot(index="date", columns="ticker", values="return").sort_index().tail(252)
        market_return = returns.reindex(columns=benchmark.index).mean(axis=1)
        market_variance = float(market_return.var(ddof=1))
        if len(returns) < 60 or not isfinite(market_variance) or market_variance <= 0:
            raise ChampionStockTargetUnavailable("Gold has insufficient history for PIT beta")
        beta = returns.apply(lambda values: values.cov(market_return) / market_variance)
        current = members.set_index("ticker", drop=False).copy()
        size = pd.to_numeric(current["log_market_cap"], errors="coerce")
        if size.isna().any():
            raise ChampionStockTargetUnavailable("PIT size exposure is incomplete")
        size_bucket = pd.qcut(
            size.rank(method="first"),
            q=5,
            labels=("micro", "small", "mid", "large", "mega"),
        ).astype(str)
        industry_time = pd.to_datetime(
            current["industry_available_at"], errors="coerce", utc=True
        )
        size_time = pd.to_datetime(
            current["size_available_at"], errors="coerce", utc=True
        )
        if "decision_time" not in current:
            raise ChampionStockTargetUnavailable("Gold has no PIT decision timestamp")
        cutoff = pd.to_datetime(current["decision_time"], errors="coerce", utc=True)
        industry_is_pit = (
            industry_time.notna()
            & size_time.notna()
            & cutoff.notna()
            & industry_time.le(cutoff)
            & size_time.le(cutoff)
        )
        metadata = pd.DataFrame(
            {
                "industry": current["industry"].astype("string"),
                "size_bucket": size_bucket,
                "beta": beta.reindex(current.index),
                "adv_20": pd.to_numeric(current["adv_20"], errors="coerce"),
                "industry_is_pit": industry_is_pit,
            },
            index=current.index,
        )
        policy = StockOptimizationPolicy(
            min_positions=spec.portfolio.minimum_position_count,
            max_positions=spec.portfolio.maximum_position_count,
            max_position_weight=spec.portfolio.maximum_stock_weight,
            industry_deviation=spec.portfolio.industry_active_weight_limit,
            size_deviation=spec.portfolio.size_active_weight_limit,
            beta_min=spec.portfolio.minimum_beta,
            beta_max=spec.portfolio.maximum_beta,
            max_adv_participation=spec.portfolio.maximum_adv_participation,
            capital=spec.portfolio.capital,
            minimum_return_observations=60,
        )
        optimized = optimize_stock_weights(
            scores,
            returns,
            metadata,
            benchmark,
            policy=policy,
        )
        if not optimized.promotion_eligible or optimized.status != "ok":
            raise ChampionStockTargetUnavailable(
                f"Sleeve {sleeve.sleeve_id!r} current target violates constraints: "
                f"{optimized.status}"
            )
        return optimized.weights, {
            "experiment_fingerprint": spec.fingerprint(),
            "signal_expression_hash": content_fingerprint(
                sleeve.signal_expression,
                domain="factor-lab/research-os/v1/live-sleeve-dsl",
            ),
            "optimizer": optimized.to_dict(),
        }

    @staticmethod
    def _new_stock_target(
        *,
        projection: ChampionAllocationProjection,
        snapshot: DataSnapshotRef,
        decision_date: date,
        generated_at: datetime,
        states: Mapping[str, LifecycleState],
        effective: Mapping[str, Any],
        target_weights: Mapping[str, float],
        component_audit: Mapping[str, Any],
        cash_weight: float,
        supersedes_target_id: str | None,
    ) -> ChampionStockTarget:
        content = {
            "schema_version": STOCK_TARGET_SCHEMA_VERSION,
            "generated_at": generated_at.isoformat(),
            "decision_date": decision_date.isoformat(),
            "allocation_projection_id": projection.projection_id,
            "gold_snapshot_id": snapshot.snapshot_id,
            "gold_snapshot_hash": snapshot.content_hash,
            "lifecycle_states": {
                key: value.value for key, value in sorted(states.items())
            },
            "effective_allocation": dict(effective),
            "target_weights": _float_map(target_weights),
            "component_audit": dict(component_audit),
            "cash_weight": float(cash_weight),
            "supersedes_target_id": supersedes_target_id,
        }
        target_id = content_fingerprint(content, domain=_STOCK_TARGET_DOMAIN)
        return ChampionStockTarget(
            target_id=target_id,
            generated_at=generated_at,
            decision_date=decision_date,
            allocation_projection_id=projection.projection_id,
            gold_snapshot_id=snapshot.snapshot_id,
            gold_snapshot_hash=snapshot.content_hash,
            lifecycle_states={
                key: value.value for key, value in sorted(states.items())
            },
            effective_allocation=dict(effective),
            target_weights=_float_map(target_weights),
            component_audit=dict(component_audit),
            cash_weight=float(cash_weight),
            supersedes_target_id=supersedes_target_id,
        )

    def persist_stock_target(self, target: ChampionStockTarget) -> RunRecord:
        metadata = {"stock_target": target.to_dict()}
        run = RunRecord(
            run_id=f"target_{target.target_id[:32]}",
            run_type=STOCK_TARGET_RUN_TYPE,
            status="completed",
            input_fingerprint=target.target_id,
            metadata=metadata,
            started_at=target.generated_at,
            completed_at=target.generated_at,
        )
        existing = self.catalog.get_run(run.run_id)
        if existing is not None:
            if (
                existing.status != "completed"
                or existing.input_fingerprint != target.target_id
                or canonical_json(existing.metadata) != canonical_json(metadata)
            ):
                raise ChampionControlError("Champion stock-target run identity collision")
            return existing
        stored, created = self.catalog.claim_run(run)
        if not created:
            raise ChampionControlError("Champion stock target was concurrently changed")
        return stored

    def latest_stock_target(
        self, *, decision_date: date | str | None = None
    ) -> ChampionStockTarget | None:
        expected = (
            None
            if decision_date is None
            else decision_date
            if isinstance(decision_date, date)
            else date.fromisoformat(str(decision_date))
        )
        rows = self.catalog.list_runs(
            limit=1_000, status="completed", run_type=STOCK_TARGET_RUN_TYPE
        )
        if len(rows) >= 1_000:
            raise ChampionControlError("Champion target listing reached safety limit")
        targets: list[ChampionStockTarget] = []
        for row in rows:
            raw = row.metadata.get("stock_target")
            if not isinstance(raw, Mapping):
                raise ChampionControlError("Champion target run is malformed")
            target = ChampionStockTarget.from_dict(raw)
            if row.input_fingerprint != target.target_id:
                raise ChampionControlError("Champion target run fingerprint differs")
            if expected is None or target.decision_date == expected:
                targets.append(target)
        if not targets:
            return None
        if expected is None:
            return max(targets, key=lambda item: (item.generated_at, item.target_id))
        ordered = sorted(targets, key=lambda item: (item.generated_at, item.target_id))
        if ordered[0].supersedes_target_id is not None:
            raise ChampionControlError("Champion target supersession chain is incomplete")
        for older, newer in zip(ordered, ordered[1:]):
            if newer.supersedes_target_id != older.target_id:
                raise ChampionControlError("Champion target supersession chain is ambiguous")
        return ordered[-1]

    def stock_target_for_trade_date(
        self, trade_date: date | str
    ) -> ChampionStockTarget | None:
        """Return the one target whose immutable calendar names ``trade_date`` next.

        Looking up by the target's creation time would permit a late or stale
        projection to trade.  The persisted exchange calendar on the target's
        decision snapshot is the authoritative mapping.
        """

        trade = (
            trade_date
            if isinstance(trade_date, date) and not isinstance(trade_date, datetime)
            else pd.Timestamp(trade_date).date()
        )
        rows = self.catalog.list_runs(
            limit=1_000, status="completed", run_type=STOCK_TARGET_RUN_TYPE
        )
        if len(rows) >= 1_000:
            raise ChampionControlError("Champion target listing reached safety limit")
        matches: list[ChampionStockTarget] = []
        for row in rows:
            raw = row.metadata.get("stock_target")
            if not isinstance(raw, Mapping):
                raise ChampionControlError("Champion target run is malformed")
            target = ChampionStockTarget.from_dict(raw)
            if row.input_fingerprint != target.target_id:
                raise ChampionControlError("Champion target run fingerprint differs")
            snapshot = self.catalog.get_snapshot(target.gold_snapshot_id)
            if snapshot is None:
                raise ChampionControlError("Champion target snapshot disappeared")
            calendar = snapshot.reference.manifest.get("trading_calendar")
            if not isinstance(calendar, Mapping):
                raise ChampionControlError("Champion target snapshot lacks trading calendar")
            try:
                sessions = tuple(date.fromisoformat(str(item)) for item in calendar["sessions"])
                index = sessions.index(target.decision_date)
            except (KeyError, TypeError, ValueError) as exc:
                raise ChampionControlError("Champion target calendar is invalid") from exc
            if index + 1 < len(sessions) and sessions[index + 1] == trade:
                matches.append(target)
        if len({item.decision_date for item in matches}) > 1:
            raise ChampionControlError(
                f"multiple authoritative Champion targets exist for {trade.isoformat()}"
            )
        if not matches:
            return None
        ordered = sorted(matches, key=lambda item: (item.generated_at, item.target_id))
        if ordered[0].supersedes_target_id is not None:
            raise ChampionControlError("Champion target supersession chain is incomplete")
        for older, newer in zip(ordered, ordered[1:]):
            if newer.supersedes_target_id != older.target_id:
                raise ChampionControlError("Champion target supersession chain is ambiguous")
        return ordered[-1]


__all__ = [
    "ADAPTIVE_APPROVAL_RUN_TYPE",
    "ALLOCATION_RUN_TYPE",
    "ALLOCATION_SCHEMA_VERSION",
    "STOCK_TARGET_RUN_TYPE",
    "STOCK_TARGET_SCHEMA_VERSION",
    "AuthoritativeChampionControl",
    "AuthoritativeSleeveEvidence",
    "ChampionAllocationProjection",
    "ChampionControlError",
    "ChampionProjectionUnavailable",
    "ChampionStockTarget",
    "ChampionStockTargetUnavailable",
]

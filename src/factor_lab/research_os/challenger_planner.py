"""Deterministic next-session targets for promoted Challenger Sleeves.

The planner deliberately has no proposal, statistics, or file-input seam.  It
discovers promoted experiments through durable Challenger role bindings, checks
their frozen roster entry and authoritative result, evaluates the registered
Sleeve DSL against an accepted Gold snapshot, and stores a content-addressed
weekly target in the run ledger.  On non-rebalance sessions it emits a mark-only
plan so the independent Challenger account continues to accumulate aligned
forward evidence.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from math import isfinite
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import pandas as pd

from .catalog import ResearchCatalog, RunRecord
from .champion_control import (
    AuthoritativeChampionControl,
    ChampionStockTargetUnavailable,
)
from .contracts import (
    DataQualityStatus,
    ExperimentStatus,
    LifecycleState,
    SnapshotTier,
)
from .fingerprint import canonical_json, content_fingerprint
from .shadow_authority import (
    ShadowEvidenceAuthority,
    ShadowRole,
    ShadowRoleBinding,
)
from .sleeve_lifecycle import DailyShadowPlan
from .sleeve_registry import SleeveRosterManifest
from .snapshots import verify_snapshot_frame_binding


CHALLENGER_TARGET_SCHEMA_VERSION = "research-os/challenger-stock-target/v1"
CHALLENGER_TARGET_RUN_TYPE = "challenger_stock_target"
_TARGET_DOMAIN = "factor-lab/research-os/v1/challenger-stock-target"
_TARGET_KEY_DOMAIN = "factor-lab/research-os/v1/challenger-target-key"
_BLOCKING_TRUST_LABELS = frozenset(
    {
        "st_history_unverified",
        "legacy_untrusted_data",
        "legacy_execution_regression_only",
        "disputed",
        "quarantined",
    }
)
_PROJECTABLE_STATES = frozenset(
    {
        LifecycleState.SHADOW,
        LifecycleState.ACTIVE,
        LifecycleState.REDUCED,
        LifecycleState.PROBATION,
    }
)


class ChallengerPlannerError(RuntimeError):
    """Raised when Challenger authority or current Gold cannot be proven."""


def _weights(values: Mapping[str, Any]) -> dict[str, float]:
    result = {str(key): float(value) for key, value in values.items()}
    if any(not key or not isfinite(value) or value < 0.0 for key, value in result.items()):
        raise ValueError("Challenger weights must be finite, non-negative ticker weights")
    return dict(sorted(result.items()))


def _aware(value: datetime, *, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must include a timezone")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True)
class ChallengerAuthority:
    binding: ShadowRoleBinding
    experiment: Any
    result: Any
    roster: SleeveRosterManifest
    lifecycle_state: LifecycleState

    @property
    def experiment_id(self) -> str:
        return str(self.experiment.experiment_id)

    @property
    def sleeve_id(self) -> str:
        sleeve = self.experiment.spec.sleeve
        assert sleeve is not None
        return sleeve.sleeve_id


@dataclass(frozen=True)
class ChallengerStockTarget:
    target_id: str
    generated_at: datetime
    decision_date: date
    experiment_id: str
    experiment_fingerprint: str
    result_hash: str
    sleeve_id: str
    account_id: str
    role_binding_id: str
    roster_id: str
    gold_snapshot_id: str
    gold_snapshot_hash: str
    target_weights: Mapping[str, float]
    cash_weight: float
    component_audit: Mapping[str, Any]
    schema_version: str = CHALLENGER_TARGET_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _aware(self.generated_at, name="generated_at")
        if self.schema_version != CHALLENGER_TARGET_SCHEMA_VERSION:
            raise ValueError("unsupported Challenger target schema")
        normalized = _weights(self.target_weights)
        if any(value > 0.02 + 1e-12 for value in normalized.values()):
            raise ValueError("Challenger stock target exceeds the 2% stock cap")
        if (
            not isfinite(float(self.cash_weight))
            or not 0.0 <= float(self.cash_weight) <= 1.0
            or abs(sum(normalized.values()) + float(self.cash_weight) - 1.0) > 1e-8
        ):
            raise ValueError("Challenger weights plus cash must sum to one")
        expected = content_fingerprint(self.content_payload(), domain=_TARGET_DOMAIN)
        if self.target_id != expected:
            raise ValueError("Challenger target identity differs from content")

    def content_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "generated_at": self.generated_at.isoformat(),
            "decision_date": self.decision_date.isoformat(),
            "experiment_id": self.experiment_id,
            "experiment_fingerprint": self.experiment_fingerprint,
            "result_hash": self.result_hash,
            "sleeve_id": self.sleeve_id,
            "account_id": self.account_id,
            "role_binding_id": self.role_binding_id,
            "roster_id": self.roster_id,
            "gold_snapshot_id": self.gold_snapshot_id,
            "gold_snapshot_hash": self.gold_snapshot_hash,
            "target_weights": _weights(self.target_weights),
            "cash_weight": float(self.cash_weight),
            "component_audit": dict(self.component_audit),
        }

    def to_dict(self) -> dict[str, Any]:
        return {"target_id": self.target_id, **self.content_payload()}

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "ChallengerStockTarget":
        return cls(
            target_id=str(payload["target_id"]),
            generated_at=datetime.fromisoformat(
                str(payload["generated_at"]).replace("Z", "+00:00")
            ),
            decision_date=date.fromisoformat(str(payload["decision_date"])),
            experiment_id=str(payload["experiment_id"]),
            experiment_fingerprint=str(payload["experiment_fingerprint"]),
            result_hash=str(payload["result_hash"]),
            sleeve_id=str(payload["sleeve_id"]),
            account_id=str(payload["account_id"]),
            role_binding_id=str(payload["role_binding_id"]),
            roster_id=str(payload["roster_id"]),
            gold_snapshot_id=str(payload["gold_snapshot_id"]),
            gold_snapshot_hash=str(payload["gold_snapshot_hash"]),
            target_weights=_weights(dict(payload["target_weights"])),
            cash_weight=float(payload["cash_weight"]),
            component_audit=dict(payload["component_audit"]),
            schema_version=str(payload["schema_version"]),
        )


class AuthoritativeChallengerPlanner:
    """Build weekly targets and daily plans only for durable promoted roles."""

    def __init__(
        self,
        catalog: ResearchCatalog,
        *,
        shadow_authority: ShadowEvidenceAuthority,
    ) -> None:
        if shadow_authority is None:
            raise ChallengerPlannerError(
                "production Challenger planning requires formal shadow authority"
            )
        self.catalog = catalog
        self.shadow_authority = shadow_authority
        self._weight_engine = AuthoritativeChampionControl(
            catalog, shadow_authority=shadow_authority
        )

    def _roster(self, roster_id: str) -> SleeveRosterManifest:
        run = self.catalog.get_run(f"sleeve_roster_{roster_id}")
        if (
            run is None
            or run.run_type != "sleeve_roster"
            or run.status != "completed"
            or run.input_fingerprint != roster_id
        ):
            raise ChallengerPlannerError("Challenger frozen roster is absent or changed")
        manifest = run.metadata.get("manifest")
        if not isinstance(manifest, Mapping):
            raise ChallengerPlannerError("Challenger roster run is malformed")
        try:
            roster = SleeveRosterManifest.model_validate(manifest)
        except Exception as exc:
            raise ChallengerPlannerError("Challenger roster manifest is invalid") from exc
        if roster.roster_id != roster_id:
            raise ChallengerPlannerError("Challenger roster identity differs")
        return roster

    def _authority(
        self, experiment_id: str, *, require_projectable: bool
    ) -> ChallengerAuthority:
        binding = self.shadow_authority.active_binding(
            role=ShadowRole.CHALLENGER, role_key=experiment_id
        )
        if (
            binding is None
            or not binding.active
            or binding.experiment_id != experiment_id
            or not binding.sleeve_id
        ):
            raise ChallengerPlannerError("active Challenger role binding is missing")
        account = self.catalog.get_shadow_account(binding.account_id)
        if account is None or account.status != "active":
            raise ChallengerPlannerError("Challenger account is not active")
        experiment = self.catalog.get_experiment(experiment_id)
        result = self.catalog.get_authoritative_result(experiment_id)
        if (
            experiment is None
            or experiment.status is not ExperimentStatus.COMPLETED
            or experiment.spec.factor is not None
            or experiment.spec.sleeve is None
            or experiment.fingerprint != experiment.spec.fingerprint()
            or result is None
            or not result.authoritative
            or result.outcome != "promoted_to_shadow"
            or experiment.spec.sleeve.sleeve_id != binding.sleeve_id
        ):
            raise ChallengerPlannerError(
                "Challenger role is not backed by a promoted authoritative Sleeve"
            )
        expected_result_hash = content_fingerprint(
            {
                "experiment_id": experiment_id,
                "outcome": result.outcome,
                "metrics": dict(result.metrics),
                "artifact_uri": result.artifact_uri,
            },
            domain="factor-lab/research-os/v1/authoritative-result",
        )
        if (
            result.result_hash != expected_result_hash
            or str(binding.metadata.get("result_id") or "") != result.result_id
            or str(binding.metadata.get("result_hash") or "") != result.result_hash
            or binding.bound_at < result.completed_at
        ):
            raise ChallengerPlannerError(
                "Challenger role binding differs from authoritative result evidence"
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
            or research_snapshot.reference.snapshot_id
            != research_snapshot.reference.content_hash
            or _BLOCKING_TRUST_LABELS & set(research_snapshot.reference.trust_labels)
        ):
            raise ChallengerPlannerError(
                "promoted Challenger research snapshot is not accepted Gold"
            )
        portfolio = experiment.spec.portfolio
        universe = experiment.spec.universe
        if (
            portfolio.mode != "long_only"
            or universe.mode != "monthly_liquid_top_n"
            or universe.target_size != 500
            or not universe.point_in_time
            or universe.membership_lag_months < 1
            or abs(float(portfolio.capital) - 50_000_000.0) > 1e-6
            or portfolio.rebalance_sessions != 5
            or not 50
            <= portfolio.target_position_count
            <= 100
            or portfolio.maximum_stock_weight > 0.02 + 1e-12
            or portfolio.maximum_adv_participation > 0.05 + 1e-12
        ):
            raise ChallengerPlannerError(
                "promoted Challenger differs from the formal long-only policy"
            )
        lifecycle = self.catalog.latest_lifecycle_state(binding.sleeve_id)
        if require_projectable and lifecycle not in _PROJECTABLE_STATES:
            raise ChallengerPlannerError(
                f"Challenger lifecycle {lifecycle!s} is not projectable"
            )
        roster_id = str(binding.metadata.get("roster_manifest_id") or "").strip()
        if not roster_id:
            raise ChallengerPlannerError("Challenger role lacks frozen roster identity")
        roster = self._roster(roster_id)
        entry = roster.by_sleeve_id().get(binding.sleeve_id)
        if entry is None or entry.sleeve != experiment.spec.sleeve:
            raise ChallengerPlannerError(
                "promoted Sleeve differs from its frozen roster entry"
            )
        assert lifecycle is not None
        return ChallengerAuthority(binding, experiment, result, roster, lifecycle)

    def active_authorities(
        self, *, projectable_only: bool = False
    ) -> tuple[ChallengerAuthority, ...]:
        events = self.catalog.list_lifecycle_events(limit=1_000)
        if len(events) >= 1_000:
            raise ChallengerPlannerError("lifecycle listing reached the safety limit")
        experiment_ids = sorted(
            {
                str(event.evidence.get("promotion", {}).get("experiment_id") or "")
                for event in events
                if event.cause == "challenger_shadow_account_bound"
                and isinstance(event.evidence.get("promotion"), Mapping)
            }
            - {""}
        )
        authorities = tuple(
            self._authority(experiment_id, require_projectable=False)
            for experiment_id in experiment_ids
        )
        if not projectable_only:
            return authorities
        return tuple(
            item for item in authorities if item.lifecycle_state in _PROJECTABLE_STATES
        )

    @staticmethod
    def _calendar(snapshot: Any) -> tuple[date, ...]:
        raw = snapshot.manifest.get("trading_calendar")
        if not isinstance(raw, Mapping) or not isinstance(
            raw.get("sessions"), (list, tuple)
        ):
            raise ChallengerPlannerError("accepted Gold lacks a trading calendar")
        try:
            sessions = tuple(date.fromisoformat(str(value)) for value in raw["sessions"])
        except (TypeError, ValueError) as exc:
            raise ChallengerPlannerError("accepted Gold calendar is invalid") from exc
        if sessions != tuple(sorted(set(sessions))):
            raise ChallengerPlannerError("accepted Gold calendar is not canonical")
        return sessions

    def _targets(self) -> tuple[ChallengerStockTarget, ...]:
        rows = self.catalog.list_runs(
            limit=1_000, status="completed", run_type=CHALLENGER_TARGET_RUN_TYPE
        )
        if len(rows) >= 1_000:
            raise ChallengerPlannerError("Challenger target listing reached safety limit")
        targets: list[ChallengerStockTarget] = []
        for run in rows:
            raw = run.metadata.get("challenger_stock_target")
            if not isinstance(raw, Mapping):
                raise ChallengerPlannerError("Challenger target run is malformed")
            target = ChallengerStockTarget.from_dict(raw)
            if run.input_fingerprint != target.target_id:
                raise ChallengerPlannerError("Challenger target fingerprint differs")
            targets.append(target)
        return tuple(targets)

    def latest_target(self, experiment_id: str) -> ChallengerStockTarget | None:
        rows = [row for row in self._targets() if row.experiment_id == experiment_id]
        return max(rows, key=lambda row: (row.decision_date, row.target_id)) if rows else None

    def target_for_trade_date(
        self, experiment_id: str, trade_date: date | str
    ) -> ChallengerStockTarget | None:
        trade = (
            trade_date
            if isinstance(trade_date, date) and not isinstance(trade_date, datetime)
            else date.fromisoformat(str(trade_date))
        )
        matches: list[ChallengerStockTarget] = []
        for target in self._targets():
            if target.experiment_id != experiment_id:
                continue
            snapshot = self.catalog.get_snapshot(target.gold_snapshot_id)
            if snapshot is None:
                raise ChallengerPlannerError("Challenger decision snapshot disappeared")
            sessions = self._calendar(snapshot.reference)
            try:
                index = sessions.index(target.decision_date)
            except ValueError as exc:
                raise ChallengerPlannerError(
                    "Challenger target decision is absent from its calendar"
                ) from exc
            if index + 1 < len(sessions) and sessions[index + 1] == trade:
                matches.append(target)
        if len(matches) > 1:
            raise ChallengerPlannerError(
                f"multiple Challenger targets exist for {experiment_id} on {trade}"
            )
        return matches[0] if matches else None

    def _persist(self, target: ChallengerStockTarget) -> RunRecord:
        target_key = content_fingerprint(
            {
                "experiment_id": target.experiment_id,
                "decision_date": target.decision_date.isoformat(),
            },
            domain=_TARGET_KEY_DOMAIN,
        )
        run = RunRecord(
            run_id=f"challenger_target_{target_key[:32]}",
            run_type=CHALLENGER_TARGET_RUN_TYPE,
            status="completed",
            input_fingerprint=target.target_id,
            started_at=target.generated_at,
            completed_at=target.generated_at,
            metadata={"challenger_stock_target": target.to_dict()},
        )
        existing = self.catalog.get_run(run.run_id)
        if existing is not None:
            if (
                existing.run_type != run.run_type
                or existing.status != run.status
                or existing.input_fingerprint != run.input_fingerprint
                or canonical_json(existing.metadata) != canonical_json(run.metadata)
            ):
                raise ChallengerPlannerError(
                    "Challenger target decision was concurrently changed"
                )
            return existing
        stored, won = self.catalog.claim_run(run)
        if not won and (
            stored.input_fingerprint != run.input_fingerprint
            or canonical_json(stored.metadata) != canonical_json(run.metadata)
        ):
            raise ChallengerPlannerError(
                "Challenger target decision was concurrently changed"
            )
        return stored

    def generate_due_targets(
        self,
        *,
        gold_snapshot_id: str,
        gold_frame: pd.DataFrame,
        decision_date: date | str,
    ) -> tuple[ChallengerStockTarget, ...]:
        decision = (
            decision_date
            if isinstance(decision_date, date) and not isinstance(decision_date, datetime)
            else date.fromisoformat(str(decision_date))
        )
        registered = self.catalog.get_snapshot(gold_snapshot_id)
        if registered is None:
            raise ChallengerPlannerError("current Challenger Gold is unregistered")
        snapshot = registered.reference
        if (
            snapshot.tier is not SnapshotTier.GOLD
            or snapshot.quality_status is not DataQualityStatus.ACCEPTED
            or snapshot.snapshot_id != snapshot.content_hash
            or _BLOCKING_TRUST_LABELS & set(snapshot.trust_labels)
        ):
            raise ChallengerPlannerError("current Challenger Gold is not accepted")
        verify_snapshot_frame_binding(snapshot, gold_frame)
        sessions = self._calendar(snapshot)
        try:
            decision_index = sessions.index(decision)
            _next_session = sessions[decision_index + 1]
        except (ValueError, IndexError) as exc:
            raise ChallengerPlannerError(
                "Gold calendar cannot prove the next Challenger session"
            ) from exc
        shanghai = ZoneInfo("Asia/Shanghai")
        decision_close = datetime.combine(
            decision, time(hour=15), tzinfo=shanghai
        ).astimezone(timezone.utc)
        next_open = datetime.combine(
            _next_session, time(hour=9, minute=30), tzinfo=shanghai
        ).astimezone(timezone.utc)
        generated_at = snapshot.as_of.astimezone(timezone.utc)
        if generated_at < decision_close or generated_at >= next_open:
            raise ChallengerPlannerError(
                "Gold publication must be after decision close and before next open"
            )

        try:
            frame = self._weight_engine._normalise_gold_frame(gold_frame, decision)
        except ChampionStockTargetUnavailable as exc:
            raise ChallengerPlannerError(str(exc)) from exc
        current = frame.loc[frame["date"].eq(pd.Timestamp(decision))].copy()
        members = current.loc[current["universe_member"].astype(bool)].copy()
        if len(members) != 500 or members["ticker"].nunique() != 500:
            raise ChallengerPlannerError(
                "current Challenger PIT universe must contain exactly 500 securities"
            )
        benchmark = pd.Series(
            pd.to_numeric(members["benchmark_weight"], errors="coerce").to_numpy(),
            index=members["ticker"].astype(str),
            dtype=float,
        )
        if benchmark.isna().any() or abs(float(benchmark.sum()) - 1.0) > 1e-8:
            raise ChallengerPlannerError("current Challenger benchmark is invalid")

        generated: list[ChallengerStockTarget] = []
        for authority in self.active_authorities(projectable_only=True):
            prior = self.latest_target(authority.experiment_id)
            if prior is not None:
                if prior.decision_date == decision:
                    generated.append(prior)
                    continue
                try:
                    prior_index = sessions.index(prior.decision_date)
                except ValueError as exc:
                    raise ChallengerPlannerError(
                        "current Gold cannot prove prior Challenger rebalance cadence"
                    ) from exc
                if decision_index - prior_index < authority.experiment.spec.portfolio.rebalance_sessions:
                    continue
            try:
                weights, optimizer_audit = self._weight_engine._sleeve_stock_weights(
                    authority.experiment.spec, frame, members, benchmark, decision
                )
            except ChampionStockTargetUnavailable as exc:
                raise ChallengerPlannerError(str(exc)) from exc
            normalized = _weights(weights)
            invested = sum(normalized.values())
            cash_weight = max(0.0, 1.0 - invested)
            content = {
                "schema_version": CHALLENGER_TARGET_SCHEMA_VERSION,
                "generated_at": generated_at.isoformat(),
                "decision_date": decision.isoformat(),
                "experiment_id": authority.experiment_id,
                "experiment_fingerprint": authority.experiment.fingerprint,
                "result_hash": authority.result.result_hash,
                "sleeve_id": authority.sleeve_id,
                "account_id": authority.binding.account_id,
                "role_binding_id": authority.binding.binding_id,
                "roster_id": authority.roster.roster_id,
                "gold_snapshot_id": snapshot.snapshot_id,
                "gold_snapshot_hash": snapshot.content_hash,
                "target_weights": normalized,
                "cash_weight": cash_weight,
                "component_audit": {
                    "source": "promoted_sleeve_frozen_roster_current_gold_dsl",
                    "decision_snapshot_id": snapshot.snapshot_id,
                    "optimizer": optimizer_audit,
                },
            }
            target = ChallengerStockTarget(
                target_id=content_fingerprint(content, domain=_TARGET_DOMAIN),
                generated_at=generated_at,
                decision_date=decision,
                experiment_id=authority.experiment_id,
                experiment_fingerprint=authority.experiment.fingerprint,
                result_hash=authority.result.result_hash,
                sleeve_id=authority.sleeve_id,
                account_id=authority.binding.account_id,
                role_binding_id=authority.binding.binding_id,
                roster_id=authority.roster.roster_id,
                gold_snapshot_id=snapshot.snapshot_id,
                gold_snapshot_hash=snapshot.content_hash,
                target_weights=normalized,
                cash_weight=cash_weight,
                component_audit=content["component_audit"],
            )
            self._persist(target)
            generated.append(target)
        return tuple(generated)

    def plans_for_trade_date(self, trade_date: date | str) -> tuple[DailyShadowPlan, ...]:
        trade = (
            trade_date
            if isinstance(trade_date, date) and not isinstance(trade_date, datetime)
            else date.fromisoformat(str(trade_date))
        )
        plans: list[DailyShadowPlan] = []
        for authority in self.active_authorities():
            target = (
                self.target_for_trade_date(authority.experiment_id, trade)
                if authority.lifecycle_state in _PROJECTABLE_STATES
                else None
            )
            if target is not None and (
                target.account_id != authority.binding.account_id
                or target.role_binding_id != authority.binding.binding_id
                or target.result_hash != authority.result.result_hash
            ):
                raise ChallengerPlannerError(
                    "Challenger target authority differs from its active role"
                )
            plans.append(
                DailyShadowPlan(
                    account_id=authority.binding.account_id,
                    role="challenger",
                    role_key=authority.experiment_id,
                    target_weights=(None if target is None else target.target_weights),
                    decision_snapshot_id=(
                        None if target is None else target.gold_snapshot_id
                    ),
                    model_version=(
                        None
                        if target is None
                        else f"challenger:{target.experiment_fingerprint}"
                    ),
                )
            )
        return tuple(plans)


__all__ = [
    "CHALLENGER_TARGET_RUN_TYPE",
    "CHALLENGER_TARGET_SCHEMA_VERSION",
    "AuthoritativeChallengerPlanner",
    "ChallengerAuthority",
    "ChallengerPlannerError",
    "ChallengerStockTarget",
]

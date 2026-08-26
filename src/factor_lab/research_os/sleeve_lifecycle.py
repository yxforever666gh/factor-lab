"""Authoritative Sleeve-result promotion and daily shadow fleet coordination."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

import pandas as pd

from .catalog import LifecycleEvent, ResearchCatalog, ShadowEventInput
from .contracts import DataQualityStatus, LifecycleState, SnapshotTier
from .fingerprint import content_fingerprint
from .lifecycle import (
    LifecycleDecision,
    ShadowActivationEvidence,
    ShadowPromotionEvidence,
    SleeveLifecycleRecord,
    SleeveState,
    authorize_shadow_activation,
    promote_authoritative_result_to_shadow,
)
from .recovery import RecoveryCoordinator
from .shadow import ShadowSnapshotBindings
from .shadow_authority import ShadowAuthorityError, ShadowEvidenceAuthority, ShadowRole
from .shadow_catalog import ShadowStepResult, ShadowStepService
from .sleeve_registry import SleeveRosterManifest


class SleeveLifecycleBridgeError(RuntimeError):
    pass


@dataclass(frozen=True)
class PromotedShadowBinding:
    sleeve_id: str
    experiment_id: str
    result_id: str
    shadow_account_id: str
    lifecycle: LifecycleDecision
    recovery_case_id: str | None = None
    role_binding_id: str | None = None


@dataclass(frozen=True)
class DailyShadowPlan:
    account_id: str
    role: str
    target_weights: Mapping[str, float] | None = None
    decision_snapshot_id: str | None = None
    model_version: str | None = None
    role_key: str | None = None

    def __post_init__(self) -> None:
        if not self.account_id.strip() or self.role not in {"champion", "challenger", "sleeve"}:
            raise ValueError("daily shadow plan requires account_id and a known role")
        if self.target_weights is not None and self.decision_snapshot_id is None:
            raise ValueError("a rebalance plan requires decision_snapshot_id")
        if self.role_key is not None and not self.role_key.strip():
            raise ValueError("role_key cannot be blank")


class SleeveShadowLifecycleService:
    def __init__(
        self,
        catalog: ResearchCatalog,
        *,
        shadow_authority: ShadowEvidenceAuthority | None = None,
        legacy_mode: bool | None = None,
    ) -> None:
        self.catalog = catalog
        self.shadow_authority = shadow_authority
        # Native SQLite is the explicitly non-production catalog used by unit
        # tests and legacy import.  SQLAlchemy/PostgreSQL callers must provide
        # the 0007 authority service; they cannot silently fall back to event
        # counts.
        inferred_legacy = (
            getattr(catalog, "_backend", None).__class__.__name__ == "_SQLiteCatalog"
        )
        self.legacy_mode = inferred_legacy if legacy_mode is None else bool(legacy_mode)

    @staticmethod
    def _account_id(experiment_id: str, result_hash: str) -> str:
        digest = content_fingerprint(
            {"experiment_id": experiment_id, "result_hash": result_hash},
            domain="factor-lab/research-os/v1/promoted-shadow-account",
        )
        return f"challenger_{digest[:32]}"

    def promote(
        self,
        *,
        record: SleeveLifecycleRecord,
        experiment_id: str,
        roster: SleeveRosterManifest,
        promoted_at: datetime,
        initial_capital: float = 50_000_000.0,
        recovery_case_id: str | None = None,
    ) -> PromotedShadowBinding:
        if promoted_at.tzinfo is None or promoted_at.utcoffset() is None:
            raise ValueError("promoted_at must include a timezone")
        if self.shadow_authority is None and not self.legacy_mode:
            raise SleeveLifecycleBridgeError(
                "production SHADOW promotion requires the formal shadow authority"
            )
        experiment = self.catalog.get_experiment(experiment_id)
        result = self.catalog.get_authoritative_result(experiment_id)
        if experiment is None or result is None:
            raise SleeveLifecycleBridgeError(
                "promotion requires a registered experiment and authoritative result"
            )
        sleeve = experiment.spec.sleeve
        if sleeve is None or sleeve.sleeve_id != record.sleeve_id:
            raise SleeveLifecycleBridgeError("experiment is not for the supplied Sleeve record")
        roster_entry = roster.by_sleeve_id().get(record.sleeve_id)
        if roster_entry is None:
            raise SleeveLifecycleBridgeError("Sleeve is absent from the frozen roster")
        if roster_entry.sleeve != sleeve:
            raise SleeveLifecycleBridgeError("experiment Sleeve differs from frozen roster content")
        snapshot = self.catalog.get_snapshot(experiment.spec.snapshot.snapshot_id)
        if (
            snapshot is None
            or snapshot.reference.tier is not SnapshotTier.GOLD
            or snapshot.reference.quality_status is not DataQualityStatus.ACCEPTED
        ):
            raise SleeveLifecycleBridgeError("promoted result is not bound to accepted Gold")

        account_id = self._account_id(experiment_id, result.result_hash)
        promotion_evidence = ShadowPromotionEvidence(
            experiment_id=experiment_id,
            result_id=result.result_id,
            result_hash=result.result_hash,
            shadow_account_id=account_id,
            roster_manifest_id=roster.roster_id,
            authoritative_outcome=result.outcome,
            roster_member=True,
        )
        decision = promote_authoritative_result_to_shadow(
            SleeveLifecycleRecord(
                sleeve_id=record.sleeve_id,
                state=SleeveState.WALK_FORWARD,
                target_weight=record.target_weight,
                effective_weight=record.effective_weight,
            ),
            promotion_evidence,
            as_of_date=promoted_at.astimezone(timezone.utc).date(),
        )
        if decision.record.state is not SleeveState.SHADOW:
            raise SleeveLifecycleBridgeError(
                "authoritative result did not satisfy the controlled SHADOW gate: "
                + ",".join(decision.alarm_reasons)
            )

        account = self.catalog.create_shadow_account(
            account_id=account_id,
            name=f"{record.sleeve_id} Challenger",
            initial_capital=initial_capital,
            opened_at=promoted_at,
        )
        existing_baseline = self.catalog.list_shadow_events_by_type(
            account_id=account_id,
            event_type="account_projected",
            since=None,
            through=promoted_at,
            limit=10,
        )
        if not existing_baseline:
            self.catalog.append_shadow_event(
                account_id=account_id,
                event_type="account_projected",
                occurred_at=promoted_at,
                expected_previous_hash=account.last_event_hash,
                payload={
                    "research_os_shadow_step": {
                        "step_id": f"promotion_{result.result_hash[:24]}",
                        "kind": "promotion_baseline",
                    },
                    "account_status": "active",
                    "account_state": {
                        "cash": initial_capital,
                        "nav": initial_capital,
                        "benchmark_nav": initial_capital,
                    },
                    "promotion": asdict(promotion_evidence),
                },
            )
        lifecycle_evidence = {
            "promotion": asdict(promotion_evidence),
            "snapshot_id": experiment.spec.snapshot.snapshot_id,
            "snapshot_hash": experiment.spec.snapshot.content_hash,
            "experiment_fingerprint": experiment.fingerprint,
            "role_binding_id": None,
        }
        lifecycle_history = self.catalog.list_lifecycle_events(
            sleeve_id=record.sleeve_id, limit=1_000
        )
        latest_event = lifecycle_history[0] if lifecycle_history else None
        current_state = None if latest_event is None else latest_event.to_state
        if current_state is LifecycleState.FROZEN_DATA:
            raise SleeveLifecycleBridgeError(
                "a frozen_data Sleeve cannot create or bind a Challenger"
            )
        allowed_existing_states = {
            LifecycleState.WALK_FORWARD,
            LifecycleState.SHADOW,
            LifecycleState.ACTIVE,
            LifecycleState.REDUCED,
            LifecycleState.DORMANT,
        }
        if current_state is not None and current_state not in allowed_existing_states:
            raise SleeveLifecycleBridgeError(
                f"a {current_state.value} Sleeve cannot create a Challenger"
            )
        promotion_keys = {
            f"shadow-promotion:{result.result_hash}",
            f"shadow-bootstrap:{result.result_hash}:{LifecycleState.SHADOW.value}",
        }
        existing_promotions = tuple(
            event
            for event in lifecycle_history
            if event.idempotency_key in promotion_keys
        )
        if len(existing_promotions) > 1:
            raise SleeveLifecycleBridgeError(
                "shadow promotion has ambiguous durable lifecycle evidence"
            )
        existing_promotion = (
            existing_promotions[0] if existing_promotions else None
        )
        if existing_promotion is not None:
            if not (
                existing_promotion.to_state
                in {
                    LifecycleState.SHADOW,
                    LifecycleState.ACTIVE,
                    LifecycleState.REDUCED,
                    LifecycleState.DORMANT,
                    LifecycleState.PROBATION,
                    LifecycleState.RETIRED,
                }
                and dict(existing_promotion.evidence) == lifecycle_evidence
            ):
                raise SleeveLifecycleBridgeError(
                    "shadow promotion lifecycle replay differs from durable evidence"
                )
            assert latest_event is not None and current_state is not None
            # The promotion event proves the research decision, but it is not
            # the current CAS base after later monitor/incident transitions.
            effective_state = current_state
            promotion_time = max(
                promoted_at,
                latest_event.occurred_at + timedelta(microseconds=1),
            )
        elif current_state is None:
            promotion_time = promoted_at
            bootstrap_states = (
                LifecycleState.PREREGISTERED,
                LifecycleState.CANARY,
                LifecycleState.WALK_FORWARD,
                LifecycleState.SHADOW,
            )
            bootstrap_events: list[LifecycleEvent] = []
            expected_state: LifecycleState | None = None
            for index, state in enumerate(bootstrap_states):
                bootstrap_events.append(
                    LifecycleEvent(
                        idempotency_key=(
                            f"shadow-bootstrap:{result.result_hash}:{state.value}"
                        ),
                        sleeve_id=record.sleeve_id,
                        from_state=expected_state,
                        to_state=state,
                        cause=(
                            "authoritative_result_promoted_to_shadow"
                            if state is LifecycleState.SHADOW
                            else "authoritative_research_path_materialized"
                        ),
                        occurred_at=promotion_time
                        + timedelta(microseconds=index),
                        evidence=lifecycle_evidence,
                    )
                )
                expected_state = state
            self.catalog.append_lifecycle_path(bootstrap_events)
            effective_state = LifecycleState.SHADOW
            promotion_time = bootstrap_events[-1].occurred_at
        else:
            promotion_time = max(
                promoted_at,
                latest_event.occurred_at + timedelta(microseconds=1),
            )
            effective_state = (
                LifecycleState.SHADOW
                if current_state is LifecycleState.WALK_FORWARD
                else current_state
            )
            self.catalog.append_lifecycle_event(
                LifecycleEvent(
                    idempotency_key=f"shadow-promotion:{result.result_hash}",
                    sleeve_id=record.sleeve_id,
                    from_state=current_state,
                    to_state=effective_state,
                    cause=(
                        "authoritative_result_promoted_to_shadow"
                        if current_state is LifecycleState.WALK_FORWARD
                        else "authoritative_challenger_created_without_state_change"
                    ),
                    occurred_at=promotion_time,
                    evidence=lifecycle_evidence,
                )
            )
        role_binding = None
        if self.shadow_authority is not None:
            role_binding = self.shadow_authority.bind_role(
                role=ShadowRole.CHALLENGER,
                role_key=experiment_id,
                account_id=account_id,
                sleeve_id=record.sleeve_id,
                experiment_id=experiment_id,
                bound_at=promotion_time,
                metadata={
                    "result_id": result.result_id,
                    "result_hash": result.result_hash,
                    "roster_manifest_id": roster.roster_id,
                },
            )
        binding_evidence = {
            **lifecycle_evidence,
            "role_binding_id": (
                None if role_binding is None else role_binding.binding_id
            ),
            "shadow_account_id": account_id,
        }
        binding_key = f"shadow-binding:{result.result_hash}"
        refreshed_history = self.catalog.list_lifecycle_events(
            sleeve_id=record.sleeve_id, limit=1_000
        )
        existing_bindings = tuple(
            event
            for event in refreshed_history
            if event.idempotency_key == binding_key
        )
        if len(existing_bindings) > 1:
            raise SleeveLifecycleBridgeError(
                "shadow binding has ambiguous durable lifecycle evidence"
            )
        if existing_bindings:
            existing_binding = existing_bindings[0]
            if not (
                existing_binding.from_state == existing_binding.to_state
                and existing_binding.cause == "challenger_shadow_account_bound"
                and dict(existing_binding.evidence) == binding_evidence
            ):
                raise SleeveLifecycleBridgeError(
                    "shadow binding replay differs from durable lifecycle evidence"
                )
            binding_time = existing_binding.occurred_at
        else:
            latest_after_role = refreshed_history[0] if refreshed_history else None
            if latest_after_role is None or latest_after_role.to_state not in {
                LifecycleState.SHADOW,
                LifecycleState.ACTIVE,
                LifecycleState.REDUCED,
                LifecycleState.DORMANT,
            }:
                raise SleeveLifecycleBridgeError(
                    "current Sleeve state forbids Challenger binding materialization"
                )
            effective_state = latest_after_role.to_state
            binding_time = max(
                promotion_time + timedelta(microseconds=1),
                latest_after_role.occurred_at + timedelta(microseconds=1),
            )
            self.catalog.append_lifecycle_event(
                LifecycleEvent(
                    idempotency_key=binding_key,
                    sleeve_id=record.sleeve_id,
                    from_state=effective_state,
                    to_state=effective_state,
                    cause="challenger_shadow_account_bound",
                    occurred_at=binding_time,
                    evidence=binding_evidence,
                )
            )
        if recovery_case_id is not None:
            recovery = RecoveryCoordinator(self.catalog)
            case = self.catalog.get_recovery_case(recovery_case_id)
            if case is None:
                raise SleeveLifecycleBridgeError("recovery case was not found")
            if experiment_id not in case.challenger_ids:
                case = recovery.register_challengers(
                    recovery_case_id,
                    (experiment_id,),
                    registered_at=binding_time,
                )
            recovery.bind_shadow_account(
                recovery_case_id,
                experiment_id,
                account_id,
                bound_at=binding_time + timedelta(microseconds=1),
            )
        return PromotedShadowBinding(
            sleeve_id=record.sleeve_id,
            experiment_id=experiment_id,
            result_id=result.result_id,
            shadow_account_id=account_id,
            lifecycle=decision,
            recovery_case_id=recovery_case_id,
            role_binding_id=(
                None if role_binding is None else role_binding.binding_id
            ),
        )

    def authorize_activation(
        self,
        *,
        record: SleeveLifecycleRecord,
        shadow_account_id: str,
        observation_started_on: date,
        as_of: datetime,
        minimum_new_sessions: int = 60,
        champion_account_id: str | None = None,
    ) -> LifecycleDecision:
        if as_of.tzinfo is None or as_of.utcoffset() is None:
            raise ValueError("as_of must include a timezone")
        account = self.catalog.get_shadow_account(shadow_account_id)
        if account is None:
            raise SleeveLifecycleBridgeError("shadow account was not found")
        if self.shadow_authority is not None:
            if not champion_account_id:
                raise SleeveLifecycleBridgeError(
                    "formal Challenger probation requires the static Champion account"
                )
            try:
                aligned = self.shadow_authority.aligned_forward_window(
                    champion_account_id=champion_account_id,
                    challenger_account_id=shadow_account_id,
                    minimum_sessions=minimum_new_sessions,
                    through=as_of.date(),
                )
            except ShadowAuthorityError as exc:
                raise SleeveLifecycleBridgeError(
                    f"formal Challenger probation evidence was rejected: {exc}"
                ) from exc
            if not (
                aligned.challenger_binding.active
                and aligned.challenger_binding.account_id == shadow_account_id
                and aligned.challenger_binding.sleeve_id == record.sleeve_id
                and aligned.challenger_binding.experiment_id is not None
                and aligned.challenger_binding.role_key
                == aligned.challenger_binding.experiment_id
            ):
                raise SleeveLifecycleBridgeError(
                    "probation sleeve and Challenger shadow authority are mismatched"
                )
            evidence = ShadowActivationEvidence(
                shadow_account_id=shadow_account_id,
                observed_sessions=aligned.observed_sessions,
                chain_verified=True,
                data_quality_ok=aligned.data_quality_ok,
                event_chain_evidence_hash=aligned.evidence_hash,
                champion_account_id=champion_account_id,
                epoch_id=aligned.epoch_id,
                evidence_window_hash=aligned.evidence_window_hash,
                common_session_hash=aligned.evidence_hash,
                challenger_outperformed_static=(
                    aligned.challenger_outperformed_static
                ),
                forward_authority_verified=True,
            )
            decision = authorize_shadow_activation(
                record,
                evidence,
                as_of_date=as_of.date(),
                minimum_new_sessions=minimum_new_sessions,
                require_formal_authority=True,
            )
            if decision.transition is not None:
                self.catalog.append_lifecycle_event(
                    LifecycleEvent(
                        idempotency_key=(
                            f"shadow-probation:{record.sleeve_id}:{aligned.evidence_hash}"
                        ),
                        sleeve_id=record.sleeve_id,
                        from_state=LifecycleState(record.state.value),
                        to_state=LifecycleState(decision.record.state.value),
                        cause="controlled_shadow_probation",
                        occurred_at=as_of,
                        evidence={
                            "shadow_account_id": shadow_account_id,
                            "champion_account_id": champion_account_id,
                            "observed_sessions": aligned.observed_sessions,
                            "epoch_id": aligned.epoch_id,
                            "evidence_window_hash": aligned.evidence_window_hash,
                            "common_session_hash": aligned.evidence_hash,
                            "shadow_excess": aligned.shadow_excess,
                            "minimum_new_sessions": minimum_new_sessions,
                        },
                    )
                )
            return decision
        if not self.legacy_mode:
            raise SleeveLifecycleBridgeError(
                "production activation cannot use legacy event-count evidence"
            )
        projections = self.catalog.list_shadow_events_by_type(
            account_id=shadow_account_id,
            event_type="account_projected",
            since=None,
            through=as_of,
            limit=1_000,
        )
        session_events = self.catalog.list_shadow_events_by_type(
            account_id=shadow_account_id,
            event_type="session_evidence",
            since=None,
            through=as_of,
            limit=1_000,
        )
        incidents = self.catalog.list_shadow_events_by_type(
            account_id=shadow_account_id,
            event_type="data_incident",
            since=None,
            through=as_of,
            limit=1_000,
        )
        revalidations = self.catalog.list_shadow_events_by_type(
            account_id=shadow_account_id,
            event_type="data_revalidated",
            since=None,
            through=as_of,
            limit=1_000,
        )
        events = (*projections, *session_events, *incidents, *revalidations)
        daily_steps = {
            str(event.payload.get("research_os_shadow_step", {}).get("step_id") or "")
            for event in events
            if event.event_type == "session_evidence"
        }
        projected_hashes = tuple(
            sorted(
                event.event_hash
                for event in events
                if event.event_type == "account_projected"
                and event.payload.get("research_os_shadow_step", {}).get("kind")
                == "account_projection"
                and str(
                    event.payload.get("research_os_shadow_step", {}).get("step_id")
                    or ""
                )
                in daily_steps
                and event.occurred_at.date() > observation_started_on
                and event.occurred_at <= as_of
            )
        )
        session_count = self.catalog.count_shadow_sessions(
            account_id=shadow_account_id,
            since=observation_started_on,
            through=as_of.date(),
        )
        # count_shadow_sessions includes only dates after ``since`` but may see
        # non-daily legacy projections; bind the gate to explicit daily hashes.
        session_count = min(session_count, len(projected_hashes))
        latest_incident = max((event.occurred_at for event in incidents), default=None)
        latest_revalidation = max(
            (event.occurred_at for event in revalidations), default=None
        )
        data_ok = latest_incident is None or (
            latest_revalidation is not None and latest_revalidation > latest_incident
        )
        prefix_last_hash = max(
            events, key=lambda event: event.sequence_number
        ).event_hash
        evidence_hash = content_fingerprint(
            {
                "account_id": shadow_account_id,
                "observation_started_on": observation_started_on,
                "as_of": as_of,
                "projected_hashes": projected_hashes,
                "last_event_hash": prefix_last_hash,
            },
            domain="factor-lab/research-os/v1/shadow-activation-evidence",
        )
        evidence = ShadowActivationEvidence(
            shadow_account_id=shadow_account_id,
            observed_sessions=session_count,
            chain_verified=self.catalog.verify_shadow_chain(shadow_account_id),
            data_quality_ok=data_ok,
            event_chain_evidence_hash=evidence_hash,
        )
        decision = authorize_shadow_activation(
            record,
            evidence,
            as_of_date=as_of.date(),
            minimum_new_sessions=minimum_new_sessions,
            require_formal_authority=False,
        )
        if decision.transition is not None:
            self.catalog.append_lifecycle_event(
                LifecycleEvent(
                    idempotency_key=(
                        f"shadow-probation:{record.sleeve_id}:{evidence_hash}"
                    ),
                    sleeve_id=record.sleeve_id,
                    from_state=LifecycleState(record.state.value),
                    to_state=LifecycleState(decision.record.state.value),
                    cause="controlled_shadow_probation",
                    occurred_at=as_of,
                    evidence={
                        "shadow_account_id": shadow_account_id,
                        "observed_sessions": session_count,
                        "event_chain_evidence_hash": evidence_hash,
                        "minimum_new_sessions": minimum_new_sessions,
                    },
                )
            )
        return decision


class ShadowFleetCoordinator:
    """Advance Champion and every Challenger through the same daily service."""

    def __init__(
        self,
        catalog: ResearchCatalog,
        *,
        shadow_authority: ShadowEvidenceAuthority | None = None,
        legacy_mode: bool | None = None,
    ) -> None:
        self.service = ShadowStepService(catalog)
        self.shadow_authority = shadow_authority
        inferred_legacy = (
            getattr(catalog, "_backend", None).__class__.__name__ == "_SQLiteCatalog"
        )
        self.legacy_mode = inferred_legacy if legacy_mode is None else bool(legacy_mode)

    def project_daily(
        self,
        *,
        plans: Sequence[DailyShadowPlan],
        trade_date: date,
        market_bars: pd.DataFrame,
        execution_snapshot_id: str,
        mark_snapshot_id: str,
        benchmark_return: float,
        session_metrics: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> tuple[ShadowStepResult, ...]:
        if not plans:
            raise ValueError("at least one Champion/Challenger plan is required")
        account_ids = [plan.account_id for plan in plans]
        if len(set(account_ids)) != len(account_ids):
            raise ValueError("daily fleet plans contain duplicate accounts")
        if self.shadow_authority is None and not self.legacy_mode:
            raise SleeveLifecycleBridgeError(
                "production daily shadow projection requires the formal authority"
            )
        role_bindings = {}
        if self.shadow_authority is not None:
            active_fleet = self.shadow_authority.active_fleet_bindings()
            if not any(
                binding.role is ShadowRole.CHAMPION for binding in active_fleet
            ):
                raise SleeveLifecycleBridgeError(
                    "daily production shadow fleet has no active Champion"
                )
            if len({binding.account_id for binding in active_fleet}) != len(
                active_fleet
            ):
                raise SleeveLifecycleBridgeError(
                    "daily production shadow fleet reuses an account"
                )
            active_by_account = {
                binding.account_id: binding for binding in active_fleet
            }
            if set(account_ids) != set(active_by_account):
                missing = sorted(set(active_by_account) - set(account_ids))
                unexpected = sorted(set(account_ids) - set(active_by_account))
                raise SleeveLifecycleBridgeError(
                    "daily plans must exactly cover the active Champion/Challenger fleet; "
                    f"missing={missing}, unexpected={unexpected}"
                )
            for plan in plans:
                binding = active_by_account[plan.account_id]
                role_key = str(plan.role_key or plan.account_id)
                if (
                    binding.role is not ShadowRole(plan.role)
                    or binding.role_key != role_key
                ):
                    raise SleeveLifecycleBridgeError(
                        f"daily plan {plan.account_id!r} differs from its active role binding"
                    )
                role_bindings[plan.account_id] = binding
        results: list[ShadowStepResult] = []
        role_order = {"champion": 0, "challenger": 1, "sleeve": 2}
        for plan in sorted(
            plans,
            key=lambda item: (role_order[item.role], item.account_id),
        ):
            result = self.service.project_or_recover_session(
                account_id=plan.account_id,
                trade_date=trade_date,
                market_bars=market_bars,
                snapshot_bindings=ShadowSnapshotBindings(
                    decision_snapshot_id=(
                        plan.decision_snapshot_id
                        if plan.target_weights is not None
                        else None
                    ),
                    execution_snapshot_id=execution_snapshot_id,
                    mark_snapshot_id=mark_snapshot_id,
                ),
                benchmark_return=benchmark_return,
                target_weights=plan.target_weights,
                model_version=plan.model_version,
                session_metrics=(session_metrics or {}).get(plan.account_id, {}),
            )
            if self.shadow_authority is not None:
                self.shadow_authority.record_projection(
                    role_binding_id=role_bindings[plan.account_id].binding_id,
                    account_event_hash=result.last_event_hash,
                    trade_date=trade_date,
                    recorded_at=datetime.now(timezone.utc),
                )
            results.append(result)
        if self.shadow_authority is not None:
            self.shadow_authority.close_fleet_day(trade_date)
        return tuple(results)


__all__ = [
    "DailyShadowPlan",
    "PromotedShadowBinding",
    "ShadowFleetCoordinator",
    "SleeveLifecycleBridgeError",
    "SleeveShadowLifecycleService",
]

"""Deterministic sleeve health, degradation and recovery state machine."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date
from enum import Enum
from typing import Any


class SleeveState(str, Enum):
    PROPOSED = "proposed"
    PREREGISTERED = "preregistered"
    CANARY = "canary"
    WALK_FORWARD = "walk_forward"
    SHADOW = "shadow"
    ACTIVE = "active"
    REDUCED = "reduced"
    DORMANT = "dormant"
    PROBATION = "probation"
    RETIRED = "retired"
    FROZEN_DATA = "frozen_data"


PREPRODUCTION_SEQUENCE = (
    SleeveState.PROPOSED,
    SleeveState.PREREGISTERED,
    SleeveState.CANARY,
    SleeveState.WALK_FORWARD,
    SleeveState.SHADOW,
)


@dataclass(frozen=True)
class SleeveHealthObservation:
    as_of_date: date
    active_ir_13w: float
    active_ir_26w: float
    ic_26w: float
    expected_ic_direction: int = 1
    cost_ratio_to_baseline: float = 1.0
    active_drawdown: float = 0.0
    drawdown_95_limit: float = 0.25
    active_return_20d: float = 0.0
    active_return_60d: float = 0.0
    new_sessions_since_dormant: int = 0
    data_quality_ok: bool = True
    data_revalidation_passed: bool = False
    critical_risk_alert: bool = False

    @property
    def ic_direction_ok(self) -> bool:
        direction = 1 if self.expected_ic_direction >= 0 else -1
        return float(self.ic_26w) * direction > 0

    def alarm_reasons(self) -> tuple[str, ...]:
        reasons: list[str] = []
        if float(self.active_ir_13w) < 0:
            reasons.append("negative_13w_information_ratio")
        if not self.ic_direction_ok:
            reasons.append("wrong_26w_ic_direction")
        if float(self.cost_ratio_to_baseline) > 1.5:
            reasons.append("cost_above_1_5x_baseline")
        if abs(float(self.active_drawdown)) > abs(float(self.drawdown_95_limit)):
            reasons.append("drawdown_above_training_95pct")
        if self.critical_risk_alert:
            reasons.append("critical_risk_alert")
        return tuple(reasons)


@dataclass(frozen=True)
class LifecycleTransition:
    from_state: SleeveState
    to_state: SleeveState
    as_of_date: date
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class SleeveLifecycleRecord:
    sleeve_id: str
    state: SleeveState = SleeveState.PROPOSED
    target_weight: float = 0.0
    effective_weight: float = 0.0
    consecutive_multi_alarm_checks: int = 0
    reduced_weeks: int = 0
    probation_weeks: int = 0
    dormant_since: date | None = None
    transitions: tuple[LifecycleTransition, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class LifecycleDecision:
    record: SleeveLifecycleRecord
    transition: LifecycleTransition | None
    alarm_reasons: tuple[str, ...]
    recommended_action: str


@dataclass(frozen=True)
class ShadowPromotionEvidence:
    experiment_id: str
    result_id: str
    result_hash: str
    shadow_account_id: str
    roster_manifest_id: str
    authoritative_outcome: str
    roster_member: bool = True

    def __post_init__(self) -> None:
        if not all(
            str(value).strip()
            for value in (
                self.experiment_id,
                self.result_id,
                self.shadow_account_id,
                self.roster_manifest_id,
            )
        ):
            raise ValueError("promotion evidence identifiers are required")
        if len(self.result_hash) != 64 or any(
            character not in "0123456789abcdef" for character in self.result_hash
        ):
            raise ValueError("result_hash must be a lowercase SHA-256 digest")


@dataclass(frozen=True)
class ShadowActivationEvidence:
    shadow_account_id: str
    observed_sessions: int
    chain_verified: bool
    data_quality_ok: bool
    event_chain_evidence_hash: str
    champion_account_id: str | None = None
    epoch_id: str | None = None
    evidence_window_hash: str | None = None
    common_session_hash: str | None = None
    challenger_outperformed_static: bool = False
    forward_authority_verified: bool = False

    def __post_init__(self) -> None:
        if not self.shadow_account_id.strip():
            raise ValueError("shadow_account_id is required")
        if self.observed_sessions < 0:
            raise ValueError("observed_sessions cannot be negative")
        if len(self.event_chain_evidence_hash) != 64 or any(
            character not in "0123456789abcdef"
            for character in self.event_chain_evidence_hash
        ):
            raise ValueError("event_chain_evidence_hash must be a lowercase SHA-256 digest")
        formal_values = (
            self.champion_account_id,
            self.epoch_id,
            self.evidence_window_hash,
            self.common_session_hash,
        )
        if self.forward_authority_verified and any(value is None for value in formal_values):
            raise ValueError("verified forward authority requires Champion, epoch and window bindings")
        for name, value in (
            ("evidence_window_hash", self.evidence_window_hash),
            ("common_session_hash", self.common_session_hash),
        ):
            if value is not None and (
                len(value) != 64
                or any(character not in "0123456789abcdef" for character in value)
            ):
                raise ValueError(f"{name} must be a lowercase SHA-256 digest")


def _move(
    record: SleeveLifecycleRecord,
    target: SleeveState,
    observation: SleeveHealthObservation,
    reasons: tuple[str, ...],
    **changes: Any,
) -> LifecycleDecision:
    transition = LifecycleTransition(
        from_state=record.state,
        to_state=target,
        as_of_date=observation.as_of_date,
        reasons=reasons,
    )
    updated = replace(record, state=target, transitions=record.transitions + (transition,), **changes)
    action = {
        SleeveState.REDUCED: "halve_weight",
        SleeveState.DORMANT: "move_weight_to_benchmark",
        SleeveState.PROBATION: "start_weight_ramp",
        SleeveState.ACTIVE: "continue",
        SleeveState.FROZEN_DATA: "move_all_weight_to_cash",
    }.get(target, "monitor")
    return LifecycleDecision(updated, transition, observation.alarm_reasons(), action)


def advance_preproduction_stage(
    record: SleeveLifecycleRecord,
    *,
    gate_passed: bool,
    as_of_date: date,
    reason: str = "stage_gate_passed",
) -> LifecycleDecision:
    """Advance exactly one preregistered research stage.

    A failed gate never advances or silently changes a threshold.
    """

    if record.state not in PREPRODUCTION_SEQUENCE or record.state == SleeveState.SHADOW:
        return LifecycleDecision(record, None, (), "no_stage_change")
    if not gate_passed:
        return LifecycleDecision(record, None, ("stage_gate_failed",), "retain_stage")
    index = PREPRODUCTION_SEQUENCE.index(record.state)
    target = PREPRODUCTION_SEQUENCE[index + 1]
    obs = SleeveHealthObservation(as_of_date, 0.0, 0.0, 0.0)
    return _move(record, target, obs, (reason,))


def promote_authoritative_result_to_shadow(
    record: SleeveLifecycleRecord,
    evidence: ShadowPromotionEvidence,
    *,
    as_of_date: date,
) -> LifecycleDecision:
    """Bind a registered roster Sleeve to an authoritative promoted result.

    Roster membership is never promotion.  The exact authoritative evaluator
    outcome and a WALK_FORWARD state are both required before SHADOW exists.
    """

    if record.state is SleeveState.SHADOW:
        return LifecycleDecision(record, None, (), "already_in_shadow")
    if record.state is not SleeveState.WALK_FORWARD:
        return LifecycleDecision(record, None, ("walk_forward_not_complete",), "retain_stage")
    if not evidence.roster_member:
        return LifecycleDecision(record, None, ("sleeve_not_in_frozen_roster",), "reject")
    if evidence.authoritative_outcome != "promoted_to_shadow":
        return LifecycleDecision(
            record,
            None,
            ("authoritative_result_did_not_promote",),
            "retain_stage",
        )
    observation = SleeveHealthObservation(as_of_date, 0.0, 0.0, 0.0)
    return _move(
        record,
        SleeveState.SHADOW,
        observation,
        ("authoritative_result_promoted", "challenger_shadow_account_bound"),
        effective_weight=0.0,
    )


def authorize_shadow_activation(
    record: SleeveLifecycleRecord,
    evidence: ShadowActivationEvidence,
    *,
    as_of_date: date,
    minimum_new_sessions: int = 60,
    require_formal_authority: bool = False,
) -> LifecycleDecision:
    """Controlled SHADOW -> PROBATION gate based on persisted new epochs.

    Sixty sessions authorize only the beginning of the bounded monthly ramp.
    They can never jump directly from SHADOW to ACTIVE.  Production callers
    additionally require database-bound Champion/Challenger evidence and a
    deterministic positive comparison against the static Champion.
    """

    if minimum_new_sessions < 60:
        raise ValueError("shadow activation cannot use fewer than 60 new sessions")
    if record.state is not SleeveState.SHADOW:
        return LifecycleDecision(record, None, (), "not_in_shadow")
    if not evidence.chain_verified:
        return LifecycleDecision(record, None, ("invalid_shadow_event_chain",), "retain_shadow")
    if not evidence.data_quality_ok:
        observation = SleeveHealthObservation(
            as_of_date,
            0.0,
            0.0,
            0.0,
            data_quality_ok=False,
        )
        return _move(
            record,
            SleeveState.FROZEN_DATA,
            observation,
            ("data_quality_failed",),
            effective_weight=0.0,
        )
    if evidence.observed_sessions < minimum_new_sessions:
        return LifecycleDecision(
            record,
            None,
            (f"shadow_sessions_{evidence.observed_sessions}_of_{minimum_new_sessions}",),
            "continue_shadow_observation",
        )
    if require_formal_authority and not evidence.forward_authority_verified:
        return LifecycleDecision(
            record,
            None,
            ("formal_forward_authority_missing",),
            "retain_shadow",
        )
    if require_formal_authority and not evidence.challenger_outperformed_static:
        return LifecycleDecision(
            record,
            None,
            ("challenger_did_not_beat_static_champion",),
            "fallback_static_champion",
        )
    observation = SleeveHealthObservation(as_of_date, 0.0, 0.0, 1.0)
    return _move(
        record,
        SleeveState.PROBATION,
        observation,
        ("sixty_new_shadow_sessions", "controlled_probation_authorized"),
        effective_weight=min(max(record.target_weight, 0.0), 0.05),
        probation_weeks=1,
        consecutive_multi_alarm_checks=0,
    )


def advance_lifecycle(
    record: SleeveLifecycleRecord,
    observation: SleeveHealthObservation,
) -> LifecycleDecision:
    """Apply one weekly, point-in-time health observation."""

    alarms = observation.alarm_reasons()
    if not observation.data_quality_ok:
        if record.state == SleeveState.FROZEN_DATA:
            return LifecycleDecision(record, None, ("data_quality_failed",), "remain_in_cash")
        return _move(
            record,
            SleeveState.FROZEN_DATA,
            observation,
            ("data_quality_failed",),
            effective_weight=0.0,
        )

    if record.state == SleeveState.FROZEN_DATA:
        if not observation.data_revalidation_passed:
            return LifecycleDecision(record, None, (), "await_data_revalidation")
        return _move(
            record,
            SleeveState.DORMANT,
            observation,
            ("data_revalidation_passed",),
            dormant_since=observation.as_of_date,
            reduced_weeks=0,
            consecutive_multi_alarm_checks=0,
        )

    if record.state in {SleeveState.RETIRED, *PREPRODUCTION_SEQUENCE}:
        return LifecycleDecision(record, None, alarms, "no_automatic_transition")

    multi_alarm = len(alarms) >= 2
    consecutive = record.consecutive_multi_alarm_checks + 1 if multi_alarm else 0

    if record.state == SleeveState.ACTIVE:
        if consecutive >= 2:
            reduced_weight = min(record.effective_weight, record.target_weight * 0.5)
            return _move(
                record,
                SleeveState.REDUCED,
                observation,
                alarms or ("confirmed_health_degradation",),
                effective_weight=max(reduced_weight, 0.0),
                consecutive_multi_alarm_checks=consecutive,
                reduced_weeks=1,
            )
        return LifecycleDecision(
            replace(record, consecutive_multi_alarm_checks=consecutive),
            None,
            alarms,
            "monitor" if alarms else "continue",
        )

    if record.state == SleeveState.REDUCED:
        reduced_weeks = record.reduced_weeks + 1
        if reduced_weeks >= 4 and float(observation.active_ir_26w) < 0:
            return _move(
                record,
                SleeveState.DORMANT,
                observation,
                ("four_weeks_reduced", "negative_26w_information_ratio"),
                effective_weight=0.0,
                reduced_weeks=reduced_weeks,
                dormant_since=observation.as_of_date,
                consecutive_multi_alarm_checks=consecutive,
            )
        if not alarms and float(observation.active_ir_26w) >= 0 and reduced_weeks >= 4:
            return _move(
                record,
                SleeveState.ACTIVE,
                observation,
                ("health_recovered_before_dormancy",),
                effective_weight=min(record.target_weight, record.effective_weight + 0.05),
                reduced_weeks=0,
                consecutive_multi_alarm_checks=0,
            )
        return LifecycleDecision(
            replace(
                record,
                reduced_weeks=reduced_weeks,
                consecutive_multi_alarm_checks=consecutive,
            ),
            None,
            alarms,
            "remain_reduced",
        )

    if record.state == SleeveState.DORMANT:
        recovery_ready = (
            observation.new_sessions_since_dormant >= 60
            and observation.active_return_20d > 0
            and observation.active_return_60d > 0
            and observation.ic_direction_ok
            and not alarms
        )
        if recovery_ready:
            return _move(
                record,
                SleeveState.PROBATION,
                observation,
                ("sixty_new_sessions", "multi_horizon_shadow_recovery"),
                probation_weeks=1,
                effective_weight=min(0.05, record.target_weight),
            )
        return LifecycleDecision(record, None, alarms, "continue_shadow_observation")

    if record.state == SleeveState.PROBATION:
        if len(alarms) >= 2 or observation.active_return_20d <= 0:
            return _move(
                record,
                SleeveState.DORMANT,
                observation,
                alarms or ("probation_return_reversed",),
                effective_weight=0.0,
                probation_weeks=0,
                dormant_since=observation.as_of_date,
            )
        probation_weeks = record.probation_weeks + 1
        ramped_weight = min(record.target_weight, record.effective_weight + 0.05)
        if probation_weeks >= 4 and observation.active_ir_13w >= 0 and observation.active_ir_26w >= 0:
            return _move(
                record,
                SleeveState.ACTIVE,
                observation,
                ("four_clean_probation_checks",),
                effective_weight=ramped_weight,
                probation_weeks=probation_weeks,
                consecutive_multi_alarm_checks=0,
            )
        return LifecycleDecision(
            replace(record, probation_weeks=probation_weeks, effective_weight=ramped_weight),
            None,
            alarms,
            "continue_probation_ramp",
        )

    return LifecycleDecision(record, None, alarms, "no_state_change")


@dataclass(frozen=True)
class RecoveryCaseProgress:
    case_id: str
    opened_session: int
    drift_registered_session: int | None = None
    diagnosis_session: int | None = None
    challenger_session: int | None = None
    shadow_start_session: int | None = None
    current_session: int = 0

    def status(self) -> dict[str, Any]:
        detection_age = None if self.drift_registered_session is None else self.drift_registered_session - self.opened_session
        challenger_age = None if self.challenger_session is None else self.challenger_session - self.opened_session
        shadow_sessions = 0 if self.shadow_start_session is None else max(0, self.current_session - self.shadow_start_session)
        return {
            "case_id": self.case_id,
            "detection_met": detection_age is not None and detection_age <= 5,
            "diagnosis_met": self.diagnosis_session is not None and self.diagnosis_session - self.opened_session <= 20,
            "challenger_met": challenger_age is not None and challenger_age <= 20,
            "shadow_sessions": shadow_sessions,
            "recovery_observation_complete": shadow_sessions >= 60,
            "detection_overdue": self.current_session - self.opened_session > 5 and self.drift_registered_session is None,
            "challenger_overdue": self.current_session - self.opened_session > 20 and self.challenger_session is None,
        }

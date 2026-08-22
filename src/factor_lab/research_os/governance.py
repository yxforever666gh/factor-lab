"""Pure trial-ledger admission and deterministic promotion decisions."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from typing import Any, Iterable, Mapping

from factor_lab.research_os.contracts import TrialOutcome
from factor_lab.research_os.negative_controls import (
    NegativeControlDecision,
    NegativeControlMetric,
    evaluate_negative_control_gate,
)
from factor_lab.research_os.statistics import (
    BlockBootstrapResult,
    DeflatedSharpeResult,
    OnlineAlphaDecision,
    block_bootstrap,
    deflated_sharpe_ratio,
    dependence_adjusted_online_alpha,
    holm_adjust,
)
from factor_lab.research_os.walk_forward import StitchedOuterOOS


class TrialKind(str, Enum):
    EXPLORATORY = "exploratory"
    CONFIRMATORY = "confirmatory"
    DIAGNOSTIC = "diagnostic"
    NEGATIVE_CONTROL = "negative_control"
    REVERSE_DIRECTION = "reverse_direction"
    MANUAL = "manual"


class EvidenceClass(str, Enum):
    PRISTINE_FORWARD = "pristine_forward"
    PSEUDO_OOS = "pseudo_oos"
    OBSERVED = "observed"


# Every historical row was already observable when Research OS v1 froze.  A
# request payload cannot mint a new holdout name or promote it to forward
# evidence; all historical confirmation is recorded under this one durable
# pseudo-OOS window.
HISTORICAL_HOLDOUT_ID = "historical-observed-pseudo-oos-v1"


class TrialAdmissionStatus(str, Enum):
    """Durable decision made before a confirmatory trial is executed."""

    ADMITTED = "admitted"
    REJECTED = "rejected"


@dataclass(frozen=True)
class TrialRegistration:
    trial_id: str
    experiment_fingerprint: str
    hypothesis_id: str
    family: str
    kind: TrialKind
    registered_at: datetime
    holdout_id: str | None = None
    requested_evidence_class: EvidenceClass = EvidenceClass.PSEUDO_OOS
    variant_id: str | None = None
    diagnostic_branch: int | None = None
    research_equivalence_hash: str | None = None

    def __post_init__(self) -> None:
        if self.registered_at.tzinfo is None or self.registered_at.utcoffset() is None:
            raise ValueError("registered_at must be timezone-aware")
        if not isinstance(self.kind, TrialKind):
            object.__setattr__(self, "kind", TrialKind(str(self.kind)))
        if not isinstance(self.requested_evidence_class, EvidenceClass):
            object.__setattr__(
                self,
                "requested_evidence_class",
                EvidenceClass(str(self.requested_evidence_class)),
            )
        for name in ("trial_id", "experiment_fingerprint", "hypothesis_id", "family"):
            if not str(getattr(self, name)).strip():
                raise ValueError(f"{name} must not be empty")
        if self.diagnostic_branch is not None and self.diagnostic_branch < 1:
            raise ValueError("diagnostic_branch must be positive")
        if self.research_equivalence_hash is not None and (
            len(self.research_equivalence_hash) != 64
            or any(ch not in "0123456789abcdef" for ch in self.research_equivalence_hash)
        ):
            raise ValueError("research_equivalence_hash must be a lowercase SHA-256 digest")


@dataclass(frozen=True)
class TrialRecord:
    registration: TrialRegistration
    outcome: TrialOutcome
    completed_at: datetime | None = None
    p_value: float | None = None
    alpha_allocated: float | None = None
    notes: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.outcome, TrialOutcome):
            object.__setattr__(self, "outcome", TrialOutcome(str(self.outcome)))
        if self.p_value is not None and not 0 <= self.p_value <= 1:
            raise ValueError("p_value must be in [0, 1]")
        if self.alpha_allocated is not None and not 0 <= self.alpha_allocated <= 1:
            raise ValueError("alpha_allocated must be in [0, 1]")
        object.__setattr__(self, "metadata", dict(self.metadata))

    @classmethod
    def from_catalog_entry(cls, entry: Mapping[str, Any] | Any) -> "TrialRecord":
        """Adapt a catalog ``TrialLedgerEntry`` without coupling persistence here."""

        def value(name: str, default: Any = None) -> Any:
            if isinstance(entry, Mapping):
                return entry.get(name, default)
            return getattr(entry, name, default)

        metadata = value("metadata", {}) or {}
        if not isinstance(metadata, Mapping):
            raise ValueError("catalog trial metadata must be a mapping")
        metadata = dict(metadata)
        admission_status = value("admission_status")
        if admission_status is not None:
            metadata.setdefault(
                "admission_status",
                getattr(admission_status, "value", str(admission_status)),
            )
        persisted_fingerprint = value("experiment_fingerprint")
        if persisted_fingerprint is not None:
            metadata.setdefault("experiment_fingerprint", persisted_fingerprint)
        equivalence_hash = value("research_equivalence_hash")
        if equivalence_hash is not None:
            metadata.setdefault("research_equivalence_hash", equivalence_hash)
        occurred_at = value("occurred_at")
        if not isinstance(occurred_at, datetime):
            raise ValueError("catalog trial occurred_at must be a datetime")
        kind_text = str(metadata.get("trial_kind") or TrialKind.MANUAL.value)
        requested_text = str(metadata.get("evidence_class") or EvidenceClass.OBSERVED.value)
        registration = TrialRegistration(
            trial_id=str(value("trial_id")),
            experiment_fingerprint=str(
                metadata.get("experiment_fingerprint")
                or value("experiment_id")
                or f"catalog:{value('trial_id')}"
            ),
            hypothesis_id=str(metadata.get("hypothesis_id") or value("candidate_id")),
            family=str(value("family")),
            kind=TrialKind(kind_text),
            registered_at=occurred_at,
            holdout_id=metadata.get("holdout_id"),
            requested_evidence_class=EvidenceClass(requested_text),
            variant_id=metadata.get("variant_id"),
            diagnostic_branch=metadata.get("diagnostic_branch"),
            research_equivalence_hash=metadata.get("research_equivalence_hash"),
        )
        raw_outcome = value("outcome")
        outcome = raw_outcome if isinstance(raw_outcome, TrialOutcome) else TrialOutcome(str(raw_outcome))
        alpha = value("alpha_spent")
        return cls(
            registration=registration,
            outcome=outcome,
            completed_at=value("completed_at", occurred_at),
            p_value=value("p_value"),
            alpha_allocated=None if alpha is None else float(alpha),
            notes=(str(value("reason")),) if value("reason") else (),
            metadata=dict(metadata),
        )


@dataclass(frozen=True)
class TrialAdmissionDecision:
    allowed: bool
    evidence_class: EvidenceClass
    family_trial_index: int
    reasons: tuple[str, ...]


class TrialLedger:
    """Immutable view over catalog records; persistence remains catalog-owned."""

    def __init__(self, records: Iterable[TrialRecord] = ()):  # noqa: D107
        self._records = tuple(
            sorted(
                records,
                key=lambda item: (
                    item.registration.registered_at,
                    item.registration.trial_id,
                ),
            )
        )

    @classmethod
    def from_catalog_entries(cls, entries: Iterable[Mapping[str, Any] | Any]) -> "TrialLedger":
        return cls(TrialRecord.from_catalog_entry(item) for item in entries)

    @property
    def records(self) -> tuple[TrialRecord, ...]:
        return self._records

    def family_records(self, family: str) -> tuple[TrialRecord, ...]:
        return tuple(item for item in self._records if item.registration.family == family)

    @staticmethod
    def was_admitted(record: TrialRecord) -> bool:
        """Treat pre-migration rows as admitted while excluding durable rejections."""

        return str(
            record.metadata.get(
                "admission_status", TrialAdmissionStatus.ADMITTED.value
            )
        ) == TrialAdmissionStatus.ADMITTED.value

    def family_p_values(
        self,
        family: str,
        *,
        confirmatory_only: bool = True,
        include_missing_as_one: bool = True,
    ) -> tuple[float, ...]:
        rows = self.family_records(family)
        return tuple(
            float(item.p_value if item.p_value is not None else 1.0)
            for item in rows
            if (item.p_value is not None or include_missing_as_one)
            and (not confirmatory_only or item.registration.kind is TrialKind.CONFIRMATORY)
            and self.was_admitted(item)
        )

    def admit(
        self,
        registration: TrialRegistration,
        *,
        maximum_monthly_confirmatory_trials: int = 3,
        maximum_monthly_confirmatory_trials_per_family: int = 1,
        maximum_diagnostic_branches: int = 2,
    ) -> TrialAdmissionDecision:
        if maximum_monthly_confirmatory_trials < 1 or maximum_monthly_confirmatory_trials_per_family < 1:
            raise ValueError("monthly confirmation limits must be positive")
        if maximum_diagnostic_branches < 0:
            raise ValueError("maximum_diagnostic_branches must be non-negative")
        reasons: list[str] = []
        admitted_records = tuple(item for item in self._records if self.was_admitted(item))
        if any(item.registration.trial_id == registration.trial_id for item in admitted_records):
            reasons.append("duplicate_trial_id")
        if any(item.registration.experiment_fingerprint == registration.experiment_fingerprint for item in admitted_records):
            reasons.append("duplicate_experiment_fingerprint")
        if registration.research_equivalence_hash and any(
            item.registration.research_equivalence_hash
            == registration.research_equivalence_hash
            for item in admitted_records
        ):
            reasons.append("duplicate_research_equivalence")

        month_key = _month_key(registration.registered_at)
        if registration.kind is TrialKind.CONFIRMATORY:
            monthly = [
                item
                for item in admitted_records
                if item.registration.kind is TrialKind.CONFIRMATORY
                and _month_key(item.registration.registered_at) == month_key
            ]
            if len(monthly) >= maximum_monthly_confirmatory_trials:
                reasons.append("monthly_confirmation_budget_exhausted")
            if sum(item.registration.family == registration.family for item in monthly) >= maximum_monthly_confirmatory_trials_per_family:
                reasons.append("monthly_family_confirmation_budget_exhausted")

        if registration.kind is TrialKind.DIAGNOSTIC:
            prior_branches = {
                item.registration.diagnostic_branch
                for item in admitted_records
                if item.registration.hypothesis_id == registration.hypothesis_id
                and item.registration.kind is TrialKind.DIAGNOSTIC
                and item.registration.diagnostic_branch is not None
            }
            if registration.diagnostic_branch is None:
                reasons.append("diagnostic_branch_missing")
            elif registration.diagnostic_branch > maximum_diagnostic_branches or len(prior_branches) >= maximum_diagnostic_branches:
                reasons.append("diagnostic_branch_budget_exhausted")

        evidence_class = registration.requested_evidence_class
        if evidence_class is EvidenceClass.PRISTINE_FORWARD and not registration.holdout_id:
            evidence_class = EvidenceClass.PSEUDO_OOS
        if registration.holdout_id and any(
            item.registration.holdout_id == registration.holdout_id for item in admitted_records
        ):
            evidence_class = EvidenceClass.OBSERVED
        if registration.kind in {TrialKind.EXPLORATORY, TrialKind.DIAGNOSTIC, TrialKind.NEGATIVE_CONTROL, TrialKind.REVERSE_DIRECTION}:
            evidence_class = EvidenceClass.OBSERVED
        family_index = sum(
            self.was_admitted(item) for item in self.family_records(registration.family)
        ) + 1
        return TrialAdmissionDecision(
            allowed=not reasons,
            evidence_class=evidence_class,
            family_trial_index=family_index,
            reasons=tuple(reasons),
        )


def _month_key(value: datetime | date) -> tuple[int, int]:
    if isinstance(value, datetime) and value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.year, value.month


@dataclass(frozen=True)
class PromotionMetrics:
    net_excess_annual_return: float
    net_sharpe: float
    information_ratio: float
    max_drawdown: float
    positive_half_year_ratio: float
    positive_outer_years: int
    evaluated_outer_years: int
    capacity_violations: int

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "PromotionMetrics":
        return cls(
            net_excess_annual_return=float(payload.get("net_excess_annual_return", payload.get("excess_return", 0.0))),
            net_sharpe=float(payload.get("net_sharpe", 0.0)),
            information_ratio=float(payload.get("information_ratio", payload.get("active_information_ratio", 0.0))),
            max_drawdown=float(payload.get("max_drawdown", 0.0)),
            positive_half_year_ratio=float(payload.get("positive_half_year_ratio", 0.0)),
            positive_outer_years=int(payload.get("positive_outer_years", 0)),
            evaluated_outer_years=int(payload.get("evaluated_outer_years", payload.get("outer_year_count", 0))),
            capacity_violations=int(payload.get("capacity_violations", payload.get("capacity_violation_count", 0))),
        )


@dataclass(frozen=True)
class PromotionEvidence:
    data_audit_blockers: tuple[str, ...] = ()
    methodology_blockers: tuple[str, ...] = ()
    statistical_budget_passed: bool = False
    holm_passed: bool = False
    deflated_sharpe_probability: float = 0.0
    minimum_deflated_sharpe_probability: float = 0.95
    bootstrap_probability_positive: float = 0.0
    minimum_bootstrap_probability_positive: float = 0.90
    negative_controls_passed: bool = False
    required_outer_years: int = 5
    diagnostic_only: bool = False


@dataclass(frozen=True)
class PromotionDecision:
    promoted: bool
    verdict: str
    failures: tuple[str, ...]
    checks: Mapping[str, bool] = field(default_factory=dict)


@dataclass(frozen=True)
class CandidatePromotionAssessment:
    promotion: PromotionDecision
    evidence: PromotionEvidence
    outer_fold_ids: tuple[str, ...]
    block_bootstrap: BlockBootstrapResult | None
    deflated_sharpe: DeflatedSharpeResult | None
    online_alpha: tuple[OnlineAlphaDecision, ...]
    holm_adjusted_p_values: tuple[float, ...]
    negative_controls: NegativeControlDecision


def _criterion(criteria: Mapping[str, Any] | Any, name: str, default: Any) -> Any:
    if isinstance(criteria, Mapping):
        return criteria.get(name, default)
    return getattr(criteria, name, default)


def evaluate_promotion(
    metrics: PromotionMetrics | Mapping[str, Any],
    criteria: Mapping[str, Any] | Any,
    evidence: PromotionEvidence,
) -> PromotionDecision:
    """Apply every promotion gate without rounding or discretionary overrides."""

    if not isinstance(metrics, PromotionMetrics):
        metrics = PromotionMetrics.from_mapping(metrics)
    minimum_excess = float(_criterion(criteria, "minimum_net_excess_annual_return", 0.0))
    strict_excess = bool(_criterion(criteria, "require_strictly_positive_excess", True))
    excess_pass = metrics.net_excess_annual_return > minimum_excess if strict_excess else metrics.net_excess_annual_return >= minimum_excess
    checks = {
        "net_excess_annual_return": excess_pass,
        "net_sharpe": metrics.net_sharpe >= float(_criterion(criteria, "minimum_net_sharpe", 0.8)),
        "information_ratio": metrics.information_ratio >= float(_criterion(criteria, "minimum_information_ratio", 0.5)),
        "max_drawdown": abs(metrics.max_drawdown) <= float(_criterion(criteria, "maximum_absolute_drawdown", 0.25)),
        "positive_half_year_ratio": metrics.positive_half_year_ratio >= float(_criterion(criteria, "minimum_positive_half_year_ratio", 0.60)),
        "positive_outer_years": metrics.positive_outer_years >= int(_criterion(criteria, "minimum_positive_outer_years", 3)),
        "complete_outer_years": metrics.evaluated_outer_years >= evidence.required_outer_years,
        "capacity": metrics.capacity_violations <= int(_criterion(criteria, "maximum_capacity_violations", 0)),
        "clean_data": not evidence.data_audit_blockers,
        "methodology_integrity": not evidence.methodology_blockers,
        "statistical_budget": evidence.statistical_budget_passed,
        "holm_family_test": evidence.holm_passed,
        "deflated_sharpe": evidence.deflated_sharpe_probability >= evidence.minimum_deflated_sharpe_probability,
        "block_bootstrap": evidence.bootstrap_probability_positive >= evidence.minimum_bootstrap_probability_positive,
        "negative_controls": evidence.negative_controls_passed,
        "promotion_window": not evidence.diagnostic_only,
    }
    if not bool(_criterion(criteria, "require_clean_data", True)):
        checks["clean_data"] = True
    if not bool(_criterion(criteria, "require_statistical_budget_pass", True)):
        checks["statistical_budget"] = True
    failures = tuple(name for name, passed in checks.items() if not passed)
    if evidence.diagnostic_only:
        verdict = "diagnostic_only"
    else:
        verdict = "promote" if not failures else "reject"
    return PromotionDecision(promoted=not failures, verdict=verdict, failures=failures, checks=checks)


def assess_candidate_promotion(
    metrics: PromotionMetrics | Mapping[str, Any],
    validation_protocol: Mapping[str, Any] | Any,
    *,
    family: str,
    trial_ledger: TrialLedger,
    candidate_trial_id: str,
    stitched_outer_oos_returns: Iterable[float] | StitchedOuterOOS,
    within_family_p_values: Iterable[float],
    outer_fold_ids: Iterable[str] | None = None,
    candidate_within_family_index: int = -1,
    lifetime_trial_sharpes: Iterable[float] = (),
    negative_control_results: Iterable[NegativeControlMetric | Mapping[str, object]] = (),
    data_audit_blockers: Iterable[str] = (),
    diagnostic_only: bool = False,
    maximum_family_trials: int = 120,
    periods_per_year: float = 252.0 / 5.0,
    bootstrap_block_size: int | None = None,
    bootstrap_resamples: int = 2_000,
    seed: int = 0,
) -> CandidatePromotionAssessment:
    """Compose all selection and promotion evidence into one fail-closed gate.

    The caller must pass the stitched outer-OOS return stream, never an inner
    tuning stream or the 2026 diagnostic window.  Every lifetime family trial
    in ``trial_ledger`` contributes to online alpha and selection deflation.
    """

    if not isinstance(metrics, PromotionMetrics):
        metrics = PromotionMetrics.from_mapping(metrics)
    criteria = _criterion(validation_protocol, "promotion_criteria", {})
    statistical_budget = _criterion(validation_protocol, "statistical_budget", {})
    expected_outer_ids = tuple(
        f"outer-{int(year)}" for year in _criterion(validation_protocol, "outer_test_years", (2021, 2022, 2023, 2024, 2025))
    )
    if isinstance(stitched_outer_oos_returns, StitchedOuterOOS):
        observed_outer_ids = stitched_outer_oos_returns.fold_ids
        oos_returns = tuple(float(item) for item in stitched_outer_oos_returns.returns)
        if outer_fold_ids is not None and tuple(str(item) for item in outer_fold_ids) != observed_outer_ids:
            methodology_blockers = ["stitched_outer_fold_id_mismatch"]
        else:
            methodology_blockers = []
    else:
        observed_outer_ids = tuple(str(item) for item in (outer_fold_ids or ()))
        oos_returns = tuple(float(item) for item in stitched_outer_oos_returns)
        methodology_blockers = []
    if len(set(observed_outer_ids)) != len(observed_outer_ids):
        methodology_blockers.append("duplicate_outer_fold_evidence")
    if observed_outer_ids != expected_outer_ids:
        methodology_blockers.append("incomplete_or_unordered_outer_fold_evidence")
    if metrics.evaluated_outer_years != len(expected_outer_ids):
        methodology_blockers.append("outer_year_metric_count_mismatch")

    family_records = trial_ledger.family_records(family)
    confirmatory_records = tuple(
        item
        for item in family_records
        if item.registration.kind is TrialKind.CONFIRMATORY
        and trial_ledger.was_admitted(item)
    )
    family_p_values = tuple(
        float(item.p_value if item.p_value is not None else 1.0)
        for item in confirmatory_records
    )
    candidate_family_index = next(
        (
            index
            for index, item in enumerate(confirmatory_records)
            if item.registration.trial_id == candidate_trial_id
        ),
        None,
    )
    if candidate_family_index is None:
        methodology_blockers.append("candidate_confirmatory_trial_missing")
    alpha_budget = float(_criterion(statistical_budget, "family_alpha_budget", 0.10))
    online_alpha: tuple[OnlineAlphaDecision, ...] = ()
    if not family_p_values:
        methodology_blockers.append("family_trial_p_values_missing")
    else:
        try:
            online_alpha = dependence_adjusted_online_alpha(
                family_p_values,
                family_alpha_budget=alpha_budget,
                maximum_family_trials=maximum_family_trials,
            )
        except ValueError:
            methodology_blockers.append("family_alpha_budget_or_trial_limit_invalid")
    statistical_budget_passed = bool(
        online_alpha
        and candidate_family_index is not None
        and online_alpha[candidate_family_index].rejected
    )

    raw_within_p_values = tuple(float(item) for item in within_family_p_values)
    adjusted_p_values: tuple[float, ...] = ()
    holm_passed = False
    if not raw_within_p_values:
        methodology_blockers.append("within_family_p_values_missing")
    else:
        try:
            adjusted_p_values = holm_adjust(raw_within_p_values)
            resolved_index = candidate_within_family_index
            if resolved_index < 0:
                resolved_index += len(adjusted_p_values)
            if not 0 <= resolved_index < len(adjusted_p_values):
                methodology_blockers.append("candidate_p_value_index_invalid")
            else:
                candidate_alpha = (
                    online_alpha[candidate_family_index].alpha
                    if online_alpha and candidate_family_index is not None
                    else 0.0
                )
                holm_passed = adjusted_p_values[resolved_index] <= candidate_alpha
        except ValueError:
            methodology_blockers.append("within_family_p_values_invalid")

    bootstrap_result: BlockBootstrapResult | None = None
    deflated_result: DeflatedSharpeResult | None = None
    try:
        bootstrap_result = block_bootstrap(
            oos_returns,
            statistic="annualized_mean",
            periods_per_year=periods_per_year,
            block_size=bootstrap_block_size,
            resamples=bootstrap_resamples,
            seed=seed,
        )
    except ValueError:
        methodology_blockers.append("outer_oos_bootstrap_unavailable")
    try:
        deflated_result = deflated_sharpe_ratio(
            oos_returns,
            number_of_trials=max(len(family_records), 1),
            trial_sharpes=tuple(lifetime_trial_sharpes),
            periods_per_year=periods_per_year,
        )
    except ValueError:
        methodology_blockers.append("deflated_sharpe_unavailable")

    control_decision = evaluate_negative_control_gate(negative_control_results)
    evidence = PromotionEvidence(
        data_audit_blockers=tuple(str(item) for item in data_audit_blockers),
        methodology_blockers=tuple(dict.fromkeys(methodology_blockers)),
        statistical_budget_passed=statistical_budget_passed,
        holm_passed=holm_passed,
        deflated_sharpe_probability=(
            deflated_result.deflated_sharpe_probability if deflated_result else 0.0
        ),
        bootstrap_probability_positive=(
            bootstrap_result.probability_positive if bootstrap_result else 0.0
        ),
        negative_controls_passed=control_decision.passed,
        required_outer_years=len(expected_outer_ids),
        diagnostic_only=diagnostic_only,
    )
    promotion = evaluate_promotion(metrics, criteria, evidence)
    return CandidatePromotionAssessment(
        promotion=promotion,
        evidence=evidence,
        outer_fold_ids=observed_outer_ids,
        block_bootstrap=bootstrap_result,
        deflated_sharpe=deflated_result,
        online_alpha=online_alpha,
        holm_adjusted_p_values=adjusted_p_values,
        negative_controls=control_decision,
    )

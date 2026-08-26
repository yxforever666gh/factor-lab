"""Versioned, immutable contracts for the Factor Lab Research OS.

The contracts in this module are deliberately independent from persistence and
orchestration.  They are the stable boundary shared by data ingestion,
research methodology, portfolio simulation, monitoring, and the Web UI.
"""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


SCHEMA_VERSION = "research-os/v1"
PHYSICAL_CANARY_SNAPSHOT_REFERENCE_SCHEMA = (
    "research-os/physical-canary-snapshot-reference/v1"
)
Sha256Digest = str


class ContractModel(BaseModel):
    """Base class for public contracts.

    Unknown fields are rejected so that a misspelled research parameter cannot
    silently disappear from an experiment fingerprint.
    """

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        validate_default=True,
    )

    schema_version: Literal["research-os/v1"] = SCHEMA_VERSION


class SnapshotTier(str, Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class DataQualityStatus(str, Enum):
    ACCEPTED = "accepted"
    DISPUTED = "disputed"
    QUARANTINED = "quarantined"
    FROZEN = "frozen"


class FactorDirection(str, Enum):
    HIGHER_IS_BETTER = "higher_is_better"
    LOWER_IS_BETTER = "lower_is_better"
    TRAIN_FROZEN = "train_frozen"


class HypothesisDirection(str, Enum):
    POSITIVE = "positive"
    NEGATIVE = "negative"
    MIXED = "mixed"
    REGIME_CONDITIONAL = "regime_conditional"


class CandidateKind(str, Enum):
    FACTOR = "factor"
    SLEEVE = "sleeve"


class ExperimentStatus(str, Enum):
    PREREGISTERED = "preregistered"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    BLOCKED = "blocked"


class TrialOutcome(str, Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    MISSING_DATA = "missing_data"
    REVERSE_TEST = "reverse_test"
    MANUAL = "manual"
    REJECTED = "rejected"


class LifecycleState(str, Enum):
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


class RecoveryCaseStatus(str, Enum):
    OPEN = "open"
    DIAGNOSING = "diagnosing"
    OBSERVING = "observing"
    RECOVERED = "recovered"
    CLOSED = "closed"


class DataSnapshotRef(ContractModel):
    snapshot_id: str = Field(min_length=1, max_length=160)
    tier: SnapshotTier
    uri: str = Field(min_length=1)
    content_hash: Sha256Digest = Field(pattern=r"^[0-9a-f]{64}$")
    parent_snapshot_ids: tuple[str, ...] = ()
    as_of: datetime
    quality_status: DataQualityStatus = DataQualityStatus.ACCEPTED
    trust_labels: tuple[str, ...] = ()
    manifest: dict[str, Any] = Field(default_factory=dict)

    @field_validator("as_of")
    @classmethod
    def _require_aware_as_of(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("as_of must include a timezone")
        return value


class UniverseSpec(ContractModel):
    mode: Literal["monthly_liquid_top_n"] = "monthly_liquid_top_n"
    market: Literal["a_share"] = "a_share"
    target_size: int = Field(default=500, ge=1)
    liquidity_lookback_sessions: int = Field(default=60, ge=1)
    minimum_liquidity_observations: int = Field(default=40, ge=1)
    minimum_listing_days: int = Field(default=180, ge=0)
    membership_lag_months: int = Field(default=1, ge=1)
    st_policy: Literal["exclude_historical", "unverified_block"] = "exclude_historical"
    ordinary_a_share_only: bool = True
    include_inactive_securities: bool = True
    point_in_time: bool = True

    @model_validator(mode="after")
    def _validate_liquidity_observations(self) -> "UniverseSpec":
        if self.minimum_liquidity_observations > self.liquidity_lookback_sessions:
            raise ValueError(
                "minimum_liquidity_observations cannot exceed liquidity_lookback_sessions"
            )
        return self


class LabelSpec(ContractModel):
    label_id: str = "open_t_plus_1_to_t_plus_6"
    kind: Literal["forward_open_to_open"] = "forward_open_to_open"
    entry_delay_sessions: int = Field(default=1, ge=1)
    horizon_sessions: int = Field(default=5, ge=1)
    price_adjustment: Literal["post", "pre", "raw"] = "post"
    diagnostic_only: bool = False


class FeatureSpec(ContractModel):
    feature_id: str = Field(min_length=1, max_length=160)
    dtype: Literal["float64", "float32", "int64", "bool", "string", "date", "datetime"]
    expression: str | dict[str, Any]
    required_columns: tuple[str, ...] = ()
    availability_lag_sessions: int = Field(default=0, ge=0)
    point_in_time_required: bool = True
    description: str = ""


class SignalFieldSpec(ContractModel):
    """Fingerprint-bound PIT registry for a factor or Sleeve DSL input."""

    name: str = Field(min_length=1, max_length=160)
    value_type: Literal["numeric", "categorical", "boolean", "datetime"] = "numeric"
    role: Literal["feature", "label", "execution", "identifier"] = "feature"
    # Keep the complete DSL availability vocabulary in the fingerprint-bound
    # registry.  In particular, collapsing ``open`` into ``pre_open`` (or
    # ``next_session`` into ``post_close``) would let two different PIT
    # meanings share an experiment identity.
    availability: Literal[
        "pre_open", "open", "intraday", "close", "post_close", "next_session"
    ] = "close"
    minimum_lag_sessions: int = Field(default=0, ge=0)
    available_at_column: str | None = None


class FactorSpec(ContractModel):
    factor_id: str = Field(min_length=1, max_length=160)
    family: str = Field(min_length=1, max_length=160)
    name: str = Field(min_length=1, max_length=240)
    mechanism: str = Field(min_length=1)
    expression: str | dict[str, Any]
    # Runtime field registries are forbidden from changing the meaning of a
    # registered factor: PIT availability is part of the experiment identity.
    signal_field_registry: tuple[SignalFieldSpec, ...] = ()
    direction: FactorDirection
    allow_in_long_only: bool = True
    expected_regimes: tuple[str, ...] = ()
    falsification_criteria: tuple[str, ...] = Field(min_length=1)
    allowed_variants: tuple[str, ...] = ()
    data_requirements: tuple[str, ...] = ()

    @model_validator(mode="after")
    def _unique_signal_fields(self) -> "FactorSpec":
        names = [item.name for item in self.signal_field_registry]
        if len(names) != len(set(names)):
            raise ValueError("signal_field_registry names must be unique")
        return self


class SleeveSpec(ContractModel):
    sleeve_id: str = Field(min_length=1, max_length=160)
    name: str = Field(min_length=1, max_length=240)
    mechanism: str = Field(min_length=1)
    factor_ids: tuple[str, ...] = Field(min_length=1)
    # The aggregate signal is part of the experiment fingerprint.  Historical
    # promotion code accepts only this typed DSL; runtime column names/Series
    # are legacy diagnostics and cannot bypass PIT validation.
    signal_expression: dict[str, Any] | None = None
    signal_field_registry: tuple[SignalFieldSpec, ...] = ()
    cluster_id: str | None = None
    maximum_weight: float = Field(default=0.35, gt=0.0, le=1.0)
    long_only: bool = True
    expected_regimes: tuple[str, ...] = ()
    falsification_criteria: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _unique_signal_fields(self) -> "SleeveSpec":
        names = [item.name for item in self.signal_field_registry]
        if len(names) != len(set(names)):
            raise ValueError("signal_field_registry names must be unique")
        return self


class PortfolioPolicy(ContractModel):
    mode: Literal["long_only"] = "long_only"
    capital: float = Field(default=50_000_000.0, gt=0.0)
    rebalance_sessions: int = Field(default=5, ge=1)
    target_position_count: int = Field(default=50, ge=1)
    minimum_position_count: int = Field(default=50, ge=1)
    maximum_position_count: int = Field(default=100, ge=1)
    maximum_stock_weight: float = Field(default=0.02, gt=0.0, le=1.0)
    maximum_adv_participation: float = Field(default=0.05, gt=0.0, le=1.0)
    benchmark: Literal["eligible_universe_equal_weight"] = "eligible_universe_equal_weight"
    industry_active_weight_limit: float = Field(default=0.05, ge=0.0, le=0.05)
    size_active_weight_limit: float = Field(default=0.05, ge=0.0, le=0.05)
    minimum_beta: float = Field(default=0.9, ge=0.9, le=1.1)
    maximum_beta: float = Field(default=1.1, ge=0.9, le=1.1)
    covariance_estimator: Literal["ledoit_wolf"] = "ledoit_wolf"
    blocked_buy_policy: Literal["leave_cash"] = "leave_cash"
    blocked_sell_policy: Literal["retain_position"] = "retain_position"

    @model_validator(mode="after")
    def _validate_position_and_beta_bounds(self) -> "PortfolioPolicy":
        if not (
            self.minimum_position_count
            <= self.target_position_count
            <= self.maximum_position_count
        ):
            raise ValueError(
                "position counts must satisfy minimum <= target <= maximum"
            )
        if self.minimum_beta > self.maximum_beta:
            raise ValueError("minimum_beta cannot exceed maximum_beta")
        if self.target_position_count * self.maximum_stock_weight < 1.0 - 1e-12:
            raise ValueError(
                "target positions and maximum_stock_weight cannot deploy full capital"
            )
        return self


class PromotionCriteria(ContractModel):
    minimum_net_excess_annual_return: float = 0.0
    require_strictly_positive_excess: bool = True
    minimum_net_sharpe: float = 0.8
    minimum_information_ratio: float = 0.5
    maximum_absolute_drawdown: float = Field(default=0.25, ge=0.0, le=1.0)
    minimum_positive_half_year_ratio: float = Field(default=0.60, ge=0.0, le=1.0)
    minimum_positive_outer_years: int = Field(default=3, ge=1)
    maximum_capacity_violations: int = Field(default=0, ge=0)
    require_clean_data: bool = True
    require_statistical_budget_pass: bool = True

    @model_validator(mode="after")
    def _cannot_loosen_frozen_promotion_gate(self) -> "PromotionCriteria":
        weaker = (
            not self.require_strictly_positive_excess
            or self.minimum_net_excess_annual_return < 0.0
            or self.minimum_net_sharpe < 0.8
            or self.minimum_information_ratio < 0.5
            or self.maximum_absolute_drawdown > 0.25
            or self.minimum_positive_half_year_ratio < 0.60
            or self.minimum_positive_outer_years < 3
            or self.maximum_capacity_violations != 0
            or not self.require_clean_data
            or not self.require_statistical_budget_pass
        )
        if weaker:
            raise ValueError("promotion criteria cannot loosen the frozen Research OS gate")
        return self


class StatisticalBudget(ContractModel):
    family_alpha_budget: float = Field(default=0.10, gt=0.0, le=1.0)
    family_method: Literal["online_fdr_dependence_adjusted"] = (
        "online_fdr_dependence_adjusted"
    )
    within_family_method: Literal["holm"] = "holm"
    maximum_confirmatory_challengers_per_month: int = Field(default=3, ge=1)
    maximum_confirmatory_challengers_per_family_per_month: int = Field(
        default=1, ge=1
    )
    maximum_diagnostic_branches: int = Field(default=2, ge=0)

    @model_validator(mode="after")
    def _cannot_expand_frozen_trial_budget(self) -> "StatisticalBudget":
        if (
            self.family_alpha_budget > 0.10
            or self.maximum_confirmatory_challengers_per_month > 3
            or self.maximum_confirmatory_challengers_per_family_per_month > 1
            or self.maximum_diagnostic_branches > 2
        ):
            raise ValueError("statistical budget cannot exceed the frozen Research OS budget")
        return self


class ValidationProtocol(ContractModel):
    protocol_id: str = "expanding_nested_walk_forward_v1"
    initial_train_start: date = date(2017, 1, 1)
    initial_train_end: date = date(2020, 12, 31)
    outer_test_years: tuple[int, ...] = (2021, 2022, 2023, 2024, 2025)
    diagnostic_years: tuple[int, ...] = (2026,)
    purge_sessions: int = Field(default=6, ge=0)
    embargo_sessions: int = Field(default=5, ge=0)
    annualization_sessions: int = Field(default=252, ge=1)
    promotion_criteria: PromotionCriteria = Field(default_factory=PromotionCriteria)
    statistical_budget: StatisticalBudget = Field(default_factory=StatisticalBudget)

    @model_validator(mode="after")
    def _validate_windows(self) -> "ValidationProtocol":
        if self.initial_train_start > self.initial_train_end:
            raise ValueError("initial_train_start cannot be after initial_train_end")
        if not self.outer_test_years:
            raise ValueError("outer_test_years cannot be empty")
        if tuple(sorted(set(self.outer_test_years))) != self.outer_test_years:
            raise ValueError("outer_test_years must be unique and strictly increasing")
        if set(self.outer_test_years) & set(self.diagnostic_years):
            raise ValueError("outer_test_years and diagnostic_years cannot overlap")
        frozen = (
            self.protocol_id == "expanding_nested_walk_forward_v1"
            and self.initial_train_start == date(2017, 1, 1)
            and self.initial_train_end == date(2020, 12, 31)
            and self.outer_test_years == (2021, 2022, 2023, 2024, 2025)
            and self.diagnostic_years == (2026,)
            and self.purge_sessions == 6
            and self.embargo_sessions == 5
            and self.annualization_sessions == 252
        )
        if not frozen:
            raise ValueError(
                "expanding_nested_walk_forward_v1 boundaries are frozen; register a new protocol version instead"
            )
        return self


class EnvironmentRef(ContractModel):
    code_hash: Sha256Digest = Field(pattern=r"^[0-9a-f]{64}$")
    dependency_lock_hash: Sha256Digest = Field(pattern=r"^[0-9a-f]{64}$")
    configuration_hash: Sha256Digest = Field(pattern=r"^[0-9a-f]{64}$")
    dirty_patch_hash: Sha256Digest | None = Field(
        default=None, pattern=r"^[0-9a-f]{64}$"
    )
    python_version: str = Field(min_length=1)
    platform: str = Field(min_length=1)
    evaluator_build: str = Field(min_length=1)


class EvaluationInputBindings(ContractModel):
    """Bindings for every caller-supplied value that can change a result.

    Empty optional evidence is represented by ``None``.  Once evidence is
    supplied at runtime its canonical content hash must be preregistered here,
    making the experiment fingerprint independent of call order.  Bootstrap
    settings are scalar protocol inputs, so they are stored directly instead
    of relying on function defaults.
    """

    exposure_frame_hash: Sha256Digest | None = Field(
        default=None, pattern=r"^[0-9a-f]{64}$"
    )
    returns_history_hash: Sha256Digest | None = Field(
        default=None, pattern=r"^[0-9a-f]{64}$"
    )
    benchmark_weights_hash: Sha256Digest | None = Field(
        default=None, pattern=r"^[0-9a-f]{64}$"
    )
    optimization_policy_hash: Sha256Digest | None = Field(
        default=None, pattern=r"^[0-9a-f]{64}$"
    )
    negative_controls_hash: Sha256Digest | None = Field(
        default=None, pattern=r"^[0-9a-f]{64}$"
    )
    within_family_p_values_hash: Sha256Digest | None = Field(
        default=None, pattern=r"^[0-9a-f]{64}$"
    )
    data_audit_blockers_hash: Sha256Digest | None = Field(
        default=None, pattern=r"^[0-9a-f]{64}$"
    )
    bootstrap_resamples: int = Field(default=2_000, ge=100)
    bootstrap_seed: int = Field(default=0, ge=0)


class Preregistration(ContractModel):
    hypothesis_id: str = Field(min_length=1, max_length=160)
    economic_mechanism: str = Field(min_length=1)
    direction: HypothesisDirection
    expected_regimes: tuple[str, ...] = ()
    falsification_criteria: tuple[str, ...] = Field(min_length=1)
    allowed_variants: tuple[str, ...] = ()
    stop_rules: tuple[str, ...] = Field(min_length=1)
    statistical_budget: StatisticalBudget = Field(default_factory=StatisticalBudget)


class ExperimentSpec(ContractModel):
    snapshot: DataSnapshotRef
    universe: UniverseSpec = Field(default_factory=UniverseSpec)
    label: LabelSpec = Field(default_factory=LabelSpec)
    features: tuple[FeatureSpec, ...] = ()
    factor: FactorSpec | None = None
    sleeve: SleeveSpec | None = None
    portfolio: PortfolioPolicy = Field(default_factory=PortfolioPolicy)
    validation: ValidationProtocol = Field(default_factory=ValidationProtocol)
    evaluator_version: str = Field(min_length=1)
    environment: EnvironmentRef
    evaluation_inputs: EvaluationInputBindings = Field(
        default_factory=EvaluationInputBindings
    )
    preregistration: Preregistration

    @model_validator(mode="after")
    def _require_exactly_one_candidate(self) -> "ExperimentSpec":
        if (self.factor is None) == (self.sleeve is None):
            raise ValueError("exactly one of factor or sleeve must be supplied")
        return self

    @property
    def candidate_kind(self) -> CandidateKind:
        return CandidateKind.FACTOR if self.factor is not None else CandidateKind.SLEEVE

    @property
    def candidate_id(self) -> str:
        if self.factor is not None:
            return self.factor.factor_id
        assert self.sleeve is not None
        return self.sleeve.sleeve_id

    @property
    def family(self) -> str:
        if self.factor is not None:
            return self.factor.family
        assert self.sleeve is not None
        return self.sleeve.sleeve_id

    def fingerprint(self) -> str:
        from .fingerprint import experiment_fingerprint

        return experiment_fingerprint(self)


class RecoveryCase(ContractModel):
    recovery_case_id: str = Field(min_length=1, max_length=160)
    sleeve_id: str = Field(min_length=1, max_length=160)
    status: RecoveryCaseStatus = RecoveryCaseStatus.OPEN
    lifecycle_state: LifecycleState
    triggered_at: datetime
    drift_event_due_at: datetime
    diagnosis_due_at: datetime
    earliest_recovery_review_at: datetime
    trigger_evidence: dict[str, Any] = Field(default_factory=dict)
    challenger_ids: tuple[str, ...] = Field(default=(), max_length=3)
    data_integrity_failure: bool = False
    # Optimistic concurrency token for the mutable read projection. Lifecycle
    # events remain the evidence authority; this prevents a stale coordinator
    # snapshot from overwriting a newer projection.
    projection_version: int = Field(default=0, ge=0)

    @field_validator(
        "triggered_at",
        "drift_event_due_at",
        "diagnosis_due_at",
        "earliest_recovery_review_at",
    )
    @classmethod
    def _require_aware_timestamps(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("recovery timestamps must include a timezone")
        return value

    @model_validator(mode="after")
    def _validate_sla_order(self) -> "RecoveryCase":
        if not (
            self.triggered_at
            <= self.drift_event_due_at
            <= self.diagnosis_due_at
            <= self.earliest_recovery_review_at
        ):
            raise ValueError("recovery SLA timestamps must be chronological")
        if self.data_integrity_failure and self.lifecycle_state != LifecycleState.FROZEN_DATA:
            raise ValueError(
                "data_integrity_failure recovery cases must use frozen_data lifecycle state"
            )
        return self


__all__ = [
    "SCHEMA_VERSION",
    "CandidateKind",
    "ContractModel",
    "DataQualityStatus",
    "DataSnapshotRef",
    "EnvironmentRef",
    "ExperimentSpec",
    "ExperimentStatus",
    "FactorDirection",
    "FactorSpec",
    "FeatureSpec",
    "HypothesisDirection",
    "LabelSpec",
    "LifecycleState",
    "PortfolioPolicy",
    "Preregistration",
    "PromotionCriteria",
    "RecoveryCase",
    "RecoveryCaseStatus",
    "SignalFieldSpec",
    "SleeveSpec",
    "SnapshotTier",
    "StatisticalBudget",
    "TrialOutcome",
    "UniverseSpec",
    "ValidationProtocol",
]

"""Authoritative monthly discovery, admission, execution, and promotion.

Production callers provide only an untrusted hypothesis/Factor DSL and a
fixed family identifier.  Snapshot selection, portfolio/validation policy,
runtime environment, evaluation inputs, trial budget, and lifecycle authority
are resolved server-side from durable Research OS state.
"""

from __future__ import annotations

import copy
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Event, Lock, Thread
from typing import Any, Callable, Literal, Mapping, Protocol, Sequence

import numpy as np
import pandas as pd

from .catalog import (
    CatalogConflict,
    CatalogNotFound,
    ResearchCatalog,
    ResearchFamilyRecord,
    ResearchSubmissionRecord,
    RunRecord,
    TERMINAL_SUBMISSION_STATUSES,
    research_submission_lease_token,
)
from .contracts import (
    DataQualityStatus,
    DataSnapshotRef,
    EnvironmentRef,
    EvaluationInputBindings,
    ExperimentSpec,
    FactorDirection,
    FactorSpec,
    FeatureSpec,
    HypothesisDirection,
    LabelSpec,
    PortfolioPolicy,
    Preregistration,
    SignalFieldSpec,
    SleeveSpec,
    SnapshotTier,
    TrialOutcome,
    UniverseSpec,
    ValidationProtocol,
)
from .cycle import (
    HistoricalResearchCycle,
    ResearchCycleResult,
    evaluation_input_hash,
)
from .dsl import Availability, FieldRole, FieldSpec, ValueType, compile_factor_graph
from .evaluator import CANONICAL_EVALUATOR_VERSION
from .fingerprint import canonical_json, content_fingerprint
from .gold_panel import GoldPanelError, load_gold_research_panel
from .governance import (
    EvidenceClass,
    HISTORICAL_HOLDOUT_ID,
    TrialKind,
    TrialRegistration,
)
from .proposals import (
    HypothesisProposalPort,
    ProposalDecision,
    persist_proposal_decision,
    review_llm_proposal,
)
from .snapshots import SnapshotIntegrityError


_BLOCKING_TRUST_LABELS = frozenset(
    {
        "st_history_unverified",
        "legacy_untrusted_data",
        "legacy_execution_regression_only",
        "disputed",
        "quarantined",
    }
)
_COMMUTATIVE_BINARY_OPS = frozenset({"add", "multiply", "equal", "and", "or"})
_SUBMISSION_DOMAIN = "factor-lab/research-os/v1/monthly-submission"
_EQUIVALENCE_DOMAIN = "factor-lab/research-os/v1/research-equivalence"


class MonthlyResearchError(RuntimeError):
    """Base class for fail-closed coordinator errors."""


class AuthoritativeResearchInputUnavailable(MonthlyResearchError):
    """No accepted, verifiable Gold input can bind a production submission."""


class ResearchSubmissionLeaseLost(MonthlyResearchError):
    """The current worker no longer owns the claimed submission generation."""


class ResearchCyclePort(Protocol):
    def run(self, spec: ExperimentSpec, frame: pd.DataFrame, **kwargs: Any) -> Any: ...


@dataclass(frozen=True)
class AuthoritativeResearchInputs:
    snapshot: DataSnapshotRef
    frame: pd.DataFrame
    exposure_frame: pd.DataFrame
    returns_history: pd.DataFrame
    benchmark_weights: pd.DataFrame

    def __post_init__(self) -> None:
        if (
            self.snapshot.tier is not SnapshotTier.GOLD
            or self.snapshot.quality_status is not DataQualityStatus.ACCEPTED
            or self.snapshot.snapshot_id != self.snapshot.content_hash
            or _BLOCKING_TRUST_LABELS.intersection(self.snapshot.trust_labels)
        ):
            raise ValueError("authoritative inputs require a clean content-addressed Gold snapshot")
        for name in ("frame", "exposure_frame", "returns_history", "benchmark_weights"):
            if not isinstance(getattr(self, name), pd.DataFrame):
                raise TypeError(f"{name} must be a pandas DataFrame")

    def bindings(self, *, bootstrap_resamples: int, bootstrap_seed: int) -> EvaluationInputBindings:
        return EvaluationInputBindings(
            exposure_frame_hash=evaluation_input_hash(
                "exposure_frame", self.exposure_frame
            ),
            returns_history_hash=evaluation_input_hash(
                "returns_history", self.returns_history
            ),
            benchmark_weights_hash=evaluation_input_hash(
                "benchmark_weights", self.benchmark_weights
            ),
            bootstrap_resamples=bootstrap_resamples,
            bootstrap_seed=bootstrap_seed,
        )


@dataclass(frozen=True)
class ProposalAdmission:
    decision: ProposalDecision
    submission: ResearchSubmissionRecord | None
    violations: tuple[str, ...] = ()

    @property
    def accepted(self) -> bool:
        return self.submission is not None and not self.violations


@dataclass(frozen=True)
class SubmissionExecution:
    submission: ResearchSubmissionRecord
    claimed: bool
    result: ResearchCycleResult | None = None
    shadow_account_id: str | None = None


class _SubmissionLeaseGuard:
    """Keep one submission lease alive and fence all terminal side effects."""

    def __init__(
        self,
        catalog: ResearchCatalog,
        submission: ResearchSubmissionRecord,
        *,
        worker_id: str,
        lease_seconds: int,
    ) -> None:
        self.catalog = catalog
        self.submission_id = submission.submission_id
        self.worker_id = worker_id
        self.lease_token = research_submission_lease_token(submission)
        self.lease_for = timedelta(seconds=lease_seconds)
        self.interval_seconds = min(float(lease_seconds) / 3.0, 30.0)
        self._stop = Event()
        self._renew_lock = Lock()
        self._state_lock = Lock()
        self._failure: Exception | None = None
        self._thread = Thread(
            target=self._heartbeat,
            name=f"research-lease-{submission.submission_id[:16]}",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        self._thread.join()

    def _remember_failure(self, exc: Exception) -> None:
        with self._state_lock:
            if self._failure is None:
                self._failure = exc
        self._stop.set()

    def _raise_if_lost(self) -> None:
        with self._state_lock:
            failure = self._failure
        if failure is not None:
            raise ResearchSubmissionLeaseLost(
                "research submission lease renewal failed"
            ) from failure

    def _renew_once(self) -> None:
        with self._renew_lock:
            self._raise_if_lost()
            renewed_at = self.catalog.database_now()
            try:
                self.catalog.renew_research_submission(
                    self.submission_id,
                    worker_id=self.worker_id,
                    lease_token=self.lease_token,
                    renewed_at=renewed_at,
                    lease_expires_at=renewed_at + self.lease_for,
                )
            except Exception as exc:
                self._remember_failure(exc)
                raise ResearchSubmissionLeaseLost(
                    "research submission lease renewal failed"
                ) from exc

    def _heartbeat(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            try:
                self._renew_once()
            except ResearchSubmissionLeaseLost:
                return

    def assert_owned(self) -> None:
        """Synchronously renew before any terminal or promotion side effect."""

        self._renew_once()

    def finish(
        self,
        *,
        status: str,
        experiment_id: str | None,
        error: str | None,
        finished_at: datetime | None = None,
    ) -> ResearchSubmissionRecord:
        self.assert_owned()
        try:
            return self.catalog.finish_research_submission(
                self.submission_id,
                worker_id=self.worker_id,
                lease_token=self.lease_token,
                status=status,
                experiment_id=experiment_id,
                error=error,
                finished_at=finished_at,
            )
        except CatalogConflict as exc:
            self._remember_failure(exc)
            raise ResearchSubmissionLeaseLost(
                "research submission terminal write lost its lease"
            ) from exc


def _pick_column(frame: pd.DataFrame, *names: str) -> str:
    for name in names:
        if name in frame.columns:
            return name
    raise AuthoritativeResearchInputUnavailable(
        f"authoritative Gold is missing all aliases: {', '.join(names)}"
    )


def derive_authoritative_evaluation_inputs(
    frame: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Derive all risk inputs from Gold without caller-supplied files/statistics."""

    date_col = _pick_column(frame, "date", "trade_date")
    ticker_col = _pick_column(frame, "ticker", "ts_code", "symbol")
    required = {
        "close_adj",
        "industry",
        "log_market_cap",
        "adv_20",
        "benchmark_weight",
        "decision_cutoff",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise AuthoritativeResearchInputUnavailable(
            "Gold cannot derive production risk inputs; missing " + ",".join(missing)
        )
    source = frame.copy()
    source[date_col] = pd.to_datetime(source[date_col], errors="raise").dt.tz_localize(None).dt.normalize()
    source[ticker_col] = source[ticker_col].astype(str)
    source = source.sort_values([ticker_col, date_col], kind="stable")
    source["_daily_return"] = source.groupby(ticker_col, sort=False)[
        "close_adj"
    ].pct_change(fill_method=None)
    member = (
        source["universe_member"].astype("boolean").fillna(False)
        if "universe_member" in source
        else pd.Series(True, index=source.index, dtype="boolean")
    )
    market_return = (
        source.loc[member]
        .groupby(date_col, sort=False)["_daily_return"]
        .mean()
        .rename("_market_return")
    )
    source = source.join(market_return, on=date_col)
    source["_beta"] = np.nan
    for _, rows in source.groupby(ticker_col, sort=False):
        returns = pd.to_numeric(rows["_daily_return"], errors="coerce")
        market = pd.to_numeric(rows["_market_return"], errors="coerce")
        variance = market.rolling(60, min_periods=20).var(ddof=1)
        beta = returns.rolling(60, min_periods=20).cov(market) / variance.where(
            variance.gt(0)
        )
        source.loc[rows.index, "_beta"] = beta.to_numpy()
    size_rank = source.groupby(date_col, sort=False)["log_market_cap"].rank(
        method="first", pct=True
    )
    source["_size_bucket"] = (
        np.ceil(size_rank * 5.0).clip(1, 5).astype("Int64").astype("string")
    )
    availability_columns = [
        name
        for name in (
            "industry_available_at",
            "size_available_at",
            "daily_available_at",
            "decision_cutoff",
        )
        if name in source.columns
    ]
    availability = pd.concat(
        [pd.to_datetime(source[name], errors="coerce", utc=True) for name in availability_columns],
        axis=1,
    ).max(axis=1)
    industry_available = pd.to_datetime(
        source.get("industry_available_at", source["decision_cutoff"]),
        errors="coerce",
        utc=True,
    )
    cutoff = pd.to_datetime(source["decision_cutoff"], errors="coerce", utc=True)
    exposure = pd.DataFrame(
        {
            "date": source[date_col],
            "ticker": source[ticker_col],
            "industry": source["industry"].astype("string"),
            "size_bucket": source["_size_bucket"],
            "beta": pd.to_numeric(source["_beta"], errors="coerce"),
            "adv_20": pd.to_numeric(source["adv_20"], errors="coerce"),
            "industry_is_pit": industry_available.notna()
            & cutoff.notna()
            & industry_available.le(cutoff),
            "available_at": availability,
        }
    )
    exposure = exposure.loc[
        member.to_numpy()
        & exposure[["industry", "size_bucket", "beta", "adv_20", "available_at"]]
        .notna()
        .all(axis=1)
        & exposure["adv_20"].gt(0)
        & exposure["industry_is_pit"]
    ].reset_index(drop=True)
    if exposure.empty:
        raise AuthoritativeResearchInputUnavailable(
            "Gold produced no complete PIT risk exposure rows"
        )
    returns_history = pd.DataFrame(
        {
            "date": source[date_col],
            "ticker": source[ticker_col],
            "return": pd.to_numeric(source["_daily_return"], errors="coerce"),
        }
    ).dropna(subset=["return"])
    benchmark = pd.DataFrame(
        {
            "date": source[date_col],
            "ticker": source[ticker_col],
            "benchmark_weight": pd.to_numeric(
                source["benchmark_weight"], errors="coerce"
            ),
            "available_at": cutoff,
        }
    )
    benchmark = benchmark.loc[
        member.to_numpy()
        & benchmark["benchmark_weight"].gt(0)
        & benchmark["available_at"].notna()
    ].reset_index(drop=True)
    if returns_history.empty or benchmark.empty:
        raise AuthoritativeResearchInputUnavailable(
            "Gold produced empty returns or benchmark evidence"
        )
    return exposure, returns_history.reset_index(drop=True), benchmark


def _field_specs(family: ResearchFamilyRecord) -> tuple[FieldSpec, ...]:
    result: list[FieldSpec] = []
    for row in family.field_registry:
        if str(row.get("role", "feature")) != "feature":
            raise MonthlyResearchError("family registries may expose only feature fields")
        result.append(
            FieldSpec(
                name=str(row["name"]),
                value_type=ValueType(str(row.get("value_type", "numeric"))),
                role=FieldRole.FEATURE,
                availability=Availability(str(row.get("availability", "close"))),
                minimum_lag_sessions=int(row.get("minimum_lag_sessions", 0)),
                available_at_column=(
                    None
                    if not row.get("available_at_column")
                    else str(row["available_at_column"])
                ),
            )
        )
    return tuple(sorted(result, key=lambda item: item.name))


def research_family_from_sleeve(
    sleeve: SleeveSpec, *, created_at: datetime
) -> ResearchFamilyRecord:
    if not sleeve.cluster_id or sleeve.signal_expression is None:
        raise ValueError("a fixed family requires a clustered typed Sleeve")
    return ResearchFamilyRecord(
        family_id=sleeve.sleeve_id,
        display_name=sleeve.name,
        mechanism_key=sleeve.cluster_id,
        cluster_id=sleeve.cluster_id,
        allowed_fields=tuple(item.name for item in sleeve.signal_field_registry),
        field_registry=tuple(
            item.model_dump(mode="json", exclude_none=False)
            for item in sleeve.signal_field_registry
        ),
        created_at=created_at,
    )


def _canonical_graph(expression: Mapping[str, Any]) -> Any:
    nodes = tuple(expression.get("nodes") or ())
    node_map = {
        str(row.get("id") or row.get("node_id") or ""): dict(row) for row in nodes
    }
    output = str(expression.get("output_id") or expression.get("output") or "")
    memo: dict[str, Any] = {}

    def visit(node_id: str) -> Any:
        if node_id in memo:
            return memo[node_id]
        row = node_map[node_id]
        op = str(row.get("op") or "").lower()
        if op == "field":
            value: Any = {"op": op, "field": str(row.get("field") or "")}
        elif op == "constant":
            value = {"op": op, "value": row.get("value")}
        elif op in {"lag", "rolling_mean", "rolling_std", "rolling_sum", "rolling_min", "rolling_max"}:
            value = {"op": op, "input": visit(str(row.get("input") or row.get("input_id") or ""))}
            if op == "lag":
                value["periods"] = int(row.get("periods", 0))
            else:
                value.update(
                    {
                        "window": int(row.get("window", 0)),
                        "min_periods": row.get("min_periods"),
                    }
                )
        elif op in {"negate", "abs", "log", "sqrt", "not", "rank", "zscore", "winsorize"}:
            value = {"op": op, "input": visit(str(row.get("input") or row.get("input_id") or ""))}
            if op == "winsorize":
                value.update(
                    {
                        "lower_quantile": float(row.get("lower_quantile", 0.01)),
                        "upper_quantile": float(row.get("upper_quantile", 0.99)),
                    }
                )
        elif op in {"add", "subtract", "multiply", "divide", "greater", "greater_equal", "less", "less_equal", "equal", "and", "or"}:
            left = visit(str(row.get("left") or row.get("left_id") or ""))
            right = visit(str(row.get("right") or row.get("right_id") or ""))
            if op in _COMMUTATIVE_BINARY_OPS:
                left, right = sorted((left, right), key=canonical_json)
            value = {"op": op, "left": left, "right": right}
        elif op == "where":
            value = {
                "op": op,
                "condition": visit(str(row.get("condition") or row.get("condition_id") or "")),
                "if_true": visit(str(row.get("if_true") or row.get("true_id") or "")),
                "if_false": visit(str(row.get("if_false") or row.get("false_id") or "")),
            }
        elif op == "neutralize":
            categorical = set(
                map(
                    str,
                    row.get("categorical_exposures", row.get("categorical_exposure_ids", ())),
                )
            )
            exposures = [
                {
                    "value": visit(str(item)),
                    "categorical": str(item) in categorical,
                }
                for item in row.get("exposures", row.get("exposure_ids", ()))
            ]
            value = {
                "op": op,
                "input": visit(str(row.get("input") or row.get("input_id") or "")),
                "exposures": sorted(exposures, key=canonical_json),
            }
        else:
            raise ValueError(f"unsupported canonical DSL operation {op!r}")
        memo[node_id] = value
        return value

    if not output or output not in node_map:
        raise ValueError("factor DSL has no canonical output")
    return {"schema_version": "research-os/factor-dsl/v1", "output": visit(output)}


def _effective_direction(
    factor: FactorSpec, preregistration: Preregistration
) -> Literal["higher", "lower"]:
    if factor.direction is FactorDirection.HIGHER_IS_BETTER:
        return "higher"
    if factor.direction is FactorDirection.LOWER_IS_BETTER:
        return "lower"
    if preregistration.direction is HypothesisDirection.POSITIVE:
        return "higher"
    if preregistration.direction is HypothesisDirection.NEGATIVE:
        return "lower"
    raise ValueError("train_frozen direction must be preregistered positive or negative")


def assemble_factor_sleeve(
    factor_spec: ExperimentSpec, family: ResearchFamilyRecord
) -> ExperimentSpec:
    factor = factor_spec.factor
    if factor is None or factor_spec.sleeve is not None:
        raise ValueError("factor assembler requires exactly one FactorSpec")
    if factor.family != family.family_id:
        raise ValueError("Factor family differs from the fixed Family Registry")
    if not factor.allow_in_long_only:
        raise ValueError("Factor is not permitted in a long-only Sleeve")
    if not isinstance(factor.expression, Mapping):
        raise ValueError("Factor Sleeve assembly requires typed DSL")
    expression = copy.deepcopy(dict(factor.expression))
    direction = _effective_direction(factor, factor_spec.preregistration)
    if direction == "lower":
        identifiers = {
            str(row.get("id") or row.get("node_id") or "")
            for row in expression.get("nodes", ())
        }
        adjustment = "__direction_adjustment__"
        while adjustment in identifiers:
            adjustment += "_"
        prior_output = str(expression.get("output_id") or expression.get("output"))
        expression["nodes"] = [
            *list(expression.get("nodes") or ()),
            {"id": adjustment, "op": "negate", "input": prior_output},
        ]
        expression["output_id"] = adjustment
        expression.pop("output", None)
    graph_hash = content_fingerprint(
        _canonical_graph(expression), domain=f"{_EQUIVALENCE_DOMAIN}/factor-graph"
    )
    safe_family = re.sub(r"[^a-zA-Z0-9_]+", "_", family.family_id)[:100]
    sleeve = SleeveSpec(
        sleeve_id=f"{safe_family}__{graph_hash[:20]}",
        name=f"{family.display_name} Challenger {graph_hash[:8]}",
        mechanism=family.mechanism_key,
        factor_ids=(f"factor_{graph_hash[:24]}",),
        signal_expression=expression,
        signal_field_registry=factor.signal_field_registry,
        cluster_id=family.cluster_id,
        maximum_weight=0.35,
        long_only=True,
        expected_regimes=tuple(sorted(set(factor.expected_regimes))),
        falsification_criteria=tuple(
            sorted(
                set(factor.falsification_criteria)
                | set(factor_spec.preregistration.falsification_criteria)
            )
        ),
    )
    compile_factor_graph(expression, _field_specs(family))
    return factor_spec.model_copy(update={"factor": None, "sleeve": sleeve})


def research_equivalence_hash(
    spec: ExperimentSpec, family: ResearchFamilyRecord
) -> str:
    if spec.sleeve is not None:
        expression = spec.sleeve.signal_expression
        registry = spec.sleeve.signal_field_registry
        long_only = spec.sleeve.long_only
        maximum_weight = spec.sleeve.maximum_weight
    else:
        assert spec.factor is not None
        expression = spec.factor.expression
        registry = spec.factor.signal_field_registry
        long_only = spec.factor.allow_in_long_only
        maximum_weight = None
    if not isinstance(expression, Mapping):
        raise ValueError("research equivalence requires typed DSL")
    features = []
    for item in spec.features:
        payload = item.model_dump(mode="json", exclude_none=False)
        payload.pop("description", None)
        features.append(payload)
    payload = {
        "schema_version": "research-os/research-equivalence/v1",
        "family_id": family.family_id,
        "family_registry_hash": family.registry_hash,
        "cluster_id": family.cluster_id,
        "snapshot_content_hash": spec.snapshot.content_hash,
        "universe": spec.universe,
        "label": spec.label,
        "features": sorted(features, key=canonical_json),
        "signal_graph": _canonical_graph(expression),
        "signal_registry": sorted(
            (
                item.model_dump(mode="json", exclude_none=False)
                for item in registry
            ),
            key=lambda row: str(row["name"]),
        ),
        "long_only": long_only,
        "maximum_sleeve_weight": maximum_weight,
        "portfolio": spec.portfolio,
        "validation": spec.validation,
        "evaluator_version": spec.evaluator_version,
        "environment": spec.environment,
        "evaluation_inputs": spec.evaluation_inputs,
        "preregistered_direction": spec.preregistration.direction,
        "statistical_budget": spec.preregistration.statistical_budget,
    }
    return content_fingerprint(payload, domain=_EQUIVALENCE_DOMAIN)


class MonthlyResearchCoordinator:
    """The only production admission path for monthly confirmatory research."""

    def __init__(
        self,
        catalog: ResearchCatalog,
        *,
        lake_root: str | Path,
        environment: EnvironmentRef,
        universe: UniverseSpec | None = None,
        label: LabelSpec | None = None,
        portfolio: PortfolioPolicy | None = None,
        validation: ValidationProtocol | None = None,
        evaluator_version: str = CANONICAL_EVALUATOR_VERSION,
        bootstrap_resamples: int = 2_000,
        bootstrap_seed: int = 0,
        mode: Literal["production", "test", "legacy"] = "production",
        input_resolver: Callable[[], AuthoritativeResearchInputs] | None = None,
        cycle: ResearchCyclePort | None = None,
        shadow_authority: Any | None = None,
    ) -> None:
        if environment.evaluator_build != evaluator_version:
            raise ValueError("environment evaluator build differs from coordinator evaluator")
        if mode == "production" and input_resolver is not None:
            raise ValueError("production cannot accept a caller-supplied research input resolver")
        self.catalog = catalog
        self.lake_root = Path(lake_root)
        self.environment = environment
        self.universe = universe or UniverseSpec()
        self.label = label or LabelSpec()
        self.portfolio = portfolio or PortfolioPolicy()
        self.validation = validation or ValidationProtocol()
        self.evaluator_version = evaluator_version
        self.bootstrap_resamples = bootstrap_resamples
        self.bootstrap_seed = bootstrap_seed
        self.mode = mode
        self._input_resolver = input_resolver
        self.cycle = cycle or HistoricalResearchCycle(catalog)
        self.shadow_authority = shadow_authority

    def register_families(self, sleeves: Sequence[SleeveSpec]) -> tuple[ResearchFamilyRecord, ...]:
        now = self.catalog.database_now()
        return tuple(
            self.catalog.register_research_family(
                research_family_from_sleeve(sleeve, created_at=now)
            )
            for sleeve in sorted(sleeves, key=lambda item: item.sleeve_id)
        )

    def _latest_gold(self) -> DataSnapshotRef:
        for record in self.catalog.list_snapshots(
            limit=1_000,
            tier=SnapshotTier.GOLD,
            quality_status=DataQualityStatus.ACCEPTED,
        ):
            reference = record.reference
            if (
                reference.snapshot_id == reference.content_hash
                and not _BLOCKING_TRUST_LABELS.intersection(reference.trust_labels)
            ):
                return reference
        raise AuthoritativeResearchInputUnavailable("no clean accepted Gold snapshot is cataloged")

    def _inputs(self) -> AuthoritativeResearchInputs:
        if self._input_resolver is not None:
            if self.mode == "production":
                raise MonthlyResearchError("production input resolver invariant violated")
            return self._input_resolver()
        reference = self._latest_gold()
        try:
            frame = load_gold_research_panel(reference, lake_root=self.lake_root)
            exposure, returns, benchmark = derive_authoritative_evaluation_inputs(frame)
        except (GoldPanelError, SnapshotIntegrityError, FileNotFoundError, KeyError, ValueError) as exc:
            raise AuthoritativeResearchInputUnavailable(str(exc)) from exc
        return AuthoritativeResearchInputs(
            snapshot=reference,
            frame=frame,
            exposure_frame=exposure,
            returns_history=returns,
            benchmark_weights=benchmark,
        )

    def _template(
        self, family: ResearchFamilyRecord, inputs: AuthoritativeResearchInputs
    ) -> ExperimentSpec:
        fields = _field_specs(family)
        numeric = next((item for item in fields if item.value_type is ValueType.NUMERIC), None)
        if numeric is None:
            raise MonthlyResearchError("fixed family has no numeric proposal field")
        trusted = next(
            SignalFieldSpec.model_validate(item)
            for item in family.field_registry
            if str(item["name"]) == numeric.name
        )
        placeholder_factor = FactorSpec(
            factor_id="deterministic_template_placeholder_factor",
            family=family.family_id,
            name="deterministic template placeholder",
            mechanism=family.mechanism_key,
            expression={
                "schema_version": "research-os/factor-dsl/v1",
                "output_id": "field",
                "nodes": [{"id": "field", "op": "field", "field": numeric.name}],
            },
            signal_field_registry=(trusted,),
            direction=FactorDirection.HIGHER_IS_BETTER,
            falsification_criteria=("replaced by validated proposal before admission",),
        )
        return ExperimentSpec(
            snapshot=inputs.snapshot,
            universe=self.universe,
            label=self.label,
            features=(),
            factor=placeholder_factor,
            sleeve=None,
            portfolio=self.portfolio,
            validation=self.validation,
            evaluator_version=self.evaluator_version,
            environment=self.environment,
            evaluation_inputs=inputs.bindings(
                bootstrap_resamples=self.bootstrap_resamples,
                bootstrap_seed=self.bootstrap_seed,
            ),
            preregistration=Preregistration(
                hypothesis_id="deterministic_template_placeholder_hypothesis",
                economic_mechanism=family.mechanism_key,
                direction=HypothesisDirection.POSITIVE,
                falsification_criteria=("replaced by validated proposal before admission",),
                stop_rules=("replaced by validated proposal before admission",),
            ),
        )

    def _persist_admission_rejection(
        self,
        decision: ProposalDecision,
        *,
        family_id: str,
        violations: Sequence[str],
        occurred_at: datetime,
    ) -> None:
        normalized = tuple(sorted(set(map(str, violations))))
        fingerprint = content_fingerprint(
            {
                "decision_id": decision.decision_id,
                "family_id": family_id,
                "violations": normalized,
            },
            domain=f"{_SUBMISSION_DOMAIN}/rejection",
        )
        expected = RunRecord(
            run_id=f"proposal_admission_{fingerprint}",
            run_type="monthly_proposal_admission",
            status="failed",
            input_fingerprint=fingerprint,
            started_at=occurred_at,
            completed_at=occurred_at,
            error=",".join(normalized),
            metadata={
                "proposal_decision_id": decision.decision_id,
                "family_id": family_id,
                "violations": list(normalized),
                "authority": "deterministic_admission_rejection",
            },
        )
        existing = self.catalog.get_run(expected.run_id)
        if existing is None:
            self.catalog.claim_run(expected)
        elif (
            existing.input_fingerprint != expected.input_fingerprint
            or canonical_json(existing.metadata) != canonical_json(expected.metadata)
            or existing.error != expected.error
        ):
            raise CatalogConflict("proposal admission rejection identity collision")

    def submit(
        self,
        payload: Mapping[str, Any],
        *,
        family_id: str,
        recovery_case_id: str | None = None,
    ) -> ProposalAdmission:
        family = self.catalog.get_research_family(family_id)
        if family is None or not family.active:
            raise CatalogNotFound(f"active fixed family {family_id!r} was not found")
        if recovery_case_id is not None:
            case = self.catalog.get_recovery_case(recovery_case_id)
            if case is None:
                raise CatalogNotFound(f"recovery case {recovery_case_id!r} was not found")
            if case.sleeve_id != family_id:
                raise CatalogConflict("recovery case Sleeve differs from proposal family")
        inputs = self._inputs()
        decision = review_llm_proposal(
            payload,
            experiment_template=self._template(family, inputs),
            field_specs=_field_specs(family),
        )
        now = self.catalog.database_now()
        persist_proposal_decision(self.catalog, decision, reviewed_at=now)
        violations = list(decision.violations)
        if decision.accepted:
            assert decision.experiment_spec is not None
            factor = decision.experiment_spec.factor
            assert factor is not None
            if factor.family != family_id:
                violations.append("factor_family_not_in_fixed_registry")
            if not factor.allow_in_long_only:
                violations.append("factor_not_allowed_in_long_only")
        if violations:
            self._persist_admission_rejection(
                decision,
                family_id=family_id,
                violations=violations,
                occurred_at=now,
            )
            return ProposalAdmission(decision, None, tuple(sorted(set(violations))))
        assert decision.experiment_spec is not None
        sleeve_spec = assemble_factor_sleeve(decision.experiment_spec, family)
        equivalence = research_equivalence_hash(sleeve_spec, family)
        identity = content_fingerprint(
            {
                "proposal_decision_id": decision.decision_id,
                "family_id": family_id,
                "recovery_case_id": recovery_case_id,
                "research_equivalence_hash": equivalence,
                "experiment_fingerprint": sleeve_spec.fingerprint(),
            },
            domain=_SUBMISSION_DOMAIN,
        )
        submission = ResearchSubmissionRecord(
            submission_id=f"submission_{identity}",
            proposal_decision_id=decision.decision_id,
            family_id=family_id,
            recovery_case_id=recovery_case_id,
            status="reviewed",
            research_equivalence_hash=equivalence,
            experiment_fingerprint=sleeve_spec.fingerprint(),
            trial_id=f"trial_{sleeve_spec.fingerprint()[:32]}",
            spec=sleeve_spec,
            created_at=now,
            updated_at=now,
        )
        return ProposalAdmission(
            decision,
            self.catalog.create_research_submission(submission),
            (),
        )

    def propose(
        self,
        port: HypothesisProposalPort,
        *,
        family_id: str,
        recovery_case_id: str | None = None,
    ) -> ProposalAdmission:
        family = self.catalog.get_research_family(family_id)
        if family is None or not family.active:
            raise CatalogNotFound(f"active fixed family {family_id!r} was not found")
        context = {
            "schema_version": "research-os/monthly-proposal-context/v1",
            "family_id": family.family_id,
            "mechanism_key": family.mechanism_key,
            "allowed_fields": list(family.allowed_fields),
            "field_registry_hash": family.registry_hash,
            "output_scope": ["preregistration", "factor"],
            "decision_authority": "none",
        }
        return self.submit(
            port.propose(context),
            family_id=family_id,
            recovery_case_id=recovery_case_id,
        )

    def reserve(self, submission_id: str) -> ResearchSubmissionRecord:
        submission = self.catalog.get_research_submission(submission_id)
        if submission is None:
            raise CatalogNotFound(f"research submission {submission_id!r} was not found")
        if submission.status != "reviewed":
            return submission
        budget = submission.spec.validation.statistical_budget
        registration = TrialRegistration(
            trial_id=submission.trial_id,
            experiment_fingerprint=submission.experiment_fingerprint,
            hypothesis_id=submission.spec.preregistration.hypothesis_id,
            family=submission.family_id,
            kind=TrialKind.CONFIRMATORY,
            registered_at=submission.created_at,
            holdout_id=HISTORICAL_HOLDOUT_ID,
            requested_evidence_class=EvidenceClass.PSEUDO_OOS,
            research_equivalence_hash=submission.research_equivalence_hash,
        )
        self.catalog.reserve_trial(
            registration,
            candidate_id=submission.spec.candidate_id,
            maximum_monthly_confirmatory_trials=(
                budget.maximum_confirmatory_challengers_per_month
            ),
            maximum_monthly_confirmatory_trials_per_family=(
                budget.maximum_confirmatory_challengers_per_family_per_month
            ),
            maximum_diagnostic_branches=budget.maximum_diagnostic_branches,
        )
        return self.catalog.reserve_research_submission(submission_id)

    def _finish_trial_after_error(
        self,
        submission: ResearchSubmissionRecord,
        *,
        outcome: TrialOutcome,
        reason: str,
        experiment_id: str | None,
        completed_at: datetime,
    ) -> None:
        trial = self.catalog.get_trial(submission.trial_id)
        if trial is None or trial.completed_at is not None:
            return
        self.catalog.complete_trial(
            submission.trial_id,
            outcome=outcome,
            reason=reason,
            experiment_id=experiment_id,
            completed_at=completed_at,
            metadata={
                "research_equivalence_hash": submission.research_equivalence_hash,
                "coordinator_terminalization": True,
            },
        )

    def _promote_shadow(
        self,
        submission: ResearchSubmissionRecord,
        *,
        experiment_id: str,
    ) -> str | None:
        authoritative = self.catalog.get_authoritative_result(experiment_id)
        if authoritative is None or authoritative.outcome != "promoted_to_shadow":
            return None
        experiment = self.catalog.get_experiment(experiment_id)
        if experiment is None or experiment.spec.sleeve is None:
            raise MonthlyResearchError("only an authoritative promoted Sleeve can be a Challenger")
        from .lifecycle import SleeveLifecycleRecord, SleeveState
        from .sleeve_lifecycle import SleeveShadowLifecycleService
        from .sleeve_registry import (
            SleeveRosterEntry,
            build_sleeve_roster_manifest,
            persist_sleeve_roster,
        )

        roster = build_sleeve_roster_manifest(
            (
                SleeveRosterEntry(
                    sleeve=experiment.spec.sleeve,
                    representative_priority=0,
                ),
            ),
            roster_name=(
                f"monthly-{submission.family_id}-{submission.research_equivalence_hash[:16]}"
            ),
        )
        persist_sleeve_roster(
            self.catalog, roster, recorded_at=authoritative.completed_at
        )
        binding = SleeveShadowLifecycleService(
            self.catalog, shadow_authority=self.shadow_authority
        ).promote(
            record=SleeveLifecycleRecord(
                sleeve_id=experiment.spec.sleeve.sleeve_id,
                state=SleeveState.WALK_FORWARD,
            ),
            experiment_id=experiment_id,
            roster=roster,
            promoted_at=authoritative.completed_at,
            initial_capital=experiment.spec.portfolio.capital,
            recovery_case_id=submission.recovery_case_id,
        )
        return binding.shadow_account_id

    def _execute_claimed_submission(
        self,
        submission: ResearchSubmissionRecord,
        *,
        lease: _SubmissionLeaseGuard,
    ) -> SubmissionExecution:
        experiment_id: str | None = None
        try:
            inputs = self._inputs()
            if inputs.snapshot != submission.spec.snapshot:
                raise AuthoritativeResearchInputUnavailable(
                    "submission Gold snapshot is no longer the resolved immutable input"
                )
            result = self.cycle.run(
                submission.spec,
                inputs.frame,
                bootstrap_resamples=self.bootstrap_resamples,
                seed=self.bootstrap_seed,
                exposure_frame=inputs.exposure_frame,
                returns_history=inputs.returns_history,
                benchmark_weights=inputs.benchmark_weights,
                research_equivalence_hash=submission.research_equivalence_hash,
                trial_family=submission.family_id,
            )
            if not isinstance(result, ResearchCycleResult):
                raise TypeError("research cycle returned a non-authoritative result type")
            experiment_id = result.experiment_id
            trial = self.catalog.get_trial(submission.trial_id)
            terminal = (
                "completed"
                if result.status == "completed"
                else "missing_data"
                if trial is not None and trial.outcome is TrialOutcome.MISSING_DATA
                else "failed"
            )
            shadow_account_id = None
            lease.assert_owned()
            if terminal == "completed":
                shadow_account_id = self._promote_shadow(
                    submission, experiment_id=experiment_id
                )
                lease.assert_owned()
            finished = lease.finish(
                status=terminal,
                experiment_id=experiment_id,
                error=None if terminal == "completed" else ",".join(result.failures),
            )
            return SubmissionExecution(
                finished, True, result=result, shadow_account_id=shadow_account_id
            )
        except ResearchSubmissionLeaseLost:
            raise
        except AuthoritativeResearchInputUnavailable as exc:
            lease.assert_owned()
            completed_at = self.catalog.database_now()
            self._finish_trial_after_error(
                submission,
                outcome=TrialOutcome.MISSING_DATA,
                reason=f"authoritative input missing: {exc}",
                experiment_id=experiment_id,
                completed_at=completed_at,
            )
            finished = lease.finish(
                status="missing_data",
                experiment_id=experiment_id,
                error=str(exc),
                finished_at=completed_at,
            )
            return SubmissionExecution(finished, True)
        except Exception as exc:
            lease.assert_owned()
            completed_at = self.catalog.database_now()
            registered = self.catalog.get_experiment_by_fingerprint(
                submission.experiment_fingerprint
            )
            experiment_id = experiment_id or (
                None if registered is None else registered.experiment_id
            )
            self._finish_trial_after_error(
                submission,
                outcome=TrialOutcome.FAILURE,
                reason=f"coordinator execution failed: {type(exc).__name__}:{exc}",
                experiment_id=experiment_id,
                completed_at=completed_at,
            )
            finished = lease.finish(
                status="failed",
                experiment_id=experiment_id,
                error=f"{type(exc).__name__}:{exc}",
                finished_at=completed_at,
            )
            return SubmissionExecution(finished, True)

    def run(
        self,
        submission_id: str,
        *,
        worker_id: str,
        lease_seconds: int = 1_800,
    ) -> SubmissionExecution:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        submission = self.reserve(submission_id)
        if submission.status in TERMINAL_SUBMISSION_STATUSES:
            return SubmissionExecution(submission, False)
        now = self.catalog.database_now()
        claimed, won = self.catalog.claim_research_submission(
            submission_id,
            worker_id=worker_id,
            claimed_at=now,
            lease_expires_at=now + timedelta(seconds=lease_seconds),
        )
        if not won:
            return SubmissionExecution(claimed, False)
        submission = claimed
        lease = _SubmissionLeaseGuard(
            self.catalog,
            submission,
            worker_id=worker_id,
            lease_seconds=lease_seconds,
        )
        lease.start()
        try:
            return self._execute_claimed_submission(submission, lease=lease)
        except ResearchSubmissionLeaseLost:
            current = self.catalog.get_research_submission(submission_id)
            if current is None:
                raise CatalogNotFound(
                    f"research submission {submission_id!r} disappeared after lease loss"
                )
            return SubmissionExecution(current, False)
        finally:
            lease.close()

    def status(self, submission_id: str) -> ResearchSubmissionRecord:
        submission = self.catalog.get_research_submission(submission_id)
        if submission is None:
            raise CatalogNotFound(f"research submission {submission_id!r} was not found")
        return submission

    def resume(
        self,
        *,
        worker_id: str,
        limit: int = 100,
        lease_seconds: int = 1_800,
    ) -> tuple[SubmissionExecution, ...]:
        pending: dict[str, ResearchSubmissionRecord] = {}
        for status in ("reviewed", "reserved", "running"):
            for row in self.catalog.list_research_submissions(
                limit=limit, status=status
            ):
                pending[row.submission_id] = row
        ordered = sorted(
            pending.values(), key=lambda row: (row.updated_at, row.submission_id)
        )[:limit]
        return tuple(
            self.run(
                row.submission_id,
                worker_id=worker_id,
                lease_seconds=lease_seconds,
            )
            for row in ordered
        )


__all__ = [
    "AuthoritativeResearchInputUnavailable",
    "AuthoritativeResearchInputs",
    "MonthlyResearchCoordinator",
    "MonthlyResearchError",
    "ProposalAdmission",
    "SubmissionExecution",
    "assemble_factor_sleeve",
    "derive_authoritative_evaluation_inputs",
    "research_equivalence_hash",
    "research_family_from_sleeve",
]

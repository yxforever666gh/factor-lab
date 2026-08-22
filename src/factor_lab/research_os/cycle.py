"""Deterministic, ledger-backed historical research cycle.

This module is the bridge between the typed contracts, leak-safe DSL, nested
walk-forward protocol, canonical long-only simulator and statistical gate.  It
does not discover variants and it cannot submit orders: one invocation executes
exactly one preregistered experiment fingerprint.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timezone
from math import sqrt
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from .catalog import LifecycleEvent, ResearchCatalog
from .contracts import (
    DataQualityStatus,
    ExperimentSpec,
    FactorDirection,
    HypothesisDirection,
    LifecycleState,
    TrialOutcome,
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
from .evaluator import CANONICAL_EVALUATOR_VERSION, CanonicalLongOnlyEvaluator
from .fingerprint import content_fingerprint
from .governance import (
    HISTORICAL_HOLDOUT_ID,
    EvidenceClass,
    TrialKind,
    TrialLedger,
    TrialRecord,
    TrialRegistration,
    assess_candidate_promotion,
    evaluate_promotion,
    PromotionEvidence,
)
from .negative_controls import (
    NegativeControlMetric,
    generate_negative_control_signals,
    reverse_direction_signal,
)
from .risk_optimizer import StockOptimizationPolicy
from .snapshots import SnapshotIntegrityError, verify_snapshot_frame_binding
from .statistics import annualized_sharpe, block_bootstrap
from .walk_forward import build_nested_walk_forward_plan, select_fold_rows


FORBIDDEN_PROMOTION_TRUST_LABELS = frozenset(
    {
        "st_history_unverified",
        "legacy_untrusted_data",
        "legacy_execution_regression_only",
        "disputed",
        "quarantined",
    }
)


@dataclass(frozen=True)
class ResearchCycleResult:
    experiment_id: str
    fingerprint: str
    status: str
    promotion_verdict: str
    lifecycle_state: str
    metrics: Mapping[str, Any]
    failures: tuple[str, ...]
    fold_results: tuple[Mapping[str, Any], ...]
    statistical_evidence: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return _jsonable(asdict(self))


def field_specs_from_mapping(payload: Iterable[Mapping[str, Any]]) -> tuple[FieldSpec, ...]:
    """Parse the explicit field/availability registry used by the factor DSL."""

    result: list[FieldSpec] = []
    for row in payload:
        result.append(
            FieldSpec(
                name=str(row["name"]),
                value_type=ValueType(str(row.get("value_type", "numeric"))),
                role=FieldRole(str(row.get("role", "feature"))),
                availability=Availability(str(row.get("availability", "close"))),
                minimum_lag_sessions=int(row.get("minimum_lag_sessions", 0)),
                available_at_column=(
                    None
                    if not row.get("available_at_column")
                    else str(row["available_at_column"])
                ),
            )
        )
    return tuple(result)


def _jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return _jsonable(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (np.integer, np.floating)):
        return value.item()
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if hasattr(value, "value"):
        return value.value
    return value


def evaluation_input_hash(name: str, value: Any) -> str:
    """Fingerprint one optional runtime input without normalizing away row order."""

    if isinstance(value, pd.Series):
        value = value.to_frame(name=value.name or "value")
    if isinstance(value, pd.DataFrame):
        payload: Any = {
            "kind": "pandas_frame",
            "columns": [str(item) for item in value.columns],
            "dtypes": [str(item) for item in value.dtypes],
            "data": value.to_json(
                orient="split",
                date_format="iso",
                date_unit="ns",
                double_precision=15,
                default_handler=str,
            ),
        }
    elif hasattr(value, "model_dump"):
        payload = {
            "kind": type(value).__name__,
            "data": value.model_dump(mode="json", exclude_none=False),
        }
    else:
        payload = {"kind": type(value).__name__, "data": _jsonable(value)}
    return content_fingerprint(
        payload, domain=f"factor-lab/research-os/v1/evaluation-input/{name}"
    )


def _evaluation_input_blockers(
    spec: ExperimentSpec,
    *,
    exposure_frame: pd.DataFrame | None,
    returns_history: pd.DataFrame | None,
    benchmark_weights: pd.Series | pd.DataFrame | Mapping[str, float] | None,
    optimization_policy: StockOptimizationPolicy | Mapping[str, Any] | None,
    negative_controls: Sequence[NegativeControlMetric | Mapping[str, object]],
    within_family_p_values: Sequence[float],
    data_audit_blockers: Sequence[str],
    bootstrap_resamples: int,
    seed: int,
) -> tuple[str, ...]:
    runtime = {
        "exposure_frame": exposure_frame,
        "returns_history": returns_history,
        "benchmark_weights": benchmark_weights,
        "optimization_policy": optimization_policy,
        "negative_controls": negative_controls or None,
        "within_family_p_values": within_family_p_values or None,
        "data_audit_blockers": data_audit_blockers or None,
    }
    blockers: list[str] = []
    for name, value in runtime.items():
        expected = getattr(spec.evaluation_inputs, f"{name}_hash")
        if value is None and expected is not None:
            blockers.append(f"bound_evaluation_input_missing:{name}")
        elif value is not None and expected is None:
            blockers.append(f"unbound_evaluation_input:{name}")
        elif value is not None and evaluation_input_hash(name, value) != expected:
            blockers.append(f"evaluation_input_hash_mismatch:{name}")
    if int(bootstrap_resamples) != spec.evaluation_inputs.bootstrap_resamples:
        blockers.append("evaluation_input_mismatch:bootstrap_resamples")
    if int(seed) != spec.evaluation_inputs.bootstrap_seed:
        blockers.append("evaluation_input_mismatch:bootstrap_seed")
    return tuple(blockers)


def _factor_direction(spec: ExperimentSpec) -> float:
    if spec.factor is None:
        return 1.0
    if spec.factor.direction is FactorDirection.HIGHER_IS_BETTER:
        return 1.0
    if spec.factor.direction is FactorDirection.LOWER_IS_BETTER:
        return -1.0
    preregistered = spec.preregistration.direction
    if preregistered is HypothesisDirection.POSITIVE:
        return 1.0
    if preregistered is HypothesisDirection.NEGATIVE:
        return -1.0
    raise ValueError("train_frozen direction must resolve to positive or negative before outer OOS")


def _compound(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return float(np.prod(1.0 + np.asarray(values, dtype=float)) - 1.0)


def _annualize(values: Sequence[float], periods_per_year: float) -> float:
    if not values:
        return 0.0
    total = _compound(values)
    if total <= -1.0:
        return -1.0
    return float((1.0 + total) ** (periods_per_year / len(values)) - 1.0)


def _drawdown(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    curve = np.cumprod(1.0 + np.asarray(values, dtype=float))
    peaks = np.maximum.accumulate(np.r_[1.0, curve])[:-1]
    return float(np.min(curve / peaks - 1.0))


def _stitched_metrics(
    fold_results: Sequence[Mapping[str, Any]], *, periods_per_year: float
) -> tuple[dict[str, Any], tuple[float, ...]]:
    periods: list[Mapping[str, Any]] = []
    positive_outer_years = 0
    for fold in fold_results:
        rows = list(fold.get("periods") or [])
        periods.extend(rows)
        if _compound([float(row.get("excess_return") or 0.0) for row in rows]) > 0:
            positive_outer_years += 1
    periods.sort(key=lambda row: (str(row.get("start_date") or ""), str(row.get("signal_date") or "")))
    net = [float(row.get("net_return") or 0.0) for row in periods]
    benchmark = [float(row.get("benchmark_return") or 0.0) for row in periods]
    excess = tuple(float(row.get("excess_return") or 0.0) for row in periods)
    active_std = float(np.std(excess, ddof=1)) if len(excess) > 1 else 0.0
    information_ratio = (
        float(np.mean(excess) / active_std * sqrt(periods_per_year))
        if active_std > 0
        else 0.0
    )
    half_year: dict[str, list[float]] = {}
    for row in periods:
        stamp = pd.Timestamp(str(row.get("start_date")))
        key = f"{stamp.year}-H{1 if stamp.month <= 6 else 2}"
        half_year.setdefault(key, []).append(float(row.get("excess_return") or 0.0))
    positive_half_year_ratio = (
        float(np.mean([_compound(values) > 0 for values in half_year.values()]))
        if half_year
        else 0.0
    )
    holdings = [float(row.get("holding_count") or 0.0) for row in periods]
    metrics = {
        "benchmark_return": _compound(benchmark),
        "excess_return": _compound(excess),
        "net_return": _compound(net),
        "net_annual_return": _annualize(net, periods_per_year),
        "benchmark_annual_return": _annualize(benchmark, periods_per_year),
        "net_excess_annual_return": _annualize(net, periods_per_year)
        - _annualize(benchmark, periods_per_year),
        "net_sharpe": annualized_sharpe(net, periods_per_year=periods_per_year),
        "information_ratio": information_ratio,
        "max_drawdown": _drawdown(net),
        "positive_half_year_ratio": positive_half_year_ratio,
        "positive_outer_years": positive_outer_years,
        "evaluated_outer_years": len(fold_results),
        "capacity_violations": int(
            sum(int(row.get("capacity_violation_count") or 0) for row in periods)
        ),
        "blocked_trade_count": int(
            sum(int(row.get("blocked_trade_count") or 0) for row in periods)
        ),
        "actual_turnover": float(
            np.mean([float(row.get("turnover") or 0.0) for row in periods])
        )
        if periods
        else 0.0,
        "average_holding_count": float(np.mean(holdings)) if holdings else 0.0,
        "observations": len(periods),
    }
    return metrics, excess


def _rank_ic(frame: pd.DataFrame, signal: pd.Series) -> float | None:
    label = next(
        (
            name
            for name in ("forward_return_5d_open", "forward_return_5d")
            if name in frame.columns
        ),
        None,
    )
    if label is None:
        return None
    work = pd.DataFrame(
        {
            "date": pd.to_datetime(frame["date"], errors="coerce"),
            "signal": pd.to_numeric(signal, errors="coerce"),
            "label": pd.to_numeric(frame[label], errors="coerce"),
        }
    ).dropna()
    values = [
        float(value)
        for value in work.groupby("date", sort=True).apply(
            lambda group: group["signal"].corr(group["label"], method="spearman"),
            include_groups=False,
        )
        if pd.notna(value)
    ]
    return float(np.mean(values)) if values else None


def _fit_inner_calibration(signal: pd.Series) -> tuple[float, float, int]:
    """Fit the sole preregistered candidate's robust scaling on training only."""

    numeric = pd.to_numeric(signal, errors="coerce")
    finite = numeric[np.isfinite(numeric.to_numpy(dtype=float, na_value=np.nan))]
    if finite.empty:
        raise ValueError("inner training signal has no finite observations")
    location = float(finite.median())
    lower, upper = finite.quantile([0.25, 0.75]).tolist()
    scale = float(upper - lower)
    if not np.isfinite(scale) or scale <= 0.0:
        scale = float(finite.std(ddof=0))
    if not np.isfinite(scale) or scale <= 0.0:
        scale = 1.0
    return location, scale, len(finite)


def _consume_inner_folds(
    frame: pd.DataFrame,
    signal: pd.Series,
    inner_folds: Sequence[Any],
) -> tuple[dict[str, Any], ...]:
    """Run the mandatory inner model-selection process for a fixed candidate.

    There are no runtime variants to cherry-pick: the candidate is the DSL
    expression embedded in the experiment fingerprint.  Every inner fold fits
    robust scaling on its purged training rows and validates that calibration
    on its untouched validation rows.  The outer test is unreachable unless
    all registered inner folds are consumed successfully.
    """

    evidence: list[dict[str, Any]] = []
    for fold in inner_folds:
        train, validation = select_fold_rows(frame, fold)
        location, scale, train_count = _fit_inner_calibration(signal.loc[train.index])
        validation_signal = (
            pd.to_numeric(signal.loc[validation.index], errors="coerce") - location
        ) / scale
        finite_validation = int(
            np.isfinite(
                validation_signal.to_numpy(dtype=float, na_value=np.nan)
            ).sum()
        )
        if finite_validation == 0:
            raise ValueError(
                f"inner fold {fold.fold_id} has no finite validation signal"
            )
        payload = {
            "fold_id": fold.fold_id,
            "selection_rule": "single_preregistered_dsl_candidate_robust_scale_v1",
            "selected_candidate": "registered_candidate",
            "train_observations": train_count,
            "validation_observations": finite_validation,
            "train_location": location,
            "train_scale": scale,
        }
        payload["evidence_hash"] = content_fingerprint(
            payload, domain="factor-lab/research-os/v1/inner-selection"
        )
        evidence.append(payload)
    if not evidence:
        raise ValueError("outer evaluation requires at least one consumed inner fold")
    return tuple(evidence)


def _lifetime_net_sharpes(
    ledger: TrialLedger, family: str
) -> tuple[float, ...]:
    """Read DSR's selection population only from persisted trial metrics."""

    return tuple(
        float(item.metadata["net_sharpe"])
        for item in ledger.family_records(family)
        if _is_finite_number(item.metadata.get("net_sharpe"))
    )


class HistoricalResearchCycle:
    """Execute one preregistered fingerprint and persist one authoritative result."""

    def __init__(self, catalog: ResearchCatalog) -> None:
        self.catalog = catalog
        self.evaluator = CanonicalLongOnlyEvaluator()

    def run(
        self,
        spec: ExperimentSpec,
        frame: pd.DataFrame,
        *,
        field_specs: Iterable[FieldSpec] = (),
        sleeve_signal: str | pd.Series | None = None,
        negative_controls: Iterable[NegativeControlMetric | Mapping[str, object]] = (),
        within_family_p_values: Iterable[float] = (),
        data_audit_blockers: Iterable[str] = (),
        bootstrap_resamples: int = 2_000,
        seed: int = 0,
        exposure_frame: pd.DataFrame | None = None,
        returns_history: pd.DataFrame | None = None,
        benchmark_weights: pd.Series | pd.DataFrame | Mapping[str, float] | None = None,
        optimization_policy: StockOptimizationPolicy | Mapping[str, Any] | None = None,
        research_equivalence_hash: str | None = None,
        trial_family: str | None = None,
    ) -> ResearchCycleResult:
        if spec.evaluator_version != CANONICAL_EVALUATOR_VERSION:
            raise ValueError(
                f"new experiments must use {CANONICAL_EVALUATOR_VERSION!r}"
            )
        if sleeve_signal is not None:
            # Sleeve signals are part of SleeveSpec.signal_expression.  Reject
            # this compatibility argument before reserving a fingerprint so an
            # untrusted caller cannot permanently poison the authoritative
            # result for an otherwise valid experiment.
            raise ValueError(
                "runtime sleeve_signal is forbidden; register signal_expression in SleeveSpec"
            )
        negative_control_values = tuple(negative_controls)
        within_family_values = tuple(float(value) for value in within_family_p_values)
        data_blocker_values = tuple(map(str, data_audit_blockers))
        snapshot_record = self.catalog.get_snapshot(spec.snapshot.snapshot_id)
        if snapshot_record is None:
            raise SnapshotIntegrityError(
                "research snapshot is not a previously published catalog snapshot"
            )
        if snapshot_record.reference != spec.snapshot:
            raise SnapshotIntegrityError(
                "experiment snapshot ref differs from published catalog evidence"
            )
        experiment = self.catalog.register_experiment(spec)
        fingerprint = experiment.fingerprint
        existing_result = self.catalog.get_authoritative_result(experiment.experiment_id)
        if existing_result is not None:
            payload = dict(existing_result.metrics)
            return ResearchCycleResult(
                experiment_id=str(payload.get("experiment_id") or experiment.experiment_id),
                fingerprint=str(payload.get("fingerprint") or fingerprint),
                status=str(payload.get("status") or "completed"),
                promotion_verdict=str(payload.get("promotion_verdict") or "reject"),
                lifecycle_state=str(payload.get("lifecycle_state") or LifecycleState.WALK_FORWARD.value),
                metrics=dict(payload.get("metrics") or {}),
                failures=tuple(payload.get("failures") or ()),
                fold_results=tuple(payload.get("fold_results") or ()),
                statistical_evidence=dict(payload.get("statistical_evidence") or {}),
            )
        trial_id = f"trial_{fingerprint[:32]}"
        resolved_trial_family = str(trial_family or spec.family).strip()
        if not resolved_trial_family:
            raise ValueError("trial_family must not be empty")
        registration = TrialRegistration(
            trial_id=trial_id,
            experiment_fingerprint=fingerprint,
            hypothesis_id=spec.preregistration.hypothesis_id,
            family=resolved_trial_family,
            kind=TrialKind.CONFIRMATORY,
            registered_at=datetime.now(timezone.utc),
            holdout_id=HISTORICAL_HOLDOUT_ID,
            requested_evidence_class=EvidenceClass.PSEUDO_OOS,
            research_equivalence_hash=research_equivalence_hash,
        )
        budget = spec.validation.statistical_budget
        reservation = self.catalog.reserve_trial(
            registration,
            candidate_id=spec.candidate_id,
            experiment_id=experiment.experiment_id,
            maximum_monthly_confirmatory_trials=budget.maximum_confirmatory_challengers_per_month,
            maximum_monthly_confirmatory_trials_per_family=budget.maximum_confirmatory_challengers_per_family_per_month,
            maximum_diagnostic_branches=budget.maximum_diagnostic_branches,
        )
        admission = reservation.admission
        existing_ledger = TrialLedger.from_catalog_entries(self.catalog.list_trials())
        if not admission.allowed:
            return self._blocked_result(
                spec,
                experiment.experiment_id,
                trial_id,
                failures=admission.reasons,
                outcome=TrialOutcome.REJECTED,
                reason="trial admission rejected",
            )

        blockers = list(data_blocker_values)
        blockers.extend(
            _evaluation_input_blockers(
                spec,
                exposure_frame=exposure_frame,
                returns_history=returns_history,
                benchmark_weights=benchmark_weights,
                optimization_policy=optimization_policy,
                negative_controls=negative_control_values,
                within_family_p_values=within_family_values,
                data_audit_blockers=data_blocker_values,
                bootstrap_resamples=bootstrap_resamples,
                seed=seed,
            )
        )
        blockers.extend(
            sorted(set(snapshot_record.reference.trust_labels) & FORBIDDEN_PROMOTION_TRUST_LABELS)
        )
        if snapshot_record.reference.quality_status is not DataQualityStatus.ACCEPTED:
            blockers.append(f"snapshot_quality_{snapshot_record.reference.quality_status.value}")
        if spec.factor is not None and not spec.factor.allow_in_long_only:
            blockers.append("factor_not_allowed_in_long_only")
        if blockers:
            return self._blocked_result(
                spec,
                experiment.experiment_id,
                trial_id,
                failures=tuple(dict.fromkeys(blockers)),
                outcome=TrialOutcome.MISSING_DATA,
                reason="data or policy audit blocked evaluation",
                lifecycle_state=LifecycleState.FROZEN_DATA,
            )

        try:
            signal = self._signal(
                spec, frame, tuple(field_specs), sleeve_signal=sleeve_signal
            )
            snapshot_binding = verify_snapshot_frame_binding(spec.snapshot, frame)
            sessions = tuple(
                sorted(
                    pd.to_datetime(frame["date"], errors="raise")
                    .dt.normalize()
                    .unique()
                )
            )
            plan = build_nested_walk_forward_plan(sessions, spec.validation)
            fold_payloads: list[dict[str, Any]] = []
            risk_promotion_blockers: list[str] = []
            for nested in plan.outer_folds:
                inner_selection = _consume_inner_folds(frame, signal, nested.inner)
                train, test = select_fold_rows(frame, nested.outer)
                location, scale, _ = _fit_inner_calibration(signal.loc[train.index])
                test_signal = (
                    pd.to_numeric(signal.loc[test.index], errors="coerce") - location
                ) / scale
                evaluation = self.evaluator.evaluate(
                    experiment_id=experiment.experiment_id,
                    snapshot_id=spec.snapshot.snapshot_id,
                    factor_or_sleeve_id=spec.candidate_id,
                    frame=test,
                    signal=test_signal,
                    portfolio_policy=spec.portfolio,
                    pricing_frame=frame,
                    exposure_frame=exposure_frame,
                    returns_history=returns_history,
                    benchmark_weights=benchmark_weights,
                    optimization_policy=optimization_policy,
                    production_contract=True,
                    environment_hash=spec.environment.code_hash,
                )
                result = dict(evaluation.result)
                if result.get("status") != "ok":
                    return self._blocked_result(
                        spec,
                        experiment.experiment_id,
                        trial_id,
                        failures=(
                            f"{nested.outer.fold_id}:{result.get('reason') or result.get('status')}",
                        ),
                        outcome=TrialOutcome.FAILURE,
                        reason="canonical evaluator failed",
                    )
                if not bool(result.get("promotion_eligible", False)):
                    risk_promotion_blockers.extend(
                        map(
                            str,
                            result.get("promotion_blockers")
                            or ("risk_optimization_not_promotion_eligible",),
                        )
                    )
                fold_payloads.append(
                    {
                        "fold_id": nested.outer.fold_id,
                        "inner_selection": list(inner_selection),
                        "outer_train_calibration": {
                            "location": location,
                            "scale": scale,
                        },
                        **result,
                    }
                )
        except SnapshotIntegrityError as exc:
            return self._blocked_result(
                spec,
                experiment.experiment_id,
                trial_id,
                failures=(f"snapshot_frame_binding:{exc}",),
                outcome=TrialOutcome.MISSING_DATA,
                reason="snapshot data integrity blocked evaluation",
                lifecycle_state=LifecycleState.FROZEN_DATA,
            )
        except Exception as exc:
            return self._blocked_result(
                spec,
                experiment.experiment_id,
                trial_id,
                failures=(f"{type(exc).__name__}:{exc}",),
                outcome=TrialOutcome.FAILURE,
                reason="methodology execution failed",
            )

        periods_per_year = spec.validation.annualization_sessions / spec.portfolio.rebalance_sessions
        metrics, stitched_excess = _stitched_metrics(
            fold_payloads, periods_per_year=periods_per_year
        )
        metrics["rank_ic_mean"] = _rank_ic(frame, signal)
        metrics["snapshot_binding"] = _jsonable(snapshot_binding)
        metrics["promotion_eligible"] = not risk_promotion_blockers
        metrics["risk_promotion_blockers"] = list(
            dict.fromkeys(risk_promotion_blockers)
        )
        if not negative_control_values:
            (
                negative_control_values,
                negative_control_blockers,
            ) = self._deterministic_negative_control_metrics(
                spec=spec,
                frame=frame,
                signal=signal,
                plan=plan,
                exposure_frame=exposure_frame,
                returns_history=returns_history,
                benchmark_weights=benchmark_weights,
                optimization_policy=optimization_policy,
                seed=seed,
            )
            risk_promotion_blockers.extend(negative_control_blockers)
            metrics["risk_promotion_blockers"] = list(
                dict.fromkeys(risk_promotion_blockers)
            )
        metrics["negative_controls"] = [
            {
                "control_name": item.control_name,
                "metric": item.metric,
                "passed_promotion_gate": item.passed_promotion_gate,
            }
            for item in negative_control_values
        ]
        try:
            p_value = block_bootstrap(
                stitched_excess,
                periods_per_year=periods_per_year,
                resamples=bootstrap_resamples,
                seed=seed,
            ).one_sided_p_value
        except ValueError:
            p_value = 1.0

        reserved_record = TrialRecord.from_catalog_entry(reservation.entry)
        candidate_record = TrialRecord(
            registration=reserved_record.registration,
            outcome=TrialOutcome.SUCCESS,
            completed_at=datetime.now(timezone.utc),
            p_value=p_value,
        )
        assessment_ledger = TrialLedger(
            (
                *(
                    item
                    for item in existing_ledger.records
                    if item.registration.trial_id != trial_id
                ),
                candidate_record,
            )
        )
        within_p = within_family_values or (p_value,)
        lifetime_sharpes = _lifetime_net_sharpes(
            existing_ledger, resolved_trial_family
        )
        assessment = assess_candidate_promotion(
            metrics,
            spec.validation,
            family=resolved_trial_family,
            trial_ledger=assessment_ledger,
            candidate_trial_id=trial_id,
            stitched_outer_oos_returns=stitched_excess,
            outer_fold_ids=[item["fold_id"] for item in fold_payloads],
            within_family_p_values=within_p,
            lifetime_trial_sharpes=(*lifetime_sharpes, float(metrics["net_sharpe"])),
            negative_control_results=negative_control_values,
            data_audit_blockers=tuple(dict.fromkeys(risk_promotion_blockers)),
            diagnostic_only=False,
            periods_per_year=periods_per_year,
            bootstrap_resamples=bootstrap_resamples,
            seed=seed,
        )
        promoted = assessment.promotion.promoted
        outcome = TrialOutcome.SUCCESS if promoted else TrialOutcome.FAILURE
        self.catalog.complete_trial(
            trial_id,
            experiment_id=experiment.experiment_id,
            outcome=outcome,
            reason=(
                "all deterministic promotion gates passed"
                if promoted
                else ",".join(assessment.promotion.failures)
            ),
            completed_at=datetime.now(timezone.utc),
            p_value=p_value,
            alpha_spent=(
                assessment.online_alpha[-1].alpha
                if assessment.online_alpha
                else 0.0
            ),
            metadata={
                "trial_kind": TrialKind.CONFIRMATORY.value,
                "evidence_class": admission.evidence_class.value,
                "experiment_fingerprint": fingerprint,
                "research_equivalence_hash": research_equivalence_hash,
                "hypothesis_id": spec.preregistration.hypothesis_id,
                "holdout_id": registration.holdout_id,
                "net_sharpe": metrics["net_sharpe"],
            },
        )
        lifecycle_state = LifecycleState.SHADOW if promoted else LifecycleState.WALK_FORWARD
        for state in (
            LifecycleState.PREREGISTERED,
            LifecycleState.CANARY,
            LifecycleState.WALK_FORWARD,
            *((LifecycleState.SHADOW,) if promoted else ()),
        ):
            self._append_lifecycle(spec.candidate_id, fingerprint, state)
        evidence_payload = _jsonable(assessment)
        result = ResearchCycleResult(
            experiment_id=experiment.experiment_id,
            fingerprint=fingerprint,
            status="completed",
            promotion_verdict=assessment.promotion.verdict,
            lifecycle_state=lifecycle_state.value,
            metrics=metrics,
            failures=assessment.promotion.failures,
            fold_results=tuple(fold_payloads),
            statistical_evidence=evidence_payload,
        )
        self.catalog.record_authoritative_result(
            experiment.experiment_id,
            outcome="promoted_to_shadow" if promoted else "falsified_or_insufficient",
            metrics=result.to_dict(),
        )
        return result

    def _deterministic_negative_control_metrics(
        self,
        *,
        spec: ExperimentSpec,
        frame: pd.DataFrame,
        signal: pd.Series,
        plan: Any,
        exposure_frame: pd.DataFrame | None,
        returns_history: pd.DataFrame | None,
        benchmark_weights: pd.DataFrame | None,
        optimization_policy: StockOptimizationPolicy | Mapping[str, Any] | None,
        seed: int,
    ) -> tuple[tuple[NegativeControlMetric, ...], tuple[str, ...]]:
        """Evaluate internally-derived controls through the exact same outer folds.

        These controls are deterministic functions of the fingerprint-bound
        Gold frame, signal and bootstrap seed.  They are diagnostics only and
        are never accepted from a production caller.
        """

        controls = generate_negative_control_signals(frame, signal, seed=seed)
        controls["reverse_preregistered_direction"] = reverse_direction_signal(signal)
        periods_per_year = (
            spec.validation.annualization_sessions
            / spec.portfolio.rebalance_sessions
        )
        results: list[NegativeControlMetric] = []
        blockers: list[str] = []
        for control_name, control_signal in sorted(controls.items()):
            fold_payloads: list[dict[str, Any]] = []
            risk_blockers: list[str] = []
            try:
                for nested in plan.outer_folds:
                    _consume_inner_folds(frame, control_signal, nested.inner)
                    train, test = select_fold_rows(frame, nested.outer)
                    location, scale, _ = _fit_inner_calibration(
                        control_signal.loc[train.index]
                    )
                    calibrated = (
                        pd.to_numeric(
                            control_signal.loc[test.index], errors="coerce"
                        )
                        - location
                    ) / scale
                    evaluation = self.evaluator.evaluate(
                        experiment_id=spec.fingerprint(),
                        snapshot_id=spec.snapshot.snapshot_id,
                        factor_or_sleeve_id=(
                            f"{spec.candidate_id}:negative_control:{control_name}"
                        ),
                        frame=test,
                        signal=calibrated,
                        portfolio_policy=spec.portfolio,
                        pricing_frame=frame,
                        exposure_frame=exposure_frame,
                        returns_history=returns_history,
                        benchmark_weights=benchmark_weights,
                        optimization_policy=optimization_policy,
                        production_contract=True,
                        environment_hash=spec.environment.code_hash,
                    )
                    payload = dict(evaluation.result)
                    if payload.get("status") != "ok":
                        raise ValueError(
                            str(payload.get("reason") or payload.get("status"))
                        )
                    if not bool(payload.get("promotion_eligible", False)):
                        risk_blockers.extend(
                            map(str, payload.get("promotion_blockers") or ())
                        )
                    fold_payloads.append(
                        {"fold_id": nested.outer.fold_id, **payload}
                    )
                metrics, _ = _stitched_metrics(
                    fold_payloads, periods_per_year=periods_per_year
                )
                evidence = PromotionEvidence(
                    data_audit_blockers=tuple(dict.fromkeys(risk_blockers)),
                    methodology_blockers=(),
                    statistical_budget_passed=True,
                    holm_passed=True,
                    deflated_sharpe_probability=1.0,
                    bootstrap_probability_positive=1.0,
                    negative_controls_passed=True,
                    required_outer_years=len(plan.outer_folds),
                    diagnostic_only=False,
                )
                decision = evaluate_promotion(
                    metrics, spec.validation.promotion_criteria, evidence
                )
                metric = float(metrics.get("net_sharpe") or 0.0)
                if not np.isfinite(metric):
                    metric = 0.0
                results.append(
                    NegativeControlMetric(
                        control_name=control_name,
                        metric=metric,
                        passed_promotion_gate=decision.promoted,
                    )
                )
            except Exception as exc:
                blockers.append(
                    f"negative_control_evaluation_failed:{control_name}:"
                    f"{type(exc).__name__}:{exc}"
                )
                results.append(
                    NegativeControlMetric(
                        control_name=control_name,
                        metric=0.0,
                        passed_promotion_gate=False,
                    )
                )
        return tuple(results), tuple(blockers)

    def _signal(
        self,
        spec: ExperimentSpec,
        frame: pd.DataFrame,
        field_specs: tuple[FieldSpec, ...],
        *,
        sleeve_signal: str | pd.Series | None,
    ) -> pd.Series:
        if spec.factor is not None:
            if not isinstance(spec.factor.expression, Mapping):
                raise ValueError("promotion-eligible factors require the typed DAG/DSL")
            registered_fields = tuple(
                FieldSpec(
                    name=item.name,
                    value_type=ValueType(item.value_type),
                    role=FieldRole(item.role),
                    availability=Availability(item.availability),
                    minimum_lag_sessions=item.minimum_lag_sessions,
                    available_at_column=item.available_at_column,
                )
                for item in spec.factor.signal_field_registry
            )
            if not registered_fields:
                raise ValueError(
                    "factor DSL requires a fingerprint-bound signal_field_registry"
                )
            if field_specs and field_specs != registered_fields:
                raise ValueError(
                    "runtime field registry differs from FactorSpec.signal_field_registry"
                )
            signal = evaluate_factor_graph(
                spec.factor.expression,
                frame,
                registered_fields,
                context=EvaluationContext(decision_point=DecisionPoint.AFTER_CLOSE),
            )
            return pd.to_numeric(signal, errors="coerce") * _factor_direction(spec)
        assert spec.sleeve is not None
        if sleeve_signal is not None:
            raise ValueError(
                "runtime sleeve_signal is forbidden; register signal_expression in SleeveSpec"
            )
        if spec.sleeve.signal_expression is None:
            raise ValueError("sleeve experiments require a preregistered typed signal_expression")
        registered_fields = tuple(
            FieldSpec(
                name=item.name,
                value_type=ValueType(item.value_type),
                role=FieldRole(item.role),
                availability=Availability(item.availability),
                minimum_lag_sessions=item.minimum_lag_sessions,
                available_at_column=item.available_at_column,
            )
            for item in spec.sleeve.signal_field_registry
        )
        if not registered_fields:
            raise ValueError(
                "sleeve DSL requires a fingerprint-bound signal_field_registry"
            )
        if field_specs and field_specs != registered_fields:
            raise ValueError(
                "runtime field registry differs from SleeveSpec.signal_field_registry"
            )
        signal = evaluate_factor_graph(
            spec.sleeve.signal_expression,
            frame,
            registered_fields,
            context=EvaluationContext(decision_point=DecisionPoint.AFTER_CLOSE),
        )
        return pd.to_numeric(signal, errors="coerce")

    def _blocked_result(
        self,
        spec: ExperimentSpec,
        experiment_id: str,
        trial_id: str,
        *,
        failures: tuple[str, ...],
        outcome: TrialOutcome,
        reason: str,
        lifecycle_state: LifecycleState = LifecycleState.WALK_FORWARD,
    ) -> ResearchCycleResult:
        now = datetime.now(timezone.utc)
        self.catalog.complete_trial(
            trial_id,
            experiment_id=experiment_id,
            outcome=outcome,
            reason=f"{reason}: {','.join(failures)}",
            completed_at=now,
            metadata={
                "trial_kind": TrialKind.CONFIRMATORY.value,
                "evidence_class": EvidenceClass.OBSERVED.value,
                "experiment_fingerprint": spec.fingerprint(),
                "hypothesis_id": spec.preregistration.hypothesis_id,
            },
        )
        self._append_lifecycle(
            spec.candidate_id, spec.fingerprint(), lifecycle_state
        )
        result = ResearchCycleResult(
            experiment_id=experiment_id,
            fingerprint=spec.fingerprint(),
            status="blocked",
            promotion_verdict="reject",
            lifecycle_state=lifecycle_state.value,
            metrics={},
            failures=tuple(failures),
            fold_results=(),
            statistical_evidence={},
        )
        self.catalog.record_authoritative_result(
            experiment_id,
            outcome="blocked",
            metrics=result.to_dict(),
        )
        return result

    def _append_lifecycle(
        self, candidate_id: str, fingerprint: str, state: LifecycleState
    ) -> None:
        self.catalog.append_lifecycle_event(
            LifecycleEvent(
                idempotency_key=f"{fingerprint}:{state.value}",
                sleeve_id=candidate_id,
                to_state=state,
                cause="deterministic_research_cycle",
                occurred_at=datetime.now(timezone.utc),
                evidence={"experiment_fingerprint": fingerprint},
            )
        )


def _is_finite_number(value: Any) -> bool:
    if isinstance(value, (bool, np.bool_)) or not isinstance(
        value, (int, float, np.integer, np.floating)
    ):
        return False
    return bool(np.isfinite(float(value)))


__all__ = [
    "FORBIDDEN_PROMOTION_TRUST_LABELS",
    "HistoricalResearchCycle",
    "ResearchCycleResult",
    "field_specs_from_mapping",
]

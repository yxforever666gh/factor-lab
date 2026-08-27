"""Execution orchestration for the causal walk-forward research suite."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import numpy as np
import pandas as pd

from factor_lab.portfolio import LongOnlyPortfolioConfig

from .contracts import FactorSpec
from .validation import FactorValidation
from .walk_forward import (
    WalkForwardSelectorSpec,
    build_dynamic_signal,
    causal_candidate_decisions,
    phase_distribution,
    walk_forward_offsets,
    walk_forward_phase_rankings,
)


PortfolioResult = Callable[..., dict[str, Any]]
HistoricalMetrics = Callable[..., dict[str, Any]]
_DYNAMIC_FACTOR_NAME = "causal_walk_forward_dynamic"


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    temporary.replace(path)


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object: {path}")
    return payload


def _safe_name(name: str) -> str:
    value = re.sub(r"[^A-Za-z0-9_.-]+", "_", name).strip("._") or "factor"
    return value[:100]


def _validate_candidate_artifact_names(
    candidate_names: Sequence[str],
    *,
    reserved_runtime_names: Sequence[str],
) -> None:
    """Reject portable path collisions and runtime-strategy name aliases."""

    safe_candidate_names = [
        _safe_name(name).casefold() for name in candidate_names
    ]
    if len(safe_candidate_names) != len(set(safe_candidate_names)):
        raise ValueError(
            "walk_forward candidate names collide after artifact-name normalization"
        )
    reserved_artifact_names = {
        _safe_name(name).casefold(): name for name in reserved_runtime_names
    }
    for candidate_name, artifact_name in zip(
        candidate_names, safe_candidate_names, strict=True
    ):
        reserved_name = reserved_artifact_names.get(artifact_name)
        if reserved_name is not None:
            raise ValueError(
                "walk_forward candidate name conflicts with reserved runtime "
                f"strategy {reserved_name!r}: {candidate_name!r}"
            )


def _decisions_sha256(decisions: Mapping[str, Any]) -> str:
    """Bind a cached deployed-account result to one canonical decision audit."""

    canonical = json.dumps(
        decisions,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _result_sha256(result: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        result,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _result_hash_matches(
    payload: Mapping[str, Any], result: Mapping[str, Any]
) -> bool:
    try:
        return payload.get("result_sha256") == _result_sha256(result)
    except (TypeError, ValueError):
        return False


def _cached_dynamic_result(
    payload: Mapping[str, Any],
    *,
    run_fingerprint: str,
    rebalance_offset_days: int,
    dynamic_factor: str,
    fresh_decisions: Mapping[str, Any],
    evaluation_start_date: str,
) -> dict[str, Any] | None:
    """Return a cache hit only when its decision audit matches the fresh audit."""

    result = payload.get("result")
    if not isinstance(result, Mapping):
        return None
    if (
        payload.get("run_fingerprint") != run_fingerprint
        or payload.get("rebalance_offset_days") != rebalance_offset_days
        or payload.get("role") != "equal_aum_dynamic_scoring_account"
        or payload.get("evaluation_start_date") != evaluation_start_date
        or payload.get("decisions_path") != "decisions.json"
        or "decisions" in payload
        or result.get("factor_name") != dynamic_factor
        or not result.get("account_nav_path")
        or not _result_hash_matches(payload, result)
    ):
        return None
    try:
        fresh_hash = _decisions_sha256(fresh_decisions)
    except (TypeError, ValueError):
        return None
    if payload.get("decisions_sha256") != fresh_hash:
        return None
    return dict(result)


def _fixed_comparator_protocol(settings: Mapping[str, Any]) -> dict[str, str]:
    """Resolve the only supported return-blind fixed-registry comparator."""

    raw = settings.get("fixed_comparator") or {}
    if not isinstance(raw, Mapping):
        raise ValueError("walk_forward fixed_comparator must be an object")
    unknown = sorted(
        set(raw) - {"name", "weighting", "missing_signal_policy"}
    )
    if unknown:
        raise ValueError(
            "unsupported walk_forward fixed_comparator settings: "
            + ", ".join(str(value) for value in unknown)
        )
    protocol = {
        "name": str(raw.get("name", "fixed_registry_equal_weight")),
        "weighting": str(raw.get("weighting", "equal")),
        "missing_signal_policy": str(
            raw.get("missing_signal_policy", "fallback_control")
        ),
    }
    expected = {
        "name": "fixed_registry_equal_weight",
        "weighting": "equal",
        "missing_signal_policy": "fallback_control",
    }
    if protocol != expected:
        raise ValueError(
            "walk_forward fixed_comparator must use "
            "name='fixed_registry_equal_weight', weighting='equal', and "
            "missing_signal_policy='fallback_control'"
        )
    return protocol


def _fixed_registry_equal_weight_signal(
    frame: pd.DataFrame,
    *,
    candidate_signals: Mapping[str, pd.Series],
    candidate_names: Sequence[str],
    control_factor: str,
) -> pd.Series:
    """Blend the frozen registry equally without consulting realized returns."""

    if (
        not candidate_names
        or candidate_names[0] != control_factor
        or len(candidate_names) != len(set(candidate_names))
    ):
        raise ValueError(
            "fixed_registry_equal_weight requires a unique registry starting with control"
        )
    control = candidate_signals.get(control_factor)
    if control is None or len(control) != len(frame):
        raise ValueError("fixed_registry_equal_weight control signal length mismatch")
    control_values = pd.to_numeric(control, errors="coerce")
    blended = pd.Series(0.0, index=frame.index, dtype=float)
    weight = 1.0 / len(candidate_names)
    for candidate_name in candidate_names:
        candidate = candidate_signals.get(candidate_name)
        if candidate is None or len(candidate) != len(frame):
            raise ValueError(
                "fixed_registry_equal_weight candidate signal length mismatch: "
                f"{candidate_name}"
            )
        candidate_values = pd.to_numeric(candidate, errors="coerce")
        blended = blended + weight * candidate_values.where(
            candidate_values.notna(), control_values
        )
    return blended.where(control_values.notna()).rename(
        "fixed_registry_equal_weight"
    )


def _cached_fixed_comparator_result(
    payload: Mapping[str, Any],
    *,
    run_fingerprint: str,
    rebalance_offset_days: int,
    comparator_factor: str,
    candidate_names: Sequence[str],
    evaluation_start_date: str,
) -> dict[str, Any] | None:
    """Validate one equal-AUM fixed-comparator scoring cache."""

    result = payload.get("result")
    if not isinstance(result, Mapping):
        return None
    if (
        payload.get("run_fingerprint") != run_fingerprint
        or payload.get("rebalance_offset_days") != rebalance_offset_days
        or payload.get("role") != "equal_aum_fixed_comparator_scoring_account"
        or payload.get("evaluation_start_date") != evaluation_start_date
        or tuple(payload.get("candidate_registry") or ()) != tuple(candidate_names)
        or result.get("factor_name") != comparator_factor
        or not result.get("account_nav_path")
        or not _result_hash_matches(payload, result)
    ):
        return None
    return dict(result)


def _cached_static_scoring_result(
    payload: Mapping[str, Any],
    *,
    run_fingerprint: str,
    rebalance_offset_days: int,
    factor_name: str,
    evaluation_start_date: str,
) -> dict[str, Any] | None:
    """Validate one equal-AUM static scoring account cache."""

    result = payload.get("result")
    if not isinstance(result, Mapping):
        return None
    if (
        payload.get("run_fingerprint") != run_fingerprint
        or payload.get("rebalance_offset_days") != rebalance_offset_days
        or payload.get("role") != "equal_aum_static_scoring_account"
        or payload.get("evaluation_start_date") != evaluation_start_date
        or result.get("factor_name") != factor_name
        or not result.get("account_nav_path")
        or not _result_hash_matches(payload, result)
    ):
        return None
    return dict(result)


def _runtime_composed_result(
    result: Mapping[str, Any],
    *,
    factor: FactorSpec,
    execution_validation: FactorValidation,
    composition_basis: str,
) -> dict[str, Any]:
    """Remove inherited control diagnostics from a runtime-composed signal."""

    sanitized = dict(result)

    def unavailable_window(window: Any) -> dict[str, Any]:
        return {
            "split": str(window.split),
            "start": str(window.start),
            "end": str(window.end) if window.end is not None else None,
            "status": "unavailable",
            "reason": "runtime_composed_signal",
        }

    sanitized["stage_a"] = {
        "factor_name": factor.name,
        "family": factor.family,
        "frozen_direction": 1,
        "label_column": execution_validation.label_column,
        "diagnostic_status": "unavailable",
        "diagnostic_reason": "runtime_composed_signal",
        "train": unavailable_window(execution_validation.train),
        "validation": unavailable_window(execution_validation.validation),
        "audit": unavailable_window(execution_validation.audit),
        "direction_consistent": None,
        "stage_b_eligible": None,
        "blockers": ["runtime_composed_signal_stage_a_unavailable"],
        "selection_basis": "runtime_composed",
        "composition_basis": composition_basis,
        "audit_signal_failures": [],
    }
    sanitized["stage_a_metadata_status"] = "unavailable"
    sanitized["stage_a_metadata_reason"] = "runtime_composed_signal"
    windows = {
        str(name): dict(value)
        for name, value in (sanitized.get("windows") or {}).items()
        if isinstance(value, Mapping)
    }
    for window in windows.values():
        window["signal_evaluable_date_ratio"] = None
        window["signal_median_cross_section_coverage"] = None
        window["signal_diagnostic_status"] = "unavailable"
        window["signal_diagnostic_reason"] = "runtime_composed_signal"
    sanitized["windows"] = windows
    sanitized["gate_passed"] = None
    sanitized["gate_status"] = "not_applicable_runtime_composed"
    sanitized["gate_blockers"] = sorted(
        {
            *[str(value) for value in sanitized.get("gate_blockers") or []],
            "runtime_composed_signal_stage_a_unavailable",
        }
    )
    sanitized["audit_role"] = "not_applicable_runtime_composed"
    sanitized["audit_status"] = "not_applicable"
    sanitized["audit_falsified"] = None
    sanitized["audit_falsification_reasons"] = [
        "runtime_composed_signal_stage_a_unavailable"
    ]
    sanitized["validated"] = False
    return sanitized


def _selection_frequency(
    decisions: Mapping[str, Any], candidate_names: Sequence[str]
) -> tuple[dict[str, int], dict[str, dict[str, float | int]]]:
    """Summarize Top-K membership and actual equal-weight deployed exposure."""

    selections = list(decisions.get("selections") or [])
    selected_counts = {str(name): 0 for name in candidate_names}
    deployed_weight_sums = {str(name): 0.0 for name in candidate_names}
    for row in selections:
        selected = tuple(
            str(name)
            for name in (
                row.get("selected_factors")
                or [row.get("selected_factor")]
            )
            if name is not None
        )
        if not selected or len(selected) != len(set(selected)):
            raise ValueError("walk_forward selection audit has invalid selected factors")
        unknown = sorted(set(selected) - set(selected_counts))
        if unknown:
            raise ValueError(
                "walk_forward selection audit has unknown factors: "
                + ", ".join(unknown)
            )
        configured_weights = dict(row.get("selected_weights") or {})
        if configured_weights and set(configured_weights) != set(selected):
            raise ValueError(
                "walk_forward selection audit weights must cover selected factors"
            )
        weights = np.asarray(
            [
                float(configured_weights.get(name, 1.0 / len(selected)))
                for name in selected
            ],
            dtype=float,
        )
        if (
            not np.isfinite(weights).all()
            or bool((weights < 0.0).any())
            or float(weights.sum()) <= 0.0
        ):
            raise ValueError("walk_forward selection audit has invalid weights")
        weights = weights / float(weights.sum())
        for name, weight in zip(selected, weights, strict=True):
            selected_counts[name] += 1
            deployed_weight_sums[name] += float(weight)

    date_count = len(selections)
    frequency = {
        name: {
            "selected_date_count": int(selected_counts[name]),
            "selected_date_ratio": round(selected_counts[name] / date_count, 8)
            if date_count
            else 0.0,
            "mean_deployed_weight": round(deployed_weight_sums[name] / date_count, 8)
            if date_count
            else 0.0,
        }
        for name in selected_counts
    }
    return selected_counts, frequency


def _phase_deltas(
    left_metrics: Sequence[Mapping[str, Any]],
    right_metrics: Sequence[Mapping[str, Any]],
    *,
    phase_quantile: float,
) -> tuple[dict[str, dict[str, float]], float]:
    """Summarize paired phase deltas for two strategies over identical offsets."""

    metric_names = (
        "net_annual_return",
        "net_sharpe",
        "information_ratio",
        "max_drawdown",
    )
    raw_deltas: dict[str, list[float]] = {name: [] for name in metric_names}
    for left, right in zip(left_metrics, right_metrics, strict=True):
        for name in metric_names:
            left_value = left.get(name)
            right_value = right.get(name)
            if left_value is None or right_value is None:
                continue
            delta = float(left_value) - float(right_value)
            if np.isfinite(delta):
                raw_deltas[name].append(delta)
    annual_deltas = raw_deltas["net_annual_return"]
    positive_annual_ratio = (
        round(float(np.mean(np.asarray(annual_deltas) > 0.0)), 8)
        if annual_deltas
        else 0.0
    )
    return (
        {
            name: phase_distribution(values, phase_quantile)
            for name, values in raw_deltas.items()
        },
        positive_annual_ratio,
    )


def _equal_aum_account_audit(
    result: Mapping[str, Any],
    *,
    requested_start_date: str,
    initial_nav: float,
) -> dict[str, Any]:
    """Audit that one scoring account was born at the common cash boundary."""

    portfolio = result.get("portfolio") or {}
    actual_start = portfolio.get("evaluation_start_date")
    observed_initial = portfolio.get("initial_nav")
    first_pretrade = portfolio.get("first_pretrade_nav")
    end_nav = portfolio.get("end_nav")
    reasons: list[str] = []
    if portfolio.get("status") != "ok":
        reasons.append("scoring_portfolio_status_not_ok")
    try:
        if actual_start is None or pd.Timestamp(str(actual_start)) < pd.Timestamp(
            requested_start_date
        ):
            reasons.append("scoring_account_started_before_common_boundary")
    except (TypeError, ValueError):
        reasons.append("scoring_account_start_date_invalid")
    for name, value in (
        ("initial_nav", observed_initial),
        ("first_pretrade_nav", first_pretrade),
    ):
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            parsed = np.nan
        if not np.isfinite(parsed) or not np.isclose(
            parsed, float(initial_nav), rtol=0.0, atol=0.01
        ):
            reasons.append(f"scoring_account_{name}_mismatch")
    try:
        parsed_end = float(end_nav)
    except (TypeError, ValueError):
        parsed_end = np.nan
    if not np.isfinite(parsed_end) or parsed_end <= 0.0:
        reasons.append("scoring_account_end_nav_invalid")
    promotion_blockers = {
        str(value) for value in portfolio.get("promotion_blockers") or ()
    }
    if "unresolved_stale_position_observed" in promotion_blockers:
        reasons.append("scoring_account_unresolved_stale_position")
    try:
        reconciliation_error = float(
            portfolio.get("account_nav_reconciliation_error")
        )
    except (TypeError, ValueError):
        reconciliation_error = np.nan
    if not np.isfinite(reconciliation_error) or not np.isclose(
        reconciliation_error, 0.0, rtol=0.0, atol=1e-10
    ):
        reasons.append("scoring_account_nav_reconciliation_failed")

    account_nav_path = list(result.get("account_nav_path") or [])
    first_path_observation = (
        dict(account_nav_path[0]) if account_nav_path else {}
    )
    last_path_observation = (
        dict(account_nav_path[-1]) if account_nav_path else {}
    )
    if not account_nav_path:
        reasons.append("scoring_account_nav_path_missing")
    else:
        try:
            path_start_date = pd.Timestamp(
                str(first_path_observation.get("date"))
            )
        except (TypeError, ValueError):
            path_start_date = pd.NaT
        if (
            pd.isna(path_start_date)
            or path_start_date != pd.Timestamp(requested_start_date)
            or first_path_observation.get("phase") != "accounting_boundary"
            or first_path_observation.get("sequence") != 0
        ):
            reasons.append("scoring_account_nav_path_common_boundary_mismatch")
        for reason, value, expected in (
            (
                "scoring_account_nav_path_initial_nav_mismatch",
                first_path_observation.get("nav"),
                initial_nav,
            ),
            (
                "scoring_account_nav_path_end_nav_mismatch",
                last_path_observation.get("nav"),
                parsed_end,
            ),
        ):
            try:
                parsed_value = float(value)
            except (TypeError, ValueError):
                parsed_value = np.nan
            if not np.isfinite(parsed_value) or not np.isclose(
                parsed_value, float(expected), rtol=0.0, atol=0.01
            ):
                reasons.append(reason)

    # Scoring accounts may legitimately retain cash when an order is blocked
    # by a limit or suspension.  They may not, however, enter the phase
    # comparison when the inputs needed to size an executable order are
    # incomplete, drawn from the future, or breach the declared ADV cap.  The
    # predefined result windows partition the fresh account's common-start
    # history, so audit every non-empty slice and preserve the observed values
    # in the artifact instead of reducing the failure to a boolean.
    execution_integrity_windows: list[dict[str, Any]] = []
    for window_name, raw_window in sorted(
        dict(result.get("windows") or {}).items()
    ):
        window = dict(raw_window or {})
        try:
            observations = int(window.get("observations") or 0)
        except (TypeError, ValueError):
            observations = 0
        if observations <= 0:
            continue

        def finite_window_number(name: str) -> float | None:
            try:
                value = float(window.get(name))
            except (TypeError, ValueError):
                return None
            return value if np.isfinite(value) else None

        coverage = finite_window_number("execution_input_coverage")
        future_violations = finite_window_number(
            "execution_input_future_violation_count"
        )
        capacity_violations = finite_window_number("capacity_violation_count")
        window_reasons: list[dict[str, Any]] = []
        if coverage != 1.0:
            window_reasons.append(
                {
                    "reason": "execution_input_coverage_not_one",
                    "observed_value": round(coverage, 8)
                    if coverage is not None
                    else None,
                    "required_value": 1.0,
                }
            )
        if future_violations != 0.0:
            window_reasons.append(
                {
                    "reason": "execution_input_future_violation_count_not_zero",
                    "observed_value": int(future_violations)
                    if future_violations is not None
                    else None,
                    "required_value": 0,
                }
            )
        if capacity_violations != 0.0:
            window_reasons.append(
                {
                    "reason": "capacity_violation_count_not_zero",
                    "observed_value": int(capacity_violations)
                    if capacity_violations is not None
                    else None,
                    "required_value": 0,
                }
            )
        execution_integrity_windows.append(
            {
                "window": str(window_name),
                "observations": observations,
                "execution_input_coverage": round(coverage, 8)
                if coverage is not None
                else None,
                "execution_input_future_violation_count": (
                    int(future_violations)
                    if future_violations is not None
                    else None
                ),
                "capacity_violation_count": int(capacity_violations)
                if capacity_violations is not None
                else None,
                "valid": not window_reasons,
                "exclusion_reasons": window_reasons,
            }
        )
        reasons.extend(
            f"scoring_common_window_{item['reason']}"
            for item in window_reasons
        )
    if not execution_integrity_windows:
        reasons.append("scoring_common_window_has_no_observations")

    reasons = list(dict.fromkeys(reasons))
    return {
        "valid": not reasons,
        "requested_start_date": requested_start_date,
        "actual_first_signal_date": actual_start,
        "initial_nav": observed_initial,
        "first_pretrade_nav": first_pretrade,
        "end_nav": end_nav,
        "account_nav_reconciliation_error": portfolio.get(
            "account_nav_reconciliation_error"
        ),
        "account_nav_path_start": first_path_observation or None,
        "account_nav_path_end": last_path_observation or None,
        "common_window_execution_integrity": execution_integrity_windows,
        "reasons": reasons,
    }


def run_walk_forward_sweep(
    *,
    factors: Sequence[FactorSpec],
    validations: Mapping[str, FactorValidation],
    signals: Mapping[str, pd.Series],
    features: pd.DataFrame,
    execution: pd.DataFrame,
    base_config: LongOnlyPortfolioConfig,
    research_config: Mapping[str, Any],
    base_results: Sequence[Mapping[str, Any]],
    control: FactorSpec,
    output_dir: Path,
    run_fingerprint: str,
    resume: bool,
    portfolio_result: PortfolioResult,
    historical_metrics: HistoricalMetrics,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Run causal shadow candidates and one deployed account for every offset."""

    if base_config.evaluation_start_date is not None:
        raise ValueError(
            "walk-forward base_config.evaluation_start_date must be null; "
            "selector shadows require complete history"
        )
    settings = dict(research_config.get("walk_forward") or {})
    selector = WalkForwardSelectorSpec.from_mapping(settings.get("selector") or {})
    fixed_comparator_protocol = _fixed_comparator_protocol(settings)
    offsets = walk_forward_offsets(settings, base_config.rebalance_every_days)
    phase_quantile = float(settings.get("phase_quantile", 0.20))
    if not np.isfinite(phase_quantile) or not 0.0 < phase_quantile <= 0.5:
        raise ValueError("walk_forward phase_quantile must be in (0, 0.5]")
    benchmark_coverage_minimum = float(
        (research_config.get("promotion_gate") or {}).get(
            "benchmark_return_coverage_min", 0.95
        )
    )
    if not np.isfinite(benchmark_coverage_minimum) or not (
        0.0 <= benchmark_coverage_minimum <= 1.0
    ):
        raise ValueError("benchmark_return_coverage_min must be in [0, 1]")
    factor_by_name = {factor.name: factor for factor in factors}
    candidate_names = [factor.name for factor in factors]
    if (
        not candidate_names
        or candidate_names[0] != control.name
        or len(candidate_names) != len(set(candidate_names))
    ):
        raise ValueError("walk_forward candidate registry must start with unique control")
    reserved_runtime_names = (
        fixed_comparator_protocol["name"],
        _DYNAMIC_FACTOR_NAME,
    )
    _validate_candidate_artifact_names(
        candidate_names,
        reserved_runtime_names=reserved_runtime_names,
    )
    base_by_name = {str(row["factor_name"]): dict(row) for row in base_results}
    missing_base = [name for name in candidate_names if name not in base_by_name]
    if missing_base:
        raise ValueError(
            "walk_forward base results missing candidates: " + ", ".join(missing_base)
        )
    trading_dates = pd.DatetimeIndex(
        sorted(
            pd.to_datetime(
                features[base_config.date_column], errors="coerce"
            ).dropna().unique()
        )
    )
    root = output_dir / "walk-forward"
    root.mkdir(parents=True, exist_ok=True)
    dynamic_name = _DYNAMIC_FACTOR_NAME
    dynamic_factor = FactorSpec(
        name=dynamic_name,
        family="walk_forward",
        kind="ensemble",
        direction_policy="pre_directed",
        params={
            "candidate_registry": candidate_names,
            "selector": selector.to_dict(),
            "selection_history_policy": selector.history_policy,
        },
        role="walk_forward_dynamic",
    )
    dynamic_validation = replace(
        validations[control.name],
        factor_name=dynamic_name,
        family="walk_forward",
        frozen_direction=1,
        selection_basis="causal_completed_portfolio_history",
    )
    fixed_comparator_name = fixed_comparator_protocol["name"]
    fixed_comparator_factor = FactorSpec(
        name=fixed_comparator_name,
        family="walk_forward_comparator",
        kind="ensemble",
        direction_policy="pre_directed",
        params={
            "candidate_registry": candidate_names,
            **fixed_comparator_protocol,
            "selection_role": "return_blind_fixed_registry_comparator",
        },
        role="fixed_registry_comparator",
    )
    fixed_comparator_validation = replace(
        validations[control.name],
        factor_name=fixed_comparator_name,
        family="walk_forward_comparator",
        frozen_direction=1,
        selection_basis="fixed_registry_equal_weight_no_return_selection",
    )
    fixed_comparator_signal = _fixed_registry_equal_weight_signal(
        features,
        candidate_signals=signals,
        candidate_names=candidate_names,
        control_factor=control.name,
    )
    offset_payloads: list[dict[str, Any]] = []

    for offset in offsets:
        offset_dir = root / f"offset-{offset:02d}"
        static_dir = offset_dir / "static"
        static_dir.mkdir(parents=True, exist_ok=True)
        # Selector shadows must retain the complete pre-boundary history even
        # when a caller reuses a scoring-oriented base configuration.
        config = replace(
            base_config,
            rebalance_offset_days=offset,
            evaluation_start_date=None,
        )
        static_results: dict[str, dict[str, Any]] = {}
        for factor_name in candidate_names:
            factor = factor_by_name[factor_name]
            path = static_dir / f"{_safe_name(factor_name)}.json"
            cached: dict[str, Any] | None = None
            if resume and path.is_file():
                try:
                    payload = _read_json(path)
                except (OSError, ValueError, TypeError, json.JSONDecodeError):
                    payload = {}
                if (
                    payload.get("run_fingerprint") == run_fingerprint
                    and payload.get("rebalance_offset_days") == offset
                    and payload.get("role") == "causal_shadow_candidate"
                    and payload.get("evaluation_start_date") is None
                    and (payload.get("result") or {}).get("factor_name")
                    == factor_name
                    and _result_hash_matches(
                        payload, payload.get("result") or {}
                    )
                ):
                    cached = payload.get("result")
            if cached is None:
                cached = (
                    dict(base_by_name[factor_name])
                    if offset == base_config.rebalance_offset_days
                    else portfolio_result(
                        factor,
                        validations[factor_name],
                        signals[factor_name],
                        features,
                        execution,
                        config,
                        research_config,
                    )
                )
                _write_json(
                    path,
                    {
                        "run_fingerprint": run_fingerprint,
                        "rebalance_offset_days": offset,
                        "role": "causal_shadow_candidate",
                        "evaluation_start_date": None,
                        "result_sha256": _result_sha256(cached),
                        "result": cached,
                    },
                )
            static_results[factor_name] = cached

        control_periods = list(
            static_results[control.name].get("period_active_returns") or []
        )
        decisions = causal_candidate_decisions(
            trading_dates=trading_dates,
            signal_dates=[pd.Timestamp(row["signal_date"]) for row in control_periods],
            candidate_periods={
                name: list(result.get("period_active_returns") or [])
                for name, result in static_results.items()
            },
            control_factor=control.name,
            selector=selector,
            periods_per_year=base_config.periods_per_year,
        )
        decisions_hash = _decisions_sha256(decisions)
        _write_json(
            offset_dir / "decisions.json",
            {
                "run_fingerprint": run_fingerprint,
                "rebalance_offset_days": offset,
                "role": "causal_selector_audit",
                "decisions_sha256": decisions_hash,
                "decisions": decisions,
            },
        )
        ready_updates = [
            row
            for row in decisions.get("updates") or []
            if int(row.get("reference_observations") or 0)
            >= selector.minimum_completed_periods
        ]
        offset_payloads.append(
            {
                "rebalance_offset_days": offset,
                "ready_date": ready_updates[0]["decision_date"]
                if ready_updates
                else None,
                "decisions_sha256": decisions_hash,
                "decisions": decisions,
                "static_results": static_results,
            }
        )

    ready_dates = [
        pd.Timestamp(row["ready_date"])
        for row in offset_payloads
        if row.get("ready_date")
    ]
    common_start = max(ready_dates) if len(ready_dates) == len(offsets) else None
    if common_start is None:
        raise RuntimeError(
            "walk-forward cannot create equal-AUM scoring accounts without a "
            "selector-ready date for every configured offset"
        )
    common_start_date = common_start.date().isoformat()
    equal_aum_account_audits: list[dict[str, Any]] = []
    for payload in offset_payloads:
        offset = int(payload["rebalance_offset_days"])
        offset_dir = root / f"offset-{offset:02d}"
        scoring_dir = offset_dir / "scoring"
        scoring_static_dir = scoring_dir / "static"
        scoring_static_dir.mkdir(parents=True, exist_ok=True)
        scoring_config = replace(
            base_config,
            rebalance_offset_days=offset,
            evaluation_start_date=common_start_date,
        )
        scoring_static_results: dict[str, dict[str, Any]] = {}
        scoring_audits: list[dict[str, Any]] = []
        for factor_name in candidate_names:
            factor = factor_by_name[factor_name]
            path = scoring_static_dir / f"{_safe_name(factor_name)}.json"
            scoring_result: dict[str, Any] | None = None
            if resume and path.is_file():
                try:
                    cached_payload = _read_json(path)
                except (OSError, ValueError, TypeError, json.JSONDecodeError):
                    cached_payload = {}
                scoring_result = _cached_static_scoring_result(
                    cached_payload,
                    run_fingerprint=run_fingerprint,
                    rebalance_offset_days=offset,
                    factor_name=factor_name,
                    evaluation_start_date=common_start_date,
                )
            if scoring_result is None:
                scoring_result = portfolio_result(
                    factor,
                    validations[factor_name],
                    signals[factor_name],
                    features,
                    execution,
                    scoring_config,
                    research_config,
                )
                _write_json(
                    path,
                    {
                        "run_fingerprint": run_fingerprint,
                        "rebalance_offset_days": offset,
                        "role": "equal_aum_static_scoring_account",
                        "evaluation_start_date": common_start_date,
                        "shadow_history_path": (
                            f"../../static/{_safe_name(factor_name)}.json"
                        ),
                        "result_sha256": _result_sha256(scoring_result),
                        "result": scoring_result,
                    },
                )
            scoring_static_results[factor_name] = scoring_result
            audit = _equal_aum_account_audit(
                scoring_result,
                requested_start_date=common_start_date,
                initial_nav=base_config.capital,
            )
            audit.update(
                {
                    "rebalance_offset_days": offset,
                    "factor_name": factor_name,
                    "account_role": "static_scoring",
                }
            )
            scoring_audits.append(audit)

        fixed_comparator_path = offset_dir / "fixed-comparator.json"
        fixed_comparator_result: dict[str, Any] | None = None
        if resume and fixed_comparator_path.is_file():
            try:
                cached_payload = _read_json(fixed_comparator_path)
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                cached_payload = {}
            fixed_comparator_result = _cached_fixed_comparator_result(
                cached_payload,
                run_fingerprint=run_fingerprint,
                rebalance_offset_days=offset,
                comparator_factor=fixed_comparator_name,
                candidate_names=candidate_names,
                evaluation_start_date=common_start_date,
            )
        if fixed_comparator_result is None:
            fixed_comparator_result = portfolio_result(
                fixed_comparator_factor,
                fixed_comparator_validation,
                fixed_comparator_signal,
                features,
                execution,
                scoring_config,
                research_config,
            )
            fixed_comparator_result = _runtime_composed_result(
                fixed_comparator_result,
                factor=fixed_comparator_factor,
                execution_validation=fixed_comparator_validation,
                composition_basis="fixed_registry_equal_weight_no_return_selection",
            )
            _write_json(
                fixed_comparator_path,
                {
                    "run_fingerprint": run_fingerprint,
                    "rebalance_offset_days": offset,
                    "role": "equal_aum_fixed_comparator_scoring_account",
                    "evaluation_start_date": common_start_date,
                    "candidate_registry": candidate_names,
                    "protocol": fixed_comparator_protocol,
                    "result_sha256": _result_sha256(
                        fixed_comparator_result
                    ),
                    "result": fixed_comparator_result,
                },
            )
        fixed_audit = _equal_aum_account_audit(
            fixed_comparator_result,
            requested_start_date=common_start_date,
            initial_nav=base_config.capital,
        )
        fixed_audit.update(
            {
                "rebalance_offset_days": offset,
                "factor_name": fixed_comparator_name,
                "account_role": "fixed_comparator_scoring",
            }
        )
        scoring_audits.append(fixed_audit)

        decisions = payload["decisions"]
        dynamic_path = offset_dir / "dynamic.json"
        dynamic_result: dict[str, Any] | None = None
        if resume and dynamic_path.is_file():
            try:
                cached_payload = _read_json(dynamic_path)
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                cached_payload = {}
            dynamic_result = _cached_dynamic_result(
                cached_payload,
                run_fingerprint=run_fingerprint,
                rebalance_offset_days=offset,
                dynamic_factor=dynamic_name,
                fresh_decisions=decisions,
                evaluation_start_date=common_start_date,
            )
        if dynamic_result is None:
            dynamic_signal = build_dynamic_signal(
                features,
                candidate_signals=signals,
                decisions=decisions,
                control_factor=control.name,
                date_column=base_config.date_column,
            ).rename(dynamic_name)
            dynamic_result = portfolio_result(
                dynamic_factor,
                dynamic_validation,
                dynamic_signal,
                features,
                execution,
                scoring_config,
                research_config,
            )
            dynamic_result = _runtime_composed_result(
                dynamic_result,
                factor=dynamic_factor,
                execution_validation=dynamic_validation,
                composition_basis="causal_completed_portfolio_history",
            )
            _write_json(
                dynamic_path,
                {
                    "run_fingerprint": run_fingerprint,
                    "rebalance_offset_days": offset,
                    "role": "equal_aum_dynamic_scoring_account",
                    "evaluation_start_date": common_start_date,
                    "decisions_sha256": payload["decisions_sha256"],
                    "decisions_path": "decisions.json",
                    "result_sha256": _result_sha256(dynamic_result),
                    "result": dynamic_result,
                },
            )
        dynamic_audit = _equal_aum_account_audit(
            dynamic_result,
            requested_start_date=common_start_date,
            initial_nav=base_config.capital,
        )
        dynamic_audit.update(
            {
                "rebalance_offset_days": offset,
                "factor_name": dynamic_name,
                "account_role": "dynamic_scoring",
            }
        )
        scoring_audits.append(dynamic_audit)
        equal_aum_account_audits.extend(scoring_audits)
        payload["scoring_static_results"] = scoring_static_results
        payload["fixed_comparator_result"] = fixed_comparator_result
        payload["dynamic_result"] = dynamic_result
        payload["scoring_account_audits"] = scoring_audits

    metrics_by_strategy: dict[str, list[dict[str, Any]]] = {
        name: []
        for name in [*candidate_names, fixed_comparator_name, dynamic_name]
    }
    compact_offsets: list[dict[str, Any]] = []
    future_violations = 0
    for payload in offset_payloads:
        control_periods = list(
            payload["scoring_static_results"][control.name].get(
                "period_active_returns"
            )
            or []
        )
        audit_by_strategy = {
            str(row.get("factor_name")): row
            for row in payload["scoring_account_audits"]
        }

        def attach_scoring_audit(
            metrics: dict[str, Any], strategy_name: str
        ) -> dict[str, Any]:
            audit = dict(audit_by_strategy.get(strategy_name) or {})
            metrics["equal_aum_account_audit_valid"] = audit.get("valid") is True
            metrics["equal_aum_account_audit_reasons"] = list(
                audit.get("reasons") or []
            )
            metrics["equal_aum_account_execution_integrity"] = list(
                audit.get("common_window_execution_integrity") or []
            )
            return metrics

        per_strategy: dict[str, dict[str, Any]] = {}
        for name in candidate_names:
            metrics = historical_metrics(
                payload["scoring_static_results"][name],
                research_config,
                periods_per_year=base_config.periods_per_year,
                reference_periods=control_periods,
                optimization_scope="post_selection_causal_simulation",
            )
            metrics = attach_scoring_audit(metrics, name)
            metrics_by_strategy[name].append(metrics)
            per_strategy[name] = metrics
        fixed_comparator_metrics = historical_metrics(
            payload["fixed_comparator_result"],
            research_config,
            periods_per_year=base_config.periods_per_year,
            reference_periods=control_periods,
            optimization_scope="post_selection_causal_simulation",
        )
        fixed_comparator_metrics = attach_scoring_audit(
            fixed_comparator_metrics, fixed_comparator_name
        )
        metrics_by_strategy[fixed_comparator_name].append(
            fixed_comparator_metrics
        )
        per_strategy[fixed_comparator_name] = fixed_comparator_metrics
        dynamic_metrics = historical_metrics(
            payload["dynamic_result"],
            research_config,
            periods_per_year=base_config.periods_per_year,
            reference_periods=control_periods,
            optimization_scope="post_selection_causal_simulation",
        )
        dynamic_metrics = attach_scoring_audit(dynamic_metrics, dynamic_name)
        metrics_by_strategy[dynamic_name].append(dynamic_metrics)
        per_strategy[dynamic_name] = dynamic_metrics
        decisions = payload["decisions"]
        future_violations += int(decisions["future_selection_violation_count"])
        selection_counts, selection_frequency = _selection_frequency(
            decisions, candidate_names
        )
        compact_offsets.append(
            {
                "rebalance_offset_days": payload["rebalance_offset_days"],
                "ready_date": payload["ready_date"],
                "decisions_sha256": payload["decisions_sha256"],
                "signal_date_count": decisions["signal_date_count"],
                "update_count": decisions["update_count"],
                "switch_count": decisions["switch_count"],
                "future_selection_violation_count": decisions[
                    "future_selection_violation_count"
                ],
                # Backward-compatible counts now have an explicit denominator.
                "selection_counts": selection_counts,
                "selection_counts_semantics": "selected_signal_date_count_legacy",
                "selection_frequency_basis": "all_signal_dates_in_offset",
                "selection_frequency": selection_frequency,
                "scoring_account_audits": payload["scoring_account_audits"],
                "metrics": per_strategy,
            }
        )

    rankings = walk_forward_phase_rankings(
        metrics_by_strategy,
        control_factor=control.name,
        dynamic_factor=dynamic_name,
        phase_quantile=phase_quantile,
        benchmark_return_coverage_minimum=benchmark_coverage_minimum,
    )
    for row in rankings:
        if row["strategy_name"] == fixed_comparator_name:
            row["strategy_kind"] = "fixed_registry_comparator"
    eligible_rankings = [
        row for row in rankings if bool(row.get("phase_ranking_eligible"))
    ]
    excluded_from_phase_ranking = [
        {
            "strategy_name": row["strategy_name"],
            "strategy_kind": row["strategy_kind"],
            "phase_ranking_exclusion_reasons": list(
                row.get("phase_ranking_exclusion_reasons") or []
            ),
        }
        for row in rankings
        if not bool(row.get("phase_ranking_eligible"))
    ]
    dynamic_ranking = next(
        (row for row in rankings if row["strategy_name"] == dynamic_name), None
    )
    control_ranking = next(
        (row for row in rankings if row["strategy_name"] == control.name), None
    )
    fixed_comparator_ranking = next(
        (
            row
            for row in rankings
            if row["strategy_name"] == fixed_comparator_name
        ),
        None,
    )
    dynamic_metrics = (
        dynamic_ranking["phase_metrics"] if dynamic_ranking is not None else {}
    )
    dynamic_deltas = (
        dynamic_ranking["phase_deltas_vs_control"]
        if dynamic_ranking is not None
        else {}
    )
    dynamic_phase_eligible = bool(
        dynamic_ranking is not None
        and dynamic_ranking.get("phase_ranking_eligible")
    )
    control_phase_eligible = bool(
        control_ranking is not None
        and control_ranking.get("phase_ranking_eligible")
    )
    dynamic_control_common_offset_count = len(
        (dynamic_ranking or {}).get("common_complete_rebalance_offsets") or []
    )
    fixed_comparator_phase_eligible = bool(
        fixed_comparator_ranking is not None
        and fixed_comparator_ranking.get("phase_ranking_eligible")
    )
    (
        dynamic_deltas_vs_fixed_comparator,
        positive_annual_delta_vs_fixed_comparator_ratio,
    ) = _phase_deltas(
        metrics_by_strategy[dynamic_name],
        metrics_by_strategy[fixed_comparator_name],
        phase_quantile=phase_quantile,
    )
    full_coverage = all(
        float(row.get("period_coverage") or 0.0) >= 1.0
        for row in metrics_by_strategy[dynamic_name]
    )
    expected_scoring_account_count = len(offsets) * (len(candidate_names) + 2)
    equal_aum_violations = [
        row for row in equal_aum_account_audits if not bool(row.get("valid"))
    ]
    equal_aum_scoring_valid = bool(
        len(equal_aum_account_audits) == expected_scoring_account_count
        and not equal_aum_violations
    )
    causal_valid = bool(
        common_start is not None
        and future_violations == 0
        and len(offset_payloads) == base_config.rebalance_every_days
        and full_coverage
        and equal_aum_scoring_valid
    )
    # This is a same-sample threshold diagnostic across correlated offsets,
    # not an independent reliability confirmation or an OOS validation.
    historical_diagnostic_passed = bool(
        causal_valid
        and control_phase_eligible
        and dynamic_phase_eligible
        and fixed_comparator_phase_eligible
        and dynamic_control_common_offset_count == len(offsets)
        and dynamic_ranking is not None
        and float((dynamic_metrics.get("net_annual_return") or {}).get("q20", -1.0))
        > 0.0
        and float((dynamic_metrics.get("net_sharpe") or {}).get("q20", -1.0))
        > 0.0
        and float(
            (dynamic_deltas.get("net_annual_return") or {}).get("q20", -1.0)
        )
        >= 0.0
        and float((dynamic_deltas.get("net_sharpe") or {}).get("q20", -1.0))
        >= 0.0
        and float((dynamic_deltas.get("max_drawdown") or {}).get("q20", -1.0))
        >= -0.02
        and float(dynamic_ranking["positive_annual_return_delta_ratio"]) >= 0.8
        and float(
            (dynamic_deltas_vs_fixed_comparator.get("net_annual_return") or {}).get(
                "q20", -1.0
            )
        )
        >= 0.0
        and float(
            (dynamic_deltas_vs_fixed_comparator.get("net_sharpe") or {}).get(
                "q20", -1.0
            )
        )
        >= 0.0
        and float(
            (dynamic_deltas_vs_fixed_comparator.get("max_drawdown") or {}).get(
                "q20", -1.0
            )
        )
        >= -0.02
        and positive_annual_delta_vs_fixed_comparator_ratio >= 0.8
    )
    aggregate_decisions = {
        "selections": [
            selection
            for payload in offset_payloads
            for selection in payload["decisions"].get("selections") or []
        ]
    }
    aggregate_selection_counts, aggregate_selection_frequency = _selection_frequency(
        aggregate_decisions, candidate_names
    )
    summary = {
        "enabled": True,
        "protocol": "causal_walk_forward",
        "evidence_class": "post_selection_causal_simulation",
        "canary_smoke_only": False,
        "selector_executed": True,
        "ranking_available": True,
        "selector": selector.to_dict(),
        "candidate_registry": candidate_names,
        "fixed_comparator_factor": fixed_comparator_name,
        "fixed_comparator": {
            "factor_name": fixed_comparator_name,
            "protocol": fixed_comparator_protocol,
            "candidate_registry": candidate_names,
            "uses_realized_returns": False,
            "independent_cost_account_per_offset": True,
            "equal_aum_common_start_scoring": True,
            "phase_ranking_eligible": fixed_comparator_phase_eligible,
            "phase_rank": fixed_comparator_ranking.get("rank")
            if fixed_comparator_ranking is not None
            else None,
            "dynamic_phase_deltas": dynamic_deltas_vs_fixed_comparator,
            "dynamic_positive_annual_return_delta_ratio": (
                positive_annual_delta_vs_fixed_comparator_ratio
            ),
        },
        "dynamic_factor": dynamic_name,
        "dynamic_status": "experimental_account",
        "rebalance_offsets": list(offsets),
        "phase_quantile": phase_quantile,
        "benchmark_return_coverage_minimum": benchmark_coverage_minimum,
        "common_evaluation_start": common_start.date().isoformat()
        if common_start is not None
        else None,
        "scoring_account_protocol": "fresh_cash_equal_aum_common_start",
        "scoring_initial_nav": float(base_config.capital),
        "scoring_account_count": len(equal_aum_account_audits),
        "expected_scoring_account_count": expected_scoring_account_count,
        "equal_aum_scoring_valid": equal_aum_scoring_valid,
        "equal_aum_scoring_violations": equal_aum_violations,
        "shadow_account_role": "causal_selector_history_only_not_phase_scoring",
        "future_selection_violation_count": future_violations,
        "full_dynamic_period_coverage": full_coverage,
        "causal_history_valid": causal_valid,
        "dynamic_phase_ranking_eligible": dynamic_phase_eligible,
        "control_phase_ranking_eligible": control_phase_eligible,
        "dynamic_control_common_offset_count": dynamic_control_common_offset_count,
        "historical_diagnostic_passed": historical_diagnostic_passed,
        "historical_diagnostic_scope": (
            "same_sample_correlated_offset_thresholds_equal_aum_common_start"
        ),
        "historical_diagnostic_criteria": {
            "causal_history_valid": True,
            "equal_aum_scoring_valid": True,
            "control_phase_ranking_eligible": True,
            "dynamic_phase_ranking_eligible": True,
            "fixed_comparator_phase_ranking_eligible": True,
            "benchmark_return_coverage_minimum": benchmark_coverage_minimum,
            "dynamic_control_common_offset_count": len(offsets),
            "dynamic_phase_q20_net_annual_return_gt": 0.0,
            "dynamic_phase_q20_net_sharpe_gt": 0.0,
            "dynamic_phase_q20_annual_return_delta_vs_control_gte": 0.0,
            "dynamic_phase_q20_sharpe_delta_vs_control_gte": 0.0,
            "dynamic_phase_q20_max_drawdown_delta_vs_control_gte": -0.02,
            "positive_annual_return_delta_offset_ratio_gte": 0.8,
            "dynamic_phase_q20_annual_return_delta_vs_fixed_comparator_gte": 0.0,
            "dynamic_phase_q20_sharpe_delta_vs_fixed_comparator_gte": 0.0,
            "dynamic_phase_q20_max_drawdown_delta_vs_fixed_comparator_gte": -0.02,
            "positive_annual_return_delta_vs_fixed_comparator_offset_ratio_gte": 0.8,
        },
        "best_phase_strategy": eligible_rankings[0]["strategy_name"]
        if eligible_rankings
        else None,
        "dynamic_phase_rank": dynamic_ranking["rank"]
        if dynamic_ranking is not None
        else None,
        "phase_rankings": rankings,
        "excluded_from_phase_ranking": excluded_from_phase_ranking,
        "selection_counts": aggregate_selection_counts,
        "selection_counts_semantics": "selected_signal_date_count_legacy",
        "selection_frequency_basis": "all_signal_dates_across_preregistered_offsets",
        "selection_frequency": aggregate_selection_frequency,
        "offsets": compact_offsets,
    }
    _write_json(root / "walk-forward-summary.json", summary)
    base_dynamic = next(
        row["dynamic_result"]
        for row in offset_payloads
        if row["rebalance_offset_days"] == base_config.rebalance_offset_days
    )
    return summary, base_dynamic


__all__ = ["run_walk_forward_sweep"]

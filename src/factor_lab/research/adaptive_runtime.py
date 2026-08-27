"""Artifact orchestration for the frozen Factor Lab 5.0 adaptive protocol.

This module deliberately keeps the 4.1 walk-forward selector untouched.  The
four adaptive experts are continuous, independently-costed shadow accounts;
the five comparison accounts are born later from equal cash at one common
boundary.  The output contains frozen threshold gates and a deterministic
route, but never a strategy ranking.
"""

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
from .walk_forward import phase_distribution
from .walk_forward_runtime import (
    _equal_aum_account_audit,
    _read_json,
    _result_hash_matches,
    _result_sha256,
    _runtime_composed_result,
    _safe_name,
    _write_json,
)


PortfolioResult = Callable[..., dict[str, Any]]
HistoricalMetrics = Callable[..., dict[str, Any]]

_PROTOCOL_ID = "factor-lab/5.0/adaptive-core-overlay"
_EVIDENCE_CLASS = "post_selection_adaptive_simulation"
_HISTORY_POLICY = "only_periods_with_end_date_strictly_before_current_signal_date"
_ACCOUNT_NAMES = (
    "fixed_core_full",
    "fixed_core_overlay",
    "static_prior_full",
    "online_full",
    "online_overlay",
)
_COMPARISON_SPECS = {
    "core_overlay": ("fixed_core_overlay", "fixed_core_full"),
    "static_prior_diversification": ("static_prior_full", "fixed_core_full"),
    "online_vs_static": ("online_full", "static_prior_full"),
    "online_overlay_effect": ("online_overlay", "online_full"),
    "combined": ("online_overlay", "fixed_core_full"),
}
_GATE_NAMES = (
    "core_overlay",
    "online_vs_static",
    "online_overlay_effect",
    "combined",
)
_METRIC_NAMES = (
    "net_annual_return",
    "net_sharpe",
    "information_ratio",
    "max_drawdown",
)


def _canonical_sha256(value: Any) -> str:
    canonical = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _assert_no_ordering_schema_keys(value: Any) -> None:
    """Keep historical ordering semantics out of the adaptive summary schema."""

    forbidden = {
        "rank",
        "ranking",
        "ranking_available",
        "best_strategy",
        "winner",
        "phase_score",
    }
    if isinstance(value, Mapping):
        for raw_key, child in value.items():
            key = str(raw_key).casefold()
            if key in forbidden:
                raise ValueError(
                    f"adaptive summary contains forbidden ordering key: {raw_key}"
                )
            _assert_no_ordering_schema_keys(child)
    elif isinstance(value, (list, tuple)):
        for child in value:
            _assert_no_ordering_schema_keys(child)


def _mapping(value: Any, *, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"adaptive protocol {name} must be an object")
    return dict(value)


def _sequence(value: Any, *, name: str) -> tuple[Any, ...]:
    if not isinstance(value, (list, tuple)):
        raise ValueError(f"adaptive protocol {name} must be an array")
    return tuple(value)


def _finite(value: Any, *, name: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"adaptive protocol {name} must be finite") from exc
    if not np.isfinite(parsed):
        raise ValueError(f"adaptive protocol {name} must be finite")
    return parsed


def validate_adaptive_protocol(
    protocol: Mapping[str, Any],
    *,
    protocol_sha256: str,
    factors: Sequence[FactorSpec],
    control: FactorSpec,
    base_config: LongOnlyPortfolioConfig,
) -> dict[str, Any]:
    """Validate the immutable public protocol before creating any artifacts."""

    if not re.fullmatch(r"[0-9a-f]{64}", str(protocol_sha256)):
        raise ValueError("adaptive protocol_sha256 must be a lowercase SHA-256")
    if protocol.get("schema_version") != 1:
        raise ValueError("adaptive protocol schema_version must be 1")
    if protocol.get("protocol_id") != _PROTOCOL_ID:
        raise ValueError(f"adaptive protocol_id must be {_PROTOCOL_ID!r}")
    if protocol.get("release") != "5.0":
        raise ValueError("adaptive protocol release must be '5.0'")
    if protocol.get("status") != "frozen_before_historical_execution":
        raise ValueError("adaptive protocol must be frozen before historical execution")
    if protocol.get("evidence_class") != _EVIDENCE_CLASS:
        raise ValueError(f"adaptive evidence_class must be {_EVIDENCE_CLASS!r}")
    if protocol.get("investment_claim_allowed") is not False:
        raise ValueError("adaptive protocol cannot allow an investment claim")

    portfolio = _mapping(protocol.get("portfolio"), name="portfolio")
    experts = _mapping(protocol.get("experts"), name="experts")
    allocator = _mapping(protocol.get("online_allocator"), name="online_allocator")
    overlay = _mapping(protocol.get("market_overlay"), name="market_overlay")
    scoring = _mapping(protocol.get("scoring_accounts"), name="scoring_accounts")
    comparisons = _mapping(protocol.get("paired_comparisons"), name="paired_comparisons")
    aggregation = _mapping(protocol.get("phase_aggregation"), name="phase_aggregation")
    gates = _mapping(protocol.get("frozen_gates"), name="frozen_gates")
    routing = _mapping(protocol.get("routing"), name="routing")
    integrity_settings = _mapping(
        protocol.get("integrity_gates"), name="integrity_gates"
    )

    expert_names = tuple(
        str(value)
        for value in _sequence(
            experts.get("ordered_registry"), name="experts.ordered_registry"
        )
    )
    if len(expert_names) != 4 or len(set(expert_names)) != 4:
        raise ValueError("adaptive protocol requires four unique ordered experts")
    factor_names = tuple(factor.name for factor in factors)
    if factor_names != expert_names:
        raise ValueError(
            "adaptive factor registry must exactly match the frozen ordered experts"
        )
    if str(experts.get("control")) != control.name or expert_names[0] != control.name:
        raise ValueError("adaptive control must be the first frozen expert")
    fixed_core = str(experts.get("fixed_core") or "")
    if fixed_core not in expert_names:
        raise ValueError("adaptive fixed_core must be in the frozen expert registry")
    flexible_experts = tuple(
        str(value) for value in allocator.get("flexible_sleeve_experts") or ()
    )
    if flexible_experts != expert_names:
        raise ValueError("adaptive flexible sleeve must contain the ordered expert registry")

    offsets = tuple(int(value) for value in portfolio.get("rebalance_offsets") or ())
    rebalance_days = int(portfolio.get("rebalance_every_days") or 0)
    if offsets != tuple(range(10)) or rebalance_days != 10:
        raise ValueError("adaptive protocol requires the ten frozen offsets 0..9")
    if int(portfolio.get("holding_days") or 0) != 10:
        raise ValueError("adaptive protocol holding_days must be 10")
    if int(portfolio.get("position_count_per_expert") or 0) != 10:
        raise ValueError("adaptive protocol position_count_per_expert must be 10")
    if int(portfolio.get("maximum_combined_position_count") or 0) != 40:
        raise ValueError("adaptive protocol maximum_combined_position_count must be 40")
    if tuple(str(value) for value in scoring.get("per_offset") or ()) != _ACCOUNT_NAMES:
        raise ValueError("adaptive scoring account registry differs from frozen 5.0")
    if int(scoring.get("total_count") or 0) != len(offsets) * len(_ACCOUNT_NAMES):
        raise ValueError("adaptive scoring total_count must be 50")
    if int(experts.get("shadow_account_count") or 0) != len(offsets) * len(expert_names):
        raise ValueError("adaptive shadow_account_count must be 40")
    if experts.get("shadow_history_policy") != _HISTORY_POLICY:
        raise ValueError("adaptive shadow history policy is not strictly matured")

    if (
        aggregation.get("ranking") is not False
        or aggregation.get("best_strategy_selection") is not False
    ):
        raise ValueError("adaptive protocol forbids result ordering and winner selection")
    phase_quantile = _finite(aggregation.get("quantile"), name="phase_aggregation.quantile")
    if not np.isclose(phase_quantile, 0.2, rtol=0.0, atol=1e-12):
        raise ValueError("adaptive phase quantile differs from frozen 5.0")
    if int(aggregation.get("offset_count") or 0) != len(offsets):
        raise ValueError("adaptive phase offset_count must match rebalance offsets")
    if tuple(gates) != _GATE_NAMES:
        raise ValueError("adaptive frozen gate registry or order differs from 5.0")
    expected_gates = {
        "core_overlay": {
            "paired_q20_net_annual_return_delta_min": 0.0,
            "paired_q20_net_sharpe_delta_min": 0.0,
            "paired_q20_max_drawdown_delta_min": 0.0,
            "positive_annual_return_delta_ratio_min": 0.8,
            "mean_fraction_signal_dates_exposure_below_one_min": 0.1,
        },
        "online_vs_static": {
            "paired_q20_net_annual_return_delta_exclusive_min": 0.0,
            "paired_q20_net_sharpe_delta_min": 0.0,
            "paired_q20_max_drawdown_delta_min": -0.02,
            "positive_annual_return_delta_ratio_min": 0.8,
        },
        "online_overlay_effect": {
            "paired_q20_net_annual_return_delta_min": 0.0,
            "paired_q20_net_sharpe_delta_min": 0.0,
            "paired_q20_max_drawdown_delta_min": 0.0,
            "positive_annual_return_delta_ratio_min": 0.8,
            "mean_fraction_signal_dates_exposure_below_one_min": 0.1,
        },
        "combined": {
            "paired_q20_net_annual_return_delta_exclusive_min": 0.0,
            "paired_q20_net_sharpe_delta_min": 0.0,
            "paired_q20_max_drawdown_delta_min": -0.02,
            "positive_annual_return_delta_ratio_min": 0.8,
        },
    }
    if gates != expected_gates:
        raise ValueError("adaptive frozen gate criteria or thresholds differ from 5.0")
    if set(comparisons) != set(_COMPARISON_SPECS):
        raise ValueError("adaptive paired comparison registry differs from 5.0")
    for name, (left, right) in _COMPARISON_SPECS.items():
        if comparisons.get(name) != f"{left}_minus_{right}":
            raise ValueError(f"adaptive paired comparison {name!r} differs from 5.0")
    expected_routing = {
        "if_core_overlay_gate_fails": "fixed_core_full",
        "if_all_four_gates_pass": "online_overlay",
        "otherwise": "fixed_core_overlay",
        "allow_post_run_threshold_changes": False,
        "allow_historical_rerun_to_change_route_after_release": False,
    }
    if routing != expected_routing:
        raise ValueError("adaptive routing differs from the frozen 5.0 decision tree")

    expected_allocator = {
        "anchor_weight": 0.5,
        "anchor_expert": fixed_core,
        "flexible_sleeve_weight": 0.5,
        "flexible_sleeve_experts": list(expert_names),
        "score": "cumulative_sum_log1p_of_independent_costed_shadow_net_returns",
        "allocation": "softmax_score",
        "initial_sleeve_weights": [0.25, 0.25, 0.25, 0.25],
        "learning_rate": 1.0,
        "decay": None,
        "score_clipping": None,
        "hyperparameter_search": False,
        "static_prior_total_weights": [0.125, 0.625, 0.125, 0.125],
    }
    if allocator != expected_allocator:
        raise ValueError("adaptive online allocator differs from frozen 5.0")
    expected_overlay = {
        "market_proxy": "equal_weight_return_1d_over_current_PIT_eligible_universe",
        "market_level": "cumulative_product_one_plus_market_proxy_return",
        "trend_window_trading_days": 200,
        "trend_rule": "market_level_greater_than_simple_moving_average",
        "breadth_field": "momentum_120",
        "breadth_rule": "fraction_strictly_greater_than_zero_at_least_0.5",
        "volatility_window_trading_days": 60,
        "volatility_ddof": 1,
        "annualization_days": 252,
        "target_annual_volatility": 0.2,
        "volatility_scalar_clip": [0.25, 1.0],
        "regime_exposure_caps": {
            "trend_and_breadth": 1.0,
            "exactly_one": 0.6,
            "neither": 0.25,
        },
        "exposure": "minimum_of_regime_cap_and_volatility_scalar",
        "minimum_history_trading_days": 400,
        "minimum_daily_return_coverage": 0.95,
        "minimum_daily_breadth_coverage": 0.95,
        "missing_value_policy": "fail_closed_no_backfill_no_forward_fill",
        "timing": "use_close_information_on_signal_date_for_next_open_trade",
    }
    if overlay != expected_overlay:
        raise ValueError("adaptive market overlay differs from frozen 5.0")
    expected_integrity = {
        "future_selection_violation_count_max": 0,
        "capacity_violation_count_max": 0,
        "account_reconciliation_error_max": 1e-8,
        "execution_input_coverage_min": 1.0,
        "execution_period_coverage_min": 1.0,
        "overlay_signal_coverage_min": 1.0,
        "fresh_account_boundary_required": True,
        "manifest_self_hash_required": True,
        "protocol_hash_in_run_fingerprint_required": True,
    }
    if integrity_settings != expected_integrity:
        raise ValueError("adaptive integrity gates differ from frozen 5.0")

    expected_capital = _finite(scoring.get("initial_nav"), name="scoring_accounts.initial_nav")
    if not np.isclose(expected_capital, float(portfolio.get("capital")), rtol=0.0, atol=0.01):
        raise ValueError("adaptive protocol capital and scoring initial_nav disagree")
    config_checks = {
        "capital": (float(base_config.capital), expected_capital),
        "holding_days": (int(base_config.holding_days), 10),
        "rebalance_every_days": (int(base_config.rebalance_every_days), 10),
        "position_count": (int(base_config.position_count), 10),
    }
    for name, (observed, expected) in config_checks.items():
        if observed != expected:
            raise ValueError(
                f"adaptive shadow base_config {name} must equal frozen value {expected}"
            )
    if not np.isclose(float(base_config.target_weight), 0.1, rtol=0.0, atol=1e-12):
        raise ValueError("adaptive shadow base_config target_weight must be 0.1")
    if base_config.evaluation_start_date is not None:
        raise ValueError("adaptive shadow base_config evaluation_start_date must be null")
    if int(base_config.rebalance_offset_days) != 0:
        raise ValueError("adaptive reusable base results require rebalance offset zero")

    prior_weights = tuple(
        float(value) for value in allocator.get("static_prior_total_weights") or ()
    )
    if len(prior_weights) != len(expert_names) or not np.isclose(
        sum(prior_weights), 1.0, rtol=0.0, atol=1e-12
    ):
        raise ValueError("adaptive static prior must fully weight all four experts")
    if any(not np.isfinite(value) or value < 0.0 for value in prior_weights):
        raise ValueError("adaptive static prior weights must be finite and non-negative")

    return {
        "portfolio": portfolio,
        "experts": experts,
        "allocator": allocator,
        "scoring": scoring,
        "comparisons": comparisons,
        "aggregation": aggregation,
        "gates": gates,
        "routing": routing,
        "expert_names": expert_names,
        "fixed_core": fixed_core,
        "offsets": offsets,
        "phase_quantile": phase_quantile,
        "prior_weights": dict(zip(expert_names, prior_weights, strict=True)),
    }


def _period_target_rows(result: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Read the narrow target trace exported by the runner callback."""

    raw = result.get("period_target_weights")
    if raw is None:
        raw = result.get("period_targets")
    if raw is None and isinstance(result.get("portfolio"), Mapping):
        raw = (result.get("portfolio") or {}).get("period_target_weights")
    if not isinstance(raw, list):
        return []
    output: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, Mapping):
            raise ValueError("adaptive period target row must be an object")
        signal_date = pd.Timestamp(str(item.get("signal_date"))).date().isoformat()
        if signal_date in seen:
            raise ValueError("adaptive period targets contain a duplicate signal date")
        seen.add(signal_date)
        weights = item.get("target_weights")
        if not isinstance(weights, Mapping) or not weights:
            raise ValueError("adaptive period target weights must be non-empty")
        normalized: dict[str, float] = {}
        for raw_ticker, raw_weight in weights.items():
            ticker = str(raw_ticker).strip()
            weight = _finite(raw_weight, name=f"period target {ticker}")
            if not ticker or weight < 0.0:
                raise ValueError("adaptive period target weights must be long-only")
            if weight > 1e-12:
                normalized[ticker] = weight
        if not normalized or sum(normalized.values()) > 1.0 + 1e-8:
            raise ValueError("adaptive period target weights violate funding")
        output.append(
            {
                "signal_date": signal_date,
                "start_date": item.get("start_date"),
                "end_date": item.get("end_date"),
                "target_weights": dict(sorted(normalized.items())),
            }
        )
    return sorted(output, key=lambda row: row["signal_date"])


def _shadow_result_usable(result: Mapping[str, Any], factor_name: str) -> bool:
    return bool(
        result.get("factor_name") == factor_name
        and result.get("account_nav_path")
        and result.get("period_active_returns")
        and _period_target_rows(result)
    )


def _shadow_account_audit(
    result: Mapping[str, Any],
    *,
    factor_name: str,
    rebalance_offset_days: int,
    initial_nav: float,
) -> dict[str, Any]:
    """Audit every causal feedback account before its returns reach the allocator."""

    reasons: list[str] = []
    portfolio = dict(result.get("portfolio") or {})
    if result.get("factor_name") != factor_name:
        reasons.append("shadow_factor_name_mismatch")
    if portfolio.get("status") != "ok":
        reasons.append("shadow_portfolio_status_not_ok")
    try:
        observed_initial = float(portfolio.get("initial_nav"))
    except (TypeError, ValueError):
        observed_initial = np.nan
    if not np.isfinite(observed_initial) or not np.isclose(
        observed_initial, float(initial_nav), rtol=0.0, atol=0.01
    ):
        reasons.append("shadow_initial_nav_mismatch")
    try:
        reconciliation_error = abs(
            float(portfolio.get("account_nav_reconciliation_error"))
        )
    except (TypeError, ValueError):
        reconciliation_error = np.inf
    if not np.isfinite(reconciliation_error) or reconciliation_error > 1e-8:
        reasons.append("shadow_nav_reconciliation_failed")
    if int(portfolio.get("capacity_violation_count") or 0) != 0:
        reasons.append("shadow_portfolio_capacity_violation")
    if "unresolved_stale_position_observed" in {
        str(value) for value in portfolio.get("promotion_blockers") or ()
    }:
        reasons.append("shadow_unresolved_stale_position")

    periods = [
        dict(row)
        for row in result.get("period_active_returns") or []
        if isinstance(row, Mapping)
    ]
    try:
        targets = _period_target_rows(result)
    except (TypeError, ValueError):
        targets = []
        reasons.append("shadow_period_targets_invalid")
    period_identities = [
        (
            _date_key(row.get("signal_date")),
            _date_key(row.get("start_date")),
            _date_key(row.get("end_date")),
        )
        for row in periods
        if row.get("signal_date") is not None
        and row.get("start_date") is not None
        and row.get("end_date") is not None
    ]
    target_identities = [
        (row["signal_date"], _date_key(row["start_date"]), _date_key(row["end_date"]))
        for row in targets
    ]
    if not periods:
        reasons.append("shadow_period_returns_missing")
    if len(period_identities) != len(periods):
        reasons.append("shadow_period_return_dates_invalid")
    if period_identities != target_identities:
        reasons.append("shadow_period_target_cohort_mismatch")
    if period_identities:
        try:
            effective_start = _date_key(portfolio.get("evaluation_start_date"))
        except (TypeError, ValueError):
            effective_start = ""
        if effective_start != period_identities[0][0]:
            reasons.append("shadow_account_did_not_start_at_first_full_history_signal")
    for row in periods:
        try:
            net_return = float(row.get("net_return"))
        except (TypeError, ValueError):
            net_return = np.nan
        if not np.isfinite(net_return) or net_return <= -1.0:
            reasons.append("shadow_period_net_return_invalid")
            break

    nav_path = [
        dict(row)
        for row in result.get("account_nav_path") or []
        if isinstance(row, Mapping)
    ]
    if not nav_path:
        reasons.append("shadow_nav_path_missing")
    else:
        if [row.get("sequence") for row in nav_path] != list(range(len(nav_path))):
            reasons.append("shadow_nav_path_sequence_invalid")
        try:
            path_end_nav = float(nav_path[-1].get("nav"))
            portfolio_end_nav = float(portfolio.get("end_nav"))
        except (TypeError, ValueError):
            path_end_nav = portfolio_end_nav = np.nan
        if not np.isfinite(path_end_nav) or not np.isclose(
            path_end_nav, portfolio_end_nav, rtol=0.0, atol=0.01
        ):
            reasons.append("shadow_nav_path_end_mismatch")

    execution_windows: list[dict[str, Any]] = []
    for window_name, raw_window in sorted(dict(result.get("windows") or {}).items()):
        window = dict(raw_window or {})
        observations = int(window.get("observations") or 0)
        if observations <= 0:
            continue
        try:
            input_coverage = float(window.get("execution_input_coverage"))
            period_coverage = float(window.get("execution_period_coverage"))
            future_count = int(
                window.get("execution_input_future_violation_count") or 0
            )
            capacity_count = int(window.get("capacity_violation_count") or 0)
        except (TypeError, ValueError):
            input_coverage = period_coverage = np.nan
            future_count = capacity_count = -1
        window_reasons: list[str] = []
        if not np.isfinite(input_coverage) or input_coverage < 1.0:
            window_reasons.append("execution_input_coverage_below_one")
        if not np.isfinite(period_coverage) or period_coverage < 1.0:
            window_reasons.append("execution_period_coverage_below_one")
        if future_count != 0:
            window_reasons.append("execution_input_future_violation")
        if capacity_count != 0:
            window_reasons.append("capacity_violation")
        execution_windows.append(
            {
                "window": str(window_name),
                "observations": observations,
                "execution_input_coverage": input_coverage
                if np.isfinite(input_coverage)
                else None,
                "execution_period_coverage": period_coverage
                if np.isfinite(period_coverage)
                else None,
                "execution_input_future_violation_count": future_count,
                "capacity_violation_count": capacity_count,
                "valid": not window_reasons,
                "reasons": window_reasons,
            }
        )
        reasons.extend(f"shadow_{value}" for value in window_reasons)
    if not execution_windows:
        reasons.append("shadow_execution_windows_missing")

    reasons = list(dict.fromkeys(reasons))
    return {
        "valid": not reasons,
        "factor_name": factor_name,
        "rebalance_offset_days": int(rebalance_offset_days),
        "period_count": len(periods),
        "target_period_count": len(targets),
        "account_nav_observation_count": len(nav_path),
        "account_nav_reconciliation_error": reconciliation_error
        if np.isfinite(reconciliation_error)
        else None,
        "execution_integrity": execution_windows,
        "reasons": reasons,
    }


def _normalized_target_schedule(
    value: Any,
    *,
    maximum_position_count: int,
) -> dict[str, dict[str, float]]:
    if isinstance(value, Mapping):
        rows = [
            {"signal_date": raw_date, "target_weights": raw_weights}
            for raw_date, raw_weights in value.items()
        ]
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        rows = list(value)
    else:
        raise ValueError("adaptive account target schedule must be a mapping or array")
    output: dict[str, dict[str, float]] = {}
    for item in rows:
        if not isinstance(item, Mapping):
            raise ValueError("adaptive account target row must be an object")
        signal_date = pd.Timestamp(str(item.get("signal_date"))).date().isoformat()
        if signal_date in output:
            raise ValueError("adaptive account target schedule contains a duplicate date")
        raw_weights = item.get("target_weights")
        if not isinstance(raw_weights, Mapping):
            raise ValueError("adaptive account target weights must be an object")
        weights: dict[str, float] = {}
        for raw_ticker, raw_weight in raw_weights.items():
            ticker = str(raw_ticker).strip()
            weight = _finite(raw_weight, name=f"account target {ticker}")
            if not ticker or weight < 0.0:
                raise ValueError("adaptive account targets must be long-only")
            if weight > 1e-12:
                weights[ticker] = weight
        if len(weights) > maximum_position_count:
            raise ValueError("adaptive combined target exceeds the frozen position count")
        if sum(weights.values()) > 1.0 + 1e-8:
            raise ValueError("adaptive combined target exceeds 100% funding")
        output[signal_date] = dict(sorted(weights.items()))
    return dict(sorted(output.items()))


def _target_schedule_sha256(value: Mapping[str, Mapping[str, float]]) -> str:
    return _canonical_sha256(value)


def _cache_result(
    payload: Mapping[str, Any],
    *,
    run_fingerprint: str,
    protocol_sha256: str,
    offset: int,
    role: str,
    factor_name: str,
    evaluation_start_date: str | None,
    decisions_sha256: str | None = None,
    target_schedule_sha256: str | None = None,
) -> dict[str, Any] | None:
    result = payload.get("result")
    if not isinstance(result, Mapping):
        return None
    if (
        payload.get("run_fingerprint") != run_fingerprint
        or payload.get("protocol_sha256") != protocol_sha256
        or payload.get("rebalance_offset_days") != offset
        or payload.get("role") != role
        or payload.get("evaluation_start_date") != evaluation_start_date
        or result.get("factor_name") != factor_name
        or not result.get("account_nav_path")
        or not _result_hash_matches(payload, result)
    ):
        return None
    if decisions_sha256 is not None and payload.get("decisions_sha256") != decisions_sha256:
        return None
    if (
        target_schedule_sha256 is not None
        and payload.get("target_schedule_sha256") != target_schedule_sha256
    ):
        return None
    try:
        usable = evaluation_start_date is not None or _shadow_result_usable(
            result, factor_name
        )
        return dict(result) if usable else None
    except (TypeError, ValueError):
        return None


def _decision_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    method = getattr(value, "to_dict", None)
    if callable(method):
        result = method()
        if isinstance(result, Mapping):
            return dict(result)
    raise TypeError("adaptive decision objects must provide to_dict()")


def _date_key(value: Any) -> str:
    return pd.Timestamp(str(value)).date().isoformat()


def _decision_mapping(value: Mapping[Any, Any]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for raw_date, decision in value.items():
        date_value = _date_key(raw_date)
        if date_value in output:
            raise ValueError("adaptive decisions contain a duplicate signal date")
        output[date_value] = decision
    return dict(sorted(output.items()))


def _construct_spec(spec_type: Any, protocol: Mapping[str, Any]) -> Any:
    """Create a frozen pure-logic spec without binding to one constructor style."""

    from_protocol = getattr(spec_type, "from_protocol", None)
    if callable(from_protocol):
        return from_protocol(protocol)
    try:
        return spec_type()
    except TypeError:
        from_mapping = getattr(spec_type, "from_mapping", None)
        if callable(from_mapping):
            return from_mapping(protocol)
        raise


def _allocation_total_weights(
    decision: Any,
    *,
    expert_names: Sequence[str],
    fixed_core: str,
    allocator: Mapping[str, Any],
) -> dict[str, float]:
    payload = _decision_dict(decision)
    raw: Any = None
    for key in (
        "total_expert_weights",
        "total_weights",
        "expert_weights",
        "allocation_weights",
    ):
        if isinstance(payload.get(key), Mapping):
            raw = payload[key]
            break
    if raw is None:
        flexible = payload.get("flexible_sleeve_weights") or payload.get(
            "flexible_weights"
        )
        if not isinstance(flexible, Mapping):
            raise ValueError("adaptive online allocation omitted expert weights")
        anchor_weight = _finite(
            allocator.get("anchor_weight"), name="online_allocator.anchor_weight"
        )
        sleeve_weight = _finite(
            allocator.get("flexible_sleeve_weight"),
            name="online_allocator.flexible_sleeve_weight",
        )
        raw = {
            name: sleeve_weight * float(flexible.get(name, 0.0))
            + (anchor_weight if name == fixed_core else 0.0)
            for name in expert_names
        }
    weights = {str(name): float(value) for name, value in dict(raw).items()}
    if set(weights) != set(expert_names):
        raise ValueError("adaptive online allocation must cover exactly four experts")
    values = np.asarray([weights[name] for name in expert_names], dtype=float)
    if (
        not np.isfinite(values).all()
        or bool((values < 0.0).any())
        or not np.isclose(float(values.sum()), 1.0, rtol=0.0, atol=1e-8)
    ):
        raise ValueError("adaptive online allocation weights must sum to one")
    return {name: float(weights[name]) for name in expert_names}


def _overlay_exposure(decision: Any) -> float:
    payload = _decision_dict(decision)
    exposure = _finite(payload.get("exposure"), name="market overlay exposure")
    if not 0.0 <= exposure <= 1.0:
        raise ValueError("adaptive overlay exposure must be in [0, 1]")
    return exposure


def _overlay_ready(decision: Any) -> bool:
    payload = _decision_dict(decision)
    for key in ("ready", "valid", "promotion_eligible"):
        if key in payload:
            return payload.get(key) is True
    status = str(payload.get("status") or "").casefold()
    return status in {"ready", "ok", "valid"}


def _required_nonnegative_decision_int(
    payload: Mapping[str, Any], *, name: str
) -> int:
    if name not in payload:
        raise ValueError(f"adaptive decision omitted required field {name}")
    value = payload.get(name)
    if isinstance(value, bool):
        raise ValueError(f"adaptive decision {name} must be a non-negative integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"adaptive decision {name} must be a non-negative integer"
        ) from exc
    if parsed < 0 or parsed != value:
        raise ValueError(f"adaptive decision {name} must be a non-negative integer")
    return parsed


def _online_causality_violation_count(
    decisions: Mapping[str, Any],
    *,
    cohorts: Sequence[Any],
    expert_names: Sequence[str],
) -> int:
    """Recompute the strictly-matured history contract from shadow cohorts."""

    violations = 0
    for signal_date, decision in decisions.items():
        payload = _decision_dict(decision)
        if "signal_date" not in payload:
            raise ValueError("adaptive online decision omitted signal_date")
        observed_signal = _date_key(payload["signal_date"])
        if observed_signal != signal_date:
            violations += 1
        if payload.get("history_policy") != _HISTORY_POLICY:
            violations += 1
        expected_observable = [
            cohort
            for cohort in cohorts
            if _date_key(getattr(cohort, "signal_date")) <= signal_date
        ]
        expected_matured = [
            cohort
            for cohort in expected_observable
            if _date_key(getattr(cohort, "end_date")) < signal_date
        ]
        expected_latest = max(
            (_date_key(getattr(cohort, "end_date")) for cohort in expected_matured),
            default=None,
        )
        matured_count = _required_nonnegative_decision_int(
            payload, name="matured_cohort_count"
        )
        excluded_count = _required_nonnegative_decision_int(
            payload, name="excluded_unmatured_cohort_count"
        )
        if matured_count != len(expected_matured):
            violations += 1
        if excluded_count != len(expected_observable) - len(expected_matured):
            violations += 1
        observed_latest_raw = payload.get("latest_matured_end_date")
        observed_latest = (
            None if observed_latest_raw is None else _date_key(observed_latest_raw)
        )
        if observed_latest != expected_latest:
            violations += 1
        if observed_latest is not None and observed_latest >= signal_date:
            violations += 1
        history_rows = []
        for cohort in expected_matured:
            returns_method = getattr(cohort, "returns_by_expert", None)
            if not callable(returns_method):
                raise TypeError("adaptive cohort omitted returns_by_expert()")
            returns = returns_method()
            history_rows.append(
                {
                    "signal_date": _date_key(getattr(cohort, "signal_date")),
                    "start_date": _date_key(getattr(cohort, "start_date")),
                    "end_date": _date_key(getattr(cohort, "end_date")),
                    "net_returns": {
                        name: float(returns[name]) for name in expert_names
                    },
                }
            )
        if payload.get("history_sha256") != _canonical_sha256(history_rows):
            violations += 1
        violations += _required_nonnegative_decision_int(
            payload, name="future_feedback_violation_count"
        )
    return violations


def _overlay_causality_violation_count(
    decisions: Mapping[str, Any], *, features: pd.DataFrame
) -> int:
    """Audit that an overlay decision never observes beyond its signal close."""

    if "date" not in features.columns:
        raise ValueError("adaptive overlay features omitted date")
    feature_dates = tuple(
        sorted(
            {
                value.date().isoformat()
                for value in pd.to_datetime(features["date"], errors="raise")
                if not pd.isna(value)
            }
        )
    )
    violations = 0
    for signal_date, decision in decisions.items():
        payload = _decision_dict(decision)
        if "signal_date" not in payload:
            raise ValueError("adaptive overlay decision omitted signal_date")
        observed_signal = _date_key(payload["signal_date"])
        if observed_signal != signal_date:
            violations += 1
        latest_raw = payload.get("latest_input_date")
        latest = None if latest_raw is None else _date_key(latest_raw)
        if latest is not None and latest > signal_date:
            violations += 1
        ready = payload.get("ready")
        if not isinstance(ready, bool):
            raise ValueError("adaptive overlay decision ready must be boolean")
        if ready and latest != signal_date:
            violations += 1
        if not ready and not np.isclose(
            _overlay_exposure(decision), 0.0, rtol=0.0, atol=1e-12
        ):
            violations += 1
        observation_count = _required_nonnegative_decision_int(
            payload, name="observation_count"
        )
        expected_observations = sum(value <= signal_date for value in feature_dates)
        if observation_count != expected_observations:
            violations += 1
        violations += _required_nonnegative_decision_int(
            payload, name="future_overlay_violation_count"
        )
    return violations


def _combined_target_weights(decision: Any) -> dict[str, float]:
    payload = _decision_dict(decision)
    # An explicit empty mapping is a valid fail-closed 100% cash decision,
    # not an omitted field.  Do not use truthiness to select the fallback.
    raw = payload.get("combined_target_weights")
    if not isinstance(raw, Mapping):
        raw = payload.get("target_weights")
    if not isinstance(raw, Mapping):
        raw = getattr(decision, "combined_target_weights", None)
    if not isinstance(raw, Mapping):
        raise ValueError("adaptive target decision omitted combined_target_weights")
    return {str(key): float(value) for key, value in raw.items()}


def _first_complete_period_end(result: Mapping[str, Any]) -> pd.Timestamp:
    ends = [
        pd.Timestamp(str(row.get("end_date")))
        for row in result.get("period_active_returns") or []
        if row.get("end_date") is not None
    ]
    if not ends or any(pd.isna(value) for value in ends):
        raise RuntimeError("adaptive shadow account has no complete period end")
    return min(ends)


def _build_offset_decisions(
    *,
    protocol: Mapping[str, Any],
    frozen: Mapping[str, Any],
    features: pd.DataFrame,
    shadow_results: Mapping[str, Mapping[str, Any]],
    offset: int,
) -> dict[str, Any]:
    """Bridge orchestration data to the pure adaptive decision primitives."""

    # Imported lazily so the runtime and its contract tests can be developed
    # independently of the pure decision module.  Production execution still
    # fails closed if any public 5.0 primitive is unavailable.
    from .adaptive import (
        AdaptiveExpertSpec,
        MarketOverlaySpec,
        align_expert_cohorts,
        build_market_overlay,
        combine_expert_targets,
        expert_trace_from_evaluation,
        online_wealth_allocations,
    )

    expert_names = tuple(frozen["expert_names"])
    fixed_core = str(frozen["fixed_core"])
    allocator = dict(frozen["allocator"])
    expert_spec = _construct_spec(AdaptiveExpertSpec, protocol)
    overlay_spec = _construct_spec(MarketOverlaySpec, protocol)

    traces = [
        expert_trace_from_evaluation(
            name,
            shadow_results[name],
            rebalance_offset_days=offset,
        )
        for name in expert_names
    ]
    cohorts = align_expert_cohorts(traces, required_experts=expert_names)
    target_rows_by_expert = {
        name: _period_target_rows(shadow_results[name]) for name in expert_names
    }
    signal_dates = tuple(
        pd.Timestamp(row["signal_date"])
        for row in target_rows_by_expert[fixed_core]
    )
    if not signal_dates:
        raise RuntimeError("adaptive fixed core has no signal dates")
    expected_signal_dates = tuple(value.date().isoformat() for value in signal_dates)
    for name in expert_names:
        observed = tuple(row["signal_date"] for row in target_rows_by_expert[name])
        if observed != expected_signal_dates:
            raise ValueError(
                "adaptive expert target traces do not share identical signal dates"
            )

    online_raw = online_wealth_allocations(
        signal_dates,
        cohorts,
        spec=expert_spec,
    )
    overlay_raw = build_market_overlay(
        features,
        signal_dates,
        spec=overlay_spec,
    )
    if not isinstance(online_raw, Mapping) or not isinstance(overlay_raw, Mapping):
        raise TypeError("adaptive allocation and overlay builders must return mappings")
    online_by_date = _decision_mapping(online_raw)
    overlay_by_date = _decision_mapping(overlay_raw)
    if tuple(online_by_date) != expected_signal_dates:
        raise ValueError("adaptive online allocations must cover every signal date")
    if tuple(overlay_by_date) != expected_signal_dates:
        raise ValueError("adaptive overlay decisions must cover every signal date")

    targets_lookup = {
        name: {
            row["signal_date"]: dict(row["target_weights"])
            for row in target_rows_by_expert[name]
        }
        for name in expert_names
    }
    fixed_weights = {
        name: 1.0 if name == fixed_core else 0.0 for name in expert_names
    }
    prior_weights = dict(frozen["prior_weights"])
    account_schedules: dict[str, list[dict[str, Any]]] = {
        name: [] for name in _ACCOUNT_NAMES
    }
    account_audits: dict[str, dict[str, dict[str, Any]]] = {
        name: {} for name in _ACCOUNT_NAMES
    }
    target_decision_rows: dict[str, list[dict[str, Any]]] = {
        name: [] for name in _ACCOUNT_NAMES
    }
    for signal_date in expected_signal_dates:
        expert_targets = {
            name: targets_lookup[name][signal_date] for name in expert_names
        }
        online_weights = _allocation_total_weights(
            online_by_date[signal_date],
            expert_names=expert_names,
            fixed_core=fixed_core,
            allocator=allocator,
        )
        exposure = _overlay_exposure(overlay_by_date[signal_date])
        recipes = {
            "fixed_core_full": (fixed_weights, 1.0),
            "fixed_core_overlay": (fixed_weights, exposure),
            "static_prior_full": (prior_weights, 1.0),
            "online_full": (online_weights, 1.0),
            "online_overlay": (online_weights, exposure),
        }
        for account_name in _ACCOUNT_NAMES:
            expert_weights, account_exposure = recipes[account_name]
            target_decision = combine_expert_targets(
                signal_date=pd.Timestamp(signal_date),
                targets_by_expert=expert_targets,
                expert_weights=expert_weights,
                exposure=account_exposure,
                spec=expert_spec,
            )
            decision_payload = _decision_dict(target_decision)
            weights = _combined_target_weights(target_decision)
            account_schedules[account_name].append(
                {"signal_date": signal_date, "target_weights": weights}
            )
            target_decision_rows[account_name].append(decision_payload)
            account_audits[account_name][signal_date] = {
                "promotion_eligible": True,
                "protocol_id": _PROTOCOL_ID,
                "account_name": account_name,
                "signal_date": signal_date,
                "expert_weights": expert_weights,
                "exposure": account_exposure,
                "decision": decision_payload,
            }

    online_rows = [_decision_dict(online_by_date[key]) for key in online_by_date]
    overlay_rows = [_decision_dict(overlay_by_date[key]) for key in overlay_by_date]
    future_feedback_violations = _online_causality_violation_count(
        online_by_date,
        cohorts=cohorts,
        expert_names=expert_names,
    )
    future_overlay_violations = _overlay_causality_violation_count(
        overlay_by_date,
        features=features,
    )
    overlay_ready_dates = [
        pd.Timestamp(signal_date)
        for signal_date, decision in overlay_by_date.items()
        if _overlay_ready(decision)
    ]
    if not overlay_ready_dates:
        raise RuntimeError("adaptive market overlay never becomes ready")
    first_shadow_complete = max(
        _first_complete_period_end(shadow_results[name]) for name in expert_names
    )
    ready_date = max(first_shadow_complete, min(overlay_ready_dates))
    public_decisions = {
        "history_policy": _HISTORY_POLICY,
        "rebalance_offset_days": offset,
        "signal_date_count": len(expected_signal_dates),
        "ready_date": ready_date.date().isoformat(),
        "future_feedback_violation_count": future_feedback_violations,
        "future_overlay_violation_count": future_overlay_violations,
        "online_allocations": online_rows,
        "market_overlay": overlay_rows,
        "target_decisions": target_decision_rows,
    }
    return {
        "ready_date": ready_date.date().isoformat(),
        "public_decisions": public_decisions,
        "account_schedules": account_schedules,
        "account_audits": account_audits,
        "online_by_date": online_by_date,
        "overlay_by_date": overlay_by_date,
        "future_feedback_violation_count": future_feedback_violations,
        "future_overlay_violation_count": future_overlay_violations,
    }


def _phase_distributions(
    metrics_by_account: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    quantile: float,
) -> dict[str, dict[str, dict[str, float]]]:
    output: dict[str, dict[str, dict[str, float]]] = {}
    for account_name in _ACCOUNT_NAMES:
        rows = tuple(metrics_by_account.get(account_name) or ())
        output[account_name] = {
            metric_name: phase_distribution(
                [
                    float(row[metric_name])
                    for row in rows
                    if row.get(metric_name) is not None
                    and np.isfinite(float(row[metric_name]))
                ],
                quantile,
            )
            for metric_name in _METRIC_NAMES
        }
    return output


def _paired_comparisons(
    metrics_by_account: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    quantile: float,
) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for name, (left, right) in _COMPARISON_SPECS.items():
        left_rows = tuple(metrics_by_account.get(left) or ())
        right_rows = tuple(metrics_by_account.get(right) or ())
        if len(left_rows) != len(right_rows):
            raise ValueError(f"adaptive comparison {name} has unequal offset counts")
        raw: dict[str, list[float]] = {metric: [] for metric in _METRIC_NAMES}
        for left_row, right_row in zip(left_rows, right_rows, strict=True):
            for metric in _METRIC_NAMES:
                left_value = left_row.get(metric)
                right_value = right_row.get(metric)
                if left_value is None or right_value is None:
                    continue
                delta = float(left_value) - float(right_value)
                if np.isfinite(delta):
                    raw[metric].append(delta)
        annual = raw["net_annual_return"]
        output[name] = {
            "left_account": left,
            "right_account": right,
            "offset_count": len(left_rows),
            "phase_deltas": {
                metric: phase_distribution(values, quantile)
                for metric, values in raw.items()
            },
            "positive_annual_return_delta_ratio": round(
                float(np.mean(np.asarray(annual) > 0.0)), 8
            )
            if annual
            else 0.0,
        }
        output[name].update(output[name]["phase_deltas"])
    return output


def _criterion_observation(
    key: str,
    comparison: Mapping[str, Any],
    *,
    mean_overlay_fraction: float,
) -> tuple[float | None, str]:
    if key == "positive_annual_return_delta_ratio_min":
        value = comparison.get("positive_annual_return_delta_ratio")
        return (float(value), ">=") if value is not None else (None, ">=")
    if key == "mean_fraction_signal_dates_exposure_below_one_min":
        return float(mean_overlay_fraction), ">="
    match = re.fullmatch(
        r"paired_q20_(net_annual_return|net_sharpe|max_drawdown)_delta(_exclusive)?_min",
        key,
    )
    if match is None:
        raise ValueError(f"unsupported adaptive frozen gate criterion: {key}")
    metric_name = str(match.group(1))
    value = (
        ((comparison.get("phase_deltas") or {}).get(metric_name) or {}).get("q20")
    )
    return (float(value) if value is not None else None, ">" if match.group(2) else ">=")


def _evaluate_frozen_gates(
    frozen_gates: Mapping[str, Any],
    comparisons: Mapping[str, Mapping[str, Any]],
    *,
    mean_overlay_fraction: float,
) -> dict[str, dict[str, Any]]:
    """Evaluate named thresholds in file order; exclusive minima are strict."""

    if tuple(frozen_gates) != _GATE_NAMES:
        raise ValueError("adaptive frozen gate registry differs from 5.0")
    results: dict[str, dict[str, Any]] = {}
    for gate_name in _GATE_NAMES:
        raw_criteria = frozen_gates.get(gate_name)
        if not isinstance(raw_criteria, Mapping) or not raw_criteria:
            raise ValueError(f"adaptive frozen gate {gate_name} must be non-empty")
        comparison = comparisons.get(gate_name)
        if not isinstance(comparison, Mapping):
            raise ValueError(f"adaptive gate {gate_name} has no paired comparison")
        criteria: list[dict[str, Any]] = []
        for key, raw_threshold in raw_criteria.items():
            threshold = _finite(raw_threshold, name=f"frozen_gates.{gate_name}.{key}")
            observed, operator = _criterion_observation(
                str(key),
                comparison,
                mean_overlay_fraction=mean_overlay_fraction,
            )
            passed = bool(
                observed is not None
                and np.isfinite(observed)
                and (observed > threshold if operator == ">" else observed >= threshold)
            )
            criteria.append(
                {
                    "criterion": str(key),
                    "observed": round(float(observed), 8)
                    if observed is not None and np.isfinite(observed)
                    else None,
                    "operator": operator,
                    "threshold": threshold,
                    "passed": passed,
                }
            )
        results[gate_name] = {
            "comparison": gate_name,
            "passed": all(row["passed"] for row in criteria),
            "criteria": criteria,
        }
    return results


def _determine_route(
    gates: Mapping[str, Mapping[str, Any]],
    *,
    integrity_passed: bool,
) -> dict[str, Any]:
    """Apply the frozen three-branch route without comparing strategy scores."""

    gate_passes = {
        name: bool((gates.get(name) or {}).get("passed")) for name in _GATE_NAMES
    }
    if not integrity_passed:
        return {
            "evaluated": False,
            "selected_account": None,
            "reason": "runtime_integrity_failed",
            "gate_passes": gate_passes,
        }
    if not gate_passes["core_overlay"]:
        selected = "fixed_core_full"
        reason = "core_overlay_gate_failed"
    elif all(gate_passes.values()):
        selected = "online_overlay"
        reason = "all_four_frozen_gates_passed"
    else:
        selected = "fixed_core_overlay"
        reason = "core_overlay_passed_but_not_all_four_gates_passed"
    return {
        "evaluated": True,
        "selected_account": selected,
        "reason": reason,
        "gate_passes": gate_passes,
    }


def _minimum_metric(
    rows: Sequence[Mapping[str, Any]], key: str, *, default: float
) -> float:
    values: list[float] = []
    for row in rows:
        try:
            value = float(row.get(key))
        except (TypeError, ValueError):
            continue
        if np.isfinite(value):
            values.append(value)
    return min(values) if values else default


def _runtime_integrity(
    *,
    protocol: Mapping[str, Any],
    protocol_sha256: str,
    shadow_count: int,
    expected_shadow_count: int,
    shadow_audits: Sequence[Mapping[str, Any]],
    scoring_audits: Sequence[Mapping[str, Any]],
    expected_scoring_count: int,
    metrics_by_account: Mapping[str, Sequence[Mapping[str, Any]]],
    future_feedback_violations: int,
    future_overlay_violations: int,
    overlay_signal_coverage_min: float,
    target_schedule_coverage_min: float,
) -> tuple[bool, dict[str, Any]]:
    settings = _mapping(protocol.get("integrity_gates"), name="integrity_gates")
    scoring_execution_rows = [
        dict(window)
        for audit in scoring_audits
        for window in audit.get("common_window_execution_integrity") or []
        if isinstance(window, Mapping)
    ]
    shadow_execution_rows = [
        dict(window)
        for audit in shadow_audits
        for window in audit.get("execution_integrity") or []
        if isinstance(window, Mapping)
    ]
    execution_rows = [*scoring_execution_rows, *shadow_execution_rows]
    capacity_violations = sum(
        int(row.get("capacity_violation_count") or 0) for row in execution_rows
    )
    future_execution_violations = sum(
        int(row.get("execution_input_future_violation_count") or 0)
        for row in execution_rows
    )
    execution_input_coverage_min = _minimum_metric(
        execution_rows, "execution_input_coverage", default=0.0
    )
    reconciliation_errors: list[float] = []
    for audit in [*scoring_audits, *shadow_audits]:
        try:
            value = abs(float(audit.get("account_nav_reconciliation_error")))
        except (TypeError, ValueError):
            value = np.inf
        reconciliation_errors.append(value)
    reconciliation_error_max = max(reconciliation_errors, default=np.inf)
    metric_rows = [
        dict(row)
        for account_name in _ACCOUNT_NAMES
        for row in metrics_by_account.get(account_name) or []
    ]
    execution_period_coverage_min = _minimum_metric(
        metric_rows, "period_coverage", default=0.0
    )
    daily_nav_complete = bool(
        metric_rows
        and all(row.get("daily_nav_path_complete") is True for row in metric_rows)
    )
    phase_metric_complete = bool(
        all(len(tuple(metrics_by_account.get(name) or ())) == 10 for name in _ACCOUNT_NAMES)
        and all(
            row.get(metric) is not None
            and np.isfinite(float(row[metric]))
            for row in metric_rows
            for metric in _METRIC_NAMES
        )
    )
    fresh_boundary_valid = bool(
        len(scoring_audits) == expected_scoring_count
        and all(row.get("valid") is True for row in scoring_audits)
    )
    shadow_accounts_valid = bool(
        len(shadow_audits) == expected_shadow_count
        and all(row.get("valid") is True for row in shadow_audits)
    )

    def criterion(
        *, name: str, observed: Any, operator: str, threshold: Any, passed: bool
    ) -> dict[str, Any]:
        return {
            "criterion": name,
            "observed": observed,
            "operator": operator,
            "threshold": threshold,
            "passed": bool(passed),
        }

    future_max = int(settings.get("future_selection_violation_count_max") or 0)
    capacity_max = int(settings.get("capacity_violation_count_max") or 0)
    reconciliation_max = _finite(
        settings.get("account_reconciliation_error_max"),
        name="integrity_gates.account_reconciliation_error_max",
    )
    execution_input_min = _finite(
        settings.get("execution_input_coverage_min"),
        name="integrity_gates.execution_input_coverage_min",
    )
    execution_period_min = _finite(
        settings.get("execution_period_coverage_min"),
        name="integrity_gates.execution_period_coverage_min",
    )
    overlay_min = _finite(
        settings.get("overlay_signal_coverage_min"),
        name="integrity_gates.overlay_signal_coverage_min",
    )
    future_total = (
        int(future_feedback_violations)
        + int(future_overlay_violations)
        + int(future_execution_violations)
    )
    criteria = [
        criterion(
            name="shadow_account_count",
            observed=shadow_count,
            operator="==",
            threshold=expected_shadow_count,
            passed=shadow_count == expected_shadow_count,
        ),
        criterion(
            name="shadow_accounts_valid",
            observed=sum(row.get("valid") is True for row in shadow_audits),
            operator="==",
            threshold=expected_shadow_count,
            passed=shadow_accounts_valid,
        ),
        criterion(
            name="scoring_account_count",
            observed=len(scoring_audits),
            operator="==",
            threshold=expected_scoring_count,
            passed=len(scoring_audits) == expected_scoring_count,
        ),
        criterion(
            name="future_selection_violation_count",
            observed=future_total,
            operator="<=",
            threshold=future_max,
            passed=future_total <= future_max,
        ),
        criterion(
            name="capacity_violation_count",
            observed=capacity_violations,
            operator="<=",
            threshold=capacity_max,
            passed=capacity_violations <= capacity_max,
        ),
        criterion(
            name="account_reconciliation_error_max",
            observed=round(reconciliation_error_max, 12)
            if np.isfinite(reconciliation_error_max)
            else None,
            operator="<=",
            threshold=reconciliation_max,
            passed=np.isfinite(reconciliation_error_max)
            and reconciliation_error_max <= reconciliation_max,
        ),
        criterion(
            name="execution_input_coverage_min",
            observed=round(execution_input_coverage_min, 8),
            operator=">=",
            threshold=execution_input_min,
            passed=execution_input_coverage_min >= execution_input_min,
        ),
        criterion(
            name="execution_period_coverage_min",
            observed=round(execution_period_coverage_min, 8),
            operator=">=",
            threshold=execution_period_min,
            passed=execution_period_coverage_min >= execution_period_min,
        ),
        criterion(
            name="overlay_signal_coverage_min",
            observed=round(overlay_signal_coverage_min, 8),
            operator=">=",
            threshold=overlay_min,
            passed=overlay_signal_coverage_min >= overlay_min,
        ),
        criterion(
            name="target_schedule_coverage_min",
            observed=round(target_schedule_coverage_min, 8),
            operator=">=",
            threshold=1.0,
            passed=target_schedule_coverage_min >= 1.0,
        ),
        criterion(
            name="fresh_account_boundary_required",
            observed=fresh_boundary_valid,
            operator="is",
            threshold=True,
            passed=fresh_boundary_valid
            and settings.get("fresh_account_boundary_required") is True,
        ),
        criterion(
            name="required_daily_nav_path",
            observed=daily_nav_complete,
            operator="is",
            threshold=True,
            passed=daily_nav_complete,
        ),
        criterion(
            name="ten_offset_phase_metrics_complete",
            observed=phase_metric_complete,
            operator="is",
            threshold=True,
            passed=phase_metric_complete,
        ),
        criterion(
            name="protocol_hash_in_runtime_artifacts",
            observed=protocol_sha256,
            operator="matches",
            threshold="sha256",
            passed=bool(re.fullmatch(r"[0-9a-f]{64}", protocol_sha256))
            and settings.get("protocol_hash_in_run_fingerprint_required") is True,
        ),
    ]
    passed = all(row["passed"] for row in criteria)
    return passed, {
        "passed": passed,
        "criteria": criteria,
        "manifest_self_hash_required": settings.get("manifest_self_hash_required")
        is True,
        "manifest_self_hash_status": "deferred_to_runner_manifest_publication",
    }


def run_adaptive_sweep(
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
    protocol: Mapping[str, Any],
    protocol_sha256: str,
    output_dir: Path,
    run_fingerprint: str,
    resume: bool,
    portfolio_result: PortfolioResult,
    historical_metrics: HistoricalMetrics,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    """Run all frozen 5.0 shadows and fresh equal-AUM scoring accounts."""

    frozen = validate_adaptive_protocol(
        protocol,
        protocol_sha256=protocol_sha256,
        factors=factors,
        control=control,
        base_config=base_config,
    )
    expert_names = tuple(frozen["expert_names"])
    offsets = tuple(frozen["offsets"])
    phase_quantile = float(frozen["phase_quantile"])
    factor_by_name = {factor.name: factor for factor in factors}
    missing_validations = sorted(set(expert_names) - set(validations))
    missing_signals = sorted(set(expert_names) - set(signals))
    if missing_validations or missing_signals:
        raise ValueError(
            "adaptive expert inputs are incomplete: "
            f"validations={missing_validations}, signals={missing_signals}"
        )
    base_by_name = {
        str(row.get("factor_name")): dict(row) for row in base_results
    }
    root = output_dir / "adaptive"
    root.mkdir(parents=True, exist_ok=True)
    maximum_positions = int(frozen["portfolio"]["maximum_combined_position_count"])
    offset_payloads: list[dict[str, Any]] = []
    all_shadow_audits: list[dict[str, Any]] = []

    for offset in offsets:
        offset_dir = root / f"offset-{offset:02d}"
        shadow_dir = offset_dir / "shadows"
        shadow_dir.mkdir(parents=True, exist_ok=True)
        shadow_config = replace(
            base_config,
            rebalance_offset_days=offset,
            evaluation_start_date=None,
        )
        shadow_results: dict[str, dict[str, Any]] = {}
        shadow_audits: list[dict[str, Any]] = []
        for expert_name in expert_names:
            path = shadow_dir / f"{_safe_name(expert_name)}.json"
            result: dict[str, Any] | None = None
            if resume and path.is_file():
                try:
                    payload = _read_json(path)
                except (OSError, ValueError, TypeError, json.JSONDecodeError):
                    payload = {}
                result = _cache_result(
                    payload,
                    run_fingerprint=run_fingerprint,
                    protocol_sha256=protocol_sha256,
                    offset=offset,
                    role="causal_adaptive_shadow_expert",
                    factor_name=expert_name,
                    evaluation_start_date=None,
                )
            if result is None:
                reusable = base_by_name.get(expert_name) if offset == 0 else None
                if reusable is not None:
                    try:
                        reusable_ok = _shadow_result_usable(reusable, expert_name)
                    except (TypeError, ValueError):
                        reusable_ok = False
                    result = dict(reusable) if reusable_ok else None
                if result is None:
                    result = portfolio_result(
                        factor_by_name[expert_name],
                        validations[expert_name],
                        signals[expert_name],
                        features,
                        execution,
                        shadow_config,
                        research_config,
                        include_period_target_weights=True,
                    )
                if not _shadow_result_usable(result, expert_name):
                    raise RuntimeError(
                        f"adaptive shadow result omitted targets or NAV: {expert_name}"
                    )
                _write_json(
                    path,
                    {
                        "run_fingerprint": run_fingerprint,
                        "protocol_id": _PROTOCOL_ID,
                        "protocol_sha256": protocol_sha256,
                        "rebalance_offset_days": offset,
                        "role": "causal_adaptive_shadow_expert",
                        "evaluation_start_date": None,
                        "result_sha256": _result_sha256(result),
                        "result": result,
                    },
                )
            shadow_results[expert_name] = result
            shadow_audit = _shadow_account_audit(
                result,
                factor_name=expert_name,
                rebalance_offset_days=offset,
                initial_nav=base_config.capital,
            )
            shadow_audits.append(shadow_audit)
            all_shadow_audits.append(shadow_audit)

        decisions = _build_offset_decisions(
            protocol=protocol,
            frozen=frozen,
            features=features,
            shadow_results=shadow_results,
            offset=offset,
        )
        normalized_schedules = {
            account_name: _normalized_target_schedule(
                decisions["account_schedules"][account_name],
                maximum_position_count=maximum_positions,
            )
            for account_name in _ACCOUNT_NAMES
        }
        schedule_hashes = {
            account_name: _target_schedule_sha256(normalized_schedules[account_name])
            for account_name in _ACCOUNT_NAMES
        }
        public_decisions = dict(decisions["public_decisions"])
        public_decisions["target_schedule_sha256"] = schedule_hashes
        decisions_sha256 = _canonical_sha256(public_decisions)
        _write_json(
            offset_dir / "decisions.json",
            {
                "run_fingerprint": run_fingerprint,
                "protocol_id": _PROTOCOL_ID,
                "protocol_sha256": protocol_sha256,
                "rebalance_offset_days": offset,
                "role": "adaptive_decision_audit",
                "decisions_sha256": decisions_sha256,
                "decisions": public_decisions,
            },
        )
        offset_payloads.append(
            {
                "rebalance_offset_days": offset,
                "shadow_results": shadow_results,
                "shadow_audits": shadow_audits,
                "ready_date": decisions["ready_date"],
                "decisions_sha256": decisions_sha256,
                "public_decisions": public_decisions,
                "schedules": normalized_schedules,
                "schedule_hashes": schedule_hashes,
                "account_audits_by_date": decisions["account_audits"],
                "online_by_date": decisions["online_by_date"],
                "overlay_by_date": decisions["overlay_by_date"],
                "future_feedback_violation_count": decisions[
                    "future_feedback_violation_count"
                ],
                "future_overlay_violation_count": decisions[
                    "future_overlay_violation_count"
                ],
            }
        )

    ready_dates = [pd.Timestamp(row["ready_date"]) for row in offset_payloads]
    if len(ready_dates) != len(offsets) or any(pd.isna(value) for value in ready_dates):
        raise RuntimeError("adaptive common start requires readiness for every offset")
    common_start = max(ready_dates)
    common_start_date = common_start.date().isoformat()
    all_scoring_audits: list[dict[str, Any]] = []
    target_schedule_coverages: list[float] = []
    overlay_coverages: list[float] = []
    overlay_below_one_fractions: list[float] = []
    base_account_results: dict[str, dict[str, Any]] = {}

    for payload in offset_payloads:
        offset = int(payload["rebalance_offset_days"])
        offset_dir = root / f"offset-{offset:02d}"
        account_dir = offset_dir / "accounts"
        account_dir.mkdir(parents=True, exist_ok=True)
        scoring_config = replace(
            base_config,
            rebalance_offset_days=offset,
            evaluation_start_date=common_start_date,
            position_count=maximum_positions,
            target_weight=1.0,
        )
        required_dates = {
            date_value
            for date_value in payload["schedules"]["fixed_core_full"]
            if pd.Timestamp(date_value) >= common_start
        }
        if not required_dates:
            raise RuntimeError("adaptive common start leaves no scoring signal dates")
        ready_overlay = [
            decision
            for date_value, decision in payload["overlay_by_date"].items()
            if pd.Timestamp(date_value) >= common_start
        ]
        overlay_coverage = (
            sum(_overlay_ready(value) for value in ready_overlay) / len(ready_overlay)
            if ready_overlay
            else 0.0
        )
        overlay_coverages.append(float(overlay_coverage))
        exposures = [_overlay_exposure(value) for value in ready_overlay]
        overlay_below_one_fractions.append(
            float(np.mean(np.asarray(exposures) < 1.0)) if exposures else 0.0
        )

        account_results: dict[str, dict[str, Any]] = {}
        account_audits: list[dict[str, Any]] = []
        account_schedule_audits: list[dict[str, Any]] = []
        for account_name in _ACCOUNT_NAMES:
            full_schedule = payload["schedules"][account_name]
            # A scoring account is born at the common equal-AUM boundary.  Do
            # not pass pre-birth targets to the portfolio engine: besides
            # being economically irrelevant, overlay warm-up decisions may
            # intentionally be all-cash.  The cache hash must bind the exact
            # schedule actually supplied to the engine, not the wider shadow
            # decision history retained in decisions.json.
            schedule = {
                date_value: dict(weights)
                for date_value, weights in full_schedule.items()
                if pd.Timestamp(date_value) >= common_start
            }
            scoring_schedule_sha256 = _target_schedule_sha256(schedule)
            observed_dates = {
                date_value for date_value in schedule
            }
            coverage = len(required_dates & observed_dates) / len(required_dates)
            unexpected_dates = sorted(observed_dates - required_dates)
            schedule_valid = bool(coverage >= 1.0 and not unexpected_dates)
            schedule_audit = {
                "account_name": account_name,
                "required_signal_date_count": len(required_dates),
                "observed_signal_date_count": len(observed_dates),
                "coverage": round(float(coverage), 8),
                "unexpected_signal_dates": unexpected_dates,
                "valid": schedule_valid,
            }
            account_schedule_audits.append(schedule_audit)
            target_schedule_coverages.append(float(coverage) if schedule_valid else 0.0)

            account_factor = FactorSpec(
                name=account_name,
                family="adaptive",
                kind="ensemble",
                direction_policy="pre_directed",
                params={
                    "protocol_id": _PROTOCOL_ID,
                    "protocol_sha256": protocol_sha256,
                    "account_name": account_name,
                    "expert_registry": list(expert_names),
                    "decision_history_policy": _HISTORY_POLICY,
                },
                role="adaptive_scoring_account",
            )
            account_validation = replace(
                validations[control.name],
                factor_name=account_name,
                family="adaptive",
                frozen_direction=1,
                selection_basis="frozen_adaptive_protocol",
            )
            path = account_dir / f"{_safe_name(account_name)}.json"
            result: dict[str, Any] | None = None
            if resume and path.is_file():
                try:
                    cached_payload = _read_json(path)
                except (OSError, ValueError, TypeError, json.JSONDecodeError):
                    cached_payload = {}
                result = _cache_result(
                    cached_payload,
                    run_fingerprint=run_fingerprint,
                    protocol_sha256=protocol_sha256,
                    offset=offset,
                    role="equal_aum_adaptive_scoring_account",
                    factor_name=account_name,
                    evaluation_start_date=common_start_date,
                    decisions_sha256=payload["decisions_sha256"],
                    target_schedule_sha256=scoring_schedule_sha256,
                )
            if result is None:
                scoring_audit_by_date = {
                    date_value: dict(audit)
                    for date_value, audit in payload["account_audits_by_date"][
                        account_name
                    ].items()
                    if pd.Timestamp(date_value) >= common_start
                }
                result = portfolio_result(
                    account_factor,
                    account_validation,
                    signals[control.name],
                    features,
                    execution,
                    scoring_config,
                    research_config,
                    target_weights_by_date=schedule,
                    optimization_audit_by_date=scoring_audit_by_date,
                    require_optimized_targets=True,
                )
                result = _runtime_composed_result(
                    result,
                    factor=account_factor,
                    execution_validation=account_validation,
                    composition_basis="frozen_adaptive_expert_targets",
                )
                _write_json(
                    path,
                    {
                        "run_fingerprint": run_fingerprint,
                        "protocol_id": _PROTOCOL_ID,
                        "protocol_sha256": protocol_sha256,
                        "rebalance_offset_days": offset,
                        "role": "equal_aum_adaptive_scoring_account",
                        "account_name": account_name,
                        "evaluation_start_date": common_start_date,
                        "decisions_path": "../decisions.json",
                        "decisions_sha256": payload["decisions_sha256"],
                        "target_schedule_sha256": scoring_schedule_sha256,
                        "decision_target_schedule_sha256": payload[
                            "schedule_hashes"
                        ][account_name],
                        "result_sha256": _result_sha256(result),
                        "result": result,
                    },
                )
            account_results[account_name] = result
            audit = _equal_aum_account_audit(
                result,
                requested_start_date=common_start_date,
                initial_nav=base_config.capital,
            )
            audit.update(
                {
                    "rebalance_offset_days": offset,
                    "factor_name": account_name,
                    "account_name": account_name,
                    "account_role": "adaptive_scoring",
                    "target_schedule_audit": schedule_audit,
                }
            )
            if not schedule_valid:
                audit["valid"] = False
                audit["reasons"] = list(
                    dict.fromkeys(
                        [*(audit.get("reasons") or []), "target_schedule_incomplete"]
                    )
                )
            account_audits.append(audit)
            all_scoring_audits.append(audit)
        payload["account_results"] = account_results
        payload["scoring_account_audits"] = account_audits
        payload["target_schedule_audits"] = account_schedule_audits
        payload["overlay_signal_coverage"] = round(float(overlay_coverage), 8)
        payload["fraction_signal_dates_exposure_below_one"] = round(
            overlay_below_one_fractions[-1], 8
        )
        if offset == int(base_config.rebalance_offset_days):
            base_account_results = dict(account_results)

    metrics_by_account: dict[str, list[dict[str, Any]]] = {
        name: [] for name in _ACCOUNT_NAMES
    }
    compact_offsets: list[dict[str, Any]] = []
    for payload in offset_payloads:
        reference_periods = list(
            payload["account_results"]["fixed_core_full"].get(
                "period_active_returns"
            )
            or []
        )
        per_account: dict[str, dict[str, Any]] = {}
        audit_by_account = {
            str(row["account_name"]): row
            for row in payload["scoring_account_audits"]
        }
        for account_name in _ACCOUNT_NAMES:
            metrics = historical_metrics(
                payload["account_results"][account_name],
                research_config,
                periods_per_year=base_config.periods_per_year,
                reference_periods=reference_periods,
                optimization_scope=_EVIDENCE_CLASS,
            )
            # The shared historical-metrics callback also serves the legacy
            # leaderboard and therefore carries empty score scaffolding.  It
            # has no meaning in 5.0 and must not leak into adaptive artifacts.
            for key in (
                "score_weights",
                "score_percentiles",
                "historical_score",
                "score_method",
                "missing_period_score_policy",
                "incomplete_period_ranking_policy",
            ):
                metrics.pop(key, None)
            audit = audit_by_account[account_name]
            metrics["equal_aum_account_audit_valid"] = audit.get("valid") is True
            metrics["equal_aum_account_audit_reasons"] = list(
                audit.get("reasons") or []
            )
            metrics_by_account[account_name].append(metrics)
            per_account[account_name] = metrics
        compact_offsets.append(
            {
                "rebalance_offset_days": payload["rebalance_offset_days"],
                "ready_date": payload["ready_date"],
                "decisions_sha256": payload["decisions_sha256"],
                "future_feedback_violation_count": payload[
                    "future_feedback_violation_count"
                ],
                "future_overlay_violation_count": payload[
                    "future_overlay_violation_count"
                ],
                "shadow_account_audits": payload["shadow_audits"],
                "overlay_signal_coverage": payload["overlay_signal_coverage"],
                "fraction_signal_dates_exposure_below_one": payload[
                    "fraction_signal_dates_exposure_below_one"
                ],
                "target_schedule_audits": payload["target_schedule_audits"],
                "scoring_account_audits": payload["scoring_account_audits"],
                "metrics": per_account,
            }
        )

    phase_distributions = _phase_distributions(
        metrics_by_account, quantile=phase_quantile
    )
    comparisons = _paired_comparisons(
        metrics_by_account, quantile=phase_quantile
    )
    mean_overlay_fraction = float(np.mean(overlay_below_one_fractions))
    gates = _evaluate_frozen_gates(
        frozen["gates"],
        comparisons,
        mean_overlay_fraction=mean_overlay_fraction,
    )
    future_feedback_violations = sum(
        int(row["future_feedback_violation_count"]) for row in offset_payloads
    )
    future_overlay_violations = sum(
        int(row["future_overlay_violation_count"]) for row in offset_payloads
    )
    expected_shadow_count = len(offsets) * len(expert_names)
    expected_scoring_count = len(offsets) * len(_ACCOUNT_NAMES)
    actual_shadow_count = sum(
        len(dict(row.get("shadow_results") or {})) for row in offset_payloads
    )
    runtime_integrity_passed, integrity = _runtime_integrity(
        protocol=protocol,
        protocol_sha256=protocol_sha256,
        shadow_count=actual_shadow_count,
        expected_shadow_count=expected_shadow_count,
        shadow_audits=all_shadow_audits,
        scoring_audits=all_scoring_audits,
        expected_scoring_count=expected_scoring_count,
        metrics_by_account=metrics_by_account,
        future_feedback_violations=future_feedback_violations,
        future_overlay_violations=future_overlay_violations,
        overlay_signal_coverage_min=min(overlay_coverages, default=0.0),
        target_schedule_coverage_min=min(target_schedule_coverages, default=0.0),
    )
    route = _determine_route(gates, integrity_passed=runtime_integrity_passed)
    equal_aum_violations = [
        row for row in all_scoring_audits if row.get("valid") is not True
    ]
    shadow_violations = [
        row for row in all_shadow_audits if row.get("valid") is not True
    ]
    summary = {
        "schema_version": 1,
        "enabled": True,
        "protocol": "adaptive_core_overlay",
        "protocol_id": _PROTOCOL_ID,
        "protocol_sha256": protocol_sha256,
        "protocol_status": protocol.get("status"),
        "evidence_class": _EVIDENCE_CLASS,
        "canary_smoke_only": False,
        "strategy_ordering_performed": False,
        "expert_registry": list(expert_names),
        "fixed_core_expert": frozen["fixed_core"],
        "account_registry": list(_ACCOUNT_NAMES),
        "scoring_account_registry": list(_ACCOUNT_NAMES),
        "rebalance_offsets": list(offsets),
        "phase_quantile": phase_quantile,
        "common_evaluation_start_policy": frozen["portfolio"].get(
            "common_evaluation_start_policy"
        ),
        "common_evaluation_start": common_start_date,
        "shadow_account_role": "causal_feedback_only_not_scoring",
        "shadow_account_count": actual_shadow_count,
        "expected_shadow_account_count": expected_shadow_count,
        "shadow_accounts_valid": not shadow_violations
        and actual_shadow_count == expected_shadow_count,
        "shadow_account_audits": all_shadow_audits,
        "shadow_account_violations": shadow_violations,
        "scoring_account_protocol": "fresh_cash_equal_aum_common_start",
        "scoring_initial_nav": float(base_config.capital),
        "scoring_account_count": len(all_scoring_audits),
        "expected_scoring_account_count": expected_scoring_count,
        "equal_aum_scoring_valid": not equal_aum_violations
        and len(all_scoring_audits) == expected_scoring_count,
        "scoring_accounts_valid": not equal_aum_violations
        and len(all_scoring_audits) == expected_scoring_count,
        "equal_aum_scoring_violations": equal_aum_violations,
        "feedback_history_policy": _HISTORY_POLICY,
        "future_feedback_violation_count": future_feedback_violations,
        "future_overlay_violation_count": future_overlay_violations,
        "overlay_signal_coverage_min": round(min(overlay_coverages, default=0.0), 8),
        "mean_fraction_signal_dates_exposure_below_one": round(
            mean_overlay_fraction, 8
        ),
        "causal_history_valid": bool(
            future_feedback_violations == 0
            and future_overlay_violations == 0
            and min(overlay_coverages, default=0.0) >= 1.0
        ),
        "runtime_integrity": integrity,
        "integrity_valid": runtime_integrity_passed,
        "account_phase_distributions": phase_distributions,
        "paired_comparisons": comparisons,
        "gate_results": gates,
        "frozen_gate_results": gates,
        "frozen_route": route.get("selected_account"),
        "route": route,
        "offsets": compact_offsets,
        # Historical execution cannot activate the create-only ledger.  The
        # ledger transitions to ``awaiting_new_data`` only after the release
        # tag and this exact clean run have been verified by ``activate``.
        "prospective_status": "not_activated",
    }
    _assert_no_ordering_schema_keys(summary)
    _write_json(root / "adaptive-summary.json", summary)
    return summary, base_account_results


__all__ = ["run_adaptive_sweep", "validate_adaptive_protocol"]

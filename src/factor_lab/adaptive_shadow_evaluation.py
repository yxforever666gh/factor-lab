"""Paired prospective evaluation for the 5.9 adaptive shadow registry.

This module is deliberately pure.  It consumes already-costed shadow outcomes,
compares every challenger on its own cohorts paired with the frozen control,
and can recommend review but can never promote a capital route.  Portfolio
performance is measured on one master NAV formed from the mean wealth of the
ten offset sleeves, never by pretending the sleeves trade sequentially.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
import hashlib
import json
import math
import re
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
from scipy import stats


CONTROL_ID = "formal_fixed_core_full"
OFFSET_COUNT = 10
HAC_LAG = 10
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ALLOWED_CONCLUSIONS = frozenset(
    {"continue", "retire", "eligible_for_major_review"}
)


class ShadowEvaluationError(ValueError):
    """Raised when shadow evidence or its frozen evaluation contract is invalid."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _mapping(value: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ShadowEvaluationError(f"{label} must be an object")
    return dict(value)


def _integer(value: Any, *, label: str, minimum: int | None = None) -> int:
    if type(value) is not int:
        raise ShadowEvaluationError(f"{label} must be an integer")
    if minimum is not None and value < minimum:
        raise ShadowEvaluationError(f"{label} must be at least {minimum}")
    return value


def _date(value: Any, *, label: str) -> str:
    try:
        parsed = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise ShadowEvaluationError(f"{label} must be a calendar date") from exc
    if pd.isna(parsed) or parsed.tzinfo is not None or parsed != parsed.normalize():
        raise ShadowEvaluationError(f"{label} must be a timezone-free calendar date")
    return parsed.date().isoformat()


def _protocol_contract(protocol: Mapping[str, Any]) -> dict[str, Any]:
    root = _mapping(protocol, label="protocol")
    if root.get("schema_version") != 1:
        raise ShadowEvaluationError("unsupported shadow protocol schema")
    formal = _mapping(root.get("formal_route"), label="formal_route")
    if (
        formal.get("route") != "fixed_core_full"
        or formal.get("mutation_allowed") is not False
        or formal.get("automatic_promotion_allowed") is not False
    ):
        raise ShadowEvaluationError("formal route isolation differs from 5.9")
    registry = _mapping(root.get("registry"), label="registry")
    ordered = tuple(str(value) for value in registry.get("ordered_candidates") or ())
    candidates = registry.get("candidates")
    if (
        not 1 <= len(ordered) <= 3
        or len(set(ordered)) != len(ordered)
        or not isinstance(candidates, list)
        or [str(_mapping(row, label="candidate").get("id")) for row in candidates]
        != list(ordered)
    ):
        raise ShadowEvaluationError("candidate registry is invalid or reordered")
    evaluation = _mapping(root.get("evaluation"), label="evaluation")
    if set(evaluation.get("allowed_conclusions") or ()) != _ALLOWED_CONCLUSIONS:
        raise ShadowEvaluationError("evaluation conclusions differ from 5.9")
    if evaluation.get("automatic_promotion_allowed") is not False:
        raise ShadowEvaluationError("5.9 must never auto-promote")
    if evaluation.get("multiplicity_method") != (
        "holm_bonferroni_over_registered_challengers"
    ):
        raise ShadowEvaluationError("unsupported multiplicity method")
    frozen_evaluation = {
        "significance_test": (
            "one_sided_Newey_West_HAC_on_paired_daily_master_NAV_returns"
        ),
        "newey_west_lag": HAC_LAG,
        "master_nav_timeline": (
            "union_of_sealed_daily_paths_all_offsets_updated_before_daily_mean"
        ),
        "unstarted_offset_wealth": 1,
        "monthly_pass_definition": "last_checkpoint_state_for_each_natural_month",
        "monthly_close_rule": (
            "only_months_strictly_before_the_Asia_Shanghai_evaluation_month_count"
        ),
        "same_month_update_rule": "later_checkpoint_replaces_earlier_state",
        "requires_zero_candidate_blocked_orders": True,
        "requires_zero_candidate_missed_deadlines": True,
        "requires_zero_candidate_terminated_offsets": True,
    }
    if any(evaluation.get(key) != value for key, value in frozen_evaluation.items()):
        raise ShadowEvaluationError("frozen 5.9 evaluation contract differs")
    return {
        "candidate_ids": ordered,
        "expert_ids": (CONTROL_ID, *ordered),
        "evaluation": evaluation,
    }


def _normalise_outcomes(
    outcomes: Sequence[Mapping[str, Any]],
    *,
    expert_ids: Sequence[str],
    cutoff_date: str,
) -> tuple[list[dict[str, Any]], int]:
    allowed = set(expert_ids)
    matured: list[dict[str, Any]] = []
    excluded = 0
    seen: set[tuple[str, str, str, int]] = set()
    for index, raw in enumerate(outcomes):
        row = _mapping(raw, label=f"outcome[{index}]")
        expert = str(row.get("candidate_id") or "")
        if expert not in allowed:
            raise ShadowEvaluationError(f"outcome[{index}] has an unregistered expert")
        signal = _date(row.get("signal_date"), label=f"outcome[{index}].signal_date")
        end = _date(row.get("end_date"), label=f"outcome[{index}].end_date")
        if end <= signal:
            raise ShadowEvaluationError("outcome end_date must follow signal_date")
        offset = _integer(row.get("offset"), label=f"outcome[{index}].offset", minimum=0)
        if offset >= OFFSET_COUNT:
            raise ShadowEvaluationError("outcome offset must be in [0, 9]")
        net = _integer(
            row.get("net_return_ppm"),
            label=f"outcome[{index}].net_return_ppm",
        )
        if net <= -1_000_000:
            raise ShadowEvaluationError("net return must be greater than -1000000 PPM")
        opening_nav = _integer(
            row.get("opening_nav_fen"),
            label=f"outcome[{index}].opening_nav_fen",
            minimum=1,
        )
        ending_nav = _integer(
            row.get("ending_nav_fen"),
            label=f"outcome[{index}].ending_nav_fen",
            minimum=1,
        )
        blocked = _integer(
            row.get("blocked_order_count"),
            label=f"outcome[{index}].blocked_order_count",
            minimum=0,
        )
        raw_path = row.get("daily_path")
        if not isinstance(raw_path, list) or len(raw_path) != 11:
            raise ShadowEvaluationError(
                f"outcome[{index}].daily_path must contain eleven observations"
            )
        daily_path: list[dict[str, Any]] = []
        for path_index, raw_point in enumerate(raw_path):
            point = _mapping(
                raw_point,
                label=f"outcome[{index}].daily_path[{path_index}]",
            )
            daily_path.append(
                {
                    "date": _date(
                        point.get("date"),
                        label=f"outcome[{index}].daily_path[{path_index}].date",
                    ),
                    "account_nav_fen": _integer(
                        point.get("account_nav_fen"),
                        label=(
                            f"outcome[{index}].daily_path[{path_index}]"
                            ".account_nav_fen"
                        ),
                        minimum=0,
                    ),
                    "benchmark_index_ppb": _integer(
                        point.get("benchmark_index_ppb"),
                        label=(
                            f"outcome[{index}].daily_path[{path_index}]"
                            ".benchmark_index_ppb"
                        ),
                        minimum=0,
                    ),
                }
            )
        path_dates = [point["date"] for point in daily_path]
        if (
            any(left >= right for left, right in zip(path_dates, path_dates[1:]))
            or path_dates[0] <= signal
            or path_dates[-1] != end
            or daily_path[-1]["account_nav_fen"] != ending_nav
        ):
            raise ShadowEvaluationError(
                "daily_path does not bind ordered post-signal observations to ending NAV"
            )
        implied_ppm = int(round((ending_nav / opening_nav - 1.0) * 1_000_000.0))
        if abs(implied_ppm - net) > 1:
            raise ShadowEvaluationError(
                "opening/ending NAV does not reconcile to net_return_ppm"
            )
        target_sha = str(row.get("plan_targets_sha256") or "")
        if not _SHA256_RE.fullmatch(target_sha):
            raise ShadowEvaluationError("outcome plan target hash is invalid")
        decision_sha = str(row.get("formal_decision_record_sha256") or "")
        if not _SHA256_RE.fullmatch(decision_sha):
            raise ShadowEvaluationError("outcome formal decision hash is invalid")
        key = (expert, signal, end, offset)
        if key in seen:
            raise ShadowEvaluationError("duplicate candidate/cohort outcome")
        seen.add(key)
        normalized = {
            "candidate_id": expert,
            "signal_date": signal,
            "end_date": end,
            "offset": offset,
            "net_return_ppm": net,
            "opening_nav_fen": opening_nav,
            "ending_nav_fen": ending_nav,
            "blocked_order_count": blocked,
            "daily_path": daily_path,
            "plan_targets_sha256": target_sha,
            "formal_decision_record_sha256": decision_sha,
        }
        if end <= cutoff_date:
            matured.append(normalized)
        else:
            excluded += 1
    return matured, excluded


def _compound_nav_ratio_ppm(rows: Sequence[Mapping[str, Any]]) -> int:
    wealth = 1.0
    for row in rows:
        wealth *= int(row["ending_nav_fen"]) / int(row["opening_nav_fen"])
    if not math.isfinite(wealth):
        raise ShadowEvaluationError("offset wealth is non-finite")
    return int(round((wealth - 1.0) * 1_000_000.0))


def _cycle_key(row: Mapping[str, Any]) -> tuple[str, str, int]:
    return (
        str(row["daily_path"][0]["date"]),
        str(row["end_date"]),
        int(row["offset"]),
    )


def _master_nav_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Build the real daily NAV of ten independently compounded sleeves.

    Each sleeve begins with one unit of wealth.  Before its first admitted
    cycle it stays at one.  A cycle's sealed daily account NAV is rebased to
    the sleeve's wealth at that cycle boundary.  All offsets observed on the
    same calendar date are updated together before their mean is measured.
    This preserves intra-cycle drawdowns and same-day rebalance costs that an
    endpoint-only path would hide.
    """

    by_offset: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        by_offset[int(row["offset"])].append(row)

    updates: dict[str, dict[int, float]] = defaultdict(dict)
    terminal_wealth = np.ones(OFFSET_COUNT, dtype=float)
    cycle_trace: list[dict[str, Any]] = []
    for offset in range(OFFSET_COUNT):
        sleeve_wealth = 1.0
        previous_ending_nav: int | None = None
        previous_end_date: str | None = None
        for row in sorted(by_offset.get(offset, ()), key=_cycle_key):
            opening_nav = int(row["opening_nav_fen"])
            ending_nav = int(row["ending_nav_fen"])
            path = list(row["daily_path"])
            if previous_ending_nav is not None and (
                opening_nav != previous_ending_nav
                or str(path[0]["date"]) != previous_end_date
            ):
                raise ShadowEvaluationError(
                    "same-offset daily NAV is not continuous across paired cycles"
                )
            for point in path:
                wealth = sleeve_wealth * int(point["account_nav_fen"]) / opening_nav
                if not math.isfinite(wealth) or wealth < 0.0:
                    raise ShadowEvaluationError("daily sleeve wealth is non-finite or negative")
                # Adjacent cycles share a boundary date.  The newer cycle's
                # post-rebalance point intentionally replaces the older
                # pre-rebalance endpoint so fees cannot disappear.
                updates[str(point["date"])][offset] = wealth
            factor = ending_nav / opening_nav
            sleeve_wealth *= factor
            previous_ending_nav = ending_nav
            previous_end_date = str(row["end_date"])
            cycle_trace.append(
                {
                    "offset": offset,
                    "signal_date": str(row["signal_date"]),
                    "end_date": str(row["end_date"]),
                    "cycle_wealth_factor_hex": factor.hex(),
                    "terminal_sleeve_wealth_hex": sleeve_wealth.hex(),
                }
            )
        terminal_wealth[offset] = sleeve_wealth

    wealth = np.ones(OFFSET_COUNT, dtype=float)
    peak = 1.0
    worst = 0.0
    previous_master_nav = 1.0
    daily_returns_ppm: list[float] = []
    daily_dates: list[str] = []
    daily_trace: list[dict[str, Any]] = []
    for sequence, observation_date in enumerate(sorted(updates), start=1):
        changed = updates[observation_date]
        for offset, value in changed.items():
            wealth[offset] = value
        master_nav = float(np.mean(wealth))
        if not math.isfinite(master_nav) or master_nav < 0.0:
            raise ShadowEvaluationError("master NAV is non-finite or negative")
        peak = max(peak, master_nav)
        worst = min(worst, master_nav / peak - 1.0 if peak > 0.0 else -1.0)
        daily_return_ppm = (
            (master_nav / previous_master_nav - 1.0) * 1_000_000.0
            if previous_master_nav > 0.0
            else -1_000_000.0
        )
        daily_dates.append(observation_date)
        daily_returns_ppm.append(daily_return_ppm)
        daily_trace.append(
            {
                "sequence": sequence,
                "date": observation_date,
                "updated_offsets": {
                    str(offset): changed[offset].hex() for offset in sorted(changed)
                },
                "master_daily_return_ppm_hex": daily_return_ppm.hex(),
                "master_nav_hex": master_nav.hex(),
            }
        )
        previous_master_nav = master_nav

    terminal = float(np.mean(terminal_wealth))
    if abs(terminal - previous_master_nav) > 1e-12:
        raise ShadowEvaluationError("daily master NAV does not reach terminal sleeve wealth")
    return {
        "compound_return_ppm": int(round((terminal - 1.0) * 1_000_000.0)),
        "max_drawdown_ppm": int(round(worst * 1_000_000.0)),
        "daily_observation_count": len(daily_trace),
        "terminal_master_nav_hex": terminal.hex(),
        "daily_trace_sha256": _sha256(daily_trace),
        "cycle_trace_sha256": _sha256(cycle_trace),
        "daily_dates": daily_dates,
        "daily_returns_ppm": daily_returns_ppm,
    }


def _one_sided_newey_west_pvalue(
    values_ppm: Sequence[int | float], *, lag: int = HAC_LAG
) -> tuple[float, dict[str, Any]]:
    """Test a positive paired mean with a Bartlett Newey-West variance.

    The lag and standard-normal reference distribution are part of the frozen
    output contract.  Missing higher lags in a short series contribute zero;
    the Bartlett denominator remains ``lag + 1``.
    """

    if lag != HAC_LAG:
        raise ShadowEvaluationError(f"Newey-West lag must remain {HAC_LAG}")
    if len(values_ppm) < 2:
        return 1.0, {
            "lag": lag,
            "reference_distribution": "standard_normal",
            "observation_count": len(values_ppm),
            "mean_active_return_ppm_hex": float(
                np.mean(values_ppm) if values_ppm else 0.0
            ).hex(),
            "standard_error_ppm_hex": 0.0.hex(),
            "z_statistic_hex": 0.0.hex(),
        }
    values = np.asarray(values_ppm, dtype=float)
    if not bool(np.isfinite(values).all()):
        raise ShadowEvaluationError("paired active returns must be finite")
    count = len(values)
    mean = float(np.mean(values))
    residual = values - mean
    long_run_variance = float(np.dot(residual, residual) / count)
    for distance in range(1, min(lag, count - 1) + 1):
        covariance = float(
            np.dot(residual[distance:], residual[:-distance]) / count
        )
        weight = 1.0 - distance / (lag + 1.0)
        long_run_variance += 2.0 * weight * covariance
    # Floating cancellation can make a theoretically non-negative estimate
    # a few ulps negative.
    long_run_variance = max(0.0, long_run_variance)
    standard_error = math.sqrt(long_run_variance / count)
    if standard_error <= 0.0:
        z_statistic = math.inf if mean > 0.0 else (-math.inf if mean < 0.0 else 0.0)
        pvalue = 0.0 if mean > 0.0 else 1.0
    else:
        z_statistic = mean / standard_error
        pvalue = float(stats.norm.sf(z_statistic))
        if not math.isfinite(pvalue):
            pvalue = 1.0
    audit = {
        "lag": lag,
        "reference_distribution": "standard_normal",
        "observation_count": count,
        "mean_active_return_ppm_hex": mean.hex(),
        "standard_error_ppm_hex": standard_error.hex(),
        "z_statistic_hex": (
            z_statistic.hex() if math.isfinite(z_statistic) else str(z_statistic)
        ),
    }
    return min(1.0, max(0.0, pvalue)), audit


def _pvalue_ppm(value: float) -> int:
    return int(round(min(1.0, max(0.0, value)) * 1_000_000.0))


def _holm_adjust(raw: Mapping[str, float]) -> dict[str, float]:
    ordered = sorted(raw.items(), key=lambda item: (item[1], item[0]))
    count = len(ordered)
    adjusted: dict[str, float] = {}
    running = 0.0
    for rank, (candidate, pvalue) in enumerate(ordered):
        running = max(running, min(1.0, (count - rank) * pvalue))
        adjusted[candidate] = running
    return adjusted


def _month(value: Any, *, label: str) -> pd.Period:
    if not isinstance(value, str) or re.fullmatch(r"[0-9]{4}-[0-9]{2}", value) is None:
        raise ShadowEvaluationError(f"{label} must be a canonical calendar month")
    try:
        period = pd.Period(value, freq="M")
    except (TypeError, ValueError) as exc:
        raise ShadowEvaluationError(f"{label} must be a canonical calendar month") from exc
    if str(period) != value:
        raise ShadowEvaluationError(f"{label} must be a canonical calendar month")
    return period


def _normalise_prior_monthly_states(
    value: Mapping[str, Mapping[str, bool]] | None,
    *,
    candidate_ids: Sequence[str],
    cutoff_date: str,
) -> dict[str, dict[str, bool]]:
    if value is None:
        return {candidate: {} for candidate in candidate_ids}
    root = _mapping(value, label="prior_monthly_states")
    if set(root) != set(candidate_ids):
        raise ShadowEvaluationError(
            "prior_monthly_states must name every registered challenger exactly"
        )
    cutoff_month = pd.Timestamp(cutoff_date).to_period("M")
    normalized: dict[str, dict[str, bool]] = {}
    for candidate in candidate_ids:
        raw = _mapping(root[candidate], label=f"prior_monthly_states.{candidate}")
        states: dict[str, bool] = {}
        for key, state in raw.items():
            period = _month(key, label=f"prior_monthly_states.{candidate} month")
            if period > cutoff_month or type(state) is not bool:
                raise ShadowEvaluationError(
                    "prior monthly state is later than cutoff or is not boolean"
                )
            states[str(period)] = state
        normalized[candidate] = dict(sorted(states.items()))
    return normalized


def _closed_month_streak(
    cutoff_date: str,
    evaluation_date: str,
    prior_states: Mapping[str, bool],
    *,
    current_pass: bool,
) -> tuple[int, dict[str, bool], str]:
    states = dict(prior_states)
    cutoff_month = pd.Timestamp(cutoff_date).to_period("M")
    evaluation_month = pd.Timestamp(evaluation_date).to_period("M")
    states[str(cutoff_month)] = current_pass
    last_closed = evaluation_month - 1
    count = 0
    cursor = last_closed
    while states.get(str(cursor)) is True:
        count += 1
        cursor -= 1
    return count, dict(sorted(states.items())), str(last_closed)


def _normalise_evidence_quality(
    value: Mapping[str, Any],
    *,
    candidate_ids: Sequence[str],
) -> tuple[dict[str, Any], dict[str, int], dict[str, dict[str, Any]]]:
    quality = _mapping(value, label="evidence_quality")
    violations = {
        "pit": _integer(
            quality.get("pit_violation_count"),
            label="evidence_quality.pit_violation_count",
            minimum=0,
        ),
        "integrity": _integer(
            quality.get("integrity_violation_count"),
            label="evidence_quality.integrity_violation_count",
            minimum=0,
        ),
    }
    deep_replay_valid = quality.get("deep_replay_valid")
    if type(deep_replay_valid) is not bool:
        raise ShadowEvaluationError("evidence_quality.deep_replay_valid must be boolean")
    raw_candidates = _mapping(
        quality.get("candidate_quality"),
        label="evidence_quality.candidate_quality",
    )
    if set(raw_candidates) != set(candidate_ids):
        raise ShadowEvaluationError(
            "evidence_quality.candidate_quality must name every challenger exactly"
        )
    candidates: dict[str, dict[str, Any]] = {}
    for candidate in candidate_ids:
        raw = _mapping(
            raw_candidates[candidate],
            label=f"evidence_quality.candidate_quality.{candidate}",
        )
        missed_deadline_count = _integer(
            raw.get("missed_deadline_count"),
            label=f"candidate_quality.{candidate}.missed_deadline_count",
            minimum=0,
        )
        missed_record_count = _integer(
            raw.get("missed_record_count"),
            label=f"candidate_quality.{candidate}.missed_record_count",
            minimum=0,
        )
        terminated_offset_count = _integer(
            raw.get("terminated_offset_count"),
            label=f"candidate_quality.{candidate}.terminated_offset_count",
            minimum=0,
        )
        offsets = raw.get("terminated_offsets")
        shas = raw.get("missed_record_sha256s")
        if (
            not isinstance(offsets, list)
            or any(type(offset) is not int or not 0 <= offset < OFFSET_COUNT for offset in offsets)
            or offsets != sorted(set(offsets))
            or not isinstance(shas, list)
            or any(not isinstance(sha, str) or _SHA256_RE.fullmatch(sha) is None for sha in shas)
            or len(shas) != len(set(shas))
            or terminated_offset_count != len(offsets)
            or missed_record_count != len(shas)
            or missed_deadline_count > missed_record_count
            or (missed_deadline_count > 0) != (terminated_offset_count > 0)
        ):
            raise ShadowEvaluationError(
                f"candidate_quality.{candidate} counts and sealed evidence differ"
            )
        candidates[candidate] = {
            **raw,
            "missed_deadline_count": missed_deadline_count,
            "missed_record_count": missed_record_count,
            "terminated_offset_count": terminated_offset_count,
            "terminated_offsets": offsets,
            "missed_record_sha256s": shas,
        }
    normalized = {
        **quality,
        "pit_violation_count": violations["pit"],
        "integrity_violation_count": violations["integrity"],
        "deep_replay_valid": deep_replay_valid,
        "candidate_quality": candidates,
    }
    try:
        _canonical_bytes(normalized)
    except (TypeError, ValueError) as exc:
        raise ShadowEvaluationError("evidence_quality must be canonical JSON") from exc
    return normalized, violations, candidates


def evaluate_shadow_outcomes(
    protocol: Mapping[str, Any],
    outcomes: Sequence[Mapping[str, Any]],
    *,
    cutoff_date: str | date,
    evaluation_date: str | date,
    evidence_quality: Mapping[str, Any],
    prior_monthly_states: Mapping[str, Mapping[str, bool]] | None = None,
) -> dict[str, Any]:
    """Evaluate candidate-specific control pairs without promotion authority.

    Evidence-quality counts are mandatory.  A caller must attest them instead
    of silently receiving a zero-violation assumption.
    """

    contract = _protocol_contract(protocol)
    cutoff = _date(cutoff_date, label="cutoff_date")
    evaluation_day = _date(evaluation_date, label="evaluation_date")
    if evaluation_day < cutoff:
        raise ShadowEvaluationError("evaluation_date cannot predate cutoff_date")
    normalized_quality, violations, candidate_quality = _normalise_evidence_quality(
        evidence_quality,
        candidate_ids=contract["candidate_ids"],
    )
    prior_states = _normalise_prior_monthly_states(
        prior_monthly_states,
        candidate_ids=contract["candidate_ids"],
        cutoff_date=cutoff,
    )
    rows, excluded = _normalise_outcomes(
        outcomes,
        expert_ids=contract["expert_ids"],
        cutoff_date=cutoff,
    )
    by_cohort: dict[tuple[str, str, int], dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in rows:
        cohort = (row["signal_date"], row["end_date"], row["offset"])
        by_cohort[cohort][row["candidate_id"]] = row
    all_expert_complete_keys = sorted(
        cohort
        for cohort, experts in by_cohort.items()
        if set(experts) == set(contract["expert_ids"])
    )
    all_expert_incomplete_count = len(by_cohort) - len(all_expert_complete_keys)
    evaluation = contract["evaluation"]
    min_common = _integer(
        evaluation.get("minimum_common_cycles_for_major_review"),
        label="minimum_common_cycles_for_major_review",
        minimum=1,
    )
    min_per_offset = _integer(
        evaluation.get("minimum_cycles_per_offset_for_major_review"),
        label="minimum_cycles_per_offset_for_major_review",
        minimum=1,
    )
    min_positive_offsets = _integer(
        evaluation.get("minimum_positive_offsets"),
        label="minimum_positive_offsets",
        minimum=1,
    )
    required_months = _integer(
        evaluation.get("minimum_consecutive_monthly_passes"),
        label="minimum_consecutive_monthly_passes",
        minimum=1,
    )
    alpha_ppm = _integer(
        evaluation.get("familywise_alpha_ppm"),
        label="familywise_alpha_ppm",
        minimum=1,
    )
    max_dd_degradation = _integer(
        evaluation.get("maximum_drawdown_degradation_ppm"),
        label="maximum_drawdown_degradation_ppm",
        minimum=0,
    )
    raw_pvalues: dict[str, float] = {}
    provisional: dict[str, dict[str, Any]] = {}
    target_vectors: dict[str, dict[tuple[str, str, int], str]] = {}
    pair_complete_counts: dict[str, int] = {}
    pair_incomplete_counts: dict[str, int] = {}
    for candidate in contract["candidate_ids"]:
        relevant_keys = [
            cohort
            for cohort, experts in by_cohort.items()
            if CONTROL_ID in experts or candidate in experts
        ]
        pair_keys = sorted(
            (
                cohort
                for cohort in relevant_keys
                if CONTROL_ID in by_cohort[cohort]
                and candidate in by_cohort[cohort]
            ),
            key=lambda cohort: (cohort[1], cohort[0], cohort[2]),
        )
        pair_complete_counts[candidate] = len(pair_keys)
        pair_incomplete_counts[candidate] = len(relevant_keys) - len(pair_keys)
        candidate_rows = [by_cohort[key][candidate] for key in pair_keys]
        control_rows = [by_cohort[key][CONTROL_ID] for key in pair_keys]
        if any(
            candidate_row["formal_decision_record_sha256"]
            != control_row["formal_decision_record_sha256"]
            for candidate_row, control_row in zip(
                candidate_rows, control_rows, strict=True
            )
        ):
            raise ShadowEvaluationError(
                "candidate/control pair binds different formal decisions"
            )
        cycle_active = [
            int(candidate_row["net_return_ppm"])
            - int(control_row["net_return_ppm"])
            for candidate_row, control_row in zip(
                candidate_rows, control_rows, strict=True
            )
        ]
        candidate_master = _master_nav_metrics(candidate_rows)
        control_master = _master_nav_metrics(control_rows)
        candidate_net = int(candidate_master["compound_return_ppm"])
        control_net = int(control_master["compound_return_ppm"])
        active_compound = candidate_net - control_net
        candidate_master_daily_returns = list(candidate_master["daily_returns_ppm"])
        control_master_daily_returns = list(control_master["daily_returns_ppm"])
        if candidate_master["daily_dates"] != control_master["daily_dates"]:
            raise ShadowEvaluationError("paired master NAV daily calendars differ")
        master_active = [
            candidate_value - control_value
            for candidate_value, control_value in zip(
                candidate_master_daily_returns,
                control_master_daily_returns,
                strict=True,
            )
        ]
        offset_counts: dict[str, int] = {}
        offset_active: dict[str, int] = {}
        for offset in range(OFFSET_COUNT):
            indexes = [
                index for index, key in enumerate(pair_keys) if key[2] == offset
            ]
            offset_counts[str(offset)] = len(indexes)
            if indexes:
                candidate_offset = _compound_nav_ratio_ppm(
                    [candidate_rows[index] for index in indexes]
                )
                control_offset = _compound_nav_ratio_ppm(
                    [control_rows[index] for index in indexes]
                )
                offset_active[str(offset)] = candidate_offset - control_offset
            else:
                offset_active[str(offset)] = 0
        positive_offsets = sum(value > 0 for value in offset_active.values())
        candidate_dd = int(candidate_master["max_drawdown_ppm"])
        control_dd = int(control_master["max_drawdown_ppm"])
        dd_degradation = max(0, abs(candidate_dd) - abs(control_dd))
        raw_pvalue, hac_audit = _one_sided_newey_west_pvalue(
            master_active, lag=HAC_LAG
        )
        raw_pvalues[candidate] = raw_pvalue
        target_vectors[candidate] = {
            key: str(row["plan_targets_sha256"])
            for key, row in zip(pair_keys, candidate_rows)
        }
        provisional[candidate] = {
            "common_cycle_count": len(pair_keys),
            "pair_complete_cohort_count": len(pair_keys),
            "pair_incomplete_cohort_count": pair_incomplete_counts[candidate],
            "offset_cycle_counts": offset_counts,
            "candidate_compound_net_return_ppm": candidate_net,
            "control_compound_net_return_ppm": control_net,
            "active_compound_net_return_ppm": active_compound,
            "mean_active_return_ppm": int(round(float(np.mean(master_active))))
            if master_active
            else 0,
            "mean_paired_cycle_active_return_ppm": int(
                round(float(np.mean(cycle_active)))
            )
            if cycle_active
            else 0,
            "positive_offset_count": positive_offsets,
            "offset_active_return_ppm": offset_active,
            "candidate_master_max_drawdown_ppm": candidate_dd,
            "control_master_max_drawdown_ppm": control_dd,
            "drawdown_degradation_ppm": dd_degradation,
            "master_nav_event_order": "calendar_date;all_offsets_then_mean",
            "candidate_master_nav_event_count": candidate_master[
                "daily_observation_count"
            ],
            "control_master_nav_event_count": control_master[
                "daily_observation_count"
            ],
            "candidate_master_nav_daily_observation_count": candidate_master[
                "daily_observation_count"
            ],
            "control_master_nav_daily_observation_count": control_master[
                "daily_observation_count"
            ],
            "candidate_terminal_master_nav_hex": candidate_master[
                "terminal_master_nav_hex"
            ],
            "control_terminal_master_nav_hex": control_master[
                "terminal_master_nav_hex"
            ],
            "candidate_master_nav_trace_sha256": candidate_master[
                "daily_trace_sha256"
            ],
            "control_master_nav_trace_sha256": control_master[
                "daily_trace_sha256"
            ],
            "candidate_cycle_trace_sha256": candidate_master[
                "cycle_trace_sha256"
            ],
            "control_cycle_trace_sha256": control_master["cycle_trace_sha256"],
            "paired_evidence_sha256": _sha256(
                [
                    {
                        "candidate": candidate_row,
                        "control": control_row,
                    }
                    for candidate_row, control_row in zip(
                        candidate_rows, control_rows, strict=True
                    )
                ]
            ),
            "candidate_blocked_order_count": sum(
                int(row["blocked_order_count"]) for row in candidate_rows
            ),
            "control_blocked_order_count": sum(
                int(row["blocked_order_count"]) for row in control_rows
            ),
            "newey_west_hac": hac_audit,
            "raw_one_sided_hac_pvalue_ppm": _pvalue_ppm(raw_pvalue),
            # Kept as a schema-compatible alias; its implementation is HAC,
            # not the former iid t-test.
            "raw_one_sided_pvalue_ppm": _pvalue_ppm(raw_pvalue),
        }
    adjusted = _holm_adjust(raw_pvalues)
    candidate_reports: list[dict[str, Any]] = []
    any_eligible = False
    all_retired = True
    for candidate in contract["candidate_ids"]:
        metrics = provisional[candidate]
        quality_row = candidate_quality[candidate]
        homogeneous = sorted(
            other
            for other in contract["candidate_ids"]
            if other != candidate
            and target_vectors[candidate]
            and target_vectors[other].keys() == target_vectors[candidate].keys()
            and target_vectors[other] == target_vectors[candidate]
        )
        gates = {
            "minimum_common_cycles": metrics["common_cycle_count"] >= min_common,
            "minimum_cycles_each_offset": all(
                count >= min_per_offset
                for count in metrics["offset_cycle_counts"].values()
            ),
            "zero_pair_incomplete_cohorts": (
                metrics["pair_incomplete_cohort_count"] == 0
            ),
            "positive_absolute_net_return": (
                metrics["candidate_compound_net_return_ppm"] > 0
            ),
            "positive_active_net_return": (
                metrics["active_compound_net_return_ppm"] > 0
            ),
            "positive_offsets": metrics["positive_offset_count"] >= min_positive_offsets,
            "holm_significant": adjusted[candidate] <= alpha_ppm / 1_000_000.0,
            "drawdown_not_materially_worse": (
                metrics["drawdown_degradation_ppm"] <= max_dd_degradation
            ),
            "not_homogeneous_with_another_challenger": not homogeneous,
            "zero_pit_violations": violations["pit"] == 0,
            "zero_integrity_violations": (
                violations["integrity"] == 0
                and normalized_quality["deep_replay_valid"] is True
            ),
            "deep_replay_valid": normalized_quality["deep_replay_valid"] is True,
            "zero_missed_deadlines": quality_row["missed_deadline_count"] == 0,
            "zero_terminated_offsets": quality_row["terminated_offset_count"] == 0,
            "zero_blocked_orders": metrics["candidate_blocked_order_count"] == 0,
            # The frozen protocol names the execution-quality gate in terms
            # of capacity.  Outcomes expose the stronger executed-kernel
            # blocked-order total, so the legacy gate is bound to that count.
            "zero_capacity_violations": (
                metrics["candidate_blocked_order_count"] == 0
            ),
        }
        current_pass = all(gates.values())
        consecutive, monthly_states, last_closed_month = _closed_month_streak(
            cutoff,
            evaluation_day,
            prior_states[candidate],
            current_pass=current_pass,
        )
        gates["consecutive_monthly_passes"] = consecutive >= required_months
        fatal_miss = (
            quality_row["missed_deadline_count"] > 0
            or quality_row["terminated_offset_count"] > 0
        )
        conclusion = (
            "eligible_for_major_review"
            if all(gates.values())
            else ("retire" if fatal_miss else "continue")
        )
        any_eligible = any_eligible or conclusion == "eligible_for_major_review"
        all_retired = all_retired and conclusion == "retire"
        candidate_reports.append(
            {
                "candidate_id": candidate,
                **metrics,
                "candidate_evidence_quality": quality_row,
                "holm_adjusted_pvalue_ppm": _pvalue_ppm(adjusted[candidate]),
                "homogeneous_with": homogeneous,
                "major_gate_pass_now": current_pass,
                "monthly_state_month": str(pd.Timestamp(cutoff).to_period("M")),
                "last_closed_month": last_closed_month,
                "monthly_states_after_current": monthly_states,
                "monthly_states_sha256": _sha256(monthly_states),
                "consecutive_monthly_pass_count": consecutive,
                "gates": gates,
                "conclusion": conclusion,
            }
        )
    payload = {
        "schema_version": 1,
        "evaluation_id": "factor-lab/adaptive-shadow-paired-evaluation/5.9",
        "cutoff_date": cutoff,
        "evaluation_date": evaluation_day,
        "control_id": CONTROL_ID,
        "candidate_ids": list(contract["candidate_ids"]),
        "matured_outcome_count": len(rows),
        "excluded_unmatured_outcome_count": excluded,
        # These two legacy aggregate counts remain for checkpoint schema
        # compatibility.  Candidate gates use the pair-specific counts below.
        "complete_common_cohort_count": len(all_expert_complete_keys),
        "incomplete_cohort_count": all_expert_incomplete_count,
        "candidate_pair_complete_cohort_counts": pair_complete_counts,
        "candidate_pair_incomplete_cohort_counts": pair_incomplete_counts,
        "pairing_policy": "candidate_and_control_complete;other_challengers_not_required",
        "master_nav_policy": {
            "offset_count": OFFSET_COUNT,
            "initial_wealth_each": 1,
            "aggregation": "mean_of_offset_wealth",
            "timeline": "union_of_sealed_daily_paths",
            "unstarted_offset_wealth": 1,
            "same_date_rule": "update_all_offsets_then_measure_mean",
            "shared_offset_boundary_rule": "new_post_rebalance_point_replaces_old_endpoint",
        },
        "significance_policy": {
            "test": "one_sided_positive_mean_of_candidate_minus_control_master_daily_returns",
            "variance": "newey_west_hac",
            "lag": HAC_LAG,
            "kernel": "bartlett",
            "reference_distribution": "standard_normal",
            "multiplicity": "holm_bonferroni_over_registered_challengers",
        },
        "evidence_quality": normalized_quality,
        "violations": violations,
        "candidate_reports": candidate_reports,
        "conclusion": (
            "eligible_for_major_review"
            if any_eligible
            else ("retire" if all_retired else "continue")
        ),
        "automatic_promotion_allowed": False,
        "required_transition_release": "6.0",
    }
    payload["evaluation_sha256"] = _sha256(payload)
    return payload


__all__ = [
    "CONTROL_ID",
    "HAC_LAG",
    "OFFSET_COUNT",
    "ShadowEvaluationError",
    "evaluate_shadow_outcomes",
]

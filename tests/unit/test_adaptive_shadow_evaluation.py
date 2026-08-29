from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pytest
from scipy import stats

from factor_lab.adaptive_shadow_evaluation import (
    ShadowEvaluationError,
    evaluate_shadow_outcomes,
)


ROOT = Path(__file__).resolve().parents[2]
SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64
AUDIT_SHA = "d" * 64


def _protocol() -> dict:
    return json.loads(
        (ROOT / "protocols/5.9-adaptive-shadow.json").read_text(encoding="utf-8")
    )


def _candidate_ids() -> tuple[str, str]:
    values = tuple(_protocol()["registry"]["ordered_candidates"])
    assert len(values) == 2
    return values  # type: ignore[return-value]


def _evaluate(
    protocol: dict,
    rows: list[dict],
    *,
    cutoff_date: str = "2026-08-30",
    evaluation_date: str = "2026-09-01",
    prior_monthly_states: dict[str, dict[str, bool]] | None = None,
    pit_violation_count: int = 0,
    integrity_violation_count: int = 0,
    deep_replay_valid: bool = True,
    candidate_quality: dict[str, dict[str, Any]] | None = None,
) -> dict:
    candidates = _candidate_ids()
    if prior_monthly_states is None:
        prior_monthly_states = {candidate: {} for candidate in candidates}
    if candidate_quality is None:
        candidate_quality = _zero_candidate_quality()
    return evaluate_shadow_outcomes(
        protocol,
        rows,
        cutoff_date=cutoff_date,
        evaluation_date=evaluation_date,
        evidence_quality={
            "pit_violation_count": pit_violation_count,
            "integrity_violation_count": integrity_violation_count,
            "deep_replay_valid": deep_replay_valid,
            "candidate_quality": candidate_quality,
            "audit_sha256": AUDIT_SHA,
        },
        prior_monthly_states=prior_monthly_states,
    )


def _zero_candidate_quality() -> dict[str, dict[str, Any]]:
    return {
        candidate: {
            "missed_deadline_count": 0,
            "missed_record_count": 0,
            "terminated_offset_count": 0,
            "terminated_offsets": [],
            "missed_record_sha256s": [],
        }
        for candidate in _candidate_ids()
    }


def _passing_prior_months() -> dict[str, dict[str, bool]]:
    return {
        candidate: {"2026-06": True, "2026-07": True}
        for candidate in _candidate_ids()
    }


def _rechain_nav_paths(rows: list[dict]) -> None:
    grouped: dict[tuple[str, int], list[dict]] = defaultdict(list)
    for row in rows:
        grouped[(row["candidate_id"], row["offset"])].append(row)
    for group in grouped.values():
        opening_nav = 1_000_000
        for row in sorted(group, key=lambda value: value["signal_date"]):
            ending_nav = int(
                round(
                    opening_nav
                    * (1.0 + int(row["net_return_ppm"]) / 1_000_000.0)
                )
            )
            row["opening_nav_fen"] = opening_nav
            row["ending_nav_fen"] = ending_nav
            for point in row["daily_path"]:
                point["account_nav_fen"] = opening_nav
            row["daily_path"][-1]["account_nav_fen"] = ending_nav
            opening_nav = ending_nav


def _rows(
    cycles_per_offset: int,
    *,
    first_return: int = 1000,
    second_return: int = 1200,
    control_return: int = 0,
) -> list[dict]:
    rows: list[dict] = []
    first, second = _candidate_ids()
    for offset in range(10):
        for cycle in range(cycles_per_offset):
            signal_date = date(2025, 1, 1) + timedelta(days=cycle * 10)
            signal = signal_date.isoformat()
            end = (signal_date + timedelta(days=11)).isoformat()
            formal_decision_sha = hashlib.sha256(
                f"{signal}|{end}|{offset}".encode()
            ).hexdigest()
            for candidate, value, digest in (
                ("formal_fixed_core_full", control_return, SHA_A),
                (first, first_return, SHA_B),
                (second, second_return, SHA_C),
            ):
                opening_nav = 1_000_000
                ending_nav = opening_nav + value
                rows.append(
                    {
                        "candidate_id": candidate,
                        "signal_date": signal,
                        "end_date": end,
                        "offset": offset,
                        "net_return_ppm": value,
                        "opening_nav_fen": opening_nav,
                        "ending_nav_fen": ending_nav,
                        "blocked_order_count": 0,
                        "daily_path": [
                            {
                                "date": (
                                    signal_date + timedelta(days=path_index)
                                ).isoformat(),
                                "account_nav_fen": (
                                    ending_nav if path_index == 11 else opening_nav
                                ),
                                "benchmark_index_ppb": 1_000_000_000,
                            }
                            for path_index in range(1, 12)
                        ],
                        "plan_targets_sha256": digest,
                        "formal_decision_record_sha256": formal_decision_sha,
                    }
                )
    _rechain_nav_paths(rows)
    return rows


def _set_return(row: dict, value: int) -> None:
    row["net_return_ppm"] = value


def _daily_master_series(
    rows: list[dict], candidate_id: str
) -> tuple[list[str], list[float], list[float]]:
    by_offset: dict[int, list[dict]] = defaultdict(list)
    for row in rows:
        if row["candidate_id"] == candidate_id:
            by_offset[int(row["offset"])].append(row)
    updates: dict[str, dict[int, float]] = defaultdict(dict)
    for offset in range(10):
        sleeve_wealth = 1.0
        for row in sorted(
            by_offset[offset],
            key=lambda value: (
                value["daily_path"][0]["date"],
                value["end_date"],
                value["offset"],
            ),
        ):
            opening_nav = int(row["opening_nav_fen"])
            for point in row["daily_path"]:
                updates[point["date"]][offset] = (
                    sleeve_wealth
                    * int(point["account_nav_fen"])
                    / opening_nav
                )
            sleeve_wealth *= int(row["ending_nav_fen"]) / opening_nav
    wealth = np.ones(10, dtype=float)
    dates: list[str] = []
    master_navs: list[float] = []
    daily_returns_ppm: list[float] = []
    previous = 1.0
    for observation_date in sorted(updates):
        for offset, value in updates[observation_date].items():
            wealth[offset] = value
        current = float(np.mean(wealth))
        dates.append(observation_date)
        master_navs.append(current)
        daily_returns_ppm.append((current / previous - 1.0) * 1_000_000.0)
        previous = current
    return dates, master_navs, daily_returns_ppm


def _monthly_states(report: dict) -> dict[str, dict[str, bool]]:
    return {
        row["candidate_id"]: row["monthly_states_after_current"]
        for row in report["candidate_reports"]
    }


def test_insufficient_common_prospective_evidence_only_continues() -> None:
    report = _evaluate(
        _protocol(),
        _rows(2),
        cutoff_date="2026-08-30",
    )
    assert report["complete_common_cohort_count"] == 20
    assert report["conclusion"] == "continue"
    assert report["automatic_promotion_allowed"] is False
    assert report["required_transition_release"] == "6.0"


def test_future_outcomes_are_excluded_not_used() -> None:
    rows = _rows(1)
    future = dict(rows[0])
    future.update(
        {
            "signal_date": "2026-09-01",
            "end_date": "2026-09-12",
            "net_return_ppm": 1000,
            "opening_nav_fen": 1_000_000,
            "ending_nav_fen": 1_001_000,
            "daily_path": [
                {
                    "date": (date(2026, 9, 1) + timedelta(days=index)).isoformat(),
                    "account_nav_fen": 1_001_000 if index == 11 else 1_000_000,
                    "benchmark_index_ppb": 1_000_000_000,
                }
                for index in range(1, 12)
            ],
        }
    )
    rows.append(future)
    report = _evaluate(
        _protocol(), rows, cutoff_date="2026-08-30"
    )
    assert report["excluded_unmatured_outcome_count"] == 1
    assert report["complete_common_cohort_count"] == 10


def test_missing_one_expert_excludes_the_whole_common_cohort() -> None:
    first, second = _candidate_ids()
    rows = _rows(1)
    rows.pop()
    report = _evaluate(
        _protocol(), rows, cutoff_date="2026-08-30"
    )
    assert report["complete_common_cohort_count"] == 9
    assert report["incomplete_cohort_count"] == 1
    assert report["candidate_pair_complete_cohort_counts"] == {
        first: 10,
        second: 9,
    }
    reports = {row["candidate_id"]: row for row in report["candidate_reports"]}
    assert reports[first]["common_cycle_count"] == 10
    assert reports[second]["common_cycle_count"] == 9
    assert reports[first]["gates"]["zero_pair_incomplete_cohorts"] is True
    assert reports[second]["gates"]["zero_pair_incomplete_cohorts"] is False


def test_identical_challenger_targets_cannot_be_major_review_eligible() -> None:
    _first, second = _candidate_ids()
    rows = _rows(25)
    for row in rows:
        if row["candidate_id"] == second:
            row["plan_targets_sha256"] = SHA_B
    report = _evaluate(
        _protocol(),
        rows,
        cutoff_date="2026-08-30",
        prior_monthly_states=_passing_prior_months(),
    )
    assert report["conclusion"] == "continue"
    assert all(
        not row["gates"]["not_homogeneous_with_another_challenger"]
        for row in report["candidate_reports"]
    )


def test_all_preregistered_gates_only_make_candidate_eligible_for_review() -> None:
    report = _evaluate(
        _protocol(),
        _rows(25),
        cutoff_date="2026-08-30",
        prior_monthly_states=_passing_prior_months(),
    )
    assert report["conclusion"] == "eligible_for_major_review"
    assert {
        row["conclusion"] for row in report["candidate_reports"]
    } == {"eligible_for_major_review"}
    assert report["automatic_promotion_allowed"] is False


def test_one_offset_winner_and_violation_fail_closed() -> None:
    rows = _rows(25, first_return=-100, second_return=-100)
    for row in rows:
        if row["offset"] == 0 and row["candidate_id"] != "formal_fixed_core_full":
            _set_return(row, 10000)
    _rechain_nav_paths(rows)
    report = _evaluate(
        _protocol(),
        rows,
        cutoff_date="2026-08-30",
        pit_violation_count=1,
    )
    assert report["conclusion"] == "continue"
    assert all(row["positive_offset_count"] == 1 for row in report["candidate_reports"])
    assert all(not row["gates"]["zero_pit_violations"] for row in report["candidate_reports"])


def test_master_nav_uses_union_daily_paths_and_batches_same_date_offsets() -> None:
    first, _second = _candidate_ids()
    rows = _rows(1, first_return=100_000, second_return=0)
    report = _evaluate(
        _protocol(),
        rows,
    )
    metrics = {
        row["candidate_id"]: row for row in report["candidate_reports"]
    }[first]
    dates, master_navs, _daily_returns = _daily_master_series(rows, first)

    assert metrics["candidate_compound_net_return_ppm"] == 100_000
    assert metrics["candidate_compound_net_return_ppm"] != round(
        ((1.1**10) - 1.0) * 1_000_000
    )
    assert len(dates) == 11
    assert master_navs[-1] == pytest.approx(1.1)
    assert metrics["candidate_master_nav_event_count"] == len(dates)
    assert metrics["candidate_master_nav_daily_observation_count"] == len(dates)
    assert float.fromhex(metrics["candidate_terminal_master_nav_hex"]) == pytest.approx(
        master_navs[-1]
    )
    assert report["master_nav_policy"] == {
        "offset_count": 10,
        "initial_wealth_each": 1,
        "aggregation": "mean_of_offset_wealth",
        "timeline": "union_of_sealed_daily_paths",
        "unstarted_offset_wealth": 1,
        "same_date_rule": "update_all_offsets_then_measure_mean",
        "shared_offset_boundary_rule": (
            "new_post_rebalance_point_replaces_old_endpoint"
        ),
    }


def test_intracycle_deep_drawdown_survives_endpoint_recovery() -> None:
    first, _second = _candidate_ids()
    rows = _rows(1, first_return=0, second_return=0)
    for row in rows:
        if row["candidate_id"] == first:
            row["daily_path"][5]["account_nav_fen"] = 500_000
    forward = _evaluate(_protocol(), rows)
    reverse = _evaluate(_protocol(), list(reversed(rows)))
    forward_metrics = {
        row["candidate_id"]: row for row in forward["candidate_reports"]
    }[first]
    reverse_metrics = {
        row["candidate_id"]: row for row in reverse["candidate_reports"]
    }[first]

    assert forward_metrics["candidate_compound_net_return_ppm"] == 0
    assert forward_metrics["candidate_master_max_drawdown_ppm"] == -500_000
    assert (
        forward_metrics["candidate_master_nav_trace_sha256"]
        == reverse_metrics["candidate_master_nav_trace_sha256"]
    )


def test_hac_uses_paired_union_daily_master_returns_with_frozen_lag_ten() -> None:
    first, _second = _candidate_ids()
    rows = _rows(2, first_return=0, second_return=0)
    candidate_rows = sorted(
        (row for row in rows if row["candidate_id"] == first),
        key=lambda row: (row["end_date"], row["signal_date"], row["offset"]),
    )
    cycle_values = [20_000] * 10 + [-10_000] * 10
    for row, value in zip(candidate_rows, cycle_values, strict=True):
        _set_return(row, value)
    _rechain_nav_paths(rows)

    candidate_dates, _candidate_navs, candidate_returns = _daily_master_series(
        rows, first
    )
    control_dates, _control_navs, control_returns = _daily_master_series(
        rows, "formal_fixed_core_full"
    )
    assert candidate_dates == control_dates
    assert len(candidate_dates) == 21
    values = np.asarray(candidate_returns, dtype=float) - np.asarray(
        control_returns, dtype=float
    )
    residual = values - values.mean()
    count = len(values)
    long_run_variance = float(np.dot(residual, residual) / count)
    for distance in range(1, min(10, count - 1) + 1):
        covariance = float(np.dot(residual[distance:], residual[:-distance]) / count)
        long_run_variance += 2.0 * (1.0 - distance / 11.0) * covariance
    standard_error = np.sqrt(max(0.0, long_run_variance) / count)
    expected = int(
        round(float(stats.norm.sf(float(values.mean() / standard_error))) * 1_000_000)
    )

    report = _evaluate(_protocol(), rows)
    metrics = {
        row["candidate_id"]: row for row in report["candidate_reports"]
    }[first]
    assert metrics["raw_one_sided_hac_pvalue_ppm"] == expected
    assert metrics["newey_west_hac"]["lag"] == 10
    assert metrics["newey_west_hac"]["observation_count"] == len(candidate_dates)
    assert metrics["mean_active_return_ppm"] == round(float(values.mean()))
    assert report["significance_policy"]["lag"] == 10
    assert report["significance_policy"]["test"] == (
        "one_sided_positive_mean_of_candidate_minus_control_master_daily_returns"
    )


def test_blocked_orders_are_candidate_specific_and_fail_the_gate() -> None:
    first, second = _candidate_ids()
    rows = _rows(1)
    next(
        row for row in rows if row["candidate_id"] == first
    )["blocked_order_count"] = 2
    report = _evaluate(_protocol(), rows)
    metrics = {row["candidate_id"]: row for row in report["candidate_reports"]}

    assert metrics[first]["candidate_blocked_order_count"] == 2
    assert metrics[first]["gates"]["zero_blocked_orders"] is False
    assert metrics[second]["candidate_blocked_order_count"] == 0
    assert metrics[second]["gates"]["zero_blocked_orders"] is True


def test_later_same_month_failure_overwrites_earlier_pass() -> None:
    early = _evaluate(
        _protocol(),
        _rows(25),
        cutoff_date="2026-08-10",
        evaluation_date="2026-08-15",
        prior_monthly_states=_passing_prior_months(),
    )
    assert all(
        row["major_gate_pass_now"] is True
        and row["monthly_states_after_current"]["2026-08"] is True
        for row in early["candidate_reports"]
    )

    late = _evaluate(
        _protocol(),
        _rows(25),
        cutoff_date="2026-08-20",
        evaluation_date="2026-08-25",
        prior_monthly_states=_monthly_states(early),
        pit_violation_count=1,
    )
    assert all(
        row["major_gate_pass_now"] is False
        and row["monthly_states_after_current"]["2026-08"] is False
        for row in late["candidate_reports"]
    )


def test_current_unclosed_month_does_not_count_toward_streak() -> None:
    report = _evaluate(
        _protocol(),
        _rows(25),
        cutoff_date="2026-08-30",
        evaluation_date="2026-08-31",
        prior_monthly_states=_passing_prior_months(),
    )
    for row in report["candidate_reports"]:
        assert row["major_gate_pass_now"] is True
        assert row["monthly_states_after_current"]["2026-08"] is True
        assert row["last_closed_month"] == "2026-07"
        assert row["consecutive_monthly_pass_count"] == 2
        assert row["gates"]["consecutive_monthly_passes"] is False
        assert row["conclusion"] == "continue"


def test_major_review_requires_three_consecutive_closed_months() -> None:
    gap_prior = {
        candidate: {
            "2026-05": True,
            "2026-06": False,
            "2026-07": True,
        }
        for candidate in _candidate_ids()
    }
    gap = _evaluate(
        _protocol(),
        _rows(25),
        cutoff_date="2026-08-31",
        evaluation_date="2026-09-01",
        prior_monthly_states=gap_prior,
    )
    assert all(
        row["consecutive_monthly_pass_count"] == 2
        and row["gates"]["consecutive_monthly_passes"] is False
        and row["conclusion"] == "continue"
        for row in gap["candidate_reports"]
    )

    continuous = _evaluate(
        _protocol(),
        _rows(25),
        cutoff_date="2026-08-31",
        evaluation_date="2026-09-01",
        prior_monthly_states=_passing_prior_months(),
    )
    assert all(
        row["last_closed_month"] == "2026-08"
        and row["consecutive_monthly_pass_count"] == 3
        and row["gates"]["consecutive_monthly_passes"] is True
        and row["conclusion"] == "eligible_for_major_review"
        for row in continuous["candidate_reports"]
    )


@pytest.mark.parametrize("fatal_candidate_index", [0, 1])
def test_missed_and_terminated_quality_permanently_retires_only_that_candidate(
    fatal_candidate_index: int,
) -> None:
    candidates = _candidate_ids()
    fatal_candidate = candidates[fatal_candidate_index]
    unaffected_candidate = candidates[1 - fatal_candidate_index]
    quality = _zero_candidate_quality()
    quality[fatal_candidate] = {
        "missed_deadline_count": 1,
        "missed_record_count": 1,
        "terminated_offset_count": 1,
        "terminated_offsets": [3],
        "missed_record_sha256s": ["e" * 64],
    }
    first = _evaluate(
        _protocol(),
        _rows(25),
        cutoff_date="2026-08-31",
        evaluation_date="2026-09-01",
        prior_monthly_states=_passing_prior_months(),
        candidate_quality=quality,
    )
    first_reports = {
        row["candidate_id"]: row for row in first["candidate_reports"]
    }
    assert first_reports[fatal_candidate]["gates"]["zero_missed_deadlines"] is False
    assert first_reports[fatal_candidate]["gates"]["zero_terminated_offsets"] is False
    assert first_reports[fatal_candidate]["conclusion"] == "retire"
    assert first_reports[unaffected_candidate]["gates"]["zero_missed_deadlines"] is True
    assert first_reports[unaffected_candidate]["gates"]["zero_terminated_offsets"] is True
    assert (
        first_reports[unaffected_candidate]["conclusion"]
        == "eligible_for_major_review"
    )

    later = _evaluate(
        _protocol(),
        _rows(25),
        cutoff_date="2026-09-30",
        evaluation_date="2026-10-01",
        prior_monthly_states=_monthly_states(first),
        candidate_quality=quality,
    )
    later_reports = {
        row["candidate_id"]: row for row in later["candidate_reports"]
    }
    assert later_reports[fatal_candidate]["gates"]["zero_missed_deadlines"] is False
    assert later_reports[fatal_candidate]["gates"]["zero_terminated_offsets"] is False
    assert later_reports[fatal_candidate]["conclusion"] == "retire"
    assert later_reports[unaffected_candidate]["conclusion"] == (
        "eligible_for_major_review"
    )


def test_evidence_quality_is_required_and_cannot_default_to_zero() -> None:
    with pytest.raises(TypeError, match="evidence_quality"):
        evaluate_shadow_outcomes(
            _protocol(),
            _rows(1),
            cutoff_date="2026-08-30",
            evaluation_date="2026-09-01",
            prior_monthly_states={candidate: {} for candidate in _candidate_ids()},
        )
    with pytest.raises(ShadowEvaluationError, match="pit_violation_count"):
        evaluate_shadow_outcomes(
            _protocol(),
            _rows(1),
            cutoff_date="2026-08-30",
            evaluation_date="2026-09-01",
            evidence_quality={},
            prior_monthly_states={candidate: {} for candidate in _candidate_ids()},
        )


@pytest.mark.parametrize("tamper", ["net", "ending", "daily_path"])
def test_cycle_nav_and_daily_path_must_reconcile(tamper: str) -> None:
    rows = _rows(1)
    row = rows[0]
    if tamper == "net":
        row["net_return_ppm"] += 100
    elif tamper == "ending":
        row["ending_nav_fen"] += 100
    else:
        row["daily_path"][-1]["account_nav_fen"] += 100
    with pytest.raises(ShadowEvaluationError, match="reconcile|daily_path"):
        _evaluate(_protocol(), rows)


def test_duplicate_or_unregistered_outcomes_are_rejected() -> None:
    rows = _rows(1)
    with pytest.raises(ShadowEvaluationError, match="duplicate"):
        _evaluate(
            _protocol(), [*rows, dict(rows[0])], cutoff_date="2026-08-30"
        )
    rows[0]["candidate_id"] = "post_hoc_candidate"
    with pytest.raises(ShadowEvaluationError, match="unregistered"):
        _evaluate(_protocol(), rows, cutoff_date="2026-08-30")


def test_protocol_cannot_enable_auto_promotion() -> None:
    protocol = _protocol()
    protocol["evaluation"]["automatic_promotion_allowed"] = True
    with pytest.raises(ShadowEvaluationError, match="auto-promote"):
        _evaluate(protocol, [], cutoff_date="2026-08-30")

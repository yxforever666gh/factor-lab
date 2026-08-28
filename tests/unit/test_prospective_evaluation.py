from __future__ import annotations

from copy import deepcopy
from datetime import date, timedelta
import hashlib
import json
from pathlib import Path
import random
from typing import Any, Callable

import pytest

from factor_lab.prospective_evaluation import (
    EVALUATION_CONTRACT_CANONICAL_JSON,
    EVALUATION_CONTRACT_SHA256,
    EVALUATOR_ID,
    ProspectiveEvaluationError,
    canonical_evaluation_json_bytes,
    evaluate_prospective_outcomes,
)
from factor_lab.prospective_execution import (
    CycleOutcome,
    FROZEN_EXECUTION_CONTRACT,
    SleeveAccountState,
)


DEPLOYMENT_SHA = "d" * 64
ROOT = Path(__file__).resolve().parents[2]


def test_release_manifest_binds_exact_evaluator_contract() -> None:
    manifest = json.loads(
        (ROOT / "protocols/5.2-target-generator.json").read_text(encoding="utf-8")
    )
    contract = manifest["evaluation_contract"]
    assert contract["evaluator_id"] == EVALUATOR_ID
    assert contract["contract_sha256"] == EVALUATION_CONTRACT_SHA256


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _outer(
    outcome: CycleOutcome,
    *,
    label: str,
) -> dict[str, Any]:
    return {
        "schema_version": 2,
        "decision_record_sha256": _sha(f"decision:{label}"),
        "attestation_receipt_record_sha256": _sha(f"receipt:{label}"),
        "execution_snapshot_sha256": outcome.execution_snapshot_sha256,
        "cycle_outcome_sha256": outcome.outcome_sha256,
        "cycle_outcome": outcome.to_dict(),
    }


def _evidence(
    cycles_per_offset: int,
    *,
    net_return: Callable[[int, int], int],
    benchmark_return: Callable[[int, int], int] | None = None,
    broken_previous: tuple[int, int] | None = None,
    daily_nav: Callable[[int, int, int, int, int], int] | None = None,
) -> list[dict[str, Any]]:
    benchmark = benchmark_return or (lambda _offset, _cycle: 0)
    output: list[dict[str, Any]] = []
    base = date(2030, 1, 2)
    for offset in range(10):
        state = SleeveAccountState.genesis(
            deployment_sha256=DEPLOYMENT_SHA,
            offset=offset,
        )
        for cycle_index in range(cycles_per_offset):
            label = f"o{offset}:c{cycle_index}"
            holding_start = base + timedelta(days=offset + cycle_index * 10)
            signal = holding_start - timedelta(days=1)
            holding_end = holding_start + timedelta(days=10)
            net_ppb = net_return(offset, cycle_index)
            benchmark_ppb = benchmark(offset, cycle_index)
            ending_nav = int(round(state.nav_fen * (1.0 + net_ppb / 1_000_000_000)))
            generation_sha = _sha(f"generation:{label}")
            execution_sha = _sha(f"execution:{label}")
            next_state = SleeveAccountState(
                deployment_sha256=DEPLOYMENT_SHA,
                offset=offset,
                cycle_count=cycle_index + 1,
                cash_hex=(ending_nav / 100.0).hex(),
                positions=(),
                nav_fen=ending_nav,
                last_holding_end_date=holding_end.isoformat(),
                last_generation_result_sha256=generation_sha,
                last_execution_snapshot_sha256=execution_sha,
            )
            path = []
            for day in range(11):
                account_nav = (
                    daily_nav(offset, cycle_index, day, state.nav_fen, ending_nav)
                    if daily_nav is not None
                    else (ending_nav if day == 10 else state.nav_fen)
                )
                benchmark_index = (
                    1_000_000_000 + benchmark_ppb if day == 10 else 1_000_000_000
                )
                path.append(
                    {
                        "date": (holding_start + timedelta(days=day)).isoformat(),
                        "account_nav_fen": account_nav,
                        "benchmark_index_ppb": benchmark_index,
                    }
                )
            outcome = CycleOutcome(
                contract_sha256=FROZEN_EXECUTION_CONTRACT.contract_sha256,
                deployment_sha256=DEPLOYMENT_SHA,
                generation_result_sha256=generation_sha,
                execution_snapshot_sha256=execution_sha,
                previous_account_state_sha256=(
                    "f" * 64
                    if broken_previous == (offset, cycle_index)
                    else state.state_sha256
                ),
                offset=offset,
                signal_date=signal.isoformat(),
                holding_start_date=holding_start.isoformat(),
                holding_end_date=holding_end.isoformat(),
                observation_available_at_utc=f"{holding_end.isoformat()}T12:00:00Z",
                execution_status="complete",
                not_executed_reason=None,
                opening_nav_fen=state.nav_fen,
                pretrade_nav_fen=state.nav_fen,
                ending_nav_fen=ending_nav,
                gross_return_ppb=net_ppb,
                net_return_ppb=net_ppb,
                benchmark_return_ppb=benchmark_ppb,
                turnover_ppm=100_000,
                fees_fen=0,
                executed_order_count=1,
                blocked_order_count=0,
                benchmark_expected_count=10,
                benchmark_complete_count=10,
                daily_path=path,
                next_account_state=next_state,
            )
            output.append(_outer(outcome, label=label))
            state = next_state
    return output


def _contains_float(value: Any) -> bool:
    if isinstance(value, float):
        return True
    if isinstance(value, dict):
        return any(
            _contains_float(key) or _contains_float(child)
            for key, child in value.items()
        )
    if isinstance(value, (list, tuple)):
        return any(_contains_float(child) for child in value)
    return False


def test_contract_hash_is_stable_and_json_has_no_float() -> None:
    assert EVALUATION_CONTRACT_SHA256 == (
        "b3aff959751ae317f5783ec0e21fe98b03a2f047e8ec134053252feee8cb3a0c"
    )
    assert hashlib.sha256(EVALUATION_CONTRACT_CANONICAL_JSON).hexdigest() == (
        EVALUATION_CONTRACT_SHA256
    )
    payload = json.loads(EVALUATION_CONTRACT_CANONICAL_JSON)
    assert payload["evidence_schedule"]["one_year_directional_gate"]["cohort"] == (
        "first_25_per_offset"
    )
    assert not _contains_float(payload)


def test_empty_outcomes_return_hashed_accumulating_zero_status() -> None:
    result = evaluate_prospective_outcomes([])

    assert result["stage"] == "collecting"
    assert result["status"] == "accumulating"
    assert result["input"]["outcome_count"] == 0
    assert [row["outcome_count"] for row in result["input"]["offset_counts"]] == [
        0
    ] * 10
    assert result["engineering_closure_complete"] is False
    assert result["direction_gate_passed"] is False
    assert result["performance"] is None
    assert canonical_evaluation_json_bytes(result)


def test_ten_outcomes_are_engineering_closure_only_and_order_invariant() -> None:
    outcomes = _evidence(
        1,
        net_return=lambda _offset, _cycle: 500_000_000,
        benchmark_return=lambda _offset, _cycle: -500_000_000,
    )
    shuffled = list(outcomes)
    random.Random(17).shuffle(shuffled)

    first = evaluate_prospective_outcomes(outcomes)
    second = evaluate_prospective_outcomes(shuffled)

    assert first == second
    assert first["stage"] == "engineering_closure"
    assert first["status"] == "engineering_closure_only"
    assert first["engineering_closure_complete"] is True
    assert first["performance"] is None
    assert first["direction_gate_passed"] is False
    assert first["performance_promotion_claim_allowed"] is False
    assert canonical_evaluation_json_bytes(first)
    assert not _contains_float(first)


def test_sixty_outcomes_can_only_reject() -> None:
    outcomes = _evidence(
        6,
        net_return=lambda offset, cycle: (
            (10_000_000 if cycle % 2 == 0 else 12_000_000)
            if offset < 2
            else (-10_000_000 if cycle % 2 == 0 else -8_000_000)
        ),
    )

    result = evaluate_prospective_outcomes(outcomes)

    assert result["stage"] == "early_stop"
    assert result["status"] == "reject_major_direction"
    assert result["reject_major_direction"] is True
    assert result["performance_promotion_claim_allowed"] is False
    assert result["direction_gate_passed"] is False
    assert result["early_stop_checks"] == {
        "positive_net_compound_offset_count": 2,
        "positive_active_compound_offset_count": 2,
        "positive_net_lte_2": True,
        "positive_active_lte_2": True,
        "reject_if_both": True,
        "promotion_allowed": False,
    }
    offset_zero = result["performance"]["offsets"][0]
    assert offset_zero["not_executed_count"] == 0
    assert offset_zero["complete_count"] == 6
    assert float.fromhex(offset_zero["net_compound_return_hex"]) > 0.0


def test_any_complete_cycle_ending_at_zero_is_immediate_terminal_rejection() -> None:
    evidence = _evidence(
        1,
        net_return=lambda offset, _cycle: (
            -1_000_000_000 if offset == 0 else 10_000_000
        ),
    )

    result = evaluate_prospective_outcomes(evidence)

    assert result["input"]["outcome_count"] == 10
    assert result["stage"] == "terminal_insolvency"
    assert result["status"] == "reject_major_direction"
    assert result["reject_major_direction"] is True
    assert result["direction_gate_passed"] is False
    assert result["formal_gate_evaluated"] is False
    assert result["performance"] is None


def test_sixty_outcomes_with_three_positive_offsets_only_continue() -> None:
    outcomes = _evidence(
        6,
        net_return=lambda offset, cycle: (
            (9_000_000 + cycle * 100_000)
            if offset < 3
            else (-9_000_000 + cycle * 100_000)
        ),
    )

    result = evaluate_prospective_outcomes(outcomes)

    assert result["status"] == "continue_accumulating"
    assert result["reject_major_direction"] is False
    assert result["direction_gate_passed"] is False
    assert result["performance_promotion_claim_allowed"] is False


def test_formal_250_outcome_gate_passes_all_preregistered_checks() -> None:
    outcomes = _evidence(
        25,
        net_return=lambda _offset, cycle: (8_000_000 if cycle % 2 == 0 else 12_000_000),
        benchmark_return=lambda _offset, cycle: (
            2_000_000 if cycle % 2 == 0 else 3_000_000
        ),
    )

    result = evaluate_prospective_outcomes(outcomes)

    assert result["stage"] == "one_year_directional_gate"
    assert result["status"] == "one_year_directional_gate_passed"
    assert result["direction_gate_passed"] is True
    assert result["reject_major_direction"] is False
    assert result["performance_promotion_claim_allowed"] is False
    assert result["stable_profit_claim_allowed"] is False
    assert all(result["formal_gate_checks"].values())
    assert result["performance"]["cohort_outcome_count"] == 250
    assert result["performance"]["positive_net_compound_offset_count"] == 10
    assert result["performance"]["positive_active_compound_offset_count"] == 10
    assert (
        float.fromhex(result["performance"]["cross_offset"]["net_cagr"]["q20_hex"])
        > 0.0
    )
    offset_zero = result["performance"]["offsets"][0]
    assert float.fromhex(offset_zero["active_cagr_hex"]) == (
        float.fromhex(offset_zero["net_cagr_hex"])
        - float.fromhex(offset_zero["benchmark_cagr_hex"])
    )
    assert not _contains_float(result)


def test_formal_gate_accepts_exact_minus_25_percent_drawdown_boundary() -> None:
    def net_return(_offset: int, cycle: int) -> int:
        if cycle == 0:
            return 500_000_000
        if cycle == 1:
            return -250_000_000
        return 19_000_000 if cycle % 2 == 0 else 21_000_000

    result = evaluate_prospective_outcomes(_evidence(25, net_return=net_return))

    assert result["status"] == "one_year_directional_gate_passed"
    assert result["formal_gate_checks"]["worst_max_drawdown_gte_minus_25pct"] is True
    assert (
        float.fromhex(
            result["performance"]["cross_offset"]["holding_session_max_drawdown"][
                "worst_hex"
            ]
        )
        == -0.25
    )


def test_formal_gate_rejects_cycle_endpoint_drawdown_below_25_percent() -> None:
    def net_return(_offset: int, cycle: int) -> int:
        if cycle == 0:
            return 500_000_000
        if cycle == 1:
            return -300_000_000
        return 19_000_000 if cycle % 2 == 0 else 21_000_000

    result = evaluate_prospective_outcomes(_evidence(25, net_return=net_return))

    assert result["status"] == "one_year_directional_gate_failed"
    assert result["formal_gate_checks"]["worst_max_drawdown_gte_minus_25pct"] is False
    assert all(
        value
        for key, value in result["formal_gate_checks"].items()
        if key
        not in {
            "worst_max_drawdown_gte_minus_25pct",
            "master_daily_max_drawdown_gte_minus_25pct",
        }
    )
    assert result["reject_major_direction"] is True
    assert result["performance_promotion_claim_allowed"] is False


def test_formal_gate_constant_positive_return_has_zero_sharpe_and_fails() -> None:
    result = evaluate_prospective_outcomes(
        _evidence(25, net_return=lambda _offset, _cycle: 10_000_000)
    )

    assert result["status"] == "one_year_directional_gate_failed"
    assert result["formal_gate_checks"]["net_sharpe_q20_gt_zero"] is False
    assert (
        float.fromhex(result["performance"]["cross_offset"]["net_sharpe"]["q20_hex"])
        == 0.0
    )
    assert all(
        value
        for key, value in result["formal_gate_checks"].items()
        if key != "net_sharpe_q20_gt_zero"
    )


def test_formal_gate_net_cagr_q20_equal_zero_does_not_pass_strict_threshold() -> None:
    result = evaluate_prospective_outcomes(
        _evidence(
            25,
            net_return=lambda _offset, _cycle: 0,
            benchmark_return=lambda _offset, cycle: (
                -2_000_000 if cycle % 2 == 0 else -1_000_000
            ),
        )
    )

    assert result["stage"] == "one_year_directional_gate"
    assert result["formal_gate_checks"]["net_cagr_q20_gt_zero"] is False
    assert (
        float.fromhex(result["performance"]["cross_offset"]["net_cagr"]["q20_hex"])
        == 0.0
    )
    assert result["direction_gate_passed"] is False


def test_formal_gate_positive_offset_count_boundaries_are_inclusive() -> None:
    result = evaluate_prospective_outcomes(
        _evidence(
            25,
            net_return=lambda offset, cycle: (
                (9_000_000 if cycle % 2 == 0 else 11_000_000)
                if offset < 8
                else (-11_000_000 if cycle % 2 == 0 else -9_000_000)
            ),
            benchmark_return=lambda offset, cycle: (
                (19_000_000 if cycle % 2 == 0 else 21_000_000) if offset == 7 else 0
            ),
        )
    )

    assert result["performance"]["positive_net_compound_offset_count"] == 8
    assert result["performance"]["positive_active_compound_offset_count"] == 7
    assert result["formal_gate_checks"]["positive_net_offset_count_gte_8"] is True
    assert result["formal_gate_checks"]["positive_active_offset_count_gte_7"] is True


def test_formal_cohort_is_first_25_per_offset_not_optional_later_data() -> None:
    first = _evidence(
        25,
        net_return=lambda _offset, cycle: (7_000_000 if cycle % 2 == 0 else 11_000_000),
    )
    extended = _evidence(
        26,
        net_return=lambda _offset, cycle: (
            900_000_000
            if cycle == 25
            else (7_000_000 if cycle % 2 == 0 else 11_000_000)
        ),
    )

    first_result = evaluate_prospective_outcomes(first)
    extended_result = evaluate_prospective_outcomes(extended)

    assert first_result["performance"] == extended_result["performance"]
    assert first_result["evaluation_sha256"] != extended_result["evaluation_sha256"]
    assert extended_result["input"]["outcome_count"] == 260


def test_offset_checks_cannot_promote_a_losing_fifty_million_master_book() -> None:
    def net_return(offset: int, cycle: int) -> int:
        if offset == 0:
            return -250_000_000 if cycle == 0 else 0
        if offset == 1:
            return -400_000
        return 100_000 if cycle % 2 == 0 else 300_000

    result = evaluate_prospective_outcomes(_evidence(25, net_return=net_return))

    offset_only_checks = {
        "net_cagr_q20_gt_zero",
        "net_sharpe_q20_gt_zero",
        "active_cagr_q20_gt_zero",
        "positive_net_offset_count_gte_8",
        "positive_active_offset_count_gte_7",
        "worst_max_drawdown_gte_minus_25pct",
    }
    assert all(result["formal_gate_checks"][name] for name in offset_only_checks)
    master = result["performance"]["master_portfolio"]
    assert master["terminal_nav_fen"] < master["initial_nav_fen"]
    assert result["formal_gate_checks"]["master_terminal_wealth_gt_initial"] is False
    assert result["status"] == "one_year_directional_gate_failed"
    assert result["direction_gate_passed"] is False


def test_holding_session_crash_and_recovery_fails_daily_drawdown_gate() -> None:
    def intracycle_crash(
        offset: int,
        cycle: int,
        day: int,
        opening_nav: int,
        ending_nav: int,
    ) -> int:
        # All ten offset paths refer to the same absolute session when this
        # expression equals 65, despite having different cycle/day labels.
        if offset + cycle * 10 + day == 65 and day != 10:
            return opening_nav // 2
        return ending_nav if day == 10 else opening_nav

    result = evaluate_prospective_outcomes(
        _evidence(
            25,
            net_return=lambda _offset, cycle: (
                8_000_000 if cycle % 2 == 0 else 12_000_000
            ),
            daily_nav=intracycle_crash,
        )
    )

    assert result["formal_gate_checks"]["worst_max_drawdown_gte_minus_25pct"] is False
    assert (
        result["formal_gate_checks"]["master_daily_max_drawdown_gte_minus_25pct"]
        is False
    )
    assert (
        float.fromhex(
            result["performance"]["cross_offset"]["holding_session_max_drawdown"][
                "worst_hex"
            ]
        )
        <= -0.5
    )
    assert result["status"] == "one_year_directional_gate_failed"


def test_first_six_early_rejection_is_terminal_at_250_outcomes() -> None:
    outcomes = _evidence(
        25,
        net_return=lambda _offset, cycle: (-10_000_000 if cycle < 6 else 200_000_000),
    )

    result = evaluate_prospective_outcomes(outcomes)

    assert result["input"]["outcome_count"] == 250
    assert result["stage"] == "early_stop"
    assert result["status"] == "reject_major_direction"
    assert result["early_stop_evaluated"] is True
    assert result["formal_gate_evaluated"] is False
    assert result["cohort_cycle_count_per_offset"] == 6
    assert result["early_stop_checks"]["reject_if_both"] is True
    assert result["direction_gate_passed"] is False


def test_duplicate_incomplete_and_broken_accounting_evidence_fail_closed() -> None:
    outcomes = _evidence(1, net_return=lambda _offset, _cycle: 1_000_000)
    with pytest.raises(ProspectiveEvaluationError, match="duplicate"):
        evaluate_prospective_outcomes([*outcomes, outcomes[0]])

    incomplete = deepcopy(outcomes[0])
    del incomplete["cycle_outcome"]
    with pytest.raises(ProspectiveEvaluationError, match="exact rich schema-2"):
        evaluate_prospective_outcomes([incomplete])

    broken = _evidence(
        2,
        net_return=lambda _offset, _cycle: 1_000_000,
        broken_previous=(0, 1),
    )
    with pytest.raises(
        ProspectiveEvaluationError, match="state hash is not continuous"
    ):
        evaluate_prospective_outcomes(broken)


def test_embedded_outcome_must_be_exact_not_constructor_normalized() -> None:
    outcomes = _evidence(1, net_return=lambda _offset, _cycle: 1_000_000)
    wrong_type = deepcopy(outcomes[0])
    wrong_type["cycle_outcome"]["schema_version"] = True

    with pytest.raises(ProspectiveEvaluationError, match="schema_version"):
        evaluate_prospective_outcomes([wrong_type])

    noncomplete = deepcopy(outcomes[0])
    noncomplete["cycle_outcome"]["execution_status"] = "not_executed"
    noncomplete["cycle_outcome"]["not_executed_reason"] = "service unavailable"
    with pytest.raises(ProspectiveEvaluationError, match="CycleOutcome is invalid"):
        evaluate_prospective_outcomes([noncomplete])


def test_evaluation_self_hash_detects_mutation() -> None:
    result = evaluate_prospective_outcomes(
        _evidence(1, net_return=lambda _offset, _cycle: 1_000_000)
    )
    mutated = deepcopy(result)
    mutated["status"] = "forged"

    with pytest.raises(ProspectiveEvaluationError, match="does not match"):
        canonical_evaluation_json_bytes(mutated)

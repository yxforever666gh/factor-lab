from __future__ import annotations

from copy import deepcopy

import pandas as pd
import pytest

from factor_lab.portfolio.long_only import (
    LongOnlyPortfolioConfig,
    evaluate_long_only_portfolio,
)
from factor_lab.prospective_execution import (
    CycleOutcome,
    ExecutionContract,
    ExecutionSnapshot,
    FROZEN_EXECUTION_CONTRACT,
    ProspectiveExecutionError,
    SleeveAccountState,
    evaluate_due_sleeve_cycle,
)
from factor_lab.prospective_targets import (
    GenerationResult,
    SleeveState,
    TenSleeveState,
    calendar_prefix_sha256,
)


DEPLOYMENT_SHA = "a" * 64
TARGETS = tuple(f"{index:06d}.SZ" for index in range(1, 11))


def _generation(
    sessions: list[str],
    signal_index: int,
    *,
    result_input_sha: str = "e" * 64,
    action: str = "seed",
) -> GenerationResult:
    due_offset = signal_index % 10
    target_map = {ticker: 100_000 for ticker in TARGETS}
    sleeves = []
    for offset in range(10):
        if offset == due_offset:
            sleeves.append(
                SleeveState(
                    offset=offset,
                    initialized=True,
                    last_signal_date=sessions[signal_index],
                    last_calendar_index=signal_index,
                    targets_ppm=target_map,
                    cash_ppm=0,
                )
            )
        else:
            sleeves.append(SleeveState(offset=offset))
    state = TenSleeveState(
        deployment_sha256=DEPLOYMENT_SHA,
        activation_record_sha256="b" * 64,
        implementation_upgrade_record_sha256="c" * 64,
        last_processed_calendar_index=signal_index,
        last_processed_session=sessions[signal_index],
        sleeves=sleeves,
    )
    plans = []
    for sleeve in state.sleeves:
        plans.append(
            {
                "action": action if sleeve.offset == due_offset else "cash",
                **sleeve.to_dict(),
            }
        )
    return GenerationResult(
        deployment_sha256=DEPLOYMENT_SHA,
        input_snapshot_sha256=result_input_sha,
        previous_state_sha256="d" * 64,
        signal_date=sessions[signal_index],
        trade_date=sessions[signal_index + 1],
        calendar_index=signal_index,
        due_offset=due_offset,
        skipped_sessions=(),
        sleeve_plans=plans,
        aggregate_targets_ppm={ticker: 10_000 for ticker in TARGETS},
        aggregate_cash_ppm=900_000,
        next_state=state,
    )


def _observations(
    sessions: list[str],
    signal_index: int,
    *,
    start_price: float,
    end_price: float,
) -> list[dict]:
    window = sessions[signal_index + 1 : signal_index + 12]
    rows: list[dict] = []
    for day_index, day in enumerate(window):
        fraction = day_index / 10.0
        price = start_price + (end_price - start_price) * fraction
        for ticker in TARGETS:
            is_start = day_index == 0
            rows.append(
                {
                    "date": day,
                    "ticker": ticker,
                    "open_adj_hex": float(price).hex(),
                    "adv_20_asof_hex": (1_000_000_000.0).hex() if is_start else None,
                    "volatility_20_asof_hex": (0.0).hex() if is_start else None,
                    "execution_input_date": sessions[signal_index] if is_start else None,
                    "is_one_price_limit_up": False,
                    "is_one_price_limit_down": False,
                    "is_suspended": False,
                    "is_delisted": False,
                }
            )
    return rows


def _snapshot(
    generation: GenerationResult,
    sessions: list[str],
    signal_index: int,
    *,
    start_price: float = 100.0,
    end_price: float = 110.0,
    source_sha: str = "f" * 64,
) -> ExecutionSnapshot:
    signal = sessions[signal_index]
    start = sessions[signal_index + 1]
    end = sessions[signal_index + 11]
    return ExecutionSnapshot(
        generation_result_sha256=generation.result_sha256,
        execution_source_sha256=source_sha,
        official_calendar_sha256=calendar_prefix_sha256(sessions[: signal_index + 12]),
        signal_date=signal,
        holding_start_date=start,
        holding_end_date=end,
        calendar_sessions=sessions[: signal_index + 12],
        benchmark_tickers=TARGETS,
        rows=_observations(
            sessions,
            signal_index,
            start_price=start_price,
            end_price=end_price,
        ),
        calendar_available_at_utc=f"{signal}T08:00:00Z",
        decision_inputs_available_at_utc=f"{signal}T12:00:00Z",
        trade_deadline_utc=f"{start}T01:15:00Z",
        start_open_available_at_utc=f"{start}T01:30:00Z",
        end_open_available_at_utc=f"{end}T01:30:00Z",
        observation_available_at_utc=f"{end}T02:00:00Z",
    )


def _calendar(periods: int = 33) -> list[str]:
    return [value.date().isoformat() for value in pd.bdate_range("2026-09-01", periods=periods)]


def test_frozen_contract_and_all_envelopes_round_trip_by_self_hash() -> None:
    sessions = _calendar()
    generation = _generation(sessions, 11)
    snapshot = _snapshot(generation, sessions, 11)
    account = SleeveAccountState.genesis(
        deployment_sha256=DEPLOYMENT_SHA,
        offset=generation.due_offset,
    )
    outcome = evaluate_due_sleeve_cycle(
        generation_result=generation,
        execution_snapshot=snapshot,
        previous_account_state=account,
    )

    assert FROZEN_EXECUTION_CONTRACT.contract_sha256 == (
        "cce767248a9722739198e2dec9417f507726076bf6401e03f059c16345bce06f"
    )
    assert ExecutionContract.from_mapping(FROZEN_EXECUTION_CONTRACT.to_dict()).to_dict() == (
        FROZEN_EXECUTION_CONTRACT.to_dict()
    )
    assert ExecutionSnapshot.from_mapping(snapshot.to_dict()).to_dict() == snapshot.to_dict()
    assert SleeveAccountState.from_mapping(account.to_dict()).to_dict() == account.to_dict()
    assert CycleOutcome.from_mapping(outcome.to_dict()).to_dict() == outcome.to_dict()

    tampered = deepcopy(outcome.to_dict())
    tampered["ending_nav_fen"] += 1
    with pytest.raises(ProspectiveExecutionError):
        CycleOutcome.from_mapping(tampered)
    partial_benchmark = deepcopy(outcome.to_dict())
    partial_benchmark["benchmark_complete_count"] -= 1
    partial_benchmark["outcome_sha256"] = "0" * 64
    with pytest.raises(ProspectiveExecutionError, match="every frozen roster"):
        CycleOutcome.from_mapping(partial_benchmark)
    missing_hash = deepcopy(snapshot.to_dict())
    missing_hash["snapshot_sha256"] = ""
    with pytest.raises(ProspectiveExecutionError, match="snapshot_sha256"):
        ExecutionSnapshot.from_mapping(missing_hash)


def test_schema_versions_reject_boolean_alias_for_integer_one() -> None:
    contract = FROZEN_EXECUTION_CONTRACT.to_dict()
    contract.pop("contract_sha256")
    contract["schema_version"] = True
    with pytest.raises(ProspectiveExecutionError, match="schema_version"):
        ExecutionContract(**contract)

    account = SleeveAccountState.genesis(
        deployment_sha256=DEPLOYMENT_SHA,
        offset=0,
    ).to_dict()
    account.pop("state_sha256")
    account["schema_version"] = True
    with pytest.raises(ProspectiveExecutionError, match="version"):
        SleeveAccountState(**account)


def test_complete_cycle_reuses_frozen_costs_and_emits_exact_ledger_v2_shape() -> None:
    sessions = _calendar()
    generation = _generation(sessions, 11)
    snapshot = _snapshot(generation, sessions, 11)
    account = SleeveAccountState.genesis(
        deployment_sha256=DEPLOYMENT_SHA,
        offset=generation.due_offset,
    )

    outcome = evaluate_due_sleeve_cycle(
        generation_result=generation,
        execution_snapshot=snapshot,
        previous_account_state=account,
    )

    assert outcome.offset == generation.due_offset == 1
    assert outcome.opening_nav_fen == 500_000_000
    assert outcome.ending_nav_fen > outcome.opening_nav_fen
    assert outcome.gross_return_ppb > outcome.net_return_ppb > 0
    assert outcome.benchmark_return_ppb == 100_000_000
    assert outcome.turnover_ppm > 990_000
    assert outcome.fees_fen > 0
    assert outcome.executed_order_count == 10
    assert outcome.blocked_order_count == 0
    assert outcome.benchmark_complete_count == outcome.benchmark_expected_count == 10
    assert len(outcome.daily_path) == 11
    assert outcome.daily_path[0].date == outcome.holding_start_date
    assert outcome.daily_path[-1].date == outcome.holding_end_date
    assert outcome.daily_path[-1].account_nav_fen == outcome.ending_nav_fen
    assert outcome.daily_path[0].benchmark_index_ppb == 1_000_000_000
    assert outcome.daily_path[-1].benchmark_index_ppb == 1_100_000_000
    assert outcome.next_account_state.cycle_count == 1
    assert generation.next_state.sleeves[1].capital_fen == 500_000_000
    assert outcome.next_account_state.nav_fen != generation.next_state.sleeves[1].capital_fen

    ledger = outcome.to_ledger_v2_outcome(
        decision_record_sha256="1" * 64,
        attestation_receipt_record_sha256="2" * 64,
    )
    assert set(ledger) == {
        "schema_version",
        "decision_record_sha256",
        "attestation_receipt_record_sha256",
        "execution_snapshot_sha256",
        "cycle_outcome_sha256",
        "cycle_outcome",
    }
    assert ledger["schema_version"] == 2
    assert ledger["cycle_outcome_sha256"] == outcome.outcome_sha256
    assert ledger["cycle_outcome"] == outcome.to_dict()
    assert outcome.execution_snapshot_sha256 == snapshot.snapshot_sha256
    assert ledger["execution_snapshot_sha256"] == snapshot.snapshot_sha256


def test_complete_cycle_matches_the_frozen_historical_long_only_period() -> None:
    sessions = _calendar()
    signal_index = 11
    generation = _generation(sessions, signal_index)
    snapshot = _snapshot(generation, sessions, signal_index)
    account = SleeveAccountState.genesis(
        deployment_sha256=DEPLOYMENT_SHA,
        offset=1,
    )
    outcome = evaluate_due_sleeve_cycle(
        generation_result=generation,
        execution_snapshot=snapshot,
        previous_account_state=account,
    )

    observation_prices = {
        row.date: float.fromhex(row.open_adj_hex)
        for row in snapshot.rows
        if row.ticker == TARGETS[0] and row.open_adj_hex is not None
    }
    frame_rows = []
    for day in sessions[: signal_index + 12]:
        for ticker in TARGETS:
            frame_rows.append(
                {
                    "date": day,
                    "ticker": ticker,
                    "open_adj": observation_prices.get(day, 100.0),
                    "adv_20": 1_000_000_000.0,
                    "volatility_20": 0.0,
                    "eligible": True,
                    "universe_member": True,
                    "is_one_price_limit_up": False,
                    "is_one_price_limit_down": False,
                    "is_suspended": False,
                    "is_delisted": False,
                    "signal": 1.0,
                }
            )
    frame = pd.DataFrame(frame_rows)
    signal_date = sessions[signal_index]
    historical = evaluate_long_only_portfolio(
        frame,
        "signal",
        LongOnlyPortfolioConfig(
            capital=5_000_000.0,
            holding_days=10,
            rebalance_every_days=10,
            rebalance_offset_days=1,
            position_count=10,
            retention_buffer=5,
            target_weight=0.1,
            max_adv_participation=0.05,
            periods_per_year=25.2,
            open_column="open_adj",
            price_source="synthetic_adjusted_total_return",
            evaluation_start_date=signal_date,
        ),
        target_weights_by_date={signal_date: {ticker: 0.1 for ticker in TARGETS}},
        optimization_audit_by_date={signal_date: {"promotion_eligible": True}},
        require_optimized_targets=True,
    )

    assert historical.status == "ok"
    assert len(historical.periods) == 1
    period = historical.periods[0]
    assert outcome.ending_nav_fen == int(round(float(period["end_nav"]) * 100.0))
    assert outcome.net_return_ppb == int(round(float(period["net_return"]) * 1_000_000_000))
    assert outcome.gross_return_ppb == int(round(float(period["gross_return"]) * 1_000_000_000))
    assert outcome.benchmark_return_ppb == int(
        round(float(period["benchmark_return"]) * 1_000_000_000)
    )
    assert outcome.turnover_ppm == int(round(float(period["turnover"]) * 1_000_000))
    assert outcome.fees_fen == int(round(float(period["costs"]["total"]) * 100.0))


def test_same_offset_nav_is_continuous_but_other_offset_stays_independent() -> None:
    sessions = _calendar()
    first_generation = _generation(sessions, 11)
    first_snapshot = _snapshot(first_generation, sessions, 11)
    first_account = SleeveAccountState.genesis(
        deployment_sha256=DEPLOYMENT_SHA,
        offset=1,
    )
    other_account = SleeveAccountState.genesis(
        deployment_sha256=DEPLOYMENT_SHA,
        offset=2,
    )
    first = evaluate_due_sleeve_cycle(
        generation_result=first_generation,
        execution_snapshot=first_snapshot,
        previous_account_state=first_account,
    )

    second_generation = _generation(
        sessions, 21, result_input_sha="9" * 64, action="rebalance"
    )
    second_snapshot = _snapshot(
        second_generation,
        sessions,
        21,
        start_price=110.0,
        end_price=121.0,
        source_sha="8" * 64,
    )
    second = evaluate_due_sleeve_cycle(
        generation_result=second_generation,
        execution_snapshot=second_snapshot,
        previous_account_state=first.next_account_state,
    )

    assert first.holding_end_date == second.holding_start_date
    assert second.opening_nav_fen == first.ending_nav_fen
    assert second.next_account_state.cycle_count == 2
    assert other_account.cycle_count == 0
    assert other_account.nav_fen == 500_000_000


def test_outcome_time_not_executed_choice_is_not_exposed_and_missing_data_fails() -> None:
    sessions = _calendar()
    generation = _generation(sessions, 11)
    snapshot = _snapshot(generation, sessions, 11)
    account = SleeveAccountState.genesis(
        deployment_sha256=DEPLOYMENT_SHA,
        offset=1,
    )

    with pytest.raises(TypeError, match="unexpected keyword argument"):
        evaluate_due_sleeve_cycle(
            generation_result=generation,
            execution_snapshot=snapshot,
            previous_account_state=account,
            execution_status="not_executed",  # type: ignore[call-arg]
        )


def test_benchmark_freezes_roster_uses_start_cash_and_never_survivor_reweights() -> None:
    sessions = _calendar()
    generation = _generation(sessions, 11)
    snapshot = _snapshot(generation, sessions, 11)
    account = SleeveAccountState.genesis(
        deployment_sha256=DEPLOYMENT_SHA,
        offset=generation.due_offset,
    )

    missing_start = snapshot.to_dict()
    for row in missing_start["rows"]:
        if row["date"] == missing_start["holding_start_date"] and row["ticker"] == TARGETS[0]:
            row["open_adj_hex"] = None
    missing_start["snapshot_sha256"] = ""
    cash_outcome = evaluate_due_sleeve_cycle(
        generation_result=generation,
        execution_snapshot=ExecutionSnapshot(**missing_start),
        previous_account_state=account,
    )
    assert cash_outcome.benchmark_return_ppb == 90_000_000
    assert cash_outcome.benchmark_complete_count == cash_outcome.benchmark_expected_count == 10

    suspended = snapshot.to_dict()
    suspension_date = suspended["calendar_sessions"][-6]
    for row in suspended["rows"]:
        if row["ticker"] == TARGETS[0] and row["date"] == suspension_date:
            row["open_adj_hex"] = None
            row["is_suspended"] = True
    suspended["snapshot_sha256"] = ""
    suspension_outcome = evaluate_due_sleeve_cycle(
        generation_result=generation,
        execution_snapshot=ExecutionSnapshot(**suspended),
        previous_account_state=account,
    )
    suspension_point = next(
        point for point in suspension_outcome.daily_path if point.date == suspension_date
    )
    assert suspension_point.benchmark_index_ppb == 1_049_000_000

    delisted = _snapshot(generation, sessions, 11).to_dict()
    delist_date = delisted["calendar_sessions"][-6]
    for row in delisted["rows"]:
        if row["ticker"] == TARGETS[0] and row["date"] == delist_date:
            row["open_adj_hex"] = None
            row["is_delisted"] = True
    delisted["snapshot_sha256"] = ""
    delist_outcome = evaluate_due_sleeve_cycle(
        generation_result=generation,
        execution_snapshot=ExecutionSnapshot(**delisted),
        previous_account_state=account,
    )
    # Nine constituents gain 10%; the delisted constituent is permanently
    # written to zero rather than silently removed and the survivors reweighted.
    assert delist_outcome.benchmark_return_ppb == -10_000_000

    incomplete = deepcopy(snapshot.to_dict())
    for row in incomplete["rows"]:
        if row["date"] == snapshot.holding_start_date and row["ticker"] == TARGETS[0]:
            row["adv_20_asof_hex"] = None
            row["volatility_20_asof_hex"] = None
            row["execution_input_date"] = None
    incomplete["snapshot_sha256"] = ""
    incomplete_snapshot = ExecutionSnapshot(**incomplete)
    with pytest.raises(ProspectiveExecutionError, match="lacks causal ADV/volatility"):
        evaluate_due_sleeve_cycle(
            generation_result=generation,
            execution_snapshot=incomplete_snapshot,
            previous_account_state=account,
        )


def test_snapshot_rejects_future_inputs_incomplete_rectangle_and_future_calendar() -> None:
    sessions = _calendar()
    generation = _generation(sessions, 11)
    snapshot = _snapshot(generation, sessions, 11)

    future_input = deepcopy(snapshot.to_dict())
    for row in future_input["rows"]:
        if row["date"] == snapshot.holding_start_date:
            row["execution_input_date"] = snapshot.holding_start_date
    future_input["snapshot_sha256"] = ""
    with pytest.raises(ProspectiveExecutionError, match="later than the signal"):
        ExecutionSnapshot(**future_input)

    missing_row = deepcopy(snapshot.to_dict())
    missing_row["rows"].pop()
    missing_row["snapshot_sha256"] = ""
    with pytest.raises(ProspectiveExecutionError, match="complete session/security rectangle"):
        ExecutionSnapshot(**missing_row)

    future_calendar = deepcopy(snapshot.to_dict())
    future_calendar["calendar_sessions"].append(sessions[23])
    future_calendar["official_calendar_sha256"] = calendar_prefix_sha256(
        future_calendar["calendar_sessions"]
    )
    future_calendar["snapshot_sha256"] = ""
    with pytest.raises(ProspectiveExecutionError, match="end exactly"):
        ExecutionSnapshot(**future_calendar)


def test_offset_or_shared_boundary_mismatch_fails_closed() -> None:
    sessions = _calendar()
    generation = _generation(sessions, 11)
    snapshot = _snapshot(generation, sessions, 11)
    wrong_offset = SleeveAccountState.genesis(
        deployment_sha256=DEPLOYMENT_SHA,
        offset=2,
    )
    with pytest.raises(ProspectiveExecutionError, match="due offset"):
        evaluate_due_sleeve_cycle(
            generation_result=generation,
            execution_snapshot=snapshot,
            previous_account_state=wrong_offset,
        )

    account = SleeveAccountState.genesis(
        deployment_sha256=DEPLOYMENT_SHA,
        offset=1,
    )
    first = evaluate_due_sleeve_cycle(
        generation_result=generation,
        execution_snapshot=snapshot,
        previous_account_state=account,
    )
    second_generation = _generation(
        sessions, 21, result_input_sha="9" * 64, action="rebalance"
    )
    revised = _snapshot(
        second_generation,
        sessions,
        21,
        start_price=109.0,
        end_price=121.0,
        source_sha="8" * 64,
    )
    with pytest.raises(ProspectiveExecutionError, match="shared-boundary mark changed"):
        evaluate_due_sleeve_cycle(
            generation_result=second_generation,
            execution_snapshot=revised,
            previous_account_state=first.next_account_state,
        )

from __future__ import annotations

from copy import deepcopy

import pandas as pd
import pytest

from factor_lab.adaptive_shadow_execution import (
    AdaptiveShadowExecutionError,
    ShadowCycleOutcome,
    ShadowCyclePlan,
    ShadowExecutionSnapshot,
    evaluate_shadow_cycle,
    genesis_shadow_account,
)
from factor_lab.prospective_execution import (
    AccountPosition,
    ExecutionSnapshot,
    FROZEN_EXECUTION_CONTRACT,
    ProspectiveExecutionError,
    SleeveAccountState,
    evaluate_due_sleeve_cycle,
)
from factor_lab.prospective_targets import (
    GenerationResult,
    SLEEVE_CAPITAL_FEN,
    SleeveState,
    TenSleeveState,
    calendar_prefix_sha256,
)


FORMAL_DEPLOYMENT_SHA = "a" * 64
FORMAL_INPUT_SHA = "e" * 64
FORMAL_DECISION_SHA = "9" * 64
TARGETS = tuple(f"{index:06d}.SZ" for index in range(1, 11))


def _calendar(periods: int = 45) -> list[str]:
    return [value.date().isoformat() for value in pd.bdate_range("2026-09-01", periods=periods)]


def _generation(sessions: list[str], signal_index: int) -> GenerationResult:
    due_offset = signal_index % 10
    target_map = {ticker: 100_000 for ticker in TARGETS}
    sleeves = tuple(
        SleeveState(
            offset=offset,
            initialized=offset == due_offset,
            last_signal_date=sessions[signal_index] if offset == due_offset else None,
            last_calendar_index=signal_index if offset == due_offset else None,
            targets_ppm=target_map if offset == due_offset else {},
            cash_ppm=0 if offset == due_offset else 1_000_000,
        )
        for offset in range(10)
    )
    state = TenSleeveState(
        deployment_sha256=FORMAL_DEPLOYMENT_SHA,
        activation_record_sha256="b" * 64,
        implementation_upgrade_record_sha256="c" * 64,
        last_processed_calendar_index=signal_index,
        last_processed_session=sessions[signal_index],
        sleeves=sleeves,
    )
    plans = tuple(
        {"action": "seed" if sleeve.offset == due_offset else "cash", **sleeve.to_dict()}
        for sleeve in state.sleeves
    )
    return GenerationResult(
        deployment_sha256=FORMAL_DEPLOYMENT_SHA,
        input_snapshot_sha256=FORMAL_INPUT_SHA,
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


def _rows(
    sessions: list[str],
    signal_index: int,
    *,
    start_price: float = 100.0,
    end_price: float = 110.0,
) -> list[dict]:
    output: list[dict] = []
    for day_index, day in enumerate(sessions[signal_index + 1 : signal_index + 12]):
        price = start_price + (end_price - start_price) * day_index / 10.0
        for ticker in TARGETS:
            is_start = day_index == 0
            output.append(
                {
                    "date": day,
                    "ticker": ticker,
                    "open_adj_hex": price.hex(),
                    "adv_20_asof_hex": (1_000_000_000.0).hex() if is_start else None,
                    "volatility_20_asof_hex": (0.0).hex() if is_start else None,
                    "execution_input_date": sessions[signal_index] if is_start else None,
                    "is_one_price_limit_up": False,
                    "is_one_price_limit_down": False,
                    "is_suspended": False,
                    "is_delisted": False,
                }
            )
    return output


def _formal_snapshot(
    generation: GenerationResult,
    sessions: list[str],
    signal_index: int,
) -> ExecutionSnapshot:
    signal = sessions[signal_index]
    start = sessions[signal_index + 1]
    end = sessions[signal_index + 11]
    return ExecutionSnapshot(
        generation_result_sha256=generation.result_sha256,
        execution_source_sha256="f" * 64,
        official_calendar_sha256=calendar_prefix_sha256(sessions[: signal_index + 12]),
        signal_date=signal,
        holding_start_date=start,
        holding_end_date=end,
        calendar_sessions=sessions[: signal_index + 12],
        benchmark_tickers=TARGETS,
        rows=_rows(sessions, signal_index),
        calendar_available_at_utc=f"{signal}T08:00:00Z",
        decision_inputs_available_at_utc=f"{signal}T12:00:00Z",
        trade_deadline_utc=f"{start}T01:15:00Z",
        start_open_available_at_utc=f"{start}T01:30:00Z",
        end_open_available_at_utc=f"{end}T01:30:00Z",
        observation_available_at_utc=f"{end}T02:00:00Z",
    )


def _plan(sessions: list[str], signal_index: int, **overrides: object) -> ShadowCyclePlan:
    values: dict[str, object] = {
        "registry_sha256": "1" * 64,
        "candidate_id": "low_turnover_20_v1",
        "candidate_sha256": "2" * 64,
        "offset": signal_index % 10,
        "signal_date": sessions[signal_index],
        "trade_date": sessions[signal_index + 1],
        "targets_ppm": {ticker: 100_000 for ticker in TARGETS},
        "formal_input_snapshot_sha256": FORMAL_INPUT_SHA,
        "formal_decision_record_sha256": FORMAL_DECISION_SHA,
        "planned_at_utc": f"{sessions[signal_index]}T13:00:00Z",
        "formal_trade_deadline_utc": f"{sessions[signal_index + 1]}T01:15:00Z",
    }
    values.update(overrides)
    return ShadowCyclePlan(**values)


def _bundle(
    sessions: list[str], signal_index: int
) -> tuple[GenerationResult, ExecutionSnapshot, ShadowCyclePlan, ShadowExecutionSnapshot]:
    generation = _generation(sessions, signal_index)
    formal_snapshot = _formal_snapshot(generation, sessions, signal_index)
    plan = _plan(sessions, signal_index)
    wrapper = ShadowExecutionSnapshot(
        target_plan_sha256=plan.plan_sha256,
        formal_input_snapshot_sha256=plan.formal_input_snapshot_sha256,
        formal_decision_record_sha256=plan.formal_decision_record_sha256,
        execution_snapshot=formal_snapshot,
    )
    return generation, formal_snapshot, plan, wrapper


def _account_values(state: SleeveAccountState) -> tuple[object, ...]:
    return (
        state.offset,
        state.initial_capital_fen,
        state.cycle_count,
        state.cash_hex,
        tuple(row.to_dict() for row in state.positions),
        state.nav_fen,
        state.last_holding_end_date,
    )


def test_shadow_and_formal_fixed_core_are_accounting_identical() -> None:
    sessions = _calendar()
    generation, formal_snapshot, plan, wrapper = _bundle(sessions, 11)
    formal = evaluate_due_sleeve_cycle(
        generation_result=generation,
        execution_snapshot=formal_snapshot,
        previous_account_state=SleeveAccountState.genesis(
            deployment_sha256=FORMAL_DEPLOYMENT_SHA,
            offset=generation.due_offset,
        ),
    )
    shadow = evaluate_shadow_cycle(plan, wrapper, genesis_shadow_account(plan))

    scalar_fields = (
        "offset",
        "signal_date",
        "holding_start_date",
        "holding_end_date",
        "observation_available_at_utc",
        "opening_nav_fen",
        "pretrade_nav_fen",
        "ending_nav_fen",
        "gross_return_ppb",
        "net_return_ppb",
        "benchmark_return_ppb",
        "turnover_ppm",
        "fees_fen",
        "executed_order_count",
        "blocked_order_count",
        "benchmark_expected_count",
        "benchmark_complete_count",
    )
    assert {field: getattr(shadow, field) for field in scalar_fields} == {
        field: getattr(formal, field) for field in scalar_fields
    }
    assert [row.to_dict() for row in shadow.daily_path] == [
        row.to_dict() for row in formal.daily_path
    ]
    assert _account_values(shadow.next_account_state) == _account_values(
        formal.next_account_state
    )
    assert shadow.contract_sha256 == FROZEN_EXECUTION_CONTRACT.contract_sha256
    assert shadow.target_plan_sha256 == plan.plan_sha256
    assert shadow.market_execution_snapshot_sha256 == formal_snapshot.snapshot_sha256
    assert shadow.next_account_state.last_generation_result_sha256 == plan.plan_sha256
    assert shadow.next_account_state.last_execution_snapshot_sha256 == wrapper.snapshot_sha256


def test_envelopes_round_trip_and_crash_recompute_is_bit_identical() -> None:
    sessions = _calendar()
    _generation_result, _market, plan, wrapper = _bundle(sessions, 11)
    genesis = genesis_shadow_account(plan)
    first = evaluate_shadow_cycle(plan, wrapper, genesis)
    replay_after_crash = evaluate_shadow_cycle(
        ShadowCyclePlan.from_mapping(plan.to_dict()),
        ShadowExecutionSnapshot.from_mapping(wrapper.to_dict()),
        SleeveAccountState.from_mapping(genesis.to_dict()),
    )

    assert first.to_dict() == replay_after_crash.to_dict()
    assert ShadowCycleOutcome.from_mapping(first.to_dict()).to_dict() == first.to_dict()
    assert genesis == genesis_shadow_account(plan)
    with pytest.raises(AdaptiveShadowExecutionError, match="already applied"):
        evaluate_shadow_cycle(plan, wrapper, first.next_account_state)


def test_all_ten_offsets_have_independent_five_million_accounts() -> None:
    sessions = _calendar(50)
    outcomes = []
    state_hashes = set()
    for signal_index in range(10, 20):
        _generation_result, _market, plan, wrapper = _bundle(sessions, signal_index)
        genesis = genesis_shadow_account(plan)
        outcome = evaluate_shadow_cycle(plan, wrapper, genesis)
        assert genesis.nav_fen == SLEEVE_CAPITAL_FEN == 500_000_000
        assert genesis.cash_hex == (5_000_000.0).hex()
        assert genesis.offset == signal_index % 10
        assert outcome.previous_account_state_sha256 == genesis.state_sha256
        outcomes.append(outcome)
        state_hashes.add(outcome.next_account_state.state_sha256)

    assert {outcome.offset for outcome in outcomes} == set(range(10))
    assert len({outcome.target_plan_sha256 for outcome in outcomes}) == 10
    assert len(state_hashes) == 10
    assert len({outcome.account_deployment_sha256 for outcome in outcomes}) == 1

    other = _plan(
        sessions,
        10,
        candidate_id="low_volatility_252_v1",
        candidate_sha256="3" * 64,
    )
    assert other.account_deployment_sha256 != outcomes[0].account_deployment_sha256


def test_rejects_target_plan_cost_binding_and_deadline_tampering() -> None:
    sessions = _calendar()
    _generation_result, market, plan, wrapper = _bundle(sessions, 11)

    target_tamper = deepcopy(plan.to_dict())
    target_tamper["targets_ppm"][TARGETS[0]] = 99_999
    with pytest.raises(AdaptiveShadowExecutionError, match="equal Top10"):
        ShadowCyclePlan.from_mapping(target_tamper)

    hash_tamper = deepcopy(plan.to_dict())
    hash_tamper["candidate_id"] = "low_volatility_252_v1"
    with pytest.raises(AdaptiveShadowExecutionError, match="plan_sha256"):
        ShadowCyclePlan.from_mapping(hash_tamper)

    wrong_binding = ShadowExecutionSnapshot(
        target_plan_sha256="0" * 64,
        formal_input_snapshot_sha256=plan.formal_input_snapshot_sha256,
        formal_decision_record_sha256=plan.formal_decision_record_sha256,
        execution_snapshot=market,
    )
    with pytest.raises(AdaptiveShadowExecutionError, match="another target plan"):
        evaluate_shadow_cycle(plan, wrong_binding, genesis_shadow_account(plan))

    wrong_formal_input = ShadowExecutionSnapshot(
        target_plan_sha256=plan.plan_sha256,
        formal_input_snapshot_sha256="0" * 64,
        formal_decision_record_sha256=plan.formal_decision_record_sha256,
        execution_snapshot=market,
    )
    with pytest.raises(AdaptiveShadowExecutionError, match="another formal input"):
        evaluate_shadow_cycle(plan, wrong_formal_input, genesis_shadow_account(plan))

    cost_tamper = deepcopy(market.to_dict())
    cost_tamper["contract_sha256"] = "0" * 64
    with pytest.raises(ProspectiveExecutionError, match="different contract"):
        ExecutionSnapshot.from_mapping(cost_tamper)

    late = deepcopy(plan.to_dict())
    late["planned_at_utc"] = f"{sessions[12]}T01:15:01Z"
    late["plan_sha256"] = ""
    with pytest.raises(AdaptiveShadowExecutionError, match="after the formal trade deadline"):
        ShadowCyclePlan(**{key: value for key, value in late.items() if key != "plan_sha256"})


def test_rejects_wrong_offset_future_execution_inputs_and_raw_formal_snapshot() -> None:
    sessions = _calendar()
    generation, market, plan, wrapper = _bundle(sessions, 11)
    wrong_offset = _plan(sessions, 11, offset=2)
    wrong_wrapper = ShadowExecutionSnapshot(
        target_plan_sha256=wrong_offset.plan_sha256,
        formal_input_snapshot_sha256=wrong_offset.formal_input_snapshot_sha256,
        formal_decision_record_sha256=wrong_offset.formal_decision_record_sha256,
        execution_snapshot=market,
    )
    with pytest.raises(AdaptiveShadowExecutionError, match="modulo ten"):
        evaluate_shadow_cycle(
            wrong_offset,
            wrong_wrapper,
            genesis_shadow_account(wrong_offset),
        )

    with pytest.raises(AdaptiveShadowExecutionError, match="target_plan_sha256"):
        evaluate_shadow_cycle(plan, market, genesis_shadow_account(plan))

    future_rows = _rows(sessions, 11)
    for row in future_rows:
        if row["date"] == sessions[12]:
            row["execution_input_date"] = sessions[12]
    with pytest.raises(ProspectiveExecutionError, match="later than the signal"):
        ExecutionSnapshot(
            generation_result_sha256=generation.result_sha256,
            execution_source_sha256="f" * 64,
            official_calendar_sha256=calendar_prefix_sha256(sessions[:23]),
            signal_date=sessions[11],
            holding_start_date=sessions[12],
            holding_end_date=sessions[22],
            calendar_sessions=sessions[:23],
            benchmark_tickers=TARGETS,
            rows=future_rows,
            calendar_available_at_utc=f"{sessions[11]}T08:00:00Z",
            decision_inputs_available_at_utc=f"{sessions[11]}T12:00:00Z",
            trade_deadline_utc=f"{sessions[12]}T01:15:00Z",
            start_open_available_at_utc=f"{sessions[12]}T01:30:00Z",
            end_open_available_at_utc=f"{sessions[22]}T01:30:00Z",
            observation_available_at_utc=f"{sessions[22]}T02:00:00Z",
        )


def _prior_state(
    plan: ShadowCyclePlan,
    start_date: str,
    *,
    ticker: str,
    last_price: float,
) -> SleeveAccountState:
    quantity = 1.0
    cash = 5_000_000.0 - quantity * last_price
    return SleeveAccountState(
        deployment_sha256=plan.account_deployment_sha256,
        offset=plan.offset,
        cycle_count=1,
        cash_hex=cash.hex(),
        positions=(
            AccountPosition(
                ticker=ticker,
                quantity_hex=quantity.hex(),
                last_price_hex=last_price.hex(),
                average_cost_hex=last_price.hex(),
                last_observation_date=start_date,
            ),
        ),
        nav_fen=500_000_000,
        last_holding_end_date=start_date,
        last_generation_result_sha256="7" * 64,
        last_execution_snapshot_sha256="8" * 64,
    )


def test_rejects_missing_prior_holding_row_and_changed_shared_boundary_mark() -> None:
    sessions = _calendar()
    _generation_result, _market, plan, wrapper = _bundle(sessions, 11)
    start = sessions[12]

    missing = _prior_state(plan, start, ticker="999999.SZ", last_price=100.0)
    with pytest.raises(AdaptiveShadowExecutionError, match="prior-held"):
        evaluate_shadow_cycle(plan, wrapper, missing)

    changed = _prior_state(plan, start, ticker=TARGETS[0], last_price=99.0)
    with pytest.raises(AdaptiveShadowExecutionError, match="shared-boundary mark changed"):
        evaluate_shadow_cycle(plan, wrapper, changed)

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest

from factor_lab.research.multi_asset import (
    ACCOUNTING_ABS_TOL_RMB,
    ALL_CODES,
    BASE_COST_BPS_PER_SIDE,
    CANDIDATE_ID,
    CASH_CODE,
    CASH_ONLY_ID,
    CONTROL_ID,
    INITIAL_CAPITAL_RMB,
    LOT_SIZE,
    MAX_SIGNAL_ADV_PARTICIPATION,
    QUARTERLY_BORDA_DIAGNOSTIC_COLUMNS,
    QUARTERLY_BORDA_END_LAG,
    QUARTERLY_BORDA_ID,
    QUARTERLY_BORDA_START_LAG,
    QUARTERLY_DUAL_CONFIRM_BASE_WEIGHT,
    QUARTERLY_DUAL_CONFIRM_BLEND_ID,
    QUARTERLY_DUAL_CONFIRM_DIAGNOSTIC_COLUMNS,
    QUARTERLY_DUAL_CONFIRM_SHORT_START_LAG,
    QUARTERLY_DUAL_CONFIRM_TOP_K,
    QUARTERLY_DUAL_CONFIRM_WEIGHT,
    RISK_BUDGETS,
    RISK_CODES,
    STRESS_COST_BPS_PER_SIDE,
    VOLATILITY_BALANCED_ID,
    VOLATILITY_FLOOR,
    VOLATILITY_LEVEL_LOOKBACK,
    VOLATILITY_RETURN_COUNT,
    SimulationConfig,
    build_monthly_targets,
    combine_phase_metrics,
    evaluate_phase_gate,
    phase_metrics,
    run_strategy,
    selection_gate,
    simulate_targets,
)


def _market(periods: int = 430) -> tuple[dict[str, pd.DataFrame], pd.DatetimeIndex]:
    dates = pd.bdate_range("2018-01-02", periods=periods)
    growth = {
        "510300.SH": 1.0008,
        "159920.SZ": 0.9997,
        "513100.SH": 1.0006,
        "518880.SH": 1.0000,
        "511010.SH": 1.0003,
        CASH_CODE: 1.0001,
    }
    market: dict[str, pd.DataFrame] = {}
    for offset, code in enumerate(ALL_CODES):
        index = np.arange(periods, dtype=float)
        close = (10.0 + offset * 5.0) * np.power(growth[code], index)
        market[code] = pd.DataFrame(
            {
                "trade_date": dates,
                "open": close,
                "close": close,
                "dividend_cash": np.zeros(periods),
                "dividend_pay_date": pd.Series([pd.NaT] * periods, dtype="datetime64[ns]"),
                "total_return_index": np.power(growth[code], index),
                "adv20_rmb": np.full(periods, 100_000_000.0),
            }
        )
    return market, dates


def _flat_market(
    start: str = "2024-01-31", end: str = "2024-03-08"
) -> tuple[dict[str, pd.DataFrame], pd.DatetimeIndex]:
    dates = pd.bdate_range(start, end)
    market: dict[str, pd.DataFrame] = {}
    for offset, code in enumerate(ALL_CODES):
        price = 10.0 + offset * 5.0
        market[code] = pd.DataFrame(
            {
                "trade_date": dates,
                "open": np.full(len(dates), price),
                "close": np.full(len(dates), price),
                "dividend_cash": np.zeros(len(dates)),
                "dividend_pay_date": pd.Series(
                    [pd.NaT] * len(dates), dtype="datetime64[ns]"
                ),
                "total_return_index": np.ones(len(dates)),
                "adv20_rmb": np.full(len(dates), 100_000_000.0),
            }
        )
    return market, dates


def _volatility_market(
    periods: int = 430,
) -> tuple[dict[str, pd.DataFrame], pd.DatetimeIndex]:
    market, dates = _market(periods)
    scales = {
        "510300.SH": 0.010,
        "159920.SZ": 0.014,
        "513100.SH": 0.008,
        "518880.SH": 0.004,
        "511010.SH": 0.002,
    }
    index = np.arange(periods, dtype=float)
    for offset, (code, scale) in enumerate(scales.items(), start=1):
        returns = (
            0.0002
            + scale * np.sin(index * (0.11 + offset * 0.013))
            + scale * 0.35 * np.cos(index * (0.07 + offset * 0.009))
        )
        assert np.all(returns > -1.0)
        market[code]["total_return_index"] = np.cumprod(1.0 + returns)
    return market, dates


def _manual_targets(
    market: dict[str, pd.DataFrame],
    signal_date: pd.Timestamp,
    execution_date: pd.Timestamp,
    weights: dict[str, float],
    *,
    strategy_id: str = CANDIDATE_ID,
) -> pd.DataFrame:
    rows = []
    for code in ALL_CODES:
        source = market[code].set_index("trade_date")
        rows.append(
            {
                "strategy_id": strategy_id,
                "signal_date": signal_date,
                "execution_date": execution_date,
                "code": code,
                "target_weight": float(weights.get(code, 0.0)),
                "signal_adv20_rmb": float(source.at[signal_date, "adv20_rmb"]),
            }
        )
    return pd.DataFrame(rows)


def _month_end_pair(dates: pd.DatetimeIndex, *, minimum_index: int = 260) -> tuple[int, int]:
    for index in range(minimum_index, len(dates) - 1):
        if dates[index].month != dates[index + 1].month:
            return index, index + 1
    raise AssertionError("fixture lacks a usable month end")


def _quarter_end_pair(
    dates: pd.DatetimeIndex, *, minimum_index: int = 260
) -> tuple[int, int]:
    for index in range(minimum_index, len(dates) - 1):
        current = (dates[index].year, (dates[index].month - 1) // 3)
        following = (dates[index + 1].year, (dates[index + 1].month - 1) // 3)
        if current != following:
            return index, index + 1
    raise AssertionError("fixture lacks a usable quarter end")


def test_fixed_registry_and_official_month_end_need_no_future_price_row() -> None:
    market, official = _market()
    signal_index, execution_index = _month_end_pair(official, minimum_index=300)
    signal_date, execution_date = official[signal_index], official[execution_index]
    prefix = {
        code: frame.loc[frame["trade_date"].le(signal_date)].copy()
        for code, frame in market.items()
    }

    targets = build_monthly_targets(prefix, official)
    last = targets.loc[targets["signal_date"].eq(signal_date)].set_index("code")
    assert set(ALL_CODES) == set(last.index)
    assert "513100.SH" in last.index and "513500.SH" not in last.index
    assert last["execution_date"].eq(execution_date).all()
    assert not any(frame["trade_date"].eq(execution_date).any() for frame in prefix.values())

    assert last.at["510300.SH", "target_weight"] == 0.30
    assert last.at["513100.SH", "target_weight"] == 0.10
    assert last.at["511010.SH", "target_weight"] == 0.30
    assert last.at["159920.SZ", "target_weight"] == 0.0
    assert last.at["518880.SH", "target_weight"] == 0.0
    assert last.at[CASH_CODE, "target_weight"] == pytest.approx(0.30)


def test_future_append_does_not_change_targets_or_signal_close_order_shares() -> None:
    market, official = _market()
    signal_index, execution_index = _month_end_pair(official, minimum_index=330)
    signal_date, execution_date = official[signal_index], official[execution_index]
    prefix = {
        code: frame.loc[frame["trade_date"].le(signal_date)].copy()
        for code, frame in market.items()
    }
    prefix_targets = build_monthly_targets(prefix, official)
    prefix_result = simulate_targets(prefix, prefix_targets, official)

    gapped = {code: frame.copy() for code, frame in market.items()}
    for frame in gapped.values():
        frame.loc[frame["trade_date"].eq(execution_date), "open"] *= 3.0
    full_targets = build_monthly_targets(gapped, official)
    comparable = full_targets.loc[full_targets["signal_date"].le(signal_date)].reset_index(drop=True)
    pdt.assert_frame_equal(prefix_targets.reset_index(drop=True), comparable)
    full_result = simulate_targets(gapped, full_targets, official)

    columns = [
        "code",
        "side",
        "signal_price",
        "signal_adv20_rmb",
        "requested_delta_shares",
        "capacity_capped_shares",
        "planned_delta_shares",
    ]
    old_plan = prefix_result["orders"].loc[
        prefix_result["orders"]["signal_date"].eq(signal_date), columns
    ].sort_values("code").reset_index(drop=True)
    new_plan = full_result["orders"].loc[
        full_result["orders"]["signal_date"].eq(signal_date), columns
    ].sort_values("code").reset_index(drop=True)
    pdt.assert_frame_equal(old_plan, new_plan)
    assert prefix_result["orders"].loc[
        prefix_result["orders"]["signal_date"].eq(signal_date), "status"
    ].eq("pending").all()
    assert full_result["trades"].loc[
        full_result["trades"]["execution_date"].eq(execution_date),
        "planned_shares",
    ].tolist() == full_result["orders"].loc[
        full_result["orders"]["execution_date"].eq(execution_date),
        "planned_shares",
    ].tolist()


def test_static_control_uses_exact_base_budgets() -> None:
    market, official = _market()
    targets = build_monthly_targets(market, official, CONTROL_ID)
    first = targets.loc[targets["signal_date"].eq(targets["signal_date"].min())]
    weights = dict(zip(first["code"], first["target_weight"], strict=True))
    assert {code: weights[code] for code in RISK_BUDGETS} == RISK_BUDGETS
    assert weights[CASH_CODE] == 0.0
    assert math.fsum(weights.values()) == pytest.approx(1.0)


def test_volatility_balanced_budget_uses_exact_causal_sample_std_weights() -> None:
    market, official = _volatility_market()
    signal_index, execution_index = _month_end_pair(official, minimum_index=300)
    signal_date = official[signal_index]
    targets = build_monthly_targets(
        market, official, VOLATILITY_BALANCED_ID
    )
    selected = targets.loc[targets["signal_date"].eq(signal_date)].set_index("code")

    assert VOLATILITY_LEVEL_LOOKBACK == 127
    assert VOLATILITY_RETURN_COUNT == 126
    assert VOLATILITY_FLOOR == 1e-12
    assert set(targets["strategy_id"]) == {VOLATILITY_BALANCED_ID}
    assert selected["execution_date"].eq(official[execution_index]).all()
    volatilities: dict[str, float] = {}
    for code in RISK_BUDGETS:
        source = market[code].loc[
            market[code]["trade_date"].le(signal_date), "total_return_index"
        ].tail(VOLATILITY_LEVEL_LOOKBACK)
        levels = source.to_numpy(dtype=float)
        simple_returns = levels[1:] / levels[:-1] - 1.0
        assert len(simple_returns) == VOLATILITY_RETURN_COUNT
        volatilities[code] = float(
            np.std(simple_returns, ddof=1) * math.sqrt(252.0)
        )
        assert selected.at[code, "realized_volatility"] == volatilities[code]
    raw = {
        code: RISK_BUDGETS[code] / volatilities[code]
        for code in RISK_BUDGETS
    }
    denominator = math.fsum(raw.values())
    expected = {code: raw[code] / denominator for code in RISK_BUDGETS}
    last_code = tuple(RISK_BUDGETS)[-1]
    expected[last_code] = 1.0 - math.fsum(
        expected[code] for code in tuple(RISK_BUDGETS)[:-1]
    )
    assert {
        code: float(selected.at[code, "target_weight"])
        for code in RISK_BUDGETS
    } == expected
    assert selected.at[CASH_CODE, "target_weight"] == 0.0
    assert pd.isna(selected.at[CASH_CODE, "realized_volatility"])
    assert selected["volatility_fallback_static"].eq(False).all()
    assert math.isclose(
        math.fsum(selected["target_weight"]),
        1.0,
        rel_tol=0.0,
        abs_tol=1e-12,
    )


def test_volatility_balanced_budget_is_prefix_invariant() -> None:
    market, official = _volatility_market()
    signal_index, _execution_index = _month_end_pair(official, minimum_index=330)
    signal_date = official[signal_index]
    prefix = {
        code: frame.loc[frame["trade_date"].le(signal_date)].copy()
        for code, frame in market.items()
    }

    prefix_targets = build_monthly_targets(
        prefix, official, VOLATILITY_BALANCED_ID
    )
    full_targets = build_monthly_targets(
        market, official, VOLATILITY_BALANCED_ID
    )
    comparable = full_targets.loc[
        full_targets["signal_date"].le(signal_date)
    ].reset_index(drop=True)
    pdt.assert_frame_equal(prefix_targets.reset_index(drop=True), comparable)


@pytest.mark.parametrize("fallback_kind", ["insufficient", "zero_volatility"])
def test_volatility_balanced_budget_falls_back_as_one_static_group(
    fallback_kind: str,
) -> None:
    if fallback_kind == "insufficient":
        market, official = _volatility_market(periods=120)
    else:
        market, official = _volatility_market()
        market["513100.SH"]["total_return_index"] = 1.0
    targets = build_monthly_targets(
        market, official, VOLATILITY_BALANCED_ID
    )

    assert not targets.empty
    for _signal_date, group in targets.groupby("signal_date", sort=True):
        indexed = group.set_index("code")
        assert {
            code: float(indexed.at[code, "target_weight"])
            for code in RISK_BUDGETS
        } == RISK_BUDGETS
        assert indexed.at[CASH_CODE, "target_weight"] == 0.0
        assert indexed["realized_volatility"].isna().all()
        assert indexed["volatility_fallback_static"].eq(True).all()


def test_volatility_balanced_budget_uses_observed_levels_not_carried_levels() -> None:
    market, official = _volatility_market()
    signal_index, _execution_index = _month_end_pair(official, minimum_index=330)
    signal_date = official[signal_index]
    code = "510300.SH"
    removed_date = official[signal_index - 20]
    market[code] = market[code].loc[
        market[code]["trade_date"].ne(removed_date)
    ].reset_index(drop=True)

    targets = build_monthly_targets(
        market, official, VOLATILITY_BALANCED_ID
    )
    selected = targets.loc[targets["signal_date"].eq(signal_date)].set_index("code")
    observed = market[code].loc[
        market[code]["trade_date"].le(signal_date), "total_return_index"
    ].tail(VOLATILITY_LEVEL_LOOKBACK)
    levels = observed.to_numpy(dtype=float)
    expected = float(
        np.std(levels[1:] / levels[:-1] - 1.0, ddof=1) * math.sqrt(252.0)
    )

    assert selected.at[code, "realized_volatility"] == expected
    assert selected["volatility_fallback_static"].eq(False).all()


def test_volatility_balanced_targets_reject_weight_diagnostic_and_id_tampering() -> None:
    market, official = _volatility_market()
    targets = build_monthly_targets(
        market, official, VOLATILITY_BALANCED_ID
    )
    signal_date = targets["signal_date"].max()
    first_risk = targets["signal_date"].eq(signal_date) & targets["code"].eq(
        "510300.SH"
    )
    cash = targets["signal_date"].eq(signal_date) & targets["code"].eq(CASH_CODE)

    weight_tamper = targets.copy()
    weight_tamper.loc[first_risk, "target_weight"] -= 0.01
    weight_tamper.loc[cash, "target_weight"] += 0.01
    with pytest.raises(ValueError, match="weights differ from causal history"):
        simulate_targets(market, weight_tamper, official)

    volatility_tamper = targets.copy()
    volatility_tamper.loc[first_risk, "realized_volatility"] *= 2.0
    with pytest.raises(ValueError, match="realized volatility differs"):
        simulate_targets(market, volatility_tamper, official)

    fallback_tamper = targets.copy()
    fallback_tamper.loc[
        fallback_tamper["signal_date"].eq(signal_date),
        "volatility_fallback_static",
    ] = True
    with pytest.raises(ValueError, match="fallback identity differs"):
        simulate_targets(market, fallback_tamper, official)

    id_tamper = targets.copy()
    id_tamper["strategy_id"] = CONTROL_ID
    with pytest.raises(ValueError, match="diagnostics require the exact"):
        simulate_targets(market, id_tamper, official)

    missing_diagnostic = targets.drop(columns="realized_volatility")
    with pytest.raises(ValueError, match="lack frozen diagnostics"):
        simulate_targets(market, missing_diagnostic, official)


def test_quarterly_borda_uses_exact_session_lags_cash_excess_and_borda_points() -> None:
    market, official = _market()
    signal_index, execution_index = _quarter_end_pair(official, minimum_index=300)
    signal_date = official[signal_index]
    targets = build_monthly_targets(market, official, QUARTERLY_BORDA_ID)
    selected = targets.loc[targets["signal_date"].eq(signal_date)].set_index("code")

    assert QUARTERLY_BORDA_ID == "quarterly_12_1_dual_momentum_rank_budget"
    assert QUARTERLY_BORDA_START_LAG == 252
    assert QUARTERLY_BORDA_END_LAG == 21
    assert set(QUARTERLY_BORDA_DIAGNOSTIC_COLUMNS).issubset(targets.columns)
    assert set(targets["strategy_id"]) == {QUARTERLY_BORDA_ID}
    assert selected["execution_date"].eq(official[execution_index]).all()
    assert selected["momentum_start_date"].eq(
        official[signal_index - QUARTERLY_BORDA_START_LAG]
    ).all()
    assert selected["momentum_end_date"].eq(
        official[signal_index - QUARTERLY_BORDA_END_LAG]
    ).all()
    assert selected["endpoint_fallback_all_cash"].eq(False).all()

    start = official[signal_index - QUARTERLY_BORDA_START_LAG]
    end = official[signal_index - QUARTERLY_BORDA_END_LAG]
    levels = {
        code: market[code].set_index("trade_date")["total_return_index"]
        for code in ALL_CODES
    }
    cash_log = math.log(levels[CASH_CODE].at[end] / levels[CASH_CODE].at[start])
    relative = {
        code: math.log(levels[code].at[end] / levels[code].at[start]) - cash_log
        for code in RISK_BUDGETS
    }
    order = {code: index for index, code in enumerate(RISK_BUDGETS)}
    ranked = sorted(
        (code for code in RISK_BUDGETS if relative[code] > 0.0),
        key=lambda code: (-relative[code], order[code]),
    )
    assert ranked == ["510300.SH", "513100.SH", "511010.SH"]
    assert selected["cash_log_momentum"].eq(cash_log).all()
    for rank, code in enumerate(ranked, start=1):
        points = len(ranked) - rank + 1
        assert selected.at[code, "relative_cash_log_momentum"] == relative[code]
        assert selected.at[code, "borda_rank"] == rank
        assert selected.at[code, "borda_points"] == points
    assert selected["positive_momentum_count"].eq(len(ranked)).all()
    assert selected.at["510300.SH", "target_weight"] == 3.0 / 6.0
    assert selected.at["513100.SH", "target_weight"] == 2.0 / 6.0
    assert selected.at["511010.SH", "target_weight"] == 1.0 / 6.0
    assert selected.at[CASH_CODE, "target_weight"] == 0.0

    prefix = {
        code: frame.loc[frame["trade_date"].le(signal_date)].copy()
        for code, frame in market.items()
    }
    prefix_targets = build_monthly_targets(prefix, official, QUARTERLY_BORDA_ID)
    comparable = targets.loc[targets["signal_date"].le(signal_date)].reset_index(
        drop=True
    )
    pdt.assert_frame_equal(prefix_targets.reset_index(drop=True), comparable)


def test_quarterly_borda_exact_ties_use_all_risk_codes_in_frozen_order() -> None:
    market, official = _market()
    index = np.arange(len(official), dtype=float)
    for code in RISK_BUDGETS:
        market[code]["total_return_index"] = np.power(1.0005, index)
    signal_index, _ = _quarter_end_pair(official, minimum_index=300)
    signal_date = official[signal_index]
    selected = build_monthly_targets(
        market, official, QUARTERLY_BORDA_ID
    ).loc[lambda frame: frame["signal_date"].eq(signal_date)].set_index("code")

    assert selected["positive_momentum_count"].eq(len(RISK_BUDGETS)).all()
    expected_weights = {
        code: (len(RISK_BUDGETS) - rank + 1) / 15.0
        for rank, code in enumerate(RISK_BUDGETS, start=1)
    }
    for rank, code in enumerate(RISK_BUDGETS, start=1):
        points = len(RISK_BUDGETS) - rank + 1
        assert selected.at[code, "borda_rank"] == rank
        assert selected.at[code, "borda_points"] == points
        assert selected.at[code, "target_weight"] == expected_weights[code]
    assert selected.at[CASH_CODE, "target_weight"] == 1.0 - math.fsum(
        expected_weights[code] for code in RISK_CODES
    )
    assert math.fsum(selected["target_weight"]) == 1.0


def test_quarterly_borda_missing_any_required_endpoint_falls_back_as_one_group() -> None:
    market, official = _market()
    signal_index, _ = _quarter_end_pair(official, minimum_index=300)
    signal_date = official[signal_index]
    missing_date = official[signal_index - QUARTERLY_BORDA_START_LAG]
    market["513100.SH"] = market["513100.SH"].loc[
        market["513100.SH"]["trade_date"].ne(missing_date)
    ].reset_index(drop=True)
    selected = build_monthly_targets(
        market, official, QUARTERLY_BORDA_ID
    ).loc[lambda frame: frame["signal_date"].eq(signal_date)].set_index("code")

    assert selected["endpoint_fallback_all_cash"].eq(True).all()
    assert selected["positive_momentum_count"].eq(0).all()
    assert selected["cash_log_momentum"].isna().all()
    assert selected["relative_cash_log_momentum"].isna().all()
    assert selected["borda_rank"].isna().all()
    assert selected["borda_points"].eq(0).all()
    assert selected.loc[list(RISK_BUDGETS), "target_weight"].eq(0.0).all()
    assert selected.at[CASH_CODE, "target_weight"] == 1.0
    assert simulate_targets(market, selected.reset_index(), official)[
        "strategy_id"
    ] == QUARTERLY_BORDA_ID


def test_quarterly_borda_rejects_malformed_tri_instead_of_calling_it_cash_fallback() -> None:
    market, official = _market()
    signal_index, _ = _quarter_end_pair(official, minimum_index=300)
    endpoint = official[signal_index - QUARTERLY_BORDA_START_LAG]
    market["513100.SH"].loc[
        market["513100.SH"]["trade_date"].eq(endpoint), "total_return_index"
    ] = np.nan
    with pytest.raises(ValueError, match="invalid total_return_index"):
        build_monthly_targets(market, official, QUARTERLY_BORDA_ID)


def test_quarterly_borda_rejects_weight_and_each_frozen_diagnostic_tamper() -> None:
    market, official = _market()
    targets = build_monthly_targets(market, official, QUARTERLY_BORDA_ID)
    signal_date = targets["signal_date"].max()
    one_signal = targets.loc[targets["signal_date"].eq(signal_date)].reset_index(
        drop=True
    )
    risk = one_signal["code"].eq("510300.SH")
    cash = one_signal["code"].eq(CASH_CODE)

    weight_tamper = one_signal.copy()
    weight_tamper.loc[risk, "target_weight"] -= 0.01
    weight_tamper.loc[cash, "target_weight"] += 0.01
    with pytest.raises(ValueError, match="target weights differ from causal history"):
        simulate_targets(market, weight_tamper, official)

    for name in QUARTERLY_BORDA_DIAGNOSTIC_COLUMNS:
        tampered = one_signal.copy()
        if name == "momentum_start_date":
            tampered[name] += pd.Timedelta(days=1)
        elif name == "momentum_end_date":
            tampered[name] -= pd.Timedelta(days=1)
        elif name in {"cash_log_momentum", "positive_momentum_count"}:
            tampered[name] += 1
        elif name in {"relative_cash_log_momentum", "borda_rank", "borda_points"}:
            tampered.loc[risk, name] += 1
        else:
            tampered[name] = True
        with pytest.raises(ValueError, match="diagnostics differ from causal history"):
            simulate_targets(market, tampered, official)

    with pytest.raises(ValueError, match="lack frozen diagnostics"):
        simulate_targets(
            market, one_signal.drop(columns="borda_points"), official
        )
    id_tamper = one_signal.copy()
    id_tamper["strategy_id"] = CONTROL_ID
    with pytest.raises(ValueError, match="diagnostics require the exact strategy_id"):
        simulate_targets(market, id_tamper, official)


def test_quarterly_dual_confirm_blends_exact_long_borda_and_short_top3() -> None:
    market, official = _market()
    signal_index, execution_index = _quarter_end_pair(official, minimum_index=300)
    signal = official[signal_index]
    selected = build_monthly_targets(
        market, official, QUARTERLY_DUAL_CONFIRM_BLEND_ID
    ).loc[lambda frame: frame["signal_date"].eq(signal)].set_index("code")

    assert QUARTERLY_DUAL_CONFIRM_BLEND_ID == (
        "quarterly_dual_confirm_top3_borda_blend_75_25"
    )
    assert QUARTERLY_DUAL_CONFIRM_SHORT_START_LAG == 126
    assert QUARTERLY_DUAL_CONFIRM_TOP_K == 3
    assert QUARTERLY_DUAL_CONFIRM_WEIGHT == 0.75
    assert QUARTERLY_DUAL_CONFIRM_BASE_WEIGHT == 0.25
    assert set(QUARTERLY_DUAL_CONFIRM_DIAGNOSTIC_COLUMNS).issubset(
        selected.columns
    )
    assert selected["execution_date"].eq(official[execution_index]).all()
    assert selected["momentum_start_date"].eq(official[signal_index - 252]).all()
    assert selected["short_momentum_start_date"].eq(
        official[signal_index - 126]
    ).all()
    assert selected["momentum_end_date"].eq(official[signal_index - 21]).all()
    dual_codes = {"510300.SH", "513100.SH", "511010.SH"}
    assert set(selected.index[selected["dual_selected"]]) == dual_codes
    assert selected["dual_eligible_count"].eq(3).all()
    base = {
        "510300.SH": 3.0 / 6.0,
        "513100.SH": 2.0 / 6.0,
        "511010.SH": 1.0 / 6.0,
    }
    expected = {}
    for code in RISK_CODES:
        expected[code] = 0.75 * (1.0 / 3.0 if code in dual_codes else 0.0) + 0.25 * base.get(code, 0.0)
        assert selected.at[code, "base_borda_weight"] == base.get(code, 0.0)
        assert selected.at[code, "dual_top3_weight"] == (
            1.0 / 3.0 if code in dual_codes else 0.0
        )
        assert selected.at[code, "target_weight"] == expected[code]
    assert selected.at[CASH_CODE, "target_weight"] == 1.0 - math.fsum(
        expected.values()
    )
    assert math.fsum(selected["target_weight"]) == 1.0


def test_quarterly_dual_confirm_requires_positive_short_momentum() -> None:
    market, official = _market()
    signal_index, _ = _quarter_end_pair(official, minimum_index=300)
    signal = official[signal_index]
    code = "511010.SH"
    source = market[code].set_index("trade_date")
    source.at[official[signal_index - 252], "total_return_index"] = 1.0
    source.at[official[signal_index - 126], "total_return_index"] = 2.0
    source.at[official[signal_index - 21], "total_return_index"] = 1.5
    market[code] = source.reset_index()
    selected = build_monthly_targets(
        market, official, QUARTERLY_DUAL_CONFIRM_BLEND_ID
    ).loc[lambda frame: frame["signal_date"].eq(signal)].set_index("code")

    assert selected.at[code, "relative_cash_log_momentum"] > 0.0
    assert selected.at[code, "short_relative_cash_log_momentum"] < 0.0
    assert bool(selected.at[code, "dual_eligible"]) is False
    assert bool(selected.at[code, "dual_selected"]) is False
    assert selected.at[code, "dual_top3_weight"] == 0.0
    assert selected.at[code, "base_borda_weight"] > 0.0
    for risk_code in RISK_CODES:
        assert selected.at[risk_code, "target_weight"] == (
            0.75 * selected.at[risk_code, "dual_top3_weight"]
            + 0.25 * selected.at[risk_code, "base_borda_weight"]
        )


def test_quarterly_dual_confirm_missing_short_endpoint_falls_back_all_cash() -> None:
    market, official = _market()
    signal_index, _ = _quarter_end_pair(official, minimum_index=300)
    signal = official[signal_index]
    missing = official[signal_index - QUARTERLY_DUAL_CONFIRM_SHORT_START_LAG]
    code = "513100.SH"
    market[code] = market[code].loc[
        market[code]["trade_date"].ne(missing)
    ].reset_index(drop=True)
    selected = build_monthly_targets(
        market, official, QUARTERLY_DUAL_CONFIRM_BLEND_ID
    ).loc[lambda frame: frame["signal_date"].eq(signal)].set_index("code")

    assert selected["endpoint_fallback_all_cash"].eq(True).all()
    assert selected.loc[list(RISK_CODES), "target_weight"].eq(0.0).all()
    assert selected.at[CASH_CODE, "target_weight"] == 1.0
    assert selected["short_relative_cash_log_momentum"].isna().all()
    assert selected["dual_selected"].eq(False).all()


def test_quarterly_dual_confirm_is_future_tail_invariant_and_rejects_tamper() -> None:
    market, official = _market()
    signal_index, _ = _quarter_end_pair(official, minimum_index=330)
    signal = official[signal_index]
    prefix = {
        code: frame.loc[frame["trade_date"].le(signal)].copy()
        for code, frame in market.items()
    }
    prefix_targets = build_monthly_targets(
        prefix, official, QUARTERLY_DUAL_CONFIRM_BLEND_ID
    )
    poisoned = {code: frame.copy() for code, frame in market.items()}
    for frame in poisoned.values():
        future = frame["trade_date"].gt(signal)
        frame.loc[future, ["open", "close", "total_return_index"]] *= 11.0
    full_targets = build_monthly_targets(
        poisoned, official, QUARTERLY_DUAL_CONFIRM_BLEND_ID
    )
    comparable = full_targets.loc[full_targets["signal_date"].le(signal)].reset_index(
        drop=True
    )
    pdt.assert_frame_equal(prefix_targets.reset_index(drop=True), comparable)

    one = prefix_targets.loc[prefix_targets["signal_date"].eq(signal)].reset_index(
        drop=True
    )
    risk = one["code"].eq("510300.SH")
    cash = one["code"].eq(CASH_CODE)
    weight = one.copy()
    weight.loc[risk, "target_weight"] -= 0.01
    weight.loc[cash, "target_weight"] += 0.01
    with pytest.raises(ValueError, match="target weights differ from causal history"):
        simulate_targets(prefix, weight, official)
    for column in (
        "short_relative_cash_log_momentum",
        "base_borda_weight",
        "dual_selected",
        "dual_top3_weight",
    ):
        tampered = one.copy()
        if column == "dual_selected":
            tampered.loc[risk, column] = ~tampered.loc[risk, column]
        else:
            tampered.loc[risk, column] += 1.0
        with pytest.raises(ValueError, match="diagnostics differ from causal history"):
            simulate_targets(prefix, tampered, official)
    with pytest.raises(ValueError, match="lack frozen diagnostics"):
        simulate_targets(prefix, one.drop(columns="dual_top3_weight"), official)


def test_cash_only_uses_exact_cash_weight_and_next_official_open() -> None:
    market, official = _market()
    targets = build_monthly_targets(market, official, CASH_ONLY_ID)

    assert not targets.empty
    assert set(targets["strategy_id"]) == {CASH_ONLY_ID}
    official_position = {date: index for index, date in enumerate(official)}
    for signal_date, group in targets.groupby("signal_date", sort=True):
        weights = group.set_index("code")["target_weight"]
        fractions = group.set_index("code")["trend_positive_fraction"]
        execution_dates = set(group["execution_date"])
        assert len(execution_dates) == 1
        execution_date = next(iter(execution_dates))
        assert official_position[execution_date] == official_position[signal_date] + 1
        assert (signal_date.year, signal_date.month) != (
            execution_date.year,
            execution_date.month,
        )
        assert weights.loc[list(RISK_BUDGETS)].eq(0.0).all()
        assert weights.at[CASH_CODE] == 1.0
        assert fractions.loc[list(RISK_BUDGETS)].eq(0.0).all()
        assert pd.isna(fractions.at[CASH_CODE])

    result = simulate_targets(market, targets, official)
    assert result["strategy_id"] == CASH_ONLY_ID
    assert not result["trades"].empty
    assert set(result["trades"]["code"]) == {CASH_CODE}


def test_cash_only_rejects_weight_and_strategy_id_tampering() -> None:
    market, official = _market()
    targets = build_monthly_targets(market, official, CASH_ONLY_ID)
    first_signal = targets["signal_date"].min()

    weight_tamper = targets.copy()
    first_cash = weight_tamper["signal_date"].eq(first_signal) & weight_tamper[
        "code"
    ].eq(CASH_CODE)
    first_risk = weight_tamper["signal_date"].eq(first_signal) & weight_tamper[
        "code"
    ].eq("510300.SH")
    weight_tamper.loc[first_cash, "target_weight"] = 0.99
    weight_tamper.loc[first_risk, "target_weight"] = 0.01
    with pytest.raises(ValueError, match="cash-only targets must allocate exactly"):
        simulate_targets(market, weight_tamper, official)

    id_tamper = targets.copy()
    id_tamper.loc[id_tamper.index[0], "strategy_id"] = CONTROL_ID
    with pytest.raises(ValueError, match="exactly one registered strategy"):
        simulate_targets(market, id_tamper, official)

    with pytest.raises(ValueError, match="unknown multi-asset strategy_id"):
        build_monthly_targets(market, official, "cash_only_typo")


def test_signal_adv_capacity_is_ten_percent_and_rejected_fraction_not_full_order() -> None:
    market, official = _flat_market(end="2024-02-02")
    signal_date, execution_date = official[0], official[1]
    code = "510300.SH"
    market[code].loc[:, ["open", "close"]] = 1_000.0
    market[code].loc[:, "adv20_rmb"] = 6_000_000.0
    targets = _manual_targets(market, signal_date, execution_date, {code: 1.0})
    result = simulate_targets(market, targets, official)
    order = result["orders"].loc[result["orders"]["code"].eq(code)].iloc[0]

    assert INITIAL_CAPITAL_RMB == 1_000_000.0
    assert MAX_SIGNAL_ADV_PARTICIPATION == 0.10
    assert order["requested_shares"] == 1_000
    assert order["capacity_capped_shares"] == 600
    assert order["capacity_limited_shares"] == 400
    assert order["capacity_rmb"] == 600_000.0
    assert result["capacity"]["capacity_limited_requested_notional_ratio"] == pytest.approx(0.40)
    assert result["capacity"]["capacity_violation_count"] == 0
    assert (result["trades"]["executed_shares"] % LOT_SIZE).eq(0).all()


@pytest.mark.parametrize(
    ("execution_open", "expected_fill"),
    [(500.0, 0.60), (2_000.0, 0.40)],
)
def test_execution_open_gap_values_fill_and_capacity_in_same_open_units(
    execution_open: float, expected_fill: float
) -> None:
    market, official = _flat_market(end="2024-02-02")
    signal_date, execution_date = official[0], official[1]
    code = "510300.SH"
    market[code].loc[:, ["open", "close"]] = 1_000.0
    market[code].loc[:, "adv20_rmb"] = 6_000_000.0
    market[code].loc[
        market[code]["trade_date"].eq(execution_date), "open"
    ] = execution_open
    targets = _manual_targets(market, signal_date, execution_date, {code: 1.0})
    result = simulate_targets(market, targets, official)
    trade = result["trades"].loc[result["trades"]["code"].eq(code)].iloc[0]
    assert trade["requested_execution_notional"] == pytest.approx(
        1_000 * execution_open
    )
    assert trade["capacity_limited_execution_notional"] == pytest.approx(
        400 * execution_open
    )
    assert result["capacity"]["capacity_limited_requested_notional_ratio"] == pytest.approx(
        0.40
    )
    assert result["capacity"]["requested_notional_fill_ratio"] == pytest.approx(
        expected_fill
    )


def test_blocked_sell_keeps_sealed_buys_and_cash_reduction_is_pro_rata() -> None:
    market, official = _flat_market()
    january_signal = official[official.month == 1][-1]
    february_open = official[official.month == 2][0]
    february_signal = official[official.month == 2][-1]
    march_open = official[official.month == 3][0]
    old_code, buy_a, buy_b = "510300.SH", "159920.SZ", "513100.SH"
    initial = _manual_targets(
        market,
        january_signal,
        february_open,
        {old_code: 0.50, CASH_CODE: 0.50},
    )
    rotation = _manual_targets(
        market,
        february_signal,
        march_open,
        {buy_a: 0.50, buy_b: 0.50},
    )
    market[old_code].loc[
        market[old_code]["trade_date"].eq(march_open), "open"
    ] = np.nan
    result = simulate_targets(
        market, pd.concat([initial, rotation], ignore_index=True), official
    )

    march_orders = result["orders"].loc[
        result["orders"]["execution_date"].eq(march_open)
    ].set_index("code")
    march_trades = result["trades"].loc[
        result["trades"]["execution_date"].eq(march_open)
    ].set_index("code")
    assert march_trades.at[old_code, "status"] == "blocked_missing_open"
    assert march_trades.at[old_code, "executed_shares"] == 0
    assert march_trades.at[buy_a, "executed_shares"] > 0
    assert march_trades.at[buy_b, "executed_shares"] > 0
    assert march_trades.at[buy_a, "executed_shares"] < march_orders.at[buy_a, "planned_shares"]
    assert march_trades.at[buy_b, "executed_shares"] < march_orders.at[buy_b, "planned_shares"]
    assert march_orders.at[buy_a, "planned_shares"] == abs(
        march_orders.at[buy_a, "planned_delta_shares"]
    )
    assert march_orders.at[buy_b, "planned_shares"] == abs(
        march_orders.at[buy_b, "planned_delta_shares"]
    )
    previous_date = official[official.get_loc(march_open) - 1]
    cash_before = float(
        result["daily_nav"].set_index("trade_date").at[previous_date, "cash"]
    )
    sell_rows = march_trades.loc[march_trades["side"].eq("sell")]
    available_cash = cash_before + float(
        (sell_rows["actual_executed_notional"] - sell_rows["cost"]).sum()
    )
    buy_rows = march_trades.loc[[buy_a, buy_b]]
    required_cash = float(
        (
            buy_rows["planned_shares"]
            * buy_rows["execution_price"]
            * (1.0 + BASE_COST_BPS_PER_SIDE / 10_000.0)
        ).sum()
    )
    common_scale = available_cash / required_cash
    for code in (buy_a, buy_b):
        expected = (
            math.floor(march_orders.at[code, "planned_shares"] * common_scale / LOT_SIZE)
            * LOT_SIZE
        )
        assert march_trades.at[code, "executed_shares"] == expected


def test_ex_date_receivable_is_not_spendable_until_pay_date() -> None:
    market, official = _flat_market()
    january_signal = official[official.month == 1][-1]
    february_open = official[official.month == 2][0]
    february_signal = official[official.month == 2][-1]
    ex_date = official[official.month == 3][0]
    pay_date = pd.Timestamp("2024-03-05")
    code = "510300.SH"
    initial = _manual_targets(market, january_signal, february_open, {code: 1.0})
    exit_target = _manual_targets(
        market, february_signal, ex_date, {CASH_CODE: 1.0}
    )
    row = market[code]["trade_date"].eq(ex_date)
    market[code].loc[row, "dividend_cash"] = 1.0
    market[code].loc[row, "dividend_pay_date"] = pay_date
    result = simulate_targets(
        market, pd.concat([initial, exit_target], ignore_index=True), official
    )
    daily = result["daily_nav"].set_index("trade_date")
    entitled_shares = int(
        result["holdings"].loc[
            result["holdings"]["trade_date"].eq(official[official.get_loc(ex_date) - 1])
            & result["holdings"]["code"].eq(code),
            "shares",
        ].iloc[0]
    )
    assert entitled_shares > 0
    assert daily.at[ex_date, "dividend_accrual"] == pytest.approx(entitled_shares)
    assert daily.at[ex_date, "dividend_cash_paid"] == 0.0
    assert daily.at[ex_date, "dividend_receivable"] == pytest.approx(entitled_shares)
    assert daily.at[pay_date, "dividend_cash_paid"] == pytest.approx(entitled_shares)
    assert daily.at[pay_date, "dividend_receivable"] == 0.0
    assert result["reconciliation"]["passed"]


def test_unit_event_scales_holding_and_pending_order_after_old_unit_entitlement() -> None:
    market, official = _flat_market()
    january_signal = official[official.month == 1][-1]
    february_open = official[official.month == 2][0]
    february_signal = official[official.month == 2][-1]
    event_date = official[official.month == 3][0]
    pay_date = pd.Timestamp("2024-03-05")
    code = "510300.SH"
    initial = _manual_targets(
        market,
        january_signal,
        february_open,
        {code: 0.50, CASH_CODE: 0.50},
    )
    increase = _manual_targets(
        market,
        february_signal,
        event_date,
        {code: 1.0},
    )
    for frame in market.values():
        frame["unit_multiplier"] = 1.0
        frame["reference_price_reset"] = False
    event = market[code]["trade_date"].eq(event_date)
    market[code].loc[event, "unit_multiplier"] = 2.0
    market[code].loc[event, ["open", "close"]] = 5.0
    market[code].loc[event, "dividend_cash"] = 1.0
    market[code].loc[event, "dividend_pay_date"] = pay_date
    cash_event = market[CASH_CODE]["trade_date"].eq(event_date)
    market[CASH_CODE].loc[cash_event, "reference_price_reset"] = True

    result = simulate_targets(
        market, pd.concat([initial, increase], ignore_index=True), official
    )
    prior_date = official[official.get_loc(event_date) - 1]
    prior_shares = int(
        result["holdings"].loc[
            result["holdings"]["trade_date"].eq(prior_date)
            & result["holdings"]["code"].eq(code),
            "shares",
        ].iloc[0]
    )
    order = result["orders"].loc[
        result["orders"]["execution_date"].eq(event_date)
        & result["orders"]["code"].eq(code)
    ].iloc[0]
    trade = result["trades"].loc[
        result["trades"]["execution_date"].eq(event_date)
        & result["trades"]["code"].eq(code)
    ].iloc[0]
    original_requested = int(round(order["requested_signal_notional"] / order["signal_price"]))
    original_planned = int(round(order["planned_signal_notional"] / order["signal_price"]))
    assert order["execution_unit_multiplier"] == 2.0
    assert order["requested_shares"] == original_requested * 2
    assert order["capacity_capped_shares"] == original_planned * 2
    assert order["planned_shares"] == original_planned * 2
    assert order["requested_signal_notional"] == pytest.approx(
        original_requested * order["signal_price"]
    )
    assert order["planned_signal_notional"] == pytest.approx(
        original_planned * order["signal_price"]
    )
    assert order["capacity_rmb"] == pytest.approx(
        order["signal_adv20_rmb"] * MAX_SIGNAL_ADV_PARTICIPATION
    )
    event_holding = int(
        result["holdings"].loc[
            result["holdings"]["trade_date"].eq(event_date)
            & result["holdings"]["code"].eq(code),
            "shares",
        ].iloc[0]
    )
    assert event_holding == prior_shares * 2 + int(trade["executed_shares"])
    daily = result["daily_nav"].set_index("trade_date")
    assert daily.at[event_date, "dividend_accrual"] == pytest.approx(prior_shares)
    assert daily.at[event_date, "unit_event_count"] == 1
    assert daily.at[event_date, "reference_price_reset_count"] == 1
    assert abs(daily.at[event_date, "accounting_error"]) <= ACCOUNTING_ABS_TOL_RMB
    assert result["reconciliation"]["passed"]

    cash_order = result["orders"].loc[
        result["orders"]["execution_date"].eq(event_date)
        & result["orders"]["code"].eq(CASH_CODE)
    ].iloc[0]
    assert cash_order["execution_unit_multiplier"] == 1.0


def test_unit_event_rejects_non_lot_scaling_and_unanchored_first_row() -> None:
    market, official = _flat_market()
    for frame in market.values():
        frame["unit_multiplier"] = 1.0
        frame["reference_price_reset"] = False
    first = official[0]
    market["510300.SH"].loc[
        market["510300.SH"]["trade_date"].eq(first), "unit_multiplier"
    ] = 2.0
    with pytest.raises(ValueError, match="starts with an unanchored"):
        build_monthly_targets(market, official)

    market, official = _flat_market()
    for frame in market.values():
        frame["unit_multiplier"] = 1.0
        frame["reference_price_reset"] = False
    market["510300.SH"].loc[
        market["510300.SH"]["trade_date"].eq(first), "reference_price_reset"
    ] = True
    with pytest.raises(ValueError, match="starts with an unanchored"):
        build_monthly_targets(market, official)

    market, official = _flat_market()
    for frame in market.values():
        frame["unit_multiplier"] = 1.0
        frame["reference_price_reset"] = False
    code = "510300.SH"
    # Exactly 100 units are bought in February; a 0.5 multiplier would leave
    # 50 units, which violates the frozen 100-unit lot contract.
    market[code].loc[:, ["open", "close"]] = 1_000.0
    january_signal = official[official.month == 1][-1]
    february_open = official[official.month == 2][0]
    february_signal = official[official.month == 2][-1]
    event_date = official[official.month == 3][0]
    initial = _manual_targets(
        market, january_signal, february_open, {code: 0.20, CASH_CODE: 0.80}
    )
    hold = _manual_targets(
        market, february_signal, event_date, {code: 0.20, CASH_CODE: 0.80}
    )
    market[code].loc[
        market[code]["trade_date"].eq(event_date), "unit_multiplier"
    ] = 0.5
    with pytest.raises(RuntimeError, match="non-integer or non-lot-aligned"):
        simulate_targets(
            market, pd.concat([initial, hold], ignore_index=True), official
        )


def test_missing_execution_open_blocks_without_substitute() -> None:
    market, official = _flat_market(end="2024-02-02")
    signal_date, execution_date = official[0], official[1]
    code = "513100.SH"
    market[code].loc[
        market[code]["trade_date"].eq(execution_date), "open"
    ] = np.nan
    targets = _manual_targets(market, signal_date, execution_date, {code: 1.0})
    result = simulate_targets(market, targets, official)
    order = result["trades"].loc[result["trades"]["code"].eq(code)].iloc[0]
    assert order["status"] == "blocked_missing_open"
    assert order["executed_shares"] == 0
    assert result["daily_nav"].iloc[-1]["cash"] == pytest.approx(INITIAL_CAPITAL_RMB)
    assert result["holdings"]["shares"].eq(0).all()


def test_eight_and_sixteen_bp_costs_and_full_account_reconciliation() -> None:
    market, official = _market()
    base = run_strategy(
        market, official, CONTROL_ID, SimulationConfig(BASE_COST_BPS_PER_SIDE)
    )
    stress = run_strategy(
        market, official, CONTROL_ID, SimulationConfig(STRESS_COST_BPS_PER_SIDE)
    )
    assert stress["daily_nav"]["nav"].iloc[-1] < base["daily_nav"]["nav"].iloc[-1]
    assert stress["daily_nav"]["trade_cost"].sum() > base["daily_nav"]["trade_cost"].sum()
    assert base["reconciliation"]["passed"]
    assert base["reconciliation"]["max_abs_daily_accounting_error_rmb"] <= ACCOUNTING_ABS_TOL_RMB
    assert base["reconciliation"]["cumulative_identity_error_rmb"] == pytest.approx(
        0.0, abs=ACCOUNTING_ABS_TOL_RMB
    )
    assert len(base["holdings"]) == len(base["daily_nav"]) * len(ALL_CODES)
    assert (base["holdings"]["shares"] % LOT_SIZE).eq(0).all()
    traded_day = base["daily_nav"].loc[base["daily_nav"]["trade_count"].gt(0)].iloc[0]
    actual_notional = base["trades"].loc[
        base["trades"]["execution_date"].eq(traded_day["trade_date"]),
        "actual_executed_notional",
    ].sum()
    assert traded_day["turnover"] == pytest.approx(
        actual_notional / traded_day["pretrade_nav"]
    )


def test_single_missing_513100_session_carries_nav_but_cannot_signal_or_trade() -> None:
    market, official = _market()
    gap_index = next(
        index
        for index in range(300, len(official) - 1)
        if official[index - 1].month == official[index].month == official[index + 1].month
    )
    gap_date = official[gap_index]
    code = "513100.SH"
    market[code] = market[code].loc[
        ~market[code]["trade_date"].eq(gap_date)
    ].reset_index(drop=True)
    result = run_strategy(market, official, CONTROL_ID)
    holding = result["holdings"].loc[
        result["holdings"]["trade_date"].eq(gap_date)
        & result["holdings"]["code"].eq(code)
    ].iloc[0]
    assert not holding["observed"] and not holding["tradable"]
    assert result["trades"].loc[result["trades"]["execution_date"].eq(gap_date)].empty
    assert abs(
        result["daily_nav"].set_index("trade_date").at[gap_date, "accounting_error"]
    ) <= ACCOUNTING_ABS_TOL_RMB


def test_missing_official_month_end_skips_month_and_two_day_gap_fails() -> None:
    market, official = _market()
    original = build_monthly_targets(market, official)
    missing_signal = pd.Timestamp(original["signal_date"].drop_duplicates().iloc[1])
    code = "513100.SH"
    single = {ticker: frame.copy() for ticker, frame in market.items()}
    single[code] = single[code].loc[
        ~single[code]["trade_date"].eq(missing_signal)
    ].reset_index(drop=True)
    rebuilt = build_monthly_targets(single, official)
    assert not rebuilt["signal_date"].eq(missing_signal).any()
    previous = pd.Timestamp(
        market[code].loc[market[code]["trade_date"].lt(missing_signal), "trade_date"].iloc[-1]
    )
    assert not rebuilt["signal_date"].eq(previous).any()

    double = {ticker: frame.copy() for ticker, frame in market.items()}
    double[code] = double[code].loc[
        ~double[code]["trade_date"].isin([official[310], official[311]])
    ].reset_index(drop=True)
    with pytest.raises(ValueError, match="consecutive official-session gap"):
        build_monthly_targets(double, official)


def test_filtered_phase_starts_fresh_and_first_execution_cost_enters_return() -> None:
    market, official = _market()
    targets = build_monthly_targets(market, official, CONTROL_ID)
    executions = sorted(targets["execution_date"].unique())
    phase_start = pd.Timestamp(executions[1])
    filtered = targets.loc[targets["execution_date"].ge(phase_start)].copy()
    result = simulate_targets(market, filtered, official)
    before = result["daily_nav"].loc[result["daily_nav"]["trade_date"].lt(phase_start)]
    assert before["nav"].eq(INITIAL_CAPITAL_RMB).all()
    assert result["holdings"].loc[
        result["holdings"]["trade_date"].lt(phase_start), "shares"
    ].eq(0).all()
    metrics = phase_metrics(result, start=phase_start)
    assert metrics["performance_start"] == phase_start.date().isoformat()
    assert metrics["start_nav"] == INITIAL_CAPITAL_RMB
    assert metrics["total_return"] == pytest.approx(
        result["daily_nav"]["nav"].iloc[-1] / INITIAL_CAPITAL_RMB - 1.0
    )

    flat, flat_official = _flat_market(end="2024-02-02")
    one = _manual_targets(
        flat, flat_official[0], flat_official[1], {"510300.SH": 1.0}
    )
    one_result = simulate_targets(flat, one, flat_official)
    one_phase = phase_metrics(one_result, start=flat_official[1])
    assert one_phase["total_return"] < 0.0
    assert one_phase["total_return"] == pytest.approx(
        one_result["daily_nav"]["nav"].iloc[-1] / INITIAL_CAPITAL_RMB - 1.0
    )


def test_phase_rejects_unseeded_start_truncated_years_and_nan_evidence() -> None:
    dates = pd.bdate_range("2020-01-02", "2021-12-30")
    nav = INITIAL_CAPITAL_RMB * np.cumprod(np.full(len(dates), 1.0001))
    daily = pd.DataFrame(
        {
            "trade_date": dates,
            "nav": nav,
            "accounting_error": np.zeros(len(dates)),
        }
    )
    with pytest.raises(ValueError, match="preceding seed NAV"):
        phase_metrics(daily, start=dates[0])

    truncated = phase_metrics(
        daily,
        start=pd.Timestamp("2020-01-15"),
        end=pd.Timestamp("2020-12-18"),
    )
    assert truncated["complete_year_count"] == 0

    bad_nav = daily.copy()
    bad_nav.loc[10, "nav"] = np.nan
    with pytest.raises(ValueError, match="invalid dates or NAV"):
        phase_metrics(bad_nav)
    bad_accounting = daily.copy()
    bad_accounting.loc[10, "accounting_error"] = np.nan
    with pytest.raises(ValueError, match="invalid accounting_error"):
        phase_metrics(bad_accounting)


def test_relative_stress_phase_gates_and_train_first_selection() -> None:
    dates = pd.bdate_range("2020-01-02", "2022-12-30")
    wave = 0.0002 * np.sin(np.arange(len(dates)) / 9.0)

    def account(base_return: float) -> pd.DataFrame:
        returns = base_return + wave
        return pd.DataFrame(
            {
                "trade_date": dates,
                "nav": INITIAL_CAPITAL_RMB * np.cumprod(1.0 + returns),
                "turnover": np.full(len(dates), 0.001),
                "requested_notional": np.full(len(dates), 1_000.0),
                "executed_notional": np.full(len(dates), 999.9),
                "capacity_limited_requested_notional": np.zeros(len(dates)),
                "accounting_error": np.zeros(len(dates)),
                "future_input_violation_count": np.zeros(len(dates)),
            }
        )

    base = phase_metrics(account(0.00035))
    stress = phase_metrics(account(0.00030))
    control = phase_metrics(account(0.00025))
    combined = combine_phase_metrics(base, stress, control)
    thresholds = {
        "cagr_min": 0.01,
        "sharpe_min": 0.6,
        "max_drawdown_min": -0.20,
        "positive_complete_years_min": 2,
        "stress_cagr_min": 0.0,
        "relative_cagr_min": 0.0,
        "relative_sharpe_min": 0.0,
        "relative_max_drawdown_min": 0.0,
        "requested_notional_fill_ratio_min": 0.99,
        "capacity_limited_requested_notional_ratio_max": 0.01,
        "max_abs_accounting_error_max": 1e-8,
    }
    assert evaluate_phase_gate(combined, thresholds)["passed"]

    failed = dict(combined, cagr=-0.01)
    short = selection_gate(
        failed,
        combined,
        train_config=thresholds,
        validation_config=thresholds,
    )
    assert short["status"] == "train_failed"
    assert not short["validation_evaluated"]
    assert short["selected_candidate_id"] is None
    selected = selection_gate(
        combined,
        combined,
        train_config=thresholds,
        validation_config=thresholds,
    )
    assert selected["selected_candidate_id"] == CANDIDATE_ID


def test_adv_warmup_may_be_missing_but_signal_adv_may_not() -> None:
    market, official = _market()
    for frame in market.values():
        frame.loc[:18, "adv20_rmb"] = np.nan
    targets = build_monthly_targets(market, official)
    assert not targets.empty
    signal = targets["signal_date"].min()
    broken = {code: frame.copy() for code, frame in market.items()}
    broken["510300.SH"].loc[
        broken["510300.SH"]["trade_date"].eq(signal), "adv20_rmb"
    ] = np.nan
    with pytest.raises(ValueError, match="lacks finite ADV20"):
        build_monthly_targets(broken, official)

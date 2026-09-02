"""Continuous 13.0 real-share simulation and exact group P&L attribution."""

from __future__ import annotations

from dataclasses import dataclass
from math import fsum, isfinite
from typing import Any

import numpy as np
import pandas as pd

from factor_lab.portfolio.execution import AShareCostPolicy
from factor_lab.research.pit_stock import (
    PITStockContractError,
    STRATEGY_ID,
    annualized_metrics,
    canonical_sha256,
    select_quarterly_targets,
)
from factor_lab.research.pit_stock_account import SuspensionProjection
from factor_lab.research.pit_stock_real_account import (
    PnlPosting,
    RealShareAccount,
    RealShareAction,
    RealShareOrder,
    RealSharePolicy,
    accrue_ex_open,
    capture_record_close,
    execute_real_share_rebalance,
    mark_owned_shares,
    prepare_real_share_actions,
    settle_pay_and_list_open,
    write_down_delists,
)
from factor_lab.research.pit_stock_minute_execution import (
    MINUTE_EXECUTION_BAR_COLUMNS,
    MINUTE_EXECUTION_CONTEXT_COLUMNS,
    MinuteRealShareOrder,
    SequentialMinutePolicy,
    begin_sequential_minute_rebalance,
    observe_window_a,
    observe_window_b,
    observe_window_c,
)


@dataclass(frozen=True)
class RealShareSimulationBuild:
    daily_nav: pd.DataFrame
    boundaries: pd.DataFrame
    orders: pd.DataFrame
    postings: pd.DataFrame
    periods: pd.DataFrame
    period_ticker_pnl: pd.DataFrame
    group_pnl: pd.DataFrame
    metrics: dict[str, Any]
    target_payload_sha256: str


DEVELOPMENT_TARGET_PAYLOAD_SHA256 = (
    "1022288372cd07f97b0e963b670c3a7e9ddfc94414b4d21cb7cbea9c643e76be"
)
RECONCILIATION_TOLERANCE = 1e-8


def _date(value: Any) -> pd.Timestamp:
    result = pd.Timestamp(value)
    if pd.isna(result):
        raise PITStockContractError("unknown simulation date")
    if result.tzinfo is not None:
        result = result.tz_convert(None)
    return result.normalize()


def _minute_provider_trace_violations(
    calls: list[dict[str, Any]],
    *,
    signals: tuple[pd.Timestamp, ...],
    roles: tuple[str, ...],
) -> list[str]:
    expected: list[tuple[str, str, str]] = []
    for signal in signals:
        date = signal.date().isoformat()
        expected.append((date, "all_roles_boundary_proof", "09:30_anchor"))
        if signal == signals[-1]:
            continue
        expected.extend((date, role, "context") for role in roles)
        expected.extend((date, "all_roles", phase) for phase in ("A", "B", "C"))
    actual = [
        (str(value.get("signal_date")), str(value.get("role")), str(value.get("phase")))
        for value in calls
    ]
    violations: list[str] = []
    if actual != expected:
        violations.append("minute provider call order differs from anchor/all-context/A/B/C lockstep")
    for index, value in enumerate(calls):
        count = value.get("ticker_count")
        digest = str(value.get("ticker_payload_sha256") or "")
        if (
            isinstance(count, bool)
            or not isinstance(count, int)
            or count < 0
            or len(digest) != 64
        ):
            violations.append(f"minute provider call audit fields differ at {index}")
    return violations


def _intraday_resume_carry_tickers(
    *,
    minute_no_anchor: set[str],
    session_resume_open_blocks: set[str],
) -> set[str]:
    return {
        str(ticker)
        for ticker in minute_no_anchor & session_resume_open_blocks
    }


def _auction_anchor_matches_daily_open(
    *,
    auction_open: float,
    daily_open: float,
    daily_pre_close: float,
    zero_liquidity_flat_price: bool,
) -> bool:
    prices = (auction_open, daily_open, daily_pre_close)
    if not all(isfinite(value) and value > 0.0 for value in prices):
        return False
    if abs(auction_open - daily_open) <= 0.005 + 1e-12:
        return True
    return bool(
        zero_liquidity_flat_price
        and abs(auction_open - daily_pre_close) <= 0.005 + 1e-12
    )


def _target_maps(
    panel: pd.DataFrame, stored_targets: pd.DataFrame
) -> tuple[
    dict[pd.Timestamp, dict[str, float]],
    dict[pd.Timestamp, dict[str, Any]],
    str,
]:
    work = panel.copy()
    required_panel = {"signal_date", "ticker"}
    required_targets = {
        "strategy_id",
        "signal_date",
        "ticker",
        "target_weight",
    }
    if not required_panel.issubset(work.columns) or not required_targets.issubset(
        stored_targets.columns
    ):
        raise PITStockContractError("real-share target inputs lack required columns")
    work["signal_date"] = pd.to_datetime(work["signal_date"]).dt.normalize()
    if work.duplicated(["signal_date", "ticker"]).any():
        raise PITStockContractError("development panel contains duplicate identities")
    stored = stored_targets.copy()
    stored["signal_date"] = pd.to_datetime(stored["signal_date"]).dt.normalize()
    if not stored["strategy_id"].astype(str).eq(STRATEGY_ID).all():
        raise PITStockContractError("stored target strategy ID differs")
    if stored.duplicated(["signal_date", "ticker"]).any():
        raise PITStockContractError("stored targets contain duplicate identities")
    targets = {}
    decisions = {}
    expected_identities: set[tuple[pd.Timestamp, str]] = set()
    for signal, snapshot in work.groupby("signal_date", sort=True):
        selected, decision = select_quarterly_targets(snapshot)
        expected = {
            str(row.ticker): float(row.target_weight)
            for row in selected[["ticker", "target_weight"]].itertuples(
                index=False
            )
        }
        actual_rows = stored.loc[stored["signal_date"].eq(signal)]
        actual = {
            str(row.ticker): float(row.target_weight)
            for row in actual_rows[["ticker", "target_weight"]].itertuples(
                index=False
            )
        }
        if expected != actual:
            raise PITStockContractError(
                f"real-share target differs from 12.0 at {signal.date()}"
            )
        expected_identities.update(
            (pd.Timestamp(signal), ticker) for ticker in expected
        )
        targets[pd.Timestamp(signal)] = expected
        decisions[pd.Timestamp(signal)] = decision.to_dict()
    actual_identities = set(
        (pd.Timestamp(row.signal_date), str(row.ticker))
        for row in stored[["signal_date", "ticker"]].itertuples(index=False)
    )
    if actual_identities != expected_identities:
        raise PITStockContractError("stored targets contain extra/missing identities")
    payload_rows = [
        {
            "strategy_id": STRATEGY_ID,
            "signal_date": str(signal.date()),
            "ticker": ticker,
            "target_weight": float(weight),
        }
        for signal, target in sorted(targets.items())
        for ticker, weight in sorted(target.items())
    ]
    digest = canonical_sha256(payload_rows)
    if digest != DEVELOPMENT_TARGET_PAYLOAD_SHA256:
        raise PITStockContractError("12.0 development target payload differs")
    return targets, decisions, digest


def _benchmark_targets(
    panel: pd.DataFrame,
) -> dict[pd.Timestamp, dict[str, float]]:
    work = panel.copy()
    work["signal_date"] = pd.to_datetime(work["signal_date"]).dt.normalize()
    result: dict[pd.Timestamp, dict[str, float]] = {}
    for signal, snapshot in work.groupby("signal_date", sort=True):
        members = snapshot.loc[snapshot["universe_member"]].assign(
            _ticker=snapshot.loc[snapshot["universe_member"], "ticker"].astype(
                str
            )
        )
        selected = members.sort_values(
            ["adv20", "_ticker"],
            ascending=[False, True],
            kind="mergesort",
        ).head(500)
        if len(selected) != 500:
            raise PITStockContractError("ADV500 benchmark is incomplete")
        result[pd.Timestamp(signal)] = {
            str(ticker): 1.0 / 500.0 for ticker in selected["ticker"]
        }
    return result


def _real_market(
    raw: pd.DataFrame,
    *,
    required: set[str],
    suspended: set[str],
    delisted: set[str],
    signal_snapshot: pd.DataFrame | None,
    session: pd.Timestamp,
) -> pd.DataFrame:
    indexed = raw.rename(columns={"ts_code": "ticker"}).set_index("ticker")
    observed = set(indexed.index.astype(str))
    unexplained = sorted(required - observed - suspended - delisted)
    if unexplained:
        raise PITStockContractError(
            f"required ticker lacks bar/event proof on {session.date()}: {unexplained[:10]}"
        )
    rows = indexed.loc[indexed.index.intersection(required)].copy()
    rows["is_one_price_limit_up"] = rows["high"].eq(rows["low"]) & rows[
        "open"
    ].gt(rows["pre_close"])
    rows["is_one_price_limit_down"] = rows["high"].eq(rows["low"]) & rows[
        "open"
    ].lt(rows["pre_close"])
    rows["is_suspended"] = rows.index.isin(suspended)
    if signal_snapshot is None:
        rows["signal_adv20"] = 1.0
        rows["signal_vol_daily"] = 0.0
    else:
        signal = signal_snapshot.set_index("ticker")
        rows["signal_adv20"] = pd.to_numeric(
            signal["adv20"], errors="coerce"
        ).reindex(rows.index)
        rows["signal_vol_daily"] = pd.to_numeric(
            signal["vol63"], errors="coerce"
        ).reindex(rows.index) / np.sqrt(252.0)
    missing = []
    for ticker in sorted(required - observed):
        missing.append(
            {
                "ticker": ticker,
                "open": np.nan,
                "close": np.nan,
                "pre_close": np.nan,
                "adj_factor": np.nan,
                "signal_adv20": np.nan,
                "signal_vol_daily": np.nan,
                "is_one_price_limit_up": False,
                "is_one_price_limit_down": False,
                "is_suspended": ticker in suspended,
            }
        )
    rows = rows.reset_index()
    if missing:
        rows = pd.concat(
            [rows, pd.DataFrame(missing)], ignore_index=True, sort=False
        )
    return rows.sort_values("ticker", kind="mergesort").reset_index(drop=True)


def _market_state(snapshot: pd.DataFrame) -> str:
    universe = snapshot.loc[snapshot["universe_member"]]
    long_positive = float(universe["mom12"].median()) > 0.0
    short_positive = float(universe["mom6"].median()) > 0.0
    if long_positive and short_positive:
        return "both_positive"
    if long_positive:
        return "long_positive_short_nonpositive"
    if short_positive:
        return "long_nonpositive_short_positive"
    return "both_nonpositive"


def _groups(snapshot: pd.DataFrame) -> tuple[dict[str, dict[str, str]], str]:
    state = _market_state(snapshot)
    values = {
        str(row.ticker): {
            "industry": str(row.industry)
            if pd.notna(row.industry)
            else "UNKNOWN",
            "size_bucket": str(row.size_bucket)
            if pd.notna(row.size_bucket)
            else "UNKNOWN_SIZE",
            "market_state": state,
        }
        for row in snapshot[["ticker", "industry", "size_bucket"]].itertuples(
            index=False
        )
    }
    return values, state


def _posting_frame(postings: list[PnlPosting]) -> pd.DataFrame:
    return pd.DataFrame(
        [posting.to_dict() for posting in postings],
        columns=list(PnlPosting.__dataclass_fields__),
    )


def _role_metrics(
    daily: pd.DataFrame,
    periods: pd.DataFrame,
    orders: pd.DataFrame,
    *,
    initial_nav: float,
) -> dict[str, Any]:
    returns = daily["net_return"].to_numpy(dtype=float)
    nav = np.concatenate(([initial_nav], daily["nav"].to_numpy(dtype=float)))
    years = len(returns) / 252.0
    volatility = float(returns.std(ddof=1) * np.sqrt(252.0))
    period_returns = periods["period_return"].to_numpy(dtype=float)
    train = periods["signal_date"].lt(pd.Timestamp("2021-01-01")).to_numpy()
    yearly = {
        str(year): float(
            np.prod(
                1.0
                + returns[daily["trade_date"].dt.year.eq(year).to_numpy()]
            )
            - 1.0
        )
        for year in sorted(daily["trade_date"].dt.year.unique())
    }
    requested = pd.to_numeric(orders["requested_notional"], errors="raise")
    target_gap = pd.to_numeric(
        orders["target_gap_notional"], errors="raise"
    ) if "target_gap_notional" in orders.columns else requested
    executed = pd.to_numeric(orders["executed_notional"], errors="raise")
    filled_decision = pd.to_numeric(
        orders["filled_decision_notional"], errors="raise"
    )
    requested_total = float(requested.sum())
    target_gap_total = float(target_gap.sum())
    fill_ratio = (
        float(filled_decision.sum()) / target_gap_total
        if target_gap_total
        else 1.0
    )
    if not -1e-12 <= fill_ratio <= 1.0 + 1e-12:
        raise PITStockContractError("decision-notional fill ratio escapes [0,1]")
    capacity_requested = float(
        target_gap.loc[orders["capacity_limited"].astype(bool)].sum()
    )
    return {
        "daily_observations": len(returns),
        "initial_nav": initial_nav,
        "end_nav": float(daily.iloc[-1]["nav"]),
        "daily_cagr": float((nav[-1] / initial_nav) ** (1.0 / years) - 1.0),
        "daily_sharpe": float(returns.mean() * 252.0 / volatility)
        if volatility
        else 0.0,
        "daily_max_drawdown": float(
            (nav / np.maximum.accumulate(nav) - 1.0).min()
        ),
        "full_quarterly": annualized_metrics(period_returns),
        "train_quarterly": annualized_metrics(period_returns[train]),
        "validation_quarterly": annualized_metrics(period_returns[~train]),
        "calendar_year_return": yearly,
        "positive_2019_2022_year_count": sum(
            yearly.get(str(year), 0.0) > 0.0
            for year in (2019, 2020, 2021, 2022)
        ),
        "positive_2019_2022_year_fraction": sum(
            yearly.get(str(year), 0.0) > 0.0
            for year in (2019, 2020, 2021, 2022)
        )
        / 4.0,
        "requested_notional": requested_total,
        "target_gap_notional": target_gap_total,
        "executed_notional": float(executed.sum()),
        "filled_decision_notional": float(filled_decision.sum()),
        "requested_notional_fill_ratio": fill_ratio,
        "target_gap_fill_ratio": fill_ratio,
        "capacity_limited_requested_notional_ratio": (
            capacity_requested / target_gap_total if target_gap_total else 0.0
        ),
        "capacity_limited_target_gap_ratio": (
            capacity_requested / target_gap_total if target_gap_total else 0.0
        ),
        "blocked_order_count": int(orders["status"].eq("blocked").sum()),
        "unfilled_order_count": int(orders["status"].eq("unfilled").sum()),
        "negative_cash_observation_count": int(
            daily["cash"].lt(-1e-8).sum()
        ),
        "leverage_observation_count": int(
            daily["gross_stock_exposure"].gt(1.0 + 1e-8).sum()
        ),
        "max_daily_reconciliation_error": float(
            daily["reconciliation_error"].abs().max()
        ),
        "max_period_reconciliation_error": float(
            periods["reconciliation_error"].abs().max()
        ),
    }


def _finalize_period(
    *,
    role: str,
    period: dict[str, Any],
    end_date: pd.Timestamp,
    end_nav: float,
    postings: list[PnlPosting],
    period_rows: list[dict[str, Any]],
    ticker_rows: list[dict[str, Any]],
    group_rows: list[dict[str, Any]],
) -> None:
    signal = str(period["signal_date"])
    selected = [posting for posting in postings if posting.period_signal == signal]
    pnl = fsum(posting.amount for posting in selected)
    start_nav = float(period["start_nav"])
    period_return = end_nav / start_nav - 1.0
    reconciliation = pnl / start_nav - period_return
    if not isfinite(reconciliation) or abs(reconciliation) > RECONCILIATION_TOLERANCE:
        raise PITStockContractError("real-share period P&L does not reconcile")
    period_rows.append(
        {
            "role": role,
            "signal_date": pd.Timestamp(signal),
            "execution_date": pd.Timestamp(period["execution_date"]),
            "outcome_end": end_date,
            "start_nav": start_nav,
            "end_nav": end_nav,
            "period_return": period_return,
            "net_pnl": pnl,
            "reconciliation_error": reconciliation,
            "market_state": period["market_state"],
        }
    )
    ticker_pnl: dict[str, float] = {}
    for posting in selected:
        ticker_pnl[posting.ticker] = ticker_pnl.get(posting.ticker, 0.0) + posting.amount
    tickers = set(ticker_pnl) | set(period["start_weights"])
    default = {
        "industry": "UNKNOWN",
        "size_bucket": "UNKNOWN_SIZE",
        "market_state": period["market_state"],
    }
    for ticker in sorted(tickers):
        groups = period["groups"].get(ticker, default)
        ticker_rows.append(
            {
                "role": role,
                "signal_date": pd.Timestamp(signal),
                "ticker": ticker,
                "net_pnl": ticker_pnl.get(ticker, 0.0),
                "start_weight": period["start_weights"].get(ticker, 0.0),
                **groups,
            }
        )
    for dimension in ("industry", "size_bucket", "market_state"):
        grouped: dict[str, dict[str, float]] = {}
        for ticker in tickers:
            group = period["groups"].get(ticker, default)[dimension]
            item = grouped.setdefault(
                group, {"net_pnl": 0.0, "start_weight": 0.0}
            )
            item["net_pnl"] += ticker_pnl.get(ticker, 0.0)
            item["start_weight"] += period["start_weights"].get(ticker, 0.0)
        if dimension == "market_state" and not grouped:
            grouped[period["market_state"]] = {
                "net_pnl": 0.0,
                "start_weight": 0.0,
            }
        group_error = (
            fsum(item["net_pnl"] for item in grouped.values()) - pnl
        ) / start_nav
        if (
            not isfinite(group_error)
            or abs(group_error) > RECONCILIATION_TOLERANCE
        ):
            raise PITStockContractError(f"{dimension} P&L does not reconcile")
        for group, item in sorted(grouped.items()):
            group_rows.append(
                {
                    "role": role,
                    "signal_date": pd.Timestamp(signal),
                    "dimension": dimension,
                    "group": group,
                    **item,
                }
            )


def _phase_gate_metrics(
    *,
    role_metrics: dict[str, Any],
    periods: pd.DataFrame,
    ticker_pnl: pd.DataFrame,
    group_pnl: pd.DataFrame,
    orders: pd.DataFrame,
    candidate_targets: dict[pd.Timestamp, dict[str, float]],
    include_benchmark: bool,
    future_input_violation_count: int,
) -> dict[str, Any]:
    candidate_roles = ("candidate_base", "candidate_stress")
    segment_keys = {
        "full": "full_quarterly",
        "train": "train_quarterly",
        "validation": "validation_quarterly",
    }
    positive_segments = {
        role: {
            segment: float(role_metrics[role][key]["cagr"]) > 0.0
            for segment, key in segment_keys.items()
        }
        for role in candidate_roles
    }
    edge_segments: dict[str, bool] = {}
    if include_benchmark:
        edge_segments = {
            segment: float(role_metrics["candidate_base"][key]["cagr"])
            > float(role_metrics["adv500_base"][key]["cagr"])
            for segment, key in segment_keys.items()
        }

    size_metrics: dict[str, Any] = {}
    industry_metrics: dict[str, Any] = {}
    for role in candidate_roles:
        role_groups = group_pnl.loc[group_pnl["role"].eq(role)]
        size = (
            role_groups.loc[role_groups["dimension"].eq("size_bucket")]
            .groupby("group", sort=True)["net_pnl"]
            .sum()
        )
        size_values = {
            bucket: float(size.get(bucket, 0.0))
            for bucket in ("small", "mid", "large")
        }
        size_metrics[role] = {
            "cumulative_net_pnl": size_values,
            "all_strictly_positive": all(
                value > 0.0 for value in size_values.values()
            ),
        }
        industry = role_groups.loc[
            role_groups["dimension"].eq("industry")
        ].copy()
        sufficient: dict[str, dict[str, float | int]] = {}
        for group, values in industry.groupby("group", sort=True):
            invested = values.loc[values["start_weight"].gt(0.0)]
            invested_count = len(invested)
            mean_weight = (
                float(invested["start_weight"].mean())
                if invested_count
                else 0.0
            )
            if invested_count >= 4 and mean_weight >= 0.01:
                sufficient[str(group)] = {
                    "invested_period_count": invested_count,
                    "mean_invested_start_weight": mean_weight,
                    "cumulative_net_pnl": float(values["net_pnl"].sum()),
                }
        count = len(sufficient)
        positive = sum(
            float(value["cumulative_net_pnl"]) > 0.0
            for value in sufficient.values()
        )
        total_pnl = float(
            periods.loc[periods["role"].eq(role), "net_pnl"].sum()
        )
        loo_positive = sum(
            total_pnl - float(value["cumulative_net_pnl"]) > 0.0
            for value in sufficient.values()
        )
        industry_metrics[role] = {
            "sufficient": sufficient,
            "sufficient_count": count,
            "positive_fraction": positive / count if count else 0.0,
            "leave_one_out_positive_fraction": (
                loo_positive / count if count else 0.0
            ),
        }

    non_both_signals = set(
        pd.Timestamp(value)
        for value in periods.loc[
            periods["role"].eq("candidate_base")
            & periods["market_state"].ne("both_positive"),
            "signal_date",
        ]
    )
    non_both_targets_empty = all(
        not candidate_targets[signal] for signal in non_both_signals
    )
    non_both_buys = orders.loc[
        orders["role"].isin(candidate_roles)
        & orders["signal_date"].isin(non_both_signals)
        & orders["side"].eq("buy")
        & orders["executed_shares"].gt(0)
    ]
    unexplained_residuals: list[dict[str, str]] = []
    residuals = ticker_pnl.loc[
        ticker_pnl["role"].isin(candidate_roles)
        & ticker_pnl["signal_date"].isin(non_both_signals)
        & ticker_pnl["start_weight"].gt(0.0)
    ]
    for residual in residuals.itertuples(index=False):
        proof = orders.loc[
            orders["role"].eq(residual.role)
            & orders["signal_date"].eq(residual.signal_date)
            & orders["ticker"].eq(residual.ticker)
            & orders["side"].eq("sell")
        ]
        constrained = proof.loc[
            proof["status"].isin(("blocked", "unfilled"))
            | proof["capacity_limited"].astype(bool)
            | proof["lot_limited"].astype(bool)
            | proof["pending_limited"].astype(bool)
        ]
        if constrained.empty:
            unexplained_residuals.append(
                {
                    "role": str(residual.role),
                    "signal_date": str(pd.Timestamp(residual.signal_date).date()),
                    "ticker": str(residual.ticker),
                }
            )

    operational = {
        role: {
            "fill_pass": role_metrics[role]["target_gap_fill_ratio"]
            >= 0.98,
            "capacity_pass": role_metrics[role][
                "capacity_limited_target_gap_ratio"
            ]
            <= 0.02,
            "negative_cash_pass": role_metrics[role][
                "negative_cash_observation_count"
            ]
            == 0,
            "leverage_pass": role_metrics[role][
                "leverage_observation_count"
            ]
            == 0,
            "daily_reconciliation_pass": role_metrics[role][
                "max_daily_reconciliation_error"
            ]
            <= RECONCILIATION_TOLERANCE,
            "period_reconciliation_pass": role_metrics[role][
                "max_period_reconciliation_error"
            ]
            <= RECONCILIATION_TOLERANCE,
        }
        for role in role_metrics
    }
    checks = {
        "candidate_positive_every_segment": all(
            all(values.values()) for values in positive_segments.values()
        ),
        "candidate_base_above_adv500_every_segment": (
            all(edge_segments.values()) if include_benchmark else False
        ),
        "operational": all(
            all(values.values()) for values in operational.values()
        ),
        "size_groups_positive": all(
            value["all_strictly_positive"] for value in size_metrics.values()
        ),
        "industry_positive_fraction": all(
            value["positive_fraction"] >= 0.60
            for value in industry_metrics.values()
        ),
        "industry_leave_one_out_fraction": all(
            value["leave_one_out_positive_fraction"] >= 0.80
            for value in industry_metrics.values()
        ),
        "positive_calendar_year_fraction": all(
            role_metrics[role]["positive_2019_2022_year_fraction"] >= 0.75
            for role in candidate_roles
        ),
        "non_both_positive_behavior": (
            non_both_targets_empty
            and non_both_buys.empty
            and not unexplained_residuals
        ),
        "future_input_violations": int(future_input_violation_count) == 0,
    }
    stage_1_checks = {
        key: value
        for key, value in checks.items()
        if key != "candidate_base_above_adv500_every_segment"
    }
    stage_1_passed = all(stage_1_checks.values())
    return {
        "complete": include_benchmark,
        "positive_segments": positive_segments,
        "base_edge_vs_adv500": edge_segments,
        "operational": operational,
        "size": size_metrics,
        "industry": industry_metrics,
        "market_state": {
            "non_both_positive_signal_count": len(non_both_signals),
            "targets_empty": non_both_targets_empty,
            "executed_buy_count": len(non_both_buys),
            "unexplained_residuals": unexplained_residuals,
        },
        "future_input_violation_count": int(future_input_violation_count),
        "checks": checks,
        "stage_1_checks": stage_1_checks,
        "stage_1_passed": stage_1_passed,
        "passed": include_benchmark and all(checks.values()),
    }


def simulate_candidate_real_share_accounts(
    *,
    store: Any,
    panel: pd.DataFrame,
    stored_targets: pd.DataFrame,
    resolved_actions: pd.DataFrame,
    suspension_events: pd.DataFrame,
    initial_capital: float = 10_000_000.0,
    include_benchmark: bool = True,
    tushare_reference_diagnostics: pd.DataFrame | None = None,
    execution_mode: str = "daily_open_hypothetical",
    minute_market_provider: Any | None = None,
) -> RealShareSimulationBuild:
    panel = panel.copy()
    if execution_mode not in {
        "daily_open_hypothetical",
        "sequential_minute",
    }:
        raise PITStockContractError("real-share execution mode differs")
    if execution_mode == "sequential_minute" and minute_market_provider is None:
        raise PITStockContractError("sequential minute market provider is required")
    panel["signal_date"] = pd.to_datetime(panel["signal_date"]).dt.normalize()
    signals = tuple(sorted(pd.Timestamp(v) for v in panel["signal_date"].unique()))
    if len(signals) < 2:
        raise PITStockContractError("real-share simulation requires two signals")
    targets, decisions, target_digest = _target_maps(panel, stored_targets)
    benchmark_targets = _benchmark_targets(panel) if include_benchmark else {}
    execution_dates: dict[pd.Timestamp, pd.Timestamp] = {}
    for signal in signals:
        if signal not in store.sessions:
            raise PITStockContractError("signal is absent from official sessions")
        index = store.sessions.index(signal)
        if index + 1 >= len(store.sessions):
            raise PITStockContractError("signal lacks a proven next session")
        execution_dates[signal] = store.sessions[index + 1]
    start, end = execution_dates[signals[0]], execution_dates[signals[-1]]
    if end > store.maximum_read_date:
        raise PITStockContractError("real-share simulation would read after cutoff")
    sessions = tuple(
        value for value in store.market_sessions if start <= value <= end
    )
    expected_sessions = tuple(
        value for value in store.sessions if start <= value <= end
    )
    if sessions != expected_sessions or not sessions or sessions[0] != start or sessions[-1] != end:
        raise PITStockContractError("real-share market sessions are truncated")
    actions = prepare_real_share_actions(resolved_actions, store.sessions)
    action_by_record: dict[str, list[RealShareAction]] = {}
    action_by_ex: dict[str, list[RealShareAction]] = {}
    action_by_due: dict[str, list[RealShareAction]] = {}
    for action in actions:
        action_by_record.setdefault(action.record_date, []).append(action)
        action_by_ex.setdefault(action.ex_date, []).append(action)
        for due in {action.pay_session, action.list_session} - {None}:
            action_by_due.setdefault(str(due), []).append(action)
    rejected_action_by_ex: dict[str, set[str]] = {}
    rejected_event_by_ex: dict[str, list[tuple[str, str]]] = {}
    rejected_record_by_date: dict[str, list[tuple[str, str]]] = {}
    if tushare_reference_diagnostics is not None:
        required_diagnostic = {
            "action_id",
            "ticker",
            "record_date",
            "ex_date",
            "fallback_eligible",
            "status",
        }
        if not required_diagnostic.issubset(tushare_reference_diagnostics.columns):
            raise PITStockContractError(
                "Tushare reference diagnostics lack required columns"
            )
        rejected_statuses = {
            "ambiguous_multiple_tushare_actions",
            "invalid_event_session",
            "missing_adjacent_raw_bars",
            "invalid_raw_reference",
            "raw_reference_mismatch",
        }
        rejected = tushare_reference_diagnostics.loc[
            tushare_reference_diagnostics["fallback_eligible"].ne(True)
            & tushare_reference_diagnostics["status"].isin(rejected_statuses)
        ]
        for row in rejected[["action_id", "ticker", "record_date", "ex_date"]].itertuples(
            index=False
        ):
            rejected_action_by_ex.setdefault(str(row.ex_date), set()).add(
                str(row.ticker)
            )
            rejected_event_by_ex.setdefault(str(row.ex_date), []).append(
                (str(row.action_id), str(row.ticker))
            )
            rejected_record_by_date.setdefault(str(row.record_date), []).append(
                (str(row.action_id), str(row.ticker))
            )
    event_start = pd.to_datetime(
        suspension_events["date"], errors="raise"
    ).dt.normalize().min()
    projection_sessions = tuple(
        value
        for value in store.market_sessions
        if event_start <= value <= end
    )
    projection = SuspensionProjection(
        suspension_events, store=store, official_sessions=projection_sessions
    )
    normalized_suspensions = suspension_events.copy()
    normalized_suspensions["date"] = pd.to_datetime(
        normalized_suspensions["date"], errors="raise"
    ).dt.normalize()
    resume_open_blocks: dict[pd.Timestamp, set[str]] = {}
    resumes = normalized_suspensions.loc[
        normalized_suspensions["suspend_type"].astype(str).eq("R")
    ]
    for row in resumes[["ticker", "date", "suspend_timing"]].itertuples(
        index=False
    ):
        timing = "" if pd.isna(row.suspend_timing) else str(row.suspend_timing).strip()
        digits = "".join(character for character in timing if character.isdigit())
        proven_by_open = len(digits) >= 4 and digits[:4] <= "0930"
        if not proven_by_open:
            resume_open_blocks.setdefault(pd.Timestamp(row.date), set()).add(
                str(row.ticker)
            )
    for warm in projection_sessions:
        if warm >= start:
            break
        raw = store.read_market(warm)
        projection.advance(
            warm, observed_tickers=set(raw["ts_code"].astype(str))
        )
    master = store.security_master.copy()
    master["delist_date"] = pd.to_datetime(
        master["delist_date"], errors="coerce"
    ).dt.normalize()
    session_array = np.asarray(store.sessions, dtype="datetime64[ns]")
    delists: dict[pd.Timestamp, set[str]] = {}
    for row in master.loc[master["delist_date"].notna()].itertuples(
        index=False
    ):
        index = int(
            np.searchsorted(
                session_array, np.datetime64(row.delist_date), side="left"
            )
        )
        if index < len(store.sessions):
            delists.setdefault(store.sessions[index], set()).add(str(row.ts_code))
    policies: dict[str, RealSharePolicy] = {
        "candidate_base": RealSharePolicy(
            max_adv_participation=0.05,
            max_position_weight=0.0125,
            costs=AShareCostPolicy(),
        ),
        "candidate_stress": RealSharePolicy(
            max_adv_participation=0.05,
            max_position_weight=0.0125,
            costs=AShareCostPolicy(slippage_bps_per_side=10.0),
        ),
    }
    role_targets: dict[str, dict[pd.Timestamp, dict[str, float]]] = {
        "candidate_base": targets,
        "candidate_stress": targets,
    }
    if include_benchmark:
        policies.update(
            {
                "adv500_base": RealSharePolicy(
                    max_adv_participation=0.05,
                    max_position_weight=1.0 / 500.0,
                    costs=AShareCostPolicy(),
                ),
                "adv500_stress": RealSharePolicy(
                    max_adv_participation=0.05,
                    max_position_weight=1.0 / 500.0,
                    costs=AShareCostPolicy(slippage_bps_per_side=10.0),
                ),
            }
        )
        role_targets.update(
            {
                "adv500_base": benchmark_targets,
                "adv500_stress": benchmark_targets,
            }
        )
    accounts = {role: RealShareAccount(initial_capital) for role in policies}
    rejected_record_entitlements: dict[str, dict[str, int]] = {
        role: {} for role in policies
    }
    for account in accounts.values():
        for action in actions:
            if _date(action.record_date) < start:
                account.record_entitlements[action.action_id] = 0
                account.applied_stages.add((action.action_id, "record"))
    for role in policies:
        for record_date, rows in rejected_record_by_date.items():
            if _date(record_date) < start:
                for action_id, _ in rows:
                    rejected_record_entitlements[role][action_id] = 0
    signal_by_execution = {
        date: signal for signal, date in execution_dates.items()
    }
    snapshots = {
        signal: frame.copy()
        for signal, frame in panel.groupby("signal_date", sort=True)
    }
    postings = {role: [] for role in policies}
    ignored_factor_drift_count = {role: 0 for role in policies}
    current_period = {role: None for role in policies}
    daily_rows: list[dict[str, Any]] = []
    boundary_rows: list[dict[str, Any]] = []
    order_rows: list[dict[str, Any]] = []
    period_rows: list[dict[str, Any]] = []
    ticker_rows: list[dict[str, Any]] = []
    group_rows: list[dict[str, Any]] = []
    minute_provider_calls: list[dict[str, Any]] = []

    def finish_role_day(
        *,
        role: str,
        account: RealShareAccount,
        session: pd.Timestamp,
        signal: pd.Timestamp | None,
        market: pd.DataFrame,
        suspended: set[str],
        raw_tickers: set[str],
        day_start_nav: float,
        posting_start: int,
    ) -> None:
        final_boundary = signal == signals[-1]
        if not final_boundary:
            new_signal = (
                None
                if current_period[role] is None
                else str(current_period[role]["signal_date"])
            )
            mark_owned_shares(
                account,
                market,
                session=session,
                price_column="close",
                suspended_tickers=suspended - raw_tickers,
                delisted_tickers=delists.get(session, set()),
                postings=postings[role],
                period_signal=new_signal,
                phase="new_period_close",
            )
            capture_record_close(
                account,
                action_by_record.get(session.date().isoformat(), []),
                session=session,
            )
            for action_id, ticker in rejected_record_by_date.get(
                session.date().isoformat(), []
            ):
                rejected_record_entitlements[role][action_id] = (
                    account.positions.get(ticker, 0)
                )
        end_nav = account.nav()
        day_pnl = fsum(
            posting.amount for posting in postings[role][posting_start:]
        )
        error = end_nav - day_start_nav - day_pnl
        if not isfinite(error) or abs(error) > RECONCILIATION_TOLERANCE:
            raise PITStockContractError(
                f"daily real-share P&L does not reconcile: {role} {session.date()}"
            )
        if not isfinite(end_nav) or end_nav <= 0.0:
            raise PITStockContractError("real-share NAV is not positive/finite")
        gross_stock_value = fsum(
            account.owned_shares(ticker) * account.marks[ticker].price
            for ticker in account.required_tickers()
            if ticker in account.marks
        )
        daily_rows.append(
            {
                "role": role,
                "trade_date": session,
                "nav": end_nav,
                "net_return": end_nav / day_start_nav - 1.0,
                "cash": account.cash,
                "gross_stock_exposure": gross_stock_value / end_nav,
                "position_count": len(account.positions),
                "pending_lot_count": len(account.pending_shares),
                "receivable_count": len(account.receivables),
                "reconciliation_error": error,
                "mark_kind": "terminal_open" if final_boundary else "close",
            }
        )

    previous_raw: pd.DataFrame | None = None
    for session in sessions:
        raw = store.read_market(session)
        raw_index = raw.set_index("ts_code")
        session_resume_open_blocks = resume_open_blocks.get(session, set())
        suspended = projection.advance(
            session, observed_tickers=set(raw_index.index.astype(str))
        ) | session_resume_open_blocks
        signal = signal_by_execution.get(session)
        snapshot = snapshots.get(signal) if signal is not None else None
        required = set().union(
            *(account.required_tickers() for account in accounts.values())
        )
        if signal is not None and signal != signals[-1]:
            for target_map in role_targets.values():
                required |= set(target_map[signal])
        ex_actions = action_by_ex.get(session.date().isoformat(), [])
        entitlement_tickers_by_role: dict[str, set[str]] = {}
        deferred_minute_states: dict[str, Any] = {}
        deferred_day_state: dict[str, tuple[float, int]] = {}
        for role, account in accounts.items():
            entitlement_tickers = {
                action.ticker
                for action in ex_actions
                if account.record_entitlements.get(action.action_id, 0) > 0
            }
            entitlement_tickers_by_role[role] = entitlement_tickers
            required |= entitlement_tickers
        market = _real_market(
            raw,
            required=required,
            suspended=suspended,
            delisted=delists.get(session, set()),
            signal_snapshot=snapshot,
            session=session,
        )
        minute_anchor_rows: pd.DataFrame | None = None
        minute_no_anchor: set[str] = set()
        intraday_resume_carry: set[str] = set()
        if (
            execution_mode == "sequential_minute"
            and signal is not None
        ):
            assert minute_market_provider is not None and snapshot is not None
            held_at_boundary = set().union(
                *(account.required_tickers() for account in accounts.values())
            ) | set().union(*entitlement_tickers_by_role.values())
            anchor_result = minute_market_provider.build_auction_anchors(
                signal_date=signal,
                execution_date=session,
                required_tickers=set(held_at_boundary),
            )
            minute_provider_calls.append(
                {
                    "signal_date": signal.date().isoformat(),
                    "execution_date": session.date().isoformat(),
                    "role": "all_roles_boundary_proof",
                    "phase": "09:30_anchor",
                    "ticker_count": len(held_at_boundary),
                    "ticker_payload_sha256": canonical_sha256(
                        sorted(held_at_boundary)
                    ),
                }
            )
            if (
                not isinstance(anchor_result, tuple)
                or len(anchor_result) != 2
                or not isinstance(anchor_result[0], pd.DataFrame)
            ):
                raise PITStockContractError(
                    "sequential minute auction provider result differs"
                )
            minute_anchor_rows = anchor_result[0]
            minute_no_anchor = {str(value) for value in anchor_result[1]}
            auction_columns = (
                "ticker",
                "trade_time",
                "observable_at",
                "open",
                "zero_liquidity_flat_price",
            )
            if tuple(map(str, minute_anchor_rows.columns)) != auction_columns:
                raise PITStockContractError(
                    "sequential minute auction columns differ"
                )
            anchor_tickers = minute_anchor_rows["ticker"].astype(str)
            if (
                anchor_tickers.duplicated().any()
                or set(anchor_tickers) & minute_no_anchor
                or set(anchor_tickers) | minute_no_anchor != held_at_boundary
            ):
                raise PITStockContractError(
                    "sequential minute auction scope differs"
                )
            minute_index = minute_anchor_rows.set_index("ticker")
            for ticker in sorted(held_at_boundary):
                if ticker in minute_no_anchor:
                    if ticker in raw_index.index:
                        if ticker not in session_resume_open_blocks:
                            raise PITStockContractError(
                                "held missing auction anchor with raw bar lacks intraday-resume proof"
                            )
                    elif ticker not in (
                        suspended | delists.get(session, set())
                    ):
                        raise PITStockContractError(
                            "held missing auction anchor lacks matching suspend/delist proof"
                        )
                    continue
                auction_open = float(minute_index.at[ticker, "open"])
                if ticker not in raw_index.index:
                    if isfinite(auction_open) or ticker not in (
                        suspended | delists.get(session, set())
                    ):
                        raise PITStockContractError(
                            "held missing raw bar lacks matching suspend/delist proof"
                        )
                    continue
                if (
                    pd.Timestamp(minute_index.at[ticker, "trade_time"])
                    != pd.Timestamp(f"{session.date()} 09:30:00")
                    or pd.Timestamp(minute_index.at[ticker, "observable_at"])
                    != pd.Timestamp(f"{session.date()} 09:31:00")
                ):
                    raise PITStockContractError(
                        "held minute auction time/observability differs"
                    )
                daily_open = float(raw_index.at[ticker, "open"])
                daily_pre_close = float(raw_index.at[ticker, "pre_close"])
                zero_liquidity_flat_price = bool(
                    minute_index.at[ticker, "zero_liquidity_flat_price"]
                )
                if not _auction_anchor_matches_daily_open(
                    auction_open=auction_open,
                    daily_open=daily_open,
                    daily_pre_close=daily_pre_close,
                    zero_liquidity_flat_price=zero_liquidity_flat_price,
                ):
                    raise PITStockContractError(
                        "held minute auction anchor differs from raw daily open "
                        "without a zero-liquidity pre-close placeholder proof: "
                        f"session={session.date()} ticker={ticker} "
                        f"minute_auction_open={auction_open:.12g} "
                        f"raw_daily_open={daily_open:.12g} "
                        f"raw_pre_close={daily_pre_close:.12g} "
                        "zero_liquidity_flat_price="
                        f"{zero_liquidity_flat_price}"
                    )
            intraday_resume_carry = _intraday_resume_carry_tickers(
                minute_no_anchor=minute_no_anchor,
                session_resume_open_blocks=session_resume_open_blocks,
            )
        for role, account in accounts.items():
            day_start_nav = account.nav()
            posting_start = len(postings[role])
            old_signal = (
                None
                if current_period[role] is None
                else str(current_period[role]["signal_date"])
            )
            entitlement_tickers = entitlement_tickers_by_role[role]
            rejected_today = rejected_action_by_ex.get(
                session.date().isoformat(), set()
            )
            rejected_entitled = {
                ticker
                for action_id, ticker in rejected_event_by_ex.get(
                    session.date().isoformat(), []
                )
                if rejected_record_entitlements[role].get(action_id, 0) > 0
            }
            rejected_exposure = rejected_today & (
                account.required_tickers() | rejected_entitled
            )
            if rejected_exposure:
                raise PITStockContractError(
                    "held unresolved Tushare diagnostic action: "
                    f"{sorted(rejected_exposure)[:10]}"
                )
            mark_owned_shares(
                account,
                market,
                session=session,
                price_column="open",
                suspended_tickers=(
                    (
                        suspended - set(raw_index.index.astype(str))
                    )
                    | intraday_resume_carry
                    if execution_mode == "sequential_minute"
                    else suspended
                ),
                delisted_tickers=delists.get(session, set()),
                postings=postings[role],
                period_signal=old_signal,
                phase="old_period_open",
                additional_tickers=entitlement_tickers,
            )
            if previous_raw is not None:
                previous_index = previous_raw.set_index("ts_code")
                action_map: dict[str, list[RealShareAction]] = {}
                for action in ex_actions:
                    action_map.setdefault(action.ticker, []).append(action)
                economic_tickers = account.required_tickers() | entitlement_tickers
                for ticker in sorted(economic_tickers):
                    if ticker not in raw_index.index or ticker not in previous_index.index:
                        if ticker in action_map:
                            raise PITStockContractError(
                                "held ex-date action lacks adjacent raw bars"
                            )
                        continue
                    factor_ratio = float(raw_index.at[ticker, "adj_factor"]) / float(
                        previous_index.at[ticker, "adj_factor"]
                    )
                    previous_close = float(previous_index.at[ticker, "close"])
                    reference = float(raw_index.at[ticker, "pre_close"])
                    if not all(
                        isfinite(value) and value > 0.0
                        for value in (factor_ratio, previous_close, reference)
                    ):
                        raise PITStockContractError(
                            f"held raw action reference is invalid: {ticker} {session.date()}"
                        )
                    price_ratio = previous_close / reference
                    ticker_actions = action_map.get(ticker, [])
                    if not ticker_actions:
                        if (
                            abs(price_ratio - 1.0) > 1e-12
                            or abs(factor_ratio - price_ratio) > 0.0011
                        ):
                            raise PITStockContractError(
                                f"held raw-reference jump lacks action: {ticker} {session.date()}"
                            )
                        if abs(factor_ratio - 1.0) > 1e-12:
                            ignored_factor_drift_count[role] += 1
                    if ticker_actions:
                        cash = fsum(
                            action.cash_dividend_before_tax_per_share
                            for action in ticker_actions
                        )
                        stock = fsum(
                            action.stock_dividend_per_share
                            for action in ticker_actions
                        )
                        theoretical = (previous_close - cash) / (1.0 + stock)
                        if (
                            abs(theoretical / reference - 1.0) > 0.01
                            or abs(
                                factor_ratio - previous_close / reference
                            )
                            > 0.0011
                        ):
                            raise PITStockContractError(
                                f"held action reference mismatch: {ticker} {session.date()}"
                            )
            accrue_ex_open(
                account,
                ex_actions,
                session=session,
                cash_withholding_rate=0.20,
                postings=postings[role],
                period_signal=old_signal,
            )
            settle_pay_and_list_open(
                account,
                action_by_due.get(session.date().isoformat(), []),
                session=session,
            )
            write_down_delists(
                account,
                delists.get(session, set()),
                session=session,
                postings=postings[role],
                period_signal=old_signal,
            )
            pretrade_nav = account.nav()
            if signal is not None and current_period[role] is not None:
                _finalize_period(
                    role=role,
                    period=current_period[role],
                    end_date=session,
                    end_nav=pretrade_nav,
                    postings=postings[role],
                    period_rows=period_rows,
                    ticker_rows=ticker_rows,
                    group_rows=group_rows,
                )
            if signal is not None:
                boundary_target = role_targets[role][signal]
                boundary_target_sha = canonical_sha256(
                    [
                        {"ticker": ticker, "target_weight": float(weight)}
                        for ticker, weight in sorted(boundary_target.items())
                    ]
                )
                if (
                    role.startswith("candidate_")
                    and boundary_target_sha != decisions[signal]["target_sha256"]
                ):
                    raise PITStockContractError(
                        "candidate boundary target hash differs"
                    )
                boundary_rows.append(
                    {
                        "role": role,
                        "signal_date": signal,
                        "execution_date": session,
                        "pretrade_nav": pretrade_nav,
                        "execution_mode": execution_mode,
                        "target_kind": (
                            "candidate" if role.startswith("candidate_") else "adv500"
                        ),
                        "target_sha256": boundary_target_sha,
                    }
                )
                if signal == signals[-1]:
                    current_period[role] = None
                else:
                    groups, state = _groups(snapshot)
                    current_period[role] = {
                        "signal_date": signal.date().isoformat(),
                        "execution_date": session.date().isoformat(),
                        "start_nav": pretrade_nav,
                        "groups": groups,
                        "market_state": state,
                        "start_weights": {},
                    }
                    if execution_mode == "sequential_minute":
                        assert minute_market_provider is not None
                        role_required = account.required_tickers() | set(
                            role_targets[role][signal]
                        )
                        minute_context = minute_market_provider.build_context(
                            signal_date=signal,
                            execution_date=session,
                            required_tickers=set(role_required),
                            signal_snapshot=snapshot.copy(),
                        )
                        if (
                            not isinstance(minute_context, pd.DataFrame)
                            or tuple(map(str, minute_context.columns))
                            != MINUTE_EXECUTION_CONTEXT_COLUMNS
                            or minute_context["ticker"].astype(str).duplicated().any()
                            or set(minute_context["ticker"].astype(str))
                            != role_required
                        ):
                            raise PITStockContractError(
                                "sequential minute provider context differs"
                            )
                        minute_provider_calls.append(
                            {
                                "signal_date": signal.date().isoformat(),
                                "execution_date": session.date().isoformat(),
                                "role": role,
                                "phase": "context",
                                "ticker_count": len(role_required),
                                "ticker_payload_sha256": canonical_sha256(
                                    sorted(role_required)
                                ),
                            }
                        )
                        deferred_minute_states[role] = (
                            begin_sequential_minute_rebalance(
                                account,
                                role_targets[role][signal],
                                minute_context,
                                trade_date=session,
                                policy=SequentialMinutePolicy(policies[role]),
                                postings=postings[role],
                                period_signal=signal.date().isoformat(),
                                raw_open_carried_tickers=(
                                    account.required_tickers()
                                    - set(raw_index.index.astype(str))
                                )
                                | (
                                    intraday_resume_carry
                                    & account.required_tickers()
                                ),
                            )
                        )
                        deferred_day_state[role] = (
                            day_start_nav,
                            posting_start,
                        )
                        continue
                    else:
                        execution = execute_real_share_rebalance(
                            account,
                            role_targets[role][signal],
                            trade_date=session,
                            market=market,
                            policy=policies[role],
                            postings=postings[role],
                            period_signal=signal.date().isoformat(),
                        )
                    for order in execution.orders:
                        order_value = order.to_dict()
                        if execution_mode == "sequential_minute":
                            order_value["capacity_limited"] = bool(
                                order.signal_adv_limited
                                or order.window_capacity_limited
                            )
                        else:
                            order_value["filled_decision_notional"] = float(
                                order.executed_notional
                            )
                        order_value["execution_mode"] = execution_mode
                        order_rows.append(
                            {
                                "role": role,
                                "signal_date": signal,
                                **order_value,
                            }
                        )
                    start_weight_nav = (
                        execution.posttrade_nav
                        if execution_mode == "sequential_minute"
                        else pretrade_nav
                    )
                    for ticker in account.required_tickers():
                        mark = account.marks.get(ticker)
                        if mark is not None:
                            current_period[role]["start_weights"][ticker] = (
                                account.owned_shares(ticker)
                                * mark.price
                                / start_weight_nav
                            )
            finish_role_day(
                role=role,
                account=account,
                session=session,
                signal=signal,
                market=market,
                suspended=suspended,
                raw_tickers=set(raw_index.index.astype(str)),
                day_start_nav=day_start_nav,
                posting_start=posting_start,
            )
        if deferred_minute_states:
            if (
                execution_mode != "sequential_minute"
                or signal is None
                or signal == signals[-1]
                or set(deferred_minute_states) != set(policies)
            ):
                raise PITStockContractError(
                    "sequential minute deferred role scope differs"
                )
            assert minute_market_provider is not None
            executions: dict[str, Any] = {}
            for window_name, observer in (
                ("A", observe_window_a),
                ("B", observe_window_b),
                ("C", observe_window_c),
            ):
                role_scopes = {
                    role: set(state.required_bar_tickers)
                    for role, state in deferred_minute_states.items()
                }
                union_scope = set().union(*role_scopes.values())
                window_result = minute_market_provider.build_window(
                    signal_date=signal,
                    execution_date=session,
                    required_tickers=set(union_scope),
                    window=window_name,
                )
                if (
                    not isinstance(window_result, tuple)
                    or len(window_result) != 2
                    or not isinstance(window_result[0], pd.DataFrame)
                    or tuple(map(str, window_result[0].columns))
                    != MINUTE_EXECUTION_BAR_COLUMNS
                ):
                    raise PITStockContractError(
                        "sequential minute provider window differs"
                    )
                union_bars = window_result[0]
                union_no_bar = {str(value) for value in window_result[1]}
                observed_tickers = set(union_bars["ticker"].astype(str))
                if (
                    union_bars["ticker"].astype(str).duplicated().any()
                    or observed_tickers & union_no_bar
                    or observed_tickers | union_no_bar != union_scope
                ):
                    raise PITStockContractError(
                        "sequential minute provider window scope differs"
                    )
                minute_provider_calls.append(
                    {
                        "signal_date": signal.date().isoformat(),
                        "execution_date": session.date().isoformat(),
                        "role": "all_roles",
                        "phase": window_name,
                        "ticker_count": len(union_scope),
                        "ticker_payload_sha256": canonical_sha256(
                            sorted(union_scope)
                        ),
                    }
                )
                next_states: dict[str, Any] = {}
                for role in policies:
                    role_scope = role_scopes[role]
                    role_bars = union_bars.loc[
                        union_bars["ticker"].astype(str).isin(role_scope)
                    ].reset_index(drop=True)
                    observed = observer(
                        deferred_minute_states[role],
                        accounts[role],
                        role_bars,
                        postings=postings[role],
                        complete_no_bar_tickers=union_no_bar & role_scope,
                    )
                    if window_name == "C":
                        executions[role] = observed
                    else:
                        next_states[role] = observed
                if window_name != "C":
                    deferred_minute_states = next_states
            if set(executions) != set(policies):
                raise PITStockContractError(
                    "sequential minute all-role execution did not complete"
                )
            for role in policies:
                execution = executions[role]
                account = accounts[role]
                for order in execution.orders:
                    order_value = order.to_dict()
                    order_value["capacity_limited"] = bool(
                        order.signal_adv_limited
                        or order.window_capacity_limited
                    )
                    order_value["execution_mode"] = execution_mode
                    order_rows.append(
                        {
                            "role": role,
                            "signal_date": signal,
                            **order_value,
                        }
                    )
                for ticker in account.required_tickers():
                    mark = account.marks.get(ticker)
                    if mark is not None:
                        current_period[role]["start_weights"][ticker] = (
                            account.owned_shares(ticker)
                            * mark.price
                            / execution.posttrade_nav
                        )
                day_start_nav, posting_start = deferred_day_state[role]
                finish_role_day(
                    role=role,
                    account=account,
                    session=session,
                    signal=signal,
                    market=market,
                    suspended=suspended,
                    raw_tickers=set(raw_index.index.astype(str)),
                    day_start_nav=day_start_nav,
                    posting_start=posting_start,
                )
        previous_raw = raw
        if signal == signals[-1]:
            break
    future_input_violations = (
        _minute_provider_trace_violations(
            minute_provider_calls,
            signals=signals,
            roles=tuple(policies),
        )
        if execution_mode == "sequential_minute"
        else []
    )
    if future_input_violations:
        raise PITStockContractError("; ".join(future_input_violations))
    future_input_violation_count = len(future_input_violations)
    daily = pd.DataFrame(daily_rows)
    periods = pd.DataFrame(period_rows)
    boundaries = pd.DataFrame(boundary_rows)
    for role in policies:
        role_daily = daily.loc[daily["role"].eq(role)]
        role_boundaries = boundaries.loc[boundaries["role"].eq(role)]
        role_periods = periods.loc[periods["role"].eq(role)]
        if len(role_daily) != len(sessions):
            raise PITStockContractError("real-share daily path is truncated")
        if len(role_boundaries) != len(signals):
            raise PITStockContractError("real-share boundaries are truncated")
        if len(role_periods) != len(signals) - 1:
            raise PITStockContractError("real-share periods are truncated")
        actual_signals = tuple(role_boundaries["signal_date"].tolist())
        if actual_signals != signals:
            raise PITStockContractError("real-share boundary signals differ")
        if any(posting.period_signal is None for posting in postings[role]):
            raise PITStockContractError("investment P&L lacks a period signal")
    order_columns = (
        [
            "role",
            "signal_date",
            *MinuteRealShareOrder.__dataclass_fields__,
            "capacity_limited",
            "execution_mode",
        ]
        if execution_mode == "sequential_minute"
        else [
            "role",
            "signal_date",
            *RealShareOrder.__dataclass_fields__,
            "filled_decision_notional",
            "execution_mode",
        ]
    )
    orders_frame = pd.DataFrame(order_rows, columns=order_columns)
    metrics = {
        role: _role_metrics(
            daily.loc[daily["role"].eq(role)].reset_index(drop=True),
            periods.loc[periods["role"].eq(role)].reset_index(drop=True),
            orders_frame.loc[orders_frame["role"].eq(role)].reset_index(
                drop=True
            ),
            initial_nav=initial_capital,
        )
        for role in policies
    }
    for role in policies:
        metrics[role]["ignored_subthreshold_factor_drift_count"] = (
            ignored_factor_drift_count[role]
        )
    ticker_frame = pd.DataFrame(
        ticker_rows,
        columns=[
            "role",
            "signal_date",
            "ticker",
            "net_pnl",
            "start_weight",
            "industry",
            "size_bucket",
            "market_state",
        ],
    )
    group_frame = pd.DataFrame(
        group_rows,
        columns=[
            "role",
            "signal_date",
            "dimension",
            "group",
            "net_pnl",
            "start_weight",
        ],
    )
    metrics["phase_gate"] = _phase_gate_metrics(
        role_metrics=metrics,
        periods=periods,
        ticker_pnl=ticker_frame,
        group_pnl=group_frame,
        orders=orders_frame,
        candidate_targets=targets,
        include_benchmark=include_benchmark,
        future_input_violation_count=future_input_violation_count,
    )
    metrics["minute_provider_protocol"] = {
        "enabled": execution_mode == "sequential_minute",
        "future_input_violation_count": future_input_violation_count,
        "call_count": len(minute_provider_calls),
        "calls": minute_provider_calls,
    }
    posting_frames = []
    for role in policies:
        frame = _posting_frame(postings[role])
        frame["role"] = role
        posting_frames.append(frame)
    return RealShareSimulationBuild(
        daily_nav=daily,
        boundaries=boundaries,
        orders=orders_frame,
        postings=pd.concat(posting_frames, ignore_index=True),
        periods=periods,
        period_ticker_pnl=ticker_frame,
        group_pnl=group_frame,
        metrics=metrics,
        target_payload_sha256=target_digest,
    )


__all__ = [
    "RealShareSimulationBuild",
    "simulate_candidate_real_share_accounts",
]

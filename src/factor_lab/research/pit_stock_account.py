"""Continuous adjusted-total-return screening account for the 12.0 route.

This is deliberately a *screening* execution layer.  It uses continuous
synthetic total-return units (lot_size=0); it must not be presented as the
later raw-price, 100-share production gate.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from math import isfinite, sqrt
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from factor_lab.data.pit_stock import PITStockRawStore
from factor_lab.data.suspensions import audit_suspensions_snapshot
from factor_lab.portfolio.execution import (
    AShareCostPolicy,
    ExecutionAccount,
    ExecutionColumns,
    ExecutionPolicy,
    execute_rebalance,
    process_account_observation,
)
from factor_lab.research.pit_stock import (
    PITStockContractError,
    PITStockStrategyConfig,
    annualized_metrics,
    canonical_sha256,
    select_quarterly_targets,
)


@dataclass(frozen=True)
class PITStockScreeningAccountConfig:
    initial_capital_rmb: float = 10_000_000.0
    max_adv_participation: float = 0.05
    candidate_position_count: int = 80
    benchmark_position_count: int = 500
    base_slippage_bps_per_side: float = 5.0
    stress_slippage_bps_per_side: float = 10.0

    def __post_init__(self) -> None:
        values = (
            self.initial_capital_rmb,
            self.max_adv_participation,
            self.base_slippage_bps_per_side,
            self.stress_slippage_bps_per_side,
        )
        if not all(isfinite(float(value)) for value in values):
            raise ValueError("screening account settings must be finite")
        if self.initial_capital_rmb <= 0 or not 0 < self.max_adv_participation <= 1:
            raise ValueError("capital/ADV participation is invalid")
        if self.candidate_position_count <= 0 or self.benchmark_position_count <= 0:
            raise ValueError("position counts must be positive")
        if self.base_slippage_bps_per_side < 0 or self.stress_slippage_bps_per_side < 0:
            raise ValueError("slippage cannot be negative")


@dataclass(frozen=True)
class PITStockAccountBuild:
    daily_nav: pd.DataFrame
    boundaries: pd.DataFrame
    orders: pd.DataFrame
    holdings: pd.DataFrame
    metrics: dict[str, Any]
    target_payloads: dict[str, str]
    source_receipt: dict[str, Any]


class SuspensionProjection:
    """Causal open-state projection over the attested suspend_d event stream."""

    def __init__(
        self,
        events: pd.DataFrame,
        *,
        store: PITStockRawStore,
        official_sessions: Sequence[pd.Timestamp],
    ) -> None:
        required = {"ticker", "date", "suspend_type", "suspend_timing"}
        if set(events.columns) != required:
            raise PITStockContractError("suspend_d snapshot schema differs")
        work = events.copy()
        work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.normalize()
        if work["date"].isna().any() or not work["suspend_type"].isin(["S", "R"]).all():
            raise PITStockContractError("suspend_d contains invalid date/type")
        work["ticker"] = [
            store.canonical_ticker(str(ticker), date)
            for ticker, date in zip(work["ticker"], work["date"])
        ]
        self._events = {
            pd.Timestamp(date): group.sort_values(
                ["ticker", "suspend_type", "suspend_timing"], kind="mergesort"
            ).reset_index(drop=True)
            for date, group in work.groupby("date", sort=True)
        }
        self._sessions = tuple(pd.Timestamp(value).normalize() for value in official_sessions)
        self._index = {value: index for index, value in enumerate(self._sessions)}
        self._last_index: int | None = None
        self._carried: set[str] = set()

    @staticmethod
    def _blocks_open(value: Any) -> bool:
        if value is None or pd.isna(value) or not str(value).strip():
            return True
        text = str(value).strip().split(",", 1)[0].split("-", 1)[0].strip()
        try:
            parsed = pd.Timestamp(f"2000-01-01 {text}")
        except (TypeError, ValueError) as exc:
            raise PITStockContractError(f"invalid suspend_timing: {value!r}") from exc
        return (parsed.hour, parsed.minute, parsed.second) <= (9, 30, 0)

    def advance(
        self, date: pd.Timestamp, *, observed_tickers: set[str]
    ) -> set[str]:
        value = pd.Timestamp(date).normalize()
        if value not in self._index:
            raise PITStockContractError("suspension date is not an official session")
        index = self._index[value]
        if self._last_index is not None and index != self._last_index + 1:
            raise PITStockContractError("suspension projection has a calendar gap")
        rows = self._events.get(value, pd.DataFrame(columns=["ticker", "suspend_type", "suspend_timing"]))
        resumed = set(rows.loc[rows["suspend_type"].eq("R"), "ticker"].astype(str))
        suspended_rows = rows.loc[rows["suspend_type"].eq("S")]
        blocking = {
            str(row.ticker)
            for row in suspended_rows[["ticker", "suspend_timing"]].itertuples(index=False)
            if self._blocks_open(row.suspend_timing)
        }
        timing = suspended_rows["suspend_timing"].astype("string").str.strip()
        carryable = set(
            suspended_rows.loc[timing.isna() | timing.eq(""), "ticker"].astype(str)
        )
        at_open = blocking | (self._carried - resumed - observed_tickers)
        self._carried = (self._carried | carryable) - observed_tickers - resumed
        self._last_index = index
        return at_open


def _load_suspensions(
    project_root: Path, *, start: pd.Timestamp, end: pd.Timestamp
) -> tuple[pd.DataFrame, dict[str, Any]]:
    path = project_root / "runtime" / "data" / "top500" / "suspensions.parquet"
    metadata = path.with_name("suspensions.meta.json")
    receipt = audit_suspensions_snapshot(
        path,
        metadata,
        requested_start=str(start.date()),
        requested_end=str(end.date()),
    )
    frame = pd.read_parquet(path)
    return frame, receipt


def _target_payload(targets: Mapping[pd.Timestamp, Mapping[str, float]]) -> str:
    rows = [
        {
            "signal_date": str(signal.date()),
            "ticker": ticker,
            "target_weight": float(weight),
        }
        for signal, target in sorted(targets.items())
        for ticker, weight in sorted(target.items())
    ]
    return canonical_sha256(rows)


def _candidate_targets(
    panel: pd.DataFrame,
    stored_targets: pd.DataFrame,
) -> tuple[dict[pd.Timestamp, dict[str, float]], dict[pd.Timestamp, dict[str, Any]]]:
    work = panel.copy()
    work["signal_date"] = pd.to_datetime(work["signal_date"]).dt.normalize()
    stored = stored_targets.copy()
    stored["signal_date"] = pd.to_datetime(stored["signal_date"]).dt.normalize()
    result: dict[pd.Timestamp, dict[str, float]] = {}
    decisions: dict[pd.Timestamp, dict[str, Any]] = {}
    for signal, snapshot in work.groupby("signal_date", sort=True):
        selected, decision = select_quarterly_targets(snapshot)
        target = {
            str(row.ticker): float(row.target_weight)
            for row in selected[["ticker", "target_weight"]].itertuples(index=False)
        }
        actual = stored.loc[stored["signal_date"].eq(signal)]
        stored_target = {
            str(row.ticker): float(row.target_weight)
            for row in actual[["ticker", "target_weight"]].itertuples(index=False)
        }
        if target != stored_target:
            raise PITStockContractError(f"stored target differs at {signal.date()}")
        result[pd.Timestamp(signal)] = target
        decisions[pd.Timestamp(signal)] = decision.to_dict()
    return result, decisions


def _benchmark_targets(panel: pd.DataFrame) -> dict[pd.Timestamp, dict[str, float]]:
    work = panel.copy()
    work["signal_date"] = pd.to_datetime(work["signal_date"]).dt.normalize()
    result = {}
    for signal, snapshot in work.groupby("signal_date", sort=True):
        members = snapshot.loc[snapshot["universe_member"]].assign(
            _ticker=snapshot.loc[snapshot["universe_member"], "ticker"].astype(str)
        )
        selected = members.sort_values(
            ["adv20", "_ticker"], ascending=[False, True], kind="mergesort"
        ).head(500)
        if len(selected) != 500:
            raise PITStockContractError("ADV500 benchmark is incomplete")
        result[pd.Timestamp(signal)] = {
            str(ticker): 1.0 / 500.0 for ticker in selected["ticker"]
        }
    return result


def _cost_policy(slippage_bps: float) -> AShareCostPolicy:
    return AShareCostPolicy(slippage_bps_per_side=float(slippage_bps))


def _execution_market(
    raw: pd.DataFrame,
    *,
    required_tickers: set[str],
    signal_snapshot: pd.DataFrame | None,
    suspended_at_open: set[str],
    delisted_today: set[str],
    date: pd.Timestamp,
) -> pd.DataFrame:
    indexed = raw.rename(columns={"ts_code": "ticker"}).set_index("ticker")
    observed = set(indexed.index.astype(str))
    unexplained = sorted(
        required_tickers - observed - suspended_at_open - delisted_today
    )
    if unexplained:
        raise PITStockContractError(
            f"required holdings/targets lack bar, suspension, or delist proof on {date.date()}: {unexplained[:10]}"
        )
    rows = indexed.loc[indexed.index.intersection(required_tickers)].copy()
    rows["is_one_price_limit_up"] = rows["high"].eq(rows["low"]) & rows["open"].gt(rows["pre_close"])
    rows["is_one_price_limit_down"] = rows["high"].eq(rows["low"]) & rows["open"].lt(rows["pre_close"])
    rows["is_suspended"] = rows.index.isin(suspended_at_open)
    rows["is_delisted"] = False
    if signal_snapshot is None:
        rows["signal_adv20"] = 1.0
        rows["signal_vol_daily"] = 0.0
    else:
        signal = signal_snapshot.set_index("ticker")
        rows["signal_adv20"] = pd.to_numeric(signal["adv20"], errors="coerce").reindex(rows.index)
        rows["signal_vol_daily"] = pd.to_numeric(signal["vol63"], errors="coerce").reindex(rows.index) / sqrt(252.0)
    missing_rows = []
    for ticker in sorted(required_tickers - observed):
        missing_rows.append(
            {
                "ticker": ticker,
                "open_adj": np.nan,
                "close_adj": np.nan,
                "signal_adv20": np.nan,
                "signal_vol_daily": np.nan,
                "is_one_price_limit_up": False,
                "is_one_price_limit_down": False,
                "is_suspended": ticker in suspended_at_open,
                "is_delisted": ticker in delisted_today,
            }
        )
    rows = rows.reset_index()
    if missing_rows:
        rows = pd.concat([rows, pd.DataFrame(missing_rows)], ignore_index=True, sort=False)
    return rows.sort_values("ticker", kind="mergesort").reset_index(drop=True)


def _metrics(
    daily: pd.DataFrame,
    boundaries: pd.DataFrame,
    orders: pd.DataFrame,
    *,
    initial_capital: float,
) -> dict[str, Any]:
    values = daily["net_return"].to_numpy(dtype=float)
    if not np.isfinite(values).all() or np.any(values <= -1):
        raise PITStockContractError("daily account return is invalid")
    end_nav = float(daily.iloc[-1]["nav"])
    years = len(values) / 252.0
    cagr = (end_nav / initial_capital) ** (1.0 / years) - 1.0
    volatility = float(values.std(ddof=1) * sqrt(252.0)) if len(values) > 1 else 0.0
    nav = np.concatenate(([initial_capital], daily["nav"].to_numpy(dtype=float)))
    drawdown = nav / np.maximum.accumulate(nav) - 1.0
    boundary = boundaries.sort_values("execution_date", kind="mergesort")
    period_rows = []
    for index in range(len(boundary) - 1):
        current = boundary.iloc[index]
        following = boundary.iloc[index + 1]
        period_rows.append(
            {
                "signal_date": str(pd.Timestamp(current["signal_date"]).date()),
                "outcome_end": str(pd.Timestamp(following["execution_date"]).date()),
                "return": float(following["pretrade_nav"] / current["pretrade_nav"] - 1.0),
            }
        )
    train = [row["return"] for row in period_rows if row["signal_date"] < "2021-01-01"]
    validation = [row["return"] for row in period_rows if row["signal_date"] >= "2021-01-01"]
    requested = float(orders["requested_notional"].sum()) if not orders.empty else 0.0
    executed = float(orders["executed_notional"].sum()) if not orders.empty else 0.0
    limited = (
        float(orders.loc[orders["capacity_limited"], "requested_notional"].sum())
        if not orders.empty
        else 0.0
    )
    return {
        "observations": len(values),
        "initial_nav": initial_capital,
        "end_nav": end_nav,
        "compound_return": end_nav / initial_capital - 1.0,
        "cagr": cagr,
        "annual_volatility": volatility,
        "sharpe": float(values.mean() * 252.0 / volatility) if volatility else 0.0,
        "max_drawdown": float(drawdown.min()),
        "positive_day_fraction": float(np.mean(values > 0)),
        "train_quarterly": annualized_metrics(train),
        "validation_quarterly": annualized_metrics(validation),
        "requested_notional_fill_ratio": executed / requested if requested else 1.0,
        "capacity_limited_requested_notional_ratio": limited / requested if requested else 0.0,
        "blocked_order_count": int((orders["status"] == "blocked").sum()) if not orders.empty else 0,
        "capacity_violation_count": 0,
        "negative_cash_observation_count": int(daily["cash"].lt(-1e-6).sum()),
        "leverage_observation_count": int(daily["invested_weight"].gt(1 + 1e-9).sum()),
        "max_nav_reconciliation_error": float(daily["nav_reconciliation_error"].abs().max()),
        "period_trace": period_rows,
    }


def summarize_screening_account(
    daily: pd.DataFrame,
    boundaries: pd.DataFrame,
    orders: pd.DataFrame,
    *,
    initial_capital: float,
) -> dict[str, Any]:
    """Public deterministic verifier for one persisted screening role."""

    return _metrics(
        daily.copy(), boundaries.copy(), orders.copy(), initial_capital=initial_capital
    )


def simulate_screening_accounts(
    store: PITStockRawStore,
    panel: pd.DataFrame,
    stored_targets: pd.DataFrame,
    *,
    config: PITStockScreeningAccountConfig = PITStockScreeningAccountConfig(),
) -> PITStockAccountBuild:
    panel = panel.copy()
    panel["signal_date"] = pd.to_datetime(panel["signal_date"]).dt.normalize()
    signals = tuple(sorted(pd.Timestamp(value) for value in panel["signal_date"].unique()))
    if len(signals) < 2:
        raise PITStockContractError("screening account requires at least two signals")
    candidate, decisions = _candidate_targets(panel, stored_targets)
    benchmark = _benchmark_targets(panel)
    execution_dates = {
        signal: store.sessions[store.sessions.index(signal) + 1] for signal in signals
    }
    start_date, end_date = execution_dates[signals[0]], execution_dates[signals[-1]]
    if end_date > store.maximum_read_date:
        raise PITStockContractError("screening account would read after its cutoff")
    events, suspension_receipt = _load_suspensions(
        store.project_root, start=start_date, end=end_date
    )
    event_start = pd.to_datetime(events["date"], errors="raise").dt.normalize().min()
    projection_sessions = tuple(
        value for value in store.market_sessions if event_start <= value <= end_date
    )
    run_sessions = tuple(
        value for value in projection_sessions if start_date <= value <= end_date
    )
    projection = SuspensionProjection(
        events, store=store, official_sessions=projection_sessions
    )
    # Reconstruct carried suspension state causally from the attested event
    # stream and actually observed bars before the first account session.
    for warm_date in projection_sessions:
        if warm_date >= start_date:
            break
        warm_market = store.read_market(warm_date)
        projection.advance(
            warm_date, observed_tickers=set(warm_market["ts_code"].astype(str))
        )
    signal_by_execution = {date: signal for signal, date in execution_dates.items()}
    snapshot_by_signal = {
        signal: group.copy()
        for signal, group in panel.groupby("signal_date", sort=True)
    }
    roles = {
        "candidate_base": (candidate, config.candidate_position_count, config.base_slippage_bps_per_side),
        "candidate_stress": (candidate, config.candidate_position_count, config.stress_slippage_bps_per_side),
        "adv500_base": (benchmark, config.benchmark_position_count, config.base_slippage_bps_per_side),
        "adv500_stress": (benchmark, config.benchmark_position_count, config.stress_slippage_bps_per_side),
    }
    accounts = {
        role: ExecutionAccount(config.initial_capital_rmb) for role in roles
    }
    policies = {
        role: ExecutionPolicy(
            max_adv_participation=config.max_adv_participation,
            max_position_weight=1.0 / count,
            lot_size=0,
            costs=_cost_policy(slippage),
            max_stale_position_age_days=None,
        )
        for role, (_, count, slippage) in roles.items()
    }
    columns = ExecutionColumns(
        open="open_adj",
        mark="close_adj",
        adv="signal_adv20",
        volatility="signal_vol_daily",
        limit_up="is_one_price_limit_up",
        limit_down="is_one_price_limit_down",
        suspended="is_suspended",
        delisted="is_delisted",
        split_ratio=None,
        cash_dividend=None,
    )
    master = store.security_master.copy()
    master["delist_date"] = pd.to_datetime(master["delist_date"], errors="coerce").dt.normalize()
    delist_events: dict[pd.Timestamp, set[str]] = {}
    session_array = np.asarray(store.sessions, dtype="datetime64[ns]")
    for row in master.loc[master["delist_date"].notna()].itertuples(index=False):
        index = int(np.searchsorted(session_array, np.datetime64(row.delist_date), side="left"))
        if index < len(store.sessions):
            delist_events.setdefault(store.sessions[index], set()).add(str(row.ts_code))
    daily_rows: list[dict[str, Any]] = []
    boundary_rows: list[dict[str, Any]] = []
    order_rows: list[dict[str, Any]] = []
    holding_rows: list[dict[str, Any]] = []
    for date in run_sessions:
        raw = store.read_market(date)
        observed = set(raw["ts_code"].astype(str))
        suspended = projection.advance(date, observed_tickers=observed)
        signal = signal_by_execution.get(date)
        targets_for_date = {
            role: target_by_signal.get(signal, {}) if signal is not None else {}
            for role, (target_by_signal, _, _) in roles.items()
        }
        required = set().union(
            *(set(account.positions) for account in accounts.values()),
            *(set(value) for value in targets_for_date.values()),
        )
        signal_snapshot = snapshot_by_signal.get(signal) if signal is not None else None
        market = _execution_market(
            raw,
            required_tickers=required,
            signal_snapshot=signal_snapshot,
            suspended_at_open=suspended,
            delisted_today=delist_events.get(date, set()),
            date=date,
        )
        final_boundary = signal == signals[-1]
        for role, account in accounts.items():
            prior_nav = account.nav()
            costs = 0.0
            turnover = 0.0
            blocked = 0
            actions = ()
            if signal is not None:
                observation = process_account_observation(
                    account,
                    market,
                    observation_date=date,
                    policy=policies[role],
                    columns=columns,
                    mark_at_open=True,
                    process_events=True,
                    process_corporate_actions=False,
                )
                pretrade_nav = observation.nav
                boundary_rows.append(
                    {
                        "role": role,
                        "signal_date": signal,
                        "execution_date": date,
                        "pretrade_nav": pretrade_nav,
                        "target_sha256": canonical_sha256(
                            [
                                {"ticker": key, "target_weight": float(value)}
                                for key, value in sorted(targets_for_date[role].items())
                            ]
                        ),
                    }
                )
                actions = observation.corporate_actions
                if final_boundary:
                    nav = pretrade_nav
                    mark_kind = "terminal_next_open_pretrade"
                else:
                    execution = execute_rebalance(
                        account,
                        targets_for_date[role],
                        market,
                        trade_date=date,
                        policy=policies[role],
                        columns=columns,
                        process_corporate_actions=False,
                        process_events=False,
                    )
                    costs = float(execution.costs["total"])
                    turnover = execution.traded_notional / pretrade_nav
                    blocked = execution.blocked_trade_count
                    for order in execution.orders:
                        order_rows.append(
                            {
                                "role": role,
                                "signal_date": signal,
                                **order.to_trade_dict(rounded=False),
                            }
                        )
                    close_observation = process_account_observation(
                        account,
                        market,
                        observation_date=date,
                        policy=policies[role],
                        columns=columns,
                        mark_at_open=False,
                        process_events=False,
                        process_corporate_actions=False,
                    )
                    nav = close_observation.nav
                    mark_kind = "close_after_rebalance"
            else:
                observation = process_account_observation(
                    account,
                    market,
                    observation_date=date,
                    policy=policies[role],
                    columns=columns,
                    mark_at_open=False,
                    process_events=True,
                    process_corporate_actions=False,
                )
                nav = observation.nav
                actions = observation.corporate_actions
                mark_kind = "close"
            reconciliation = abs(nav - account.nav())
            daily_rows.append(
                {
                    "role": role,
                    "trade_date": date,
                    "mark_kind": mark_kind,
                    "nav": nav,
                    "net_return": nav / prior_nav - 1.0,
                    "cash": account.cash,
                    "market_value": nav - account.cash,
                    "invested_weight": (nav - account.cash) / nav if nav > 0 else 0.0,
                    "position_count": len(account.positions),
                    "trade_cost": costs,
                    "turnover": turnover,
                    "blocked_order_count": blocked,
                    "delist_write_down_count": sum(
                        action.action_type == "delist_write_down" for action in actions
                    ),
                    "nav_reconciliation_error": reconciliation,
                }
            )
            if signal is not None:
                group_map = snapshot_by_signal[signal].set_index("ticker")
                for ticker, position in sorted(account.positions.items()):
                    holding_rows.append(
                        {
                            "role": role,
                            "signal_date": signal,
                            "execution_date": date,
                            "ticker": ticker,
                            "quantity": position.quantity,
                            "mark_price": position.last_price,
                            "market_value": position.market_value,
                            "weight": position.market_value / nav if nav > 0 else 0.0,
                            "industry": str(group_map.at[ticker, "industry"]) if ticker in group_map.index else "UNKNOWN",
                            "size_bucket": str(group_map.at[ticker, "size_bucket"]) if ticker in group_map.index else "UNKNOWN_SIZE",
                        }
                    )
        if final_boundary:
            break
    daily = pd.DataFrame(daily_rows)
    boundaries = pd.DataFrame(boundary_rows)
    orders = pd.DataFrame(order_rows)
    holdings = pd.DataFrame(holding_rows)
    metrics = {
        role: _metrics(
            daily.loc[daily["role"].eq(role)].reset_index(drop=True),
            boundaries.loc[boundaries["role"].eq(role)].reset_index(drop=True),
            orders.loc[orders["role"].eq(role)].reset_index(drop=True),
            initial_capital=config.initial_capital_rmb,
        )
        for role in roles
    }
    return PITStockAccountBuild(
        daily_nav=daily,
        boundaries=boundaries,
        orders=orders,
        holdings=holdings,
        metrics=metrics,
        target_payloads={
            "candidate": _target_payload(candidate),
            "adv500": _target_payload(benchmark),
        },
        source_receipt={
            "market": store.source_receipt(),
            "suspensions": suspension_receipt,
            "config": asdict(config),
            "decisions_payload_sha256": canonical_sha256(
                [decisions[signal] for signal in sorted(decisions)]
            ),
        },
    )


__all__ = [
    "PITStockAccountBuild",
    "PITStockScreeningAccountConfig",
    "SuspensionProjection",
    "simulate_screening_accounts",
    "summarize_screening_account",
]

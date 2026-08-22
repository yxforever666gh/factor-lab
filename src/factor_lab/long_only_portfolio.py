"""Weekly, long-only A-share portfolio evaluation.

The evaluator in this module intentionally lives beside, rather than inside,
``portfolio.py``.  It models an executable long-only portfolio: signals are
observed at the close, orders are placed at the next trading day's open and
the portfolio is marked at the open five trading days later.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from math import sqrt
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from factor_lab.research_os.execution_kernel import (
    ExecutionAccount,
    ExecutionColumns,
    ExecutionPolicy,
    execute_rebalance,
    mark_to_market,
)


@dataclass(frozen=True)
class LongOnlyCostConfig:
    """Per-side A-share transaction cost assumptions."""

    commission_rate: float = 0.0003
    slippage_bps_per_side: float = 5.0
    stamp_duty_before_2023_08_28: float = 0.001
    stamp_duty_from_2023_08_28: float = 0.0005
    exchange_handling_rate: float = 0.0000341
    transfer_fee_rate: float = 0.00001
    impact_coefficient: float = 0.5


@dataclass(frozen=True)
class LongOnlyPortfolioConfig:
    """Configuration for :func:`evaluate_long_only_portfolio`."""

    capital: float = 50_000_000.0
    holding_days: int = 5
    rebalance_every_days: int = 5
    position_count: int = 50
    target_weight: float = 0.02
    max_adv_participation: float = 0.05
    periods_per_year: float = 252.0 / 5.0
    date_column: str = "date"
    ticker_column: str = "ticker"
    open_column: str = "open"
    adv_column: str = "adv_20"
    volatility_column: str = "volatility_20"
    eligible_columns: tuple[str, ...] = ("eligible", "universe_member")
    limit_up_column: str = "is_one_price_limit_up"
    limit_down_column: str = "is_one_price_limit_down"
    costs: LongOnlyCostConfig = field(default_factory=LongOnlyCostConfig)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "LongOnlyPortfolioConfig":
        """Create a config from either a flat mapping or the project JSON shape."""

        portfolio = value.get("portfolio") if isinstance(value.get("portfolio"), Mapping) else value
        if isinstance(value.get("costs"), Mapping):
            costs_value = value["costs"]
        elif isinstance(value.get("cost_config"), Mapping):
            costs_value = value["cost_config"]
        else:
            costs_value = value
        cost_config = LongOnlyCostConfig(**_known_kwargs(LongOnlyCostConfig, costs_value or {}))
        aliases = {
            "top_n": "position_count",
            "rebalance_every_n_days": "rebalance_every_days",
            "max_position_weight": "target_weight",
        }
        kwargs = dict(_known_kwargs(cls, portfolio))
        for source, target in aliases.items():
            if source in portfolio and target not in kwargs:
                kwargs[target] = portfolio[source]
        if "eligible_columns" in kwargs:
            kwargs["eligible_columns"] = tuple(kwargs["eligible_columns"])
        kwargs["costs"] = cost_config
        return cls(**kwargs)


@dataclass
class LongOnlyPortfolioEvaluation:
    status: str
    reason: str | None
    missing_columns: list[str]
    benchmark_return: float
    excess_return: float
    gross_return: float
    net_return: float
    gross_annual_return: float
    net_annual_return: float
    benchmark_annual_return: float
    net_excess_annual_return: float
    annual_volatility: float
    net_sharpe: float
    max_drawdown: float
    win_rate: float
    actual_turnover: float
    capacity_usage: float
    blocked_trade_count: int
    capacity_violation_count: int
    observations: int
    rebalance_count: int
    average_holding_count: float
    max_holding_count: int
    average_cash_weight: float
    max_position_weight: float
    positive_half_year_ratio: float
    trade_count: int
    buy_trade_count: int
    sell_trade_count: int
    total_traded_notional: float
    total_cost: float
    commission_cost: float
    slippage_cost: float
    stamp_duty_cost: float
    exchange_handling_cost: float
    transfer_fee_cost: float
    impact_cost: float
    yearly_segments: list[dict[str, Any]]
    half_year_segments: list[dict[str, Any]]
    periods: list[dict[str, Any]]
    trades: list[dict[str, Any]]
    promotion_eligible: bool = True
    promotion_blockers: list[str] = field(default_factory=list)
    target_weight_mode: str = "equal_weight"
    optimization_audit: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _known_kwargs(cls: type, values: Mapping[str, Any]) -> dict[str, Any]:
    names = cls.__dataclass_fields__.keys()
    return {name: values[name] for name in names if name in values and name != "costs"}


def _as_config(config: LongOnlyPortfolioConfig | Mapping[str, Any] | None) -> LongOnlyPortfolioConfig:
    if config is None:
        result = LongOnlyPortfolioConfig()
    elif isinstance(config, LongOnlyPortfolioConfig):
        result = config
    elif isinstance(config, Mapping):
        result = LongOnlyPortfolioConfig.from_mapping(config)
    else:
        raise TypeError("config must be LongOnlyPortfolioConfig, a mapping, or None")
    if result.capital <= 0:
        raise ValueError("capital must be positive")
    if result.holding_days <= 0 or result.rebalance_every_days <= 0:
        raise ValueError("holding_days and rebalance_every_days must be positive")
    if result.holding_days > result.rebalance_every_days:
        raise ValueError("holding_days cannot exceed rebalance_every_days for non-overlapping periods")
    if result.position_count <= 0:
        raise ValueError("position_count must be positive")
    if not 0 < result.target_weight <= 1:
        raise ValueError("target_weight must be in (0, 1]")
    if not 0 < result.max_adv_participation <= 1:
        raise ValueError("max_adv_participation must be in (0, 1]")
    return result


def _resolve_column(frame: pd.DataFrame, preferred: str, aliases: Sequence[str]) -> str | None:
    for name in (preferred, *aliases):
        if name in frame.columns:
            return name
    return None


def _bool_series(values: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(values):
        return values.fillna(False)
    if pd.api.types.is_numeric_dtype(values):
        return pd.to_numeric(values, errors="coerce").fillna(0).ne(0)
    return values.fillna("").astype(str).str.strip().str.lower().isin({"1", "true", "yes", "y", "on"})


def _signal_values(frame: pd.DataFrame, signal: str | pd.Series | Sequence[float]) -> pd.Series:
    if isinstance(signal, str):
        if signal not in frame.columns:
            raise KeyError(signal)
        return pd.to_numeric(frame[signal], errors="coerce")
    if isinstance(signal, pd.Series):
        if len(signal) != len(frame):
            raise ValueError("signal length must equal frame length")
        if signal.index.equals(frame.index):
            return pd.to_numeric(signal, errors="coerce")
        return pd.Series(pd.to_numeric(signal, errors="coerce").to_numpy(), index=frame.index)
    if len(signal) != len(frame):
        raise ValueError("signal length must equal frame length")
    return pd.Series(pd.to_numeric(pd.Series(signal), errors="coerce").to_numpy(), index=frame.index)


def _empty_evaluation(reason: str, missing_columns: Sequence[str] = ()) -> LongOnlyPortfolioEvaluation:
    return LongOnlyPortfolioEvaluation(
        status="insufficient_data",
        reason=reason,
        missing_columns=list(missing_columns),
        benchmark_return=0.0,
        excess_return=0.0,
        gross_return=0.0,
        net_return=0.0,
        gross_annual_return=0.0,
        net_annual_return=0.0,
        benchmark_annual_return=0.0,
        net_excess_annual_return=0.0,
        annual_volatility=0.0,
        net_sharpe=0.0,
        max_drawdown=0.0,
        win_rate=0.0,
        actual_turnover=0.0,
        capacity_usage=0.0,
        blocked_trade_count=0,
        capacity_violation_count=0,
        observations=0,
        rebalance_count=0,
        average_holding_count=0.0,
        max_holding_count=0,
        average_cash_weight=1.0,
        max_position_weight=0.0,
        positive_half_year_ratio=0.0,
        trade_count=0,
        buy_trade_count=0,
        sell_trade_count=0,
        total_traded_notional=0.0,
        total_cost=0.0,
        commission_cost=0.0,
        slippage_cost=0.0,
        stamp_duty_cost=0.0,
        exchange_handling_cost=0.0,
        transfer_fee_cost=0.0,
        impact_cost=0.0,
        yearly_segments=[],
        half_year_segments=[],
        periods=[],
        trades=[],
        promotion_eligible=False,
        promotion_blockers=[reason],
    )


def _normalized_date_mapping(
    values: Mapping[Any, Mapping[str, Any]] | None,
    *,
    name: str,
) -> dict[pd.Timestamp, dict[str, Any]]:
    output: dict[pd.Timestamp, dict[str, Any]] = {}
    for raw_date, raw_values in (values or {}).items():
        timestamp = pd.Timestamp(raw_date)
        if timestamp.tzinfo is not None:
            timestamp = timestamp.tz_convert(None)
        timestamp = timestamp.normalize()
        if timestamp in output:
            raise ValueError(f"{name} contains duplicate normalized date {timestamp.date()}")
        if not isinstance(raw_values, Mapping):
            raise TypeError(f"{name}[{raw_date!r}] must be a mapping")
        output[timestamp] = {str(key): value for key, value in raw_values.items()}
    return output


def _row_map(day: pd.DataFrame, ticker_column: str) -> dict[str, pd.Series]:
    return {str(row[ticker_column]): row for _, row in day.iterrows()}


def _finite_positive(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if np.isfinite(number) and number > 0 else None


def _annualized_return(period_returns: Sequence[float], periods_per_year: float) -> float:
    if not period_returns:
        return 0.0
    wealth = float(np.prod(1.0 + np.asarray(period_returns, dtype=float)))
    if wealth <= 0:
        return -1.0
    return wealth ** (periods_per_year / len(period_returns)) - 1.0


def _sharpe(period_returns: Sequence[float], periods_per_year: float) -> float:
    if len(period_returns) < 2:
        return 0.0
    values = np.asarray(period_returns, dtype=float)
    standard_deviation = float(values.std(ddof=1))
    return float(values.mean() / standard_deviation * sqrt(periods_per_year)) if standard_deviation > 1e-12 else 0.0


def _max_drawdown(period_returns: Sequence[float]) -> float:
    if not period_returns:
        return 0.0
    wealth = np.cumprod(1.0 + np.asarray(period_returns, dtype=float))
    peaks = np.maximum.accumulate(np.concatenate(([1.0], wealth)))
    drawdowns = np.concatenate(([1.0], wealth)) / peaks - 1.0
    return float(drawdowns.min())


def _compound(period_returns: Sequence[float]) -> float:
    if not period_returns:
        return 0.0
    return float(np.prod(1.0 + np.asarray(period_returns, dtype=float)) - 1.0)


def _segment_rows(periods: list[dict[str, Any]], *, half_year: bool, periods_per_year: float) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in periods:
        end = pd.Timestamp(row["end_date"])
        label = f"{end.year}-H{1 if end.month <= 6 else 2}" if half_year else str(end.year)
        grouped.setdefault(label, []).append(row)
    output: list[dict[str, Any]] = []
    for label, rows in sorted(grouped.items()):
        gross = [float(row["gross_return"]) for row in rows]
        net = [float(row["net_return"]) for row in rows]
        benchmark = [float(row["benchmark_return"]) for row in rows]
        net_total = _compound(net)
        benchmark_total = _compound(benchmark)
        output.append({
            "label": label,
            "start_date": rows[0]["start_date"],
            "end_date": rows[-1]["end_date"],
            "observations": len(rows),
            "gross_return": round(_compound(gross), 8),
            "net_return": round(net_total, 8),
            "benchmark_return": round(benchmark_total, 8),
            "excess_return": round(net_total - benchmark_total, 8),
            "gross_annual_return": round(_annualized_return(gross, periods_per_year), 8),
            "net_annual_return": round(_annualized_return(net, periods_per_year), 8),
            "benchmark_annual_return": round(_annualized_return(benchmark, periods_per_year), 8),
            "net_sharpe": round(_sharpe(net, periods_per_year), 8),
            "max_drawdown": round(_max_drawdown(net), 8),
            "actual_turnover": round(float(np.mean([row["turnover"] for row in rows])), 8),
            "average_holding_count": round(float(np.mean([row["holding_count"] for row in rows])), 6),
            "total_cost": round(float(sum(row["costs"]["total"] for row in rows)), 4),
        })
    return output


def evaluate_long_only_portfolio(
    frame: pd.DataFrame,
    signal: str | pd.Series | Sequence[float],
    config: LongOnlyPortfolioConfig | Mapping[str, Any] | None = None,
    *,
    pricing_frame: pd.DataFrame | None = None,
    target_weights_by_date: Mapping[Any, Mapping[str, float]] | None = None,
    optimization_audit_by_date: Mapping[Any, Mapping[str, Any]] | None = None,
    promotion_blockers: Sequence[str] = (),
    require_optimized_targets: bool = False,
) -> LongOnlyPortfolioEvaluation:
    """Evaluate a non-overlapping weekly long-only portfolio.

    For a signal observed on trading date ``t``, orders execute at ``t+1``
    open and the period is marked at ``t+6`` open.  Every actual order is
    subject to the configured ADV cap and per-side transaction costs.
    """

    cfg = _as_config(config)
    normalized_targets = _normalized_date_mapping(
        target_weights_by_date, name="target_weights_by_date"
    )
    normalized_audit = _normalized_date_mapping(
        optimization_audit_by_date, name="optimization_audit_by_date"
    )
    risk_blockers = list(dict.fromkeys(map(str, promotion_blockers)))
    for signal_date, weights in normalized_targets.items():
        numeric = pd.Series(weights, dtype=float)
        if numeric.empty or not np.isfinite(numeric.to_numpy()).all():
            raise ValueError(f"optimized target weights are empty or non-finite at {signal_date.date()}")
        if numeric.lt(0).any() or numeric.sum() > 1.0 + 1e-8:
            raise ValueError(f"optimized target weights violate long-only funding at {signal_date.date()}")
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return _empty_evaluation("empty_frame")

    price_source = pricing_frame if pricing_frame is not None else frame
    date_col = _resolve_column(price_source, cfg.date_column, ("trade_date",))
    ticker_col = _resolve_column(price_source, cfg.ticker_column, ("ts_code", "symbol"))
    open_col = _resolve_column(price_source, cfg.open_column, ("open_price",))
    adv_col = _resolve_column(price_source, cfg.adv_column, ("amount_20d_avg", "adv", "average_daily_value"))
    volatility_col = _resolve_column(price_source, cfg.volatility_column, ("volatility", "vol_20"))
    missing = [
        name
        for name, resolved in [
            (cfg.date_column, date_col),
            (cfg.ticker_column, ticker_col),
            (cfg.open_column, open_col),
            (cfg.adv_column, adv_col),
            (cfg.volatility_column, volatility_col),
        ]
        if resolved is None
    ]
    if isinstance(signal, str) and signal not in frame.columns:
        missing.append(signal)
    if missing:
        return _empty_evaluation("missing_columns", sorted(set(missing)))

    assert date_col and ticker_col and open_col and adv_col and volatility_col
    try:
        signal_values = _signal_values(frame, signal)
    except KeyError as exc:
        return _empty_evaluation("missing_columns", [str(exc.args[0])])

    present_eligible = [column for column in cfg.eligible_columns if column in price_source.columns]
    limit_up_col = _resolve_column(
        price_source,
        cfg.limit_up_column,
        ("one_price_limit_up", "limit_up", "is_limit_up", "up_limit_locked"),
    )
    limit_down_col = _resolve_column(
        price_source,
        cfg.limit_down_column,
        ("one_price_limit_down", "limit_down", "is_limit_down", "down_limit_locked"),
    )
    suspended_col = _resolve_column(
        price_source, "is_suspended", ("suspended", "is_pause", "paused")
    )
    delisted_col = _resolve_column(
        price_source, "is_delisted", ("delisted", "delist_flag")
    )
    split_ratio_col = _resolve_column(
        price_source, "split_ratio", ("share_split_ratio",)
    )
    cash_dividend_col = _resolve_column(
        price_source, "cash_dividend", ("cash_dividend_per_share",)
    )
    selected_columns = list(dict.fromkeys([
        date_col,
        ticker_col,
        open_col,
        adv_col,
        volatility_col,
        *present_eligible,
        *([limit_up_col] if limit_up_col else []),
        *([limit_down_col] if limit_down_col else []),
        *([suspended_col] if suspended_col else []),
        *([delisted_col] if delisted_col else []),
        *([split_ratio_col] if split_ratio_col else []),
        *([cash_dividend_col] if cash_dividend_col else []),
    ]))
    work = price_source[selected_columns].copy()
    if pricing_frame is None:
        work["_signal"] = signal_values
    else:
        signal_date_col = _resolve_column(frame, cfg.date_column, ("trade_date",))
        signal_ticker_col = _resolve_column(frame, cfg.ticker_column, ("ts_code", "symbol"))
        if signal_date_col is None or signal_ticker_col is None:
            return _empty_evaluation("missing_signal_keys", [cfg.date_column, cfg.ticker_column])
        signal_lookup = frame[[signal_date_col, signal_ticker_col]].copy()
        signal_lookup["_signal"] = signal_values.to_numpy()
        signal_lookup[signal_date_col] = pd.to_datetime(signal_lookup[signal_date_col], errors="coerce")
        signal_lookup[signal_ticker_col] = signal_lookup[signal_ticker_col].astype(str)
        signal_lookup = signal_lookup.drop_duplicates([signal_date_col, signal_ticker_col], keep="last")
        work[date_col] = pd.to_datetime(work[date_col], errors="coerce")
        work[ticker_col] = work[ticker_col].astype(str)
        if signal_date_col != date_col or signal_ticker_col != ticker_col:
            signal_lookup = signal_lookup.rename(columns={signal_date_col: date_col, signal_ticker_col: ticker_col})
        work = work.merge(signal_lookup, on=[date_col, ticker_col], how="left", validate="one_to_one")
    work[date_col] = pd.to_datetime(work[date_col], errors="coerce")
    work[ticker_col] = work[ticker_col].astype(str)
    work[open_col] = pd.to_numeric(work[open_col], errors="coerce")
    work[adv_col] = pd.to_numeric(work[adv_col], errors="coerce")
    work[volatility_col] = pd.to_numeric(work[volatility_col], errors="coerce")
    work = work.dropna(subset=[date_col, ticker_col]).sort_values([date_col, ticker_col])
    work = work.drop_duplicates([date_col, ticker_col], keep="last")
    dates = [pd.Timestamp(value) for value in sorted(work[date_col].unique())]
    required_span = cfg.holding_days + 2
    if len(dates) < required_span:
        return _empty_evaluation("not_enough_trading_days")

    by_date = {pd.Timestamp(date): group.copy() for date, group in work.groupby(date_col, sort=True)}

    execution_account = ExecutionAccount(cash=float(cfg.capital))
    execution_columns = ExecutionColumns(
        open=open_col,
        mark=open_col,
        adv=adv_col,
        volatility=volatility_col,
        limit_up=limit_up_col,
        limit_down=limit_down_col,
        suspended=suspended_col,
        delisted=delisted_col,
        split_ratio=split_ratio_col,
        cash_dividend=cash_dividend_col,
    )
    execution_policy = ExecutionPolicy(
        max_adv_participation=cfg.max_adv_participation,
        max_position_weight=cfg.target_weight,
        lot_size=0,
        costs=cfg.costs,
    )
    periods: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    total_costs = {
        "commission": 0.0,
        "slippage": 0.0,
        "stamp_duty": 0.0,
        "exchange_handling": 0.0,
        "transfer_fee": 0.0,
        "impact": 0.0,
        "total": 0.0,
    }
    blocked_trade_count = 0
    capacity_violation_count = 0
    capacity_usages: list[float] = []
    turnover_values: list[float] = []
    holding_counts: list[int] = []
    cash_weights: list[float] = []

    signal_indices = range(0, len(dates) - cfg.holding_days - 1, cfg.rebalance_every_days)
    for signal_index in signal_indices:
        signal_date = dates[signal_index]
        trade_date = dates[signal_index + 1]
        end_date = dates[signal_index + cfg.holding_days + 1]
        signal_day = by_date[signal_date]
        trade_day = by_date[trade_date]
        end_day = by_date[end_date]
        trade_map = _row_map(trade_day, ticker_col)
        end_map = _row_map(end_day, ticker_col)

        eligible_mask = pd.Series(True, index=signal_day.index)
        for column in present_eligible:
            eligible_mask &= _bool_series(signal_day[column])
        benchmark_day = signal_day[eligible_mask].copy()
        eligible_day = benchmark_day[benchmark_day["_signal"].notna()].copy()
        eligible_day = eligible_day.sort_values(["_signal", ticker_col], ascending=[False, True])
        if eligible_day.empty:
            # Pricing frames may contain pre-training history while the signal
            # frame covers only one outer fold.  Empty dates are not portfolio
            # decisions and must not create artificial all-cash observations.
            continue
        signal_key = signal_date.normalize()
        optimized = normalized_targets.get(signal_key)
        period_optimization_audit = dict(normalized_audit.get(signal_key) or {})
        if optimized is None:
            selected = eligible_day.head(cfg.position_count)[ticker_col].astype(str).tolist()
            equal_weight = min(cfg.target_weight, 1.0 / len(selected))
            target_weights = {ticker: equal_weight for ticker in selected}
            target_weight_mode = (
                "equal_weight_fallback" if require_optimized_targets else "equal_weight"
            )
            if require_optimized_targets:
                risk_blockers.append(
                    f"optimized_target_missing:{signal_date.date().isoformat()}"
                )
        else:
            eligible_tickers = set(eligible_day[ticker_col].astype(str))
            unknown = sorted(set(optimized) - eligible_tickers)
            if unknown:
                risk_blockers.append(
                    f"optimized_target_not_eligible:{signal_date.date().isoformat()}"
                )
            target_weights = {
                ticker: float(weight)
                for ticker, weight in optimized.items()
                if ticker in eligible_tickers and float(weight) > 1e-12
            }
            score_order = {
                ticker: position
                for position, ticker in enumerate(
                    eligible_day[ticker_col].astype(str).tolist()
                )
            }
            selected = sorted(target_weights, key=lambda ticker: score_order[ticker])
            target_weight_mode = "optimized"
            if require_optimized_targets and not period_optimization_audit:
                risk_blockers.append(
                    f"optimization_audit_missing:{signal_date.date().isoformat()}"
                )
            elif require_optimized_targets and not bool(
                period_optimization_audit.get("promotion_eligible", False)
            ):
                risk_blockers.append(
                    f"optimization_audit_not_eligible:{signal_date.date().isoformat()}"
                )

        period_trade_start = len(trades)
        execution = execute_rebalance(
            execution_account,
            target_weights,
            trade_map,
            trade_date=trade_date,
            policy=execution_policy,
            columns=execution_columns,
            ticker_column=ticker_col,
            process_corporate_actions=True,
        )
        pretrade_nav = execution.pretrade_nav
        period_costs = dict(execution.costs)
        for key, value in period_costs.items():
            total_costs[key] += value
        blocked_trade_count += execution.blocked_trade_count
        capacity_violation_count += execution.capacity_violation_count
        if execution.capacity_usage > 0:
            capacity_usages.append(execution.capacity_usage)
        trades.extend(order.to_trade_dict() for order in execution.orders)
        weights = dict(execution.weights)
        holding_count = len(weights)
        cash_weight = execution.cash_weight
        end_nav = mark_to_market(
            execution_account,
            end_map,
            columns=execution_columns,
            ticker_column=ticker_col,
        )
        net_period_return = end_nav / pretrade_nav - 1.0
        gross_period_return = (end_nav + period_costs["total"]) / pretrade_nav - 1.0

        benchmark_returns: list[float] = []
        for ticker in benchmark_day[ticker_col].astype(str):
            start_row = trade_map.get(ticker)
            finish_row = end_map.get(ticker)
            start_price = _finite_positive(start_row[open_col]) if start_row is not None else None
            finish_price = _finite_positive(finish_row[open_col]) if finish_row is not None else None
            if start_price is not None and finish_price is not None:
                benchmark_returns.append(finish_price / start_price - 1.0)
        benchmark_period_return = float(np.mean(benchmark_returns)) if benchmark_returns else 0.0

        period_trades = trades[period_trade_start:]
        traded_notional = sum(float(row.get("executed_notional") or 0.0) for row in period_trades)
        turnover = traded_notional / pretrade_nav if pretrade_nav > 0 else 0.0
        turnover_values.append(turnover)
        holding_counts.append(holding_count)
        cash_weights.append(cash_weight)
        periods.append({
            "signal_date": str(signal_date.date()),
            "start_date": str(trade_date.date()),
            "end_date": str(end_date.date()),
            "gross_return": round(gross_period_return, 10),
            "net_return": round(net_period_return, 10),
            "benchmark_return": round(benchmark_period_return, 10),
            "excess_return": round(net_period_return - benchmark_period_return, 10),
            "turnover": round(turnover, 10),
            "holding_count": holding_count,
            "cash_weight": round(cash_weight, 10),
            "selected_tickers": selected,
            "target_weights": {
                key: round(float(value), 10)
                for key, value in sorted(target_weights.items())
            },
            "target_weight_mode": target_weight_mode,
            "optimization_audit": period_optimization_audit,
            "weights": {key: round(value, 10) for key, value in sorted(weights.items())},
            "costs": {key: round(value, 6) for key, value in period_costs.items()},
            "blocked_trade_count": sum(1 for row in period_trades if row.get("status") == "blocked"),
            "capacity_violation_count": sum(
                1
                for row in period_trades
                if row.get("status") == "executed"
                and float(row.get("requested_notional") or 0.0) > float(row.get("executed_notional") or 0.0) + 1e-8
                and float(row.get("participation") or 0.0) >= cfg.max_adv_participation - 1e-8
            ),
        })

    if not periods:
        return _empty_evaluation("no_rebalance_periods")

    gross_returns = [float(row["gross_return"]) for row in periods]
    net_returns = [float(row["net_return"]) for row in periods]
    benchmark_returns = [float(row["benchmark_return"]) for row in periods]
    gross_total = _compound(gross_returns)
    net_total = _compound(net_returns)
    benchmark_total = _compound(benchmark_returns)
    executed_trades = [row for row in trades if row.get("status") == "executed"]
    total_traded_notional = sum(float(row.get("executed_notional") or 0.0) for row in executed_trades)
    annual_volatility = float(np.std(net_returns, ddof=1) * sqrt(cfg.periods_per_year)) if len(net_returns) > 1 else 0.0
    gross_annual_return = _annualized_return(gross_returns, cfg.periods_per_year)
    net_annual_return = _annualized_return(net_returns, cfg.periods_per_year)
    benchmark_annual_return = _annualized_return(benchmark_returns, cfg.periods_per_year)
    yearly_segments = _segment_rows(periods, half_year=False, periods_per_year=cfg.periods_per_year)
    half_year_segments = _segment_rows(periods, half_year=True, periods_per_year=cfg.periods_per_year)
    positive_half_year_ratio = float(np.mean([row["excess_return"] > 0 for row in half_year_segments])) if half_year_segments else 0.0
    max_position_weight = max(
        (float(weight) for period in periods for weight in period["weights"].values()),
        default=0.0,
    )
    unique_blockers = list(dict.fromkeys(risk_blockers))
    modes = {str(row["target_weight_mode"]) for row in periods}
    target_weight_mode = next(iter(modes)) if len(modes) == 1 else "mixed"
    optimization_audit = [
        {
            "signal_date": row["signal_date"],
            "target_weight_mode": row["target_weight_mode"],
            **dict(row.get("optimization_audit") or {}),
        }
        for row in periods
    ]

    return LongOnlyPortfolioEvaluation(
        status="ok",
        reason=None,
        missing_columns=[],
        benchmark_return=round(benchmark_total, 8),
        excess_return=round(net_total - benchmark_total, 8),
        gross_return=round(gross_total, 8),
        net_return=round(net_total, 8),
        gross_annual_return=round(gross_annual_return, 8),
        net_annual_return=round(net_annual_return, 8),
        benchmark_annual_return=round(benchmark_annual_return, 8),
        net_excess_annual_return=round(net_annual_return - benchmark_annual_return, 8),
        annual_volatility=round(annual_volatility, 8),
        net_sharpe=round(_sharpe(net_returns, cfg.periods_per_year), 8),
        max_drawdown=round(_max_drawdown(net_returns), 8),
        win_rate=round(float(np.mean(np.asarray(net_returns) > 0)), 8),
        actual_turnover=round(float(np.mean(turnover_values)), 8),
        capacity_usage=round(max(capacity_usages, default=0.0), 8),
        blocked_trade_count=blocked_trade_count,
        capacity_violation_count=capacity_violation_count,
        observations=len(periods),
        rebalance_count=len(periods),
        average_holding_count=round(float(np.mean(holding_counts)), 6),
        max_holding_count=max(holding_counts, default=0),
        average_cash_weight=round(float(np.mean(cash_weights)), 8),
        max_position_weight=round(max_position_weight, 8),
        positive_half_year_ratio=round(positive_half_year_ratio, 8),
        trade_count=len(executed_trades),
        buy_trade_count=sum(1 for row in executed_trades if row.get("side") == "buy"),
        sell_trade_count=sum(1 for row in executed_trades if row.get("side") == "sell"),
        total_traded_notional=round(total_traded_notional, 4),
        total_cost=round(total_costs["total"], 4),
        commission_cost=round(total_costs["commission"], 4),
        slippage_cost=round(total_costs["slippage"], 4),
        stamp_duty_cost=round(total_costs["stamp_duty"], 4),
        exchange_handling_cost=round(total_costs["exchange_handling"], 4),
        transfer_fee_cost=round(total_costs["transfer_fee"], 4),
        impact_cost=round(total_costs["impact"], 4),
        yearly_segments=yearly_segments,
        half_year_segments=half_year_segments,
        periods=periods,
        trades=trades,
        promotion_eligible=not unique_blockers,
        promotion_blockers=unique_blockers,
        target_weight_mode=target_weight_mode,
        optimization_audit=optimization_audit,
    )


__all__ = [
    "LongOnlyCostConfig",
    "LongOnlyPortfolioConfig",
    "LongOnlyPortfolioEvaluation",
    "evaluate_long_only_portfolio",
]

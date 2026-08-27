"""Weekly, long-only A-share portfolio evaluation.

The evaluator shares the lightweight ``factor_lab.portfolio`` package with
the deterministic execution kernel.  It models an executable long-only
portfolio: signals are observed at the close, orders are placed at the next
trading day's open and the portfolio is marked at the open five trading days
later.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from math import sqrt
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from factor_lab.portfolio.execution import (
    ExecutionAccount,
    ExecutionColumns,
    ExecutionPolicy,
    execute_rebalance,
    process_account_observation,
)


_ASOF_ADV_COLUMN = "_factor_lab_adv_asof"
_ASOF_VOLATILITY_COLUMN = "_factor_lab_volatility_asof"
_ASOF_DATE_COLUMN = "_factor_lab_execution_input_date"

ADJUSTED_TOTAL_RETURN_PRICE_BASIS = "adjusted_total_return"
RAW_WITH_ACTIONS_PRICE_BASIS = "raw_with_actions"
_PRICE_BASES = {
    ADJUSTED_TOTAL_RETURN_PRICE_BASIS,
    RAW_WITH_ACTIONS_PRICE_BASIS,
}


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
    rebalance_offset_days: int = 0
    position_count: int = 50
    retention_buffer: int = 0
    target_weight: float = 0.02
    max_adv_participation: float = 0.05
    periods_per_year: float = 252.0 / 5.0
    date_column: str = "date"
    ticker_column: str = "ticker"
    open_column: str = "open"
    price_basis: str = ADJUSTED_TOTAL_RETURN_PRICE_BASIS
    price_source: str | None = None
    lot_size: int = 0
    adv_column: str = "adv_20"
    volatility_column: str = "volatility_20"
    eligible_columns: tuple[str, ...] = ("eligible", "universe_member")
    limit_up_column: str = "is_one_price_limit_up"
    limit_down_column: str = "is_one_price_limit_down"
    max_stale_position_age_days: int | None = 21
    costs: LongOnlyCostConfig = field(default_factory=LongOnlyCostConfig)
    evaluation_start_date: str | None = None

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
    benchmark_expected_endpoint_count: int
    benchmark_observed_endpoint_count: int
    benchmark_complete_return_count: int
    benchmark_missing_start_count: int
    benchmark_missing_end_count: int
    benchmark_endpoint_coverage: float
    benchmark_return_coverage: float
    annual_volatility: float
    net_sharpe: float
    max_drawdown: float
    win_rate: float
    actual_turnover: float
    capacity_usage: float
    blocked_trade_count: int
    capacity_violation_count: int
    capacity_limited_count: int
    observations: int
    rebalance_count: int
    average_holding_count: float
    max_holding_count: int
    average_cash_weight: float
    max_position_weight: float
    retention_buffer: int
    rebalance_offset_days: int
    average_target_entry_count: float
    total_target_entry_count: int
    total_target_exit_count: int
    execution_input_policy: str
    max_execution_input_age_days: int
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
    account_nav_path: list[dict[str, Any]]
    promotion_eligible: bool = True
    promotion_blockers: list[str] = field(default_factory=list)
    target_weight_mode: str = "equal_weight"
    optimization_audit: list[dict[str, Any]] = field(default_factory=list)
    evaluation_start_date: str | None = None
    initial_nav: float = 0.0
    first_pretrade_nav: float = 0.0
    end_nav: float = 0.0
    stale_position_observation_count: int = 0
    max_stale_position_count: int = 0
    max_stale_position_notional: float = 0.0
    max_stale_position_age_days: int = 0
    stale_position_blocked_reasons: dict[str, int] = field(default_factory=dict)
    forced_delist_write_down_count: int = 0
    forced_delist_write_down_notional: float = 0.0
    account_nav_reconciliation_error: float = 0.0
    price_basis: str = ADJUSTED_TOTAL_RETURN_PRICE_BASIS
    price_source: str = "unresolved"
    execution_price_column: str | None = None
    corporate_action_mode: str = "embedded_in_adjusted_prices"
    lot_size: int = 0

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
    if result.holding_days != result.rebalance_every_days:
        raise ValueError(
            "holding_days must equal rebalance_every_days so account and "
            "benchmark periods share every accounting boundary"
        )
    if (
        int(result.rebalance_offset_days) != result.rebalance_offset_days
        or not 0 <= int(result.rebalance_offset_days) < result.rebalance_every_days
    ):
        raise ValueError("rebalance_offset_days must be an integer in [0, rebalance_every_days)")
    _parse_evaluation_start_date(result.evaluation_start_date)
    if result.position_count <= 0:
        raise ValueError("position_count must be positive")
    if int(result.retention_buffer) != result.retention_buffer or result.retention_buffer < 0:
        raise ValueError("retention_buffer must be a non-negative integer")
    if not 0 < result.target_weight <= 1:
        raise ValueError("target_weight must be in (0, 1]")
    if not 0 < result.max_adv_participation <= 1:
        raise ValueError("max_adv_participation must be in (0, 1]")
    if result.price_basis not in _PRICE_BASES:
        raise ValueError(
            "price_basis must be 'adjusted_total_return' or 'raw_with_actions'"
        )
    if not isinstance(result.open_column, str) or not result.open_column.strip():
        raise ValueError("open_column must be a non-empty string")
    if (
        result.price_basis == RAW_WITH_ACTIONS_PRICE_BASIS
        and result.open_column.casefold() in {"open_adj", "open_hfq"}
    ):
        raise ValueError(
            "raw_with_actions requires a non-adjusted execution price column"
        )
    if (
        isinstance(result.lot_size, bool)
        or int(result.lot_size) != result.lot_size
        or result.lot_size < 0
    ):
        raise ValueError("lot_size must be a non-negative integer")
    if (
        result.price_basis == ADJUSTED_TOTAL_RETURN_PRICE_BASIS
        and result.lot_size != 0
    ):
        raise ValueError(
            "adjusted_total_return uses synthetic total-return units and requires lot_size=0"
        )
    if result.price_source is not None and (
        not isinstance(result.price_source, str) or not result.price_source.strip()
    ):
        raise ValueError("price_source must be a non-empty string or null")
    max_stale_age = result.max_stale_position_age_days
    if max_stale_age is not None and (
        isinstance(max_stale_age, bool)
        or int(max_stale_age) != max_stale_age
        or max_stale_age < 0
    ):
        raise ValueError(
            "max_stale_position_age_days must be a non-negative integer or null"
        )
    return result


def _parse_evaluation_start_date(value: str | None) -> pd.Timestamp | None:
    """Parse the optional scheduled-signal lower bound in canonical form."""

    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ValueError("evaluation_start_date must be an ISO date in YYYY-MM-DD format")
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "evaluation_start_date must be an ISO date in YYYY-MM-DD format"
        ) from exc
    if (
        pd.isna(timestamp)
        or timestamp.tzinfo is not None
        or value != timestamp.strftime("%Y-%m-%d")
    ):
        raise ValueError("evaluation_start_date must be an ISO date in YYYY-MM-DD format")
    return timestamp.normalize()


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
    normalized = values.astype("string").fillna("").str.strip().str.lower()
    return normalized.isin({"1", "true", "yes", "y", "on"})


def _numeric_event_values(values: pd.Series, *, column: str) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    provided = values.notna()
    invalid = provided & (~np.isfinite(numeric))
    if bool(invalid.any()):
        raise ValueError(
            f"corporate-action column {column!r} contains non-finite values"
        )
    return numeric


def _validate_price_basis_events(
    frame: pd.DataFrame,
    *,
    config: LongOnlyPortfolioConfig,
    split_ratio_column: str | None,
    cash_dividend_column: str | None,
) -> None:
    """Validate the mutually exclusive adjusted-price and raw-event models."""

    split_values = (
        _numeric_event_values(frame[split_ratio_column], column=split_ratio_column)
        if split_ratio_column is not None
        else None
    )
    dividend_values = (
        _numeric_event_values(
            frame[cash_dividend_column], column=cash_dividend_column
        )
        if cash_dividend_column is not None
        else None
    )
    if config.price_basis == ADJUSTED_TOTAL_RETURN_PRICE_BASIS:
        non_neutral_split = (
            split_values.notna()
            & ~np.isclose(split_values, 1.0, rtol=0.0, atol=1e-12)
            if split_values is not None
            else pd.Series(False, index=frame.index)
        )
        non_neutral_dividend = (
            dividend_values.notna()
            & ~np.isclose(dividend_values, 0.0, rtol=0.0, atol=1e-12)
            if dividend_values is not None
            else pd.Series(False, index=frame.index)
        )
        if bool(non_neutral_split.any()) or bool(non_neutral_dividend.any()):
            raise ValueError(
                "adjusted_total_return forbids non-neutral split/dividend inputs; "
                "corporate actions are already embedded in adjusted prices"
            )
        return

    if split_values is not None and bool(
        (split_values.notna() & split_values.le(0.0)).any()
    ):
        raise ValueError("raw_with_actions requires positive split ratios")
    if dividend_values is not None and bool(
        (dividend_values.notna() & dividend_values.lt(0.0)).any()
    ):
        raise ValueError("raw_with_actions requires non-negative cash dividends")


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


def _corporate_action_mode(config: LongOnlyPortfolioConfig) -> str:
    return (
        "embedded_in_adjusted_prices"
        if config.price_basis == ADJUSTED_TOTAL_RETURN_PRICE_BASIS
        else "explicit_split_and_cash_events"
    )


def _resolved_price_source(
    config: LongOnlyPortfolioConfig,
    execution_price_column: str | None,
) -> str:
    return config.price_source or (
        f"input_column:{execution_price_column or config.open_column}"
    )


def _empty_evaluation(
    reason: str,
    missing_columns: Sequence[str] = (),
    *,
    config: LongOnlyPortfolioConfig | None = None,
    execution_price_column: str | None = None,
) -> LongOnlyPortfolioEvaluation:
    price_config = config or LongOnlyPortfolioConfig()
    return LongOnlyPortfolioEvaluation(
        status="insufficient_data",
        reason=reason,
        missing_columns=list(missing_columns),
        evaluation_start_date=None,
        initial_nav=0.0,
        first_pretrade_nav=0.0,
        end_nav=0.0,
        benchmark_return=0.0,
        excess_return=0.0,
        gross_return=0.0,
        net_return=0.0,
        gross_annual_return=0.0,
        net_annual_return=0.0,
        benchmark_annual_return=0.0,
        net_excess_annual_return=0.0,
        benchmark_expected_endpoint_count=0,
        benchmark_observed_endpoint_count=0,
        benchmark_complete_return_count=0,
        benchmark_missing_start_count=0,
        benchmark_missing_end_count=0,
        benchmark_endpoint_coverage=0.0,
        benchmark_return_coverage=0.0,
        annual_volatility=0.0,
        net_sharpe=0.0,
        max_drawdown=0.0,
        win_rate=0.0,
        actual_turnover=0.0,
        capacity_usage=0.0,
        blocked_trade_count=0,
        capacity_violation_count=0,
        capacity_limited_count=0,
        observations=0,
        rebalance_count=0,
        average_holding_count=0.0,
        max_holding_count=0,
        average_cash_weight=1.0,
        max_position_weight=0.0,
        retention_buffer=0,
        rebalance_offset_days=0,
        average_target_entry_count=0.0,
        total_target_entry_count=0,
        total_target_exit_count=0,
        execution_input_policy="previous_valid_ticker_observation",
        max_execution_input_age_days=0,
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
        account_nav_path=[],
        promotion_eligible=False,
        promotion_blockers=[reason],
        price_basis=price_config.price_basis,
        price_source=_resolved_price_source(
            price_config, execution_price_column
        ),
        execution_price_column=execution_price_column,
        corporate_action_mode=_corporate_action_mode(price_config),
        lot_size=int(price_config.lot_size),
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


def _row_map(day: pd.DataFrame, ticker_column: str) -> dict[str, Mapping[str, Any]]:
    """Build the execution lookup without pandas' per-row Series overhead."""

    if day.empty:
        return {}
    columns = tuple(day.columns)
    ticker_position = day.columns.get_loc(ticker_column)
    values = day.to_numpy(copy=False)
    return {
        str(row[ticker_position]): dict(zip(columns, row))
        for row in values
    }


def _row_map_for_tickers(
    day: pd.DataFrame,
    ticker_column: str,
    tickers: Sequence[str],
) -> dict[str, Mapping[str, Any]]:
    """Build a daily mark/event lookup only for positions the account holds.

    A ticker requested by the account but absent from ``day`` deliberately
    remains absent from the result.  ``process_account_observation`` must see
    that absence so it can emit a ``missing_market_bar`` stale diagnostic.
    """

    requested = {str(ticker) for ticker in tickers}
    if day.empty or not requested:
        return {}
    mask = day[ticker_column].astype(str).isin(requested)
    return _row_map(day.loc[mask], ticker_column)


def _finite_positive(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if np.isfinite(number) and number > 0 else None


def _finite_nonnegative(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if np.isfinite(number) and number >= 0 else None


def _truthy(value: Any) -> bool:
    if value is None:
        return False
    try:
        if bool(pd.isna(value)):
            return False
    except (TypeError, ValueError):
        pass
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def _benchmark_open(
    row: Mapping[str, Any] | None,
    *,
    open_column: str,
    suspended_column: str | None,
    delisted_column: str | None,
) -> float | None:
    """Return a trustworthy observed benchmark endpoint, if one exists."""

    if row is None:
        return None
    if suspended_column and _truthy(row.get(suspended_column)):
        return None
    if delisted_column and _truthy(row.get(delisted_column)):
        return None
    return _finite_positive(row.get(open_column))


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


def _max_drawdown_from_nav_path(
    account_nav_path: Sequence[Mapping[str, Any]],
) -> float:
    if not account_nav_path:
        return 0.0
    nav = np.asarray(
        [float(row["nav"]) for row in account_nav_path], dtype=float
    )
    if not np.isfinite(nav).all() or bool((nav < 0.0).any()):
        raise RuntimeError("account NAV path contains an invalid value")
    peaks = np.maximum.accumulate(nav)
    with np.errstate(divide="ignore", invalid="ignore"):
        drawdowns = np.where(peaks > 0.0, nav / peaks - 1.0, 0.0)
    return float(drawdowns.min())


def _linked_account_nav_path(
    account_nav_path: Sequence[Mapping[str, Any]],
    periods: Sequence[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    ranges = [
        (
            int(row["account_nav_path_start_sequence"]),
            int(row["account_nav_path_end_sequence"]),
        )
        for row in periods
    ]
    return [
        row
        for row in account_nav_path
        if any(first <= int(row["sequence"]) <= last for first, last in ranges)
    ]


def _compound(period_returns: Sequence[float]) -> float:
    if not period_returns:
        return 0.0
    return float(np.prod(1.0 + np.asarray(period_returns, dtype=float)) - 1.0)


def _benchmark_coverage(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    expected = int(sum(int(row.get("benchmark_expected_endpoint_count") or 0) for row in rows))
    observed = int(sum(int(row.get("benchmark_observed_endpoint_count") or 0) for row in rows))
    complete = int(sum(int(row.get("benchmark_complete_return_count") or 0) for row in rows))
    missing_start = int(sum(int(row.get("benchmark_missing_start_count") or 0) for row in rows))
    missing_end = int(sum(int(row.get("benchmark_missing_end_count") or 0) for row in rows))
    constituent_count = expected // 2
    return {
        "benchmark_expected_endpoint_count": expected,
        "benchmark_observed_endpoint_count": observed,
        "benchmark_complete_return_count": complete,
        "benchmark_missing_start_count": missing_start,
        "benchmark_missing_end_count": missing_end,
        "benchmark_endpoint_coverage": observed / expected if expected else 0.0,
        "benchmark_return_coverage": complete / constituent_count if constituent_count else 0.0,
    }


def _segment_rows(
    periods: list[dict[str, Any]],
    account_nav_path: Sequence[Mapping[str, Any]],
    *,
    half_year: bool,
    periods_per_year: float,
) -> list[dict[str, Any]]:
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
        segment_nav_path = _linked_account_nav_path(account_nav_path, rows)
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
            "max_drawdown": round(
                _max_drawdown_from_nav_path(segment_nav_path), 8
            ),
            "max_drawdown_basis": "daily_account_nav",
            "account_nav_path_start_sequence": int(
                segment_nav_path[0]["sequence"]
            ),
            "account_nav_path_end_sequence": int(
                segment_nav_path[-1]["sequence"]
            ),
            "actual_turnover": round(float(np.mean([row["turnover"] for row in rows])), 8),
            "average_holding_count": round(float(np.mean([row["holding_count"] for row in rows])), 6),
            "total_cost": round(float(sum(row["costs"]["total"] for row in rows)), 4),
            **{
                key: round(value, 8) if isinstance(value, float) else value
                for key, value in _benchmark_coverage(rows).items()
            },
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
    requested_evaluation_start_date = _parse_evaluation_start_date(
        cfg.evaluation_start_date
    )
    normalized_targets = _normalized_date_mapping(
        target_weights_by_date, name="target_weights_by_date"
    )
    normalized_audit = _normalized_date_mapping(
        optimization_audit_by_date, name="optimization_audit_by_date"
    )
    risk_blockers = list(dict.fromkeys(map(str, promotion_blockers)))
    for signal_date, weights in normalized_targets.items():
        numeric = pd.Series(weights, dtype=float)
        # In strict externally-scheduled mode an explicit empty mapping means
        # a deliberate 100% cash target.  It remains distinct from an omitted
        # date, which the complete-schedule validation below rejects.  Legacy
        # optional target maps retain the old non-empty requirement so an
        # accidental empty mapping cannot silently disable rank construction.
        if numeric.empty and not require_optimized_targets:
            raise ValueError(
                f"optimized target weights are empty or non-finite at {signal_date.date()}"
            )
        if not np.isfinite(numeric.to_numpy()).all():
            raise ValueError(f"optimized target weights are empty or non-finite at {signal_date.date()}")
        if numeric.lt(0).any() or numeric.sum() > 1.0 + 1e-8:
            raise ValueError(f"optimized target weights violate long-only funding at {signal_date.date()}")
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return _empty_evaluation("empty_frame", config=cfg)

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
        return _empty_evaluation(
            "missing_columns", sorted(set(missing)), config=cfg
        )

    assert date_col and ticker_col and open_col and adv_col and volatility_col
    try:
        signal_values = _signal_values(frame, signal)
    except KeyError as exc:
        return _empty_evaluation(
            "missing_columns", [str(exc.args[0])], config=cfg
        )

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
    signal_calendar_start: pd.Timestamp | None = None
    signal_calendar_end: pd.Timestamp | None = None
    if pricing_frame is None:
        work["_signal"] = signal_values
    else:
        signal_date_col = _resolve_column(frame, cfg.date_column, ("trade_date",))
        signal_ticker_col = _resolve_column(frame, cfg.ticker_column, ("ts_code", "symbol"))
        if signal_date_col is None or signal_ticker_col is None:
            return _empty_evaluation(
                "missing_signal_keys",
                [cfg.date_column, cfg.ticker_column],
                config=cfg,
                execution_price_column=open_col,
            )
        signal_lookup = frame[[signal_date_col, signal_ticker_col]].copy()
        signal_lookup["_signal"] = signal_values.to_numpy()
        signal_lookup[signal_date_col] = pd.to_datetime(signal_lookup[signal_date_col], errors="coerce")
        signal_lookup[signal_ticker_col] = signal_lookup[signal_ticker_col].astype(str)
        signal_lookup = signal_lookup.drop_duplicates([signal_date_col, signal_ticker_col], keep="last")
        signal_calendar_start = signal_lookup[signal_date_col].dropna().min()
        signal_calendar_end = signal_lookup[signal_date_col].dropna().max()
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
    _validate_price_basis_events(
        work,
        config=cfg,
        split_ratio_column=split_ratio_col,
        cash_dividend_column=cash_dividend_col,
    )
    # Orders are submitted at the next session's open.  The current session's
    # rolling ADV and close-to-close volatility are not known at that time, so
    # execution must use the latest strictly earlier *valid* observation for
    # each ticker.  Event-only suspension/delist rows deliberately contain no
    # price inputs and must not break this history chain or impersonate the
    # actual as-of date of the carried inputs.
    valid_execution_input = (
        np.isfinite(work[open_col])
        & work[open_col].gt(0.0)
        & np.isfinite(work[adv_col])
        & work[adv_col].gt(0.0)
        & np.isfinite(work[volatility_col])
        & work[volatility_col].ge(0.0)
    )
    if suspended_col is not None:
        valid_execution_input &= ~_bool_series(work[suspended_col])
    if delisted_col is not None:
        valid_execution_input &= ~_bool_series(work[delisted_col])
    historical_inputs = work[[adv_col, volatility_col, date_col]].where(
        valid_execution_input
    )
    carried_inputs = historical_inputs.groupby(
        work[ticker_col], sort=False
    ).ffill()
    asof_inputs = carried_inputs.groupby(work[ticker_col], sort=False).shift(1)
    work[_ASOF_ADV_COLUMN] = asof_inputs[adv_col]
    work[_ASOF_VOLATILITY_COLUMN] = asof_inputs[volatility_col]
    work[_ASOF_DATE_COLUMN] = asof_inputs[date_col]
    dates = [pd.Timestamp(value) for value in sorted(work[date_col].unique())]
    required_span = cfg.holding_days + 2
    if len(dates) < required_span:
        return _empty_evaluation(
            "not_enough_trading_days",
            config=cfg,
            execution_price_column=open_col,
        )

    by_date = {pd.Timestamp(date): group.copy() for date, group in work.groupby(date_col, sort=True)}

    explicit_corporate_actions = (
        cfg.price_basis == RAW_WITH_ACTIONS_PRICE_BASIS
    )
    execution_columns = ExecutionColumns(
        open=open_col,
        mark=open_col,
        adv=_ASOF_ADV_COLUMN,
        volatility=_ASOF_VOLATILITY_COLUMN,
        limit_up=limit_up_col,
        limit_down=limit_down_col,
        suspended=suspended_col,
        delisted=delisted_col,
        split_ratio=split_ratio_col if explicit_corporate_actions else None,
        cash_dividend=(
            cash_dividend_col if explicit_corporate_actions else None
        ),
    )
    execution_policy = ExecutionPolicy(
        max_adv_participation=cfg.max_adv_participation,
        max_position_weight=cfg.target_weight,
        lot_size=int(cfg.lot_size),
        costs=cfg.costs,
        max_stale_position_age_days=cfg.max_stale_position_age_days,
    )
    periods: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    account_nav_path: list[dict[str, Any]] = []

    def record_account_nav(
        observation_date: pd.Timestamp, phase: str, nav: float
    ) -> int:
        sequence = len(account_nav_path)
        account_nav_path.append(
            {
                "date": observation_date.date().isoformat(),
                "phase": phase,
                "nav": float(nav),
                "sequence": sequence,
            }
        )
        return sequence
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
    capacity_limited_count = 0
    capacity_usages: list[float] = []
    turnover_values: list[float] = []
    holding_counts: list[int] = []
    cash_weights: list[float] = []
    target_entry_counts: list[int] = []
    target_exit_counts: list[int] = []
    execution_input_ages: list[int] = []
    net_return_values: list[float] = []
    gross_return_values: list[float] = []
    benchmark_return_values: list[float] = []
    stale_position_observation_count = 0
    max_stale_position_count = 0
    max_stale_position_notional = 0.0
    max_stale_position_age_days = 0
    stale_position_blocked_reasons: dict[str, int] = {}
    forced_delist_write_down_count = 0
    forced_delist_write_down_notional = 0.0
    previous_target_tickers: set[str] = set()
    cached_row_map_date: pd.Timestamp | None = None
    cached_row_map: dict[str, Mapping[str, Any]] | None = None
    processed_observation_dates: set[pd.Timestamp] = set()
    first_pretrade_nav: float | None = None
    latest_end_nav = float(cfg.capital)

    if signal_calendar_start is None:
        signal_calendar_start = dates[0]
    first_signal_index = next(
        (index for index, value in enumerate(dates) if value >= signal_calendar_start),
        len(dates),
    )
    signal_indices = [
        index
        for index in range(
            first_signal_index + int(cfg.rebalance_offset_days),
            len(dates) - cfg.holding_days - 1,
            cfg.rebalance_every_days,
        )
        if requested_evaluation_start_date is None
        or dates[index].normalize() >= requested_evaluation_start_date
        if signal_calendar_end is None or dates[index] <= signal_calendar_end
    ]
    if require_optimized_targets:
        scheduled_signal_dates = [dates[index].normalize() for index in signal_indices]
        missing_targets = [
            value.date().isoformat()
            for value in scheduled_signal_dates
            if value not in normalized_targets
        ]
        if missing_targets:
            raise ValueError(
                "optimized targets are required for every scheduled signal date; "
                f"missing={missing_targets}"
            )
        missing_audits = [
            value.date().isoformat()
            for value in scheduled_signal_dates
            if value not in normalized_audit
        ]
        if missing_audits:
            raise ValueError(
                "optimization audits are required for every scheduled signal date; "
                f"missing={missing_audits}"
            )
        ineligible_audits = [
            value.date().isoformat()
            for value in scheduled_signal_dates
            if not bool(normalized_audit[value].get("promotion_eligible", False))
        ]
        if ineligible_audits:
            raise ValueError(
                "optimized targets require promotion_eligible audits; "
                f"ineligible={ineligible_audits}"
            )
    effective_evaluation_start_date = (
        dates[signal_indices[0]].normalize() if signal_indices else None
    )
    # The account is deliberately born at this evaluation boundary.  Earlier
    # scheduled signals never mutate cash, holdings, or the target-retention state.
    execution_account = ExecutionAccount(cash=float(cfg.capital))
    for signal_index in signal_indices:
        signal_date = dates[signal_index]
        trade_date = dates[signal_index + 1]
        end_date = dates[signal_index + cfg.holding_days + 1]
        signal_day = by_date[signal_date]
        trade_day = by_date[trade_date]
        if trade_date == cached_row_map_date:
            assert cached_row_map is not None
            trade_map = cached_row_map
        else:
            trade_map = _row_map(trade_day, ticker_col)

        eligible_mask = pd.Series(True, index=signal_day.index)
        for column in present_eligible:
            eligible_mask &= _bool_series(signal_day[column])
        benchmark_day = signal_day[eligible_mask].copy()
        eligible_day = benchmark_day[benchmark_day["_signal"].notna()].copy()
        eligible_day = eligible_day.sort_values(["_signal", ticker_col], ascending=[False, True])
        if eligible_day.empty:
            # A scheduled decision cannot disappear from the account path.
            # Silently skipping it can omit terminal NAV or merge several
            # accounting intervals against one benchmark period.  Return an
            # explicit unusable evaluation instead of inventing cash returns.
            return _empty_evaluation(
                f"empty_signal_cross_section:{signal_date.date().isoformat()}",
                config=cfg,
                execution_price_column=open_col,
            )
        signal_key = signal_date.normalize()
        optimized = normalized_targets.get(signal_key)
        period_optimization_audit = dict(normalized_audit.get(signal_key) or {})
        prior_target_tickers = set(previous_target_tickers)
        if optimized is None:
            ranked_tickers = eligible_day[ticker_col].astype(str).tolist()
            selected: list[str] = []
            if cfg.retention_buffer and prior_target_tickers:
                retention_rank = cfg.position_count + int(cfg.retention_buffer)
                retained = {
                    ticker
                    for rank, ticker in enumerate(ranked_tickers, start=1)
                    if rank <= retention_rank and ticker in prior_target_tickers
                }
                selected.extend(ticker for ticker in ranked_tickers if ticker in retained)
            for ticker in ranked_tickers:
                if len(selected) >= cfg.position_count:
                    break
                if ticker not in selected:
                    selected.append(ticker)
            rank_order = {ticker: rank for rank, ticker in enumerate(ranked_tickers)}
            selected.sort(key=lambda ticker: rank_order[ticker])
            equal_weight = min(cfg.target_weight, 1.0 / len(selected))
            target_weights = {ticker: equal_weight for ticker in selected}
            if cfg.retention_buffer:
                target_weight_mode = "equal_weight_retention_buffer"
            else:
                target_weight_mode = "equal_weight"
        else:
            eligible_tickers = set(eligible_day[ticker_col].astype(str))
            unknown = sorted(set(optimized) - eligible_tickers)
            if unknown:
                if require_optimized_targets:
                    raise ValueError(
                        "optimized targets include ineligible tickers at "
                        f"{signal_date.date().isoformat()}: {unknown}"
                    )
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

        current_target_tickers = set(target_weights)
        retained_target_count = len(prior_target_tickers & current_target_tickers)
        target_entry_count = len(current_target_tickers - prior_target_tickers)
        target_exit_count = len(prior_target_tickers - current_target_tickers)
        target_entry_counts.append(target_entry_count)
        target_exit_counts.append(target_exit_count)
        previous_target_tickers = current_target_tickers

        period_trade_start = len(trades)
        trade_events_already_processed = trade_date.normalize() in processed_observation_dates
        execution = execute_rebalance(
            execution_account,
            target_weights,
            trade_map,
            trade_date=trade_date,
            policy=execution_policy,
            columns=execution_columns,
            ticker_column=ticker_col,
            process_corporate_actions=(
                explicit_corporate_actions
                and not trade_events_already_processed
            ),
            process_events=not trade_events_already_processed,
        )
        processed_observation_dates.add(trade_date.normalize())
        accounting_start_nav = execution.accounting_start_nav
        pretrade_nav = execution.pretrade_nav
        accounting_boundary_date = (
            requested_evaluation_start_date
            if not account_nav_path
            and requested_evaluation_start_date is not None
            else signal_date
            if not account_nav_path
            else trade_date
        )
        period_nav_path_start_sequence = record_account_nav(
            accounting_boundary_date,
            "accounting_boundary",
            accounting_start_nav,
        )
        record_account_nav(trade_date, "posttrade", execution.posttrade_nav)
        if first_pretrade_nav is None:
            first_pretrade_nav = float(accounting_start_nav)
        stale_position_observation_count += execution.stale_position_count
        max_stale_position_count = max(
            max_stale_position_count, execution.stale_position_count
        )
        max_stale_position_notional = max(
            max_stale_position_notional, execution.stale_position_notional
        )
        max_stale_position_age_days = max(
            max_stale_position_age_days,
            execution.max_stale_position_age_days,
        )
        for reason, count in execution.stale_position_blocked_reasons.items():
            stale_position_blocked_reasons[reason] = (
                stale_position_blocked_reasons.get(reason, 0) + int(count)
            )
        delist_actions = [
            action
            for action in execution.corporate_actions
            if action.action_type == "delist_write_down"
        ]
        forced_delist_write_down_count += len(delist_actions)
        forced_delist_write_down_notional += sum(
            float(action.payload.get("carrying_notional") or 0.0)
            for action in delist_actions
        )
        if any(
            diagnostic.blocked_reason in {"missing_market_bar", "missing_open"}
            for diagnostic in execution.stale_position_diagnostics
        ):
            risk_blockers.append("unresolved_stale_position_observed")
        period_costs = dict(execution.costs)
        for key, value in period_costs.items():
            total_costs[key] += value
        blocked_trade_count += execution.blocked_trade_count
        capacity_violation_count += execution.capacity_violation_count
        capacity_limited_count += execution.capacity_limited_order_count
        if execution.capacity_usage > 0:
            capacity_usages.append(execution.capacity_usage)
        for order in execution.orders:
            trade = order.to_trade_dict()
            market_row = trade_map.get(order.ticker)
            raw_input_date = (
                market_row.get(_ASOF_DATE_COLUMN)
                if market_row is not None
                else None
            )
            if raw_input_date is not None and not pd.isna(raw_input_date):
                input_date = pd.Timestamp(raw_input_date).normalize()
                trade["execution_input_date"] = str(input_date.date())
                execution_input_ages.append(max((trade_date.normalize() - input_date).days, 0))
            else:
                trade["execution_input_date"] = None
            trade["execution_input_adv_available"] = bool(
                market_row is not None
                and _finite_positive(market_row.get(_ASOF_ADV_COLUMN)) is not None
            )
            trade["execution_input_volatility_available"] = bool(
                market_row is not None
                and _finite_nonnegative(market_row.get(_ASOF_VOLATILITY_COLUMN)) is not None
            )
            trade["execution_input_complete"] = bool(
                trade["execution_input_date"]
                and trade["execution_input_adv_available"]
                and trade["execution_input_volatility_available"]
            )
            trades.append(trade)
        weights = dict(execution.weights)
        holding_count = len(weights)
        cash_weight = execution.cash_weight
        period_stale_position_count = execution.stale_position_count
        period_stale_position_notional = execution.stale_position_notional
        period_max_stale_position_age_days = execution.max_stale_position_age_days
        period_stale_diagnostics = list(execution.stale_position_diagnostics)
        period_stale_reasons = dict(execution.stale_position_blocked_reasons)
        end_observation = None
        end_map: dict[str, Mapping[str, Any]] | None = None
        # Observe every execution session after the opening rebalance.  These
        # calls never create orders: they keep the last trustworthy mark
        # current and apply mid-period security/corporate events on their real
        # session instead of postponing them to the next rebalance boundary.
        for observation_index in range(
            signal_index + 2,
            signal_index + cfg.holding_days + 2,
        ):
            observation_date = dates[observation_index]
            observation_day = by_date[observation_date]
            if observation_date == end_date:
                # The full terminal cross-section is required below for the
                # benchmark endpoints and is also reused as the next period's
                # trade map when accounting boundaries touch.
                observation_map = _row_map(observation_day, ticker_col)
                end_map = observation_map
            else:
                # No orders can be created here.  Only held securities can
                # affect marks, security events, or corporate actions, so do
                # not materialize thousands of irrelevant rows every day.
                observation_map = _row_map_for_tickers(
                    observation_day,
                    ticker_col,
                    execution_account.positions.keys(),
                )
            events_already_processed = (
                observation_date.normalize() in processed_observation_dates
            )
            observation = process_account_observation(
                execution_account,
                observation_map,
                observation_date=observation_date,
                policy=execution_policy,
                columns=execution_columns,
                ticker_column=ticker_col,
                process_events=not events_already_processed,
                process_corporate_actions=(
                    explicit_corporate_actions and not events_already_processed
                ),
            )
            processed_observation_dates.add(observation_date.normalize())
            stale_position_observation_count += observation.stale_position_count
            max_stale_position_count = max(
                max_stale_position_count, observation.stale_position_count
            )
            max_stale_position_notional = max(
                max_stale_position_notional, observation.stale_position_notional
            )
            max_stale_position_age_days = max(
                max_stale_position_age_days,
                observation.max_stale_position_age_days,
            )
            period_stale_position_count += observation.stale_position_count
            period_stale_position_notional = max(
                period_stale_position_notional,
                observation.stale_position_notional,
            )
            period_max_stale_position_age_days = max(
                period_max_stale_position_age_days,
                observation.max_stale_position_age_days,
            )
            period_stale_diagnostics.extend(observation.stale_position_diagnostics)
            for reason, count in observation.stale_position_blocked_reasons.items():
                stale_position_blocked_reasons[reason] = (
                    stale_position_blocked_reasons.get(reason, 0) + int(count)
                )
                period_stale_reasons[reason] = (
                    period_stale_reasons.get(reason, 0) + int(count)
                )
            observation_delist_actions = [
                action
                for action in observation.corporate_actions
                if action.action_type == "delist_write_down"
            ]
            delist_actions.extend(observation_delist_actions)
            forced_delist_write_down_count += len(observation_delist_actions)
            forced_delist_write_down_notional += sum(
                float(action.payload.get("carrying_notional") or 0.0)
                for action in observation_delist_actions
            )
            if any(
                diagnostic.blocked_reason
                in {"missing_market_bar", "missing_open"}
                for diagnostic in observation.stale_position_diagnostics
            ):
                risk_blockers.append("unresolved_stale_position_observed")
            record_account_nav(
                observation_date, "daily_end", observation.nav
            )
            end_observation = observation

        assert end_observation is not None
        assert end_map is not None
        cached_row_map_date = end_date
        cached_row_map = end_map
        end_nav = end_observation.nav
        period_nav_path_end_sequence = len(account_nav_path) - 1
        latest_end_nav = float(end_nav)
        # Use the prior accounting boundary, not the post-event sizing NAV.
        # This makes returns continuous across idle gaps and ensures a delist
        # write-down cannot disappear between two reported periods.
        net_period_return = end_nav / accounting_start_nav - 1.0
        gross_period_return = (
            (end_nav + period_costs["total"]) / accounting_start_nav - 1.0
        )

        benchmark_returns: list[float] = []
        benchmark_missing_start_count = 0
        benchmark_missing_end_count = 0
        benchmark_start_count = 0
        benchmark_end_count = 0
        for ticker in benchmark_day[ticker_col].astype(str):
            start_row = trade_map.get(ticker)
            finish_row = end_map.get(ticker)
            start_price = _benchmark_open(
                start_row,
                open_column=open_col,
                suspended_column=suspended_col,
                delisted_column=delisted_col,
            )
            finish_price = _benchmark_open(
                finish_row,
                open_column=open_col,
                suspended_column=suspended_col,
                delisted_column=delisted_col,
            )
            if start_price is None:
                benchmark_missing_start_count += 1
            else:
                benchmark_start_count += 1
            if finish_price is None:
                benchmark_missing_end_count += 1
            else:
                benchmark_end_count += 1
            if start_price is not None and finish_price is not None:
                benchmark_returns.append(finish_price / start_price - 1.0)
        benchmark_period_return = float(np.mean(benchmark_returns)) if benchmark_returns else 0.0
        net_return_values.append(net_period_return)
        gross_return_values.append(gross_period_return)
        benchmark_return_values.append(benchmark_period_return)

        period_trades = trades[period_trade_start:]
        period_input_dates = sorted(
            pd.Timestamp(row["execution_input_date"])
            for row in period_trades
            if row.get("execution_input_date")
        )
        period_input_ages = [
            max((trade_date.normalize() - value.normalize()).days, 0)
            for value in period_input_dates
        ]
        execution_input_required_count = len(period_trades)
        execution_input_observed_count = sum(
            bool(row.get("execution_input_complete")) for row in period_trades
        )
        execution_input_future_violation_count = sum(
            bool(row.get("execution_input_date"))
            and pd.Timestamp(str(row["execution_input_date"])) > signal_date
            for row in period_trades
        )
        traded_notional = sum(float(row.get("executed_notional") or 0.0) for row in period_trades)
        turnover = traded_notional / pretrade_nav if pretrade_nav > 0 else 0.0
        turnover_values.append(turnover)
        holding_counts.append(holding_count)
        cash_weights.append(cash_weight)
        period_nav_path = account_nav_path[
            period_nav_path_start_sequence : period_nav_path_end_sequence + 1
        ]
        periods.append({
            "signal_date": str(signal_date.date()),
            "start_date": str(trade_date.date()),
            "end_date": str(end_date.date()),
            "accounting_boundary_date": str(
                accounting_boundary_date.date()
            ),
            "rebalance_offset_days": int(cfg.rebalance_offset_days),
            "execution_input_policy": "previous_valid_ticker_observation",
            "execution_input_min_date": str(period_input_dates[0].date()) if period_input_dates else None,
            "execution_input_max_date": str(period_input_dates[-1].date()) if period_input_dates else None,
            "execution_input_required_count": execution_input_required_count,
            "execution_input_observed_count": execution_input_observed_count,
            "execution_input_future_violation_count": int(
                execution_input_future_violation_count
            ),
            "execution_input_coverage": round(
                execution_input_observed_count / execution_input_required_count,
                10,
            )
            if execution_input_required_count
            else 1.0,
            "max_execution_input_age_days": max(period_input_ages, default=0),
            "accounting_start_nav": round(accounting_start_nav, 4),
            "pretrade_nav": round(pretrade_nav, 4),
            "end_nav": round(end_nav, 4),
            "account_nav_path_start_sequence": period_nav_path_start_sequence,
            "account_nav_path_end_sequence": period_nav_path_end_sequence,
            "daily_nav_observation_count": cfg.holding_days,
            "max_drawdown": round(
                _max_drawdown_from_nav_path(period_nav_path), 8
            ),
            "max_drawdown_basis": "daily_account_nav",
            "preorder_nav_change": round(pretrade_nav - accounting_start_nav, 4),
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
            "retention_buffer": int(cfg.retention_buffer),
            "effective_retention_rank": cfg.position_count + int(cfg.retention_buffer),
            "retained_target_count": retained_target_count,
            "target_entry_count": target_entry_count,
            "target_exit_count": target_exit_count,
            "optimization_audit": period_optimization_audit,
            "weights": {key: round(value, 10) for key, value in sorted(weights.items())},
            "costs": {key: round(value, 6) for key, value in period_costs.items()},
            "benchmark_expected_endpoint_count": 2 * len(benchmark_day),
            "benchmark_observed_endpoint_count": benchmark_start_count + benchmark_end_count,
            "benchmark_complete_return_count": len(benchmark_returns),
            "benchmark_missing_start_count": benchmark_missing_start_count,
            "benchmark_missing_end_count": benchmark_missing_end_count,
            "benchmark_endpoint_coverage": round(
                (benchmark_start_count + benchmark_end_count) / (2 * len(benchmark_day)), 10
            ) if len(benchmark_day) else 0.0,
            "benchmark_return_coverage": round(
                len(benchmark_returns) / len(benchmark_day), 10
            ) if len(benchmark_day) else 0.0,
            "blocked_trade_count": sum(1 for row in period_trades if row.get("status") == "blocked"),
            # The execution kernel checks capacity on full-precision values
            # and raises on any violation.  Recomputing this audit from the
            # four-decimal serialized trade rows can create a false positive
            # when an order lands exactly on its ADV cap.
            "capacity_violation_count": int(execution.capacity_violation_count),
            "capacity_limited_count": sum(
                bool(row.get("capacity_limited")) for row in period_trades
            ),
            "stale_position_count": (
                period_stale_position_count
            ),
            "stale_position_notional": round(
                period_stale_position_notional,
                4,
            ),
            "max_stale_position_age_days": (
                period_max_stale_position_age_days
            ),
            "stale_position_blocked_reasons": dict(sorted(period_stale_reasons.items())),
            "stale_position_diagnostics": [
                diagnostic.to_dict()
                for diagnostic in period_stale_diagnostics
            ],
            "forced_delist_write_down_count": len(delist_actions),
            "forced_delist_write_down_notional": round(
                sum(
                    float(action.payload.get("carrying_notional") or 0.0)
                    for action in delist_actions
                ),
                4,
            ),
        })

    if not periods:
        return _empty_evaluation(
            "no_rebalance_periods",
            config=cfg,
            execution_price_column=open_col,
        )
    assert effective_evaluation_start_date is not None
    assert first_pretrade_nav is not None

    gross_returns = gross_return_values
    net_returns = net_return_values
    benchmark_returns = benchmark_return_values
    gross_total = _compound(gross_returns)
    net_total = _compound(net_returns)
    benchmark_total = _compound(benchmark_returns)
    account_net_return = latest_end_nav / float(cfg.capital) - 1.0
    reconciliation_error = net_total - account_net_return
    if not np.isclose(reconciliation_error, 0.0, rtol=0.0, atol=1e-10):
        raise RuntimeError(
            "portfolio period returns do not reconcile to account NAV: "
            f"{net_total} != {account_net_return}"
        )
    net_total = account_net_return
    path_start_nav = float(account_nav_path[0]["nav"])
    path_end_nav = float(account_nav_path[-1]["nav"])
    path_net_return = path_end_nav / path_start_nav - 1.0
    if (
        not np.isclose(path_start_nav, float(cfg.capital), rtol=0.0, atol=0.01)
        or not np.isclose(path_end_nav, latest_end_nav, rtol=0.0, atol=0.01)
        or not np.isclose(
            path_net_return,
            account_net_return,
            rtol=0.0,
            atol=max(1e-10, 0.01 / float(cfg.capital)),
        )
    ):
        raise RuntimeError("daily account NAV path does not reconcile to account NAV")
    executed_trades = [row for row in trades if row.get("status") == "executed"]
    total_traded_notional = sum(float(row.get("executed_notional") or 0.0) for row in executed_trades)
    annual_volatility = float(np.std(net_returns, ddof=1) * sqrt(cfg.periods_per_year)) if len(net_returns) > 1 else 0.0
    gross_annual_return = _annualized_return(gross_returns, cfg.periods_per_year)
    net_annual_return = _annualized_return(net_returns, cfg.periods_per_year)
    benchmark_annual_return = _annualized_return(benchmark_returns, cfg.periods_per_year)
    benchmark_coverage = _benchmark_coverage(periods)
    yearly_segments = _segment_rows(
        periods,
        account_nav_path,
        half_year=False,
        periods_per_year=cfg.periods_per_year,
    )
    half_year_segments = _segment_rows(
        periods,
        account_nav_path,
        half_year=True,
        periods_per_year=cfg.periods_per_year,
    )
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
        price_basis=cfg.price_basis,
        price_source=_resolved_price_source(cfg, open_col),
        execution_price_column=open_col,
        corporate_action_mode=_corporate_action_mode(cfg),
        lot_size=int(cfg.lot_size),
        evaluation_start_date=str(effective_evaluation_start_date.date()),
        initial_nav=round(float(cfg.capital), 4),
        first_pretrade_nav=round(float(first_pretrade_nav), 4),
        end_nav=round(latest_end_nav, 4),
        stale_position_observation_count=stale_position_observation_count,
        max_stale_position_count=max_stale_position_count,
        max_stale_position_notional=round(max_stale_position_notional, 4),
        max_stale_position_age_days=max_stale_position_age_days,
        stale_position_blocked_reasons=dict(
            sorted(stale_position_blocked_reasons.items())
        ),
        forced_delist_write_down_count=forced_delist_write_down_count,
        forced_delist_write_down_notional=round(
            forced_delist_write_down_notional, 4
        ),
        account_nav_reconciliation_error=round(reconciliation_error, 12),
        benchmark_return=round(benchmark_total, 8),
        excess_return=round(net_total - benchmark_total, 8),
        gross_return=round(gross_total, 8),
        net_return=round(net_total, 8),
        gross_annual_return=round(gross_annual_return, 8),
        net_annual_return=round(net_annual_return, 8),
        benchmark_annual_return=round(benchmark_annual_return, 8),
        net_excess_annual_return=round(net_annual_return - benchmark_annual_return, 8),
        **{
            key: round(value, 8) if isinstance(value, float) else value
            for key, value in benchmark_coverage.items()
        },
        annual_volatility=round(annual_volatility, 8),
        net_sharpe=round(_sharpe(net_returns, cfg.periods_per_year), 8),
        max_drawdown=round(
            _max_drawdown_from_nav_path(account_nav_path), 8
        ),
        win_rate=round(float(np.mean(np.asarray(net_returns) > 0)), 8),
        actual_turnover=round(float(np.mean(turnover_values)), 8),
        capacity_usage=round(max(capacity_usages, default=0.0), 8),
        blocked_trade_count=blocked_trade_count,
        capacity_violation_count=capacity_violation_count,
        capacity_limited_count=capacity_limited_count,
        observations=len(periods),
        rebalance_count=len(periods),
        average_holding_count=round(float(np.mean(holding_counts)), 6),
        max_holding_count=max(holding_counts, default=0),
        average_cash_weight=round(float(np.mean(cash_weights)), 8),
        max_position_weight=round(max_position_weight, 8),
        retention_buffer=int(cfg.retention_buffer),
        rebalance_offset_days=int(cfg.rebalance_offset_days),
        average_target_entry_count=round(float(np.mean(target_entry_counts)), 8),
        total_target_entry_count=int(sum(target_entry_counts)),
        total_target_exit_count=int(sum(target_exit_counts)),
        execution_input_policy="previous_valid_ticker_observation",
        max_execution_input_age_days=max(execution_input_ages, default=0),
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
        account_nav_path=account_nav_path,
        promotion_eligible=not unique_blockers,
        promotion_blockers=unique_blockers,
        target_weight_mode=target_weight_mode,
        optimization_audit=optimization_audit,
    )


__all__ = [
    "ADJUSTED_TOTAL_RETURN_PRICE_BASIS",
    "LongOnlyCostConfig",
    "LongOnlyPortfolioConfig",
    "LongOnlyPortfolioEvaluation",
    "RAW_WITH_ACTIONS_PRICE_BASIS",
    "evaluate_long_only_portfolio",
]

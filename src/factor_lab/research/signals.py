"""Safe expression and builtin signal evaluation."""

from __future__ import annotations

import ast
from collections.abc import Callable, Mapping
from typing import Any

import numpy as np
import pandas as pd

from .contracts import FactorSpec, is_forbidden_signal_field


BuiltinSignal = Callable[[pd.DataFrame, Mapping[str, Any]], pd.Series]
FINANCIAL_QUALITY_FIELDS = (
    "fundamental_roic",
    "fundamental_q_ocf_to_sales",
    "fundamental_debt_to_assets",
)
_BINARY_OPERATORS: dict[type[ast.operator], Callable[[Any, Any], Any]] = {
    ast.Add: lambda left, right: left + right,
    ast.Sub: lambda left, right: left - right,
    ast.Mult: lambda left, right: left * right,
    ast.Div: lambda left, right: left / right,
}


class SafeExpressionEvaluator:
    """Evaluate a deliberately small arithmetic expression language."""

    def __init__(
        self,
        frame: pd.DataFrame,
        *,
        date_column: str = "date",
        aliases: Mapping[str, pd.Series] | None = None,
    ) -> None:
        if date_column not in frame.columns:
            raise ValueError(f"missing date column: {date_column}")
        self.frame = frame
        self.date_column = date_column
        self.aliases = dict(aliases or {})

    def evaluate(self, expression: str) -> pd.Series:
        try:
            tree = ast.parse(expression, mode="eval")
        except SyntaxError as exc:
            raise ValueError(f"invalid factor expression: {exc.msg}") from exc
        values = self._evaluate_node(tree.body)
        result = self._as_series(values)
        result = pd.to_numeric(result, errors="coerce").replace([np.inf, -np.inf], np.nan)
        return result.astype(float)

    def _as_series(self, value: Any) -> pd.Series:
        if isinstance(value, pd.Series):
            return value.reindex(self.frame.index)
        if np.isscalar(value):
            return pd.Series(value, index=self.frame.index)
        raise ValueError("factor expression did not produce a one-dimensional signal")

    def _evaluate_node(self, node: ast.AST) -> Any:
        if isinstance(node, ast.Name):
            name = node.id
            if is_forbidden_signal_field(name):
                raise ValueError(f"forbidden future/label field: {name}")
            if name in self.aliases:
                return self._as_series(self.aliases[name])
            if name not in self.frame.columns:
                raise ValueError(f"unknown field in factor expression: {name}")
            return self.frame[name]
        if isinstance(node, ast.Constant):
            if isinstance(node.value, bool) or not isinstance(node.value, (int, float)):
                raise ValueError("only numeric constants are allowed")
            return node.value
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.USub, ast.UAdd)):
            value = self._evaluate_node(node.operand)
            return -value if isinstance(node.op, ast.USub) else value
        if isinstance(node, ast.BinOp) and type(node.op) in _BINARY_OPERATORS:
            left = self._evaluate_node(node.left)
            right = self._evaluate_node(node.right)
            with np.errstate(divide="ignore", invalid="ignore"):
                return _BINARY_OPERATORS[type(node.op)](left, right)
        if isinstance(node, ast.Call):
            if (
                not isinstance(node.func, ast.Name)
                or node.func.id != "rank"
                or len(node.args) != 1
                or node.keywords
            ):
                raise ValueError("only rank(value) calls are allowed")
            values = self._as_series(self._evaluate_node(node.args[0]))
            return values.groupby(self.frame[self.date_column], sort=False).rank(
                method="average", pct=True
            )
        raise ValueError(f"unsupported factor expression syntax: {type(node).__name__}")


def evaluate_expression(
    frame: pd.DataFrame,
    expression: str,
    *,
    date_column: str = "date",
    aliases: Mapping[str, pd.Series] | None = None,
) -> pd.Series:
    """Evaluate an arithmetic/rank expression without using ``eval``."""

    return SafeExpressionEvaluator(
        frame, date_column=date_column, aliases=aliases
    ).evaluate(expression)


def directed_rank_blend(
    frame: pd.DataFrame,
    control: pd.Series,
    challenger: pd.Series,
    *,
    control_direction: int,
    challenger_direction: int,
    challenger_weight: float,
    date_column: str = "date",
) -> pd.Series:
    """Blend two frozen-direction signals using daily percentile ranks.

    A missing control signal always leaves the blend missing. When only the
    challenger is missing, its rank falls back to the control rank so partial
    challenger coverage does not dilute or exclude the control observation.
    """

    if date_column not in frame.columns:
        raise ValueError(f"missing date column: {date_column}")
    for label, direction in (
        ("control_direction", control_direction),
        ("challenger_direction", challenger_direction),
    ):
        if isinstance(direction, (bool, np.bool_)) or direction not in {-1, 1}:
            raise ValueError(f"{label} must be -1 or 1")
    if isinstance(challenger_weight, (bool, np.bool_)):
        raise ValueError("challenger_weight must be between 0 and 1")
    try:
        weight = float(challenger_weight)
    except (TypeError, ValueError) as exc:
        raise ValueError("challenger_weight must be between 0 and 1") from exc
    if not np.isfinite(weight) or not 0.0 <= weight <= 1.0:
        raise ValueError("challenger_weight must be between 0 and 1")

    dates = frame[date_column]

    def _directed_rank(signal: pd.Series, direction: int) -> pd.Series:
        values = pd.to_numeric(
            pd.Series(signal, index=frame.index), errors="coerce"
        ).replace([np.inf, -np.inf], np.nan)
        return (values * direction).groupby(dates, sort=False).rank(
            method="average", pct=True
        )

    control_rank = _directed_rank(control, control_direction)
    challenger_rank = _directed_rank(challenger, challenger_direction)
    effective_challenger = challenger_rank.where(challenger_rank.notna(), control_rank)
    blended = (1.0 - weight) * control_rank + weight * effective_challenger
    return blended.where(control_rank.notna()).astype(float).rename("directed_rank_blend")


def pit_cashflow_quality(
    frame: pd.DataFrame,
    params: Mapping[str, Any] | None = None,
    *,
    date_column: str = "date",
) -> pd.Series:
    """Return a PIT-safe, equal-weight cash-flow quality score.

    This deliberately uses actual point-in-time financial fields rather than
    the legacy ``roe`` proxy. At least two of ROIC, quarterly operating cash
    flow/sales, and low leverage must be available. Report age is a stale-data
    filter only; it is never treated as alpha. Percentile ranks make the
    composite insensitive to vendor unit choices and extreme ratios.
    """

    settings = dict(params or {})
    allowed = {
        "minimum_components",
        "maximum_age_days",
        "industry_neutral",
        "size_neutral",
    }
    unknown = sorted(set(settings) - allowed)
    if unknown:
        raise ValueError("unsupported pit_cashflow_quality params: " + ", ".join(unknown))
    minimum = int(settings.get("minimum_components", 2))
    if minimum not in {2, 3}:
        raise ValueError("pit_cashflow_quality minimum_components must be 2 or 3")
    maximum_age = int(settings.get("maximum_age_days", 550))
    if maximum_age <= 0:
        raise ValueError("pit_cashflow_quality maximum_age_days must be positive")
    industry_neutral = bool(settings.get("industry_neutral", True))
    size_neutral = bool(settings.get("size_neutral", True))
    required = {
        date_column,
        "financial_available_date",
        "fundamental_age_days",
        *FINANCIAL_QUALITY_FIELDS,
    }
    if industry_neutral:
        required.add("industry_pit")
    if size_neutral:
        required.add("total_mv")
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError("pit_cashflow_quality missing fields: " + ", ".join(missing))

    dates = pd.to_datetime(frame[date_column], errors="coerce").dt.normalize()
    available = pd.to_datetime(frame["financial_available_date"], errors="coerce").dt.normalize()
    components = frame.loc[:, FINANCIAL_QUALITY_FIELDS].apply(
        pd.to_numeric, errors="coerce"
    ).replace([np.inf, -np.inf], np.nan)
    ages = pd.to_numeric(frame["fundamental_age_days"], errors="coerce")
    has_financial_data = components.notna().any(axis=1)
    future = has_financial_data & available.notna() & dates.notna() & (available > dates)
    if bool(future.any()):
        raise ValueError("financial PIT availability violation: available_date is after signal date")

    visible = (
        has_financial_data
        & available.notna()
        & dates.notna()
        & (available <= dates)
        & ages.between(0, maximum_age, inclusive="both")
    )
    components = components.where(visible)
    # Higher leverage is lower quality.  All other components are
    # monotonically higher-is-better by contract.
    components["fundamental_debt_to_assets"] = -components[
        "fundamental_debt_to_assets"
    ]
    ranked = components.groupby(dates, sort=False).rank(method="average", pct=True)
    count = ranked.notna().sum(axis=1)
    score = ranked.mean(axis=1, skipna=True).where(count >= minimum).astype(float)

    if industry_neutral or size_neutral:
        industries = (
            frame["industry_pit"].astype("string").str.strip().replace("", pd.NA)
            if industry_neutral
            else pd.Series("all", index=frame.index, dtype="string")
        )
        sizes = (
            pd.to_numeric(frame["total_mv"], errors="coerce").where(lambda value: value > 0)
            if size_neutral
            else pd.Series(1.0, index=frame.index)
        )
        log_size = np.log(sizes)
        residual = pd.Series(np.nan, index=frame.index, dtype=float)
        work = pd.DataFrame(
            {
                "date": dates,
                "score": score,
                "size": log_size,
                "industry": industries,
            }
        )
        for _, group in work.groupby("date", sort=False):
            required_columns = ["score"]
            if size_neutral:
                required_columns.append("size")
            if industry_neutral:
                required_columns.append("industry")
            finite = group.replace([np.inf, -np.inf], np.nan).dropna(
                subset=required_columns
            )
            if len(finite) < 2:
                continue
            y = finite["score"].to_numpy(dtype=float)
            columns = [np.ones(len(finite), dtype=float)]
            if size_neutral:
                size_values = finite["size"].to_numpy(dtype=float)
                columns.append(size_values - size_values.mean())
            if industry_neutral:
                dummies = pd.get_dummies(
                    finite["industry"], drop_first=True, dtype=float
                ).to_numpy(dtype=float)
                columns.extend(dummies[:, column] for column in range(dummies.shape[1]))
            design = np.column_stack(columns)
            coefficients = np.linalg.lstsq(design, y, rcond=None)[0]
            residual.loc[finite.index] = y - design @ coefficients
        score = residual
    return score.astype(float)


DEFAULT_BUILTINS: dict[str, BuiltinSignal] = {
    "pit_cashflow_quality": lambda frame, params: pit_cashflow_quality(frame, params),
    # Compatibility alias for artifacts created during the recovery design.
    "cashflow_quality_pit": lambda frame, params: pit_cashflow_quality(frame, params),
}


def evaluate_factor_signal(
    frame: pd.DataFrame,
    factor: FactorSpec,
    *,
    date_column: str = "date",
    aliases: Mapping[str, pd.Series] | None = None,
    builtins: Mapping[str, BuiltinSignal] | None = None,
) -> pd.Series:
    """Evaluate a registered factor and return a numeric named Series."""

    if factor.kind == "ensemble":
        raise ValueError(
            "ensemble factors require a precomputed runtime signal and cannot be evaluated directly"
        )
    missing = sorted(
        field
        for field in factor.required_fields
        if field not in frame.columns and field not in (aliases or {})
    )
    if missing:
        raise ValueError(f"factor {factor.name} is missing required fields: {missing}")

    if factor.kind == "expression":
        signal = evaluate_expression(
            frame,
            factor.expression or "",
            date_column=date_column,
            aliases=aliases,
        )
    else:
        registry = {**DEFAULT_BUILTINS, **dict(builtins or {})}
        if factor.builtin not in registry:
            raise ValueError(f"unknown builtin factor: {factor.builtin}")
        signal = registry[factor.builtin](frame, factor.params)
        signal = pd.to_numeric(pd.Series(signal, index=frame.index), errors="coerce")
        signal = signal.replace([np.inf, -np.inf], np.nan).astype(float)
    return signal.rename(factor.name)


__all__ = [
    "BuiltinSignal",
    "DEFAULT_BUILTINS",
    "FINANCIAL_QUALITY_FIELDS",
    "SafeExpressionEvaluator",
    "directed_rank_blend",
    "evaluate_expression",
    "evaluate_factor_signal",
    "pit_cashflow_quality",
]

"""Safe expression and builtin signal evaluation."""

from __future__ import annotations

import ast
from collections.abc import Callable, Mapping
from typing import Any

import numpy as np
import pandas as pd

from .contracts import FactorSpec, is_forbidden_signal_field


BuiltinSignal = Callable[[pd.DataFrame, Mapping[str, Any]], pd.Series]
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


def evaluate_factor_signal(
    frame: pd.DataFrame,
    factor: FactorSpec,
    *,
    date_column: str = "date",
    aliases: Mapping[str, pd.Series] | None = None,
    builtins: Mapping[str, BuiltinSignal] | None = None,
) -> pd.Series:
    """Evaluate a registered factor and return a numeric named Series."""

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
        registry = dict(builtins or {})
        if factor.builtin not in registry:
            raise ValueError(f"unknown builtin factor: {factor.builtin}")
        signal = registry[factor.builtin](frame, factor.params)
        signal = pd.to_numeric(pd.Series(signal, index=frame.index), errors="coerce")
        signal = signal.replace([np.inf, -np.inf], np.nan).astype(float)
    return signal.rename(factor.name)

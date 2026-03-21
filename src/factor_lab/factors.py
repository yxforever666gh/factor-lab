from __future__ import annotations

import ast
from dataclasses import dataclass
from typing import Dict

import pandas as pd


@dataclass
class FactorDefinition:
    name: str
    expression: str


_ALLOWED_BINOPS = {
    ast.Add: lambda a, b: a + b,
    ast.Sub: lambda a, b: a - b,
    ast.Mult: lambda a, b: a * b,
    ast.Div: lambda a, b: a / b,
}


class SafeExpressionEvaluator:
    def __init__(self, frame: pd.DataFrame) -> None:
        self.frame = frame
        self.columns: Dict[str, pd.Series] = {
            column: frame[column] for column in frame.columns if column not in {"date", "ticker"}
        }
        # Backward-compatible aliases for factor expressions.
        # Some candidate definitions refer to fields not present in the current feature frame.
        # Provide conservative proxies to avoid hard-stopping the research loop.
        self.aliases: Dict[str, str] = {
            # ROE is not currently materialized by TushareDataProvider; use earnings_yield as a quality proxy.
            "roe": "earnings_yield",
        }

    def evaluate(self, expression: str) -> pd.Series:
        tree = ast.parse(expression, mode="eval")
        return self._eval(tree.body)

    def _eval(self, node):
        if isinstance(node, ast.Name):
            name = node.id
            if name not in self.columns and name in self.aliases:
                alias = self.aliases[name]
                if alias in self.columns:
                    return self.columns[alias]
            if name not in self.columns:
                raise ValueError(f"Unknown field in expression: {name}")
            return self.columns[name]
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
            return -self._eval(node.operand)
        if isinstance(node, ast.BinOp):
            op_type = type(node.op)
            if op_type not in _ALLOWED_BINOPS:
                raise ValueError(f"Unsupported operator: {op_type.__name__}")
            return _ALLOWED_BINOPS[op_type](self._eval(node.left), self._eval(node.right))
        raise ValueError(f"Unsupported syntax in expression: {ast.dump(node)}")


def apply_factor(frame: pd.DataFrame, definition: FactorDefinition) -> pd.Series:
    evaluator = SafeExpressionEvaluator(frame)
    values = evaluator.evaluate(definition.expression)
    return pd.Series(values, index=frame.index, name=definition.name)

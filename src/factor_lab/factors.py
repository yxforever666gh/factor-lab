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

    def evaluate(self, expression: str) -> pd.Series:
        tree = ast.parse(expression, mode="eval")
        return self._eval(tree.body)

    def _eval(self, node):
        if isinstance(node, ast.Name):
            if node.id not in self.columns:
                raise ValueError(f"Unknown field in expression: {node.id}")
            return self.columns[node.id]
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

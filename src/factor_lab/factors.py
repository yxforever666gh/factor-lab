from __future__ import annotations

import ast
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List

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
        self.aliases: Dict[str, str] = {}

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


def expand_factor_family_config(config_path: str | Path) -> list[dict]:
    payload = json.loads(Path(config_path).read_text(encoding='utf-8'))
    expanded: list[dict] = []
    for family_row in payload.get('families', []):
        family = family_row.get('family') or 'other'
        for variant in family_row.get('variants', []):
            expanded.append(
                {
                    'family': family,
                    'name': variant['name'],
                    'expression': variant['expression'],
                    'role': variant.get('role') or 'alpha_seed',
                    'allow_in_portfolio': bool(variant.get('allow_in_portfolio', True)),
                }
            )
    return expanded


def resolve_factor_definitions(config: dict, *, config_dir: str | Path | None = None) -> list[dict]:
    if config.get('factors'):
        return list(config['factors'])
    family_cfg = config.get('factor_family_config')
    if not family_cfg:
        return []
    config_dir = Path(config_dir) if config_dir else Path.cwd()
    family_path = Path(family_cfg)
    if not family_path.is_absolute():
        family_path = (config_dir / family_cfg).resolve()
    return expand_factor_family_config(family_path)

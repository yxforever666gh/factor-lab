from __future__ import annotations

import ast
from dataclasses import dataclass
from typing import Iterable

from factor_lab.feature_schema import EXPRESSION_ALIASES


@dataclass
class ExpressionValidation:
    ok: bool
    unknown_fields: list[str]
    resolved_fields: list[str]


def expression_fields(expression: str) -> list[str]:
    tree = ast.parse(expression, mode="eval")
    names: list[str] = []

    class Visitor(ast.NodeVisitor):
        def visit_Name(self, node: ast.Name):
            names.append(node.id)

    Visitor().visit(tree)
    # Unique but stable order.
    seen = set()
    out = []
    for name in names:
        if name in seen:
            continue
        seen.add(name)
        out.append(name)
    return out


def validate_expression(expression: str, *, available_fields: Iterable[str]) -> ExpressionValidation:
    available = set(available_fields)
    unknown: list[str] = []
    resolved: list[str] = []
    for name in expression_fields(expression):
        if name in available:
            resolved.append(name)
            continue
        alias = EXPRESSION_ALIASES.get(name)
        if alias and alias in available:
            resolved.append(alias)
            continue
        unknown.append(name)
    unknown = sorted(set(unknown))
    return ExpressionValidation(ok=not unknown, unknown_fields=unknown, resolved_fields=resolved)

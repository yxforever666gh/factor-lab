"""Versioned contracts for the lightweight historical research loop."""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Mapping


_FORBIDDEN_EXACT_FIELDS = frozenset(
    {
        "forward_return",
        "forward_return_5d",
        "forward_return_5d_open",
        "future_return",
        "label",
        "target",
        "y",
    }
)
_FORBIDDEN_PREFIXES = ("forward_", "future_", "label_", "target_")


def is_forbidden_signal_field(name: str) -> bool:
    """Return whether *name* is reserved for a future value or label."""

    normalized = str(name).strip().casefold()
    return normalized in _FORBIDDEN_EXACT_FIELDS or normalized.startswith(
        _FORBIDDEN_PREFIXES
    )


def referenced_expression_fields(expression: str) -> tuple[str, ...]:
    """Extract signal fields and reject known future/label references.

    Full syntax validation lives in :mod:`factor_lab.research.signals`; this
    lightweight pass makes an invalid field fail as soon as a ``FactorSpec``
    is registered.
    """

    try:
        tree = ast.parse(expression, mode="eval")
    except SyntaxError as exc:
        raise ValueError(f"invalid factor expression: {exc.msg}") from exc
    fields = tuple(
        sorted(
            {
                node.id
                for node in ast.walk(tree)
                if isinstance(node, ast.Name) and node.id != "rank"
            }
        )
    )
    forbidden = [name for name in fields if is_forbidden_signal_field(name)]
    if forbidden:
        raise ValueError(
            "factor references forbidden future/label fields: "
            + ", ".join(forbidden)
        )
    return fields


@dataclass(frozen=True, slots=True)
class FactorSpec:
    """A pre-registered signal definition.

    ``direction_policy`` is intentionally fixed to ``train_ic``.  Historical
    configuration may describe a preferred sign, but validation must learn and
    freeze the executable direction using the 2017--2022 training segment.
    """

    name: str
    family: str
    expression: str | None = None
    kind: str = "expression"
    builtin: str | None = None
    required_fields: tuple[str, ...] = ()
    direction_policy: str = "train_ic"
    params: Mapping[str, Any] = field(default_factory=dict)
    role: str = "challenger"

    def __post_init__(self) -> None:
        name = self.name.strip()
        family = self.family.strip()
        kind = self.kind.strip().casefold()
        if not name:
            raise ValueError("factor name must not be empty")
        if not family:
            raise ValueError("factor family must not be empty")
        if kind not in {"expression", "builtin"}:
            raise ValueError("factor kind must be 'expression' or 'builtin'")
        if self.direction_policy != "train_ic":
            raise ValueError("direction_policy must be 'train_ic'")

        expression = self.expression.strip() if self.expression else None
        builtin = self.builtin.strip() if self.builtin else None
        if kind == "expression":
            if not expression:
                raise ValueError("expression factors need a non-empty expression")
            if builtin:
                raise ValueError("expression factors cannot also define builtin")
            inferred = referenced_expression_fields(expression)
            required = (*inferred, *self.required_fields)
        else:
            if not builtin:
                raise ValueError("builtin factors need a builtin name")
            if expression:
                raise ValueError("builtin factors cannot also define expression")
            required = self.required_fields
            if not required:
                raise ValueError("builtin factors must declare required_fields")

        normalized_fields = tuple(dict.fromkeys(str(item).strip() for item in required))
        if any(not item for item in normalized_fields):
            raise ValueError("required_fields cannot contain empty names")
        forbidden = [item for item in normalized_fields if is_forbidden_signal_field(item)]
        if forbidden:
            raise ValueError(
                "factor requires forbidden future/label fields: " + ", ".join(forbidden)
            )

        object.__setattr__(self, "name", name)
        object.__setattr__(self, "family", family)
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "expression", expression)
        object.__setattr__(self, "builtin", builtin)
        object.__setattr__(self, "required_fields", normalized_fields)
        object.__setattr__(self, "params", dict(self.params))
        object.__setattr__(self, "role", self.role.strip() or "challenger")

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "FactorSpec":
        return cls(
            name=str(payload.get("name") or ""),
            family=str(payload.get("family") or "other"),
            expression=payload.get("expression"),
            kind=str(payload.get("kind") or ("builtin" if payload.get("builtin") else "expression")),
            builtin=payload.get("builtin"),
            required_fields=tuple(payload.get("required_fields") or ()),
            direction_policy=str(payload.get("direction_policy") or "train_ic"),
            params=dict(payload.get("params") or {}),
            role=str(payload.get("role") or "challenger"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "family": self.family,
            "kind": self.kind,
            "expression": self.expression,
            "builtin": self.builtin,
            "required_fields": list(self.required_fields),
            "direction_policy": self.direction_policy,
            "params": dict(self.params),
            "role": self.role,
        }


@dataclass(frozen=True, slots=True)
class ValidationSpec:
    """Frozen historical split and Stage-A screening policy."""

    train_start: str = "2017-01-01"
    train_end: str = "2022-12-31"
    validation_start: str = "2023-01-01"
    validation_end: str = "2024-12-31"
    audit_start: str = "2025-01-01"
    holding_days: int = 5
    min_cross_section: int = 10
    min_evaluable_ratio: float = 0.80
    min_median_coverage: float = 0.80
    max_challengers: int = 3
    date_column: str = "date"
    label_columns: tuple[str, ...] = (
        "forward_return_5d_open",
        "forward_return_5d",
    )

    def __post_init__(self) -> None:
        bounds = [
            date.fromisoformat(self.train_start),
            date.fromisoformat(self.train_end),
            date.fromisoformat(self.validation_start),
            date.fromisoformat(self.validation_end),
            date.fromisoformat(self.audit_start),
        ]
        if not (bounds[0] <= bounds[1] < bounds[2] <= bounds[3] < bounds[4]):
            raise ValueError("validation windows must be ordered and non-overlapping")
        if self.holding_days <= 0:
            raise ValueError("holding_days must be positive")
        if self.min_cross_section < 3:
            raise ValueError("min_cross_section must be at least 3")
        for name, value in (
            ("min_evaluable_ratio", self.min_evaluable_ratio),
            ("min_median_coverage", self.min_median_coverage),
        ):
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be between 0 and 1")
        if not 1 <= self.max_challengers <= 3:
            raise ValueError("max_challengers must be between 1 and 3")
        if not self.date_column.strip():
            raise ValueError("date_column must not be empty")
        if not self.label_columns:
            raise ValueError("at least one diagnostic label column is required")

    def to_dict(self) -> dict[str, Any]:
        return {
            "train": {"start": self.train_start, "end": self.train_end},
            "validation": {
                "start": self.validation_start,
                "end": self.validation_end,
            },
            "audit": {"start": self.audit_start, "end": None},
            "holding_days": self.holding_days,
            "min_cross_section": self.min_cross_section,
            "min_evaluable_ratio": self.min_evaluable_ratio,
            "min_median_coverage": self.min_median_coverage,
            "max_challengers": self.max_challengers,
            "date_column": self.date_column,
            "label_columns": list(self.label_columns),
        }

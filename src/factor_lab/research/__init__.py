"""Lightweight, deterministic factor-research primitives.

This package deliberately contains no database, orchestration, web, or
production lifecycle dependencies.  It is safe to import in the minimal CLI.
"""

from .contracts import FactorSpec, ValidationSpec
from .signals import evaluate_factor_signal, evaluate_expression
from .validation import (
    FactorValidation,
    WindowDiagnostics,
    evaluate_stage_a,
    select_stage_b,
)

__all__ = [
    "FactorSpec",
    "ValidationSpec",
    "WindowDiagnostics",
    "FactorValidation",
    "evaluate_expression",
    "evaluate_factor_signal",
    "evaluate_stage_a",
    "select_stage_b",
]

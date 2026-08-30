"""Lightweight, deterministic factor-research primitives.

This package deliberately contains no database, orchestration, web, or
production lifecycle dependencies.  It is safe to import in the minimal CLI.
"""

from .contracts import FactorSpec, ValidationSpec
from .signals import evaluate_factor_signal, evaluate_expression, pit_cashflow_quality
from .validation import (
    BootstrapInterval,
    FactorValidation,
    SignalSimilarity,
    StageASelection,
    WindowDiagnostics,
    build_stage_a_selection,
    deterministic_block_bootstrap_mean,
    diagnose_train_similarity,
    evaluate_stage_a,
    select_stage_b,
)
from .wide_universe import (
    CHALLENGER_IDS,
    CONTROL_ID,
    PhaseBounds,
    build_target_decisions,
    candidate_gate,
    select_winner,
)

__all__ = [
    "CHALLENGER_IDS",
    "CONTROL_ID",
    "FactorSpec",
    "PhaseBounds",
    "ValidationSpec",
    "WindowDiagnostics",
    "FactorValidation",
    "BootstrapInterval",
    "SignalSimilarity",
    "StageASelection",
    "evaluate_expression",
    "evaluate_factor_signal",
    "pit_cashflow_quality",
    "evaluate_stage_a",
    "select_stage_b",
    "build_stage_a_selection",
    "build_target_decisions",
    "candidate_gate",
    "diagnose_train_similarity",
    "deterministic_block_bootstrap_mean",
    "select_winner",
]

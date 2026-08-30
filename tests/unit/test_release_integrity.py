from __future__ import annotations

import pytest

from factor_lab.release_integrity import _winner_from_frozen_gates
from factor_lab.research.wide_universe import CHALLENGER_IDS


def _passed_gate() -> dict:
    return {
        "passed": True,
        "paired_relative_cagr": {"q20": 0.01, "median": 0.02},
        "worst_capacity_limited_requested_notional_ratio": 0.01,
    }


def test_frozen_gate_replay_recomputes_unique_winner() -> None:
    freeze = {
        "train_passers": list(CHALLENGER_IDS),
        "train_gates": {candidate: _passed_gate() for candidate in CHALLENGER_IDS},
        "validation_gates": {
            candidate: _passed_gate() for candidate in CHALLENGER_IDS
        },
        "turnover_by_candidate": {
            CHALLENGER_IDS[0]: 0.12,
            CHALLENGER_IDS[1]: 0.10,
        },
    }

    assert _winner_from_frozen_gates(freeze) == CHALLENGER_IDS[1]


def test_frozen_gate_replay_rejects_passers_inconsistent_with_gates() -> None:
    freeze = {
        "train_passers": [CHALLENGER_IDS[0]],
        "train_gates": {candidate: _passed_gate() for candidate in CHALLENGER_IDS},
        "validation_gates": {CHALLENGER_IDS[0]: _passed_gate()},
        "turnover_by_candidate": {CHALLENGER_IDS[0]: 0.10},
    }

    with pytest.raises(ValueError, match="train passers"):
        _winner_from_frozen_gates(freeze)

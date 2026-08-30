from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from factor_lab.release_integrity import (
    _verify_integrated_prior_amendment,
    _verify_prior_release,
    _winner_from_frozen_gates,
    verify_frozen_runtime_contract,
    verify_wide_protocol_contract,
)
from factor_lab.research.wide_universe import CHALLENGER_IDS


ROOT = Path(__file__).resolve().parents[2]


def _read_json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def test_6_2_binds_the_published_6_1_pre_return_failure() -> None:
    protocol = _read_json(ROOT / "protocols/6.2-wide-universe.json")

    failure = _verify_prior_release(ROOT, protocol["prior_release"])

    assert failure["status"] == "pre_return_data_admission_failed"
    assert failure["opening_state"]["portfolio_returns_opened"] is False


def test_6_2_protocol_and_code_share_the_exact_admission_contract() -> None:
    protocol = _read_json(ROOT / "protocols/6.2-wide-universe.json")

    verify_wide_protocol_contract(protocol)
    changed = copy.deepcopy(protocol)
    changed["common_base"]["finite_score_admission"]["per_signal_per_arm"][
        "finite_score_count_min"
    ] = 24

    with pytest.raises(ValueError, match="finite-score admission"):
        verify_wide_protocol_contract(changed)


def test_6_2_runtime_capsule_has_the_exact_static_identity() -> None:
    runtime = _read_json(ROOT / "protocols/6.2-runtime.json")

    verify_frozen_runtime_contract(runtime)
    changed = copy.deepcopy(runtime)
    changed["source_package_version"] = "6.1.0"

    with pytest.raises(ValueError, match="runtime identity"):
        verify_frozen_runtime_contract(changed)


def test_6_2_exactly_integrates_the_published_6_1_red_team_amendment() -> None:
    amendment = _read_json(
        ROOT / "protocols/6.2-wide-universe-amendment-1.json"
    )

    prior = _verify_integrated_prior_amendment(ROOT, amendment)

    assert amendment["effective_overrides"] == prior["effective_overrides"]
    assert amendment["required_tests_add"] == prior["required_tests_add"]


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

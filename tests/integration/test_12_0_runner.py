from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "run-12.0-pit-stock.py"


def _module():
    spec = importlib.util.spec_from_file_location("factor_lab_12_0_runner", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _role(
    *, cagr: float = 0.08, train: float = 0.10, validation: float = 0.02,
    sharpe: float = 0.5, drawdown: float = -0.3,
) -> dict:
    return {
        "cagr": cagr,
        "sharpe": sharpe,
        "max_drawdown": drawdown,
        "train_quarterly": {"cagr": train},
        "validation_quarterly": {"cagr": validation},
        "requested_notional_fill_ratio": 0.999,
        "capacity_limited_requested_notional_ratio": 0.0,
        "capacity_violation_count": 0,
        "negative_cash_observation_count": 0,
        "leverage_observation_count": 0,
        "max_nav_reconciliation_error": 0.0,
    }


def test_runner_protocol_binding_and_defaults() -> None:
    module = _module()
    assert module._read_protocol()["payload_sha256"] == module.PROTOCOL_PAYLOAD_SHA256
    assert module.DEFAULT_OUTPUT.name == "development"
    assert module.DEFAULT_SCREENING_OUTPUT.name == "development-screening"


def test_screening_gate_fails_only_frozen_drawdown_check_for_exact_metrics() -> None:
    module = _module()
    metrics = {
        "candidate_base": _role(drawdown=-0.4005292245704233),
        "candidate_stress": _role(cagr=0.078, sharpe=0.50, drawdown=-0.402),
        "adv500_base": _role(cagr=-0.08, train=-0.06, validation=-0.11, sharpe=-0.2, drawdown=-0.42),
    }
    result = module._screening_gate(metrics)
    failed = [name for name, passed in result["checks"].items() if not passed]
    assert failed == ["base_max_drawdown_at_least_negative_0_35"]
    assert result["pre_attribution_passed"] is False
    assert result["winner_freeze_allowed"] is False

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd
import pytest

from factor_lab.portfolio import LongOnlyPortfolioConfig
from factor_lab.research.contracts import FactorSpec
from factor_lab.research.validation import FactorValidation, WindowDiagnostics
from factor_lab.research.walk_forward_runtime import (
    _decisions_sha256,
    _fixed_comparator_protocol,
    _selection_frequency,
    run_walk_forward_sweep,
)


def _window(split: str) -> WindowDiagnostics:
    return WindowDiagnostics(
        split=split,
        start="2025-01-01",
        end=None if split == "audit" else "2025-12-31",
        expected_date_count=4,
        evaluable_date_count=4,
        evaluable_date_ratio=1.0,
        median_cross_section_coverage=1.0,
        rank_ic_mean=0.25,
        rank_ic_std=0.10,
        signed_rank_ic_mean=0.25,
    )


def _validation(name: str, family: str) -> FactorValidation:
    return FactorValidation(
        factor_name=name,
        family=family,
        frozen_direction=1,
        label_column="future_return_1d",
        train=_window("train"),
        validation=_window("validation"),
        audit=_window("audit"),
        direction_consistent=True,
        stage_b_eligible=True,
        blockers=(),
        selection_basis="fixed_ex_ante_direction",
    )


def _period(
    signal_date: pd.Timestamp, end_date: pd.Timestamp, net_return: float
) -> dict[str, Any]:
    return {
        "signal_date": signal_date.date().isoformat(),
        "end_date": end_date.date().isoformat(),
        "net_return": net_return,
        "benchmark_return": 0.0,
        "active_return": net_return,
    }


def test_selection_frequency_separates_date_membership_from_deployed_weight() -> None:
    counts, frequency = _selection_frequency(
        {
            "selections": [
                {
                    "signal_date": "2025-01-02",
                    "selected_factors": ["control"],
                    "selected_weights": {"control": 1.0},
                },
                {
                    "signal_date": "2025-01-03",
                    "selected_factors": ["candidate_a", "candidate_b"],
                    "selected_weights": {"candidate_a": 0.5, "candidate_b": 0.5},
                },
            ]
        },
        ["control", "candidate_a", "candidate_b"],
    )

    assert counts == {"control": 1, "candidate_a": 1, "candidate_b": 1}
    assert frequency == {
        "control": {
            "selected_date_count": 1,
            "selected_date_ratio": 0.5,
            "mean_deployed_weight": 0.5,
        },
        "candidate_a": {
            "selected_date_count": 1,
            "selected_date_ratio": 0.5,
            "mean_deployed_weight": 0.25,
        },
        "candidate_b": {
            "selected_date_count": 1,
            "selected_date_ratio": 0.5,
            "mean_deployed_weight": 0.25,
        },
    }


@pytest.mark.parametrize(
    "override",
    [
        {"name": "tuned_winner"},
        {"weighting": "score_weighted"},
        {"missing_signal_policy": "drop"},
        {"candidate_subset": ["control"]},
    ],
)
def test_fixed_comparator_protocol_rejects_tunable_variants(
    override: Mapping[str, Any],
) -> None:
    with pytest.raises(ValueError, match="fixed_comparator"):
        _fixed_comparator_protocol({"fixed_comparator": override})


def test_sweep_rejects_stale_dynamic_decisions_and_sanitizes_runtime_metadata(
    tmp_path: Path,
) -> None:
    dates = pd.bdate_range("2025-01-02", periods=6)
    features = pd.DataFrame(
        {
            "date": dates,
            "ticker": ["A"] * len(dates),
        }
    )
    control = FactorSpec(
        name="control",
        family="value",
        expression="x",
        direction_policy="fixed",
        params={"fixed_direction": 1},
        role="control",
    )
    candidate = FactorSpec(
        name="candidate",
        family="defensive",
        expression="-volatility_20",
        direction_policy="fixed",
        params={"fixed_direction": 1},
    )
    validations = {
        control.name: _validation(control.name, control.family),
        candidate.name: _validation(candidate.name, candidate.family),
    }
    control_periods = [
        _period(dates[0], dates[1], 0.01),
        _period(dates[1], dates[2], -0.01),
        _period(dates[3], dates[4], 0.02),
        _period(dates[4], dates[5], 0.01),
    ]
    candidate_periods = [
        _period(dates[0], dates[1], 0.03),
        _period(dates[1], dates[2], 0.01),
        _period(dates[3], dates[4], 0.04),
        _period(dates[4], dates[5], 0.02),
    ]
    base_results = [
        {"factor_name": control.name, "period_active_returns": control_periods},
        {"factor_name": candidate.name, "period_active_returns": candidate_periods},
    ]
    research_config = {
        "walk_forward": {
            "rebalance_offsets": [0],
            "phase_quantile": 0.2,
            "fixed_comparator": {
                "name": "fixed_registry_equal_weight",
                "weighting": "equal",
                "missing_signal_policy": "fallback_control",
            },
            "selector": {
                "lookback_trading_days": 5,
                "minimum_completed_periods": 2,
                "update_every_trading_days": 1,
                "control_score_guard": 0.0,
                "selection_count": 2,
            },
        }
    }
    config = LongOnlyPortfolioConfig(
        holding_days=1,
        rebalance_every_days=1,
        rebalance_offset_days=0,
        position_count=1,
        target_weight=1.0,
        periods_per_year=252.0,
    )
    output_dir = tmp_path / "run"
    dynamic_path = output_dir / "walk-forward" / "offset-00" / "dynamic.json"
    dynamic_path.parent.mkdir(parents=True)
    stale_decisions = {
        "selections": [
            {
                "signal_date": dates[0].date().isoformat(),
                "selected_factor": "candidate",
            }
        ]
    }
    dynamic_path.write_text(
        json.dumps(
            {
                "run_fingerprint": "fingerprint",
                "rebalance_offset_days": 0,
                "role": "causal_deployed_account",
                "decisions_sha256": _decisions_sha256(stale_decisions),
                "decisions": stale_decisions,
                "result": {
                    "factor_name": "causal_walk_forward_dynamic",
                    "period_active_returns": control_periods,
                },
            }
        ),
        encoding="utf-8",
    )

    portfolio_calls: list[str] = []
    portfolio_signals: dict[str, list[float]] = {}
    fixed_comparator_periods = [
        {**row, "net_return": 0.05, "active_return": 0.05}
        for row in control_periods
    ]

    def portfolio_result(
        factor: FactorSpec,
        validation: FactorValidation,
        signal: pd.Series,
        *_args: Any,
        **_kwargs: Any,
    ) -> dict[str, Any]:
        portfolio_calls.append(factor.name)
        portfolio_signals[factor.name] = signal.tolist()
        periods = (
            fixed_comparator_periods
            if factor.name == "fixed_registry_equal_weight"
            else control_periods
        )
        return {
            "factor_name": factor.name,
            "factor": factor.to_dict(),
            "stage_a": validation.to_dict(),
            "windows": {
                "train": {
                    "signal_evaluable_date_ratio": 1.0,
                    "signal_median_cross_section_coverage": 1.0,
                }
            },
            "gate_passed": True,
            "gate_blockers": [],
            "audit_role": "falsification_only",
            "audit_status": "not_falsified",
            "audit_falsified": False,
            "audit_falsification_reasons": [],
            "validated": True,
            "period_active_returns": periods,
        }

    def historical_metrics(
        result: Mapping[str, Any],
        _research_config: Mapping[str, Any],
        *,
        periods_per_year: float,
        reference_periods: Sequence[Mapping[str, Any]],
        optimization_scope: str,
    ) -> dict[str, Any]:
        del periods_per_year, optimization_scope
        observed_by_date = {
            str(row["signal_date"]): row
            for row in result.get("period_active_returns") or []
        }
        aligned = [
            observed_by_date[str(row["signal_date"])]
            for row in reference_periods
            if str(row["signal_date"]) in observed_by_date
        ]
        annual_return = sum(float(row["net_return"]) for row in aligned) / len(
            aligned
        )
        return {
            "observations": len(reference_periods),
            "period_coverage": len(aligned) / len(reference_periods),
            "net_annual_return": annual_return,
            "net_sharpe": annual_return * 10.0,
            "information_ratio": annual_return * 5.0,
            "max_drawdown": -0.1,
        }

    summary, dynamic_result = run_walk_forward_sweep(
        factors=[control, candidate],
        validations=validations,
        signals={
            control.name: pd.Series(range(len(features)), dtype=float),
            candidate.name: pd.Series([6.0, None, 4.0, 3.0, 2.0, 1.0]),
        },
        features=features,
        execution=pd.DataFrame(),
        base_config=config,
        research_config=research_config,
        base_results=base_results,
        control=control,
        output_dir=output_dir,
        run_fingerprint="fingerprint",
        resume=True,
        portfolio_result=portfolio_result,
        historical_metrics=historical_metrics,
    )

    assert portfolio_calls == [
        "fixed_registry_equal_weight",
        "causal_walk_forward_dynamic",
    ]
    assert portfolio_signals["fixed_registry_equal_weight"] == pytest.approx(
        [3.0, 1.0, 3.0, 3.0, 3.0, 3.0]
    )
    assert summary["selector_executed"] is True
    assert summary["dynamic_status"] == "experimental_account"
    assert summary["control_phase_ranking_eligible"] is True
    assert summary["dynamic_control_common_offset_count"] == 1
    assert "historically_reliable" not in summary
    assert summary["historical_diagnostic_passed"] is False
    assert summary["fixed_comparator"]["factor_name"] == (
        "fixed_registry_equal_weight"
    )
    assert summary["fixed_comparator"]["uses_realized_returns"] is False
    assert (
        summary["fixed_comparator"]["dynamic_phase_deltas"][
            "net_annual_return"
        ]["q20"]
        < 0.0
    )
    assert (
        summary["fixed_comparator"][
            "dynamic_positive_annual_return_delta_ratio"
        ]
        == 0.0
    )
    fixed_ranking = next(
        row
        for row in summary["phase_rankings"]
        if row["strategy_name"] == "fixed_registry_equal_weight"
    )
    assert fixed_ranking["strategy_kind"] == "fixed_registry_comparator"
    assert dynamic_result["stage_a"]["diagnostic_status"] == "unavailable"
    assert dynamic_result["stage_a"]["diagnostic_reason"] == (
        "runtime_composed_signal"
    )
    assert dynamic_result["stage_a"]["train"] == {
        "split": "train",
        "start": "2025-01-01",
        "end": "2025-12-31",
        "status": "unavailable",
        "reason": "runtime_composed_signal",
    }
    assert dynamic_result["windows"]["train"]["signal_evaluable_date_ratio"] is None
    assert dynamic_result["gate_passed"] is None
    assert dynamic_result["gate_status"] == "not_applicable_runtime_composed"
    assert dynamic_result["validated"] is False

    dynamic_payload = json.loads(dynamic_path.read_text(encoding="utf-8"))
    audit_payload = json.loads(
        (dynamic_path.parent / "decisions.json").read_text(encoding="utf-8")
    )
    assert dynamic_payload["decisions"] == audit_payload["decisions"]
    assert dynamic_payload["decisions"] != stale_decisions
    assert dynamic_payload["decisions_sha256"] == _decisions_sha256(
        dynamic_payload["decisions"]
    )
    assert dynamic_payload["decisions_sha256"] == audit_payload["decisions_sha256"]
    fixed_payload = json.loads(
        (dynamic_path.parent / "fixed-comparator.json").read_text(encoding="utf-8")
    )
    assert fixed_payload["role"] == "fixed_registry_comparator"
    assert fixed_payload["candidate_registry"] == ["control", "candidate"]
    assert fixed_payload["result"]["factor_name"] == (
        "fixed_registry_equal_weight"
    )
    assert summary["selection_frequency"]
    assert sum(
        row["mean_deployed_weight"]
        for row in summary["selection_frequency"].values()
    ) == pytest.approx(1.0)

    def unexpected_portfolio_result(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise AssertionError("matching decision-bound dynamic cache should be reused")

    resumed, _ = run_walk_forward_sweep(
        factors=[control, candidate],
        validations=validations,
        signals={
            control.name: pd.Series(range(len(features)), dtype=float),
            candidate.name: pd.Series([6.0, None, 4.0, 3.0, 2.0, 1.0]),
        },
        features=features,
        execution=pd.DataFrame(),
        base_config=config,
        research_config=research_config,
        base_results=base_results,
        control=control,
        output_dir=output_dir,
        run_fingerprint="fingerprint",
        resume=True,
        portfolio_result=unexpected_portfolio_result,
        historical_metrics=historical_metrics,
    )
    assert resumed["offsets"][0]["decisions_sha256"] == dynamic_payload[
        "decisions_sha256"
    ]

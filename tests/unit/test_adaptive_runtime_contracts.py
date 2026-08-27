from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd
import pytest

from factor_lab.portfolio import LongOnlyPortfolioConfig
from factor_lab.research import adaptive_runtime
from factor_lab.research.adaptive_runtime import (
    _combined_target_weights,
    _assert_no_ordering_schema_keys,
    _cache_result,
    _determine_route,
    _evaluate_frozen_gates,
    _normalized_target_schedule,
    _result_sha256,
    run_adaptive_sweep,
)
from factor_lab.research.contracts import FactorSpec
from factor_lab.research.validation import FactorValidation, WindowDiagnostics


ACCOUNT_NAMES = (
    "fixed_core_full",
    "fixed_core_overlay",
    "static_prior_full",
    "online_full",
    "online_overlay",
)


def test_explicit_empty_combined_target_is_preserved_as_full_cash() -> None:
    assert _combined_target_weights({"combined_target_weights": {}}) == {}


def _window(split: str) -> WindowDiagnostics:
    return WindowDiagnostics(
        split=split,
        start="2017-01-01",
        end=None if split == "audit" else "2025-12-31",
        expected_date_count=10,
        evaluable_date_count=10,
        evaluable_date_ratio=1.0,
        median_cross_section_coverage=1.0,
        rank_ic_mean=0.1,
        rank_ic_std=0.1,
        signed_rank_ic_mean=0.1,
    )


def _validation(name: str) -> FactorValidation:
    return FactorValidation(
        factor_name=name,
        family="adaptive_test",
        frozen_direction=1,
        label_column="future_return_5d",
        train=_window("train"),
        validation=_window("validation"),
        audit=_window("audit"),
        direction_consistent=True,
        stage_b_eligible=True,
        blockers=(),
        selection_basis="fixed_ex_ante_direction",
    )


def _comparison(
    annual: float = 0.0,
    sharpe: float = 0.0,
    drawdown: float = 0.0,
    positive_ratio: float = 0.8,
) -> dict[str, Any]:
    phase = {
        "net_annual_return": {"q20": annual},
        "net_sharpe": {"q20": sharpe},
        "information_ratio": {"q20": 0.0},
        "max_drawdown": {"q20": drawdown},
    }
    return {
        "phase_deltas": phase,
        "positive_annual_return_delta_ratio": positive_ratio,
    }


def test_frozen_exclusive_minimum_is_strict_and_route_has_only_three_branches() -> None:
    gates = {
        "core_overlay": {
            "paired_q20_net_annual_return_delta_min": 0.0,
            "paired_q20_net_sharpe_delta_min": 0.0,
            "paired_q20_max_drawdown_delta_min": 0.0,
            "positive_annual_return_delta_ratio_min": 0.8,
            "mean_fraction_signal_dates_exposure_below_one_min": 0.1,
        },
        "online_vs_static": {
            "paired_q20_net_annual_return_delta_exclusive_min": 0.0,
            "paired_q20_net_sharpe_delta_min": 0.0,
            "paired_q20_max_drawdown_delta_min": -0.02,
            "positive_annual_return_delta_ratio_min": 0.8,
        },
        "online_overlay_effect": {
            "paired_q20_net_annual_return_delta_min": 0.0,
            "paired_q20_net_sharpe_delta_min": 0.0,
            "paired_q20_max_drawdown_delta_min": 0.0,
            "positive_annual_return_delta_ratio_min": 0.8,
            "mean_fraction_signal_dates_exposure_below_one_min": 0.1,
        },
        "combined": {
            "paired_q20_net_annual_return_delta_exclusive_min": 0.0,
            "paired_q20_net_sharpe_delta_min": 0.0,
            "paired_q20_max_drawdown_delta_min": -0.02,
            "positive_annual_return_delta_ratio_min": 0.8,
        },
    }
    comparisons = {name: _comparison() for name in gates}

    observed = _evaluate_frozen_gates(
        gates, comparisons, mean_overlay_fraction=0.1
    )

    assert observed["core_overlay"]["passed"] is True
    assert observed["online_overlay_effect"]["passed"] is True
    assert observed["online_vs_static"]["passed"] is False
    assert observed["combined"]["passed"] is False
    exclusive = observed["online_vs_static"]["criteria"][0]
    assert exclusive["operator"] == ">"
    assert exclusive["observed"] == exclusive["threshold"] == 0.0
    assert _determine_route(observed, integrity_passed=True)["selected_account"] == (
        "fixed_core_overlay"
    )

    comparisons["online_vs_static"] = _comparison(annual=1e-8)
    comparisons["combined"] = _comparison(annual=1e-8)
    all_pass = _evaluate_frozen_gates(
        gates, comparisons, mean_overlay_fraction=0.1
    )
    assert _determine_route(all_pass, integrity_passed=True)["selected_account"] == (
        "online_overlay"
    )

    core_fail = dict(all_pass)
    core_fail["core_overlay"] = {"passed": False}
    assert _determine_route(core_fail, integrity_passed=True)["selected_account"] == (
        "fixed_core_full"
    )
    invalid = _determine_route(all_pass, integrity_passed=False)
    assert invalid["evaluated"] is False
    assert invalid["selected_account"] is None


def test_ordering_schema_guard_does_not_reject_frozen_expert_names() -> None:
    _assert_no_ordering_schema_keys(
        {
            "expert_weights": {
                "causal_blend__earnings_yield_over_pb__value_defensive_rank__w0p7": 1.0
            }
        }
    )
    with pytest.raises(ValueError, match="forbidden ordering key"):
        _assert_no_ordering_schema_keys({"ranking_available": False})


def test_combined_schedule_preserves_overlapping_weight_above_expert_cap() -> None:
    schedule = _normalized_target_schedule(
        {
            "2025-01-02": {
                "overlap": 0.625,
                "other": 0.375,
            }
        },
        maximum_position_count=40,
    )

    assert schedule["2025-01-02"] == {"other": 0.375, "overlap": 0.625}
    with pytest.raises(ValueError, match="100% funding"):
        _normalized_target_schedule(
            {"2025-01-02": {"overlap": 0.7, "other": 0.4}},
            maximum_position_count=40,
        )


def test_real_adaptive_bridge_preserves_strict_history_and_decision_schema() -> None:
    protocol_path = Path(__file__).resolve().parents[2] / "protocols" / "5.0.json"
    protocol = json.loads(protocol_path.read_text(encoding="utf-8"))
    expert_names = tuple(protocol["experts"]["ordered_registry"])
    dates = pd.bdate_range("2020-01-02", periods=430)
    signal_dates = (dates[399], dates[405], dates[410])
    boundaries = (
        (dates[400], dates[405]),
        (dates[406], dates[409]),
        (dates[411], dates[415]),
    )
    feature_rows = [
        {
            "date": date_value,
            "ticker": ticker,
            "return_1d": 0.001,
            "momentum_120": 1.0,
        }
        for date_value in dates
        for ticker in ("A", "B")
    ]
    shadow_results: dict[str, dict[str, Any]] = {}
    for expert_index, expert_name in enumerate(expert_names):
        periods = [
            {
                "signal_date": signal_date.date().isoformat(),
                "start_date": start.date().isoformat(),
                "end_date": end.date().isoformat(),
                "net_return": 0.001 * (expert_index + 1),
            }
            for signal_date, (start, end) in zip(
                signal_dates, boundaries, strict=True
            )
        ]
        shadow_results[expert_name] = {
            "factor_name": expert_name,
            "period_active_returns": periods,
            "period_target_weights": [
                {
                    "signal_date": row["signal_date"],
                    "start_date": row["start_date"],
                    "end_date": row["end_date"],
                    "target_weights": {f"T{expert_index}": 1.0},
                }
                for row in periods
            ],
        }
    frozen = {
        "expert_names": expert_names,
        "fixed_core": protocol["experts"]["fixed_core"],
        "allocator": protocol["online_allocator"],
        "prior_weights": dict(
            zip(
                expert_names,
                protocol["online_allocator"]["static_prior_total_weights"],
                strict=True,
            )
        ),
    }

    observed = adaptive_runtime._build_offset_decisions(
        protocol=protocol,
        frozen=frozen,
        features=pd.DataFrame(feature_rows),
        shadow_results=shadow_results,
        offset=0,
    )

    online = observed["public_decisions"]["online_allocations"]
    assert [row["matured_cohort_count"] for row in online] == [0, 0, 2]
    assert [row["excluded_unmatured_cohort_count"] for row in online] == [1, 2, 1]
    assert online[1]["latest_matured_end_date"] is None
    assert online[2]["latest_matured_end_date"] == dates[409].date().isoformat()
    assert all(row["history_policy"] == protocol["experts"]["shadow_history_policy"] for row in online)
    assert all(row["future_feedback_violation_count"] == 0 for row in online)
    assert observed["future_feedback_violation_count"] == 0
    overlay = observed["public_decisions"]["market_overlay"]
    assert all(row["ready"] is True for row in overlay)
    assert [row["latest_input_date"] for row in overlay] == [
        value.date().isoformat() for value in signal_dates
    ]
    assert all(row["future_overlay_violation_count"] == 0 for row in overlay)
    assert observed["ready_date"] == dates[405].date().isoformat()
    assert set(observed["account_schedules"]) == set(ACCOUNT_NAMES)
    json.dumps(observed["public_decisions"], allow_nan=False)
    _assert_no_ordering_schema_keys(observed["public_decisions"])


def test_scoring_cache_is_bound_to_protocol_decisions_and_target_schedule() -> None:
    result = {
        "factor_name": "online_overlay",
        "account_nav_path": [{"date": "2025-01-02", "nav": 1.0}],
    }
    payload = {
        "run_fingerprint": "f" * 64,
        "protocol_sha256": "a" * 64,
        "rebalance_offset_days": 3,
        "role": "equal_aum_adaptive_scoring_account",
        "evaluation_start_date": "2025-01-02",
        "decisions_sha256": "b" * 64,
        "target_schedule_sha256": "c" * 64,
        "result_sha256": _result_sha256(result),
        "result": result,
    }
    kwargs = {
        "run_fingerprint": "f" * 64,
        "protocol_sha256": "a" * 64,
        "offset": 3,
        "role": "equal_aum_adaptive_scoring_account",
        "factor_name": "online_overlay",
        "evaluation_start_date": "2025-01-02",
        "decisions_sha256": "b" * 64,
        "target_schedule_sha256": "c" * 64,
    }

    assert _cache_result(payload, **kwargs) == result
    for field, replacement in (
        ("protocol_sha256", "0" * 64),
        ("decisions_sha256", "1" * 64),
        ("target_schedule_sha256", "2" * 64),
    ):
        stale = dict(payload)
        stale[field] = replacement
        assert _cache_result(stale, **kwargs) is None


def test_sweep_creates_40_shadows_and_50_fresh_accounts_without_ordering(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    protocol_path = Path(__file__).resolve().parents[2] / "protocols" / "5.0.json"
    protocol = json.loads(protocol_path.read_text(encoding="utf-8"))
    protocol_sha256 = hashlib.sha256(protocol_path.read_bytes()).hexdigest()
    expert_names = tuple(protocol["experts"]["ordered_registry"])
    factors = [
        FactorSpec(
            name=name,
            family="adaptive_test",
            kind="expression" if index == 0 else "ensemble",
            expression="x" if index == 0 else None,
            direction_policy="fixed" if index == 0 else "pre_directed",
            params={"fixed_direction": 1} if index == 0 else {},
            role="control" if index == 0 else "adaptive_expert",
        )
        for index, name in enumerate(expert_names)
    ]
    validations = {name: _validation(name) for name in expert_names}
    features = pd.DataFrame(
        {
            "date": pd.bdate_range("2017-01-02", periods=3),
            "ticker": ["A", "A", "A"],
            "return_1d": [0.0, 0.0, 0.0],
            "momentum_120": [1.0, 1.0, 1.0],
        }
    )
    signals = {
        name: pd.Series([1.0, 1.0, 1.0], index=features.index) for name in expert_names
    }
    config = LongOnlyPortfolioConfig(
        capital=50_000_000.0,
        holding_days=10,
        rebalance_every_days=10,
        position_count=10,
        target_weight=0.1,
        retention_buffer=5,
        periods_per_year=25.2,
    )
    signal_dates = ("2020-02-03", "2020-02-13")
    ready_dates = pd.bdate_range("2020-01-02", periods=10)

    def fake_decisions(
        *,
        protocol: Mapping[str, Any],
        frozen: Mapping[str, Any],
        features: pd.DataFrame,
        shadow_results: Mapping[str, Mapping[str, Any]],
        offset: int,
    ) -> dict[str, Any]:
        del frozen, features, shadow_results
        schedules = {
            account: [
                {"signal_date": date_value, "target_weights": {"A": 0.6}}
                for date_value in signal_dates
            ]
            for account in ACCOUNT_NAMES
        }
        audits = {
            account: {
                date_value: {
                    "promotion_eligible": True,
                    "account_name": account,
                }
                for date_value in signal_dates
            }
            for account in ACCOUNT_NAMES
        }
        overlay = {
            date_value: {"signal_date": date_value, "ready": True, "exposure": 0.6}
            for date_value in signal_dates
        }
        online = {
            date_value: {
                "signal_date": date_value,
                "total_expert_weights": {
                    name: 1.0 / len(expert_names) for name in expert_names
                },
            }
            for date_value in signal_dates
        }
        ready = ready_dates[offset].date().isoformat()
        return {
            "ready_date": ready,
            "public_decisions": {
                "history_policy": protocol["experts"]["shadow_history_policy"]
                if protocol
                else "strict",
                "ready_date": ready,
                "offset": offset,
            },
            "account_schedules": schedules,
            "account_audits": audits,
            "online_by_date": online,
            "overlay_by_date": overlay,
            "future_feedback_violation_count": 0,
            "future_overlay_violation_count": 0,
        }

    monkeypatch.setattr(adaptive_runtime, "_build_offset_decisions", fake_decisions)
    calls: list[dict[str, Any]] = []

    def portfolio_result(
        factor: FactorSpec,
        validation: FactorValidation,
        signal: pd.Series,
        feature_frame: pd.DataFrame,
        execution_frame: pd.DataFrame,
        portfolio_config: LongOnlyPortfolioConfig,
        research_config: Mapping[str, Any],
        **kwargs: Any,
    ) -> dict[str, Any]:
        del signal, feature_frame, execution_frame, research_config
        calls.append(
            {
                "factor_name": factor.name,
                "start": portfolio_config.evaluation_start_date,
                "position_count": portfolio_config.position_count,
                "target_weight": portfolio_config.target_weight,
                **kwargs,
            }
        )
        requested_start = portfolio_config.evaluation_start_date
        shadow = requested_start is None
        periods = [
            {
                "signal_date": "2017-01-02" if shadow else signal_dates[0],
                "start_date": "2017-01-03" if shadow else "2020-02-04",
                "end_date": "2017-01-16" if shadow else "2020-02-17",
                "net_return": 0.01,
                "benchmark_return": 0.0,
                "active_return": 0.01,
                "benchmark_return_coverage": 1.0,
                "benchmark_endpoint_coverage": 1.0,
            },
            {
                "signal_date": "2017-01-17" if shadow else signal_dates[1],
                "start_date": "2017-01-18" if shadow else "2020-02-14",
                "end_date": "2017-01-31" if shadow else "2020-02-27",
                "net_return": 0.01,
                "benchmark_return": 0.0,
                "active_return": 0.01,
                "benchmark_return_coverage": 1.0,
                "benchmark_endpoint_coverage": 1.0,
            },
        ]
        boundary = requested_start or "2017-01-02"
        return {
            "factor_name": factor.name,
            "factor": factor.to_dict(),
            "stage_a": validation.to_dict(),
            "windows": {
                "train": {
                    "observations": len(periods),
                    "execution_period_coverage": 1.0,
                    "execution_input_coverage": 1.0,
                    "execution_input_future_violation_count": 0,
                    "capacity_violation_count": 0,
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
            "validated": False,
            "period_active_returns": periods,
            "period_target_weights": [
                {
                    "signal_date": row["signal_date"],
                    "start_date": row["start_date"],
                    "end_date": row["end_date"],
                    "target_weights": {"A": 0.1},
                }
                for row in periods
            ]
            if shadow
            else [],
            "account_nav_path": [
                {
                    "date": boundary,
                    "phase": "accounting_boundary",
                    "nav": portfolio_config.capital,
                    "sequence": 0,
                },
                {
                    "date": periods[-1]["end_date"],
                    "phase": "daily_end",
                    "nav": portfolio_config.capital * 1.01,
                    "sequence": 1,
                },
            ],
            "portfolio": {
                "status": "ok",
                "evaluation_start_date": boundary,
                "initial_nav": portfolio_config.capital,
                "first_pretrade_nav": portfolio_config.capital,
                "end_nav": portfolio_config.capital * 1.01,
                "account_nav_reconciliation_error": 0.0,
            },
        }

    annual_returns = {
        "fixed_core_full": 0.10,
        "fixed_core_overlay": 0.11,
        "static_prior_full": 0.105,
        "online_full": 0.12,
        "online_overlay": 0.13,
    }

    def historical_metrics(
        result: Mapping[str, Any],
        research_config: Mapping[str, Any],
        *,
        periods_per_year: float,
        reference_periods: Sequence[Mapping[str, Any]],
        optimization_scope: str,
    ) -> dict[str, Any]:
        del research_config, periods_per_year, optimization_scope
        annual = annual_returns[str(result["factor_name"])]
        return {
            "observations": len(reference_periods),
            "period_coverage": 1.0,
            "benchmark_return_coverage_min": 1.0,
            "net_annual_return": annual,
            "net_sharpe": annual * 10.0,
            "information_ratio": annual * 5.0,
            "max_drawdown": -0.20 + annual,
            "max_drawdown_basis": "daily_account_nav",
            "daily_nav_path_complete": True,
            "daily_nav_observations": 20,
        }

    output_dir = tmp_path / "run"
    summary, base_accounts = run_adaptive_sweep(
        factors=factors,
        validations=validations,
        signals=signals,
        features=features,
        execution=pd.DataFrame(),
        base_config=config,
        research_config={},
        base_results=[],
        control=factors[0],
        protocol=protocol,
        protocol_sha256=protocol_sha256,
        output_dir=output_dir,
        run_fingerprint="f" * 64,
        resume=True,
        portfolio_result=portfolio_result,
        historical_metrics=historical_metrics,
    )

    assert len([row for row in calls if row["start"] is None]) == 40
    scoring_calls = [row for row in calls if row["start"] is not None]
    assert len(scoring_calls) == 50
    assert all(row["position_count"] == 40 for row in scoring_calls)
    assert all(row["target_weight"] == 1.0 for row in scoring_calls)
    assert all(row["require_optimized_targets"] is True for row in scoring_calls)
    assert summary["shadow_account_count"] == 40
    assert summary["scoring_account_count"] == 50
    assert summary["shadow_accounts_valid"] is True
    assert summary["scoring_accounts_valid"] is True
    assert summary["integrity_valid"] is True
    assert summary["common_evaluation_start"] == ready_dates[-1].date().isoformat()
    assert summary["future_feedback_violation_count"] == 0
    assert summary["future_overlay_violation_count"] == 0
    assert summary["frozen_route"] == "online_overlay"
    assert "ranking_available" not in summary
    assert summary["prospective_status"] == "not_activated"
    assert tuple(base_accounts) == ACCOUNT_NAMES
    assert len(list((output_dir / "adaptive").glob("offset-*/shadows/*.json"))) == 40
    assert len(list((output_dir / "adaptive").glob("offset-*/accounts/*.json"))) == 50
    assert len(list((output_dir / "adaptive").glob("offset-*/decisions.json"))) == 10

    def assert_no_ordering_keys(value: Any) -> None:
        if isinstance(value, Mapping):
            assert not ({"rank", "best_strategy", "winner", "phase_score"} & set(value))
            for child in value.values():
                assert_no_ordering_keys(child)
        elif isinstance(value, list):
            for child in value:
                assert_no_ordering_keys(child)

    assert_no_ordering_keys(summary)
    sample_account = json.loads(
        (
            output_dir
            / "adaptive"
            / "offset-00"
            / "accounts"
            / "online_overlay.json"
        ).read_text(encoding="utf-8")
    )
    assert sample_account["protocol_sha256"] == protocol_sha256
    assert sample_account["decisions_path"] == "../decisions.json"
    decisions = json.loads(
        (
            output_dir / "adaptive" / "offset-00" / "decisions.json"
        ).read_text(encoding="utf-8")
    )
    assert sample_account["decisions_sha256"] == decisions["decisions_sha256"]

    def unexpected_portfolio_result(*args: Any, **kwargs: Any) -> dict[str, Any]:
        del args, kwargs
        raise AssertionError("matching protocol/decision-bound caches must be reused")

    resumed, _ = run_adaptive_sweep(
        factors=factors,
        validations=validations,
        signals=signals,
        features=features,
        execution=pd.DataFrame(),
        base_config=config,
        research_config={},
        base_results=[],
        control=factors[0],
        protocol=protocol,
        protocol_sha256=protocol_sha256,
        output_dir=output_dir,
        run_fingerprint="f" * 64,
        resume=True,
        portfolio_result=unexpected_portfolio_result,
        historical_metrics=historical_metrics,
    )
    assert resumed["frozen_route"] == "online_overlay"

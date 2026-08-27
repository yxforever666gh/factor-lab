from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from factor_lab.research import runner as runner_module
from factor_lab.research.contracts import FactorSpec, ValidationSpec
from factor_lab.research.reporting import render_report
from factor_lab.research.runner import run_research
from factor_lab.research.validation import evaluate_stage_a


def _promotion_gate(**overrides: object) -> dict[str, object]:
    gate: dict[str, object] = {
        "validation_net_excess_annual_return_min": 0.0,
        "validation_net_sharpe_min": 0.0,
        "validation_information_ratio_min": 0.0,
        "validation_max_drawdown_min": -1.0,
        "positive_half_year_ratio_min": 0.0,
        "average_holding_count_min": 0,
        "capacity_violation_count_max": 0,
        "validation_excess_mean_bootstrap_lower_min": 0.0,
        "benchmark_return_coverage_min": 0.95,
        "execution_input_policy_match_ratio_min": 1.0,
        "execution_input_future_violation_count_max": 0,
        "execution_input_coverage_min": 1.0,
        "validation_observations_min": 2,
        "execution_period_coverage_min": 0.9,
        "signal_evaluable_date_ratio_min": 0.8,
        "signal_median_cross_section_coverage_min": 0.8,
    }
    gate.update(overrides)
    return gate


def _frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = pd.DatetimeIndex(
        [
            *pd.bdate_range("2017-01-03", periods=25),
            *pd.bdate_range("2023-01-03", periods=25),
            *pd.bdate_range("2025-01-03", periods=31),
        ]
    )
    tickers = [f"{index:06d}.SZ" for index in range(1, 13)]
    rows: list[dict[str, object]] = []
    for day_index, date in enumerate(dates):
        for ticker_index, ticker in enumerate(tickers):
            value = (ticker_index + 1) / len(tickers)
            rows.append(
                {
                    "date": date,
                    "ticker": ticker,
                    "book_yield": value,
                    "earnings_yield": value * 0.8 + 0.01,
                    "pb": 1.0 + value,
                    "roe": value * 0.2,
                    "volatility_20": 0.3 - value * 0.1,
                    "turnover_rate": 0.2 - value * 0.05,
                    "momentum_60": ((ticker_index * 5) % len(tickers))
                    / len(tickers)
                    + day_index * 1e-8,
                    "financial_available_date": date - pd.Timedelta(days=1),
                    "fundamental_roic": value * 10.0,
                    "fundamental_q_ocf_to_sales": value * 0.5,
                    "fundamental_debt_to_assets": 1.0 - value * 0.5,
                    "fundamental_age_days": 120,
                    "industry_pit": "A" if ticker_index < 6 else "B",
                    "total_mv": 1_000_000_000.0 * (ticker_index + 1),
                    "forward_return_5d_open": value * 0.02 + day_index * 1e-8,
                    "st_filter_status": "verified",
                    "eligible": ticker_index != 0,
                    "universe_member": True,
                }
            )
    features = pd.DataFrame(rows)
    execution_dates = dates.append(pd.bdate_range(dates[-1] + pd.Timedelta(days=1), periods=6))
    execution_rows: list[dict[str, object]] = []
    for day_index, date in enumerate(execution_dates):
        for ticker_index, ticker in enumerate(tickers):
            execution_rows.append(
                {
                    "date": date,
                    "ticker": ticker,
                    "open_adj": 10.0 + day_index * 0.01 + ticker_index * 0.001,
                    "adv_20": 1_000_000_000.0,
                    "volatility_20": 0.02,
                    "eligible": True,
                    "universe_member": True,
                    "is_one_price_limit_up": False,
                    "is_one_price_limit_down": False,
                }
            )
    return features, pd.DataFrame(execution_rows)


def test_full_runner_writes_resumable_two_stage_outputs(tmp_path: Path) -> None:
    features, execution = _frames()
    feature_path = tmp_path / "features.parquet"
    execution_path = tmp_path / "execution.parquet"
    features.to_parquet(feature_path, index=False)
    execution.to_parquet(execution_path, index=False)
    repository = Path(__file__).resolve().parents[2]

    result = run_research(
        project_root=tmp_path,
        suite="next",
        mode="full",
        feature_path=feature_path,
        execution_path=execution_path,
        factors_path=repository / "configs" / "factors.json",
        research_config_path=repository / "configs" / "research.json",
        run_robustness=False,
    )

    assert result["status"] == "completed"
    assert result["control_factor"] == "earnings_yield_over_pb"
    assert 1 <= len(result["stage_b_selected"]) <= 4
    assert len(result["stage_a"]) == 5
    assert result["stage_a_selection"]["basis"] == "train_only"
    research_filter = result["data"]["research_universe_filter"]
    assert research_filter["columns_applied"] == ["eligible", "universe_member"]
    assert research_filter["excluded_row_count"] == len(features[features["eligible"] == False])
    run_dir = tmp_path / "runtime" / "runs" / result["run_id"]
    assert (run_dir / "summary.json").is_file()
    assert (run_dir / "report.md").is_file()
    assert (run_dir / "manifest.json").is_file()
    assert json.loads((tmp_path / "runtime" / "runs" / "latest.json").read_text())["run_id"] == result["run_id"]
    report = (run_dir / "report.md").read_text(encoding="utf-8")
    assert "Stage A：训练段筛选" in report
    assert "审计段只允许否证" in report

    resumed = run_research(
        project_root=tmp_path,
        suite="next",
        mode="full",
        feature_path=feature_path,
        execution_path=execution_path,
        factors_path=repository / "configs" / "factors.json",
        research_config_path=repository / "configs" / "research.json",
        run_robustness=False,
    )
    assert resumed["run_fingerprint"] == result["run_fingerprint"]


def test_registered_recovery_builtin_runs_through_stage_a(tmp_path: Path) -> None:
    features, execution = _frames()
    feature_path = tmp_path / "features.parquet"
    execution_path = tmp_path / "execution.parquet"
    features.to_parquet(feature_path, index=False)
    execution.to_parquet(execution_path, index=False)
    repository = Path(__file__).resolve().parents[2]

    result = run_research(
        project_root=tmp_path,
        suite="recovery",
        mode="canary",
        feature_path=feature_path,
        execution_path=execution_path,
        factors_path=repository / "configs" / "factors.json",
        research_config_path=repository / "configs" / "research.json",
        run_robustness=False,
    )

    recovery = next(
        row for row in result["stage_a"] if row["factor_name"] == "pit_cashflow_quality"
    )
    assert recovery["selection_basis"] == "train_only"
    assert recovery["factor_name"] in {
        row["factor_name"] for row in result["stage_a"]
    }
    assert result["validated_count"] == 0
    assert result["search_status"] == "canary_smoke"


def test_results_first_ranks_comparable_control_and_ensembles(tmp_path: Path) -> None:
    features, execution = _frames()
    feature_path = tmp_path / "features.parquet"
    execution_path = tmp_path / "execution.parquet"
    research_path = tmp_path / "research.json"
    features.to_parquet(feature_path, index=False)
    execution.to_parquet(execution_path, index=False)
    repository = Path(__file__).resolve().parents[2]
    research_config = json.loads(
        (repository / "configs" / "research.json").read_text(encoding="utf-8")
    )
    research_config["portfolio"].update(
        {"position_count": 3, "target_weight": 1 / 3, "retention_buffer": 0}
    )
    research_config["results_first"]["challenger_weights"] = [0.3, 0.7]
    research_path.write_text(json.dumps(research_config), encoding="utf-8")

    common = dict(
        project_root=tmp_path,
        suite="results-first",
        feature_path=feature_path,
        execution_path=execution_path,
        factors_path=repository / "configs" / "factors.json",
        research_config_path=research_path,
        run_robustness=False,
        resume=False,
    )
    canary = run_research(mode="canary", **common)
    assert canary["search_status"] == "results_first_canary_smoke"
    assert canary["results_first"]["ranking_available"] is False
    assert canary["results_first"]["best_historical_strategy"] is None
    assert canary["results_first"]["rankings"] == []

    result = run_research(mode="full", **common)

    rankings = result["results_first"]["rankings"]
    assert result["results_first"]["enabled"] is True
    assert result["results_first"]["optimization_scope"] == "all_observed_history"
    assert result["search_status"] == "results_first_historical_ranking_completed"
    assert result["search_stopped"] is False
    assert result["validated_count"] == 0
    assert len(result["stage_b_selected"]) == 11
    assert len(rankings) == 11
    assert result["results_first"]["excluded_from_ranking"] == []
    assert [row["rank"] for row in rankings] == list(range(1, 12))
    assert rankings == sorted(
        rankings,
        key=lambda row: (
            -float(row["historical_score"]),
            -float(row["net_annual_return"]),
            str(row["factor_name"]),
        ),
    )
    assert result["results_first"]["best_historical_strategy"] == rankings[0][
        "factor_name"
    ]
    assert any(row["strategy_kind"] == "ensemble" for row in rankings)
    assert len({row["net_annual_return"] for row in rankings}) > 1
    ensemble = next(
        row
        for row in result["stage_a"]
        if str(row["factor_name"]).startswith("blend__")
    )
    assert ensemble["selection_basis"] == "pre_directed_components"
    assert "不声称独立 OOS" in render_report(result)


def _period(
    signal_date: str,
    start_date: str,
    end_date: str,
    net_return: float,
    benchmark_return: float = 0.0,
) -> dict[str, object]:
    return {
        "signal_date": signal_date,
        "start_date": start_date,
        "end_date": end_date,
        "net_return": net_return,
        "gross_return": net_return,
        "benchmark_return": benchmark_return,
        "holding_count": 50,
        "turnover": 0.1,
        "benchmark_expected_endpoint_count": 2,
        "benchmark_observed_endpoint_count": 2,
        "benchmark_complete_return_count": 1,
        "benchmark_missing_start_count": 0,
        "benchmark_missing_end_count": 0,
        "execution_input_policy": "previous_visible_ticker_row",
        "execution_input_min_date": signal_date,
        "execution_input_max_date": signal_date,
        "execution_input_required_count": 1,
        "execution_input_observed_count": 1,
        "max_execution_input_age_days": 1,
        "capacity_violation_count": 0,
        "blocked_trade_count": 0,
        "costs": {"total": 0.0},
    }


def test_results_first_metrics_align_to_control_periods_and_expose_gaps() -> None:
    reference = [
        {
            "signal_date": "2025-01-03",
            "net_return": 0.05,
            "benchmark_return": 0.02,
            "active_return": 0.03,
        },
        {
            "signal_date": "2025-01-10",
            "net_return": -0.01,
            "benchmark_return": 0.01,
            "active_return": -0.02,
        },
    ]
    sparse = {
        "period_active_returns": [
            {
                "signal_date": "2025-01-03",
                "net_return": 0.10,
                "benchmark_return": 0.02,
                "active_return": 0.08,
            }
        ]
    }

    metrics = runner_module._results_first_metrics(
        sparse,
        {"results_first": {"incomplete_period_policy": "exclude_from_ranking"}},
        periods_per_year=2,
        reference_periods=reference,
    )

    assert metrics["observations"] == 2
    assert metrics["observed_strategy_periods"] == 1
    assert metrics["missing_strategy_periods"] == 1
    assert metrics["period_coverage"] == 0.5
    assert metrics["comparison_period_basis"] == "control_signal_dates"
    assert metrics["missing_period_score_policy"] == "cash_return_zero_diagnostic_only"


def test_window_metrics_require_the_whole_period_to_stay_inside_split() -> None:
    periods = [
        _period("2022-12-20", "2022-12-21", "2022-12-28", 0.01),
        _period("2022-12-27", "2022-12-28", "2023-01-05", 9.0),
        _period("2023-01-04", "2023-01-05", "2023-01-12", 0.02),
        _period("2024-12-27", "2024-12-30", "2025-01-07", 9.0),
    ]

    train = runner_module._window_metrics(
        periods, start="2017-01-01", end="2022-12-31", periods_per_year=252 / 5
    )
    validation = runner_module._window_metrics(
        periods, start="2023-01-01", end="2024-12-31", periods_per_year=252 / 5
    )

    assert train["observations"] == 1
    assert train["net_return"] == 0.01
    assert validation["observations"] == 1
    assert validation["net_return"] == 0.02


def test_window_metrics_report_uncertainty_coverage_and_annualized_turnover() -> None:
    periods = [
        _period(
            f"2023-01-{3 + index * 7:02d}",
            f"2023-01-{4 + index * 7:02d}",
            f"2023-01-{9 + index * 7:02d}",
            0.01,
            0.002,
        )
        for index in range(3)
    ]
    policy = ValidationSpec(bootstrap_samples=32, bootstrap_block_size=2)

    metrics = runner_module._window_metrics(
        periods,
        start="2023-01-01",
        end="2023-12-31",
        periods_per_year=252 / 5,
        bootstrap_spec=policy,
        bootstrap_key="test:validation",
    )

    assert metrics["benchmark_return_coverage"] == 1.0
    assert metrics["benchmark_endpoint_coverage"] == 1.0
    assert metrics["annualized_turnover"] == 5.04
    assert metrics["excess_return_mean_bootstrap_lower"] == 0.008
    passed, blockers = runner_module._gate(
        {
            **metrics,
            "signal_evaluable_date_ratio": 1.0,
            "signal_median_cross_section_coverage": 1.0,
        },
        {"promotion_gate": _promotion_gate()},
    )
    assert passed is True
    assert blockers == []

    failed, blockers = runner_module._gate(
        {**metrics, "excess_return_mean_bootstrap_lower": 0.0},
        {"promotion_gate": _promotion_gate()},
    )
    assert failed is False
    assert "validation_excess_bootstrap_lower_below_threshold" in blockers


def test_control_improvement_requires_paired_simultaneous_confidence() -> None:
    dates = pd.date_range("2023-01-06", periods=100, freq="7D")

    def payload(name: str, differences: list[float]) -> dict[str, object]:
        return {
            "factor_name": name,
            "period_active_returns": [
                {
                    "signal_date": str(date.date()),
                    "end_date": str((date + pd.Timedelta(days=6)).date()),
                    "net_return": 0.001 + difference,
                }
                for date, difference in zip(dates, differences)
            ],
        }

    control = payload("control", [0.0] * len(dates))
    noisy = payload("noisy", [0.01 if index % 2 else -0.01 for index in range(len(dates))])
    strong = payload("strong", [0.002] * len(dates))
    policy = ValidationSpec(bootstrap_samples=128, bootstrap_block_size=8)
    config = {"promotion_gate": _promotion_gate(validation_observations_min=90)}

    noisy_result = runner_module._control_comparison(
        noisy, control, config, policy, correction_factor=2
    )
    strong_result = runner_module._control_comparison(
        strong, control, config, policy, correction_factor=2
    )

    assert noisy_result["passed"] is False
    assert "control_improvement_bootstrap_lower_not_positive" in noisy_result["blockers"]
    assert strong_result["passed"] is True
    assert strong_result["bootstrap"]["lower"] == 0.002
    assert strong_result["simultaneous_confidence_method"] == "bonferroni_fwer"


def test_audit_can_only_veto_a_train_admitted_factor() -> None:
    features, _ = _frames()
    audit_mask = features["date"] >= "2025-01-01"
    features.loc[audit_mask, "forward_return_5d_open"] *= -1.0
    policy = ValidationSpec(
        train_start="2017-01-01",
        train_end="2017-12-31",
        validation_start="2023-01-01",
        validation_end="2024-12-31",
        audit_start="2025-01-01",
        min_cross_section=5,
        min_train_positive_year_ratio=0.0,
        bootstrap_samples=16,
        audit_min_observations=2,
    )
    factor = FactorSpec(name="alpha", family="test", expression="book_yield")
    result = evaluate_stage_a(features[features["eligible"]].copy(), factor, policy)

    falsified, reasons = runner_module._audit_falsification(
        result,
        {
            "observations": 5,
            "information_ratio": -0.5,
            "excess_return_mean_bootstrap": {"upper": -0.001},
        },
        policy,
    )

    assert result.stage_b_eligible is True
    assert result.selection_basis == "train_only"
    assert falsified is True
    assert "audit_active_return_bootstrap_upper_negative" in reasons


def test_final_factor_state_manifest_recovery_and_robustness_fingerprint(
    tmp_path: Path, monkeypatch
) -> None:
    features, execution = _frames()
    feature_path = tmp_path / "features.parquet"
    execution_path = tmp_path / "execution.parquet"
    features.to_parquet(feature_path, index=False)
    execution.to_parquet(execution_path, index=False)
    repository = Path(__file__).resolve().parents[2]
    calls: list[str] = []

    def fake_portfolio_result(factor, validation, *args, **kwargs):
        calls.append(factor.name)
        is_control = factor.name == "earnings_yield_over_pb"
        metrics = {
            "observations": 10,
            "net_return": 0.1,
            "gross_return": 0.1,
            "benchmark_return": 0.0,
            "net_annual_return": 0.12 if is_control else 0.15,
            "benchmark_annual_return": 0.0,
            "net_excess_annual_return": 0.10 if is_control else 0.12,
            "net_sharpe": 1.0 if is_control else 1.2,
            "information_ratio": 0.8,
            "max_drawdown": -0.10,
            "positive_half_year_ratio": 0.75,
            "average_holding_count": 50.0,
            "actual_turnover": 0.1,
            "capacity_violation_count": 0,
            "blocked_trade_count": 0,
            "total_cost": 0.0,
        }
        return {
            "factor_name": factor.name,
            "family": factor.family,
            "factor": factor.to_dict(),
            "frozen_direction": validation.frozen_direction,
            "stage_a": validation.to_dict(),
            "portfolio": {"status": "ok"},
            "windows": {"train": metrics, "validation": metrics, "audit": metrics},
            "gate_passed": True,
            "gate_blockers": [],
            "beats_control": False,
            "validated": False,
        }

    monkeypatch.setattr(runner_module, "_portfolio_result", fake_portfolio_result)
    monkeypatch.setattr(
        runner_module,
        "_control_comparison",
        lambda *args, **kwargs: {"passed": True, "blockers": [], "bootstrap": {}},
    )
    # This test exercises artifact finalization, not similarity clustering.
    monkeypatch.setattr(runner_module, "diagnose_train_similarity", lambda *args: [])
    arguments = {
        "project_root": tmp_path,
        "suite": "next",
        "mode": "full",
        "feature_path": feature_path,
        "execution_path": execution_path,
        "factors_path": repository / "configs" / "factors.json",
        "research_config_path": repository / "configs" / "research.json",
        "run_robustness": False,
    }
    first = run_research(**arguments)
    run_dir = tmp_path / "runtime" / "runs" / first["run_id"]
    challenger = next(row for row in first["stage_b"] if row["factor_name"] != first["control_factor"])
    factor_path = run_dir / "factors" / f"{challenger['factor_name']}.json"
    factor_payload = json.loads(factor_path.read_text(encoding="utf-8"))
    assert factor_payload["result"]["beats_control"] is True
    assert factor_payload["result"]["validated"] is True
    assert runner_module._completed_run_valid(
        run_dir / "summary.json", run_dir, first["run_fingerprint"]
    )

    # A completed checkpoint with a modified artifact must not be trusted or
    # re-blessed from the corrupted per-factor cache.
    factor_payload["result"]["validated"] = False
    factor_path.write_text(json.dumps(factor_payload), encoding="utf-8")
    calls.clear()
    repaired = run_research(**arguments)
    assert repaired["run_id"] == first["run_id"]
    assert len(calls) == len(first["stage_b_selected"])
    repaired_factor = json.loads(factor_path.read_text(encoding="utf-8"))
    assert repaired_factor["result"]["validated"] is True
    assert runner_module._completed_run_valid(
        run_dir / "summary.json", run_dir, repaired["run_fingerprint"]
    )

    with_robustness = run_research(**{**arguments, "run_robustness": True})
    assert with_robustness["run_id"] != first["run_id"]


def test_audit_veto_stops_route_without_triggering_robustness(
    tmp_path: Path, monkeypatch
) -> None:
    features, execution = _frames()
    feature_path = tmp_path / "features.parquet"
    execution_path = tmp_path / "execution.parquet"
    features.to_parquet(feature_path, index=False)
    execution.to_parquet(execution_path, index=False)
    repository = Path(__file__).resolve().parents[2]

    def fake_portfolio_result(factor, validation, *args, **kwargs):
        is_control = factor.name == "earnings_yield_over_pb"
        metrics = {
            "net_excess_annual_return": 0.10 if is_control else 0.12,
            "net_sharpe": 1.0 if is_control else 1.2,
            "information_ratio": 0.8,
            "max_drawdown": -0.10,
        }
        falsified = not is_control
        return {
            "factor_name": factor.name,
            "family": factor.family,
            "windows": {"train": metrics, "validation": metrics, "audit": metrics},
            "gate_passed": True,
            "gate_blockers": [],
            "audit_falsified": falsified,
            "audit_status": "falsified" if falsified else "not_falsified",
            "audit_falsification_reasons": ["audit_rank_ic_non_positive"]
            if falsified
            else [],
            "beats_control": False,
            "validated": False,
        }

    monkeypatch.setattr(runner_module, "_portfolio_result", fake_portfolio_result)
    monkeypatch.setattr(runner_module, "diagnose_train_similarity", lambda *args: [])
    monkeypatch.setattr(
        runner_module,
        "_control_comparison",
        lambda *args, **kwargs: {"passed": True, "blockers": [], "bootstrap": {}},
    )
    monkeypatch.setattr(
        runner_module,
        "_run_robustness",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("audit must not trigger robustness")
        ),
    )

    result = run_research(
        project_root=tmp_path,
        suite="next",
        mode="full",
        feature_path=feature_path,
        execution_path=execution_path,
        factors_path=repository / "configs" / "factors.json",
        research_config_path=repository / "configs" / "research.json",
        run_robustness=True,
    )

    assert result["validated_count"] == 0
    assert result["pre_audit_confirmed_factors"]
    assert result["robustness"] is None
    assert result["search_status"] == "audit_falsified_stop"
    assert result["search_stopped"] is True


def test_robustness_aggregates_every_fixed_anchor_without_best_selection(
    tmp_path: Path, monkeypatch
) -> None:
    factor = FactorSpec(name="alpha", family="test", expression="book_yield")
    second_factor = FactorSpec(name="beta", family="test", expression="earnings_yield")
    calls: list[tuple[str, int]] = []
    by_offset = {
        0: (0.02, 1.00, 0.70, -0.10, 0.70),
        5: (0.04, 0.90, 0.60, -0.15, 0.65),
        10: (0.01, 0.85, 0.55, -0.20, 0.65),
        15: (-0.02, 0.40, 0.20, -0.30, 0.40),
    }

    def fake_portfolio_result(factor, validation, signal, features, execution, config, research_config):
        calls.append((factor.name, config.rebalance_offset_days))
        excess, sharpe, information_ratio, drawdown, positive_half_year = by_offset[
            config.rebalance_offset_days
        ]
        metrics = {
            "observations": 20,
            "net_excess_annual_return": excess,
            "net_sharpe": sharpe,
            "information_ratio": information_ratio,
            "max_drawdown": drawdown,
            "positive_half_year_ratio": positive_half_year,
            "average_holding_count": 50.0,
            "capacity_violation_count": 0,
            "excess_return_mean_bootstrap_lower": 0.001,
            "benchmark_return_coverage": 1.0,
            "execution_input_policy_match_ratio": 1.0,
            "execution_input_future_violation_count": 0,
            "execution_input_coverage": 1.0,
            "execution_period_coverage": 1.0,
            "signal_evaluable_date_ratio": 1.0,
            "signal_median_cross_section_coverage": 1.0,
        }
        gate_passed, blockers = runner_module._gate(metrics, research_config)
        return {
            "gate_passed": gate_passed,
            "gate_blockers": blockers,
            "windows": {
                "train": {**metrics, "net_excess_annual_return": excess + 0.01},
                "validation": metrics,
                "audit": {**metrics, "net_excess_annual_return": excess - 0.01},
            },
            "portfolio": {"status": "ok"},
        }

    monkeypatch.setattr(runner_module, "_portfolio_result", fake_portfolio_result)
    research_config = {
        "promotion_gate": _promotion_gate(
            validation_net_sharpe_min=0.8,
            validation_information_ratio_min=0.5,
            validation_max_drawdown_min=-0.25,
            positive_half_year_ratio_min=0.6,
            average_holding_count_min=40,
            validation_observations_min=10,
        ),
        "robustness": {
            "position_counts": [50],
            "rebalance_every_days": [20],
            "anchor_offsets_by_rebalance_days": {"20": [0, 5, 10, 15]},
            "minimum_anchor_pass_ratio": 0.75,
        },
    }
    payload = runner_module._run_robustness(
        [factor, second_factor],
        {factor.name: object(), second_factor.name: object()},
        {
            factor.name: pd.Series(dtype=float),
            second_factor.name: pd.Series(dtype=float),
        },
        pd.DataFrame(),
        pd.DataFrame(),
        runner_module.LongOnlyPortfolioConfig(),
        research_config,
        tmp_path / "robustness.json",
    )

    assert calls == [
        ("alpha", 0),
        ("alpha", 5),
        ("alpha", 10),
        ("alpha", 15),
        ("beta", 0),
        ("beta", 5),
        ("beta", 10),
        ("beta", 15),
    ]
    assert payload["selection_basis"] == "train_shortlist_order"
    assert [row["factor_name"] for row in payload["results"]] == ["alpha", "beta"]
    row = payload["results"][0]
    assert row["anchor_offsets"] == [0, 5, 10, 15]
    assert row["anchor_count"] == 4
    assert row["anchor_pass_ratio"] == 0.75
    assert row["median_gate_passed"] is True
    assert row["robust"] is True
    assert row["promotion_eligible"] is False
    assert row["window_statistics"]["validation"]["net_excess_annual_return"] == {
        "min": -0.02,
        "median": 0.015,
        "max": 0.04,
    }
    assert row["median_windows"]["validation"]["net_sharpe"] == 0.875

    report = render_report(
        {
            "run_id": "test",
            "suite": "next",
            "mode": "full",
            "data": {},
            "stage_a": [],
            "stage_b": [],
            "robustness": payload,
            "validated_factors": [],
        }
    )
    assert "不选择最佳锚点" in report
    assert "-2.00% / 1.50% / 4.00%" in report
    assert "75.00%" in report


def test_robustness_requires_every_anchor_to_pass_data_integrity() -> None:
    anchors = [
        {"gate_blockers": []},
        {"gate_blockers": []},
        {"gate_blockers": []},
        {"gate_blockers": ["future_execution_input_detected"]},
    ]

    assert runner_module._robustness_integrity_blockers(anchors) == [
        "future_execution_input_detected"
    ]

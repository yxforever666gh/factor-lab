from __future__ import annotations

from factor_lab.research.reporting import render_report


def _walk_forward_summary(*, mode: str = "full") -> dict[str, object]:
    ranking_available = mode == "full"
    walk_forward: dict[str, object] = {
        "enabled": True,
        "protocol": "causal_walk_forward",
        "evidence_class": (
            "post_selection_causal_simulation" if ranking_available else "engineering_smoke"
        ),
        "canary_smoke_only": not ranking_available,
        "ranking_available": ranking_available,
        "selector": {
            "lookback_trading_days": 504,
            "minimum_completed_periods": 48,
            "update_every_trading_days": 20,
            "score_method": "net_sharpe",
            "control_score_guard": 0.1,
            "history_policy": "end_date_strictly_before_signal_date",
            "missing_signal_policy": "fallback_control",
        },
        "candidate_registry": ["control", "blend_70"],
        "fixed_comparator": {
            "factor_name": "fixed_registry_equal_weight",
            "protocol": {
                "name": "fixed_registry_equal_weight",
                "weighting": "equal",
                "missing_signal_policy": "fallback_control",
            },
            "dynamic_phase_deltas": (
                {
                    "net_annual_return": {"q20": 0.01},
                    "net_sharpe": {"q20": 0.05},
                    "information_ratio": {"q20": 0.02},
                    "max_drawdown": {"q20": 0.01},
                }
                if ranking_available
                else {}
            ),
            "dynamic_positive_annual_return_delta_ratio": (
                0.8 if ranking_available else None
            ),
        },
        "dynamic_factor": "causal_walk_forward_dynamic",
        "rebalance_offsets": list(range(10)) if ranking_available else [],
        "phase_quantile": 0.2,
        "common_evaluation_start": (
            "2019-01-02" if ranking_available else None
        ),
        "scoring_account_protocol": (
            "fresh_cash_equal_aum_common_start" if ranking_available else None
        ),
        "scoring_initial_nav": 50_000_000.0 if ranking_available else None,
        "scoring_account_count": 90 if ranking_available else 0,
        "expected_scoring_account_count": 90 if ranking_available else 0,
        "equal_aum_scoring_valid": ranking_available,
        "future_selection_violation_count": 0,
        "full_dynamic_period_coverage": ranking_available,
        "causal_history_valid": ranking_available,
        "historical_diagnostic_passed": ranking_available,
        "best_phase_strategy": (
            "causal_walk_forward_dynamic" if ranking_available else None
        ),
        "dynamic_phase_rank": 1 if ranking_available else None,
        "phase_rankings": [],
        "offsets": [],
        "selection_frequency": {},
    }
    if ranking_available:
        walk_forward["phase_rankings"] = [
            {
                "rank": 1,
                "strategy_name": "causal_walk_forward_dynamic",
                "strategy_kind": "walk_forward_dynamic",
                "phase_score": 0.9,
                "phase_metrics": {
                    "net_annual_return": {"q20": 0.08},
                    "net_sharpe": {"q20": 0.7},
                    "information_ratio": {"q20": 0.4},
                    "max_drawdown": {"q20": -0.2},
                },
                "phase_deltas_vs_control": {
                    "net_annual_return": {"q20": 0.02}
                },
                "positive_annual_return_delta_ratio": 0.8,
            },
            {
                "rank": 2,
                "strategy_name": "control",
                "strategy_kind": "control",
                "phase_score": 0.4,
                "phase_metrics": {
                    "net_annual_return": {"q20": 0.06},
                    "net_sharpe": {"q20": 0.5},
                    "information_ratio": {"q20": 0.0},
                    "max_drawdown": {"q20": -0.25},
                },
                "phase_deltas_vs_control": {
                    "net_annual_return": {"q20": 0.0}
                },
                "positive_annual_return_delta_ratio": 0.0,
            },
        ]
        walk_forward["offsets"] = [
            {
                "rebalance_offset_days": 0,
                "ready_date": "2019-01-02",
                "signal_date_count": 100,
                "update_count": 50,
                "switch_count": 5,
                "future_selection_violation_count": 0,
                "selection_counts": {"control": 60, "blend_70": 40},
                "metrics": {
                    "causal_walk_forward_dynamic": {
                        "net_annual_return": 0.1,
                        "net_sharpe": 0.7,
                        "information_ratio": 0.4,
                        "max_drawdown": -0.2,
                        "period_coverage": 1.0,
                    }
                },
            },
            {
                "rebalance_offset_days": 1,
                "ready_date": "2019-01-03",
                "signal_date_count": 100,
                "update_count": 50,
                "switch_count": 4,
                "future_selection_violation_count": 0,
                "selection_counts": {"control": 60, "blend_70": 40},
                "metrics": {
                    "causal_walk_forward_dynamic": {
                        "net_annual_return": 0.09,
                        "net_sharpe": 0.65,
                        "information_ratio": 0.35,
                        "max_drawdown": -0.21,
                        "period_coverage": 1.0,
                    }
                },
            },
        ]
        walk_forward["selection_frequency"] = {
            "control": {
                "selected_date_count": 120,
                "selected_date_ratio": 0.60,
                "mean_deployed_weight": 0.70,
            },
            "blend_70": {
                "selected_date_count": 80,
                "selected_date_ratio": 0.40,
                "mean_deployed_weight": 0.30,
            },
        }
    return {
        "run_id": f"walk-forward-{mode}",
        "suite": "walk-forward",
        "mode": mode,
        "data": {},
        "stage_a": [
            {
                "factor_name": "control",
                "frozen_direction": 1,
                "selection_basis": "fixed_direction",
                "stage_b_eligible": True,
                "train": {},
            },
            {
                "factor_name": "blend_70",
                "frozen_direction": 1,
                "selection_basis": "pre_directed_components",
                "stage_b_eligible": True,
                "train": {},
            },
        ],
        "stage_b_selected": ["control", "blend_70"],
        "validated_factors": [],
        "search_status": (
            "causal_walk_forward_sweep_completed"
            if ranking_available
            else "causal_walk_forward_canary_smoke"
        ),
        "walk_forward": walk_forward,
    }


def test_full_walk_forward_report_exposes_causal_protocol_and_phase_results() -> None:
    report = render_report(_walk_forward_summary())

    assert "`post_selection_causal_simulation`" in report
    assert "end_date < signal_date" in report
    assert "固定方向候选注册表" in report
    assert "`fixed_direction`" in report
    assert "`pre_directed_components`" in report
    assert "共同评价起点：`2019-01-02`" in report
    assert "`fresh_cash_equal_aum_common_start`" in report
    assert "90/90 个账户，初始 NAV 50,000,000" in report
    assert "equal-AUM 状态 `valid`" in report
    assert "只提供 selector 反馈，不参与跨策略 phase 评分" in report
    assert "Future selection violations：**0**" in report
    assert "## Phase Q20 排名" in report
    assert "## Dynamic per-offset 回放" in report
    assert "### 动态择时相对固定等权基准" in report
    assert "年化收益差 Q20：1.00%" in report
    assert "### 跨 offset 选择参与率与平均部署权重" in report
    assert "| `control` | 120 | 60.00% | 70.00% |" in report
    assert "| `blend_70` | 80 | 40.00% | 30.00% |" in report
    assert "严禁选择“最佳 offset”" in report
    assert "彼此不是独立实验" in report
    assert "post-selection causal simulation" in report
    assert "等 AUM、逐日核算和退市零回收已纳入本次执行合同" in report
    assert "ghost position" not in report
    assert "## Stage B：真实多头执行" not in report


def test_walk_forward_canary_is_explicitly_non_ranking() -> None:
    report = render_report(_walk_forward_summary(mode="canary"))

    assert "## Walk-forward canary" in report
    assert "未建立（canary / 暖机不足）" in report
    assert "不建立共同 phase 排名" in report
    assert "不执行 selector 或审计因果 cutoff" in report
    assert "不能比较候选、动态策略或 offset" in report
    assert "## Phase Q20 排名" not in report
    assert "## Dynamic per-offset 回放" not in report


def test_non_walk_forward_report_keeps_legacy_protocol_sections() -> None:
    report = render_report(
        {
            "run_id": "legacy",
            "suite": "next",
            "mode": "full",
            "data": {},
            "stage_a": [],
            "stage_b": [],
            "validated_factors": [],
        }
    )

    assert "`historical_diagnostic`（不会创建候选、影子账户或实盘动作）" in report
    assert "## Stage A：训练段筛选" in report
    assert "## Stage B：真实多头执行" in report
    assert "Walk-forward 因果协议" not in report


def test_adaptive_report_discloses_all_accounts_without_ranking() -> None:
    account_names = [
        "fixed_core_full",
        "fixed_core_overlay",
        "static_prior_full",
        "online_full",
        "online_overlay",
    ]
    phase = {
        name: {
            "net_annual_return": {"q20": 0.10, "median": 0.11, "worst": 0.09},
            "net_sharpe": {"q20": 0.60, "median": 0.70, "worst": 0.50},
            "information_ratio": {"q20": 0.10},
            "max_drawdown": {"q20": -0.20, "worst": -0.25},
        }
        for name in account_names
    }
    report = render_report(
        {
            "run_id": "adaptive-full",
            "suite": "adaptive",
            "mode": "full",
            "data": {},
            "stage_a": [],
            "stage_b_selected": [],
            "validated_factors": [],
            "adaptive": {
                "enabled": True,
                "canary_smoke_only": False,
                "expert_registry": ["control", "core", "low_vol", "low_turnover"],
                "account_registry": account_names,
                "common_evaluation_start": "2018-09-03",
                "shadow_accounts_valid": True,
                "scoring_accounts_valid": True,
                "integrity_valid": True,
                "frozen_route": "fixed_core_full",
                "account_phase_distributions": phase,
                "gate_results": {
                    "core_overlay": {
                        "passed": False,
                        "criteria": [
                            {
                                "criterion": "paired_q20_net_annual_return_delta_min",
                                "observed": -0.01,
                                "operator": ">=",
                                "threshold": 0.0,
                                "passed": False,
                            }
                        ],
                    }
                },
            },
        }
    )

    assert "### 五账户十相位绝对分布" in report
    assert all(f"| `{name}` |" in report for name in account_names)
    assert "10.00% / 11.00% / 9.00%" in report
    assert "`paired_q20_net_annual_return_delta_min`：-0.010 `>=` 0.000 → `fail`" in report
    assert "排名" not in report.split("### 五账户十相位绝对分布", 1)[1].split("### 冻结 gate", 1)[0]

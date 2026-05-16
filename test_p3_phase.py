#!/usr/bin/env python3
"""
P3 阶段综合测试

测试：
1. 因子监控体系
2. 回测验证框架
"""

import numpy as np
import pandas as pd

from src.factor_lab.factor_monitoring import (
    FactorMonitor,
    MonitoringThresholds,
    calculate_max_drawdown,
    create_monitoring_report,
    monitor_factor_batch,
)
from src.factor_lab.backtest_validation import (
    rolling_window_validation,
    monte_carlo_validation,
    validate_factor_backtest,
    create_validation_report,
    batch_validate_backtests,
)


def test_factor_monitoring():
    print("=" * 60)
    print("测试 1: 因子监控体系")
    print("=" * 60)

    np.random.seed(42)

    # baseline metrics
    baseline_corr = pd.DataFrame(
        [[1.0, 0.2, 0.1], [0.2, 1.0, 0.25], [0.1, 0.25, 1.0]],
        index=["a", "b", "c"], columns=["a", "b", "c"]
    )
    current_corr = pd.DataFrame(
        [[1.0, 0.65, 0.5], [0.65, 1.0, 0.55], [0.5, 0.55, 1.0]],
        index=["a", "b", "c"], columns=["a", "b", "c"]
    )

    baseline_returns = pd.Series(np.random.normal(0.001, 0.01, 252))
    current_returns = pd.Series(np.random.normal(-0.0005, 0.02, 126))

    baseline_metrics = {
        "ic": 0.04,
        "turnover": 0.30,
        "max_drawdown": calculate_max_drawdown(baseline_returns),
        "correlation_matrix": baseline_corr,
    }
    current_metrics = {
        "ic": 0.015,
        "turnover": 0.55,
        "returns": current_returns,
        "correlation_matrix": current_corr,
    }

    monitor = FactorMonitor(MonitoringThresholds())
    result = monitor.monitor_factor_health("test_factor", current_metrics, baseline_metrics)
    report = create_monitoring_report(result)

    print("\n单因子监控结果:")
    print(f"  健康分数: {report['health_score']}")
    print(f"  状态: {report['status']}")
    print(f"  建议动作: {report['recommended_action']}")
    print(f"  高严重性告警数: {report['high_severity_count']}")
    print(f"  告警总数: {report['alert_count']}")
    print(f"  摘要: {report['summary']}")

    print("\n告警详情:")
    for alert in report["alerts"]:
        print(f"  - {alert['metric']} [{alert['severity']}]: {alert['message']}")

    batch = monitor_factor_batch(
        {
            "healthy_factor": {"ic": 0.038, "turnover": 0.28, "max_drawdown": 0.08, "correlation_matrix": baseline_corr},
            "warning_factor": current_metrics,
        },
        {
            "healthy_factor": baseline_metrics,
            "warning_factor": baseline_metrics,
        },
    )

    print("\n批量监控结果:")
    print(batch.to_string(index=False))


def test_backtest_validation():
    print("\n\n" + "=" * 60)
    print("测试 2: 回测验证框架")
    print("=" * 60)

    rng = np.random.default_rng(42)

    # 较强且稳定的因子
    strong_returns = pd.Series(rng.normal(0.0012, 0.01, 420))
    # 过拟合/不稳定因子：前半段强，后半段弱甚至转负
    unstable_returns = pd.Series(
        np.concatenate([
            rng.normal(0.0018, 0.009, 252),
            rng.normal(-0.0002, 0.013, 168),
        ])
    )

    rolling = rolling_window_validation(strong_returns, train_size=252, test_size=63, step_size=21)
    monte = monte_carlo_validation(strong_returns, n_simulations=500)
    validation = validate_factor_backtest(strong_returns, train_size=252, test_size=63, step_size=21, n_simulations=500)
    report = create_validation_report("strong_factor", validation)

    print("\n强因子验证结果:")
    print(f"  窗口数: {rolling['window_count']}")
    print(f"  平均训练夏普: {rolling['average_train_sharpe']:.4f}")
    print(f"  平均测试夏普: {rolling['average_test_sharpe']:.4f}")
    print(f"  样本外衰减: {rolling['sample_out_decay']:.2%}")
    print(f"  Monte Carlo p-value: {monte['p_value']:.4f}")
    print(f"  最终通过: {report['passed']}")
    print(f"  原因: {report['reason']}")

    unstable_validation = validate_factor_backtest(
        unstable_returns,
        train_size=252,
        test_size=63,
        step_size=21,
        n_simulations=500,
    )
    unstable_report = create_validation_report("unstable_factor", unstable_validation)

    print("\n不稳定因子验证结果:")
    print(f"  样本外衰减: {unstable_report['sample_out_decay']:.2%}")
    print(f"  p-value: {unstable_report['p_value']:.4f}")
    print(f"  最终通过: {unstable_report['passed']}")
    print(f"  原因: {unstable_report['reason']}")

    batch = batch_validate_backtests(
        {
            "strong_factor": strong_returns,
            "unstable_factor": unstable_returns,
        },
        train_size=252,
        test_size=63,
        step_size=21,
        n_simulations=300,
    )
    print("\n批量验证结果:")
    print(batch[["factor_name", "passed", "observed_sharpe", "sample_out_decay", "p_value"]].to_string(index=False))


if __name__ == '__main__':
    test_factor_monitoring()
    test_backtest_validation()

    print("\n\n" + "=" * 60)
    print("P3 阶段所有测试完成！")
    print("=" * 60)
    print("\n已完成的功能:")
    print("  ✅ 因子监控体系（IC/换手/回撤/相关性监控）")
    print("  ✅ 回测验证框架（滚动窗口 + 蒙特卡洛）")
    print("\n下一步:")
    print("  1. 集成到现有工作流")
    print("  2. 对因子池执行真实监控和验证")
    print("  3. 整体计划全部收尾")
    print("=" * 60)

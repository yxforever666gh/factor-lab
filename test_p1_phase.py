#!/usr/bin/env python3
"""
P1 阶段综合测试

测试：
1. 投资假设库
2. 假设驱动生成
3. 增强的 Novelty Judge
4. 因子衰减分析
"""

import pandas as pd
import numpy as np
from src.factor_lab.hypothesis_library import (
    HYPOTHESIS_LIBRARY,
    get_hypothesis_by_id,
    get_hypotheses_by_category,
    list_all_hypotheses,
)
from src.factor_lab.hypothesis_driven_generator import (
    HypothesisDrivenGenerator,
    generate_factors_from_hypotheses,
    create_hypothesis_report,
)
from src.factor_lab.novelty_judge_enhanced import (
    ExpressionNormalizer,
    NoveltyJudgeEnhanced,
    batch_check_novelty,
)
from src.factor_lab.factor_decay_analysis import (
    analyze_factor_decay,
    detect_factor_failure,
    recommend_rebalance_frequency,
    create_decay_report,
)


def test_hypothesis_library():
    """测试投资假设库"""
    print("=" * 60)
    print("测试 1: 投资假设库")
    print("=" * 60)
    
    print(f"\n假设总数: {len(HYPOTHESIS_LIBRARY)}")
    
    # 按类别统计
    categories = {}
    for hyp in HYPOTHESIS_LIBRARY:
        categories[hyp.category] = categories.get(hyp.category, 0) + 1
    
    print("\n按类别统计:")
    for cat, count in sorted(categories.items()):
        print(f"  {cat}: {count} 个")
    
    # 列出所有假设
    print("\n所有假设:")
    for hyp in list_all_hypotheses():
        print(f"  - {hyp['name']} ({hyp['category']}): IC={hyp['expected_ic']:.3f}, 夏普={hyp['expected_sharpe']:.2f}")
    
    # 测试查询
    print("\n\n测试查询:")
    print("-" * 60)
    
    hyp = get_hypothesis_by_id('price_momentum')
    if hyp:
        print(f"\n查询 'price_momentum':")
        print(f"  名称: {hyp.name}")
        print(f"  假设: {hyp.hypothesis}")
        print(f"  经济学逻辑: {hyp.economic_logic}")
        print(f"  预期 IC: {hyp.expected_ic}")
        print(f"  预期持有期: {hyp.expected_holding_period}")
    
    momentum_hyps = get_hypotheses_by_category('momentum')
    print(f"\n动量类假设 ({len(momentum_hyps)} 个):")
    for hyp in momentum_hyps:
        print(f"  - {hyp.name}")


def test_hypothesis_driven_generation():
    """测试假设驱动生成"""
    print("\n\n" + "=" * 60)
    print("测试 2: 假设驱动生成")
    print("=" * 60)
    
    # 生成因子
    print("\n生成因子（基于所有假设）:")
    print("-" * 60)
    
    candidates = generate_factors_from_hypotheses(
        hypothesis_ids=['price_momentum', 'value_mean_reversion', 'earnings_growth'],
        num_variants_per_hypothesis=2,
    )
    
    print(f"\n生成了 {len(candidates)} 个因子候选:")
    for i, cand in enumerate(candidates, 1):
        print(f"\n{i}. {cand['name']}")
        print(f"   表达式: {cand['expression']}")
        print(f"   假设: {cand['hypothesis_name']}")
        print(f"   类别: {cand['category']}")
        print(f"   预期 IC: {cand['expected_ic']:.3f}")
    
    # 测试验证
    print("\n\n测试假设验证:")
    print("-" * 60)
    
    # 模拟回测结果
    factor_results = [
        {
            'name': 'price_momentum_v1',
            'hypothesis_id': 'price_momentum',
            'rank_ic_mean': 0.045,  # 接近预期 0.04
            'sharpe_net': 0.85,     # 接近预期 0.8
        },
        {
            'name': 'value_mean_reversion_v1',
            'hypothesis_id': 'value_mean_reversion',
            'rank_ic_mean': 0.015,  # 低于预期 0.03
            'sharpe_net': 0.3,      # 低于预期 0.6
        },
    ]
    
    report = create_hypothesis_report(factor_results, tolerance=0.5)
    
    print(f"\n验证报告:")
    print(f"  总数: {report['total']}")
    print(f"  通过: {report['accepted']}")
    print(f"  拒绝: {report['rejected']}")
    print(f"  通过率: {report['acceptance_rate']*100:.1f}%")
    
    for val in report['validations']:
        print(f"\n  因子: {val['hypothesis_name']}")
        print(f"    决策: {val['decision']}")
        print(f"    原因: {val['reason']}")
        print(f"    IC: 预期={val['ic_expected']:.3f}, 实际={val['ic_actual']:.3f}, 偏离={val['ic_deviation']*100:.1f}%")
        print(f"    夏普: 预期={val['sharpe_expected']:.2f}, 实际={val['sharpe_actual']:.2f}, 偏离={val['sharpe_deviation']*100:.1f}%")


def test_novelty_judge_enhanced():
    """测试增强的 Novelty Judge"""
    print("\n\n" + "=" * 60)
    print("测试 3: 增强的 Novelty Judge")
    print("=" * 60)
    
    # 测试表达式标准化
    print("\n3.1 表达式标准化:")
    print("-" * 60)
    
    test_cases = [
        ('A + B', 'B + A', True),
        ('A * B', 'B * A', True),
        ('A - B', 'B - A', False),
        ('A / B', 'B / A', False),
        ('(A + B) * C', 'C * (B + A)', True),
    ]
    
    for expr1, expr2, expected in test_cases:
        result = ExpressionNormalizer.are_equivalent(expr1, expr2)
        status = "✓" if result == expected else "✗"
        print(f"  {status} '{expr1}' vs '{expr2}': {result} (预期: {expected})")
    
    # 测试四层检查
    print("\n\n3.2 四层检查:")
    print("-" * 60)
    
    # 创建模拟数据
    np.random.seed(42)
    n = 1000
    
    # 已有因子
    factor_a = pd.Series(np.random.randn(n), name='factor_a')
    factor_b = pd.Series(np.random.randn(n), name='factor_b')
    
    existing_factors = [
        {'name': 'factor_a', 'expression': 'close / close_20d - 1', 'values': factor_a},
        {'name': 'factor_b', 'expression': 'volume / volume_20d - 1', 'values': factor_b},
    ]
    
    # 测试用例
    test_factors = [
        {
            'name': 'test_1_equivalent',
            'expression': 'close_20d / close - 1',  # 等价于 factor_a（只是符号相反）
            'values': -factor_a,
        },
        {
            'name': 'test_2_high_corr',
            'expression': 'close / close_20d * 2',
            'values': factor_a * 2 + np.random.randn(n) * 0.01,  # 高相关
        },
        {
            'name': 'test_3_moderate_corr',
            'expression': 'close / close_20d + volume / volume_20d',
            'values': factor_a * 0.5 + factor_b * 0.5 + np.random.randn(n) * 0.3,  # 中等相关
        },
        {
            'name': 'test_4_novel',
            'expression': 'new_factor',
            'values': pd.Series(np.random.randn(n)),  # 全新因子
        },
    ]
    
    judge = NoveltyJudgeEnhanced()
    
    for test_factor in test_factors:
        print(f"\n测试因子: {test_factor['name']}")
        result = judge.check_novelty(
            new_factor_name=test_factor['name'],
            new_factor_expression=test_factor['expression'],
            new_factor_values=test_factor['values'],
            existing_factors=existing_factors,
        )
        
        print(f"  决策: {result['decision']}")
        print(f"  原因: {result['reason']}")
        print(f"  层级: 第 {result['layer']} 层")
        print(f"  置信度: {result['confidence']:.2f}")


def test_factor_decay_analysis():
    """测试因子衰减分析"""
    print("\n\n" + "=" * 60)
    print("测试 4: 因子衰减分析")
    print("=" * 60)
    
    # 创建模拟因子收益序列
    np.random.seed(42)
    n = 100
    
    # 模拟一个衰减的因子收益
    t = np.arange(n)
    true_lambda = 0.05
    factor_returns = pd.Series(
        0.01 * np.exp(-true_lambda * t) + np.random.randn(n) * 0.005
    )
    
    print("\n4.1 分析因子衰减:")
    print("-" * 60)
    
    decay_profile = analyze_factor_decay(factor_returns, max_lag=20)
    
    print(f"\n衰减分析结果:")
    print(f"  半衰期: {decay_profile.half_life_days:.2f} 天")
    print(f"  最佳持有期: {decay_profile.optimal_holding_period} 天")
    print(f"  衰减率 (λ): {decay_profile.decay_rate:.4f}")
    print(f"  拟合优度 (R²): {decay_profile.r_squared:.4f}")
    
    print(f"\n衰减曲线 (前 10 个滞后期):")
    for lag, ic in zip(decay_profile.lags[:10], decay_profile.ics[:10]):
        print(f"  滞后 {lag} 天: IC = {ic:.4f}")
    
    # 测试调仓频率推荐
    print("\n\n4.2 调仓频率推荐:")
    print("-" * 60)
    
    for cost in [50, 150, 250]:
        rec = recommend_rebalance_frequency(decay_profile, cost)
        print(f"\n交易成本 {cost} bp:")
        print(f"  最佳持有期: {rec['optimal_holding_days']} 天")
        print(f"  调整后持有期: {rec['adjusted_holding_days']} 天")
        print(f"  推荐频率: {rec['rebalance_frequency_cn']}")
        print(f"  原因: {rec['reason']}")
    
    # 测试因子失效检测
    print("\n\n4.3 因子失效检测:")
    print("-" * 60)
    
    # 模拟一个失效的因子（半衰期缩短）
    factor_returns_failing = pd.Series(
        0.005 * np.exp(-0.15 * t) + np.random.randn(n) * 0.005
    )
    
    decay_profile_failing = analyze_factor_decay(factor_returns_failing, max_lag=20)
    
    failure_check = detect_factor_failure(
        current_decay=decay_profile_failing,
        historical_decay=decay_profile,
        threshold=0.5,
    )
    
    print(f"\n失效检测结果:")
    print(f"  是否失效: {failure_check['is_failing']}")
    print(f"  原因: {failure_check['reason']}")
    if failure_check['is_failing']:
        if 'current_half_life' in failure_check:
            print(f"  当前半衰期: {failure_check['current_half_life']:.2f} 天")
            print(f"  历史半衰期: {failure_check['historical_half_life']:.2f} 天")
        if 'current_ic' in failure_check:
            print(f"  当前 IC: {failure_check['current_ic']:.4f}")
            print(f"  历史 IC: {failure_check['historical_ic']:.4f}")
        print(f"  比例: {failure_check['ratio']:.2f}")
        print(f"  严重程度: {failure_check['severity']}")
    
    # 创建完整报告
    print("\n\n4.4 完整衰减报告:")
    print("-" * 60)
    
    report = create_decay_report('test_factor', decay_profile, 150)
    
    print(f"\n因子: {report['factor_name']}")
    print(f"半衰期: {report['half_life_days']:.2f} 天")
    print(f"最佳持有期: {report['optimal_holding_period']} 天")
    print(f"解释: {report['interpretation']}")
    print(f"\n调仓建议:")
    rec = report['rebalance_recommendation']
    print(f"  频率: {rec['rebalance_frequency_cn']}")
    print(f"  调整后持有期: {rec['adjusted_holding_days']} 天")
    print(f"  原因: {rec['reason']}")


if __name__ == '__main__':
    test_hypothesis_library()
    test_hypothesis_driven_generation()
    test_novelty_judge_enhanced()
    test_factor_decay_analysis()
    
    print("\n\n" + "=" * 60)
    print("P1 阶段所有测试完成！")
    print("=" * 60)
    print("\n已完成的功能:")
    print("  ✅ 投资假设库（12 个经典假设）")
    print("  ✅ 假设驱动生成")
    print("  ✅ 增强的 Novelty Judge（四层检查）")
    print("  ✅ 因子衰减分析")
    print("\n下一步:")
    print("  1. 集成到现有工作流")
    print("  2. 重新生成因子")
    print("  3. 开始 P2 阶段（组合优化）")
    print("=" * 60)

#!/usr/bin/env python3
"""
测试交易成本模型

验证：
1. TransactionCostModel 能否正常计算成本
2. evaluate_factor 能否正常工作（包含新字段）
"""

import pandas as pd
import numpy as np
from src.factor_lab.transaction_cost import TransactionCostModel, TransactionCostConfig, estimate_turnover_from_ic_decay
from src.factor_lab.evaluation import evaluate_factor, FactorEvaluation


def test_transaction_cost_model():
    """测试交易成本模型"""
    print("=" * 60)
    print("测试 1: 交易成本模型基本功能")
    print("=" * 60)
    
    config = TransactionCostConfig()
    model = TransactionCostModel(config)
    
    # 测试不同换手率的成本
    for turnover in [0.2, 0.5, 1.0]:
        cost = model.calculate_cost(
            turnover=turnover,
            position_size=1e7,  # 1000 万
            adv=1e8,  # 1 亿
        )
        print(f"\n换手率 {turnover*100:.0f}%:")
        print(f"  固定成本: {cost['fixed_cost']*10000:.2f} bp")
        print(f"  滑点成本: {cost['slippage_cost']*10000:.2f} bp")
        print(f"  冲击成本: {cost['impact_cost']*10000:.2f} bp")
        print(f"  总成本: {cost['total_cost_bps']:.2f} bp")
    
    print("\n" + "=" * 60)
    print("测试 2: 从收益序列计算净夏普")
    print("=" * 60)
    
    # 模拟一个因子的收益序列
    np.random.seed(42)
    returns = pd.Series(np.random.normal(0.001, 0.01, 100))  # 日收益
    
    result = model.calculate_cost_from_returns(
        returns=returns,
        turnover_rate=0.5,
        position_size=1e7,
        adv=1e8,
    )
    
    print(f"\n毛收益均值: {result['gross_return_mean']*10000:.2f} bp")
    print(f"净收益均值: {result['net_return_mean']*10000:.2f} bp")
    print(f"单期成本: {result['cost_bps']:.2f} bp")
    print(f"净夏普: {result['sharpe_net']:.4f}")
    
    print("\n" + "=" * 60)
    print("测试 3: 根据 IC 估算换手率")
    print("=" * 60)
    
    test_cases = [
        (0.02, 0.05, "低 IC，高波动"),
        (0.04, 0.03, "中等 IC，中等波动"),
        (0.06, 0.02, "高 IC，低波动"),
    ]
    
    for ic_mean, ic_std, desc in test_cases:
        turnover = estimate_turnover_from_ic_decay(ic_mean, ic_std)
        ir = ic_mean / ic_std if ic_std > 0 else 0
        print(f"\n{desc}:")
        print(f"  IC={ic_mean:.3f}, IR={ir:.2f}")
        print(f"  估算换手率: {turnover*100:.0f}%")


def test_evaluate_factor():
    """测试 evaluate_factor 函数（包含新字段）"""
    print("\n" + "=" * 60)
    print("测试 4: evaluate_factor 函数")
    print("=" * 60)
    
    # 构造测试数据
    np.random.seed(42)
    n_dates = 50
    n_stocks = 100
    
    dates = pd.date_range('2023-01-01', periods=n_dates, freq='D')
    data = []
    
    for date in dates:
        for stock_id in range(n_stocks):
            factor_value = np.random.randn()
            forward_return = factor_value * 0.001 + np.random.randn() * 0.01
            data.append({
                'date': date,
                'ticker': f'stock_{stock_id}',
                'factor_value': factor_value,
                'forward_return_5d': forward_return,
            })
    
    frame = pd.DataFrame(data)
    
    # 评估因子
    thresholds = {
        'min_rank_ic': 0.02,
        'min_top_bottom_spread': 0.0,
        'min_sharpe_net': 0.5,
    }
    
    result = evaluate_factor(
        frame=frame,
        factor_name='test_factor',
        expression='test_expression',
        thresholds=thresholds,
    )
    
    print(f"\n因子评估结果:")
    print(f"  观测数: {result.observations}")
    print(f"  Rank IC 均值: {result.rank_ic_mean:.4f}")
    print(f"  Rank IC IR: {result.rank_ic_ir:.4f}")
    print(f"  多空收益差: {result.top_bottom_spread_mean:.4f}")
    print(f"  换手率: {result.turnover_rate*100:.1f}%")
    print(f"  交易成本: {result.transaction_cost_bps:.2f} bp")
    print(f"  毛夏普: {result.sharpe_gross:.4f}")
    print(f"  净夏普: {result.sharpe_net:.4f}")
    print(f"  年化净收益: {result.net_return_annual*100:.2f}%")
    print(f"  通过门槛: {result.pass_gate}")
    if result.fail_reason:
        print(f"  失败原因: {result.fail_reason}")


if __name__ == '__main__':
    test_transaction_cost_model()
    test_evaluate_factor()
    print("\n" + "=" * 60)
    print("所有测试完成！")
    print("=" * 60)

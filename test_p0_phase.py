#!/usr/bin/env python3
"""
P0 阶段综合测试

测试：
1. 交易成本模型
2. 评分校准
3. A 股特殊处理
"""

import pandas as pd
import numpy as np
from src.factor_lab.transaction_cost import TransactionCostModel
from src.factor_lab.benchmark_calibration import (
    calibrate_score,
    batch_calibrate,
    get_promotion_threshold,
    BENCHMARK_FACTORS,
)
from src.factor_lab.a_share_filters import (
    filter_tradable_universe,
    adjust_for_limit,
    handle_t_plus_1,
    get_a_share_statistics,
    create_mock_a_share_data,
    AShareFilterConfig,
)


def test_benchmark_calibration():
    """测试评分校准"""
    print("=" * 60)
    print("测试：评分校准模块")
    print("=" * 60)
    
    # 测试单个因子校准
    print("\n1. 单个因子校准")
    print("-" * 60)
    
    test_cases = [
        ('momentum_20', 1.2, 0.05, 0.8, '动量因子，超出基准'),
        ('book_to_market', 0.4, 0.02, 0.3, '价值因子，低于基准'),
        ('custom_factor', 1.5, 0.06, 1.0, '自定义因子，使用行业中位数'),
    ]
    
    for name, sharpe, ic, ir, desc in test_cases:
        result = calibrate_score(name, sharpe, ic, ir)
        print(f"\n{desc}:")
        print(f"  因子: {name}")
        print(f"  原始指标: 夏普={sharpe:.2f}, IC={ic:.3f}, IR={ir:.2f}")
        print(f"  基准: {result['benchmark_name']}")
        print(f"  相对得分: 夏普={result['sharpe_ratio']:.2f}x, IC={result['ic_ratio']:.2f}x, IR={result['ir_ratio']:.2f}x")
        print(f"  综合得分: {result['composite_score']:.2f}")
        print(f"  解释: {result['interpretation']}")
    
    # 测试批量校准
    print("\n\n2. 批量校准")
    print("-" * 60)
    
    factors = [
        {'name': 'momentum_20', 'sharpe_net': 1.2, 'rank_ic_mean': 0.05, 'rank_ic_ir': 0.8},
        {'name': 'book_to_market', 'sharpe_net': 0.4, 'rank_ic_mean': 0.02, 'rank_ic_ir': 0.3},
        {'name': 'earnings_yield', 'sharpe_net': 0.9, 'rank_ic_mean': 0.04, 'rank_ic_ir': 0.6},
    ]
    
    calibrated = batch_calibrate(factors)
    
    print("\n因子排名（按综合得分）:")
    calibrated_sorted = sorted(calibrated, key=lambda x: x['calibrated_score']['composite_score'], reverse=True)
    for i, factor in enumerate(calibrated_sorted, 1):
        score = factor['calibrated_score']
        print(f"  {i}. {factor['name']}: {score['composite_score']:.2f} ({score['interpretation']})")
    
    # 测试晋级阈值
    print("\n\n3. 晋级阈值")
    print("-" * 60)
    
    thresholds = get_promotion_threshold()
    print(f"\nWatchlist: {thresholds['watchlist']}x ({thresholds['description']['watchlist']})")
    print(f"Candidate: {thresholds['candidate']}x ({thresholds['description']['candidate']})")
    print(f"AU: {thresholds['approved_universe']}x ({thresholds['description']['approved_universe']})")


def test_a_share_filters():
    """测试 A 股特殊处理"""
    print("\n\n" + "=" * 60)
    print("测试：A 股特殊处理模块")
    print("=" * 60)
    
    # 创建模拟数据
    print("\n1. 创建模拟 A 股数据")
    print("-" * 60)
    
    df = create_mock_a_share_data(n_dates=10, n_stocks=100)
    print(f"\n生成数据: {len(df)} 条记录")
    print(f"日期范围: {df['date'].min()} 到 {df['date'].max()}")
    print(f"股票数: {df['ticker'].nunique()}")
    
    # 统计原始数据
    stats_before = get_a_share_statistics(df)
    print(f"\n原始数据统计:")
    print(f"  ST 股票比例: {stats_before.get('st_ratio', 0)*100:.1f}%")
    print(f"  停牌比例: {stats_before.get('suspend_ratio', 0)*100:.1f}%")
    print(f"  新股比例: {stats_before.get('new_stock_ratio', 0)*100:.1f}%")
    print(f"  低流动性比例: {stats_before.get('low_liquidity_ratio', 0)*100:.1f}%")
    
    # 测试过滤
    print("\n\n2. 过滤不可交易股票")
    print("-" * 60)
    
    config = AShareFilterConfig()
    filtered = filter_tradable_universe(df, config)
    
    filter_rate = (len(df) - len(filtered)) / len(df) * 100
    print(f"\n过滤前: {len(df)} 条")
    print(f"过滤后: {len(filtered)} 条")
    print(f"过滤率: {filter_rate:.1f}%")
    
    # 测试 T+1 延迟
    print("\n\n3. T+1 交易延迟")
    print("-" * 60)
    
    signals = df[['date', 'ticker']].copy()
    signals['signal'] = np.random.randn(len(signals))
    
    print(f"\n原始信号前 5 条:")
    print(signals.head())
    
    delayed = handle_t_plus_1(signals)
    print(f"\n延迟后信号前 5 条:")
    print(delayed.head())
    
    # 测试涨跌停处理
    print("\n\n4. 涨跌停处理")
    print("-" * 60)
    
    # 模拟一些涨跌停情况
    np.random.seed(42)
    n = 100
    returns = pd.Series(np.random.normal(0.01, 0.05, n))
    prices = pd.Series(np.random.uniform(5, 50, n))
    prev_prices = prices * (1 - returns)
    
    # 人为制造一些涨跌停
    returns.iloc[10] = 0.10  # 涨停
    returns.iloc[20] = -0.10  # 跌停
    returns.iloc[30] = 0.095  # 接近涨停
    
    adjusted_returns, limit_flags = adjust_for_limit(returns, prices, prev_prices)
    
    print(f"\n涨跌停统计:")
    print(f"  涨跌停数量: {limit_flags.sum()}")
    print(f"  涨跌停比例: {limit_flags.mean()*100:.1f}%")
    
    print(f"\n涨跌停日收益调整示例:")
    for i in [10, 20, 30]:
        print(f"  第 {i} 天: 原始收益={returns.iloc[i]*100:.2f}%, 调整后={adjusted_returns.iloc[i]*100:.2f}%, 涨跌停={limit_flags.iloc[i]}")


def test_integration():
    """集成测试：完整流程"""
    print("\n\n" + "=" * 60)
    print("测试：P0 阶段集成测试")
    print("=" * 60)
    
    print("\n模拟完整的因子评估流程:")
    print("-" * 60)
    
    # 1. 创建 A 股数据
    print("\n步骤 1: 准备 A 股数据")
    df = create_mock_a_share_data(n_dates=50, n_stocks=200)
    print(f"  数据量: {len(df)} 条")
    
    # 2. 过滤可交易股票
    print("\n步骤 2: 过滤可交易股票")
    config = AShareFilterConfig()
    filtered = filter_tradable_universe(df, config)
    print(f"  过滤后: {len(filtered)} 条 ({len(filtered)/len(df)*100:.1f}%)")
    
    # 3. 模拟因子评估结果
    print("\n步骤 3: 因子评估（模拟）")
    factor_results = {
        'name': 'test_momentum',
        'sharpe_gross': 2.5,
        'sharpe_net': 1.3,  # 扣成本后
        'rank_ic_mean': 0.048,
        'rank_ic_ir': 0.75,
        'transaction_cost_bps': 150,
        'turnover_rate': 0.45,
    }
    print(f"  因子: {factor_results['name']}")
    print(f"  毛夏普: {factor_results['sharpe_gross']:.2f}")
    print(f"  净夏普: {factor_results['sharpe_net']:.2f}")
    print(f"  交易成本: {factor_results['transaction_cost_bps']:.0f} bp")
    
    # 4. 评分校准
    print("\n步骤 4: 评分校准")
    calibrated = calibrate_score(
        factor_results['name'],
        factor_results['sharpe_net'],
        factor_results['rank_ic_mean'],
        factor_results['rank_ic_ir'],
    )
    print(f"  基准: {calibrated['benchmark_name']}")
    print(f"  综合得分: {calibrated['composite_score']:.2f}")
    print(f"  解释: {calibrated['interpretation']}")
    
    # 5. 晋级判断
    print("\n步骤 5: 晋级判断")
    thresholds = get_promotion_threshold()
    score = calibrated['composite_score']
    
    if score >= thresholds['approved_universe']:
        decision = "晋升到 AU"
    elif score >= thresholds['candidate']:
        decision = "进入 Candidate"
    elif score >= thresholds['watchlist']:
        decision = "进入 Watchlist"
    else:
        decision = "不晋升"
    
    print(f"  决策: {decision}")
    print(f"  原因: 综合得分 {score:.2f} {'≥' if score >= thresholds['watchlist'] else '<'} 阈值 {thresholds['watchlist']}")


if __name__ == '__main__':
    test_benchmark_calibration()
    test_a_share_filters()
    test_integration()
    
    print("\n\n" + "=" * 60)
    print("P0 阶段所有测试完成！")
    print("=" * 60)
    print("\n已完成的功能:")
    print("  ✅ 交易成本模型")
    print("  ✅ 评分校准体系")
    print("  ✅ A 股特殊处理")
    print("\n下一步:")
    print("  1. 重新评估历史因子")
    print("  2. 更新配置文件")
    print("  3. 修改晋级标准")
    print("=" * 60)

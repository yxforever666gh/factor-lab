#!/usr/bin/env python3
"""
P2 阶段综合测试

测试：
1. 多因子组合优化器
2. 容量分析
3. 因子归因体系
"""

import pandas as pd
import numpy as np
from src.factor_lab.portfolio_optimizer import (
    PortfolioOptimizer,
    OptimizationConfig,
    compare_optimization_methods,
    backtest_portfolio,
)
from src.factor_lab.capacity_analysis import (
    CapacityAnalyzer,
    CapacityConfig,
    classify_capacity,
    create_capacity_report,
    batch_capacity_analysis,
)
from src.factor_lab.factor_attribution import (
    FactorAttribution,
    AttributionConfig,
    create_style_factors,
    create_attribution_report,
    batch_attribution_analysis,
)


def test_portfolio_optimizer():
    """测试多因子组合优化器"""
    print("=" * 60)
    print("测试 1: 多因子组合优化器")
    print("=" * 60)
    
    # 创建模拟因子收益
    np.random.seed(42)
    n_days = 252
    n_factors = 5
    
    # 模拟相关的因子收益
    mean_returns = np.array([0.0005, 0.0004, 0.0006, 0.0003, 0.0007])
    cov_matrix = np.array([
        [0.01, 0.003, 0.002, 0.001, 0.002],
        [0.003, 0.008, 0.002, 0.001, 0.001],
        [0.002, 0.002, 0.012, 0.003, 0.002],
        [0.001, 0.001, 0.003, 0.006, 0.001],
        [0.002, 0.001, 0.002, 0.001, 0.015],
    ]) * 0.01
    
    factor_returns = pd.DataFrame(
        np.random.multivariate_normal(mean_returns, cov_matrix, n_days),
        columns=[f'factor_{i+1}' for i in range(n_factors)]
    )
    
    print("\n1.1 最大夏普优化:")
    print("-" * 60)
    
    optimizer = PortfolioOptimizer()
    result = optimizer.optimize_weights(factor_returns)
    
    print(f"\n优化结果:")
    print(f"  优化成功: {result['optimization_success']}")
    print(f"  组合夏普: {result['portfolio_sharpe']*np.sqrt(252):.4f}")
    print(f"  等权夏普: {result['equal_weight_sharpe']*np.sqrt(252):.4f}")
    print(f"  改进: {result['improvement']*np.sqrt(252):.4f}")
    
    print(f"\n最优权重:")
    for factor, weight in result['optimal_weights'].items():
        print(f"  {factor}: {weight*100:.2f}%")
    
    print("\n\n1.2 最小方差优化:")
    print("-" * 60)
    
    min_var_result = optimizer.optimize_min_variance(factor_returns)
    
    print(f"\n优化结果:")
    print(f"  组合波动率: {min_var_result['portfolio_volatility']*np.sqrt(252)*100:.2f}%")
    print(f"  等权波动率: {min_var_result['equal_weight_volatility']*np.sqrt(252)*100:.2f}%")
    print(f"  波动率降低: {min_var_result['volatility_reduction']*np.sqrt(252)*100:.2f}%")
    
    print(f"\n最优权重:")
    for factor, weight in min_var_result['optimal_weights'].items():
        print(f"  {factor}: {weight*100:.2f}%")
    
    print("\n\n1.3 风险平价优化:")
    print("-" * 60)
    
    risk_parity_result = optimizer.optimize_risk_parity(factor_returns)
    
    print(f"\n优化结果:")
    print(f"  组合波动率: {risk_parity_result['portfolio_volatility']*np.sqrt(252)*100:.2f}%")
    
    print(f"\n最优权重:")
    for factor, weight in risk_parity_result['optimal_weights'].items():
        print(f"  {factor}: {weight*100:.2f}%")
    
    print(f"\n风险贡献:")
    for factor, contrib in risk_parity_result['risk_contributions'].items():
        print(f"  {factor}: {contrib*100:.4f}%")
    
    print("\n\n1.4 方法对比:")
    print("-" * 60)
    
    comparison = compare_optimization_methods(factor_returns)
    print(f"\n{comparison.to_string(index=False)}")


def test_capacity_analysis():
    """测试容量分析"""
    print("\n\n" + "=" * 60)
    print("测试 2: 容量分析")
    print("=" * 60)
    
    # 创建模拟市场数据
    np.random.seed(42)
    n_stocks = 500
    
    market_data = pd.DataFrame({
        'ticker': [f'stock_{i:03d}' for i in range(n_stocks)],
        'amount_20d_avg': np.random.lognormal(17, 1, n_stocks),  # 平均成交额
        'market_cap': np.random.lognormal(20, 1.5, n_stocks),  # 市值
        'price': np.random.uniform(5, 100, n_stocks),
    })
    market_data = market_data.set_index('ticker')
    
    # 创建模拟因子值
    factor_values = pd.Series(
        np.random.randn(n_stocks),
        index=market_data.index,
        name='test_factor'
    )
    
    print("\n2.1 单因子容量分析:")
    print("-" * 60)
    
    analyzer = CapacityAnalyzer()
    capacity_result = analyzer.estimate_capacity(factor_values, market_data)
    
    print(f"\n容量分析结果:")
    print(f"  总容量: {capacity_result['capacity_million']:.2f} 百万元")
    print(f"  日容量: {capacity_result['daily_capacity']/1e6:.2f} 百万元")
    print(f"  前 10% 持仓数: {capacity_result['top_holdings_count']}")
    print(f"  总 ADV: {capacity_result['total_adv']/1e8:.2f} 亿元")
    print(f"  限制因素: {capacity_result['limiting_factor']}")
    print(f"  换手周期: {capacity_result['turnover_days']} 天")
    
    # 容量分类
    classification = classify_capacity(capacity_result['capacity_million'])
    print(f"\n容量等级:")
    print(f"  等级: {classification['level_cn']}")
    print(f"  描述: {classification['description']}")
    
    # 完整报告
    report = create_capacity_report('test_factor', capacity_result)
    print(f"\n完整报告:")
    print(f"  因子: {report['factor_name']}")
    print(f"  容量等级: {report['capacity_level']}")
    print(f"  建议: {report['recommendation']}")
    
    print("\n\n2.2 不同分位数的容量:")
    print("-" * 60)
    
    quantile_results = analyzer.estimate_capacity_by_quantile(
        factor_values,
        market_data,
        quantiles=[0.05, 0.1, 0.2, 0.3]
    )
    
    print(f"\n{quantile_results.to_string(index=False)}")
    
    print("\n\n2.3 批量容量分析:")
    print("-" * 60)
    
    # 创建多个因子
    factors = {
        f'factor_{i}': pd.Series(np.random.randn(n_stocks), index=market_data.index)
        for i in range(1, 4)
    }
    
    batch_results = batch_capacity_analysis(factors, market_data)
    print(f"\n{batch_results[['factor_name', 'capacity_million', 'capacity_level', 'top_holdings_count']].to_string(index=False)}")


def test_factor_attribution():
    """测试因子归因"""
    print("\n\n" + "=" * 60)
    print("测试 3: 因子归因体系")
    print("=" * 60)
    
    # 创建模拟风格因子收益
    np.random.seed(42)
    n_days = 252
    
    style_returns = pd.DataFrame({
        'market': np.random.normal(0.0005, 0.01, n_days),
        'size': np.random.normal(0.0002, 0.008, n_days),
        'value': np.random.normal(0.0003, 0.007, n_days),
        'momentum': np.random.normal(0.0004, 0.009, n_days),
    })
    
    print("\n3.1 纯 alpha 因子:")
    print("-" * 60)
    
    # 创建一个纯 alpha 因子（与风格因子无关）
    pure_alpha = pd.Series(
        np.random.normal(0.0008, 0.01, n_days),
        name='pure_alpha'
    )
    
    attributor = FactorAttribution()
    attribution = attributor.attribute_to_styles(pure_alpha, style_returns)
    
    print(f"\n归因结果:")
    print(f"  Alpha (年化): {attribution['alpha_annual']*100:.2f}%")
    print(f"  R²: {attribution['r_squared']:.4f}")
    print(f"  观测数: {attribution['observations']}")
    
    print(f"\n风格暴露 (Beta):")
    for style, beta in attribution['betas'].items():
        print(f"  {style}: {beta:.4f}")
    
    print(f"\n解释: {attribution['interpretation']}")
    
    print("\n\n3.2 风格因子组合:")
    print("-" * 60)
    
    # 创建一个风格因子的线性组合
    style_combo = (
        style_returns['market'] * 0.5 +
        style_returns['size'] * 0.3 +
        style_returns['value'] * 0.2 +
        np.random.normal(0, 0.002, n_days)  # 少量噪音
    )
    style_combo.name = 'style_combo'
    
    attribution2 = attributor.attribute_to_styles(style_combo, style_returns)
    
    print(f"\n归因结果:")
    print(f"  Alpha (年化): {attribution2['alpha_annual']*100:.2f}%")
    print(f"  R²: {attribution2['r_squared']:.4f}")
    
    print(f"\n风格暴露 (Beta):")
    for style, beta in attribution2['betas'].items():
        print(f"  {style}: {beta:.4f}")
    
    print(f"\n解释: {attribution2['interpretation']}")
    
    print("\n\n3.3 混合型因子:")
    print("-" * 60)
    
    # 创建一个混合型因子（部分风格 + 部分 alpha）
    mixed_factor = (
        style_returns['momentum'] * 0.6 +
        np.random.normal(0.0005, 0.008, n_days)  # alpha 部分
    )
    mixed_factor.name = 'mixed_factor'
    
    attribution3 = attributor.attribute_to_styles(mixed_factor, style_returns)
    
    print(f"\n归因结果:")
    print(f"  Alpha (年化): {attribution3['alpha_annual']*100:.2f}%")
    print(f"  R²: {attribution3['r_squared']:.4f}")
    
    print(f"\n风格暴露 (Beta):")
    for style, beta in attribution3['betas'].items():
        print(f"  {style}: {beta:.4f}")
    
    print(f"\n解释: {attribution3['interpretation']}")
    
    print("\n\n3.4 批量归因分析:")
    print("-" * 60)
    
    factors = {
        'pure_alpha': pure_alpha,
        'style_combo': style_combo,
        'mixed_factor': mixed_factor,
    }
    
    batch_results = batch_attribution_analysis(factors, style_returns)
    print(f"\n{batch_results[['factor_name', 'factor_type', 'alpha_annual_pct', 'r_squared']].to_string(index=False)}")


if __name__ == '__main__':
    test_portfolio_optimizer()
    test_capacity_analysis()
    test_factor_attribution()
    
    print("\n\n" + "=" * 60)
    print("P2 阶段所有测试完成！")
    print("=" * 60)
    print("\n已完成的功能:")
    print("  ✅ 多因子组合优化器（最大夏普、最小方差、风险平价）")
    print("  ✅ 容量分析（估算因子能承载的资金量）")
    print("  ✅ 因子归因体系（判断是否有真正的 alpha）")
    print("\n下一步:")
    print("  1. 集成到现有工作流")
    print("  2. 对 AU 因子进行组合优化")
    print("  3. 开始 P3 阶段（监控体系、回测验证）")
    print("=" * 60)

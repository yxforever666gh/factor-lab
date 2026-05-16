"""
评分校准模块

建立可对标的评分体系，将原始指标转换为相对基准的得分。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class BenchmarkFactor:
    """基准因子定义"""
    name: str
    expected_sharpe: float  # 预期夏普比率（扣成本后）
    expected_ic: float  # 预期 IC
    expected_ir: float  # 预期 IR
    description: str = ""


# A 股市场经典因子的基准表现
# 数据来源：业界共识 + 历史回测
BENCHMARK_FACTORS = {
    # 动量类
    'momentum_20': BenchmarkFactor(
        name='20日动量',
        expected_sharpe=0.8,
        expected_ic=0.04,
        expected_ir=0.6,
        description='20日价格动量，中短期趋势跟踪'
    ),
    'momentum_60': BenchmarkFactor(
        name='60日动量',
        expected_sharpe=1.0,
        expected_ic=0.045,
        expected_ir=0.7,
        description='60日价格动量，中期趋势'
    ),
    
    # 价值类
    'book_to_market': BenchmarkFactor(
        name='账面市值比',
        expected_sharpe=0.6,
        expected_ic=0.03,
        expected_ir=0.5,
        description='经典价值因子，长期有效'
    ),
    'earnings_yield': BenchmarkFactor(
        name='盈利收益率',
        expected_sharpe=0.7,
        expected_ic=0.035,
        expected_ir=0.55,
        description='E/P，盈利能力指标'
    ),
    
    # 质量类
    'roa': BenchmarkFactor(
        name='资产回报率',
        expected_sharpe=0.5,
        expected_ic=0.025,
        expected_ir=0.45,
        description='ROA，盈利质量'
    ),
    'roe': BenchmarkFactor(
        name='净资产收益率',
        expected_sharpe=0.6,
        expected_ic=0.03,
        expected_ir=0.5,
        description='ROE，股东回报'
    ),
    
    # 成长类
    'earnings_growth': BenchmarkFactor(
        name='盈利增长',
        expected_sharpe=0.8,
        expected_ic=0.04,
        expected_ir=0.6,
        description='盈利同比增长率'
    ),
    'revenue_growth': BenchmarkFactor(
        name='营收增长',
        expected_sharpe=0.7,
        expected_ic=0.035,
        expected_ir=0.55,
        description='营收同比增长率'
    ),
    
    # 流动性类
    'turnover': BenchmarkFactor(
        name='换手率',
        expected_sharpe=0.4,
        expected_ic=0.02,
        expected_ir=0.4,
        description='成交量/流通股本'
    ),
    'liquidity': BenchmarkFactor(
        name='流动性',
        expected_sharpe=0.5,
        expected_ic=0.025,
        expected_ir=0.45,
        description='综合流动性指标'
    ),
}


# 行业中位数基准（当没有具体因子基准时使用）
INDUSTRY_MEDIAN_BENCHMARK = BenchmarkFactor(
    name='行业中位数',
    expected_sharpe=1.0,
    expected_ic=0.04,
    expected_ir=0.6,
    description='A 股量化因子的行业中位数水平'
)


def find_benchmark(factor_name: str) -> Optional[BenchmarkFactor]:
    """
    根据因子名称查找对应的基准
    
    Args:
        factor_name: 因子名称
    
    Returns:
        匹配的基准因子，如果没有匹配则返回 None
    """
    # 精确匹配
    if factor_name in BENCHMARK_FACTORS:
        return BENCHMARK_FACTORS[factor_name]
    
    # 模糊匹配（根据关键词）
    factor_lower = factor_name.lower()
    
    # 动量类
    if any(kw in factor_lower for kw in ['mom', 'momentum', 'trend', '动量', '趋势']):
        return BENCHMARK_FACTORS['momentum_20']
    
    # 价值类
    if any(kw in factor_lower for kw in ['value', 'book', 'pb', 'pe', 'ep', '价值', '估值']):
        return BENCHMARK_FACTORS['book_to_market']
    
    # 质量类
    if any(kw in factor_lower for kw in ['quality', 'roa', 'roe', 'profit', '质量', '盈利']):
        return BENCHMARK_FACTORS['roa']
    
    # 成长类
    if any(kw in factor_lower for kw in ['growth', 'earning', 'revenue', '成长', '增长']):
        return BENCHMARK_FACTORS['earnings_growth']
    
    # 流动性类
    if any(kw in factor_lower for kw in ['liquidity', 'turnover', 'volume', '流动性', '换手']):
        return BENCHMARK_FACTORS['turnover']
    
    return None


def calibrate_score(
    factor_name: str,
    raw_sharpe: float,
    raw_ic: float,
    raw_ir: float,
) -> dict:
    """
    将原始指标转换为相对基准的得分
    
    得分 = 相对基准的倍数
    - 1.0 = 符合预期
    - 1.5 = 超出预期 50%
    - 0.5 = 低于预期 50%
    
    Args:
        factor_name: 因子名称
        raw_sharpe: 原始夏普比率（扣成本后）
        raw_ic: 原始 IC
        raw_ir: 原始 IR
    
    Returns:
        包含校准得分和基准信息的字典
    """
    # 查找基准
    benchmark = find_benchmark(factor_name)
    if benchmark is None:
        benchmark = INDUSTRY_MEDIAN_BENCHMARK
    
    # 计算相对得分
    sharpe_ratio = raw_sharpe / benchmark.expected_sharpe if benchmark.expected_sharpe > 0 else 0.0
    ic_ratio = raw_ic / benchmark.expected_ic if benchmark.expected_ic > 0 else 0.0
    ir_ratio = raw_ir / benchmark.expected_ir if benchmark.expected_ir > 0 else 0.0
    
    # 综合得分（加权平均）
    # 夏普权重 50%，IC 权重 30%，IR 权重 20%
    composite_score = (
        sharpe_ratio * 0.5 +
        ic_ratio * 0.3 +
        ir_ratio * 0.2
    )
    
    return {
        'benchmark_name': benchmark.name,
        'benchmark_sharpe': benchmark.expected_sharpe,
        'benchmark_ic': benchmark.expected_ic,
        'benchmark_ir': benchmark.expected_ir,
        'sharpe_ratio': round(sharpe_ratio, 4),
        'ic_ratio': round(ic_ratio, 4),
        'ir_ratio': round(ir_ratio, 4),
        'composite_score': round(composite_score, 4),
        'interpretation': _interpret_score(composite_score),
    }


def _interpret_score(score: float) -> str:
    """
    解释得分的含义
    
    Args:
        score: 综合得分
    
    Returns:
        得分解释
    """
    if score >= 2.0:
        return "远超基准（2倍以上）"
    elif score >= 1.5:
        return "显著超出基准（50%以上）"
    elif score >= 1.2:
        return "超出基准（20%以上）"
    elif score >= 0.8:
        return "接近基准（±20%以内）"
    elif score >= 0.5:
        return "低于基准（20-50%）"
    else:
        return "远低于基准（50%以上）"


def get_promotion_threshold(benchmark_name: str = 'industry_median') -> dict:
    """
    获取晋级阈值（相对得分）
    
    Args:
        benchmark_name: 基准名称
    
    Returns:
        晋级阈值配置
    """
    return {
        'watchlist': 1.2,  # 超出基准 20%，进 watchlist
        'candidate': 1.5,  # 超出基准 50%，进 candidate
        'approved_universe': 2.0,  # 超出基准 100%，进 AU
        'description': {
            'watchlist': '超出基准 20%，值得关注',
            'candidate': '超出基准 50%，候选因子',
            'approved_universe': '超出基准 100%，核心因子',
        }
    }


def batch_calibrate(factors: list[dict]) -> list[dict]:
    """
    批量校准因子得分
    
    Args:
        factors: 因子列表，每个因子包含 name, sharpe_net, rank_ic_mean, rank_ic_ir
    
    Returns:
        校准后的因子列表（加入 calibrated_score 字段）
    """
    results = []
    
    for factor in factors:
        calibrated = calibrate_score(
            factor_name=factor.get('name', ''),
            raw_sharpe=factor.get('sharpe_net', 0.0),
            raw_ic=factor.get('rank_ic_mean', 0.0),
            raw_ir=factor.get('rank_ic_ir', 0.0),
        )
        
        result = factor.copy()
        result['calibrated_score'] = calibrated
        results.append(result)
    
    return results

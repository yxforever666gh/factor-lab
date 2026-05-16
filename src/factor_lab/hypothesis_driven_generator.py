"""
假设驱动的因子生成器

基于投资假设生成因子，而不是盲目组合已有因子。
"""

from __future__ import annotations

import json
from typing import List, Optional, Dict, Any

from .hypothesis_library import (
    FactorHypothesis,
    get_hypothesis_by_id,
    select_hypothesis_for_generation,
)


class HypothesisDrivenGenerator:
    """假设驱动的因子生成器"""
    
    def __init__(self, llm_client=None):
        """
        Args:
            llm_client: LLM 客户端（用于生成表达式）
        """
        self.llm_client = llm_client
    
    def generate_from_hypothesis(
        self,
        hypothesis: FactorHypothesis,
        available_fields: List[str],
        num_variants: int = 3,
    ) -> List[Dict[str, Any]]:
        """
        基于假设生成因子变体
        
        Args:
            hypothesis: 投资假设
            available_fields: 可用的数据字段
            num_variants: 生成变体数量
        
        Returns:
            因子候选列表
        """
        # 检查数据要求是否满足
        missing_fields = [
            field for field in hypothesis.data_requirements
            if field not in available_fields
        ]
        
        if missing_fields:
            return []
        
        # 如果有 LLM 客户端，使用 LLM 生成
        if self.llm_client:
            return self._generate_with_llm(hypothesis, available_fields, num_variants)
        else:
            # 否则使用示例表达式
            return self._generate_from_examples(hypothesis, num_variants)
    
    def _generate_from_examples(
        self,
        hypothesis: FactorHypothesis,
        num_variants: int,
    ) -> List[Dict[str, Any]]:
        """从示例表达式生成因子"""
        candidates = []
        
        for i, expr in enumerate(hypothesis.example_expressions[:num_variants]):
            candidates.append({
                'name': f'{hypothesis.id}_v{i+1}',
                'expression': expr,
                'hypothesis_id': hypothesis.id,
                'hypothesis_name': hypothesis.name,
                'category': hypothesis.category,
                'expected_ic': hypothesis.expected_ic,
                'expected_sharpe': hypothesis.expected_sharpe,
                'economic_logic': hypothesis.economic_logic,
            })
        
        return candidates
    
    def _generate_with_llm(
        self,
        hypothesis: FactorHypothesis,
        available_fields: List[str],
        num_variants: int,
    ) -> List[Dict[str, Any]]:
        """使用 LLM 生成因子表达式"""
        prompt = self._build_generation_prompt(hypothesis, available_fields, num_variants)
        
        # 调用 LLM
        # response = self.llm_client.generate(prompt)
        # 这里需要实际的 LLM 集成
        
        # 暂时返回示例表达式
        return self._generate_from_examples(hypothesis, num_variants)
    
    def _build_generation_prompt(
        self,
        hypothesis: FactorHypothesis,
        available_fields: List[str],
        num_variants: int,
    ) -> str:
        """构建 LLM 生成提示"""
        prompt = f"""
你是一个量化因子研究专家。请基于以下投资假设生成 {num_variants} 个因子表达式。

## 投资假设
- **名称**: {hypothesis.name}
- **类别**: {hypothesis.category}
- **假设**: {hypothesis.hypothesis}
- **经济学逻辑**: {hypothesis.economic_logic}
- **预期 IC**: {hypothesis.expected_ic}
- **预期夏普**: {hypothesis.expected_sharpe}
- **预期持有期**: {hypothesis.expected_holding_period}

## 风险因素
{chr(10).join(f'- {risk}' for risk in hypothesis.risk_factors)}

## 失效场景
{chr(10).join(f'- {mode}' for mode in hypothesis.failure_modes)}

## 可用字段
{', '.join(available_fields)}

## 要求
1. 生成 {num_variants} 个不同的因子表达式
2. 每个表达式必须符合投资假设的经济学逻辑
3. 只使用可用字段
4. 表达式应该简洁、可解释
5. 避免过度复杂的组合

## 示例表达式（供参考）
{chr(10).join(f'{i+1}. {expr}' for i, expr in enumerate(hypothesis.example_expressions))}

请以 JSON 格式返回，格式如下：
[
    {{"expression": "...", "explanation": "..."}},
    {{"expression": "...", "explanation": "..."}},
    ...
]
"""
        return prompt
    
    def validate_against_hypothesis(
        self,
        factor_result: Dict[str, Any],
        hypothesis: FactorHypothesis,
        tolerance: float = 0.5,
    ) -> Dict[str, Any]:
        """
        验证因子结果是否符合假设预期
        
        Args:
            factor_result: 因子回测结果
            hypothesis: 投资假设
            tolerance: 容忍度（允许偏离预期的比例）
        
        Returns:
            验证结果
        """
        ic_actual = factor_result.get('rank_ic_mean', 0.0)
        sharpe_actual = factor_result.get('sharpe_net', 0.0)
        
        ic_expected = hypothesis.expected_ic
        sharpe_expected = hypothesis.expected_sharpe
        
        # 计算偏离度
        ic_deviation = abs(ic_actual - ic_expected) / ic_expected if ic_expected > 0 else 1.0
        sharpe_deviation = abs(sharpe_actual - sharpe_expected) / sharpe_expected if sharpe_expected > 0 else 1.0
        
        # 判断是否符合预期
        ic_match = ic_deviation <= tolerance
        sharpe_match = sharpe_deviation <= tolerance
        
        overall_match = ic_match and sharpe_match
        
        return {
            'hypothesis_id': hypothesis.id,
            'hypothesis_name': hypothesis.name,
            'ic_expected': ic_expected,
            'ic_actual': ic_actual,
            'ic_deviation': ic_deviation,
            'ic_match': ic_match,
            'sharpe_expected': sharpe_expected,
            'sharpe_actual': sharpe_actual,
            'sharpe_deviation': sharpe_deviation,
            'sharpe_match': sharpe_match,
            'overall_match': overall_match,
            'decision': 'accept' if overall_match else 'reject',
            'reason': self._build_validation_reason(ic_match, sharpe_match, ic_deviation, sharpe_deviation),
        }
    
    def _build_validation_reason(
        self,
        ic_match: bool,
        sharpe_match: bool,
        ic_deviation: float,
        sharpe_deviation: float,
    ) -> str:
        """构建验证原因"""
        if ic_match and sharpe_match:
            return "符合假设预期"
        
        reasons = []
        if not ic_match:
            reasons.append(f"IC 偏离预期 {ic_deviation*100:.1f}%")
        if not sharpe_match:
            reasons.append(f"夏普偏离预期 {sharpe_deviation*100:.1f}%")
        
        return "; ".join(reasons)


def generate_factors_from_hypotheses(
    hypothesis_ids: Optional[List[str]] = None,
    available_fields: Optional[List[str]] = None,
    num_variants_per_hypothesis: int = 3,
) -> List[Dict[str, Any]]:
    """
    批量生成因子
    
    Args:
        hypothesis_ids: 假设 ID 列表（None 表示使用所有假设）
        available_fields: 可用字段列表
        num_variants_per_hypothesis: 每个假设生成的变体数
    
    Returns:
        因子候选列表
    """
    if available_fields is None:
        # 默认可用字段（A 股常见字段）
        available_fields = [
            'close', 'open', 'high', 'low', 'volume', 'amount',
            'close_5d', 'close_10d', 'close_20d', 'close_60d',
            'volume_20d', 'amount_20d', 'turnover_20d',
            'pb', 'pe', 'ps', 'market_cap',
            'net_profit', 'net_profit_yoy', 'revenue_yoy',
            'roa', 'roe', 'gross_margin',
            'operating_cash_flow', 'fcf',
        ]
    
    generator = HypothesisDrivenGenerator()
    all_candidates = []
    
    if hypothesis_ids:
        hypotheses = [get_hypothesis_by_id(hid) for hid in hypothesis_ids]
        hypotheses = [h for h in hypotheses if h is not None]
    else:
        from .hypothesis_library import HYPOTHESIS_LIBRARY
        hypotheses = HYPOTHESIS_LIBRARY
    
    for hypothesis in hypotheses:
        candidates = generator.generate_from_hypothesis(
            hypothesis=hypothesis,
            available_fields=available_fields,
            num_variants=num_variants_per_hypothesis,
        )
        all_candidates.extend(candidates)
    
    return all_candidates


def create_hypothesis_report(
    factor_results: List[Dict[str, Any]],
    tolerance: float = 0.5,
) -> Dict[str, Any]:
    """
    创建假设验证报告
    
    Args:
        factor_results: 因子回测结果列表
        tolerance: 容忍度
    
    Returns:
        报告
    """
    generator = HypothesisDrivenGenerator()
    validations = []
    
    for result in factor_results:
        hypothesis_id = result.get('hypothesis_id')
        if not hypothesis_id:
            continue
        
        hypothesis = get_hypothesis_by_id(hypothesis_id)
        if not hypothesis:
            continue
        
        validation = generator.validate_against_hypothesis(result, hypothesis, tolerance)
        validations.append(validation)
    
    # 统计
    total = len(validations)
    accepted = sum(1 for v in validations if v['decision'] == 'accept')
    rejected = total - accepted
    
    return {
        'total': total,
        'accepted': accepted,
        'rejected': rejected,
        'acceptance_rate': accepted / total if total > 0 else 0.0,
        'validations': validations,
    }

"""
增强的 Novelty Judge

四层检查机制：
1. 表达式等价性检查
2. 相关性检查
3. 增量信息检查（R²）
4. 经济逻辑检查（可选）
"""

from __future__ import annotations

import ast
import re
from typing import List, Tuple, Optional, Dict, Any

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression


class ExpressionNormalizer:
    """表达式标准化器"""
    
    @staticmethod
    def normalize(expression: str) -> str:
        """
        标准化表达式，使等价表达式具有相同的形式
        
        例如：
        - A + B → A + B
        - B + A → A + B
        - A * B → A * B
        - B * A → A * B
        """
        try:
            tree = ast.parse(expression, mode='eval')
            normalized = ExpressionNormalizer._normalize_node(tree.body)
            return ast.unparse(normalized)
        except:
            # 如果解析失败，返回原表达式
            return expression
    
    @staticmethod
    def _normalize_node(node):
        """递归标准化 AST 节点"""
        if isinstance(node, ast.BinOp):
            # 标准化左右子节点
            left = ExpressionNormalizer._normalize_node(node.left)
            right = ExpressionNormalizer._normalize_node(node.right)
            
            # 对于交换律运算符（+, *），按字典序排序
            if isinstance(node.op, (ast.Add, ast.Mult)):
                left_str = ast.unparse(left)
                right_str = ast.unparse(right)
                if left_str > right_str:
                    left, right = right, left
            
            return ast.BinOp(left=left, op=node.op, right=right)
        
        elif isinstance(node, ast.UnaryOp):
            operand = ExpressionNormalizer._normalize_node(node.operand)
            return ast.UnaryOp(op=node.op, operand=operand)
        
        elif isinstance(node, ast.Name):
            return node
        
        elif isinstance(node, ast.Constant):
            return node
        
        else:
            return node
    
    @staticmethod
    def are_equivalent(expr1: str, expr2: str) -> bool:
        """判断两个表达式是否等价"""
        norm1 = ExpressionNormalizer.normalize(expr1)
        norm2 = ExpressionNormalizer.normalize(expr2)
        return norm1 == norm2


class NoveltyJudgeEnhanced:
    """增强的 Novelty Judge"""
    
    def __init__(
        self,
        correlation_threshold: float = 0.95,
        r2_threshold: float = 0.95,
        moderate_correlation_threshold: float = 0.8,
        moderate_r2_threshold: float = 0.8,
    ):
        """
        Args:
            correlation_threshold: 高相关性阈值
            r2_threshold: 高 R² 阈值（完全冗余）
            moderate_correlation_threshold: 中等相关性阈值
            moderate_r2_threshold: 中等 R² 阈值
        """
        self.correlation_threshold = correlation_threshold
        self.r2_threshold = r2_threshold
        self.moderate_correlation_threshold = moderate_correlation_threshold
        self.moderate_r2_threshold = moderate_r2_threshold
    
    def check_novelty(
        self,
        new_factor_name: str,
        new_factor_expression: str,
        new_factor_values: pd.Series,
        existing_factors: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        四层检查新因子的新颖性
        
        Args:
            new_factor_name: 新因子名称
            new_factor_expression: 新因子表达式
            new_factor_values: 新因子值
            existing_factors: 已有因子列表，每个包含 name, expression, values
        
        Returns:
            检查结果
        """
        # 第一层：表达式等价性检查
        expression_check = self._check_expression_equivalence(
            new_factor_expression,
            existing_factors
        )
        
        if expression_check['is_duplicate']:
            return {
                'is_novel': False,
                'reason': 'expression_equivalent',
                'layer': 1,
                'details': expression_check,
            }
        
        # 第二层：相关性检查
        correlation_check = self._check_correlation(
            new_factor_values,
            existing_factors
        )
        
        # 第三层：增量信息检查（R²）
        incremental_check = self._check_incremental_information(
            new_factor_values,
            existing_factors,
            correlation_check
        )
        
        # 综合判断
        decision = self._make_decision(
            expression_check,
            correlation_check,
            incremental_check
        )
        
        return decision
    
    def _check_expression_equivalence(
        self,
        new_expression: str,
        existing_factors: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """第一层：表达式等价性检查"""
        normalized_new = ExpressionNormalizer.normalize(new_expression)
        
        for factor in existing_factors:
            existing_expr = factor.get('expression', '')
            if ExpressionNormalizer.are_equivalent(new_expression, existing_expr):
                return {
                    'is_duplicate': True,
                    'duplicate_factor': factor.get('name'),
                    'duplicate_expression': existing_expr,
                    'normalized_new': normalized_new,
                    'normalized_existing': ExpressionNormalizer.normalize(existing_expr),
                }
        
        return {
            'is_duplicate': False,
            'normalized_expression': normalized_new,
        }
    
    def _check_correlation(
        self,
        new_values: pd.Series,
        existing_factors: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """第二层：相关性检查"""
        correlations = []
        
        for factor in existing_factors:
            existing_values = factor.get('values')
            if existing_values is None:
                continue
            
            # 计算相关系数
            corr = new_values.corr(existing_values)
            
            correlations.append({
                'factor_name': factor.get('name'),
                'correlation': abs(corr),
            })
        
        # 按相关性排序
        correlations.sort(key=lambda x: x['correlation'], reverse=True)
        
        # 找出高相关因子
        high_corr_factors = [
            c for c in correlations
            if c['correlation'] >= self.correlation_threshold
        ]
        
        moderate_corr_factors = [
            c for c in correlations
            if self.moderate_correlation_threshold <= c['correlation'] < self.correlation_threshold
        ]
        
        return {
            'max_correlation': correlations[0]['correlation'] if correlations else 0.0,
            'high_corr_count': len(high_corr_factors),
            'high_corr_factors': high_corr_factors,
            'moderate_corr_count': len(moderate_corr_factors),
            'moderate_corr_factors': moderate_corr_factors,
            'all_correlations': correlations[:10],  # 只保留前 10 个
        }
    
    def _check_incremental_information(
        self,
        new_values: pd.Series,
        existing_factors: List[Dict[str, Any]],
        correlation_check: Dict[str, Any],
    ) -> Dict[str, Any]:
        """第三层：增量信息检查（R²）"""
        # 只对高相关或中等相关的因子做 R² 检查
        candidates = (
            correlation_check['high_corr_factors'] +
            correlation_check['moderate_corr_factors']
        )
        
        if not candidates:
            return {
                'has_incremental_info': True,
                'reason': 'no_high_correlation',
            }
        
        r2_results = []
        
        for candidate in candidates:
            factor_name = candidate['factor_name']
            
            # 找到对应的因子值
            existing_values = None
            for factor in existing_factors:
                if factor.get('name') == factor_name:
                    existing_values = factor.get('values')
                    break
            
            if existing_values is None:
                continue
            
            # 用已有因子预测新因子，计算 R²
            X = existing_values.values.reshape(-1, 1)
            y = new_values.values
            
            # 去除 NaN
            mask = ~(np.isnan(X.flatten()) | np.isnan(y))
            X_clean = X[mask]
            y_clean = y[mask]
            
            if len(X_clean) < 10:
                continue
            
            model = LinearRegression()
            model.fit(X_clean, y_clean)
            r2 = model.score(X_clean, y_clean)
            
            r2_results.append({
                'factor_name': factor_name,
                'correlation': candidate['correlation'],
                'r2': r2,
            })
        
        # 按 R² 排序
        r2_results.sort(key=lambda x: x['r2'], reverse=True)
        
        # 判断是否有增量信息
        if r2_results:
            max_r2 = r2_results[0]['r2']
            
            if max_r2 >= self.r2_threshold:
                # 完全冗余
                return {
                    'has_incremental_info': False,
                    'reason': 'high_r2',
                    'max_r2': max_r2,
                    'redundant_factor': r2_results[0]['factor_name'],
                    'r2_results': r2_results,
                }
            elif max_r2 >= self.moderate_r2_threshold:
                # 部分冗余，但可能有非线性信息
                return {
                    'has_incremental_info': True,
                    'reason': 'moderate_r2_with_potential_nonlinear',
                    'max_r2': max_r2,
                    'similar_factor': r2_results[0]['factor_name'],
                    'r2_results': r2_results,
                }
            else:
                # 有增量信息
                return {
                    'has_incremental_info': True,
                    'reason': 'low_r2',
                    'max_r2': max_r2,
                    'r2_results': r2_results,
                }
        
        return {
            'has_incremental_info': True,
            'reason': 'no_r2_check',
        }
    
    def _make_decision(
        self,
        expression_check: Dict[str, Any],
        correlation_check: Dict[str, Any],
        incremental_check: Dict[str, Any],
    ) -> Dict[str, Any]:
        """综合判断"""
        # 表达式等价 → 直接拒绝
        if expression_check['is_duplicate']:
            return {
                'is_novel': False,
                'decision': 'reject',
                'reason': 'expression_equivalent',
                'layer': 1,
                'confidence': 1.0,
                'details': {
                    'expression_check': expression_check,
                }
            }
        
        # 高相关 + 高 R² → 拒绝（完全冗余）
        if (correlation_check['max_correlation'] >= self.correlation_threshold and
            not incremental_check['has_incremental_info'] and
            incremental_check.get('reason') == 'high_r2'):
            return {
                'is_novel': False,
                'decision': 'reject',
                'reason': 'high_correlation_and_high_r2',
                'layer': 3,
                'confidence': 0.95,
                'details': {
                    'correlation_check': correlation_check,
                    'incremental_check': incremental_check,
                }
            }
        
        # 中等相关 + 低 R² → 保留（有非线性信息）
        if (self.moderate_correlation_threshold <= correlation_check['max_correlation'] < self.correlation_threshold and
            incremental_check['has_incremental_info']):
            return {
                'is_novel': True,
                'decision': 'accept',
                'reason': 'moderate_correlation_with_incremental_info',
                'layer': 3,
                'confidence': 0.7,
                'details': {
                    'correlation_check': correlation_check,
                    'incremental_check': incremental_check,
                }
            }
        
        # 低相关 → 接受
        if correlation_check['max_correlation'] < self.moderate_correlation_threshold:
            return {
                'is_novel': True,
                'decision': 'accept',
                'reason': 'low_correlation',
                'layer': 2,
                'confidence': 0.9,
                'details': {
                    'correlation_check': correlation_check,
                }
            }
        
        # 其他情况 → 人工审核
        return {
            'is_novel': None,
            'decision': 'manual_review',
            'reason': 'ambiguous',
            'layer': 3,
            'confidence': 0.5,
            'details': {
                'correlation_check': correlation_check,
                'incremental_check': incremental_check,
            }
        }


def batch_check_novelty(
    new_factors: List[Dict[str, Any]],
    existing_factors: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    批量检查因子新颖性
    
    Args:
        new_factors: 新因子列表
        existing_factors: 已有因子列表
    
    Returns:
        检查结果列表
    """
    judge = NoveltyJudgeEnhanced()
    results = []
    
    for new_factor in new_factors:
        result = judge.check_novelty(
            new_factor_name=new_factor['name'],
            new_factor_expression=new_factor['expression'],
            new_factor_values=new_factor['values'],
            existing_factors=existing_factors,
        )
        
        result['factor_name'] = new_factor['name']
        results.append(result)
    
    return results

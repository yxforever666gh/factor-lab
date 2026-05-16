"""
回测验证框架

两类验证：
1. 滚动窗口验证（样本外衰减）
2. 蒙特卡洛验证（p-value）

判断标准：
- 两者都满足 → 通过
- 样本外衰减 > 50% → 过拟合，拒绝
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd


@dataclass
class RollingWindowResult:
    train_start: int
    train_end: int
    test_start: int
    test_end: int
    train_sharpe: float
    test_sharpe: float
    degradation: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ValidationDecision:
    passed: bool
    rolling_window_passed: bool
    monte_carlo_passed: bool
    sample_out_decay: float
    p_value: float
    observed_sharpe: float
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def annualized_sharpe(returns: pd.Series, periods_per_year: int = 252) -> float:
    if returns.empty:
        return 0.0
    mean = float(returns.mean())
    std = float(returns.std(ddof=1)) if len(returns) > 1 else 0.0
    if std <= 0:
        return 0.0
    return mean / std * np.sqrt(periods_per_year)


def rolling_window_validation(
    returns: pd.Series,
    train_size: int = 252,
    test_size: int = 63,
    step_size: int = 21,
) -> Dict[str, Any]:
    """滚动窗口验证样本外衰减"""
    returns = returns.dropna().reset_index(drop=True)
    windows: list[RollingWindowResult] = []

    if len(returns) < train_size + test_size:
        return {
            "window_count": 0,
            "average_train_sharpe": 0.0,
            "average_test_sharpe": 0.0,
            "sample_out_decay": 1.0,
            "passed": False,
            "windows": [],
            "reason": "样本不足",
        }

    for start in range(0, len(returns) - train_size - test_size + 1, step_size):
        train = returns.iloc[start : start + train_size]
        test = returns.iloc[start + train_size : start + train_size + test_size]
        train_sharpe = annualized_sharpe(train)
        test_sharpe = annualized_sharpe(test)
        degradation = 1.0
        if abs(train_sharpe) > 1e-9:
            degradation = max(0.0, 1 - (test_sharpe / train_sharpe))
        windows.append(
            RollingWindowResult(
                train_start=start,
                train_end=start + train_size - 1,
                test_start=start + train_size,
                test_end=start + train_size + test_size - 1,
                train_sharpe=train_sharpe,
                test_sharpe=test_sharpe,
                degradation=degradation,
            )
        )

    avg_train = float(np.mean([w.train_sharpe for w in windows])) if windows else 0.0
    avg_test = float(np.mean([w.test_sharpe for w in windows])) if windows else 0.0
    if abs(avg_train) > 1e-9:
        sample_out_decay = max(0.0, 1 - (avg_test / avg_train))
    else:
        sample_out_decay = 1.0

    return {
        "window_count": len(windows),
        "average_train_sharpe": avg_train,
        "average_test_sharpe": avg_test,
        "sample_out_decay": sample_out_decay,
        "passed": sample_out_decay < 0.30,
        "windows": [w.to_dict() for w in windows],
        "reason": "样本外衰减在可接受范围内" if sample_out_decay < 0.30 else "样本外衰减过高",
    }


def monte_carlo_validation(
    returns: pd.Series,
    n_simulations: int = 2000,
    seed: int = 42,
) -> Dict[str, Any]:
    """蒙特卡洛检验 observed sharpe 是否显著高于随机序列"""
    returns = returns.dropna().reset_index(drop=True)
    if len(returns) < 30:
        return {
            "observed_sharpe": 0.0,
            "p_value": 1.0,
            "passed": False,
            "null_mean_sharpe": 0.0,
            "null_std_sharpe": 0.0,
            "reason": "样本不足",
        }

    observed_sharpe = annualized_sharpe(returns)
    centered = returns - returns.mean()

    rng = np.random.default_rng(seed)
    null_sharpes = []
    values = centered.values
    for _ in range(n_simulations):
        simulated = pd.Series(rng.choice(values, size=len(values), replace=True))
        null_sharpes.append(annualized_sharpe(simulated))

    null_sharpes_arr = np.array(null_sharpes)
    p_value = float(np.mean(null_sharpes_arr >= observed_sharpe))

    return {
        "observed_sharpe": observed_sharpe,
        "p_value": p_value,
        "passed": p_value < 0.05,
        "null_mean_sharpe": float(null_sharpes_arr.mean()),
        "null_std_sharpe": float(null_sharpes_arr.std(ddof=1)),
        "reason": "显著优于随机序列" if p_value < 0.05 else "未显著优于随机序列",
    }


def validate_factor_backtest(
    returns: pd.Series,
    train_size: int = 252,
    test_size: int = 63,
    step_size: int = 21,
    n_simulations: int = 2000,
) -> Dict[str, Any]:
    """完整回测验证"""
    rolling = rolling_window_validation(returns, train_size=train_size, test_size=test_size, step_size=step_size)
    monte_carlo = monte_carlo_validation(returns, n_simulations=n_simulations)

    sample_out_decay = float(rolling["sample_out_decay"])
    p_value = float(monte_carlo["p_value"])
    observed_sharpe = float(monte_carlo["observed_sharpe"])

    rolling_passed = bool(rolling["passed"])
    monte_passed = bool(monte_carlo["passed"])
    passed = rolling_passed and monte_passed

    if passed:
        reason = "滚动窗口与蒙特卡洛验证均通过"
    elif sample_out_decay > 0.50:
        reason = "样本外衰减超过 50%，高度疑似过拟合"
    elif not monte_passed:
        reason = "蒙特卡洛检验未通过，显著性不足"
    else:
        reason = "滚动窗口验证未通过"

    decision = ValidationDecision(
        passed=passed,
        rolling_window_passed=rolling_passed,
        monte_carlo_passed=monte_passed,
        sample_out_decay=sample_out_decay,
        p_value=p_value,
        observed_sharpe=observed_sharpe,
        reason=reason,
    )

    return {
        "decision": decision.to_dict(),
        "rolling_window": rolling,
        "monte_carlo": monte_carlo,
    }


def create_validation_report(
    factor_name: str,
    validation_result: Dict[str, Any],
) -> Dict[str, Any]:
    decision = validation_result["decision"]
    rolling = validation_result["rolling_window"]
    monte = validation_result["monte_carlo"]
    return {
        "factor_name": factor_name,
        "passed": decision["passed"],
        "observed_sharpe": decision["observed_sharpe"],
        "sample_out_decay": decision["sample_out_decay"],
        "p_value": decision["p_value"],
        "rolling_window_passed": decision["rolling_window_passed"],
        "monte_carlo_passed": decision["monte_carlo_passed"],
        "window_count": rolling["window_count"],
        "average_train_sharpe": rolling["average_train_sharpe"],
        "average_test_sharpe": rolling["average_test_sharpe"],
        "null_mean_sharpe": monte["null_mean_sharpe"],
        "null_std_sharpe": monte["null_std_sharpe"],
        "reason": decision["reason"],
    }


def batch_validate_backtests(
    factors: Dict[str, pd.Series],
    train_size: int = 252,
    test_size: int = 63,
    step_size: int = 21,
    n_simulations: int = 1000,
) -> pd.DataFrame:
    rows = []
    for factor_name, returns in factors.items():
        validation = validate_factor_backtest(
            returns,
            train_size=train_size,
            test_size=test_size,
            step_size=step_size,
            n_simulations=n_simulations,
        )
        rows.append(create_validation_report(factor_name, validation))
    return pd.DataFrame(rows).sort_values(["passed", "observed_sharpe"], ascending=[False, False])

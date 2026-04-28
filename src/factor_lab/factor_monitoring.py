"""
因子监控体系

实时监控因子健康度：
1. IC 衰退
2. 换手率异常
3. 最大回撤超标
4. 相关性结构变化

自动行动：
- 2 个高严重性告警 → 暂停因子
- IC 衰退 → 减半权重
- 其他告警 → 持续监控
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd


@dataclass
class MonitoringThresholds:
    ic_decay_ratio: float = 0.5  # 当前 IC < 基准 50%
    turnover_spike_ratio: float = 1.5  # 当前换手 > 基准 150%
    max_drawdown_spike_ratio: float = 1.5  # 当前回撤 > 基准 150%
    corr_change_threshold: float = 0.3  # 相关性结构变化 > 0.3
    high_severity_alerts_to_pause: int = 2


@dataclass
class MonitoringAlert:
    factor_name: str
    metric: str
    severity: str  # low / medium / high
    current_value: float
    baseline_value: float
    threshold_value: float
    message: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class MonitoringResult:
    factor_name: str
    health_score: float
    status: str  # healthy / warning / critical / paused
    recommended_action: str  # continue / halve_weight / monitor / pause
    high_severity_count: int
    alerts: List[MonitoringAlert]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["alerts"] = [alert.to_dict() for alert in self.alerts]
        return payload


def calculate_max_drawdown(returns: pd.Series) -> float:
    """计算最大回撤（正数表示回撤幅度）"""
    if returns.empty:
        return 0.0
    nav = (1 + returns.fillna(0)).cumprod()
    peak = nav.cummax()
    drawdown = nav / peak - 1
    return abs(float(drawdown.min()))


def calculate_correlation_structure_change(
    current_corr: pd.DataFrame,
    baseline_corr: pd.DataFrame,
) -> float:
    """计算相关性结构变化（平均绝对变化）"""
    common_index = current_corr.index.intersection(baseline_corr.index)
    common_cols = current_corr.columns.intersection(baseline_corr.columns)
    if len(common_index) == 0 or len(common_cols) == 0:
        return 0.0
    current_aligned = current_corr.loc[common_index, common_cols]
    baseline_aligned = baseline_corr.loc[common_index, common_cols]
    diff = (current_aligned - baseline_aligned).abs()
    return float(np.nanmean(diff.values))


class FactorMonitor:
    def __init__(self, thresholds: Optional[MonitoringThresholds] = None):
        self.thresholds = thresholds or MonitoringThresholds()

    def monitor_factor_health(
        self,
        factor_name: str,
        current_metrics: Dict[str, Any],
        baseline_metrics: Dict[str, Any],
    ) -> MonitoringResult:
        alerts: list[MonitoringAlert] = []

        # 1. IC 衰退
        current_ic = float(current_metrics.get("ic", 0.0) or 0.0)
        baseline_ic = float(baseline_metrics.get("ic", 0.0) or 0.0)
        if baseline_ic > 0 and current_ic < baseline_ic * self.thresholds.ic_decay_ratio:
            alerts.append(
                MonitoringAlert(
                    factor_name=factor_name,
                    metric="ic_decay",
                    severity="high",
                    current_value=current_ic,
                    baseline_value=baseline_ic,
                    threshold_value=baseline_ic * self.thresholds.ic_decay_ratio,
                    message=f"IC 衰退：当前 {current_ic:.4f} < 基准 {baseline_ic:.4f} 的 {self.thresholds.ic_decay_ratio:.0%}",
                )
            )

        # 2. 换手率异常
        current_turnover = float(current_metrics.get("turnover", 0.0) or 0.0)
        baseline_turnover = float(baseline_metrics.get("turnover", 0.0) or 0.0)
        if baseline_turnover > 0 and current_turnover > baseline_turnover * self.thresholds.turnover_spike_ratio:
            severity = "medium" if current_turnover <= baseline_turnover * 2 else "high"
            alerts.append(
                MonitoringAlert(
                    factor_name=factor_name,
                    metric="turnover_spike",
                    severity=severity,
                    current_value=current_turnover,
                    baseline_value=baseline_turnover,
                    threshold_value=baseline_turnover * self.thresholds.turnover_spike_ratio,
                    message=f"换手率异常：当前 {current_turnover:.2f} > 基准 {baseline_turnover:.2f} 的 {self.thresholds.turnover_spike_ratio:.1f} 倍",
                )
            )

        # 3. 最大回撤超标
        current_returns = current_metrics.get("returns")
        baseline_max_drawdown = float(baseline_metrics.get("max_drawdown", 0.0) or 0.0)
        current_max_drawdown = (
            calculate_max_drawdown(current_returns)
            if isinstance(current_returns, pd.Series)
            else float(current_metrics.get("max_drawdown", 0.0) or 0.0)
        )
        if baseline_max_drawdown > 0 and current_max_drawdown > baseline_max_drawdown * self.thresholds.max_drawdown_spike_ratio:
            severity = "medium" if current_max_drawdown <= baseline_max_drawdown * 2 else "high"
            alerts.append(
                MonitoringAlert(
                    factor_name=factor_name,
                    metric="max_drawdown_spike",
                    severity=severity,
                    current_value=current_max_drawdown,
                    baseline_value=baseline_max_drawdown,
                    threshold_value=baseline_max_drawdown * self.thresholds.max_drawdown_spike_ratio,
                    message=f"最大回撤超标：当前 {current_max_drawdown:.2%} > 基准 {baseline_max_drawdown:.2%} 的 {self.thresholds.max_drawdown_spike_ratio:.1f} 倍",
                )
            )

        # 4. 相关性结构变化
        current_corr = current_metrics.get("correlation_matrix")
        baseline_corr = baseline_metrics.get("correlation_matrix")
        corr_change = 0.0
        if isinstance(current_corr, pd.DataFrame) and isinstance(baseline_corr, pd.DataFrame):
            corr_change = calculate_correlation_structure_change(current_corr, baseline_corr)
            if corr_change > self.thresholds.corr_change_threshold:
                severity = "medium" if corr_change <= self.thresholds.corr_change_threshold * 1.5 else "high"
                alerts.append(
                    MonitoringAlert(
                        factor_name=factor_name,
                        metric="correlation_structure_change",
                        severity=severity,
                        current_value=corr_change,
                        baseline_value=0.0,
                        threshold_value=self.thresholds.corr_change_threshold,
                        message=f"相关性结构变化：平均变化 {corr_change:.3f} > 阈值 {self.thresholds.corr_change_threshold:.3f}",
                    )
                )

        high_count = sum(1 for alert in alerts if alert.severity == "high")
        medium_count = sum(1 for alert in alerts if alert.severity == "medium")

        if high_count >= self.thresholds.high_severity_alerts_to_pause:
            recommended_action = "pause"
            status = "paused"
        elif any(alert.metric == "ic_decay" and alert.severity == "high" for alert in alerts):
            recommended_action = "halve_weight"
            status = "critical"
        elif alerts:
            recommended_action = "monitor"
            status = "warning"
        else:
            recommended_action = "continue"
            status = "healthy"

        health_score = max(0.0, 100.0 - high_count * 35.0 - medium_count * 15.0 - (len(alerts) - high_count - medium_count) * 5.0)

        return MonitoringResult(
            factor_name=factor_name,
            health_score=round(health_score, 2),
            status=status,
            recommended_action=recommended_action,
            high_severity_count=high_count,
            alerts=alerts,
        )


def monitor_factor_batch(
    current_metrics_map: Dict[str, Dict[str, Any]],
    baseline_metrics_map: Dict[str, Dict[str, Any]],
    thresholds: Optional[MonitoringThresholds] = None,
) -> pd.DataFrame:
    """批量监控多个因子"""
    monitor = FactorMonitor(thresholds)
    rows = []
    factor_names = sorted(set(current_metrics_map.keys()) | set(baseline_metrics_map.keys()))
    for factor_name in factor_names:
        result = monitor.monitor_factor_health(
            factor_name,
            current_metrics_map.get(factor_name, {}),
            baseline_metrics_map.get(factor_name, {}),
        )
        rows.append(
            {
                "factor_name": result.factor_name,
                "health_score": result.health_score,
                "status": result.status,
                "recommended_action": result.recommended_action,
                "high_severity_count": result.high_severity_count,
                "alert_count": len(result.alerts),
            }
        )
    return pd.DataFrame(rows).sort_values(["health_score", "alert_count"], ascending=[True, False])


def create_monitoring_report(result: MonitoringResult) -> Dict[str, Any]:
    """创建用户可读监控报告"""
    return {
        "factor_name": result.factor_name,
        "health_score": result.health_score,
        "status": result.status,
        "recommended_action": result.recommended_action,
        "high_severity_count": result.high_severity_count,
        "alert_count": len(result.alerts),
        "alerts": [alert.to_dict() for alert in result.alerts],
        "summary": _build_monitoring_summary(result),
    }


def _build_monitoring_summary(result: MonitoringResult) -> str:
    if not result.alerts:
        return "健康状态正常，无需干预"
    alert_metrics = "、".join(alert.metric for alert in result.alerts)
    if result.recommended_action == "pause":
        return f"触发多项高严重性告警（{alert_metrics}），建议暂停因子"
    if result.recommended_action == "halve_weight":
        return f"IC 明显衰退（{alert_metrics}），建议减半权重"
    return f"发现异常（{alert_metrics}），建议持续监控"

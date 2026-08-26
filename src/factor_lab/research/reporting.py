"""Human-readable reporting for the lightweight research loop."""

from __future__ import annotations

from typing import Any, Mapping


def _number(value: Any, *, percent: bool = False) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "—"
    return f"{numeric * 100:.2f}%" if percent else f"{numeric:.3f}"


def render_report(summary: Mapping[str, Any]) -> str:
    """Render a compact Markdown report from a completed run summary."""

    data = summary.get("data") or {}
    lines = [
        "# Factor Lab 轻量研究报告",
        "",
        f"- Run: `{summary.get('run_id')}`",
        f"- Suite: `{summary.get('suite')}` / mode: `{summary.get('mode')}`",
        "- Evidence: `historical_diagnostic`（不会创建候选、影子账户或实盘动作）",
        f"- 样本：{data.get('start_date')} 至 {data.get('end_date')}，"
        f"{int(data.get('row_count') or 0):,} 行 / {int(data.get('ticker_count') or 0):,} 只股票",
        "",
        "## Stage A：方向与覆盖",
        "",
        "| 因子 | 训练方向 | 验证 signed IC | 验证覆盖 | Stage B | 阻断原因 |",
        "| --- | ---: | ---: | ---: | --- | --- |",
    ]
    for row in summary.get("stage_a") or []:
        validation = row.get("validation") or {}
        lines.append(
            "| {name} | {direction:+d} | {ic} | {coverage} | {eligible} | {blockers} |".format(
                name=row.get("factor_name"),
                direction=int(row.get("frozen_direction") or 1),
                ic=_number(validation.get("signed_rank_ic_mean")),
                coverage=_number(validation.get("median_cross_section_coverage"), percent=True),
                eligible="是" if row.get("stage_b_eligible") else "否",
                blockers=", ".join(row.get("blockers") or []) or "—",
            )
        )

    lines.extend(
        [
            "",
            "## Stage B：真实多头执行",
            "",
            "| 因子 | 验证净超额年化 | 验证 Sharpe | 验证 IR | 最大回撤 | 半年胜率 | 硬门槛 | 优于控制 |",
            "| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |",
        ]
    )
    for row in summary.get("stage_b") or []:
        metrics = (row.get("windows") or {}).get("validation") or {}
        lines.append(
            "| {name} | {excess} | {sharpe} | {ir} | {drawdown} | {half} | {gate} | {better} |".format(
                name=row.get("factor_name"),
                excess=_number(metrics.get("net_excess_annual_return"), percent=True),
                sharpe=_number(metrics.get("net_sharpe")),
                ir=_number(metrics.get("information_ratio")),
                drawdown=_number(metrics.get("max_drawdown"), percent=True),
                half=_number(metrics.get("positive_half_year_ratio"), percent=True),
                gate="通过" if row.get("gate_passed") else "失败",
                better="是" if row.get("beats_control") else "否",
            )
        )

    validated = list(summary.get("validated_factors") or [])
    lines.extend(
        [
            "",
            "## 结论",
            "",
            f"- 通过且优于控制的因子：{', '.join(validated) if validated else '0 个'}。",
        ]
    )
    if summary.get("robustness"):
        robustness = summary["robustness"]
        lines.append(
            f"- 因未产生合格改进，已完成有限稳健性矩阵：{len(robustness.get('results') or [])} 个组合；本轮停止继续搜索。"
        )
    lines.extend(
        [
            "- 历史回测不能证明未来盈利；0 个 validated 是允许且诚实的结果。",
            "",
        ]
    )
    return "\n".join(lines)


__all__ = ["render_report"]

"""Human-readable reporting for the lightweight research loop."""

from __future__ import annotations

from typing import Any, Mapping


def _number(value: Any, *, percent: bool = False) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "—"
    return f"{numeric * 100:.2f}%" if percent else f"{numeric:.3f}"


def _range_number(stats: Mapping[str, Any], *, percent: bool = False) -> str:
    """Format fixed-anchor min/median/max without implying a best anchor."""

    if not stats:
        return "—"
    return " / ".join(
        _number(stats.get(key), percent=percent) for key in ("min", "median", "max")
    )


def render_report(summary: Mapping[str, Any]) -> str:
    """Render a compact Markdown report from a completed run summary."""

    data = summary.get("data") or {}
    research_filter = data.get("research_universe_filter") or {}
    lines = [
        "# Factor Lab 轻量研究报告",
        "",
        f"- Run: `{summary.get('run_id')}`",
        f"- Suite: `{summary.get('suite')}` / mode: `{summary.get('mode')}`",
        "- Evidence: `historical_diagnostic`（不会创建候选、影子账户或实盘动作）",
        f"- 样本：{data.get('start_date')} 至 {data.get('end_date')}，"
        f"{int(data.get('row_count') or 0):,} 行 / {int(data.get('ticker_count') or 0):,} 只股票",
        f"- 数据哈希：features `{data.get('feature_sha256')}`；execution `{data.get('execution_sha256')}`",
        f"- 数据提示：`{data.get('warning') or 'none'}`",
        f"- 执行价格尾部：至 {data.get('execution_end_date')}，"
        f"{int(data.get('execution_tail_date_count') or 0)} 个交易日 / "
        f"{int(data.get('execution_tail_row_count') or 0):,} 行，仅用于最后信号退出估值。",
        "- 研究样本过滤：eligible/universe_member 纳入 "
        f"{int(research_filter.get('included_row_count') or data.get('row_count') or 0):,} 行，"
        f"排除 {int(research_filter.get('excluded_row_count') or 0):,} 行。",
        "- ST/名称状态（全量）："
        + ", ".join(
            f"`{key}`={int(value):,}"
            for key, value in sorted(
                (research_filter.get("st_filter_status_counts") or {}).items()
            )
        ),
        "",
        "## Stage A：训练段筛选",
        "",
        "shortlist 只使用 2017–2022 训练段；验证和审计不参与排序。",
        "",
        "| 因子 | 方向 | 训练 signed IC | Top-tail 超额 | Decile 单调性 | 年度稳定 | Bootstrap tail 95% CI | Stage B | 原因 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |",
    ]
    selected_names = set(summary.get("stage_b_selected") or [])
    selection = summary.get("stage_a_selection") or {}
    decisions = {
        row.get("factor_name"): row for row in selection.get("decisions") or []
    }
    for row in summary.get("stage_a") or []:
        train = row.get("train") or {}
        eligible = bool(row.get("stage_b_eligible"))
        selected = row.get("factor_name") in selected_names
        blockers = list(row.get("blockers") or [])
        decision = decisions.get(row.get("factor_name")) or {}
        if eligible and not selected and decision.get("reason"):
            reason = str(decision["reason"])
            if decision.get("correlated_with"):
                reason += f":{decision['correlated_with']}"
            blockers.append(reason)
        tail_interval = (train.get("bootstrap") or {}).get("top_tail_excess") or {}
        interval = (
            f"[{_number(tail_interval.get('lower'), percent=True)}, "
            f"{_number(tail_interval.get('upper'), percent=True)}]"
        )
        lines.append(
            "| {name} | {direction:+d} | {ic} | {tail} | {decile} | {years} | {interval} | {selected} | {blockers} |".format(
                name=row.get("factor_name"),
                direction=int(row.get("frozen_direction") or 1),
                ic=_number(train.get("signed_rank_ic_mean")),
                tail=_number(train.get("top_tail_excess_mean"), percent=True),
                decile=_number(train.get("decile_monotonicity_mean")),
                years=_number(train.get("positive_year_ratio"), percent=True),
                interval=interval,
                selected="是" if selected else "否",
                blockers=", ".join(blockers) or "—",
            )
        )

    homogeneous = [
        row
        for row in selection.get("similarities") or []
        if row.get("homogeneous")
    ]
    if homogeneous:
        lines.extend(["", "### 训练段同质信号", ""])
        for row in homogeneous:
            lines.append(
                f"- `{row.get('left')}` / `{row.get('right')}`：平均日截面相关 "
                f"{_number(row.get('mean_rank_correlation'))}，只保留训练排序代表。"
            )

    lines.extend(
        [
            "",
            "## Stage B：真实多头执行",
            "",
            "主动均值区间使用有时间顺序的循环区块 Bootstrap；下界不为正时不能晋级。",
            "",
            "| 因子 | 验证净超额年化 | 主动均值 95% CI | 相对控制均值 CI | Sharpe | IR | 最大回撤 | 基准覆盖 | 执行 PIT | 年化换手 | 审计 IR | 审计状态 | 硬门槛 | 优于控制 | 原因/诊断 |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | --- |",
        ]
    )
    for row in summary.get("stage_b") or []:
        metrics = (row.get("windows") or {}).get("validation") or {}
        audit = (row.get("windows") or {}).get("audit") or {}
        blockers = list(row.get("gate_blockers") or [])
        blockers.extend(row.get("audit_falsification_reasons") or [])
        comparison = row.get("control_comparison") or {}
        if row.get("factor_name") != summary.get("control_factor"):
            blockers.extend(comparison.get("blockers") or [])
        active_interval = metrics.get("excess_return_mean_bootstrap") or {}
        interval = (
            f"[{_number(active_interval.get('lower'), percent=True)}, "
            f"{_number(active_interval.get('upper'), percent=True)}]"
        )
        control_interval = (comparison.get("bootstrap") or {})
        comparison_text = (
            f"[{_number(control_interval.get('lower'), percent=True)}, "
            f"{_number(control_interval.get('upper'), percent=True)}]"
            if comparison
            else "—"
        )
        audit_status = {
            "falsified": "已否证",
            "not_falsified": "未否证",
            "insufficient_evidence": "证据不足",
        }.get(str(row.get("audit_status") or ""), "证据不足")
        lines.append(
            "| {name} | {excess} | {interval} | {comparison} | {sharpe} | {ir} | {drawdown} | {coverage} | {execution_pit} | {turnover} | {audit_ir} | {audit_status} | {gate} | {better} | {blockers} |".format(
                name=row.get("factor_name"),
                excess=_number(metrics.get("net_excess_annual_return"), percent=True),
                interval=interval,
                comparison=comparison_text,
                sharpe=_number(metrics.get("net_sharpe")),
                ir=_number(metrics.get("information_ratio")),
                drawdown=_number(metrics.get("max_drawdown"), percent=True),
                coverage=_number(metrics.get("benchmark_return_coverage"), percent=True),
                execution_pit=_number(metrics.get("execution_input_coverage"), percent=True),
                turnover=_number(metrics.get("annualized_turnover"), percent=True),
                audit_ir=_number(audit.get("information_ratio")),
                audit_status=audit_status,
                gate="通过" if row.get("gate_passed") else "失败",
                better="—"
                if row.get("factor_name") == summary.get("control_factor")
                else "是"
                if row.get("beats_control")
                else "否",
                blockers=", ".join(dict.fromkeys(blockers)) or "—",
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
            f"- 因未产生合格改进，已完成有限稳健性矩阵：{len(robustness.get('results') or [])} 个组合；"
            "矩阵仅为探索诊断，不会晋级，本轮停止继续搜索。"
        )
        lines.extend(
            [
                "",
                "## 有限稳健性矩阵",
                "",
                "20 日配置表示每 20 个交易日调仓并持有至下一调仓点；固定使用配置中的 "
                "`[0, 5, 10, 15]` 锚点，不选择最佳锚点。",
                "每个指标依次报告锚点间 min / median / max。只有验证锚点通过率至少达到配置阈值，"
                "且验证中位指标通过同一硬门槛，才记为 robust。",
                "",
                "| 因子 | 持仓数 | 调仓/持有日 | Split | 锚点 | 净超额年化 min/med/max | Sharpe min/med/max | IR min/med/max | 最大回撤 min/med/max | 锚点通过率 | 中位门槛 | Robust | 拒绝原因 |",
                "| --- | ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |",
            ]
        )
        for row in robustness.get("results") or []:
            for split, split_label in (
                ("train", "训练"),
                ("validation", "验证"),
                ("audit", "审计"),
            ):
                statistics = (row.get("window_statistics") or {}).get(split) or {}
                validation_row = split == "validation"
                lines.append(
                    "| {name} | {positions} | {days} | {split} | {anchors} | {excess} | {sharpe} | {ir} | {drawdown} | {pass_ratio} | {gate} | {robust} | {blockers} |".format(
                        name=row.get("factor_name"),
                        positions=row.get("position_count"),
                        days=row.get("rebalance_every_days"),
                        split=split_label,
                        anchors=", ".join(str(value) for value in row.get("anchor_offsets") or []),
                        excess=_range_number(
                            statistics.get("net_excess_annual_return") or {}, percent=True
                        ),
                        sharpe=_range_number(statistics.get("net_sharpe") or {}),
                        ir=_range_number(statistics.get("information_ratio") or {}),
                        drawdown=_range_number(
                            statistics.get("max_drawdown") or {}, percent=True
                        ),
                        pass_ratio=_number(row.get("anchor_pass_ratio"), percent=True)
                        if validation_row
                        else "—",
                        gate=("通过" if row.get("median_gate_passed") else "失败")
                        if validation_row
                        else "—",
                        robust=("是" if row.get("robust") else "否")
                        if validation_row
                        else "—",
                        blockers=(
                            ", ".join(row.get("robustness_blockers") or []) or "—"
                        )
                        if validation_row
                        else "—",
                    )
                )
    elif summary.get("search_stopped"):
        lines.append("- 本轮预注册研究已停止：不追加变体，也不降低晋级门槛。")
    lines.append(f"- 搜索终态：`{summary.get('search_status') or 'unknown'}`。")
    lines.extend(
        [
            "- 审计段只允许否证，不参与 shortlist、控制比较或参数选择。",
            f"- 历史回测不能证明未来盈利；{len(validated)} 个 validated，0 个也是允许且诚实的结果。",
            "",
        ]
    )
    return "\n".join(lines)


__all__ = ["render_report"]

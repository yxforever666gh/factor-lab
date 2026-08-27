"""Human-readable reporting for the lightweight research loop."""

from __future__ import annotations

import json
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


def _walk_forward_lines(
    summary: Mapping[str, Any],
    walk_forward: Mapping[str, Any],
    *,
    ranking_available: bool,
) -> list[str]:
    """Render the causal protocol separately from legacy validation reports."""

    selector = walk_forward.get("selector") or {}
    registry = list(walk_forward.get("candidate_registry") or [])
    stage_a = {
        str(row.get("factor_name")): row for row in summary.get("stage_a") or []
    }
    offsets = list(walk_forward.get("offsets") or [])
    configured_offsets = list(walk_forward.get("rebalance_offsets") or [])
    dynamic_factor = str(
        walk_forward.get("dynamic_factor") or "causal_walk_forward_dynamic"
    )
    fixed_comparator = dict(walk_forward.get("fixed_comparator") or {})
    fixed_comparator_name = str(
        fixed_comparator.get("factor_name") or "fixed_registry_equal_weight"
    )
    common_start = walk_forward.get("common_evaluation_start")
    future_violations = int(
        walk_forward.get("future_selection_violation_count") or 0
    )
    lines = [
        "",
        "## Walk-forward selector 内部因果协议",
        "",
        "本节属于 `post_selection_causal_simulation`：selector 的每次决策是因果的，"
        "但仍是候选注册完成后的历史模拟，不是独立 OOS，也不是未来收益证明。",
        "",
        f"- 因果 cutoff：`{selector.get('history_policy') or 'end_date_strictly_before_signal_date'}`；"
        "只有 `end_date < signal_date` 的已完成周期可进入评分，等于信号日也禁止使用。",
        f"- 固定 selector：回看 {int(selector.get('lookback_trading_days') or 0)} 个交易日，"
        f"至少 {int(selector.get('minimum_completed_periods') or 0)} 个共同完成周期，"
        f"至少间隔 {int(selector.get('update_every_trading_days') or 0)} 个交易日，"
        "并在该 offset 的下一个调仓信号日更新；"
        f"评分 `{selector.get('score_method') or '—'}`，control guard "
        f"{_number(selector.get('control_score_guard'))}；最多选择 "
        f"{int(selector.get('selection_count') or 1)} 个候选，"
        f"按 `{selector.get('selection_weighting') or 'equal'}` 权重合成。",
        "- control 始终属于合格集合；Top-K 在 control 与越过 guard 的 challenger 中排序，"
        "没有 challenger 越过 guard 时只使用 control。",
        f"- 缺失信号策略：`{selector.get('missing_signal_policy') or 'fallback_control'}`。",
        f"- 共同评价起点：`{common_start or '未建立（canary / 暖机不足）'}`。",
        f"- Future selection violations：**{future_violations}**；因果历史状态："
        f"`{'valid' if walk_forward.get('causal_history_valid') else 'not_ready_or_invalid'}`。",
        f"- 预注册 offsets：`{', '.join(str(value) for value in configured_offsets) or '—'}`。"
        "严禁选择“最佳 offset”或只报告最有利锚点。",
        "- 各 offset 的持有期高度重叠、收益相关，彼此不是独立实验；phase Q20 是相关"
        "路径的下尾敏感性汇总，不能解释为多次独立确认。",
        f"- 固定无择时基准：`{fixed_comparator_name}`；对同一 candidate registry"
        " 始终等权、候选缺失逐股票回退 control，不读取已实现收益。",
        "",
        "### 固定方向候选注册表",
        "",
        "候选及方向在 sweep 前固定；训练、验证、审计或全历史成绩均不得反转方向、"
        "追加候选或删除落后候选。",
        "",
        "| 注册顺序 | 候选 | 固定方向 | 方向依据 |",
        "| ---: | --- | ---: | --- |",
    ]
    for index, name in enumerate(registry, start=1):
        row = stage_a.get(str(name)) or {}
        direction = int(row.get("frozen_direction") or 1)
        basis = str(row.get("selection_basis") or "fixed_by_registry")
        lines.append(f"| {index} | `{name}` | {direction:+d} | `{basis}` |")

    if ranking_available:
        phase_quantile = float(walk_forward.get("phase_quantile") or 0.20)
        lines.extend(
            [
                "",
                f"## Phase Q{int(round(phase_quantile * 100))} 排名",
                "",
                "排名使用共同评价起点之后、跨全部预注册 offset 的分布下分位；"
                "它比较策略，不选择 offset。综合分沿用收益、Sharpe、IR、回撤的固定权重。",
                "",
                "| 排名 | 策略 | 类型 | 资格 | Phase 分 | 年化收益 Q20 | Sharpe Q20 | IR Q20 | 最大回撤 Q20 | 年化收益差 Q20 | 正收益差 offset 比例 |",
                "| ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for row in walk_forward.get("phase_rankings") or []:
            metrics = row.get("phase_metrics") or {}
            deltas = row.get("phase_deltas_vs_control") or {}
            lines.append(
                "| {rank} | `{name}` | {kind} | {eligibility} | {score} | {annual} | {sharpe} | {ir} | {drawdown} | {delta} | {positive} |".format(
                    rank=row.get("rank") or "—",
                    name=row.get("strategy_name"),
                    kind=row.get("strategy_kind"),
                    eligibility=(
                        "排除"
                        if row.get("excluded_from_phase_ranking")
                        else "合格"
                    ),
                    score=_number(row.get("phase_score")),
                    annual=_number(
                        (metrics.get("net_annual_return") or {}).get("q20"),
                        percent=True,
                    ),
                    sharpe=_number((metrics.get("net_sharpe") or {}).get("q20")),
                    ir=_number((metrics.get("information_ratio") or {}).get("q20")),
                    drawdown=_number(
                        (metrics.get("max_drawdown") or {}).get("q20"),
                        percent=True,
                    ),
                    delta=_number(
                        (deltas.get("net_annual_return") or {}).get("q20"),
                        percent=True,
                    ),
                    positive=_number(
                        row.get("positive_annual_return_delta_ratio"), percent=True
                    ),
                )
            )
        fixed_deltas = fixed_comparator.get("dynamic_phase_deltas") or {}
        lines.extend(
            [
                "",
                "### 动态择时相对固定等权基准",
                "",
                f"- 年化收益差 Q20：{_number((fixed_deltas.get('net_annual_return') or {}).get('q20'), percent=True)}；"
                f"Sharpe 差 Q20：{_number((fixed_deltas.get('net_sharpe') or {}).get('q20'))}；"
                f"IR 差 Q20：{_number((fixed_deltas.get('information_ratio') or {}).get('q20'))}；"
                f"最大回撤差 Q20：{_number((fixed_deltas.get('max_drawdown') or {}).get('q20'), percent=True)}。",
                "- 动态年化高于固定等权的 offset 比例："
                f"{_number(fixed_comparator.get('dynamic_positive_annual_return_delta_ratio'), percent=True)}。",
                "这组 paired delta 才是 selector 择时增量的首要历史诊断；"
                "相对 control 的改善不能替代它。",
            ]
        )
        excluded = [
            row
            for row in walk_forward.get("phase_rankings") or []
            if row.get("excluded_from_phase_ranking")
        ]
        if excluded:
            lines.extend(["", "### Phase 排名排除审计", ""])
            for row in excluded:
                reasons = row.get("phase_ranking_exclusion_reasons") or []
                lines.append(
                    f"- `{row.get('strategy_name')}`："
                    f"`{json.dumps(reasons, ensure_ascii=False, sort_keys=True)}`"
                )
    else:
        lines.extend(
            [
                "",
                "## Walk-forward canary",
                "",
                "Canary 只检查固定方向、selector 配置序列化、静态信号和执行链；"
                "它不执行 selector 或审计因果 cutoff，不建立共同 phase 排名，"
                "不选择策略，更不能选择最佳 offset。",
            ]
        )

    if offsets:
        lines.extend(
            [
                "",
                "## Dynamic per-offset 回放",
                "",
                "每行是同一预注册 selector 在一个 offset 上的 experimental account；"
                "所有指标均从共同评价起点开始。",
                "",
                "| Offset | Ready date | 信号数 | 更新数 | 切换数 | Future violations | 年化收益 | Sharpe | IR | 最大回撤 | 周期覆盖 |",
                "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        aggregate_counts: dict[str, int] = {}
        total_signal_dates = 0
        for row in offsets:
            metrics = (row.get("metrics") or {}).get(dynamic_factor) or {}
            lines.append(
                "| {offset} | {ready} | {signals} | {updates} | {switches} | {violations} | {annual} | {sharpe} | {ir} | {drawdown} | {coverage} |".format(
                    offset=row.get("rebalance_offset_days"),
                    ready=row.get("ready_date") or "—",
                    signals=int(row.get("signal_date_count") or 0),
                    updates=int(row.get("update_count") or 0),
                    switches=int(row.get("switch_count") or 0),
                    violations=int(row.get("future_selection_violation_count") or 0),
                    annual=_number(metrics.get("net_annual_return"), percent=True),
                    sharpe=_number(metrics.get("net_sharpe")),
                    ir=_number(metrics.get("information_ratio")),
                    drawdown=_number(metrics.get("max_drawdown"), percent=True),
                    coverage=_number(metrics.get("period_coverage"), percent=True),
                )
            )
            total_signal_dates += int(row.get("signal_date_count") or 0)
            for name, count in (row.get("selection_counts") or {}).items():
                aggregate_counts[str(name)] = aggregate_counts.get(str(name), 0) + int(
                    count
                )
        selection_frequency = dict(walk_forward.get("selection_frequency") or {})
        lines.extend(
            [
                "",
                "### 跨 offset 选择参与率与平均部署权重",
                "",
                "参与率以全部信号日为分母，Top-K 时各候选参与率之和可以超过 100%；"
                "平均部署权重之和应为 100%。暖机或缺失信号的 fallback_control 也包含在内。",
                "这些数字只描述 selector 的历史选择，不构成候选优胜次数或独立显著性证据。",
                "",
                "| 候选 | 入选信号日数 | 入选日比例 | 平均部署权重 |",
                "| --- | ---: | ---: | ---: |",
            ]
        )
        for name in registry:
            frequency = dict(selection_frequency.get(str(name)) or {})
            count = int(
                frequency.get("selected_date_count")
                if frequency.get("selected_date_count") is not None
                else aggregate_counts.get(str(name), 0)
            )
            selected_date_ratio = frequency.get("selected_date_ratio")
            if selected_date_ratio is None:
                selected_date_ratio = (
                    count / total_signal_dates if total_signal_dates else None
                )
            lines.append(
                f"| `{name}` | {count} | "
                f"{_number(selected_date_ratio, percent=True)} | "
                f"{_number(frequency.get('mean_deployed_weight'), percent=True)} |"
            )
    return lines


def render_report(summary: Mapping[str, Any]) -> str:
    """Render a compact Markdown report from a completed run summary."""

    data = summary.get("data") or {}
    research_filter = data.get("research_universe_filter") or {}
    results_first = summary.get("results_first") or {}
    results_first_enabled = bool(results_first.get("enabled"))
    results_first_ranking = bool(results_first.get("ranking_available"))
    walk_forward = summary.get("walk_forward") or {}
    walk_forward_enabled = bool(
        walk_forward.get("enabled") or summary.get("suite") == "walk-forward"
    )
    walk_forward_ranking = bool(
        walk_forward_enabled and walk_forward.get("ranking_available")
    )
    objective = (
        "- Objective: selector-internal causal walk-forward；每次选择只读取严格成熟历史，"
        "结果属于 `post_selection_causal_simulation`，不声称独立 OOS。"
        if walk_forward_ranking
        else "- Objective: walk-forward canary，只检查固定方向、selector 配置和静态执行链；"
        "不执行 selector，也不产生 phase 排名。"
        if walk_forward_enabled
        else "- Objective: `results_first`，使用全部已观察历史优化并排名，不声称独立 OOS。"
        if results_first_ranking
        else "- Objective: `results_first` canary，仅检查最近窗口执行链，不产生历史冠军。"
        if results_first_enabled
        else "- Objective: train/validation/audit 两阶段研究协议。"
    )
    evidence = (
        "post_selection_causal_simulation"
        if walk_forward_ranking
        else "engineering_smoke"
        if walk_forward_enabled
        else "historical_diagnostic"
    )
    evidence_scope = (
        "仅创建历史 shadow/deployed-account 模拟，不触发实盘动作"
        if walk_forward_enabled
        else "不会创建候选、影子账户或实盘动作"
    )
    lines = [
        "# Factor Lab 轻量研究报告",
        "",
        f"- Run: `{summary.get('run_id')}`",
        f"- Suite: `{summary.get('suite')}` / mode: `{summary.get('mode')}`",
        f"- Evidence: `{evidence}`（{evidence_scope}）",
        objective,
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
        "## Walk-forward 候选注册与固定方向"
        if walk_forward_enabled
        else "## Stage A：训练段筛选",
        "",
        (
            "候选注册表和方向在 sweep 前固定；下表 IC 仅作描述，不能据此反转方向、"
            "增删候选或改变 selector。"
            if walk_forward_enabled
            else "基础信号方向、组合权重与最终名次都使用全部已观察历史。"
            if results_first_ranking
            else "基础信号与组合只做最近窗口执行 smoke；本报告不解释其收益排名。"
            if results_first_enabled
            else "shortlist 只使用 2017–2022 训练段；验证和审计不参与排序。"
        ),
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

    if walk_forward_enabled:
        lines.extend(
            _walk_forward_lines(
                summary,
                walk_forward,
                ranking_available=walk_forward_ranking,
            )
        )
    elif results_first_enabled:
        eligible_count = len(results_first.get("rankings") or [])
        excluded_count = len(results_first.get("excluded_from_ranking") or [])
        if results_first_ranking:
            lines.extend(
                [
                    "",
                    "## 组合执行与可比性",
                    "",
                    "Results-first 不使用旧路线的验证硬门槛或 `beats_control` 字段。"
                    "所有入榜策略必须覆盖控制组的完整调仓日期集合；覆盖不全者只保留诊断。",
                    f"- 可比较入榜：{eligible_count}；因覆盖不全排除：{excluded_count}。",
                ]
            )
        else:
            lines.extend(
                [
                    "",
                    "## 组合执行 smoke",
                    "",
                    f"已执行 {len(summary.get('stage_b') or [])} 个策略；canary 不计算可比性或名次。",
                ]
            )
    else:
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

    if results_first_ranking:
        lines.extend(
            [
                "",
                "## Results-first 全历史成绩榜",
                "",
                "该榜单直接优化全部已观察历史的成本后成绩；它用于选当前最强历史构造，"
                "不是未观察样本验证。",
                "综合分先把候选间的年化收益、Sharpe、IR、最大回撤转成百分位，再按 "
                "`50% / 25% / 15% / 10%` 加权；收益优先，同时保留各单项。",
                "",
                "| 排名 | 策略 | 类型 | 综合分 | 年化收益 | Sharpe | IR | 最大回撤 | 相对控制分差 |",
                "| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for row in list(results_first.get("rankings") or [])[:20]:
            lines.append(
                "| {rank} | {name} | {kind} | {score} | {annual} | {sharpe} | {ir} | {drawdown} | {delta} |".format(
                    rank=row.get("rank"),
                    name=row.get("factor_name"),
                    kind=row.get("strategy_kind"),
                    score=_number(row.get("historical_score")),
                    annual=_number(row.get("net_annual_return"), percent=True),
                    sharpe=_number(row.get("net_sharpe")),
                    ir=_number(row.get("information_ratio")),
                    drawdown=_number(row.get("max_drawdown"), percent=True),
                    delta=_number(row.get("historical_score_delta_vs_control")),
                )
            )
        excluded = list(results_first.get("excluded_from_ranking") or [])
        if excluded:
            lines.extend(["", "### 覆盖不完整、未入榜", ""])
            for row in excluded:
                lines.append(
                    f"- `{row.get('factor_name')}`：覆盖 "
                    f"{_number(row.get('period_coverage'), percent=True)}，"
                    "仅保留诊断，不参与冠军排序。"
                )
    elif results_first_enabled:
        lines.extend(
            [
                "",
                "## Results-first canary",
                "",
                "本次仅检查最近窗口的数据、信号、组合和产物链路；不输出排名或“最佳策略”。",
            ]
        )

    validated = list(summary.get("validated_factors") or [])
    lines.extend(
        [
            "",
            "## 结论",
            "",
            (
                f"- Dynamic phase rank：{walk_forward.get('dynamic_phase_rank') or '—'}；"
                f"Q20 首位策略：`{walk_forward.get('best_phase_strategy') or '—'}`。"
                "该结论聚合全部预注册 offset，不授权选择最佳 offset。"
                if walk_forward_ranking
                else "- Walk-forward canary 已完成；没有共同 phase 排名、动态策略结论或最佳 offset。"
                if walk_forward_enabled
                else f"- 全历史最佳策略：`{results_first.get('best_historical_strategy')}`；"
                "这是优化结果，不使用 validated 标签。"
                if results_first_ranking
                else "- Canary 执行链已完成；未生成历史排名或最佳策略。"
                if results_first_enabled
                else f"- 通过且优于控制的因子：{', '.join(validated) if validated else '0 个'}。"
            ),
        ]
    )
    if walk_forward_ranking:
        lines.append(
            "- 因果历史完整性："
            f"`{'valid' if walk_forward.get('causal_history_valid') else 'invalid'}`；"
            "历史阈值诊断："
            f"`{'pass' if walk_forward.get('historical_diagnostic_passed') else 'not_passed'}`。"
        )
    if not walk_forward_enabled and summary.get("robustness"):
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
    elif not walk_forward_enabled and summary.get("search_stopped"):
        lines.append("- 本轮预注册研究已停止：不追加变体，也不降低晋级门槛。")
    lines.append(f"- 搜索终态：`{summary.get('search_status') or 'unknown'}`。")
    if walk_forward_ranking:
        lines.extend(
            [
                "- 共同起点后的结果是 post-selection causal simulation：选择时无未来，"
                "但候选注册和协议本身已由既有研究提出，因此不是 pristine OOS。",
                "- 全部 offset 必须一起报告；相关 offset 非独立，不能按最好路径挑选或"
                "把通过比例解释成独立显著性。",
                "- 因果边界不覆盖财务 revision vintage、退市/吸收合并 ghost position"
                " 处置或等 AUM 可比性；这些仍是已知限制。",
                "- 历史回放不能证明未来盈利，也不会产生 validated 或实盘授权。",
                "",
            ]
        )
    elif walk_forward_enabled:
        lines.extend(
            [
                "- Canary 不具备共同评价起点或 phase Q20 分布，不能比较候选、动态策略或 offset。",
                "- 历史回放不能证明未来盈利。",
                "",
            ]
        )
    elif results_first_ranking:
        lines.extend(
            [
                "- 训练、验证和审计段均参与历史成绩排名；报告不再把其中任何一段称为盲测。",
                "- 历史回测不能证明未来盈利；该模式只回答哪种当前构造在已知历史上成绩最好。",
                "",
            ]
        )
    elif results_first_enabled:
        lines.extend(
            [
                "- Canary 结果不能用于比较策略强弱；只有 full 模式才会生成全历史榜单。",
                "- 历史回测不能证明未来盈利。",
                "",
            ]
        )
    else:
        lines.extend(
            [
                "- 审计段只允许否证，不参与 shortlist、控制比较或参数选择。",
                f"- 历史回测不能证明未来盈利；{len(validated)} 个 validated，0 个也是允许且诚实的结果。",
                "",
            ]
        )
    return "\n".join(lines)


__all__ = ["render_report"]

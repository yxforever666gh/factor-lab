from __future__ import annotations

from pathlib import Path
import sqlite3

from factor_lab.promotion_scorecard import build_promotion_scorecard
from factor_lab.db_views import ensure_views


def build_run_summary(db_path: str | Path, output_path: str | Path) -> None:
    ensure_views(db_path)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    latest_run = cur.execute(
        "SELECT run_id, created_at_utc, config_path, output_dir FROM workflow_runs WHERE status='finished' ORDER BY created_at_utc DESC LIMIT 1"
    ).fetchone()
    stable_candidates = cur.execute(
        "SELECT factor_name, COUNT(*) FROM factor_results WHERE variant='candidate' GROUP BY factor_name ORDER BY COUNT(*) DESC, factor_name ASC LIMIT 5"
    ).fetchall()
    best_portfolio = cur.execute(
        "SELECT strategy_name, AVG(sharpe) AS avg_sharpe FROM portfolio_results GROUP BY strategy_name ORDER BY avg_sharpe DESC LIMIT 1"
    ).fetchone()
    candidate_leaderboard = cur.execute(
        """
        SELECT name, research_stage, status, ROUND(COALESCE(latest_recent_final_score, latest_final_score), 6), evaluation_count
        FROM v_factor_candidate_leaderboard
        ORDER BY COALESCE(latest_recent_final_score, latest_final_score, -999) DESC, evaluation_count DESC
        LIMIT 5
        """
    ).fetchall()

    if not latest_run:
        Path(output_path).write_text("暂无运行记录。", encoding="utf-8")
        return

    _, created_at_utc, config_path, output_dir = latest_run
    output_dir = Path(output_dir) if output_dir else None
    candidates_text = "、".join(name for name, _ in stable_candidates) if stable_candidates else "暂无"
    leaderboard_text = "；".join(f"{name}({stage}/{status}, {score})" for name, stage, status, score, _ in candidate_leaderboard) if candidate_leaderboard else "暂无"
    strategy_text = (
        f"当前长期平均表现最好的策略是 {best_portfolio[0]}，平均夏普 {best_portfolio[1]:.2f}。"
        if best_portfolio else
        "当前还没有可用的策略统计。"
    )

    promotion_payload = build_promotion_scorecard(db_path=db_path, limit=8)
    promotion_summary = promotion_payload.get("summary") or {}
    priority_rows = promotion_summary.get("priority_rows") or []
    if priority_rows:
        promotion_text = "；".join(
            f"{row['factor_name']}({row['decision_label']}：{row['decision_summary']})"
            for row in priority_rows
        )
    else:
        promotion_text = "暂无"

    status_snapshot = []
    if output_dir and (output_dir / 'candidate_status_snapshot.json').exists():
        import json
        status_snapshot = json.loads((output_dir / 'candidate_status_snapshot.json').read_text(encoding='utf-8'))
    stage_counts = {'explore': 0, 'watchlist': 0, 'candidate': 0, 'graveyard': 0}
    for row in status_snapshot:
        stage = row.get('research_stage')
        if stage in stage_counts:
            stage_counts[stage] += 1

    rolling_summary = []
    if output_dir and (output_dir / 'rolling_summary.json').exists():
        import json
        rolling_summary = json.loads((output_dir / 'rolling_summary.json').read_text(encoding='utf-8'))
    rolling_text = '暂无 rolling 摘要。'
    if rolling_summary:
        top_rows = sorted(rolling_summary, key=lambda row: (row.get('stability_score') or -1), reverse=True)[:3]
        rolling_text = '；'.join(
            f"{row['factor_name']}(稳定性={row.get('stability_score')}, pass_rate={row.get('pass_rate')}, flips={row.get('sign_flip_count')})"
            for row in top_rows
        )

    research_metrics = {}
    metrics_path = Path(db_path).resolve().parent / 'research_metrics.json'
    if metrics_path.exists():
        import json
        research_metrics = (json.loads(metrics_path.read_text(encoding='utf-8')) or {}).get('metrics') or {}

    approved_universe = {}
    approved_path = Path(db_path).resolve().parent / 'approved_candidate_universe.json'
    if approved_path.exists():
        import json
        approved_universe = json.loads(approved_path.read_text(encoding='utf-8')) or {}

    lines = [
        f"最新一次完成的研究任务来自 {config_path}。",
        f"运行时间：{created_at_utc}。",
        strategy_text,
        f"目前最稳定的候选因子：{candidates_text}。",
        f"当前候选榜单前列：{leaderboard_text}。",
        f"四层研究状态：explore {stage_counts['explore']} 个，watchlist {stage_counts['watchlist']} 个，candidate {stage_counts['candidate']} 个，graveyard {stage_counts['graveyard']} 个。",
        f"当前 rolling 稳定性前列：{rolling_text}",
        f"当前晋级赛优先处理：{promotion_text}。",
    ]
    if approved_universe:
        approved_rows = approved_universe.get('rows') or []
        summary = approved_universe.get('summary') or {}
        lines.append(
            f"Approved Universe：{len(approved_rows)} 个候选，版本 {approved_universe.get('selection_policy_version') or '-'}，当前入池={ '、'.join([row.get('factor_name') for row in approved_rows[:5] if row.get('factor_name')]) or '无' }。"
        )
        lines.append(
            f"AU 治理：state={summary.get('state_counts') or {}}, actions={summary.get('governance_action_counts') or {}}, bucket_budget={(approved_universe.get('budget_summary') or {}).get('bucket_allocations') or {}}。"
        )
    novelty_summary = {}
    novelty_path = Path(db_path).resolve().parent / 'novelty_judge_summary.json'
    if novelty_path.exists():
        import json
        novelty_summary = json.loads(novelty_path.read_text(encoding='utf-8')) or {}
    if novelty_summary:
        lines.append(
            f"Novelty Judge：classes={novelty_summary.get('class_counts') or {}}, promote_like={novelty_summary.get('promote_like_count') or 0}, suppress_like={novelty_summary.get('suppress_like_count') or 0}。"
        )
    decision_ab = {}
    decision_ab_path = Path(db_path).resolve().parent / 'decision_ab_report.json'
    if decision_ab_path.exists():
        import json
        decision_ab = json.loads(decision_ab_path.read_text(encoding='utf-8')) or {}
    if decision_ab:
        lines.append(
            f"Decision A/B：baseline={decision_ab.get('baseline_policy')}, candidate={decision_ab.get('candidate_policy')}, quality_delta={decision_ab.get('quality_delta')}, duplicate_delta={decision_ab.get('duplicate_delta')}, recommendation={decision_ab.get('recommendation')}。"
        )
    au_zero_diagnosis = {}
    au_zero_diagnosis_path = Path(db_path).resolve().parent / 'au_zero_diagnosis.json'
    if au_zero_diagnosis_path.exists():
        import json
        au_zero_diagnosis = json.loads(au_zero_diagnosis_path.read_text(encoding='utf-8')) or {}
    if au_zero_diagnosis:
        lines.append(
            f"AU=0 诊断：{(au_zero_diagnosis.get('summary') or {}).get('direct_cause') or '-'}。"
        )
    failure_enhancement = {}
    failure_enhancement_path = Path(db_path).resolve().parent / 'failure_analyst_enhancement.json'
    if failure_enhancement_path.exists():
        import json
        failure_enhancement = json.loads(failure_enhancement_path.read_text(encoding='utf-8')) or {}
    if failure_enhancement:
        summary = failure_enhancement.get('summary') or {}
        lines.append(
            f"Failure Analyst+：reroute={summary.get('reroute_count') or 0}, stop={summary.get('stop_count') or 0}, continue={summary.get('continue_count') or 0}, question_cards_v2={summary.get('question_card_count') or 0}。"
        )
    consistency = {}
    consistency_path = Path(db_path).resolve().parent / 'artifact_consistency_report.json'
    if consistency_path.exists():
        import json
        consistency = json.loads(consistency_path.read_text(encoding='utf-8')) or {}
    if consistency and consistency.get('warnings'):
        lines.append(
            f"Artifact Consistency：warning_count={consistency.get('warning_count') or 0}, warnings={(consistency.get('warnings') or [])[:5]}。"
        )
    effect = {}
    effect_path = Path(db_path).resolve().parent / 'factor_quality_effect_report.json'
    if effect_path.exists():
        import json
        effect = json.loads(effect_path.read_text(encoding='utf-8')) or {}
    if effect:
        final_judgment = effect.get('final_judgment') or {}
        lines.append(
            f"效果拆账：runtime={final_judgment.get('runtime')}, candidate_discovery={final_judgment.get('candidate_discovery')}, factor_quality={final_judgment.get('factor_quality')}。"
        )
    root_cause = {}
    root_cause_path = Path(db_path).resolve().parent / 'quality_not_proven_root_cause_report.json'
    if root_cause_path.exists():
        import json
        root_cause = json.loads(root_cause_path.read_text(encoding='utf-8')) or {}
    if root_cause:
        top_causes = [row.get('cause_key') for row in (root_cause.get('root_causes') or [])[:3] if row.get('cause_key')]
        next_actions = [row.get('action_key') for row in (root_cause.get('next_actions') or [])[:2] if row.get('action_key')]
        lines.append(
            f"质量未被证明根因：{top_causes or ['none']}；建议下一步：{next_actions or ['observe_more_samples']}。"
        )
    if research_metrics:
        lines.append(
            f"自治研究指标：generated_candidate_recent={research_metrics.get('generated_candidate_task_count_recent', 0)}，generated_outcomes={research_metrics.get('generated_candidate_outcome_count', 0)}，duplicate_suppression_ratio={research_metrics.get('duplicate_suppression_ratio')}，research_mode={(research_metrics.get('research_mode') or {}).get('mode', '-') }。"
        )
    Path(output_path).write_text("\n".join(lines), encoding="utf-8")
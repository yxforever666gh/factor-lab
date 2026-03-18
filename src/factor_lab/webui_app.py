from __future__ import annotations

import sqlite3
import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from jinja2 import Environment, FileSystemLoader, select_autoescape

from factor_lab.db_views import ensure_views
from factor_lab.ops import latest_task_states, trigger_script


def pretty_json_text(value: Any, empty_text: str = "暂无数据。") -> str:
    if value in (None, "", [], {}):
        return empty_text
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, indent=2)


def portfolio_positions(current: dict[str, Any]) -> list[dict[str, Any]]:
    positions = current.get("positions") or []
    return sorted(positions, key=lambda row: row.get("weight", 0), reverse=True)


def compute_health_metrics() -> dict[str, Any]:
    conn = get_conn()
    try:
        runs = [dict(row) for row in conn.execute(
            """
            SELECT run_id, created_at_utc, config_path, start_date, end_date,
                   status, factor_count, dataset_rows
            FROM workflow_runs
            ORDER BY created_at_utc DESC
            LIMIT 30
            """
        ).fetchall()]
        latest_run = runs[0] if runs else None
        recent_24h_total = conn.execute(
            "SELECT COUNT(*) FROM workflow_runs WHERE created_at_utc >= datetime('now', '-1 day')"
        ).fetchone()[0]
        recent_24h_finished = conn.execute(
            "SELECT COUNT(*) FROM workflow_runs WHERE created_at_utc >= datetime('now', '-1 day') AND status = 'finished'"
        ).fetchone()[0]
        recent_7d_total = conn.execute(
            "SELECT COUNT(*) FROM workflow_runs WHERE created_at_utc >= datetime('now', '-7 day')"
        ).fetchone()[0]
        recent_7d_finished = conn.execute(
            "SELECT COUNT(*) FROM workflow_runs WHERE created_at_utc >= datetime('now', '-7 day') AND status = 'finished'"
        ).fetchone()[0]

        candidate_rows = [dict(row) for row in conn.execute(
            """
            SELECT run_id, factor_name
            FROM factor_results
            WHERE variant = 'candidate'
            ORDER BY run_id ASC, factor_name ASC
            """
        ).fetchall()]
        candidate_by_run: dict[str, list[str]] = {}
        for row in candidate_rows:
            candidate_by_run.setdefault(row['run_id'], []).append(row['factor_name'])

        candidate_runs = [r for r in runs if r['run_id'] in candidate_by_run]
        latest_candidates = candidate_by_run.get(latest_run['run_id'], []) if latest_run else []
        previous_candidates = candidate_by_run.get(runs[1]['run_id'], []) if len(runs) > 1 else []
        stable_candidate_count = conn.execute(
            "SELECT COUNT(*) FROM v_stable_candidates WHERE candidate_runs >= 2"
        ).fetchone()[0]

        recent_candidate_counts = [len(candidate_by_run.get(r['run_id'], [])) for r in runs[:7]]
        candidate_churn = 0
        if latest_candidates or previous_candidates:
            latest_set = set(latest_candidates)
            previous_set = set(previous_candidates)
            candidate_churn = len(latest_set.symmetric_difference(previous_set))

        strategy_rows = [dict(row) for row in conn.execute(
            """
            SELECT w.created_at_utc, p.run_id, p.strategy_name, p.sharpe, p.annual_return, p.max_drawdown, p.avg_turnover
            FROM portfolio_results p
            JOIN workflow_runs w ON w.run_id = p.run_id
            ORDER BY w.created_at_utc DESC
            LIMIT 200
            """
        ).fetchall()]
        strategy_map: dict[str, list[dict[str, Any]]] = {}
        for row in strategy_rows:
            strategy_map.setdefault(row['strategy_name'], []).append(row)

        def avg_metric(strategy_name: str, field: str, limit: int = 5):
            rows = strategy_map.get(strategy_name, [])[:limit]
            values = [row[field] for row in rows if row.get(field) is not None]
            return round(sum(values) / len(values), 6) if values else None

        candidates_only_recent_sharpe = avg_metric('long_short_top_bottom_candidates_only', 'sharpe')
        all_factors_recent_sharpe = avg_metric('long_short_top_bottom_all_factors', 'sharpe')
        candidates_only_recent_return = avg_metric('long_short_top_bottom_candidates_only', 'annual_return')
        all_factors_recent_return = avg_metric('long_short_top_bottom_all_factors', 'annual_return')

        factor_score_trend = [dict(row) for row in conn.execute(
            """
            SELECT factor_name, ROUND(AVG(score), 6) AS avg_score, COUNT(*) AS runs
            FROM factor_results
            WHERE variant = 'raw_scored'
            GROUP BY factor_name
            HAVING COUNT(*) >= 2
            ORDER BY avg_score DESC
            LIMIT 5
            """
        ).fetchall()]

        base = DB_PATH.parent
        llm_status_path = base / 'llm_status.json'
        snapshot_path = base / 'llm_input_snapshot.json'
        llm_status = json.loads(llm_status_path.read_text(encoding='utf-8')) if llm_status_path.exists() else {}
        snapshot = json.loads(snapshot_path.read_text(encoding='utf-8')) if snapshot_path.exists() else {}
        paper_stability = snapshot.get('paper_portfolio_stability', {}) or {}
        recommendation_history_tail = snapshot.get('recommendation_history_tail', []) or []
        positive_count = len([row for row in recommendation_history_tail if row.get('effectiveness') == 'positive'])
        recommendation_hit_rate = round(positive_count / len(recommendation_history_tail), 4) if recommendation_history_tail else None

        run_health_score = 0
        if recent_24h_total:
            run_health_score += 60 * (recent_24h_finished / recent_24h_total)
        if latest_run and latest_run.get('status') == 'finished':
            run_health_score += 20
        if latest_run and latest_run.get('dataset_rows'):
            run_health_score += 20
        run_health_score = round(run_health_score, 1)

        research_progress_score = 0
        if stable_candidate_count:
            research_progress_score += min(stable_candidate_count * 10, 35)
        if candidate_churn == 0:
            research_progress_score += 20
        elif candidate_churn <= 2:
            research_progress_score += 10
        if recommendation_hit_rate is not None:
            research_progress_score += round(recommendation_hit_rate * 25, 1)
        if latest_candidates:
            research_progress_score += 20
        research_progress_score = round(min(research_progress_score, 100), 1)

        portfolio_progress_score = 0
        if candidates_only_recent_sharpe is not None:
            portfolio_progress_score += min(max(candidates_only_recent_sharpe, 0) * 5, 40)
        if paper_stability.get('stability_score') is not None:
            portfolio_progress_score += round(float(paper_stability['stability_score']) * 40, 1)
        if candidates_only_recent_return is not None and all_factors_recent_return is not None and candidates_only_recent_return >= 0.6 * all_factors_recent_return:
            portfolio_progress_score += 20
        portfolio_progress_score = round(min(portfolio_progress_score, 100), 1)

        return {
            'latest_run': latest_run,
            'recent_runs': runs,
            'run_health': {
                'score': run_health_score,
                'runs_24h': recent_24h_total,
                'finished_24h': recent_24h_finished,
                'runs_7d': recent_7d_total,
                'finished_7d': recent_7d_finished,
                'success_rate_24h': round(recent_24h_finished / recent_24h_total, 4) if recent_24h_total else None,
                'success_rate_7d': round(recent_7d_finished / recent_7d_total, 4) if recent_7d_total else None,
                'latest_status': latest_run.get('status') if latest_run else None,
                'latest_end_date': latest_run.get('end_date') if latest_run else None,
                'latest_dataset_rows': latest_run.get('dataset_rows') if latest_run else None,
            },
            'research_progress': {
                'score': research_progress_score,
                'stable_candidate_count': stable_candidate_count,
                'latest_candidates': latest_candidates,
                'previous_candidates': previous_candidates,
                'candidate_churn': candidate_churn,
                'recent_candidate_counts': recent_candidate_counts,
                'factor_score_trend': factor_score_trend,
                'llm_status': llm_status.get('status'),
                'recommendation_hit_rate': recommendation_hit_rate,
                'recommendation_tail_size': len(recommendation_history_tail),
            },
            'portfolio_progress': {
                'score': portfolio_progress_score,
                'candidates_only_recent_sharpe': candidates_only_recent_sharpe,
                'all_factors_recent_sharpe': all_factors_recent_sharpe,
                'candidates_only_recent_return': candidates_only_recent_return,
                'all_factors_recent_return': all_factors_recent_return,
                'paper_stability': paper_stability,
            },
        }
    finally:
        conn.close()


BASE_DIR = Path(__file__).resolve().parent
TEMPLATE_DIR = BASE_DIR / "webui_templates"
DB_PATH = Path(__file__).resolve().parents[2] / "artifacts" / "factor_lab.db"


def get_conn() -> sqlite3.Connection:
    ensure_views(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_all(query: str, params: tuple[Any, ...] = ()):
    conn = get_conn()
    try:
        return [dict(row) for row in conn.execute(query, params).fetchall()]
    finally:
        conn.close()


def fetch_one(query: str, params: tuple[Any, ...] = ()):
    conn = get_conn()
    try:
        row = conn.execute(query, params).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


env = Environment(
    loader=FileSystemLoader(TEMPLATE_DIR),
    autoescape=select_autoescape(["html", "xml"]),
)

app = FastAPI(title="Factor Lab 中文控制台")


def render(template_name: str, **context) -> HTMLResponse:
    template = env.get_template(template_name)
    return HTMLResponse(template.render(**context))


@app.get("/", response_class=HTMLResponse)
def dashboard():
    health = compute_health_metrics()
    latest_runs = fetch_all(
        "SELECT run_id, created_at_utc, status, config_path FROM workflow_runs ORDER BY created_at_utc DESC LIMIT 8"
    )
    stable_candidates = fetch_all(
        "SELECT factor_name, candidate_runs FROM v_stable_candidates ORDER BY candidate_runs DESC, factor_name ASC"
    )
    top_factors = fetch_all(
        "SELECT factor_name, ROUND(avg_score, 6) AS avg_score, runs FROM v_factor_score_avg ORDER BY avg_score DESC LIMIT 8"
    )
    top_strategies = fetch_all(
        "SELECT strategy_name, ROUND(avg_sharpe, 6) AS avg_sharpe, ROUND(avg_return, 6) AS avg_return, runs FROM v_portfolio_strategy_avg ORDER BY avg_sharpe DESC LIMIT 8"
    )
    latest_summary_path = DB_PATH.parent / "latest_summary.txt"
    change_report_path = DB_PATH.parent / "change_report.md"
    latest_summary = latest_summary_path.read_text(encoding="utf-8") if latest_summary_path.exists() else "暂无摘要。"
    change_report = change_report_path.read_text(encoding="utf-8") if change_report_path.exists() else "暂无变化报告。"
    return render(
        "dashboard.html",
        title="总览",
        health=health,
        latest_runs=latest_runs,
        stable_candidates=stable_candidates,
        top_factors=top_factors,
        top_strategies=top_strategies,
        latest_summary=latest_summary,
        change_report=change_report,
    )


@app.get("/health", response_class=HTMLResponse)
def health_page():
    health = compute_health_metrics()
    return render("health.html", title="健康度", health=health)


@app.get("/cockpit", response_class=HTMLResponse)
def cockpit_page():
    base = DB_PATH.parent
    latest_run = fetch_one(
        "SELECT run_id, created_at_utc, config_path, status FROM workflow_runs ORDER BY created_at_utc DESC LIMIT 1"
    )
    stable_candidates = fetch_all(
        "SELECT factor_name, candidate_runs FROM v_stable_candidates ORDER BY candidate_runs DESC, factor_name ASC LIMIT 10"
    )
    llm_status_path = base / "llm_status.json"
    llm_status = json.loads(llm_status_path.read_text(encoding="utf-8")) if llm_status_path.exists() else {}
    snapshot_path = base / "llm_input_snapshot.json"
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8")) if snapshot_path.exists() else {}
    change_report_path = base / "change_report.md"
    paper_current_path = base / "paper_portfolio" / "current_portfolio.json"
    recommendation_context = snapshot.get("recommendation_context", {}) or {}
    plan_validation = (llm_status.get("plan_validation", {})) or {}
    paper_portfolio = json.loads(paper_current_path.read_text(encoding="utf-8")) if paper_current_path.exists() else {}
    return render(
        "cockpit.html",
        title="驾驶舱",
        latest_run=latest_run,
        stable_candidates=stable_candidates,
        llm_status=llm_status,
        paper_stability=snapshot.get("paper_portfolio_stability", {}),
        portfolio_policy=plan_validation.get("portfolio_policy", {}),
        conservative_policy=snapshot.get("conservative_policy", {}),
        recommendation_context=recommendation_context,
        recommendation_context_text=pretty_json_text(recommendation_context, "暂无模板上下文。"),
        plan_validation=plan_validation,
        plan_validation_text=pretty_json_text(plan_validation, "暂无计划校验摘要。"),
        paper_portfolio=paper_portfolio,
        paper_portfolio_positions=portfolio_positions(paper_portfolio),
        paper_portfolio_text=pretty_json_text(paper_portfolio, "暂无纸面组合。"),
        change_report=change_report_path.read_text(encoding="utf-8") if change_report_path.exists() else "暂无变化报告。",
    )


@app.get("/runs", response_class=HTMLResponse)
def runs_page():
    runs = fetch_all(
        """
        SELECT run_id, created_at_utc, config_path, data_source, start_date, end_date,
               factor_count, dataset_rows, status, rerun_of_run_id
        FROM workflow_runs
        ORDER BY created_at_utc DESC
        LIMIT 100
        """
    )
    return render("runs.html", title="运行记录", runs=runs)


@app.get("/runs/{run_id}", response_class=HTMLResponse)
def run_detail(run_id: str):
    run = fetch_one(
        "SELECT * FROM workflow_runs WHERE run_id = ?",
        (run_id,),
    )
    if not run:
        raise HTTPException(status_code=404, detail="未找到该运行记录")

    factors = fetch_all(
        """
        SELECT factor_name, variant, expression, rank_ic_mean, rank_ic_ir,
               top_bottom_spread_mean, pass_gate, fail_reason, score, split_fail_count, high_corr_peers_json
        FROM factor_results
        WHERE run_id = ?
        ORDER BY variant, score DESC, factor_name ASC
        """,
        (run_id,),
    )
    portfolios = fetch_all(
        "SELECT * FROM portfolio_results WHERE run_id = ? ORDER BY sharpe DESC",
        (run_id,),
    )
    artifacts = fetch_all(
        "SELECT artifact_name, artifact_path FROM run_artifacts WHERE run_id = ? ORDER BY artifact_name ASC",
        (run_id,),
    )
    return render(
        "run_detail.html",
        title=f"运行详情 {run_id[:8]}",
        run=run,
        factors=factors,
        portfolios=portfolios,
        artifacts=artifacts,
    )


@app.get("/factors", response_class=HTMLResponse)
def factors_page():
    factors = fetch_all(
        """
        SELECT
            s.factor_name,
            ROUND(s.avg_score, 6) AS avg_score,
            s.runs,
            COALESCE(c.candidate_runs, 0) AS candidate_runs,
            COALESCE(g.graveyard_runs, 0) AS graveyard_runs
        FROM v_factor_score_avg s
        LEFT JOIN v_stable_candidates c ON s.factor_name = c.factor_name
        LEFT JOIN (
            SELECT factor_name, COUNT(*) AS graveyard_runs
            FROM factor_results
            WHERE variant = 'graveyard'
            GROUP BY factor_name
        ) g ON s.factor_name = g.factor_name
        ORDER BY s.avg_score DESC
        """
    )
    return render("factors.html", title="因子", factors=factors)


@app.get("/portfolios", response_class=HTMLResponse)
def portfolios_page():
    strategies = fetch_all(
        "SELECT strategy_name, ROUND(avg_sharpe, 6) AS avg_sharpe, ROUND(avg_return, 6) AS avg_return, runs FROM v_portfolio_strategy_avg ORDER BY avg_sharpe DESC"
    )
    recent = fetch_all(
        """
        SELECT p.run_id, p.strategy_name, p.annual_return, p.sharpe, p.max_drawdown, p.avg_turnover
        FROM portfolio_results p
        JOIN workflow_runs w ON p.run_id = w.run_id
        ORDER BY w.created_at_utc DESC, p.sharpe DESC
        LIMIT 30
        """
    )
    return render("portfolios.html", title="组合", strategies=strategies, recent=recent)


@app.get("/paper-portfolio", response_class=HTMLResponse)
def paper_portfolio_page():
    base = DB_PATH.parent / "paper_portfolio"
    current_path = base / "current_portfolio.json"
    history_path = base / "portfolio_history.json"
    change_log_path = base / "portfolio_change_log.md"
    retro_path = base / "portfolio_retrospective.json"
    stability_path = base / "portfolio_stability_score.json"
    current = json.loads(current_path.read_text(encoding="utf-8")) if current_path.exists() else {}
    retrospective = json.loads(retro_path.read_text(encoding="utf-8")) if retro_path.exists() else {}
    stability = json.loads(stability_path.read_text(encoding="utf-8")) if stability_path.exists() else {}
    return render(
        "paper_portfolio.html",
        title="纸面组合",
        current=current,
        current_positions=portfolio_positions(current),
        retrospective=retrospective,
        stability=stability,
        history_text=history_path.read_text(encoding="utf-8") if history_path.exists() else "暂无组合历史。",
        change_log_text=change_log_path.read_text(encoding="utf-8") if change_log_path.exists() else "暂无组合变更日志。",
        retrospective_text=pretty_json_text(retrospective, "暂无组合回溯。"),
        stability_text=pretty_json_text(stability, "暂无稳定性评分。"),
    )


@app.get("/llm", response_class=HTMLResponse)
def llm_page():
    base = DB_PATH.parent
    review_path = base / "llm_review.md"
    plan_path = base / "llm_next_batch_proposal.json"
    snapshot_path = base / "llm_input_snapshot.json"
    status_path = base / "llm_status.json"
    request_path = base / "agent_request.json"
    review_text = review_path.read_text(encoding="utf-8") if review_path.exists() else "暂无 LLM 评审。"
    plan_text = plan_path.read_text(encoding="utf-8") if plan_path.exists() else "暂无 LLM 计划。"
    snapshot_text = snapshot_path.read_text(encoding="utf-8") if snapshot_path.exists() else "暂无 LLM 输入快照。"
    status_text = status_path.read_text(encoding="utf-8") if status_path.exists() else "暂无 LLM 状态。"
    request_text = request_path.read_text(encoding="utf-8") if request_path.exists() else "暂无 bridge 请求。"
    llm_status = json.loads(status_text) if status_path.exists() else {}
    llm_plan = json.loads(plan_text) if plan_path.exists() else {}
    snapshot = json.loads(snapshot_text) if snapshot_path.exists() else {}
    agent_request = json.loads(request_text) if request_path.exists() else {}
    generated_batch_path = DB_PATH.parent / "generated_batch_from_llm.json"
    generated_batch_workflow_path = DB_PATH.parent / "generated_workflow_from_llm.json"
    feedback_path = DB_PATH.parent / "llm_plan_feedback.json"
    retrospective_path = DB_PATH.parent / "llm_retrospective.json"
    retrospective_md_path = DB_PATH.parent / "llm_retrospective.md"
    recommendation_history_path = DB_PATH.parent / "llm_recommendation_history.json"
    recommendation_weights_path = DB_PATH.parent / "llm_recommendation_weights.json"
    recommendation_context = snapshot.get("recommendation_context", {}) or {}
    recommendation_history_tail = snapshot.get("recommendation_history_tail", []) or []
    plan_validation = llm_status.get("plan_validation", {}) or {}
    generated_batch = json.loads(generated_batch_path.read_text(encoding="utf-8")) if generated_batch_path.exists() else {}
    generated_workflow = json.loads(generated_batch_workflow_path.read_text(encoding="utf-8")) if generated_batch_workflow_path.exists() else {}
    recommendation_weights = json.loads(recommendation_weights_path.read_text(encoding="utf-8")) if recommendation_weights_path.exists() else {}
    return render(
        "llm.html",
        title="LLM",
        review_text=review_text,
        plan_text=plan_text,
        snapshot_text=snapshot_text,
        status_text=status_text,
        request_text=request_text,
        llm_status=llm_status,
        llm_plan=llm_plan,
        snapshot=snapshot,
        agent_request=agent_request,
        generated_batch=generated_batch,
        generated_batch_text=pretty_json_text(generated_batch, "暂无生成的 batch。"),
        generated_workflow_text=pretty_json_text(generated_workflow, "暂无生成的 workflow。"),
        generated_feedback_text=feedback_path.read_text(encoding="utf-8") if feedback_path.exists() else "暂无 batch 执行反馈。",
        retrospective_text=retrospective_md_path.read_text(encoding="utf-8") if retrospective_md_path.exists() else "暂无建议效果回溯。",
        retrospective_json_text=retrospective_path.read_text(encoding="utf-8") if retrospective_path.exists() else "暂无建议效果回溯 JSON。",
        recommendation_history_text=recommendation_history_path.read_text(encoding="utf-8") if recommendation_history_path.exists() else "暂无建议历史。",
        recommendation_weights=recommendation_weights,
        recommendation_weights_text=pretty_json_text(recommendation_weights, "暂无建议权重。"),
        recommendation_history_tail=recommendation_history_tail,
        recommendation_history_tail_text=pretty_json_text(recommendation_history_tail, "暂无已注入 planner 的历史尾部。"),
        recommendation_context=recommendation_context,
        recommendation_context_text=pretty_json_text(recommendation_context, "暂无模板优先级摘要与疲劳度。"),
        plan_validation=plan_validation,
        plan_validation_text=pretty_json_text(plan_validation, "暂无计划校验结果。"),
    )


@app.get("/ops", response_class=HTMLResponse)
def ops_page():
    tasks = latest_task_states(limit=20)
    return render("ops.html", title="操作", tasks=tasks, result=None)


@app.get("/ops/run/{target}", response_class=HTMLResponse)
def ops_run(target: str):
    mapping = {
        "workflow": "scripts/run_tushare_workflow.py",
        "batch": "scripts/run_tushare_batch.py",
        "cycle": "scripts/run_scheduled_cycle.py",
        "llm": "scripts/run_llm_cycle.py",
        "llm-bridge": "scripts/run_llm_bridge_prepare.py",
        "llm-bridge-import": "scripts/import_llm_bridge_response.py",
        "llm-bridge-check": "scripts/check_and_import_llm_bridge.py",
        "llm-plan-generate": "scripts/generate_batch_from_llm_plan.py",
        "llm-plan-run": "scripts/run_generated_batch_from_llm.py",
        "llm-retrospective": "scripts/build_llm_retrospective.py",
        "llm-memory": "scripts/build_recommendation_memory.py",
    }
    if target not in mapping:
        raise HTTPException(status_code=404, detail="未知操作目标")
    result = trigger_script(mapping[target])
    tasks = latest_task_states(limit=20)
    return render("ops.html", title="操作", tasks=tasks, result=result)

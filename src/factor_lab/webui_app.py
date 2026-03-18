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
        latest_runs=latest_runs,
        stable_candidates=stable_candidates,
        top_factors=top_factors,
        top_strategies=top_strategies,
        latest_summary=latest_summary,
        change_report=change_report,
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
        generated_batch_text=generated_batch_path.read_text(encoding="utf-8") if generated_batch_path.exists() else "暂无生成的 batch。",
        generated_workflow_text=generated_batch_workflow_path.read_text(encoding="utf-8") if generated_batch_workflow_path.exists() else "暂无生成的 workflow。",
        generated_feedback_text=feedback_path.read_text(encoding="utf-8") if feedback_path.exists() else "暂无 batch 执行反馈。",
        retrospective_text=retrospective_md_path.read_text(encoding="utf-8") if retrospective_md_path.exists() else "暂无建议效果回溯。",
        retrospective_json_text=retrospective_path.read_text(encoding="utf-8") if retrospective_path.exists() else "暂无建议效果回溯 JSON。",
        plan_validation_text=json.dumps(llm_status.get("plan_validation", {}), ensure_ascii=False, indent=2) if llm_status.get("plan_validation") else "暂无计划校验结果。",
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
    }
    if target not in mapping:
        raise HTTPException(status_code=404, detail="未知操作目标")
    result = trigger_script(mapping[target])
    tasks = latest_task_states(limit=20)
    return render("ops.html", title="操作", tasks=tasks, result=result)

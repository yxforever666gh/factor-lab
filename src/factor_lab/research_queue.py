from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from factor_lab.dedup import config_fingerprint
from factor_lab.heartbeat import append_heartbeat
from factor_lab.storage import ExperimentStore
from factor_lab.workflow import run_workflow
from factor_lab.batch import run_batch
from factor_lab.change_detection import build_change_report
from factor_lab.reporting import write_sqlite_report
from factor_lab.html_report import build_html_report
from factor_lab.index_page import build_index_page
from factor_lab.summary import build_run_summary


DB_PATH = Path("artifacts") / "factor_lab.db"


def enqueue_baseline_tasks(store: ExperimentStore) -> list[str]:
    seeds = [
        {
            "task_type": "workflow",
            "priority": 10,
            "config_path": "configs/tushare_workflow.json",
            "output_dir": "artifacts/tushare_workflow",
            "worker_note": "标准中窗口基线",
        },
        {
            "task_type": "batch",
            "priority": 20,
            "config_path": "configs/tushare_batch.json",
            "output_dir": "artifacts/tushare_batch",
            "worker_note": "标准 batch 对比",
        },
    ]
    task_ids = []
    for seed in seeds:
        cfg = json.loads(Path(seed["config_path"]).read_text(encoding="utf-8"))
        fingerprint = f"{seed['task_type']}::{config_fingerprint(cfg)}::{seed['output_dir']}"
        task_id = store.enqueue_research_task(
            task_type=seed["task_type"],
            payload={"config_path": seed["config_path"], "output_dir": seed["output_dir"]},
            priority=seed["priority"],
            fingerprint=fingerprint,
            worker_note=seed["worker_note"],
        )
        task_ids.append(task_id)
    return task_ids


def refresh_reports() -> None:
    write_sqlite_report(db_path=DB_PATH, output_path="artifacts/sqlite_report.md")
    build_html_report(db_path=DB_PATH, output_path="artifacts/report.html")
    build_index_page(db_path=DB_PATH, output_path="artifacts/index.html")
    build_run_summary(db_path=DB_PATH, output_path="artifacts/latest_summary.txt")
    build_change_report(db_path=DB_PATH, output_path="artifacts/change_report.md")


def execute_task(task: dict[str, Any]) -> str:
    payload = task["payload"]
    task_type = task["task_type"]
    if task_type == "workflow":
        run_workflow(config_path=payload["config_path"], output_dir=payload["output_dir"])
        refresh_reports()
        return f"workflow finished: {payload['config_path']}"
    if task_type == "batch":
        run_batch(config_path=payload["config_path"], output_dir=payload["output_dir"])
        refresh_reports()
        return f"batch finished: {payload['config_path']}"
    raise ValueError(f"unsupported task_type: {task_type}")


def run_orchestrator(max_tasks: int = 1) -> dict[str, Any]:
    store = ExperimentStore(DB_PATH)
    if not store.list_research_tasks(limit=1):
        enqueue_baseline_tasks(store)

    processed = []
    append_heartbeat("research_orchestrator", "started", summary=f"orchestrator awakened (max_tasks={max_tasks})")
    for _ in range(max_tasks):
        task = store.claim_next_research_task()
        if not task:
            break
        try:
            summary = execute_task(task)
            store.finish_research_task(task["task_id"], status="finished", worker_note=summary)
            processed.append({"task_id": task["task_id"], "status": "finished", "summary": summary})
            append_heartbeat("research_orchestrator", "finished", summary=summary, task_id=task["task_id"], task_type=task["task_type"])
        except Exception as exc:
            store.finish_research_task(task["task_id"], status="failed", last_error=str(exc))
            processed.append({"task_id": task["task_id"], "status": "failed", "error": str(exc)})
            append_heartbeat("research_orchestrator", "failed", message=str(exc), task_id=task["task_id"], task_type=task["task_type"])

    if not processed:
        append_heartbeat("research_orchestrator", "idle", summary="no pending research tasks")
    return {
        "processed": processed,
        "remaining_preview": store.list_research_tasks(limit=10),
    }

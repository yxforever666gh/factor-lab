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
from factor_lab.llm_feedback import summarize_generated_batch_run
from factor_lab.llm_bridge import write_bridge_status
from datetime import datetime, timezone


BASELINE_PRIORITY = 10
VALIDATION_PRIORITY = 30
EXPLORATION_PRIORITY = 60
RETRY_PRIORITY = 15
MAX_PENDING_BASELINE = 2
MAX_PENDING_VALIDATION = 2
MAX_PENDING_EXPLORATION = 1


DB_PATH = Path("artifacts") / "factor_lab.db"


def _category_from_note(note: str | None) -> str:
    note = note or ""
    if note.startswith("baseline"):
        return "baseline"
    if note.startswith("validation"):
        return "validation"
    if note.startswith("exploration"):
        return "exploration"
    if note.startswith("retry"):
        return "retry"
    return "other"


def queue_budget_snapshot(store: ExperimentStore) -> dict[str, int]:
    tasks = store.list_research_tasks(limit=200)
    counts = {"baseline": 0, "validation": 0, "exploration": 0}
    for task in tasks:
        if task["status"] not in {"pending", "running"}:
            continue
        category = _category_from_note(task.get("worker_note"))
        if category in counts:
            counts[category] += 1
    return counts


def enqueue_baseline_tasks(store: ExperimentStore) -> list[str]:
    seeds = [
        {
            "task_type": "workflow",
            "priority": BASELINE_PRIORITY,
            "config_path": "configs/tushare_workflow.json",
            "output_dir": "artifacts/tushare_workflow",
            "worker_note": "baseline｜标准中窗口基线",
        },
        {
            "task_type": "batch",
            "priority": VALIDATION_PRIORITY,
            "config_path": "configs/tushare_batch.json",
            "output_dir": "artifacts/tushare_batch",
            "worker_note": "validation｜标准 batch 对比",
        },
    ]
    budget = queue_budget_snapshot(store)
    task_ids = []
    for seed in seeds:
        category = _category_from_note(seed["worker_note"])
        if category == "baseline" and budget["baseline"] >= MAX_PENDING_BASELINE:
            continue
        if category == "validation" and budget["validation"] >= MAX_PENDING_VALIDATION:
            continue
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
        budget[category] += 1
    return task_ids


def refresh_reports() -> None:
    write_sqlite_report(db_path=DB_PATH, output_path="artifacts/sqlite_report.md")
    build_html_report(db_path=DB_PATH, output_path="artifacts/report.html")
    build_index_page(db_path=DB_PATH, output_path="artifacts/index.html")
    build_run_summary(db_path=DB_PATH, output_path="artifacts/latest_summary.txt")
    build_change_report(db_path=DB_PATH, output_path="artifacts/change_report.md")


def _enqueue_followups_for_workflow(store: ExperimentStore, task: dict[str, Any], payload: dict[str, Any]) -> list[str]:
    config_path = payload["config_path"]
    followups: list[str] = []
    budget = queue_budget_snapshot(store)
    if config_path == "configs/tushare_workflow.json" and budget["validation"] < MAX_PENDING_VALIDATION:
        cfg = json.loads(Path("configs/tushare_batch.json").read_text(encoding="utf-8"))
        fingerprint = f"batch::{config_fingerprint(cfg)}::artifacts/tushare_batch"
        followups.append(
            store.enqueue_research_task(
                task_type="batch",
                payload={"config_path": "configs/tushare_batch.json", "output_dir": "artifacts/tushare_batch"},
                priority=VALIDATION_PRIORITY,
                fingerprint=fingerprint,
                parent_task_id=task["task_id"],
                worker_note="validation｜由 workflow 完成后自动触发的 batch 对比",
            )
        )
    generated_batch_path = Path("artifacts/generated_batch_from_llm.json")
    if generated_batch_path.exists() and budget["exploration"] < MAX_PENDING_EXPLORATION:
        generated_batch = json.loads(generated_batch_path.read_text(encoding="utf-8"))
        fingerprint = f"generated_batch::{config_fingerprint(generated_batch)}::artifacts/llm_generated_batch_run"
        followups.append(
            store.enqueue_research_task(
                task_type="generated_batch",
                payload={
                    "batch_path": str(generated_batch_path),
                    "output_dir": "artifacts/llm_generated_batch_run",
                },
                priority=EXPLORATION_PRIORITY,
                fingerprint=fingerprint,
                parent_task_id=task["task_id"],
                worker_note="exploration｜执行 LLM 生成的 batch",
            )
        )
    return followups


def _enqueue_followups_for_batch(store: ExperimentStore, task: dict[str, Any], payload: dict[str, Any]) -> list[str]:
    followups: list[str] = []
    budget = queue_budget_snapshot(store)
    comparison_path = Path(payload["output_dir"]) / "batch_comparison.json"
    if not comparison_path.exists() or budget["validation"] >= MAX_PENDING_VALIDATION:
        return followups
    comparison = json.loads(comparison_path.read_text(encoding="utf-8"))
    graveyard_presence = comparison.get("graveyard_presence", {}) or {}
    candidate_presence = comparison.get("candidate_presence", {}) or {}

    diagnostic_reasons = []
    if graveyard_presence:
        consistent_graveyard = [name for name, jobs in graveyard_presence.items() if len(jobs) >= 2]
        if consistent_graveyard:
            diagnostic_reasons.append(
                f"consistent_graveyard:{', '.join(sorted(consistent_graveyard[:5]))}"
            )
    if candidate_presence:
        stable_candidates = [name for name, jobs in candidate_presence.items() if len(jobs) >= 2]
        if stable_candidates:
            diagnostic_reasons.append(
                f"stable_candidates:{', '.join(sorted(stable_candidates[:5]))}"
            )

    if diagnostic_reasons:
        fingerprint = f"diagnostic::{payload['output_dir']}::{';'.join(diagnostic_reasons)}"
        followups.append(
            store.enqueue_research_task(
                task_type="diagnostic",
                payload={
                    "diagnostic_type": "batch_consistency_review",
                    "source_output_dir": payload["output_dir"],
                    "reasons": diagnostic_reasons,
                },
                priority=VALIDATION_PRIORITY + 5,
                fingerprint=fingerprint,
                parent_task_id=task["task_id"],
                worker_note="validation｜batch 一致性诊断",
            )
        )
    return followups


def enqueue_followup_tasks(store: ExperimentStore, task: dict[str, Any]) -> list[str]:
    payload = task["payload"]
    if task["task_type"] == "workflow":
        return _enqueue_followups_for_workflow(store, task, payload)
    if task["task_type"] == "batch":
        return _enqueue_followups_for_batch(store, task, payload)
    if task["task_type"] == "generated_batch":
        return []
    if task["task_type"] == "diagnostic":
        return []
    return []


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
    if task_type == "generated_batch":
        batch_path = Path(payload["batch_path"])
        if not batch_path.exists():
            raise FileNotFoundError(f"generated batch not found: {batch_path}")
        run_batch(str(batch_path), payload["output_dir"])
        feedback = summarize_generated_batch_run(payload["output_dir"], "artifacts/llm_plan_feedback.json")
        write_bridge_status(
            "artifacts/llm_status.json",
            {
                "mode": "openclaw_agent_bridge",
                "status": "plan_executed",
                "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                "generated_batch_path": str(batch_path),
                "generated_batch_output_dir": payload["output_dir"],
                "feedback_path": "artifacts/llm_plan_feedback.json",
                "feedback_summary": feedback.get("batch_summary", []),
            },
        )
        refresh_reports()
        return f"generated batch finished: {batch_path}"
    if task_type == "diagnostic":
        output_dir = Path("artifacts") / "diagnostics"
        output_dir.mkdir(parents=True, exist_ok=True)
        diagnostic_type = payload.get("diagnostic_type", "generic")
        source_output_dir = Path(payload.get("source_output_dir", "artifacts"))
        result = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "diagnostic_type": diagnostic_type,
            "source_output_dir": str(source_output_dir),
            "reasons": payload.get("reasons", []),
        }
        if diagnostic_type == "batch_consistency_review":
            comparison_path = source_output_dir / "batch_comparison.json"
            comparison = json.loads(comparison_path.read_text(encoding="utf-8")) if comparison_path.exists() else {}
            result["candidate_presence"] = comparison.get("candidate_presence", {})
            result["graveyard_presence"] = comparison.get("graveyard_presence", {})
            result["representative_presence"] = comparison.get("representative_presence", {})
            stable_candidates = sorted(result["candidate_presence"].keys())
            repeated_graveyard = sorted(result["graveyard_presence"].keys())
            result["summary"] = {
                "stable_candidates": stable_candidates,
                "repeated_graveyard": repeated_graveyard,
                "hypothesis": "若某些因子跨窗口稳定落入 graveyard，应优先诊断 neutralization 或 split robustness，而不是继续盲目扩展。",
            }
        out_path = output_dir / f"{task['task_id']}.json"
        out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        return f"diagnostic finished: {diagnostic_type}"
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
            followups = enqueue_followup_tasks(store, task)
            note = summary + (f" | followups={len(followups)}" if followups else "")
            store.finish_research_task(task["task_id"], status="finished", worker_note=note)
            processed.append({"task_id": task["task_id"], "status": "finished", "summary": summary, "followup_task_ids": followups})
            append_heartbeat("research_orchestrator", "finished", summary=note, task_id=task["task_id"], task_type=task["task_type"])
        except Exception as exc:
            error_text = str(exc)
            retry_task_id = None
            if (task.get("attempt_count") or 0) < 2:
                retry_fingerprint = f"retry::{task['task_id']}::{task.get('attempt_count', 0)}"
                retry_task_id = store.enqueue_research_task(
                    task_type=task["task_type"],
                    payload=task["payload"],
                    priority=RETRY_PRIORITY,
                    fingerprint=retry_fingerprint,
                    parent_task_id=task["task_id"],
                    worker_note=f"retry｜自动重试 {task['task_type']}",
                )
            store.finish_research_task(task["task_id"], status="failed", last_error=error_text)
            processed.append({"task_id": task["task_id"], "status": "failed", "error": error_text, "retry_task_id": retry_task_id})
            append_heartbeat("research_orchestrator", "failed", message=error_text, task_id=task["task_id"], task_type=task["task_type"], retry_task_id=retry_task_id)

    if not processed:
        append_heartbeat("research_orchestrator", "idle", summary="no pending research tasks")
    return {
        "processed": processed,
        "remaining_preview": store.list_research_tasks(limit=10),
    }

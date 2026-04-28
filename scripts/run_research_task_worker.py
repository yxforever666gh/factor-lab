from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPT_ROOT / "src"))

from factor_lab.agent_runtime_hooks import safe_run_data_quality_review
from factor_lab.batch import run_batch
from factor_lab.paths import artifacts_dir, project_root
from factor_lab.research_queue import request_report_refresh, validate_generated_batch_payload
from factor_lab.workflow import run_workflow
from factor_lab.llm_bridge import write_bridge_status
from factor_lab.llm_feedback import summarize_generated_batch_run


def _root_path() -> Path:
    return project_root()


def _artifacts_path() -> Path:
    return artifacts_dir()


def _src_path() -> Path:
    return _root_path() / "src"


def _feedback_path() -> Path:
    return _artifacts_path() / "llm_plan_feedback.json"


def _bridge_status_path() -> Path:
    return _artifacts_path() / "llm_status.json"


def schedule_report_refresh(*, source: str) -> tuple[bool, str | None]:
    mode = (os.getenv("RESEARCH_REPORT_REFRESH_MODE") or "deferred").strip().lower()
    if mode == "sync":
        timeout_seconds = int(os.getenv("RESEARCH_REPORT_REFRESH_TIMEOUT_SECONDS", "30"))
        root = _root_path()
        src = _src_path()
        command = [
            sys.executable,
            "-c",
            (
                "import sys; "
                f"sys.path.insert(0, {str(src)!r}); "
                "from factor_lab.research_queue import refresh_reports; "
                "print('1' if refresh_reports() else '0')"
            ),
        ]
        try:
            result = subprocess.run(
                command,
                cwd=root,
                capture_output=True,
                text=True,
                timeout=(timeout_seconds if timeout_seconds > 0 else None),
                check=False,
            )
        except subprocess.TimeoutExpired:
            return False, f"report refresh timeout after {timeout_seconds}s"
        except Exception as exc:
            return False, f"report refresh failed: {exc}"

        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "").strip()
            return False, f"report refresh failed: {detail or f'code {result.returncode}'}"

        refreshed = (result.stdout or "").strip().endswith("1")
        return refreshed, None

    request_report_refresh(source=source, reason="task_completed")
    return False, "reports_refresh=deferred"


def _observation_decision_provider() -> str:
    return (
        os.getenv("FACTOR_LAB_OBSERVATION_DECISION_PROVIDER")
        or os.getenv("FACTOR_LAB_DECISION_PROVIDER")
        or os.getenv("FACTOR_LAB_LLM_PROVIDER")
        or "heuristic"
    ).strip().lower()


def _latest_workflow_run_summary() -> dict:
    db_path = _artifacts_path() / "factor_lab.db"
    if not db_path.exists():
        return {}
    try:
        import sqlite3
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute(
                "SELECT run_id, created_at_utc, config_path, data_source, start_date, end_date, status, dataset_rows, factor_count, output_dir FROM workflow_runs ORDER BY created_at_utc DESC LIMIT 1"
            ).fetchone()
            return dict(row) if row else {}
        finally:
            conn.close()
    except Exception:
        return {}


def _run_data_quality_hook(*, task_id: str | None, task_type: str, payload: dict, last_error: str | None = None) -> None:
    latest_run = _latest_workflow_run_summary()
    context = {
        "context_id": f"data-quality:{task_id or task_type}",
        "inputs": {
            "task_type": task_type,
            "task_payload_summary": {
                "config_path": payload.get("config_path"),
                "output_dir": payload.get("output_dir"),
            },
            "latest_run": latest_run,
            "last_error": last_error or "",
        },
    }
    safe_run_data_quality_review(
        context=context,
        output_path=_artifacts_path() / "data_quality_review.json",
        provider=_observation_decision_provider(),
    )


def main() -> int:
    if len(sys.argv) != 2:
        print(json.dumps({"ok": False, "error": "usage: run_research_task_worker.py <task-json>"}, ensure_ascii=False))
        return 2

    task = json.loads(sys.argv[1])
    payload = task["payload"]
    task_type = task["task_type"]
    task_id = task.get("task_id")

    try:
        if task_type == "workflow":
            run_workflow(config_path=payload["config_path"], output_dir=payload["output_dir"])
            _run_data_quality_hook(task_id=task_id, task_type=task_type, payload=payload)
            refreshed, refresh_note = schedule_report_refresh(source="workflow")
            summary = f"workflow finished: {payload['config_path']}"
            if refresh_note:
                summary += f" | {refresh_note}"
            elif not refreshed:
                summary += " | reports_refresh=skipped"
            print(json.dumps({"ok": True, "summary": summary}, ensure_ascii=False))
            return 0
    except Exception as exc:
        _run_data_quality_hook(task_id=task_id, task_type=task_type, payload=payload, last_error=str(exc))
        raise

    if task_type == "batch":
        try:
            run_batch(config_path=payload["config_path"], output_dir=payload["output_dir"])
            _run_data_quality_hook(task_id=task_id, task_type=task_type, payload=payload)
            refreshed, refresh_note = schedule_report_refresh(source="batch")
            summary = f"batch finished: {payload['config_path']}"
            if refresh_note:
                summary += f" | {refresh_note}"
            elif not refreshed:
                summary += " | reports_refresh=skipped"
            print(json.dumps({"ok": True, "summary": summary}, ensure_ascii=False))
            return 0
        except Exception as exc:
            _run_data_quality_hook(task_id=task_id, task_type=task_type, payload=payload, last_error=str(exc))
            raise

    if task_type == "generated_batch":
        try:
            ok, validation_error = validate_generated_batch_payload(task)
            if not ok:
                _run_data_quality_hook(task_id=task_id, task_type=task_type, payload=payload, last_error=validation_error or "generated batch preflight failed")
                print(json.dumps({"ok": False, "error": validation_error or "generated batch preflight failed"}, ensure_ascii=False))
                return 1
            batch_path = Path(payload["batch_path"])
            feedback_path = _feedback_path()
            bridge_status_path = _bridge_status_path()
            run_batch(str(batch_path), payload["output_dir"])
            _run_data_quality_hook(task_id=task_id, task_type=task_type, payload=payload)
            feedback = summarize_generated_batch_run(payload["output_dir"], str(feedback_path))
            batch_summary = feedback.get("batch_summary", []) or []
            knowledge_gain = []
            if any((row.get("candidate_count") or 0) > 0 for row in batch_summary):
                knowledge_gain.append("exploration_candidate_survived")
            if any((row.get("graveyard_count") or 0) > 0 for row in batch_summary):
                knowledge_gain.append("exploration_graveyard_identified")
            if not knowledge_gain:
                knowledge_gain.append("no_significant_information_gain")
            write_bridge_status(
                str(bridge_status_path),
                {
                    "mode": "openclaw_agent_bridge",
                    "status": "plan_executed",
                    "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                    "generated_batch_path": str(batch_path),
                    "generated_batch_output_dir": payload["output_dir"],
                    "feedback_path": str(feedback_path),
                    "feedback_summary": feedback.get("batch_summary", []),
                    "knowledge_gain": knowledge_gain,
                },
            )
            refreshed, refresh_note = schedule_report_refresh(source="generated_batch")
            summary = f"generated batch finished: {batch_path} | knowledge_gain={','.join(knowledge_gain)}"
            if refresh_note:
                summary += f" | {refresh_note}"
            elif not refreshed:
                summary += " | reports_refresh=skipped"
            print(json.dumps({"ok": True, "summary": summary}, ensure_ascii=False))
            return 0
        except Exception as exc:
            _run_data_quality_hook(task_id=task_id, task_type=task_type, payload=payload, last_error=str(exc))
            raise

    print(json.dumps({"ok": False, "error": f"unsupported task_type: {task_type}"}, ensure_ascii=False))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

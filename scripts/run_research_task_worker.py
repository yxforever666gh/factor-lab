from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from factor_lab.batch import run_batch
from factor_lab.research_queue import validate_generated_batch_payload
from factor_lab.workflow import run_workflow
from factor_lab.llm_bridge import write_bridge_status
from factor_lab.llm_feedback import summarize_generated_batch_run


def safe_refresh_reports() -> tuple[bool, str | None]:
    timeout_seconds = int(os.getenv("RESEARCH_REPORT_REFRESH_TIMEOUT_SECONDS", "30"))
    command = [
        sys.executable,
        "-c",
        (
            "import sys; "
            f"sys.path.insert(0, {str(ROOT / 'src')!r}); "
            "from factor_lab.research_queue import refresh_reports; "
            "print('1' if refresh_reports() else '0')"
        ),
    ]
    try:
        result = subprocess.run(
            command,
            cwd=ROOT,
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


def main() -> int:
    if len(sys.argv) != 2:
        print(json.dumps({"ok": False, "error": "usage: run_research_task_worker.py <task-json>"}, ensure_ascii=False))
        return 2

    task = json.loads(sys.argv[1])
    payload = task["payload"]
    task_type = task["task_type"]

    if task_type == "workflow":
        run_workflow(config_path=payload["config_path"], output_dir=payload["output_dir"])
        refreshed, refresh_note = safe_refresh_reports()
        summary = f"workflow finished: {payload['config_path']}"
        if refresh_note:
            summary += f" | {refresh_note}"
        elif not refreshed:
            summary += " | reports_refresh=skipped"
        print(json.dumps({"ok": True, "summary": summary}, ensure_ascii=False))
        return 0

    if task_type == "batch":
        run_batch(config_path=payload["config_path"], output_dir=payload["output_dir"])
        refreshed, refresh_note = safe_refresh_reports()
        summary = f"batch finished: {payload['config_path']}"
        if refresh_note:
            summary += f" | {refresh_note}"
        elif not refreshed:
            summary += " | reports_refresh=skipped"
        print(json.dumps({"ok": True, "summary": summary}, ensure_ascii=False))
        return 0

    if task_type == "generated_batch":
        ok, validation_error = validate_generated_batch_payload(task)
        if not ok:
            print(json.dumps({"ok": False, "error": validation_error or "generated batch preflight failed"}, ensure_ascii=False))
            return 1
        batch_path = Path(payload["batch_path"])
        run_batch(str(batch_path), payload["output_dir"])
        feedback = summarize_generated_batch_run(payload["output_dir"], "artifacts/llm_plan_feedback.json")
        batch_summary = feedback.get("batch_summary", []) or []
        knowledge_gain = []
        if any((row.get("candidate_count") or 0) > 0 for row in batch_summary):
            knowledge_gain.append("exploration_candidate_survived")
        if any((row.get("graveyard_count") or 0) > 0 for row in batch_summary):
            knowledge_gain.append("exploration_graveyard_identified")
        if not knowledge_gain:
            knowledge_gain.append("no_significant_information_gain")
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
                "knowledge_gain": knowledge_gain,
            },
        )
        refreshed, refresh_note = safe_refresh_reports()
        summary = f"generated batch finished: {batch_path} | knowledge_gain={','.join(knowledge_gain)}"
        if refresh_note:
            summary += f" | {refresh_note}"
        elif not refreshed:
            summary += " | reports_refresh=skipped"
        print(json.dumps({"ok": True, "summary": summary}, ensure_ascii=False))
        return 0

    print(json.dumps({"ok": False, "error": f"unsupported task_type: {task_type}"}, ensure_ascii=False))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

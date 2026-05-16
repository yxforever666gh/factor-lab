import json
import subprocess
import sys
from pathlib import Path


def test_run_research_task_worker_blocks_unadmitted_workflow_before_execution(tmp_path):
    cfg = {"factors": [{"name": "x", "expression": "book_yield"}]}
    cfg_path = tmp_path / "legacy.json"
    cfg_path.write_text(json.dumps(cfg))
    task = {"task_id": "t1", "task_type": "workflow", "payload": {"config_path": str(cfg_path), "output_dir": str(tmp_path / "out")}}

    result = subprocess.run(
        [sys.executable, "scripts/run_research_task_worker.py", json.dumps(task)],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert result.returncode == 1
    payload = json.loads(result.stdout.strip().splitlines()[-1])
    assert payload["ok"] is False
    assert payload["error"] == "workflow_admission_blocked"
    assert "missing_mechanism_id" in payload["reasons"]

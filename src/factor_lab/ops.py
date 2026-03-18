from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]


def latest_task_states(limit: int = 10) -> list[dict[str, Any]]:
    tasks = []
    for path in ROOT.glob("artifacts/**/task_state.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["task_file"] = str(path)
            tasks.append(payload)
        except Exception:
            continue
    tasks.sort(key=lambda item: item.get("started_at_utc", ""), reverse=True)
    return tasks[:limit]


def trigger_script(script_relative_path: str) -> dict[str, Any]:
    script_path = ROOT / script_relative_path
    result = subprocess.run(
        ["python3", str(script_path)],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    return {
        "script": script_relative_path,
        "returncode": result.returncode,
        "stdout": result.stdout[-4000:],
        "stderr": result.stderr[-4000:],
    }

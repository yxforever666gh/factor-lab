from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from factor_lab.storage import ExperimentStore


DB_PATH = Path("artifacts") / "factor_lab.db"


def inject_research_planner_tasks(validated_path: str | Path, output_path: str | Path) -> dict[str, Any]:
    validated = json.loads(Path(validated_path).read_text(encoding="utf-8"))
    accepted = validated.get("accepted_tasks", []) or []
    store = ExperimentStore(DB_PATH)

    injected = []
    for task in accepted:
        task_id = store.enqueue_research_task(
            task_type=task["task_type"],
            payload=task["payload"],
            priority=int(task.get("priority_hint", 50)),
            fingerprint=task.get("fingerprint"),
            worker_note=(task.get("worker_note") or "") + "｜planner_selected",
        )
        injected.append({"task_id": task_id, "fingerprint": task.get("fingerprint"), "category": task.get("category")})

    payload = {
        "generated_from_validated": str(validated_path),
        "injected_count": len(injected),
        "injected_tasks": injected,
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload

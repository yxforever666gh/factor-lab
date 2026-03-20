from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from factor_lab.opportunity_to_tasks import map_opportunity_to_task
from factor_lab.storage import ExperimentStore
from factor_lab.research_runtime_state import recently_finished_same_fingerprint

ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "artifacts" / "factor_lab.db"


def enqueue_opportunities(opportunities_path: str | Path, output_path: str | Path, db_path: str | Path = DB_PATH, limit: int = 2) -> dict[str, Any]:
    opportunities_doc = json.loads(Path(opportunities_path).read_text(encoding="utf-8")) if Path(opportunities_path).exists() else {}
    opportunities = list(opportunities_doc.get("opportunities") or [])
    store = ExperimentStore(db_path)

    injected = []
    skipped = []
    considered = 0
    for opportunity in opportunities:
        if considered >= limit:
            break
        task = map_opportunity_to_task(opportunity)
        if not task:
            skipped.append({"opportunity_id": opportunity.get("opportunity_id"), "reason": "unmappable"})
            continue
        considered += 1
        fingerprint = task.get("fingerprint")
        if fingerprint and recently_finished_same_fingerprint(store, fingerprint):
            skipped.append({"opportunity_id": opportunity.get("opportunity_id"), "reason": "recently_finished_same_fingerprint"})
            continue
        task_id = store.enqueue_research_task(
            task_type=task["task_type"],
            payload=task["payload"],
            priority=int(task.get("priority") or 50),
            fingerprint=fingerprint,
            worker_note=task.get("worker_note"),
        )
        injected.append({
            "opportunity_id": opportunity.get("opportunity_id"),
            "task_id": task_id,
            "task_type": task.get("task_type"),
            "priority": task.get("priority"),
        })

    payload = {
        "source": str(opportunities_path),
        "considered": considered,
        "injected_count": len(injected),
        "injected": injected,
        "skipped": skipped,
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload

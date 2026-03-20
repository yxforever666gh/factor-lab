from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from factor_lab.opportunity_to_tasks import map_opportunity_to_task
from factor_lab.opportunity_store import sync_opportunities, update_opportunity_state
from factor_lab.opportunity_policy import should_bypass_recent_fingerprint
from factor_lab.opportunity_diagnostics import build_opportunity_review
from factor_lab.storage import ExperimentStore
from factor_lab.research_runtime_state import recently_finished_same_fingerprint

ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "artifacts" / "factor_lab.db"


def enqueue_opportunities(opportunities_path: str | Path, output_path: str | Path, db_path: str | Path = DB_PATH, limit: int = 2) -> dict[str, Any]:
    opportunities_doc = json.loads(Path(opportunities_path).read_text(encoding="utf-8")) if Path(opportunities_path).exists() else {}
    opportunities = list(opportunities_doc.get("opportunities") or [])
    sync_opportunities(opportunities)
    review = build_opportunity_review()
    blocks = review.get("blocks") or {}
    downweights = review.get("downweights") or {}
    store = ExperimentStore(db_path)

    injected = []
    skipped = []
    considered = 0
    for opportunity in opportunities:
        if considered >= limit:
            break

        oid = opportunity.get("opportunity_id")
        if oid in blocks:
            skipped.append({"opportunity_id": oid, "reason": f"blocked:{blocks[oid].get('reason')}"})
            update_opportunity_state(oid, "rejected", reason=f"blocked:{blocks[oid].get('reason')}")
            continue

        if oid in downweights:
            opportunity = {
                **opportunity,
                "priority": max(0.05, float(opportunity.get("priority") or 0.5) - float(downweights[oid].get("delta") or 0.0)),
            }

        task = map_opportunity_to_task(opportunity)
        if not task:
            skipped.append({"opportunity_id": oid, "reason": "unmappable"})
            update_opportunity_state(oid, "rejected", reason="unmappable")
            continue

        considered += 1
        fingerprint = task.get("fingerprint")
        bypass = should_bypass_recent_fingerprint(opportunity)
        if fingerprint and recently_finished_same_fingerprint(store, fingerprint) and not bypass.get("allow_bypass"):
            skipped.append({"opportunity_id": oid, "reason": "recently_finished_same_fingerprint"})
            update_opportunity_state(oid, "archived", reason="recently_finished_same_fingerprint")
            continue

        if bypass.get("allow_bypass"):
            task["payload"]["dedupe_bypass"] = True
            task["payload"]["dedupe_bypass_reason"] = bypass.get("reason")

        task_id = store.enqueue_research_task(
            task_type=task["task_type"],
            payload=task["payload"],
            priority=int(task.get("priority") or 50),
            fingerprint=fingerprint,
            worker_note=task.get("worker_note"),
        )
        injected.append({
            "opportunity_id": oid,
            "task_id": task_id,
            "task_type": task.get("task_type"),
            "priority": task.get("priority"),
            "dedupe_bypass": bool(bypass.get("allow_bypass")),
        })
        update_opportunity_state(oid, "scheduled", reason="task_enqueued", extra={"task_id": task_id, "task_type": task.get("task_type")})

    payload = {
        "source": str(opportunities_path),
        "considered": considered,
        "injected_count": len(injected),
        "injected": injected,
        "skipped": skipped,
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload

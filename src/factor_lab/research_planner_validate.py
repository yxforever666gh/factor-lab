from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from factor_lab.research_queue import recently_finished_same_fingerprint
from factor_lab.storage import ExperimentStore


CATEGORY_LIMITS_DEFAULT = {"baseline": 2, "validation": 2, "exploration": 1}
DB_PATH = Path("artifacts") / "factor_lab.db"


def validate_research_planner_proposal(proposal_path: str | Path, output_path: str | Path) -> dict[str, Any]:
    proposal = json.loads(Path(proposal_path).read_text(encoding="utf-8"))
    selected_tasks = proposal.get("selected_tasks", []) or []
    store = ExperimentStore(DB_PATH)

    category_limits = (proposal.get("selection_policy") or {}).get("category_limits") or CATEGORY_LIMITS_DEFAULT
    counts = {"baseline": 0, "validation": 0, "exploration": 0}
    accepted = []
    rejected = []

    for task in selected_tasks:
        category = task.get("category", "validation")
        fingerprint = task.get("fingerprint")
        reason = []
        ok = True

        if category in counts and counts[category] >= category_limits.get(category, 99):
            ok = False
            reason.append(f"category_limit_exceeded:{category}")

        if fingerprint and recently_finished_same_fingerprint(store, fingerprint):
            ok = False
            reason.append("recently_finished_same_fingerprint")

        if fingerprint and any(item.get("fingerprint") == fingerprint for item in accepted):
            ok = False
            reason.append("duplicate_within_plan")

        if ok:
            accepted.append(task)
            if category in counts:
                counts[category] += 1
        else:
            rejected.append({**task, "validation_reasons": reason})

    payload = {
        "generated_from_proposal": str(proposal_path),
        "summary": {
            "accepted_count": len(accepted),
            "rejected_count": len(rejected),
            "category_counts": counts,
        },
        "accepted_tasks": accepted,
        "rejected_tasks": rejected,
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload

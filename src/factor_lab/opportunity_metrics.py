from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "artifacts"
STORE_PATH = ARTIFACTS / "research_opportunity_store.json"
OUTPUT_PATH = ARTIFACTS / "opportunity_metrics.json"


def build_opportunity_metrics(store_path: str | Path | None = None, output_path: str | Path | None = None) -> dict[str, Any]:
    spath = Path(store_path) if store_path else STORE_PATH
    opath = Path(output_path) if output_path else OUTPUT_PATH
    store = json.loads(spath.read_text(encoding="utf-8")) if spath.exists() else {"opportunities": {}}
    items = list((store.get("opportunities") or {}).values())

    total = len(items)
    promoted = len([row for row in items if row.get("state") == "promoted"])
    evaluated = len([row for row in items if row.get("state") == "evaluated"])
    rejected = len([row for row in items if row.get("state") == "rejected"])
    archived = len([row for row in items if row.get("state") == "archived"])
    child_count = len([row for row in items if row.get("parent_opportunity_id")])
    with_evaluation = [row for row in items if row.get("evaluation")]
    high_gain = len([row for row in with_evaluation if (row.get("evaluation") or {}).get("evaluation_label") == "high_gain"])

    success_rate = round(promoted / total, 3) if total else None
    knowledge_gain_rate = round(high_gain / len(with_evaluation), 3) if with_evaluation else None
    branch_growth_rate = round(child_count / total, 3) if total else None

    payload = {
        "counts": {
            "total": total,
            "promoted": promoted,
            "evaluated": evaluated,
            "rejected": rejected,
            "archived": archived,
            "child_count": child_count,
        },
        "rates": {
            "success_rate": success_rate,
            "knowledge_gain_rate": knowledge_gain_rate,
            "branch_growth_rate": branch_growth_rate,
        },
    }
    opath.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload

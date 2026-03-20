from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "artifacts"
STORE_PATH = ARTIFACTS / "research_opportunity_store.json"
OUTPUT_PATH = ARTIFACTS / "opportunity_review.json"


def build_opportunity_review() -> dict[str, Any]:
    store = json.loads(STORE_PATH.read_text(encoding="utf-8")) if STORE_PATH.exists() else {"opportunities": {}}
    items = list((store.get("opportunities") or {}).values())
    review = {
        "challenger": [],
        "auditor": [],
    }
    for row in items[:20]:
        oid = row.get("opportunity_id")
        if not oid:
            continue
        if (row.get("novelty_score") or 0) < 0.5:
            review["challenger"].append(f"{oid}: 新颖度偏低，可能仍在重复旧研究问题。")
        if row.get("state") == "archived":
            review["auditor"].append(f"{oid}: 已被归档，需检查是否被去重规则过度压制。")
        if (row.get("confidence") or 0) < 0.6:
            review["auditor"].append(f"{oid}: 置信度偏低，进入执行前应谨慎。")
    OUTPUT_PATH.write_text(json.dumps(review, ensure_ascii=False, indent=2), encoding="utf-8")
    return review

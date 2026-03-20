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
        "blocks": {},
        "downweights": {},
    }
    for row in items[:50]:
        oid = row.get("opportunity_id")
        if not oid:
            continue
        novelty = float(row.get("novelty_score") or 0.0)
        confidence = float(row.get("confidence") or 0.0)
        state = row.get("state")
        if novelty < 0.5:
            msg = f"{oid}: 新颖度偏低，可能仍在重复旧研究问题。"
            review["challenger"].append(msg)
            review["downweights"][oid] = {"reason": "low_novelty", "delta": 0.08}
        if state == "archived":
            msg = f"{oid}: 已被归档，需检查是否被去重规则过度压制。"
            review["auditor"].append(msg)
        if confidence < 0.6:
            msg = f"{oid}: 置信度偏低，进入执行前应谨慎。"
            review["auditor"].append(msg)
            review["blocks"][oid] = {"reason": "low_confidence"}
    OUTPUT_PATH.write_text(json.dumps(review, ensure_ascii=False, indent=2), encoding="utf-8")
    return review

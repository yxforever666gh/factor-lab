from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "artifacts"
STORE_PATH = ARTIFACTS / "research_opportunity_store.json"
OUTPUT_PATH = ARTIFACTS / "opportunity_learning.json"


def build_opportunity_learning(store_path: str | Path | None = None, output_path: str | Path | None = None) -> dict[str, Any]:
    spath = Path(store_path) if store_path else STORE_PATH
    opath = Path(output_path) if output_path else OUTPUT_PATH
    store = json.loads(spath.read_text(encoding="utf-8")) if spath.exists() else {"opportunities": {}}
    items = list((store.get("opportunities") or {}).values())

    types: dict[str, dict[str, Any]] = {}
    for row in items:
        otype = row.get("opportunity_type") or "unknown"
        meta = types.setdefault(otype, {
            "opportunity_type": otype,
            "count": 0,
            "promoted": 0,
            "evaluated": 0,
            "rejected": 0,
            "archived": 0,
            "success_rate": None,
            "recommended_action": "keep",
        })
        meta["count"] += 1
        state = row.get("state")
        if state in meta:
            meta[state] += 1
        if state == "promoted":
            meta["promoted"] += 1
        elif state == "evaluated":
            meta["evaluated"] += 1
        elif state == "rejected":
            meta["rejected"] += 1
        elif state == "archived":
            meta["archived"] += 1

    for meta in types.values():
        terminal = meta["promoted"] + meta["evaluated"] + meta["rejected"] + meta["archived"]
        if terminal > 0:
            meta["success_rate"] = round(meta["promoted"] / terminal, 3)
        if meta["success_rate"] is None:
            meta["recommended_action"] = "keep"
        elif meta["success_rate"] >= 0.5:
            meta["recommended_action"] = "upweight"
        elif meta["rejected"] + meta["archived"] >= max(2, meta["promoted"] + meta["evaluated"]):
            meta["recommended_action"] = "downweight"
        else:
            meta["recommended_action"] = "keep"

    payload = {"types": types}
    opath.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload

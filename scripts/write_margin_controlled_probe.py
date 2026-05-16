#!/usr/bin/env python3
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from factor_lab.margin_controlled_probe import build_margin_controlled_probe, to_markdown

ARTIFACT_DIR = Path("artifacts/margin_controlled_probe")
JSON_OUT = ARTIFACT_DIR / "margin_controlled_probe.json"
MD_OUT = ARTIFACT_DIR / "margin_controlled_probe.md"
KNOWLEDGE_OUT = Path("knowledge/margin_controlled_probe.md")


def main() -> None:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    payload = build_margin_controlled_probe()
    payload.update({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "no_queue_write": True,
        "no_daemon_start": True,
        "no_broad_search": True,
    })
    JSON_OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md = to_markdown(payload)
    MD_OUT.write_text(md, encoding="utf-8")
    KNOWLEDGE_OUT.parent.mkdir(parents=True, exist_ok=True)
    KNOWLEDGE_OUT.write_text(md, encoding="utf-8")
    print(json.dumps({
        "json": str(JSON_OUT),
        "markdown": str(MD_OUT),
        "knowledge": str(KNOWLEDGE_OUT),
        "decision": payload.get("decision"),
        "coverage": payload.get("coverage"),
        "holdout": payload.get("holdout"),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

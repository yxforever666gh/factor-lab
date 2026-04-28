from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
STORE_PATH = ROOT / "artifacts" / "research_opportunity_store.json"


def build_full_run_followups(limit: int = 3) -> list[dict[str, Any]]:
    if not STORE_PATH.exists():
        return []
    store = json.loads(STORE_PATH.read_text(encoding="utf-8"))
    items = list((store.get("opportunities") or {}).values())
    followups: list[dict[str, Any]] = []
    promotable_labels = {"probe_promising", "boundary_confirmed", "new_branch_opened", "moderate_gain", "high_gain"}
    for row in items:
        evaluation = row.get("evaluation") or {}
        if not (evaluation.get("full_run_recommended") or evaluation.get("evaluation_label") in promotable_labels):
            continue
        oid = row.get("opportunity_id") or "opportunity"
        followups.append({
            "question_id": f"full-run-followup-{oid}",
            "question_type": row.get("opportunity_type") or "probe",
            "question": f"{row.get('question')}（cheap screen 已显示信号，是否升级完整验证？）",
            "hypothesis": f"{row.get('hypothesis') or '该机会'} 在 cheap screen 显示出足够信号，值得升级 full run。",
            "target_family": row.get("target_family"),
            "target_candidates": list(row.get("target_candidates") or []),
            "expected_knowledge_gain": list(row.get("expected_knowledge_gain") or []),
            "evidence_gap": "cheap screen 已显示正向信号，但尚未执行 full validation。",
            "sources": ["cheap_screen_promotion", oid],
            "origin": "cheap_screen_promotion",
            "parent_opportunity_id": oid,
            "execution_mode": "full",
        })
    return followups[:limit]

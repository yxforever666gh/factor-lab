from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


WINDOW_KEYWORDS = {
    "rolling_30d_back": "window_rolling_30d_back",
    "rolling_60d_back": "window_rolling_60d_back",
    "rolling_120d_back": "window_rolling_120d_back",
    "rolling_180d_back": "window_rolling_180d_back",
    "rolling_240d_back": "window_rolling_240d_back",
    "rolling_300d_back": "window_rolling_300d_back",
    "rolling_360d_back": "window_rolling_360d_back",
    "rolling_420d_back": "window_rolling_420d_back",
    "rolling_recent_45d": "window_recent_45d",
    "rolling_recent_90d": "window_recent_90d",
    "rolling_recent_120d": "window_recent_120d",
    "rolling_recent_150d": "window_recent_150d",
    "rolling_recent_180d": "window_recent_180d",
    "rolling_recent_210d": "window_recent_210d",
    "rolling_recent_240d": "window_recent_240d",
    "rolling_recent_270d": "window_recent_270d",
    "expanding_from_2025_10_01": "window_expanding_2025_10_01",
    "expanding_from_2025_07_01": "window_expanding_2025_07_01",
    "expanding_from_2025_04_01": "window_expanding_2025_04_01",
    "expanding_from_2025_01_01": "window_expanding_2025_01_01",
    "expanding_from_2024_10_01": "window_expanding_2024_10_01",
    "expanding_from_2024_07_01": "window_expanding_2024_07_01",
}

DIAGNOSTIC_LEVELS = {
    "batch_consistency_review": 1,
    "graveyard_neutralization_review": 2,
    "graveyard_window_sensitivity_review": 2,
    "graveyard_raw_vs_neutral_review": 3,
    "graveyard_construction_review": 4,
    "graveyard_cross_window_review": 5,
    "graveyard_regime_shift_review": 6,
    "stable_candidate_validation_review": 2,
    "stable_candidate_validation_review_v2": 3,
    "stable_candidate_validation_review_v3": 4,
    "stable_candidate_validation_review_v4": 5,
    "stable_candidate_validation_review_v5": 6,
}


def build_research_space_registry(db_path: str | Path, output_path: str | Path) -> dict[str, Any]:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    try:
        tasks = cur.execute(
            """
            SELECT task_id, task_type, worker_note, payload_json, status, created_at_utc, finished_at_utc
            FROM research_tasks
            ORDER BY created_at_utc DESC
            LIMIT 500
            """
        ).fetchall()

        windows_covered: dict[str, dict[str, Any]] = {}
        validation_depth: dict[str, int] = {}
        graveyard_diagnostics: dict[str, int] = {}
        exploration_lines: dict[str, dict[str, Any]] = {}
        knowledge_gain_counter: dict[str, int] = {}

        for task_id, task_type, worker_note, payload_json, status, created_at_utc, finished_at_utc in tasks:
            payload = json.loads(payload_json) if payload_json else {}
            note = worker_note or ""

            if task_type == "workflow":
                config_path = payload.get("config_path", "")
                for keyword, window_id in WINDOW_KEYWORDS.items():
                    if keyword in config_path:
                        windows_covered[window_id] = {
                            "task_id": task_id,
                            "config_path": config_path,
                            "status": status,
                            "finished_at_utc": finished_at_utc,
                        }

            if task_type == "diagnostic":
                diagnostic_type = payload.get("diagnostic_type")
                level = DIAGNOSTIC_LEVELS.get(diagnostic_type, 1)
                focus_factors = payload.get("focus_factors") or payload.get("reasons") or [diagnostic_type]
                key = ",".join(sorted(str(x) for x in focus_factors))
                if "stable_candidate" in (diagnostic_type or ""):
                    validation_depth[key] = max(validation_depth.get(key, 0), level)
                else:
                    graveyard_diagnostics[key] = max(graveyard_diagnostics.get(key, 0), level)

            if task_type == "generated_batch":
                line_key = payload.get("batch_path", "generated_batch")
                exploration_lines[line_key] = {
                    "task_id": task_id,
                    "status": status,
                    "finished_at_utc": finished_at_utc,
                    "worker_note": note,
                }

            gains = [g for g in (payload.get("knowledge_gain") or []) if g]
            if "knowledge_gain=" in note:
                gains.extend([x.strip() for x in note.split("knowledge_gain=", 1)[-1].split(",") if x.strip()])
            for gain in gains:
                knowledge_gain_counter[gain] = knowledge_gain_counter.get(gain, 0) + 1

        payload = {
            "windows_covered": windows_covered,
            "validation_depth": validation_depth,
            "graveyard_diagnostics": graveyard_diagnostics,
            "exploration_lines": exploration_lines,
            "knowledge_gain_counter": knowledge_gain_counter,
        }
        Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload
    finally:
        conn.close()

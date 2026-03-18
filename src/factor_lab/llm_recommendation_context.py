from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def build_recommendation_context(
    weights_path: str | Path,
    history_path: str | Path,
    output_path: str | Path,
) -> dict[str, Any]:
    weights = json.loads(Path(weights_path).read_text(encoding="utf-8")) if Path(weights_path).exists() else {}
    history = json.loads(Path(history_path).read_text(encoding="utf-8")) if Path(history_path).exists() else []

    templates = weights.get("templates", {})
    priority_summary = []
    fatigue = {}

    recent_tail = history[-5:]
    recent_counts = {}
    for row in recent_tail:
        recent_counts[row["template_type"]] = recent_counts.get(row["template_type"], 0) + 1

    for template_type, meta in templates.items():
        count = recent_counts.get(template_type, 0)
        fatigue_level = "high" if count >= 3 else "medium" if count == 2 else "low"
        fatigue[template_type] = {
            "recent_count_last_5": count,
            "fatigue_level": fatigue_level,
        }
        priority_summary.append(
            {
                "template_type": template_type,
                "recommended_action": meta.get("recommended_action"),
                "avg_effect_score": meta.get("avg_effect_score"),
                "decayed_effect_score": meta.get("decayed_effect_score"),
                "sample_count": meta.get("sample_count"),
                "fatigue_level": fatigue_level,
            }
        )

    priority_summary.sort(
        key=lambda item: (
            {"upweight": 4, "soft_upweight": 3, "keep": 2, "soft_downweight": 1, "downweight": 0}.get(item["recommended_action"], 2),
            item.get("decayed_effect_score") or 0,
        ),
        reverse=True,
    )

    payload = {
        "priority_summary": priority_summary,
        "fatigue": fatigue,
        "planner_hint": "优先尝试 higher priority 且 fatigue_level 低的模板；对 fatigue_level 高的模板，除非出现新信息，否则避免连续重复。",
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


SCORE_MAP = {"positive": 1.0, "neutral": 0.0, "negative": -1.0}


def build_recommendation_weights(history_path: str | Path, output_path: str | Path) -> dict[str, Any]:
    history = json.loads(Path(history_path).read_text(encoding="utf-8")) if Path(history_path).exists() else []
    grouped: dict[str, list[float]] = {}
    for row in history:
        grouped.setdefault(row["template_type"], []).append(SCORE_MAP.get(row.get("effectiveness", "neutral"), 0.0))

    weights = {}
    for template_type, values in grouped.items():
        avg = sum(values) / len(values) if values else 0.0
        if avg > 0.25:
            action = "upweight"
        elif avg < -0.25:
            action = "downweight"
        else:
            action = "keep"
        weights[template_type] = {
            "avg_effect_score": round(avg, 6),
            "sample_count": len(values),
            "recommended_action": action,
        }

    payload = {
        "templates": weights,
        "global_hint": "优先考虑历史上 avg_effect_score 更高的建议模板。",
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload

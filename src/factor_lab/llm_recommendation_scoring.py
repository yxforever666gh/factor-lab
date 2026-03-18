from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCORE_MAP = {"positive": 1.0, "neutral": 0.0, "negative": -1.0}


def _parse_time(ts: str | None):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def _recency_weight(recorded_at: str | None, now: datetime, half_life_days: float) -> float:
    dt = _parse_time(recorded_at)
    if dt is None:
        return 1.0
    age_days = max(0.0, (now - dt).total_seconds() / 86400.0)
    if half_life_days <= 0:
        return 1.0
    return 0.5 ** (age_days / half_life_days)


def build_recommendation_weights(
    history_path: str | Path,
    output_path: str | Path,
    min_samples_for_hard_weight: int = 3,
    half_life_days: float = 14.0,
) -> dict[str, Any]:
    history = json.loads(Path(history_path).read_text(encoding="utf-8")) if Path(history_path).exists() else []
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in history:
        grouped.setdefault(row["template_type"], []).append(row)

    now = datetime.now(timezone.utc)
    weights = {}
    for template_type, rows in grouped.items():
        weighted_sum = 0.0
        weight_sum = 0.0
        raw_values = []
        for row in rows:
            score = SCORE_MAP.get(row.get("effectiveness", "neutral"), 0.0)
            raw_values.append(score)
            recency = _recency_weight(row.get("recorded_at_utc"), now, half_life_days)
            weighted_sum += score * recency
            weight_sum += recency
        avg = (sum(raw_values) / len(raw_values)) if raw_values else 0.0
        decayed_avg = (weighted_sum / weight_sum) if weight_sum > 0 else 0.0

        if len(rows) < min_samples_for_hard_weight:
            if decayed_avg > 0.25:
                action = "soft_upweight"
            elif decayed_avg < -0.25:
                action = "soft_downweight"
            else:
                action = "keep"
        else:
            if decayed_avg > 0.25:
                action = "upweight"
            elif decayed_avg < -0.25:
                action = "downweight"
            else:
                action = "keep"

        weights[template_type] = {
            "avg_effect_score": round(avg, 6),
            "decayed_effect_score": round(decayed_avg, 6),
            "sample_count": len(rows),
            "recommended_action": action,
            "min_samples_for_hard_weight": min_samples_for_hard_weight,
            "half_life_days": half_life_days,
        }

    payload = {
        "templates": weights,
        "global_hint": "优先考虑历史 decayed_effect_score 更高的建议模板；当 sample_count 不足时，仅做 soft 级别升降权。",
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload

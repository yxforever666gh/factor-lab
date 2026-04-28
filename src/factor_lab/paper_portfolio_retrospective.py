from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def build_portfolio_retrospective(history_path: str | Path, output_path: str | Path) -> dict[str, Any]:
    history = json.loads(Path(history_path).read_text(encoding="utf-8")) if Path(history_path).exists() else []
    if not history:
        payload = {"status": "empty"}
        Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload

    latest = history[-1]
    previous = history[-2] if len(history) >= 2 else None

    latest_set = {p["ticker"] for p in latest.get("positions", [])}
    previous_set = {p["ticker"] for p in (previous.get("positions", []) if previous else [])}

    overlap = len(latest_set & previous_set)
    union = len(latest_set | previous_set) or 1
    stability_ratio = overlap / union

    payload = {
        "status": "ok",
        "history_count": len(history),
        "latest_as_of_date": latest.get("as_of_date"),
        "position_count": latest.get("position_count", 0),
        "overlap_with_previous": overlap,
        "stability_ratio": round(stability_ratio, 6),
        "added": sorted(latest_set - previous_set),
        "removed": sorted(previous_set - latest_set),
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def build_portfolio_stability_score(retro_path: str | Path, output_path: str | Path) -> dict[str, Any]:
    retro = json.loads(Path(retro_path).read_text(encoding="utf-8")) if Path(retro_path).exists() else {"status": "empty"}
    if retro.get("status") != "ok":
        payload = {"status": "empty", "stability_score": 0.0, "label": "无数据"}
        Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload

    stability_ratio = float(retro.get("stability_ratio", 0.0))
    history_count = int(retro.get("history_count", 0))

    history_bonus = min(history_count / 5.0, 1.0) * 0.2
    score = min(stability_ratio * 0.8 + history_bonus, 1.0)

    if score >= 0.85:
        label = "高稳定"
    elif score >= 0.6:
        label = "中等稳定"
    else:
        label = "低稳定"

    payload = {
        "status": "ok",
        "stability_score": round(score, 6),
        "label": label,
        "history_count": history_count,
        "stability_ratio": stability_ratio,
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload

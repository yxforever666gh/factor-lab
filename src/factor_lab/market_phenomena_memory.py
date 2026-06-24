from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def empty_phenomena_memory() -> dict[str, Any]:
    return {"schema_version": 1, "updated_at_utc": None, "phenomena": []}


def load_phenomena_memory(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return empty_phenomena_memory()
    data = json.loads(p.read_text(encoding="utf-8"))
    data.setdefault("schema_version", 1)
    data.setdefault("updated_at_utc", None)
    data.setdefault("phenomena", [])
    return data


def _memory_entry_from_verdict(verdict: dict[str, Any]) -> dict[str, Any]:
    latest = verdict.get("verdict")
    return {
        "phenomenon_id": verdict.get("phenomenon_id"),
        "title": verdict.get("title"),
        "mechanism_source": verdict.get("mechanism_source"),
        "participants": verdict.get("participants") or [],
        "observable_variables": verdict.get("observable_variables") or [],
        "prediction_target": verdict.get("prediction_target"),
        "latest_verdict": latest,
        "verdict_history": [latest] if latest else [],
        "what_was_learned": verdict.get("what_was_learned") or [],
        "what_failed": verdict.get("what_failed") or [],
        "do_not_repeat": verdict.get("do_not_repeat") or [],
        "next_research_question": verdict.get("next_research_question"),
        "times_reviewed": 1,
        "updated_at_utc": _now(),
    }


def upsert_phenomenon_verdict(memory: dict[str, Any], verdict: dict[str, Any]) -> dict[str, Any]:
    updated = {
        "schema_version": memory.get("schema_version", 1),
        "updated_at_utc": _now(),
        "phenomena": [dict(item) for item in memory.get("phenomena") or []],
    }
    phenomenon_id = verdict.get("phenomenon_id")
    if not phenomenon_id:
        return updated

    latest = verdict.get("verdict")
    for item in updated["phenomena"]:
        if item.get("phenomenon_id") == phenomenon_id:
            item["title"] = verdict.get("title") or item.get("title")
            item["mechanism_source"] = verdict.get("mechanism_source") or item.get("mechanism_source")
            item["participants"] = verdict.get("participants") or item.get("participants") or []
            item["observable_variables"] = verdict.get("observable_variables") or item.get("observable_variables") or []
            item["prediction_target"] = verdict.get("prediction_target") or item.get("prediction_target")
            item["latest_verdict"] = latest
            history = list(item.get("verdict_history") or [])
            if latest:
                history.append(latest)
            item["verdict_history"] = history
            item["what_was_learned"] = verdict.get("what_was_learned") or item.get("what_was_learned") or []
            item["what_failed"] = verdict.get("what_failed") or item.get("what_failed") or []
            item["do_not_repeat"] = verdict.get("do_not_repeat") or item.get("do_not_repeat") or []
            item["next_research_question"] = verdict.get("next_research_question") or item.get("next_research_question")
            item["times_reviewed"] = int(item.get("times_reviewed") or 0) + 1
            item["updated_at_utc"] = _now()
            return updated

    updated["phenomena"].append(_memory_entry_from_verdict(verdict))
    return updated


def memory_to_lessons_markdown(memory: dict[str, Any]) -> str:
    lines = ["# Market Phenomena Lessons", ""]
    for item in memory.get("phenomena") or []:
        lines.extend([
            f"## {item.get('phenomenon_id')}: {item.get('title')}",
            f"- latest_verdict: {item.get('latest_verdict')}",
            f"- times_reviewed: {item.get('times_reviewed')}",
            f"- next_research_question: {item.get('next_research_question')}",
            "",
            "### What was learned",
        ])
        lines.extend(f"- {x}" for x in item.get("what_was_learned") or ["none"])
        lines.append("")
        lines.append("### What failed")
        lines.extend(f"- {x}" for x in item.get("what_failed") or ["none"])
        lines.append("")
        lines.append("### Do not repeat")
        lines.extend(f"- {x}" for x in item.get("do_not_repeat") or ["none"])
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write_phenomena_memory(memory: dict[str, Any], json_path: str | Path, lessons_path: str | Path) -> dict[str, Path]:
    jp = Path(json_path)
    lp = Path(lessons_path)
    jp.parent.mkdir(parents=True, exist_ok=True)
    lp.parent.mkdir(parents=True, exist_ok=True)
    jp.write_text(json.dumps(memory, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    lp.write_text(memory_to_lessons_markdown(memory), encoding="utf-8")
    return {"json": jp, "lessons": lp}

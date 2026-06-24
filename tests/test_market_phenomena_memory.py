from __future__ import annotations

import json

from factor_lab.market_phenomena_memory import (
    empty_phenomena_memory,
    load_phenomena_memory,
    memory_to_lessons_markdown,
    upsert_phenomenon_verdict,
    write_phenomena_memory,
)


def verdict(phenomenon_id="quality_repair_delayed_repricing_v1", status="supported_for_further_research"):
    return {
        "phenomenon_id": phenomenon_id,
        "title": "盈利质量修复后的延迟重估",
        "verdict": status,
        "mechanism_source": "information_delay",
        "participants": ["低频基本面资金"],
        "observable_variables": ["profit_yoy", "pb"],
        "prediction_target": "future_60d_return_distribution",
        "what_was_learned": ["quality repair may matter"],
        "what_failed": [],
        "do_not_repeat": ["do not reduce this to PB screening"],
        "next_research_question": "Does the effect survive industry regime splits?",
    }


def test_missing_memory_loads_empty_schema(tmp_path):
    memory = load_phenomena_memory(tmp_path / "missing.json")
    assert memory["schema_version"] == 1
    assert memory["phenomena"] == []


def test_upsert_adds_new_phenomenon_verdict():
    memory = empty_phenomena_memory()
    updated = upsert_phenomenon_verdict(memory, verdict())
    assert len(updated["phenomena"]) == 1
    item = updated["phenomena"][0]
    assert item["phenomenon_id"] == "quality_repair_delayed_repricing_v1"
    assert item["latest_verdict"] == "supported_for_further_research"
    assert item["times_reviewed"] == 1


def test_upsert_updates_existing_phenomenon_verdict():
    memory = upsert_phenomenon_verdict(empty_phenomena_memory(), verdict(status="blocked_missing_data"))
    updated = upsert_phenomenon_verdict(memory, verdict(status="rejected_failed_verification"))
    assert len(updated["phenomena"]) == 1
    item = updated["phenomena"][0]
    assert item["latest_verdict"] == "rejected_failed_verification"
    assert item["times_reviewed"] == 2
    assert "blocked_missing_data" in item["verdict_history"]


def test_write_and_load_memory_roundtrip(tmp_path):
    memory = upsert_phenomenon_verdict(empty_phenomena_memory(), verdict())
    paths = write_phenomena_memory(memory, tmp_path / "memory.json", tmp_path / "lessons.md")
    assert paths["json"].exists()
    assert paths["lessons"].exists()
    loaded = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert loaded["phenomena"][0]["phenomenon_id"] == "quality_repair_delayed_repricing_v1"
    assert "Market Phenomena Lessons" in paths["lessons"].read_text(encoding="utf-8")


def test_lessons_markdown_includes_do_not_repeat():
    memory = upsert_phenonmenon = upsert_phenomenon_verdict(empty_phenomena_memory(), verdict())
    markdown = memory_to_lessons_markdown(upsert_phenonmenon)
    assert "do not reduce this to PB screening" in markdown

import json
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

from factor_lab.research_metrics import build_research_metrics


def test_build_research_metrics_includes_observation_windows(tmp_path):
    db_path = tmp_path / "factor_lab.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "create table research_tasks (task_id text, task_type text, status text, priority integer, fingerprint text, payload_json text, parent_task_id text, attempt_count integer, last_error text, created_at_utc text, started_at_utc text, finished_at_utc text, worker_note text)"
    )
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "insert into research_tasks values (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "task-1",
            "workflow",
            "finished",
            10,
            "fp1",
            json.dumps({"source": "candidate_generation"}, ensure_ascii=False),
            None,
            1,
            None,
            now,
            now,
            now,
            "generated candidate",
        ),
    )
    conn.commit()
    conn.close()

    memory_path = tmp_path / "research_memory.json"
    learning_path = tmp_path / "research_learning.json"
    candidate_pool_path = tmp_path / "research_candidate_pool.json"
    output_path = tmp_path / "research_metrics.json"

    memory_path.write_text(
        json.dumps(
            {
                "updated_at_utc": now,
                "execution_feedback": [{"updated_at_utc": now, "outcome_class": "high_value_failure"}],
                "generated_candidate_outcomes": [{"updated_at_utc": now, "operator": "combine_add", "outcome_class": "high_value_failure"}],
                "representative_candidate_reviews": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    learning_path.write_text(json.dumps({}, ensure_ascii=False), encoding="utf-8")
    candidate_pool_path.write_text(json.dumps({"summary": {}}, ensure_ascii=False), encoding="utf-8")

    payload = build_research_metrics(
        db_path=db_path,
        memory_path=memory_path,
        learning_path=learning_path,
        candidate_pool_path=candidate_pool_path,
        output_path=output_path,
    )

    assert "6h" in payload["metrics"]["observation_windows"]
    assert payload["metrics"]["observation_windows"]["6h"]["generated_candidate_outcomes"] == 1

import json
import sqlite3
from pathlib import Path

from factor_lab.research_metrics import build_research_metrics


def test_build_research_metrics_outputs_core_fields(tmp_path):
    db_path = tmp_path / "factor_lab.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "create table research_tasks (task_id text, task_type text, status text, priority integer, fingerprint text, payload_json text, parent_task_id text, attempt_count integer, last_error text, created_at_utc text, started_at_utc text, finished_at_utc text, worker_note text)"
    )
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
            "2026-04-01T00:00:00+00:00",
            "2026-04-01T00:00:01+00:00",
            "2026-04-01T00:00:02+00:00",
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
                "updated_at_utc": "2026-04-01T00:00:00+00:00",
                "execution_feedback": [{"outcome_class": "high_value_failure"}],
                "generated_candidate_outcomes": [{"operator": "combine_add"}],
                "representative_candidate_reviews": [{"branch_id": "rep"}],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    learning_path.write_text(
        json.dumps(
            {
                "operator_stats": {"combine_add": {"recommended_action": "keep"}},
                "research_mode": {"mode": "balanced"},
                "autonomy_profile": {"policy_name": "p"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    candidate_pool_path.write_text(
        json.dumps(
            {
                "summary": {
                    "candidate_count": 2,
                    "suppressed_candidate_count": 1,
                    "quality_priority": {
                        "quality_priority_mode": True,
                        "reasons": ["frontier evidence missing: gen_test"],
                        "frontier_evidence_missing": ["gen_test"],
                        "frontier_needs_validation": [],
                        "duplicate_pressure": 9,
                        "generated_candidate_budget": 0,
                    },
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    payload = build_research_metrics(
        db_path=db_path,
        memory_path=memory_path,
        learning_path=learning_path,
        candidate_pool_path=candidate_pool_path,
        output_path=output_path,
    )

    metrics = payload["metrics"]
    assert metrics["generated_candidate_task_count_recent"] == 1
    assert metrics["high_value_failure_count_recent"] == 1
    assert metrics["duplicate_suppression_ratio"] == 0.333333
    assert metrics["representative_review_count"] == 1
    assert metrics["quality_priority_mode"] is True
    assert metrics["frontier_evidence_missing_count"] == 1
    assert metrics["quality_duplicate_pressure"] == 9
    assert metrics["quality_generated_candidate_budget"] == 0

import json

from factor_lab.opportunity_executor import enqueue_opportunities


def test_enqueue_opportunities_respects_queue_aware_channel_bias(monkeypatch, tmp_path):
    opportunities_path = tmp_path / "research_opportunities.json"
    output_path = tmp_path / "opportunity_execution_plan.json"
    db_path = tmp_path / "factor_lab.db"
    opportunities_path.write_text(
        json.dumps(
            {
                "opportunities": [
                    {
                        "opportunity_id": "opp-val-1",
                        "opportunity_type": "diagnose",
                        "priority": 0.8,
                        "novelty_score": 0.6,
                    },
                    {
                        "opportunity_id": "opp-val-2",
                        "opportunity_type": "confirm",
                        "priority": 0.7,
                        "novelty_score": 0.5,
                    },
                    {
                        "opportunity_id": "opp-exp-1",
                        "opportunity_type": "probe",
                        "priority": 0.95,
                        "novelty_score": 0.9,
                    },
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr("factor_lab.opportunity_executor.sync_opportunities", lambda opportunities: None)
    monkeypatch.setattr("factor_lab.opportunity_executor.update_opportunity_state", lambda *args, **kwargs: None)
    monkeypatch.setattr("factor_lab.opportunity_executor.build_opportunity_review", lambda: {"blocks": {}, "downweights": {}})
    monkeypatch.setattr("factor_lab.opportunity_executor.should_bypass_recent_fingerprint", lambda opportunity: {"allow_bypass": False, "reason": None})
    monkeypatch.setattr("factor_lab.opportunity_executor.recently_finished_same_fingerprint", lambda *args, **kwargs: False)
    monkeypatch.setattr("factor_lab.opportunity_executor._learning_channel_bias", lambda limit: {"validation_bonus": 1, "exploration_penalty": 1})
    monkeypatch.setattr("factor_lab.opportunity_executor._queue_counts", lambda store: {"validation": 0, "exploration": 0})
    monkeypatch.setattr("factor_lab.opportunity_executor._queue_capacity", lambda: {"validation": 2, "exploration": 0})
    monkeypatch.setattr("factor_lab.opportunity_executor._queue_backlog_targets", lambda: {"validation": 2, "exploration": 0})

    def fake_map(opportunity):
        if opportunity["opportunity_type"] in {"diagnose", "confirm"}:
            return {
                "task_type": "diagnostic",
                "priority": 10,
                "fingerprint": f"diag::{opportunity['opportunity_id']}",
                "worker_note": "validation｜test",
                "payload": {"opportunity_id": opportunity["opportunity_id"]},
            }
        return {
            "task_type": "generated_batch",
            "priority": 40,
            "fingerprint": f"gen::{opportunity['opportunity_id']}",
            "worker_note": "exploration｜test",
            "payload": {"opportunity_id": opportunity["opportunity_id"]},
        }

    monkeypatch.setattr("factor_lab.opportunity_executor.map_opportunity_to_task", fake_map)

    payload = enqueue_opportunities(opportunities_path, output_path, db_path=db_path, limit=4, queue_aware=True)

    assert payload["channel_limits"] == {"validation": 2, "exploration": 0}
    assert payload["injected_count"] == 2
    assert all(row["task_type"] == "diagnostic" for row in payload["injected"])
    assert any(row["reason"] == "channel_deferred:exploration" for row in payload["skipped"])

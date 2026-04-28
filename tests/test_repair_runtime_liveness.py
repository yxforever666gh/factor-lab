from __future__ import annotations

from datetime import datetime, timedelta, timezone

from factor_lab.repair_agent_engine import build_repair_response
from factor_lab.repair_runtime import build_repair_runtime_snapshot, classify_queue_liveness


def iso_age(seconds: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds)).isoformat()


def test_empty_queue_with_recent_finished_task_is_healthy_idle():
    result = classify_queue_liveness(
        queue_counts={"pending": 0, "running": 0, "finished": 200, "failed": 0},
        recent_research_tasks=[
            {
                "status": "finished",
                "finished_at_utc": iso_age(45),
                "created_at_utc": iso_age(90),
                "task_type": "diagnostic",
                "worker_note": "diagnostic finished: opportunity_diagnose",
            }
        ],
        refill_state={"planner_injected": 1, "opportunity_injected": 2, "updated_at_utc": iso_age(50)},
        heartbeat_gap={"available": True, "seconds_since_last": 8},
        failure_state={"consecutive_failures": 0, "cooldown_active": False},
    )

    assert result["state"] == "healthy_idle"
    assert result["is_queue_stall"] is False
    assert "recent_activity" in result["reason"]


def test_empty_queue_with_no_recent_activity_is_queue_stall():
    result = classify_queue_liveness(
        queue_counts={"pending": 0, "running": 0, "finished": 200, "failed": 0},
        recent_research_tasks=[
            {
                "status": "finished",
                "finished_at_utc": iso_age(900),
                "created_at_utc": iso_age(930),
                "task_type": "workflow",
                "worker_note": "workflow finished",
            }
        ],
        refill_state={"planner_injected": 0, "opportunity_injected": 0, "updated_at_utc": iso_age(900)},
        heartbeat_gap={"available": True, "seconds_since_last": 900},
        failure_state={"consecutive_failures": 0, "cooldown_active": False},
    )

    assert result["state"] == "queue_stall"
    assert result["is_queue_stall"] is True


def test_empty_queue_during_repeat_cooldown_is_cooldown_idle():
    result = classify_queue_liveness(
        queue_counts={"pending": 0, "running": 0, "finished": 200, "failed": 0},
        recent_research_tasks=[],
        refill_state={
            "planner_injected": 0,
            "opportunity_injected": 0,
            "updated_at_utc": iso_age(30),
            "repeat_blocked_count": 2,
        },
        heartbeat_gap={"available": True, "seconds_since_last": 20},
        failure_state={"consecutive_failures": 0, "cooldown_active": False},
    )

    assert result["state"] == "cooldown_idle"
    assert result["is_queue_stall"] is False


class DummyStore:
    def list_research_tasks(self, limit=200):
        return [
            {
                "task_id": "finished-1",
                "status": "finished",
                "finished_at_utc": iso_age(30),
                "created_at_utc": iso_age(80),
                "task_type": "workflow",
            }
        ]

    def list_research_tasks_by_status(self, *args, **kwargs):
        return []


def test_repair_runtime_snapshot_includes_queue_liveness(monkeypatch, tmp_path):
    monkeypatch.setattr("factor_lab.repair_runtime.RESEARCH_DAEMON_STATUS_PATH", tmp_path / "missing-daemon.json")
    monkeypatch.setattr("factor_lab.repair_runtime.SYSTEM_HEARTBEAT_PATH", tmp_path / "missing-heartbeat.jsonl")
    monkeypatch.setattr("factor_lab.repair_runtime.ARTIFACTS", tmp_path)
    monkeypatch.setattr("factor_lab.repair_runtime.queue_budget_snapshot", lambda _store: {"baseline": 0, "validation": 0, "exploration": 0})
    monkeypatch.setattr("factor_lab.repair_runtime.recent_failure_stats", lambda _store: {"consecutive_failures": 0, "cooldown_active": False})
    monkeypatch.setattr("factor_lab.repair_runtime._recent_failed_or_risky_tasks", lambda _store: [])
    monkeypatch.setattr("factor_lab.repair_runtime._open_incidents", lambda: [])
    (tmp_path / "research_queue_refill_state.json").write_text(
        '{"planner_injected": 1, "opportunity_injected": 0, "updated_at_utc": "' + iso_age(20) + '"}',
        encoding="utf-8",
    )

    snapshot = build_repair_runtime_snapshot(DummyStore(), output_path=tmp_path / "snapshot.json")

    assert snapshot["queue_liveness"]["state"] == "healthy_idle"
    assert snapshot["queue_liveness"]["is_queue_stall"] is False


def test_repair_agent_observes_healthy_idle_instead_of_reseed():
    response = build_repair_response(
        {
            "context_id": "test",
            "inputs": {
                "runtime_snapshot": {
                    "queue_liveness": {"state": "healthy_idle", "is_queue_stall": False, "reason": "recent_activity"},
                    "queue_counts": {"pending": 0, "running": 0, "finished": 100, "failed": 0},
                }
            },
        },
        source_label="heuristic",
    )

    assert response["repair_mode"] == "observe"
    assert all(a["action_type"] != "reseed_queue" for a in response["recommended_actions"])

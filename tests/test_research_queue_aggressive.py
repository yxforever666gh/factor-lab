import json

from factor_lab import research_queue


class DummyStore:
    def list_research_tasks(self, limit=50):
        return []

    def claim_next_research_task(self):
        return None


def test_run_orchestrator_forces_expansion_when_queue_empty_during_reseed_cooldown(monkeypatch, tmp_path):
    monkeypatch.setattr(research_queue, "DB_PATH", tmp_path / "factor_lab.db")
    monkeypatch.setattr(research_queue, "STAGNATION_PATH", tmp_path / "research_stagnation.json")
    monkeypatch.setattr(research_queue, "ExperimentStore", lambda _path: DummyStore())
    monkeypatch.setattr(research_queue, "append_heartbeat", lambda *args, **kwargs: None)
    monkeypatch.setattr(research_queue, "_cleanup_stale_running_tasks", lambda *args, **kwargs: [])
    monkeypatch.setattr(research_queue, "run_research_planner_pipeline", lambda: {"injected_count": 0, "opportunity_execution": {"injected_count": 0}, "recovery_used": False, "research_flow_state": {"state": "unknown"}})
    monkeypatch.setattr(research_queue, "can_reseed_baseline", lambda store: False)
    monkeypatch.setattr(research_queue, "enqueue_baseline_tasks", lambda store: [])
    monkeypatch.setattr(research_queue, "recent_failure_stats", lambda store, limit=20, task_type=None: {"consecutive_failures": 0, "cooldown_active": False})
    monkeypatch.setattr(research_queue, "maybe_expand_research_space", lambda store, max_new_tasks=4, allow_repeat=True: ["task-a", "task-b"])

    result = research_queue.run_orchestrator(max_tasks=1)
    state = json.loads((tmp_path / "research_stagnation.json").read_text(encoding="utf-8"))

    assert result["processed"] == []
    assert state["last_reason"] in {"expanded", "cooldown_forced_expand"}
    assert state["consecutive_no_injection"] == 0

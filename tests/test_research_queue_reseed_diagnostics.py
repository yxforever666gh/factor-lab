from __future__ import annotations

from factor_lab import research_queue
from factor_lab.repair_playbooks import execute_repair_actions


class DummyStore:
    def list_research_tasks(self, limit=300):
        return []

    def enqueue_research_task(self, **kwargs):
        raise AssertionError("should not enqueue when repeat-blocked")


def test_enqueue_baseline_tasks_with_diagnostics_reports_repeat_blocked(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "tushare_workflow.json").write_text('{"a": 1}', encoding="utf-8")
    (tmp_path / "configs" / "tushare_batch.json").write_text('{"b": 2}', encoding="utf-8")

    monkeypatch.setattr(research_queue, "recently_finished_same_fingerprint", lambda *a, **kw: True)
    result = research_queue.enqueue_baseline_tasks_with_diagnostics(DummyStore())

    assert result["task_ids"] == []
    assert result["repeat_blocked_count"] == 2
    assert result["skipped"][0]["reason"] == "recently_finished_same_fingerprint"


def test_reseed_queue_action_includes_diagnostics(monkeypatch):
    diagnostics = {
        "task_ids": [],
        "repeat_blocked_count": 2,
        "budget_blocked_count": 0,
        "config_missing_count": 0,
        "enqueue_error_count": 0,
        "skipped": [{"reason": "recently_finished_same_fingerprint"}],
    }
    monkeypatch.setattr(research_queue, "enqueue_baseline_tasks_with_diagnostics", lambda _store: diagnostics)

    execution = execute_repair_actions(
        {
            "incident_type": "queue_stall",
            "repair_mode": "repair",
            "recommended_actions": [
                {"action_type": "reseed_queue", "target": "baseline", "risk_level": "low", "reason": "test"}
            ],
        },
        store=DummyStore(),
        auto_only=True,
    )

    action = execution["actions"][0]
    assert action["status"] == "failed_no_effect"
    assert action["reseed_diagnostics"]["repeat_blocked_count"] == 2

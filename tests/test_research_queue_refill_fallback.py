from __future__ import annotations

from factor_lab import research_queue


class DummyStore:
    def __init__(self):
        self.enqueued = []

    def list_research_tasks(self, limit=300):
        return []

    def enqueue_research_task(self, **kwargs):
        task_id = f"task-{len(self.enqueued)+1}"
        self.enqueued.append({"task_id": task_id, **kwargs})
        return task_id


def test_refill_fallback_uses_expansion_when_baseline_repeat_blocked(monkeypatch):
    store = DummyStore()
    monkeypatch.setattr(
        research_queue,
        "enqueue_baseline_tasks_with_diagnostics",
        lambda _store: {"task_ids": [], "repeat_blocked_count": 2, "skipped": [{"reason": "recently_finished_same_fingerprint"}]},
    )
    monkeypatch.setattr(
        research_queue,
        "maybe_expand_research_space",
        lambda _store, max_new_tasks=4, allow_repeat=False: ["expanded-1"],
    )

    result = research_queue.refill_empty_queue_with_fallback(store)

    assert result["task_ids"] == ["expanded-1"]
    assert result["source"] == "expand_research_space"
    assert result["reseed_diagnostics"]["repeat_blocked_count"] == 2

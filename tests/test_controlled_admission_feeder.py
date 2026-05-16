from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from factor_lab.controlled_admission_feeder import (
    feeder_route_counts,
    feeder_state_counts,
    load_feeder_state,
    record_feeder_event,
    run_controlled_admission_feeder,
    should_feed_controlled_work,
)


def test_feeder_does_not_inject_when_work_already_claimable():
    decision = should_feed_controlled_work(
        dry_run={"would_run_count": 1, "pending_count": 5},
        recent_injection_count=0,
        daily_injection_count=0,
        cooldown_ready=True,
        daily_budget=3,
    )

    assert decision["decision"] == "skip"
    assert "already_has_claimable_workflow" in decision["reasons"]
    assert decision["limit"] == 0


def test_feeder_does_not_inject_during_cooldown():
    decision = should_feed_controlled_work(
        dry_run={"would_run_count": 0, "pending_count": 4},
        recent_injection_count=1,
        daily_injection_count=0,
        cooldown_ready=False,
        daily_budget=3,
    )

    assert decision["decision"] == "skip"
    assert "cooldown_not_ready" in decision["reasons"]


def test_feeder_allows_one_task_when_idle_and_budget_available():
    decision = should_feed_controlled_work(
        dry_run={"would_run_count": 0, "pending_count": 4},
        recent_injection_count=0,
        daily_injection_count=1,
        cooldown_ready=True,
        daily_budget=3,
    )

    assert decision["decision"] == "feed"
    assert decision["limit"] == 1


def test_feeder_does_not_inject_when_daily_budget_exhausted():
    decision = should_feed_controlled_work(
        dry_run={"would_run_count": 0, "pending_count": 4},
        recent_injection_count=0,
        daily_injection_count=3,
        cooldown_ready=True,
        daily_budget=3,
    )

    assert decision["decision"] == "skip"
    assert "daily_budget_exhausted" in decision["reasons"]


def test_feeder_state_counts_recent_and_daily_injections():
    state = {
        "events": [
            {"created_at_utc": "2026-04-30T10:00:00+00:00", "enqueued_count": 1},
            {"created_at_utc": "2026-04-30T10:50:00+00:00", "enqueued_count": 2},
            {"created_at_utc": "2026-04-29T10:55:00+00:00", "enqueued_count": 4},
        ]
    }

    counts = feeder_state_counts(
        state=state,
        now_utc=datetime(2026, 4, 30, 11, 0, tzinfo=timezone.utc),
        cooldown_minutes=30,
    )

    assert counts == {"daily_injection_count": 3, "recent_injection_count": 2}


def test_record_feeder_injection_appends_event(tmp_path):
    state_path = tmp_path / "state.json"

    record_feeder_event(state_path, {"created_at_utc": "2026-04-30T11:00:00+00:00", "enqueued_count": 1})
    record_feeder_event(state_path, {"created_at_utc": "2026-04-30T12:00:00+00:00", "enqueued_count": 1})

    state = load_feeder_state(state_path)
    assert [event["enqueued_count"] for event in state["events"]] == [1, 1]


def test_feeder_route_counts_summarizes_recent_injected_routes():
    state = {
        "events": [
            {"created_at_utc": "2026-04-30T10:00:00+00:00", "routes": ["industry_relative_value"]},
            {"created_at_utc": "2026-04-30T10:30:00+00:00", "routes": ["industry_relative_value"]},
            {"created_at_utc": "2026-04-30T10:50:00+00:00", "routes": ["value_quality_no_distress"]},
            {"created_at_utc": "2026-04-28T10:50:00+00:00", "routes": ["old_route"]},
        ]
    }

    counts = feeder_route_counts(state, now_utc=datetime(2026, 4, 30, 11, 0, tzinfo=timezone.utc), lookback_hours=24)

    assert counts == {"industry_relative_value": 2, "value_quality_no_distress": 1}


def test_run_feeder_dry_run_does_not_enqueue(monkeypatch, tmp_path):
    calls = []

    monkeypatch.setattr(
        "factor_lab.controlled_admission_feeder.dry_run_controlled_restart",
        lambda db_path=None: {"would_run_count": 0, "pending_count": 4},
    )

    def fake_prepare(**kwargs):
        calls.append(kwargs)
        assert kwargs["dry_run"] is True
        return {"dry_run": True, "would_enqueue_count": 1, "enqueued_count": 0, "task_ids": [], "tasks": []}

    monkeypatch.setattr("factor_lab.controlled_admission_feeder.prepare_bucket_aware_tasks", fake_prepare)

    result = run_controlled_admission_feeder(
        output_dir=tmp_path,
        state_path=tmp_path / "state.json",
        write=False,
        now_utc=datetime(2026, 4, 30, 11, 0, tzinfo=timezone.utc),
    )

    assert result["decision"]["decision"] == "feed"
    assert result["prepare_result"]["enqueued_count"] == 0
    assert calls and calls[0]["dry_run"] is True
    assert (tmp_path / "feeder_run.json").exists()
    assert not (tmp_path / "state.json").exists()


def test_run_feeder_write_enqueues_one_when_decision_feed(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(
        "factor_lab.controlled_admission_feeder.dry_run_controlled_restart",
        lambda db_path=None: {"would_run_count": 0, "pending_count": 4},
    )

    def fake_prepare(**kwargs):
        calls.append(kwargs)
        return {"dry_run": False, "would_enqueue_count": 1, "enqueued_count": 1, "task_ids": ["task-1"], "tasks": [{"task_id": "task-1"}]}

    monkeypatch.setattr("factor_lab.controlled_admission_feeder.prepare_bucket_aware_tasks", fake_prepare)

    result = run_controlled_admission_feeder(
        output_dir=tmp_path,
        state_path=tmp_path / "state.json",
        write=True,
        now_utc=datetime(2026, 4, 30, 11, 0, tzinfo=timezone.utc),
    )

    assert result["decision"]["decision"] == "feed"
    assert result["prepare_result"]["task_ids"] == ["task-1"]
    assert calls[0]["dry_run"] is False
    assert calls[0]["limit"] == 1
    assert calls[0]["priority"] == 0
    assert calls[0]["force_new"] is False
    assert load_feeder_state(tmp_path / "state.json")["events"][0]["task_ids"] == ["task-1"]


def test_run_feeder_skips_when_would_run_exists(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "factor_lab.controlled_admission_feeder.dry_run_controlled_restart",
        lambda db_path=None: {"would_run_count": 1, "pending_count": 5},
    )
    monkeypatch.setattr(
        "factor_lab.controlled_admission_feeder.prepare_bucket_aware_tasks",
        lambda **kwargs: pytest.fail("prepare should not be called when work already exists"),
    )

    result = run_controlled_admission_feeder(output_dir=tmp_path, state_path=tmp_path / "state.json", write=True)

    assert result["decision"]["decision"] == "skip"
    assert result["prepare_result"] is None


def test_feeder_run_artifact_includes_effective_policy_and_runtime_interpretation(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "factor_lab.controlled_admission_feeder.dry_run_controlled_restart",
        lambda db_path=None: {"would_run_count": 0, "pending_count": 4, "blocked_count": 0},
    )

    def fake_prepare(**kwargs):
        return {"dry_run": False, "would_enqueue_count": 1, "enqueued_count": 1, "task_ids": ["task-1"], "tasks": []}

    monkeypatch.setattr("factor_lab.controlled_admission_feeder.prepare_bucket_aware_tasks", fake_prepare)

    result = run_controlled_admission_feeder(
        output_dir=tmp_path,
        state_path=tmp_path / "state.json",
        write=True,
        profile="conservative",
        limit=1,
        cooldown_minutes=60,
        daily_budget=3,
        force_new=False,
    )

    assert result["effective_policy"] == {
        "limit": 1,
        "priority": 0,
        "cooldown_minutes": 60,
        "daily_budget": 3,
        "force_new": False,
    }
    assert result["runtime_interpretation"] == "fed_one_workflow"
    assert result["safety"] == {"broad_daemon_allowed": False, "controlled_only_allowed": True}


def test_feeder_runtime_interpretation_distinguishes_idle_and_skips(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "factor_lab.controlled_admission_feeder.dry_run_controlled_restart",
        lambda db_path=None: {"would_run_count": 0, "pending_count": 4, "blocked_count": 0},
    )
    monkeypatch.setattr(
        "factor_lab.controlled_admission_feeder.prepare_bucket_aware_tasks",
        lambda **kwargs: {"dry_run": False, "would_enqueue_count": 1, "enqueued_count": 1, "task_ids": ["task-1"], "tasks": []},
    )
    result = run_controlled_admission_feeder(
        output_dir=tmp_path,
        state_path=tmp_path / "state.json",
        write=True,
        cooldown_minutes=60,
        now_utc=datetime(2026, 4, 30, 11, 0, tzinfo=timezone.utc),
    )
    assert result["runtime_interpretation"] == "fed_one_workflow"

    result = run_controlled_admission_feeder(
        output_dir=tmp_path,
        state_path=tmp_path / "state.json",
        write=True,
        cooldown_minutes=60,
        now_utc=datetime(2026, 4, 30, 11, 30, tzinfo=timezone.utc),
    )
    assert result["runtime_interpretation"] == "skipped_cooldown"

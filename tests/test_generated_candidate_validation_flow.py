import json
from pathlib import Path

from factor_lab import research_expansion, research_queue
from factor_lab.storage import ExperimentStore


class DummyStore:
    def top_promising_candidates(self, limit=4):
        return [
            {
                "definition": {
                    "name": "gen__combine_sub__earnings_yield_over_pb__mom_60_skip_5",
                    "expression": "close",
                    "generator_operator": "combine_sub",
                    "left_factor_name": "earnings_yield_over_pb",
                    "right_factor_name": "mom_60_skip_5",
                }
            }
        ]


def test_generated_candidate_validation_specs_use_light_45d_only(monkeypatch):
    captured = []

    def fake_write(config, name):
        captured.append((name, config))
        return f"artifacts/generated_configs/{name}.json"

    monkeypatch.setattr(research_expansion, "_write_generated_config", fake_write)
    monkeypatch.setattr(
        research_expansion,
        "resolve_factor_definitions",
        lambda *args, **kwargs: [
            {"name": "earnings_yield_over_pb", "expression": "close / pb"},
            {"name": "mom_60_skip_5", "expression": "momentum_60_skip_5"},
        ],
    )
    monkeypatch.setenv("RESEARCH_ENABLE_GENERATED_CANDIDATE_REVALIDATION", "1")
    monkeypatch.setenv("RESEARCH_GENERATED_CANDIDATE_UNIVERSE_LIMIT", "40")

    specs = research_expansion._candidate_validation_specs(
        DummyStore(),
        {"universe_limit": 100, "rolling_validation": {"window_size": 63, "step_size": 21}},
        "2026-03-18",
    )

    assert len(specs) == 1
    assert specs[0]["payload"]["source"] == "candidate_generation_validation"
    assert specs[0]["payload"]["validation_stage"] == "recent_45d_light"
    assert "candidate_generation" in specs[0]["worker_note"]
    assert captured[0][1]["universe_limit"] == 40
    assert captured[0][1]["generated_candidate_validation_mode"] == "light"
    assert captured[0][1]["rolling_validation"]["window_size"] == 21
    assert captured[0][1]["rolling_validation"]["step_size"] == 7
    assert captured[0][1]["factors"][0]["expression"] == "(close / pb) - (momentum_60_skip_5)"


def test_task_requires_serial_execution_for_generated_candidate_validation():
    task = {
        "task_type": "workflow",
        "payload": {"config_path": "artifacts/generated_configs/candidate_gen__x_recent_45d.json", "source": "candidate_generation_validation"},
        "worker_note": "validation｜candidate_generation gen__x recent_45d｜light",
    }

    assert research_queue._task_requires_serial_execution(task) is True


def test_recent_failure_stats_ignores_maintenance_failures(tmp_path):
    db_path = tmp_path / "factor_lab.db"
    store = ExperimentStore(db_path)
    monkeypatch_db = research_queue.DB_PATH
    research_queue.DB_PATH = db_path
    try:
        stale_id = store.enqueue_research_task(
            task_type="workflow",
            payload={"config_path": "a.json", "output_dir": "artifacts/a"},
            priority=1,
            worker_note="validation｜candidate_validation x recent_45d｜auto_cleaned_stale_running",
        )
        store.finish_research_task(stale_id, status="failed", last_error="stale_running_task_cleaned")
        real_id = store.enqueue_research_task(
            task_type="workflow",
            payload={"config_path": "b.json", "output_dir": "artifacts/b"},
            priority=1,
            worker_note="validation｜candidate_generation gen__x recent_45d｜light",
        )
        store.finish_research_task(real_id, status="failed", last_error="research task worker rss exceeded limit: 2050MB >= 2048MB")

        stats = research_queue.recent_failure_stats(store, limit=10, task_type="workflow")

        assert stats["consecutive_failures"] == 1
        assert stats["failed_recently"] == 1
    finally:
        research_queue.DB_PATH = monkeypatch_db


def test_recent_window_validation_specs_use_light_profile(monkeypatch):
    captured = []

    def fake_write(config, name):
        captured.append((name, config))
        return f"artifacts/generated_configs/{name}.json"

    class EmptyStore:
        def top_promising_candidates(self, limit=4):
            return []

        def list_research_tasks(self, limit=300):
            return []

    monkeypatch.setattr(research_expansion, "_write_generated_config", fake_write)
    monkeypatch.setenv("RESEARCH_LIGHT_VALIDATION_UNIVERSE_LIMIT", "60")

    specs = research_expansion.expansion_candidates(EmptyStore(), allow_repeat=True)
    recent = [row for row in specs if (row.get('payload') or {}).get('source') == 'recent_window_validation_light']

    assert recent
    recent_cfgs = {name: config for name, config in captured if name in {'rolling_recent_45d', 'rolling_recent_90d', 'rolling_recent_120d'}}
    assert recent_cfgs['rolling_recent_45d']['universe_limit'] == 60
    assert recent_cfgs['rolling_recent_45d']['rolling_validation']['window_size'] == 31
    assert recent_cfgs['rolling_recent_45d']['rolling_validation']['step_size'] == 10


def test_generated_candidate_validation_followup_promotes_45d_to_90d(tmp_path, monkeypatch):
    db_path = tmp_path / "factor_lab.db"
    store = ExperimentStore(db_path)
    cfg_path = tmp_path / "candidate_gen__x_recent_45d.json"
    cfg_path.write_text(
        json.dumps(
            {
                "end_date": "2026-03-18",
                "start_date": "2026-02-01",
                "output_dir": "artifacts/generated_candidate_gen__x_recent_45d",
                "factors": [{"name": "gen__x", "expression": "close", "generator_operator": "combine_sub", "left_factor_name": "a", "right_factor_name": "b"}],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(research_queue, "recently_finished_same_fingerprint", lambda *args, **kwargs: False)
    task = {"task_id": "t1", "task_type": "workflow", "worker_note": "validation｜candidate_generation gen__x recent_45d｜light"}
    payload = {"config_path": str(cfg_path), "output_dir": "artifacts/generated_candidate_gen__x_recent_45d", "source": "candidate_generation_validation", "validation_stage": "recent_45d_light"}

    followups = research_queue._enqueue_generated_candidate_validation_followup(store, task, payload)

    assert len(followups) == 1
    queued = store.get_research_task(followups[0])
    assert queued["status"] == "pending"
    assert queued["payload"]["validation_stage"] == "recent_90d_light"
    assert "recent_90d" in queued["payload"]["config_path"]

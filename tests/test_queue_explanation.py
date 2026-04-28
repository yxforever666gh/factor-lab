import importlib.util
import sqlite3
import sys
from pathlib import Path


def _load_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "build_queue_explanation.py"
    module_name = "build_queue_explanation_test"
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _create_db(path: Path):
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE research_tasks ("
        "task_id TEXT, task_type TEXT, status TEXT, priority INTEGER, payload_json TEXT, "
        "created_at_utc TEXT, started_at_utc TEXT, finished_at_utc TEXT, worker_note TEXT, last_error TEXT)"
    )
    conn.execute("INSERT INTO research_tasks VALUES ('p1','workflow','pending',1,'{}','2026-01-01',NULL,NULL,'planner_selected','')")
    conn.execute("INSERT INTO research_tasks VALUES ('r1','diagnostic','running',1,'{}','2026-01-01','2026-01-01',NULL,'note','')")
    conn.execute("INSERT INTO research_tasks VALUES ('f1','workflow','failed',1,'{}','2026-01-01','2026-01-01','2026-01-01','note','boom')")
    conn.commit()
    conn.close()


def test_build_explanation_reports_counts_and_recommendation(tmp_path):
    mod = _load_module()
    db_path = tmp_path / "factor_lab.db"
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    _create_db(db_path)
    (artifacts / "research_queue_refill_state.json").write_text('{"validation_deficit":2,"exploration_deficit":1,"planner_injected":1}', encoding="utf-8")
    (artifacts / "research_planner_validated.json").write_text('{"accepted_count":1,"rejected_count":1,"rejected":[{"reason":"recently_finished_same_fingerprint"}]}', encoding="utf-8")

    explanation = mod.build_explanation(db_path, artifacts)

    assert explanation["queue_counts"]["pending"] == 1
    assert explanation["queue_counts"]["running"] == 1
    assert explanation["queue_counts"]["failed"] == 1
    assert explanation["recommendation"] == "wait"
    assert explanation["top_skip_reasons"]["recently_finished_same_fingerprint"] == 1

import json
from pathlib import Path

from fastapi.testclient import TestClient

from factor_lab import webui_app


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def test_overview_does_not_scan_latest_tushare_artifacts(tmp_path: Path, monkeypatch):
    artifacts = tmp_path / "artifacts"
    run_dir = artifacts / "tushare_workflow"
    config_path = tmp_path / "configs" / "tushare.json"
    _write_json(config_path, {"data_source": "tushare"})
    _write_json(run_dir / "task_state.json", {
        "task_id": "tushare-run",
        "status": "finished",
        "finished_at_utc": "2026-08-21T05:29:34+00:00",
        "config_path": str(config_path),
        "output_dir": str(run_dir),
    })
    results = [
        {"factor_name": f"factor_{index}", "expression": f"x{index}", "rank_ic_mean": 0.02 - index / 1000, "top_bottom_spread_mean": -0.001, "pass_gate": False, "fail_reason": "未达到门槛"}
        for index in range(8)
    ]
    _write_json(run_dir / "results.json", results)
    _write_json(run_dir / "factor_scores.json", [{"factor_name": f"factor_{index}", "score": 8 - index} for index in range(8)])
    _write_json(run_dir / "portfolio_results.json", [{"strategy_name": "cluster_representatives_only", "annual_return": 0.01, "sharpe": 0.12, "max_drawdown": -0.2}])
    _write_json(run_dir / "timing.json", {"dataset_rows": 11552})

    monkeypatch.setattr(webui_app, "DB_PATH", artifacts / "factor_lab.db")
    monkeypatch.setattr(webui_app, "_quick_latest_runs", lambda limit=5: [])
    monkeypatch.setattr(webui_app, "_quick_research_queue_snapshot", lambda: ({"pending": 0, "running": 0, "finished_24h": 0, "failed_24h": 0}, None))
    monkeypatch.setattr(webui_app, "_quick_daemon_status", lambda: {"active": False, "label": "unknown", "detail": "test"})
    monkeypatch.setattr(webui_app, "_quick_provider_status", lambda: (None, None))
    monkeypatch.setattr(webui_app, "get_cached_health_metrics", lambda: (_ for _ in ()).throw(AssertionError("overview must stay lightweight")))
    monkeypatch.setattr(
        webui_app,
        "_build_legacy_overview_snapshot",
        lambda: (_ for _ in ()).throw(AssertionError("overview must not scan legacy artifacts")),
    )
    webui_app._OVERVIEW_CACHE.update({"key": None, "created_at": 0.0, "payload": None})

    response = TestClient(webui_app.app).get("/")

    assert response.status_code == 200
    assert "Research OS" in response.text
    assert "TUSHARE" not in response.text
    assert "factor_0" not in response.text
    assert "cluster_representatives_only" not in response.text


def test_overview_snapshot_cache_reuses_payload(monkeypatch):
    calls = []
    monkeypatch.setattr(webui_app, "_build_overview_snapshot", lambda: calls.append(1) or {"value": len(calls)})
    webui_app._OVERVIEW_CACHE.update({"key": None, "created_at": 0.0, "payload": None})

    first = webui_app.get_overview_snapshot(max_age_seconds=30)
    second = webui_app.get_overview_snapshot(max_age_seconds=30)

    assert first == second == {"value": 1}
    assert calls == [1]

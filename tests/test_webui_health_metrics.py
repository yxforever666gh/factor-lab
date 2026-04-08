import importlib
from pathlib import Path

import pytest


pytest.importorskip("fastapi")
pytest.importorskip("jinja2")


webui_app = importlib.import_module("factor_lab.webui_app")


def test_research_progress_display_falls_back_to_snapshot_when_timeline_has_no_candidates():
    payload = webui_app._build_research_progress_display(
        80.0,
        20.0,
        timeline_candidate_run_count=0,
        timeline_candidate_factor_count=0,
        timeline_point_count=12,
    )

    assert payload["score"] == 80.0
    assert payload["basis"] == "snapshot"
    assert payload["timeline_sparse"] is True


def test_load_health_paper_stability_prefers_live_file(tmp_path: Path):
    live_dir = tmp_path / "paper_portfolio"
    live_dir.mkdir(parents=True)
    (live_dir / "portfolio_stability_score.json").write_text(
        '{"status":"ok","stability_score":0.12,"label":"低稳定"}',
        encoding="utf-8",
    )

    payload, source = webui_app._load_health_paper_stability(
        tmp_path,
        {"paper_portfolio_stability": {"stability_score": 0.88, "label": "高稳定"}},
    )

    assert source == "live_portfolio_stability"
    assert payload["stability_score"] == 0.12

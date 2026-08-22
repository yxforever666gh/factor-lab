from __future__ import annotations

import json

import pandas as pd
import pytest

import factor_lab.autonomous_strategy_cache_extension as cache_extension_module

from factor_lab.autonomous_strategy_cache_extension import (
    build_history_cache_extension_plan,
    cache_extension_plan_to_markdown,
    write_cache_extension_plan,
)


@pytest.fixture(autouse=True)
def _isolate_workspace_env_file(monkeypatch: pytest.MonkeyPatch) -> None:
    # These unit tests control the token environment explicitly.  Loading the
    # developer workspace .env here would leak unrelated production profiles
    # into later tests through os.environ.setdefault.
    monkeypatch.setattr(cache_extension_module, "load_env_file", lambda: None)


def _write_cache(path, tickers=("000001.SZ", "000002.SZ")):
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for ticker in tickers:
        for day in range(3):
            rows.append({
                "date": pd.Timestamp("2020-01-01") + pd.Timedelta(days=day),
                "ticker": ticker,
                "pb": 1.0,
                "pe_ttm": 10.0,
                "forward_return_5d": 0.01,
            })
    pd.DataFrame(rows).to_csv(path, index=False)


def _coverage(source_path: str, status="blocked") -> dict:
    return {
        "overall_status": status,
        "source_path": source_path,
        "date_min": "2020-01-01",
        "date_max": "2020-01-03",
        "field_coverage": [
            {"derived_field": "pb_history_756d", "status": "insufficient_history"},
            {"derived_field": "pe_ttm_history_756d", "status": "insufficient_history"},
        ],
    }


def test_cache_extension_plan_is_dry_run_and_external_when_no_covering_cache(tmp_path, monkeypatch):
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    cache_path = tmp_path / "artifacts/tushare_cache/tushare_2020-01-01_2023-12-31_2.csv"
    _write_cache(cache_path)

    plan = build_history_cache_extension_plan(
        run_id="x",
        coverage_preflight=_coverage("artifacts/tushare_cache/tushare_2020-01-01_2023-12-31_2.csv"),
        root=tmp_path,
        cache_dir="artifacts/tushare_cache",
        token_env_var="TEST_TUSHARE_TOKEN",
    )
    assert plan["execution_status"] == "dry_run_plan_written"
    assert plan["action"] == "fetch_required"
    assert plan["external_request_required"] is True
    assert plan["token_configured"] is False
    assert "configure_tushare_token" in plan["next_allowed_actions"]
    assert plan["queue_write_allowed"] is False
    assert plan["controlled_execution_allowed"] is False
    assert plan["target_universe_count"] == 2
    assert plan["target_start_date"] < "2020-01-01"


def test_cache_extension_plan_reuses_covering_cache_when_available(tmp_path, monkeypatch):
    monkeypatch.setenv("TUSHARE_TOKEN", "dummy")
    source = tmp_path / "artifacts/tushare_cache/tushare_2020-01-01_2023-12-31_2.csv"
    covering = tmp_path / "artifacts/tushare_cache/tushare_2016-09-07_2023-12-31_2.csv"
    _write_cache(source)
    _write_cache(covering)

    plan = build_history_cache_extension_plan(
        run_id="x",
        coverage_preflight=_coverage("artifacts/tushare_cache/tushare_2020-01-01_2023-12-31_2.csv"),
        root=tmp_path,
        cache_dir="artifacts/tushare_cache",
    )

    assert plan["action"] == "reuse_covering_cache"
    assert plan["external_request_required"] is False
    assert plan["covering_cache_path"].endswith("tushare_2016-09-07_2023-12-31_2.csv")
    assert "rerun_coverage_preflight_with_covering_cache" in plan["next_allowed_actions"]


def test_cache_extension_plan_no_fetch_when_coverage_already_passes(tmp_path):
    cache_path = tmp_path / "artifacts/tushare_cache/tushare_2020-01-01_2023-12-31_2.csv"
    _write_cache(cache_path)

    plan = build_history_cache_extension_plan(
        run_id="x",
        coverage_preflight=_coverage("artifacts/tushare_cache/tushare_2020-01-01_2023-12-31_2.csv", status="pass"),
        root=tmp_path,
        cache_dir="artifacts/tushare_cache",
    )

    assert plan["action"] == "no_fetch_needed"
    assert plan["external_request_required"] is False
    assert "proceed_to_information_screen" in plan["next_allowed_actions"]


def test_cache_extension_plan_markdown_and_write(tmp_path):
    plan = {
        "run_id": "x",
        "execution_status": "dry_run_plan_written",
        "coverage_overall_status": "blocked",
        "action": "fetch_required",
        "external_request_required": True,
        "token_configured": False,
        "source_path": "source.csv",
        "target_cache_path": "target.csv",
        "target_start_date": "2017-01-01",
        "target_end_date": "2023-12-31",
        "target_universe_count": 2,
        "queue_write_allowed": False,
        "controlled_execution_allowed": False,
        "blocked_fields": ["pb_history_756d"],
        "next_allowed_actions": ["configure_tushare_token"],
        "blocked_actions": ["full_backtest"],
    }
    markdown = cache_extension_plan_to_markdown(plan)
    assert "Autonomous Strategy Cache Extension Plan" in markdown
    assert "action: fetch_required" in markdown

    paths = write_cache_extension_plan(plan, tmp_path)
    assert paths["json"].exists()
    assert paths["markdown"].exists()
    payload = json.loads(paths["json"].read_text())
    assert payload["action"] == "fetch_required"

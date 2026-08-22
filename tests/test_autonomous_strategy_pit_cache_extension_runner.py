import os
from pathlib import Path

import pandas as pd
import pytest

import factor_lab.autonomous_strategy_pit_cache_extension_runner as pit_runner_module

from factor_lab.autonomous_strategy_pit_cache_extension_runner import run_pit_cache_extension


@pytest.fixture(autouse=True)
def _isolate_workspace_env_file(monkeypatch: pytest.MonkeyPatch) -> None:
    # Token presence is part of each test input; unrelated values from the
    # developer workspace .env must not survive into the rest of the suite.
    monkeypatch.setattr(pit_runner_module, "load_env_file", lambda: None)


class FakeProvider:
    def __init__(self, enriched):
        self.enriched = enriched

    def enrich_frame_with_pit_financial_features(self, frame: pd.DataFrame, *, cache_dir: str | Path = "artifacts/tushare_cache", timing=None, retain_pit_cashflow_diagnostics: bool = False) -> pd.DataFrame:
        Path(cache_dir).mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"ticker": ["A"], "date": ["2020-01-01"], "profit_yoy": [1.0]}).to_csv(Path(cache_dir) / "pit_financial_fake.csv", index=False)
        return self.enriched.copy()


def base_frame():
    return pd.DataFrame(
        {
            "ticker": ["A", "B"],
            "date": pd.to_datetime(["2020-01-01", "2020-01-02"]),
            "profit_yoy": [pd.NA, pd.NA],
            "debt_to_asset": [pd.NA, pd.NA],
            "operating_cashflow_to_profit": [pd.NA, pd.NA],
        }
    )


def test_pit_cache_extension_blocks_without_token(tmp_path, monkeypatch):
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    report = run_pit_cache_extension(
        run_id="r",
        extension_plan={"target_overlay_coverage": 0.6},
        base_frame=base_frame(),
        base_path="base.csv",
        cache_dir=tmp_path / "cache",
        output_dir=tmp_path / "out",
        provider=FakeProvider(base_frame()),
        token_env_var="MISSING_TUSHARE_TOKEN_FOR_TEST",
    )
    assert report["execution_status"] == "blocked"
    assert report["failure_reason"] == "tushare_token_not_configured"
    assert report["controlled_execution_allowed"] is False
    assert report["queue_write_allowed"] is False


def test_pit_cache_extension_reports_failed_coverage_when_extension_sparse(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_TUSHARE_TOKEN", "token")
    enriched = base_frame().copy()
    enriched.loc[0, "profit_yoy"] = 1.0
    enriched.loc[0, "debt_to_asset"] = 0.4
    enriched.loc[0, "operating_cashflow_to_profit"] = 2.0
    report = run_pit_cache_extension(
        run_id="r",
        extension_plan={"target_overlay_coverage": 0.6},
        base_frame=base_frame(),
        base_path="base.csv",
        cache_dir=tmp_path / "cache",
        output_dir=tmp_path / "out",
        provider=FakeProvider(enriched),
        token_env_var="TEST_TUSHARE_TOKEN",
    )
    assert report["execution_status"] == "completed"
    assert report["coverage_pass"] is False
    assert report["recommended_next_step"] == "stop_proxy_route_or_reduce_universe"
    assert report["coverage_after_extension"]["profit_yoy"] == 0.5
    assert Path(report["output_feature_frame_path"]).exists()


def test_pit_cache_extension_reports_pass_when_coverage_enough(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_TUSHARE_TOKEN", "token")
    enriched = base_frame().copy()
    enriched["profit_yoy"] = [1.0, 1.1]
    enriched["debt_to_asset"] = [0.4, 0.5]
    enriched["operating_cashflow_to_profit"] = [2.0, 2.1]
    report = run_pit_cache_extension(
        run_id="r",
        extension_plan={"target_overlay_coverage": 0.6},
        base_frame=base_frame(),
        base_path="base.csv",
        cache_dir=tmp_path / "cache",
        output_dir=tmp_path / "out",
        provider=FakeProvider(enriched),
        token_env_var="TEST_TUSHARE_TOKEN",
    )
    assert report["execution_status"] == "completed"
    assert report["coverage_pass"] is True
    assert report["recommended_next_step"] == "rerun_proxy_field_resolution_with_pit_overlay"
    assert report["controlled_execution_allowed"] is False
    assert report["queue_write_allowed"] is False

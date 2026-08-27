from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from factor_lab.portfolio import (
    ExecutionAccount,
    ExecutionColumns,
    ExecutionPolicy,
    ExecutionPosition,
    LongOnlyPortfolioConfig,
    evaluate_long_only_portfolio,
)
from factor_lab.portfolio.execution import process_account_observation
from factor_lab.research import runner as runner_module
from factor_lab.research.contracts import FactorSpec, ValidationSpec
from factor_lab.research.reporting import render_report
from factor_lab.research.runner import run_research
from factor_lab.research.validation import evaluate_stage_a


def _write_suspension_snapshot(
    path: Path,
    rows: pd.DataFrame,
    *,
    query_start: str | None = None,
    query_end: str | None = None,
) -> pd.DataFrame:
    """Write the canonical parquet+metadata pair expected by the runner."""

    canonical = rows.loc[
        :, ["ticker", "date", "suspend_type", "suspend_timing"]
    ].copy()
    canonical["ticker"] = canonical["ticker"].astype("string").str.strip()
    canonical["date"] = pd.to_datetime(
        canonical["date"], errors="raise"
    ).dt.normalize()
    canonical["suspend_type"] = (
        canonical["suspend_type"].astype("string").str.strip().str.upper()
    )
    canonical["suspend_timing"] = (
        canonical["suspend_timing"].astype("string").str.strip()
    )
    canonical = (
        canonical.drop_duplicates(
            ["ticker", "date", "suspend_type", "suspend_timing"]
        )
        .sort_values(
            ["date", "ticker", "suspend_type", "suspend_timing"],
            na_position="last",
        )
        .reset_index(drop=True)
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    canonical.to_parquet(path, index=False)
    minimum = pd.Timestamp(canonical["date"].min()).date().isoformat()
    maximum = pd.Timestamp(canonical["date"].max()).date().isoformat()
    metadata_path = path.with_name("suspensions.meta.json")
    metadata = {
        "schema_version": 1,
        "status": "complete",
        "source": "tushare",
        "endpoint": "suspend_d",
        "query": {
            "start_date": query_start or minimum,
            "end_date": query_end or maximum,
            "window": "calendar_year",
            "limit": 5_000,
        },
        "retrieved_at_utc": "2026-08-27T00:00:00+00:00",
        "rows": int(len(canonical)),
        "date": {"min": minimum, "max": maximum},
        "security": int(canonical["ticker"].nunique()),
        "S": int(canonical["suspend_type"].eq("S").sum()),
        "R": int(canonical["suspend_type"].eq("R").sum()),
        "file": {
            "path": str(path.resolve()),
            "size_bytes": path.stat().st_size,
            "sha256": runner_module._sha256_file(path),
        },
        "metadata_path": str(metadata_path.resolve()),
    }
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    return canonical


@pytest.mark.parametrize(
    ("names", "message"),
    [
        (["alpha", "alpha"], "duplicate name"),
        (["Alpha", "alpha"], "artifact normalization"),
        (["x" * 100 + "a", "x" * 100 + "b"], "artifact normalization"),
    ],
)
def test_factor_artifact_names_reject_duplicates_and_portable_collisions(
    names: list[str],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        runner_module._assert_unique_artifact_names(
            names,
            context="test factor artifact",
        )


def test_research_runner_rejects_unattested_raw_price_mode() -> None:
    with pytest.raises(
        ValueError,
        match="raw_with_actions is not backed by an attested production artifact",
    ):
        runner_module._portfolio_config(
            {
                "portfolio": {
                    "price_basis": "raw_with_actions",
                    "open_column": "open",
                }
            }
        )


def test_factor_suite_rejects_duplicate_challenger_names(tmp_path: Path) -> None:
    path = tmp_path / "factors.json"
    path.write_text(
        json.dumps(
            {
                "control": {
                    "name": "control",
                    "family": "test",
                    "expression": "x",
                },
                "suites": {
                    "duplicate": [
                        {"name": "same", "family": "test", "expression": "x"},
                        {"name": "same", "family": "test", "expression": "x + 1"},
                    ]
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="duplicate name"):
        runner_module.load_factor_suite(path, "duplicate")


def test_execution_loader_injects_causal_event_only_delist_row(
    tmp_path: Path,
) -> None:
    dates = pd.bdate_range("2026-05-04", periods=5)
    execution_rows = []
    for date in dates:
        execution_rows.append(
            {
                "date": date,
                "ticker": "B",
                "open_adj": 10.0,
                "adv_20": 1_000_000.0,
                "volatility_20": 0.02,
                "eligible": True,
                "universe_member": True,
                "share_split_ratio": 1.0,
                "cash_dividend_per_share": 0.0,
            }
        )
    for date in dates[:2]:
        execution_rows.append(
            {
                "date": date,
                "ticker": "A",
                "open_adj": 10.0,
                "adv_20": 1_000_000.0,
                "volatility_20": 0.02,
                "eligible": True,
                "universe_member": True,
                "share_split_ratio": 1.0,
                "cash_dividend_per_share": 0.0,
            }
        )
    execution_path = tmp_path / "execution.parquet"
    pd.DataFrame(execution_rows).to_parquet(execution_path, index=False)
    feature_path = tmp_path / "features.parquet"
    pd.DataFrame(
        {
            "ticker": ["A", "A", "B"],
            "delist_date": [dates[2], dates[2], pd.NaT],
        }
    ).to_parquet(feature_path, index=False)

    loaded = runner_module._load_execution(
        execution_path,
        LongOnlyPortfolioConfig(),
        feature_path=feature_path,
    )

    a_rows = loaded.loc[loaded["ticker"] == "A"].sort_values("date")
    assert a_rows.loc[a_rows["date"] < dates[2], "is_delisted"].eq(False).all()
    event = a_rows.loc[a_rows["date"] == dates[2]].iloc[0]
    assert bool(event["is_delisted"]) is True
    assert pd.isna(event["open_adj"])
    assert bool(event["eligible"]) is False
    assert a_rows.loc[a_rows["date"] >= dates[2], "is_delisted"].eq(True).all()
    injection = loaded.attrs["security_event_injection"]
    assert injection["status"] == "available"
    assert injection["availability_rule"] == (
        "every_execution_session_on_or_after_delist_date"
    )
    assert injection["delist_security_count"] == 1
    assert injection["delist_flagged_session_count"] == 3
    assert injection["event_only_row_count"] == 3
    assert injection["cash_recovery_policy"] == (
        "zero_unless_explicit_event_terms_exist"
    )
    assert injection["suspension_status"] == "unavailable"
    assert injection["suspension_unavailable_reason"] == "artifact_not_configured"
    lineage_fields = runner_module._execution_lineage_fields(
        loaded,
        LongOnlyPortfolioConfig(),
    )
    assert "share_split_ratio" in loaded.columns
    assert "cash_dividend_per_share" in loaded.columns
    assert "share_split_ratio" in lineage_fields
    assert "cash_dividend_per_share" in lineage_fields


def test_authoritative_suspensions_project_601088_gap_and_carry_stale_position(
    tmp_path: Path,
) -> None:
    calendar = pd.bdate_range("2017-05-22", "2017-09-08")
    suspension_dates = pd.bdate_range("2017-06-05", "2017-08-31")
    assert len(suspension_dates) == 64
    execution_rows = [
        {
            "date": date,
            "ticker": "000001.SZ",
            "open_adj": 10.0,
            "adv_20": 100_000_000.0,
            "volatility_20": 0.02,
        }
        for date in calendar
    ]
    execution_rows.append(
        {
            "date": pd.Timestamp("2017-05-22"),
            "ticker": "601088.SH",
            "open_adj": 10.0,
            "adv_20": 100_000_000.0,
            "volatility_20": 0.02,
        }
    )
    execution_rows.extend(
        {
            "date": date,
            "ticker": "601088.SH",
            "open_adj": 10.1,
            "adv_20": 100_000_000.0,
            "volatility_20": 0.02,
        }
        for date in calendar[calendar >= pd.Timestamp("2017-09-01")]
    )
    execution_path = tmp_path / "execution.parquet"
    pd.DataFrame(execution_rows).to_parquet(execution_path, index=False)
    feature_path = tmp_path / "features.parquet"
    pd.DataFrame(
        {
            "ticker": ["000001.SZ", "601088.SH"],
            "delist_date": [pd.NaT, pd.NaT],
        }
    ).to_parquet(feature_path, index=False)
    suspension_path = tmp_path / "suspensions.parquet"
    _write_suspension_snapshot(
        suspension_path,
        pd.DataFrame(
        {
            "ticker": ["601088.SH"] * (len(suspension_dates) + 1),
            "date": [*suspension_dates, pd.Timestamp("2017-09-01")],
            "suspend_type": ["S"] * len(suspension_dates) + ["R"],
            "suspend_timing": [None] * (len(suspension_dates) + 1),
        }
        ),
        query_start=calendar.min().date().isoformat(),
        query_end=calendar.max().date().isoformat(),
    )

    loaded = runner_module._load_execution(
        execution_path,
        LongOnlyPortfolioConfig(),
        feature_path=feature_path,
        suspension_path=suspension_path,
    )

    suspended = loaded.loc[
        (loaded["ticker"] == "601088.SH")
        & loaded["date"].isin(suspension_dates)
    ]
    assert len(suspended) == 64
    assert suspended["is_suspended"].eq(True).all()
    assert suspended["open_adj"].isna().all()
    assert suspended["adv_20"].isna().all()
    resumed = loaded.loc[
        (loaded["ticker"] == "601088.SH")
        & (loaded["date"] == pd.Timestamp("2017-09-01"))
    ].iloc[0]
    assert bool(resumed["is_suspended"]) is False
    assert resumed["open_adj"] == 10.1

    resumed_evaluation = evaluate_long_only_portfolio(
        pd.DataFrame(
            {
                "date": [pd.Timestamp("2017-08-31")],
                "ticker": ["601088.SH"],
                "signal": [1.0],
            }
        ),
        "signal",
        LongOnlyPortfolioConfig(
            capital=100_000.0,
            holding_days=5,
            rebalance_every_days=5,
            position_count=1,
            target_weight=1.0,
            max_adv_participation=0.05,
            open_column="open_adj",
        ),
        pricing_frame=loaded,
    )
    resume_fill = next(
        row
        for row in resumed_evaluation.trades
        if row.get("status") == "executed"
    )
    assert resume_fill["date"] == "2017-09-01"
    assert resume_fill["execution_input_date"] == "2017-05-22"
    assert resume_fill["adv"] == 100_000_000.0
    assert resume_fill["execution_input_complete"] is True
    assert resumed_evaluation.max_execution_input_age_days == 102

    account = ExecutionAccount(
        cash=0.0,
        positions={
            "601088.SH": ExecutionPosition(
                ticker="601088.SH",
                quantity=512_600.0,
                last_price=10.0,
                last_observation_date="2017-05-22",
            )
        },
    )
    observation_date = pd.Timestamp("2017-06-21")
    observation = process_account_observation(
        account,
        loaded.loc[loaded["date"] == observation_date],
        observation_date=observation_date,
        policy=ExecutionPolicy(max_stale_position_age_days=21),
        columns=ExecutionColumns(
            open="open_adj",
            mark="open_adj",
            adv="adv_20",
            volatility="volatility_20",
        ),
    )
    diagnostic = observation.stale_position_diagnostics[0]
    assert diagnostic.ticker == "601088.SH"
    assert diagnostic.blocked_reason == "suspended"
    assert diagnostic.age_days == 30
    assert diagnostic.action == "carry"
    assert observation.nav == pytest.approx(5_126_000.0)

    injection = loaded.attrs["security_event_injection"]
    assert injection["suspension_status"] == "available"
    assert injection["suspension_source"] == "tushare_suspend_d"
    assert injection["suspension_source_row_count"] == 65
    assert injection["suspension_full_day_session_count"] == 64
    assert injection["suspension_open_intraday_session_count"] == 0
    assert injection["suspension_ignored_after_open_session_count"] == 0
    assert injection["suspension_resume_marker_count"] == 1
    assert injection["suspension_flagged_session_count"] == 64
    assert injection["suspension_security_count"] == 1
    assert injection["suspension_event_only_row_count"] == 64


def test_suspension_timing_blocks_0930_not_0931_and_delist_wins(
    tmp_path: Path,
) -> None:
    dates = pd.bdate_range("2026-05-04", periods=3)
    execution_path = tmp_path / "execution.parquet"
    pd.DataFrame(
        [
            {
                "date": date,
                "ticker": "MARKET",
                "open_adj": 10.0,
                "adv_20": 1_000_000.0,
                "volatility_20": 0.02,
            }
            for date in dates
        ]
        + [
            {
                "date": dates[1],
                "ticker": "OPEN",
                "open_adj": 11.0,
                "adv_20": 1_000_000.0,
                "volatility_20": 0.02,
            }
        ]
    ).to_parquet(execution_path, index=False)
    feature_path = tmp_path / "features.parquet"
    pd.DataFrame(
        {
            "ticker": ["MARKET", "OPEN", "LATE", "FULL", "DELIST"],
            "delist_date": [pd.NaT, pd.NaT, pd.NaT, pd.NaT, dates[0]],
        }
    ).to_parquet(feature_path, index=False)
    suspension_path = tmp_path / "suspensions.parquet"
    _write_suspension_snapshot(
        suspension_path,
        pd.DataFrame(
            {
                "ticker": [
                    "OPEN",
                    "OPEN",
                    "LATE",
                    "LATE",
                    "FULL",
                    "DELIST",
                    "OPEN",
                ],
                "date": [
                    dates[0],
                    dates[0],
                    dates[0],
                    dates[0],
                    dates[0],
                    dates[0],
                    dates[1],
                ],
                "suspend_type": ["S", "R", "S", "R", "S", "S", "R"],
                "suspend_timing": [
                    "09:30-10:00,10:01-14:57",
                    None,
                    "09:46-10:00",
                    None,
                    None,
                    "09:30-10:00",
                    None,
                ],
            }
        ),
        query_start=dates.min().date().isoformat(),
        query_end=dates.max().date().isoformat(),
    )

    loaded = runner_module._load_execution(
        execution_path,
        LongOnlyPortfolioConfig(),
        feature_path=feature_path,
        suspension_path=suspension_path,
    )

    open_event = loaded.loc[
        (loaded["ticker"] == "OPEN") & (loaded["date"] == dates[0])
    ].iloc[0]
    assert bool(open_event["is_suspended"]) is True
    assert pd.isna(open_event["open_adj"])
    assert loaded.loc[
        (loaded["ticker"] == "LATE") & (loaded["date"] == dates[0])
    ].empty
    full_event = loaded.loc[
        (loaded["ticker"] == "FULL") & (loaded["date"] == dates[0])
    ].iloc[0]
    assert bool(full_event["is_suspended"]) is True
    delist_event = loaded.loc[
        (loaded["ticker"] == "DELIST") & (loaded["date"] == dates[0])
    ].iloc[0]
    assert bool(delist_event["is_delisted"]) is True
    assert bool(delist_event["is_suspended"]) is False
    resume = loaded.loc[
        (loaded["ticker"] == "OPEN") & (loaded["date"] == dates[1])
    ].iloc[0]
    assert bool(resume["is_suspended"]) is False
    assert resume["open_adj"] == 11.0

    injection = loaded.attrs["security_event_injection"]
    assert injection["suspension_source_full_day_session_count"] == 1
    assert injection["suspension_source_open_intraday_session_count"] == 2
    assert injection["suspension_full_day_session_count"] == 1
    assert injection["suspension_open_intraday_session_count"] == 1
    assert injection["suspension_ignored_after_open_session_count"] == 1
    assert injection["suspension_resume_marker_count"] == 3
    assert injection["suspension_ignored_delisted_session_count"] == 1
    assert injection["suspension_flagged_session_count"] == 2


def _promotion_gate(**overrides: object) -> dict[str, object]:
    gate: dict[str, object] = {
        "validation_net_excess_annual_return_min": 0.0,
        "validation_net_sharpe_min": 0.0,
        "validation_information_ratio_min": 0.0,
        "validation_max_drawdown_min": -1.0,
        "positive_half_year_ratio_min": 0.0,
        "average_holding_count_min": 0,
        "capacity_violation_count_max": 0,
        "validation_excess_mean_bootstrap_lower_min": 0.0,
        "benchmark_return_coverage_min": 0.95,
        "execution_input_policy_match_ratio_min": 1.0,
        "execution_input_future_violation_count_max": 0,
        "execution_input_coverage_min": 1.0,
        "validation_observations_min": 2,
        "execution_period_coverage_min": 0.9,
        "signal_evaluable_date_ratio_min": 0.8,
        "signal_median_cross_section_coverage_min": 0.8,
    }
    gate.update(overrides)
    return gate


def _frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = pd.DatetimeIndex(
        [
            *pd.bdate_range("2017-01-03", periods=25),
            *pd.bdate_range("2023-01-03", periods=25),
            *pd.bdate_range("2025-01-03", periods=31),
        ]
    )
    tickers = [f"{index:06d}.SZ" for index in range(1, 13)]
    rows: list[dict[str, object]] = []
    for day_index, date in enumerate(dates):
        for ticker_index, ticker in enumerate(tickers):
            value = (ticker_index + 1) / len(tickers)
            rows.append(
                {
                    "date": date,
                    "ticker": ticker,
                    "book_yield": value,
                    "earnings_yield": value * 0.8 + 0.01,
                    "pb": 1.0 + value,
                    "roe": value * 0.2,
                    "volatility_20": 0.3 - value * 0.1,
                    "turnover_rate": 0.2 - value * 0.05,
                    "momentum_60": ((ticker_index * 5) % len(tickers))
                    / len(tickers)
                    + day_index * 1e-8,
                    "financial_available_date": date - pd.Timedelta(days=1),
                    "fundamental_roic": value * 10.0,
                    "fundamental_q_ocf_to_sales": value * 0.5,
                    "fundamental_debt_to_assets": 1.0 - value * 0.5,
                    "fundamental_age_days": 120,
                    "industry_pit": "A" if ticker_index < 6 else "B",
                    "total_mv": 1_000_000_000.0 * (ticker_index + 1),
                    "forward_return_5d_open": value * 0.02 + day_index * 1e-8,
                    "st_filter_status": "verified",
                    "eligible": ticker_index != 0,
                    "universe_member": True,
                }
            )
    features = pd.DataFrame(rows)
    execution_dates = dates.append(pd.bdate_range(dates[-1] + pd.Timedelta(days=1), periods=6))
    execution_rows: list[dict[str, object]] = []
    for day_index, date in enumerate(execution_dates):
        for ticker_index, ticker in enumerate(tickers):
            execution_rows.append(
                {
                    "date": date,
                    "ticker": ticker,
                    "open_adj": 10.0 + day_index * 0.01 + ticker_index * 0.001,
                    "adv_20": 1_000_000_000.0,
                    "volatility_20": 0.02,
                    "eligible": True,
                    "universe_member": True,
                    "is_one_price_limit_up": False,
                    "is_one_price_limit_down": False,
                }
            )
    return features, pd.DataFrame(execution_rows)


def test_full_runner_writes_resumable_two_stage_outputs(tmp_path: Path) -> None:
    features, execution = _frames()
    feature_path = tmp_path / "features.parquet"
    execution_path = tmp_path / "execution.parquet"
    features.to_parquet(feature_path, index=False)
    execution.to_parquet(execution_path, index=False)
    suspension_path = (
        tmp_path / "runtime" / "data" / "top500" / "suspensions.parquet"
    )
    suspension_rows = pd.DataFrame(
        {
            "ticker": [str(features.iloc[0]["ticker"])],
            "date": [pd.Timestamp(features.iloc[0]["date"])],
            "suspend_type": ["R"],
            "suspend_timing": [None],
        }
    )
    suspension_rows = _write_suspension_snapshot(
        suspension_path,
        suspension_rows,
        query_start=pd.Timestamp(execution["date"].min()).date().isoformat(),
        query_end=pd.Timestamp(execution["date"].max()).date().isoformat(),
    )
    repository = Path(__file__).resolve().parents[2]

    result = run_research(
        project_root=tmp_path,
        suite="next",
        mode="full",
        feature_path=feature_path,
        execution_path=execution_path,
        factors_path=repository / "configs" / "factors.json",
        research_config_path=repository / "configs" / "research.json",
        run_robustness=False,
    )

    assert result["status"] == "completed"
    assert result["control_factor"] == "earnings_yield_over_pb"
    assert result["price_accounting"] == {
        "price_basis": "adjusted_total_return",
        "execution_price_column": "open_adj",
        "price_source": (
            "mixed_akshare_hfq_tushare_raw_times_adj_factor_fallback"
        ),
        "corporate_action_mode": "embedded_in_adjusted_prices",
        "lot_size": 0,
        "explicit_split_dividend_events_enabled": False,
    }
    assert result["stage_b"][0]["portfolio"]["price_basis"] == (
        "adjusted_total_return"
    )
    assert result["stage_b"][0]["portfolio"]["corporate_action_mode"] == (
        "embedded_in_adjusted_prices"
    )
    assert 1 <= len(result["stage_b_selected"]) <= 4
    assert len(result["stage_a"]) == 5
    assert result["stage_a_selection"]["basis"] == "train_only"
    research_filter = result["data"]["research_universe_filter"]
    assert research_filter["columns_applied"] == ["eligible", "universe_member"]
    assert research_filter["excluded_row_count"] == len(features[features["eligible"] == False])
    run_dir = tmp_path / "runtime" / "runs" / result["run_id"]
    assert (run_dir / "summary.json").is_file()
    assert (run_dir / "report.md").is_file()
    assert (run_dir / "manifest.json").is_file()
    lineage_path = run_dir / "pit-lineage.json"
    assert lineage_path.is_file()
    lineage = json.loads(lineage_path.read_text(encoding="utf-8"))
    contract = lineage["contract"]
    assert contract["artifact_sha256"] == result["data"]["feature_sha256"]
    assert contract["execution_artifact_sha256"] == result["data"][
        "execution_sha256"
    ]
    assert contract["suspension_artifact_sha256"] == result["data"][
        "suspension_sha256"
    ]
    assert result["data"]["suspension_status"] == "available"
    assert result["data"]["suspension_path"] == str(suspension_path.resolve())
    assert result["data"]["suspension_metadata_sha256"] == (
        runner_module._sha256_file(
            suspension_path.with_name("suspensions.meta.json")
        )
    )
    suspension_injection = result["data"]["security_event_injection"]
    assert suspension_injection["suspension_status"] == "available"
    assert suspension_injection["suspension_resume_marker_count"] == 1
    assert suspension_injection["suspension_flagged_session_count"] == 0
    assert lineage["audit"]["investment_claim_allowed"] is False
    assert result["pit_lineage"]["investment_claim_allowed"] is False
    assert result["pit_lineage"]["contract_sha256"] == (
        runner_module._sha256_value(contract)
    )
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["manifest_sha256"] == runner_module._manifest_payload_sha256(
        manifest
    )
    lineage_manifest_row = next(
        row for row in manifest["files"] if row["path"] == "pit-lineage.json"
    )
    assert lineage_manifest_row["sha256"] == runner_module._sha256_file(
        lineage_path
    )
    suspension_input = next(
        row for row in manifest["inputs"] if row["role"] == "tushare_suspend_d"
    )
    suspension_metadata_path = suspension_path.with_name("suspensions.meta.json")
    assert suspension_input["role"] == "tushare_suspend_d"
    assert suspension_input["path"] == str(suspension_path.resolve())
    assert suspension_input["status"] == "available"
    assert suspension_input["size_bytes"] == suspension_path.stat().st_size
    assert suspension_input["sha256"] == runner_module._sha256_file(
        suspension_path
    )
    assert suspension_input["metadata_path"] == str(
        suspension_metadata_path.resolve()
    )
    assert suspension_input["metadata_size_bytes"] == (
        suspension_metadata_path.stat().st_size
    )
    assert suspension_input["metadata_sha256"] == runner_module._sha256_file(
        suspension_metadata_path
    )
    assert suspension_input["audit"] == result["data"][
        "suspension_snapshot_audit"
    ]
    assert json.loads((tmp_path / "runtime" / "runs" / "latest.json").read_text())["run_id"] == result["run_id"]
    report = (run_dir / "report.md").read_text(encoding="utf-8")
    assert "Stage A：训练段筛选" in report
    assert "审计段只允许否证" in report

    resumed = run_research(
        project_root=tmp_path,
        suite="next",
        mode="full",
        feature_path=feature_path,
        execution_path=execution_path,
        factors_path=repository / "configs" / "factors.json",
        research_config_path=repository / "configs" / "research.json",
        run_robustness=False,
    )
    assert resumed["run_fingerprint"] == result["run_fingerprint"]

    suspension_rows.loc[len(suspension_rows)] = {
        "ticker": str(features.iloc[1]["ticker"]),
        "date": pd.Timestamp(features.iloc[1]["date"]),
        "suspend_type": "R",
        "suspend_timing": None,
    }
    suspension_rows = _write_suspension_snapshot(
        suspension_path,
        suspension_rows,
        query_start=pd.Timestamp(execution["date"].min()).date().isoformat(),
        query_end=pd.Timestamp(execution["date"].max()).date().isoformat(),
    )
    changed_input = run_research(
        project_root=tmp_path,
        suite="next",
        mode="full",
        feature_path=feature_path,
        execution_path=execution_path,
        factors_path=repository / "configs" / "factors.json",
        research_config_path=repository / "configs" / "research.json",
        run_robustness=False,
    )
    assert changed_input["run_id"] != result["run_id"]
    assert changed_input["data"]["suspension_sha256"] != result["data"][
        "suspension_sha256"
    ]


def test_runtime_identity_changes_run_id_and_invalidates_completed_cache(
    tmp_path: Path, monkeypatch
) -> None:
    features, execution = _frames()
    feature_path = tmp_path / "features.parquet"
    execution_path = tmp_path / "execution.parquet"
    features.to_parquet(feature_path, index=False)
    execution.to_parquet(execution_path, index=False)
    repository = Path(__file__).resolve().parents[2]
    arguments = {
        "project_root": tmp_path,
        "suite": "next",
        "mode": "canary",
        "feature_path": feature_path,
        "execution_path": execution_path,
        "factors_path": repository / "configs" / "factors.json",
        "research_config_path": repository / "configs" / "research.json",
        "run_robustness": False,
    }
    identity_a = {
        "python": {"implementation": "CPython", "version": "3.10.1"},
        "packages": {
            "numpy": "1.0-a",
            "pandas": "2.0-a",
            "pyarrow": "3.0-a",
            "scipy": "4.0-a",
        },
    }
    identity_b = {
        **identity_a,
        "packages": {**identity_a["packages"], "pandas": "2.0-b"},
    }
    calls: list[str] = []
    original_portfolio_result = runner_module._portfolio_result

    def tracked_portfolio_result(*args, **kwargs):
        factor = kwargs.get("factor") or args[0]
        calls.append(str(factor.name))
        return original_portfolio_result(*args, **kwargs)

    monkeypatch.setattr(runner_module, "_portfolio_result", tracked_portfolio_result)
    monkeypatch.setattr(runner_module, "_runtime_identity", lambda: identity_a)

    first = run_research(**arguments)
    assert first["runtime_identity"] == identity_a
    assert calls

    calls.clear()
    resumed = run_research(**arguments)
    assert resumed["run_id"] == first["run_id"]
    assert calls == []

    monkeypatch.setattr(runner_module, "_runtime_identity", lambda: identity_b)
    changed_runtime = run_research(**arguments)

    assert changed_runtime["runtime_identity"] == identity_b
    assert changed_runtime["run_id"] != first["run_id"]
    assert changed_runtime["run_fingerprint"] != first["run_fingerprint"]
    assert calls


@pytest.mark.parametrize(
    "changed_role",
    ("features", "execution", "suspensions", "suspensions_metadata"),
)
def test_runner_fails_closed_when_input_changes_during_load(
    tmp_path: Path, monkeypatch, changed_role: str
) -> None:
    features, execution = _frames()
    feature_path = tmp_path / "features.parquet"
    execution_path = tmp_path / "execution.parquet"
    features.to_parquet(feature_path, index=False)
    execution.to_parquet(execution_path, index=False)
    suspension_path = (
        tmp_path / "runtime" / "data" / "top500" / "suspensions.parquet"
    )
    suspension_rows = _write_suspension_snapshot(
        suspension_path,
        pd.DataFrame(
            {
                "ticker": [str(features.iloc[0]["ticker"])],
                "date": [pd.Timestamp(execution["date"].min())],
                "suspend_type": ["R"],
                "suspend_timing": [None],
            }
        ),
        query_start=pd.Timestamp(execution["date"].min()).date().isoformat(),
        query_end=pd.Timestamp(execution["date"].max()).date().isoformat(),
    )
    suspension_metadata_path = suspension_path.with_name("suspensions.meta.json")
    original_load_execution = runner_module._load_execution

    def load_then_change_input(*args, **kwargs):
        loaded = original_load_execution(*args, **kwargs)
        if changed_role == "features":
            changed = features.copy()
            changed.loc[0, "book_yield"] = float(changed.loc[0, "book_yield"]) + 1.0
            changed.to_parquet(feature_path, index=False)
        elif changed_role == "execution":
            changed = execution.copy()
            changed.loc[0, "open_adj"] = float(changed.loc[0, "open_adj"]) + 1.0
            changed.to_parquet(execution_path, index=False)
        elif changed_role == "suspensions":
            changed = suspension_rows.copy()
            changed.loc[len(changed)] = {
                "ticker": "999999.SZ",
                "date": pd.Timestamp(execution["date"].min()),
                "suspend_type": "R",
                "suspend_timing": None,
            }
            changed.to_parquet(suspension_path, index=False)
        else:
            metadata = json.loads(
                suspension_metadata_path.read_text(encoding="utf-8")
            )
            metadata["race_marker"] = True
            suspension_metadata_path.write_text(
                json.dumps(metadata, sort_keys=True), encoding="utf-8"
            )
        return loaded

    monkeypatch.setattr(runner_module, "_load_execution", load_then_change_input)
    repository = Path(__file__).resolve().parents[2]

    with pytest.raises(RuntimeError, match="research input changed"):
        run_research(
            project_root=tmp_path,
            suite="next",
            mode="canary",
            feature_path=feature_path,
            execution_path=execution_path,
            factors_path=repository / "configs" / "factors.json",
            research_config_path=repository / "configs" / "research.json",
            run_robustness=False,
        )

    assert not list((tmp_path / "runtime" / "runs").glob("*/summary.json"))


def test_registered_recovery_builtin_runs_through_stage_a(tmp_path: Path) -> None:
    features, execution = _frames()
    feature_path = tmp_path / "features.parquet"
    execution_path = tmp_path / "execution.parquet"
    features.to_parquet(feature_path, index=False)
    execution.to_parquet(execution_path, index=False)
    repository = Path(__file__).resolve().parents[2]

    result = run_research(
        project_root=tmp_path,
        suite="recovery",
        mode="canary",
        feature_path=feature_path,
        execution_path=execution_path,
        factors_path=repository / "configs" / "factors.json",
        research_config_path=repository / "configs" / "research.json",
        run_robustness=False,
    )

    recovery = next(
        row for row in result["stage_a"] if row["factor_name"] == "pit_cashflow_quality"
    )
    assert recovery["selection_basis"] == "train_only"
    assert recovery["factor_name"] in {
        row["factor_name"] for row in result["stage_a"]
    }
    assert result["validated_count"] == 0
    assert result["search_status"] == "canary_smoke"
    assert {
        "eligible",
        "universe_member",
        "financial_available_date",
    }.issubset(result["pit_lineage"]["required_fields"])


def test_results_first_ranks_comparable_control_and_ensembles(tmp_path: Path) -> None:
    features, execution = _frames()
    feature_path = tmp_path / "features.parquet"
    execution_path = tmp_path / "execution.parquet"
    research_path = tmp_path / "research.json"
    features.to_parquet(feature_path, index=False)
    execution.to_parquet(execution_path, index=False)
    repository = Path(__file__).resolve().parents[2]
    research_config = json.loads(
        (repository / "configs" / "research.json").read_text(encoding="utf-8")
    )
    research_config["portfolio"].update(
        {"position_count": 3, "target_weight": 1 / 3, "retention_buffer": 0}
    )
    research_config["results_first"]["portfolio"].update(
        {"position_count": 3, "target_weight": 1 / 3, "retention_buffer": 0}
    )
    research_config["results_first"]["challenger_weights"] = [0.3, 0.7]
    research_path.write_text(json.dumps(research_config), encoding="utf-8")

    common = dict(
        project_root=tmp_path,
        suite="results-first",
        feature_path=feature_path,
        execution_path=execution_path,
        factors_path=repository / "configs" / "factors.json",
        research_config_path=research_path,
        run_robustness=False,
        resume=False,
    )
    canary = run_research(mode="canary", **common)
    assert canary["search_status"] == "results_first_canary_smoke"
    assert canary["results_first"]["ranking_available"] is False
    assert canary["results_first"]["best_historical_strategy"] is None
    assert canary["results_first"]["rankings"] == []

    result = run_research(mode="full", **common)

    rankings = result["results_first"]["rankings"]
    assert result["results_first"]["enabled"] is True
    assert result["results_first"]["optimization_scope"] == "all_observed_history"
    assert result["search_status"] == "results_first_historical_ranking_completed"
    assert result["search_stopped"] is False
    assert result["validated_count"] == 0
    assert len(result["stage_b_selected"]) == 11
    assert len(rankings) == 11
    assert result["results_first"]["excluded_from_ranking"] == []
    assert [row["rank"] for row in rankings] == list(range(1, 12))
    assert rankings == sorted(
        rankings,
        key=lambda row: (
            -float(row["historical_score"]),
            -float(row["net_annual_return"]),
            str(row["factor_name"]),
        ),
    )
    assert result["results_first"]["best_historical_strategy"] == rankings[0][
        "factor_name"
    ]
    assert result["portfolio_config"]["position_count"] == 3
    assert result["portfolio_config"]["rebalance_every_days"] == 10
    assert any(row["strategy_kind"] == "ensemble" for row in rankings)
    assert len({row["net_annual_return"] for row in rankings}) > 1
    ensemble = next(
        row
        for row in result["stage_a"]
        if str(row["factor_name"]).startswith("blend__")
    )
    assert ensemble["selection_basis"] == "pre_directed_components"
    assert "不声称独立 OOS" in render_report(result)


def _period(
    signal_date: str,
    start_date: str,
    end_date: str,
    net_return: float,
    benchmark_return: float = 0.0,
) -> dict[str, object]:
    return {
        "signal_date": signal_date,
        "start_date": start_date,
        "end_date": end_date,
        "net_return": net_return,
        "gross_return": net_return,
        "benchmark_return": benchmark_return,
        "holding_count": 50,
        "turnover": 0.1,
        "benchmark_expected_endpoint_count": 2,
        "benchmark_observed_endpoint_count": 2,
        "benchmark_complete_return_count": 1,
        "benchmark_missing_start_count": 0,
        "benchmark_missing_end_count": 0,
        "execution_input_policy": "previous_valid_ticker_observation",
        "execution_input_min_date": signal_date,
        "execution_input_max_date": signal_date,
        "execution_input_required_count": 1,
        "execution_input_observed_count": 1,
        "max_execution_input_age_days": 1,
        "capacity_violation_count": 0,
        "blocked_trade_count": 0,
        "costs": {"total": 0.0},
    }


def test_results_first_metrics_align_to_control_periods_and_expose_gaps() -> None:
    reference = [
        {
            "signal_date": "2025-01-03",
            "net_return": 0.05,
            "benchmark_return": 0.02,
            "active_return": 0.03,
        },
        {
            "signal_date": "2025-01-10",
            "net_return": -0.01,
            "benchmark_return": 0.01,
            "active_return": -0.02,
        },
    ]
    sparse = {
        "period_active_returns": [
            {
                "signal_date": "2025-01-03",
                "net_return": 0.10,
                "benchmark_return": 0.02,
                "active_return": 0.08,
            }
        ]
    }

    metrics = runner_module._results_first_metrics(
        sparse,
        {"results_first": {"incomplete_period_policy": "exclude_from_ranking"}},
        periods_per_year=2,
        reference_periods=reference,
    )

    assert metrics["observations"] == 2
    assert metrics["observed_strategy_periods"] == 1
    assert metrics["missing_strategy_periods"] == 1
    assert metrics["period_coverage"] == 0.5
    assert metrics["comparison_period_basis"] == "control_signal_dates"
    assert metrics["missing_period_score_policy"] == "cash_return_zero_diagnostic_only"


def test_window_metrics_require_the_whole_period_to_stay_inside_split() -> None:
    periods = [
        _period("2022-12-20", "2022-12-21", "2022-12-28", 0.01),
        _period("2022-12-27", "2022-12-28", "2023-01-05", 9.0),
        _period("2023-01-04", "2023-01-05", "2023-01-12", 0.02),
        _period("2024-12-27", "2024-12-30", "2025-01-07", 9.0),
    ]

    train = runner_module._window_metrics(
        periods, start="2017-01-01", end="2022-12-31", periods_per_year=252 / 5
    )
    validation = runner_module._window_metrics(
        periods, start="2023-01-01", end="2024-12-31", periods_per_year=252 / 5
    )

    assert train["observations"] == 1
    assert train["net_return"] == 0.01
    assert validation["observations"] == 1
    assert validation["net_return"] == 0.02


def test_window_metrics_report_uncertainty_coverage_and_annualized_turnover() -> None:
    periods = [
        _period(
            f"2023-01-{3 + index * 7:02d}",
            f"2023-01-{4 + index * 7:02d}",
            f"2023-01-{9 + index * 7:02d}",
            0.01,
            0.002,
        )
        for index in range(3)
    ]
    policy = ValidationSpec(bootstrap_samples=32, bootstrap_block_size=2)
    account_nav_path: list[dict[str, object]] = []
    nav = 1_000.0
    for index, row in enumerate(periods):
        start_sequence = len(account_nav_path)
        account_nav_path.append(
            {
                "sequence": start_sequence,
                "date": row["start_date"],
                "phase": "accounting_boundary",
                "nav": nav,
            }
        )
        account_nav_path.append(
            {
                "sequence": len(account_nav_path),
                "date": row["start_date"],
                "phase": "posttrade",
                "nav": nav,
            }
        )
        nav *= 1.0 + float(row["net_return"])
        end_sequence = len(account_nav_path)
        account_nav_path.append(
                {
                    "sequence": end_sequence,
                    "date": row["end_date"],
                    "phase": "daily_end",
                    "nav": nav,
                }
        )
        periods[index]["account_nav_path_start_sequence"] = start_sequence
        periods[index]["account_nav_path_end_sequence"] = end_sequence
        periods[index]["daily_nav_observation_count"] = 1
        periods[index]["accounting_boundary_date"] = row["start_date"]

    metrics = runner_module._window_metrics(
        periods,
        account_nav_path=account_nav_path,
        start="2023-01-01",
        end="2023-12-31",
        periods_per_year=252 / 5,
        bootstrap_spec=policy,
        bootstrap_key="test:validation",
    )

    assert metrics["benchmark_return_coverage"] == 1.0
    assert metrics["benchmark_endpoint_coverage"] == 1.0
    assert metrics["annualized_turnover"] == 5.04
    assert metrics["excess_return_mean_bootstrap_lower"] == 0.008
    passed, blockers = runner_module._gate(
        {
            **metrics,
            "signal_evaluable_date_ratio": 1.0,
            "signal_median_cross_section_coverage": 1.0,
        },
        {"promotion_gate": _promotion_gate()},
    )
    assert passed is True
    assert blockers == []

    failed, blockers = runner_module._gate(
        {**metrics, "excess_return_mean_bootstrap_lower": 0.0},
        {"promotion_gate": _promotion_gate()},
    )
    assert failed is False
    assert "validation_excess_bootstrap_lower_below_threshold" in blockers


def test_daily_nav_drawdown_exposes_round_trip_loss_hidden_by_period_endpoints() -> None:
    periods = [
        {
            **_period("2025-01-02", "2025-01-03", "2025-01-16", 0.0),
            "account_nav_path_start_sequence": 0,
                "account_nav_path_end_sequence": 3,
                "daily_nav_observation_count": 2,
                "accounting_boundary_date": "2025-01-03",
        }
    ]
    account_nav_path = [
        {
            "sequence": 0,
            "date": "2025-01-03",
            "phase": "accounting_boundary",
            "nav": 10.0,
        },
        {
            "sequence": 1,
            "date": "2025-01-03",
            "phase": "posttrade",
            "nav": 10.0,
        },
        {
            "sequence": 2,
            "date": "2025-01-08",
            "phase": "daily_end",
            "nav": 5.0,
        },
        {
            "sequence": 3,
            "date": "2025-01-16",
            "phase": "daily_end",
            "nav": 10.0,
        },
        # A later crash must not leak into the selected window.
        {
            "sequence": 4,
            "date": "2025-01-17",
            "phase": "accounting_boundary",
            "nav": 1.0,
        },
    ]

    window = runner_module._window_metrics(
        periods,
        account_nav_path=account_nav_path,
        start="2025-01-01",
        end="2025-01-16",
        periods_per_year=25.2,
    )
    results_first = runner_module._results_first_metrics(
        {
            "period_active_returns": [
                {
                    "signal_date": "2025-01-02",
                    "start_date": "2025-01-03",
                    "end_date": "2025-01-16",
                    "net_return": 0.0,
                    "benchmark_return": 0.0,
                    "active_return": 0.0,
                    "benchmark_return_coverage": 1.0,
                    "benchmark_endpoint_coverage": 1.0,
                    "account_nav_path_start_sequence": 0,
                        "account_nav_path_end_sequence": 3,
                        "daily_nav_observation_count": 2,
                        "accounting_boundary_date": "2025-01-03",
                }
            ],
            "account_nav_path": account_nav_path,
        },
        {"results_first": {"incomplete_period_policy": "exclude_from_ranking"}},
        periods_per_year=25.2,
    )

    assert window["net_return"] == 0.0
    assert window["max_drawdown"] == -0.5
    assert window["daily_nav_observations"] == 4
    assert window["account_nav_path_end_sequence"] == 3
    assert results_first["max_drawdown"] == -0.5
    assert results_first["daily_nav_path_complete"] is True


def test_control_improvement_requires_paired_simultaneous_confidence() -> None:
    dates = pd.date_range("2023-01-06", periods=100, freq="7D")

    def payload(name: str, differences: list[float]) -> dict[str, object]:
        return {
            "factor_name": name,
            "period_active_returns": [
                {
                    "signal_date": str(date.date()),
                    "end_date": str((date + pd.Timedelta(days=6)).date()),
                    "net_return": 0.001 + difference,
                }
                for date, difference in zip(dates, differences)
            ],
        }

    control = payload("control", [0.0] * len(dates))
    noisy = payload("noisy", [0.01 if index % 2 else -0.01 for index in range(len(dates))])
    strong = payload("strong", [0.002] * len(dates))
    policy = ValidationSpec(bootstrap_samples=128, bootstrap_block_size=8)
    config = {"promotion_gate": _promotion_gate(validation_observations_min=90)}

    noisy_result = runner_module._control_comparison(
        noisy, control, config, policy, correction_factor=2
    )
    strong_result = runner_module._control_comparison(
        strong, control, config, policy, correction_factor=2
    )

    assert noisy_result["passed"] is False
    assert "control_improvement_bootstrap_lower_not_positive" in noisy_result["blockers"]
    assert strong_result["passed"] is True
    assert strong_result["bootstrap"]["lower"] == 0.002
    assert strong_result["simultaneous_confidence_method"] == "bonferroni_fwer"


def test_audit_can_only_veto_a_train_admitted_factor() -> None:
    features, _ = _frames()
    audit_mask = features["date"] >= "2025-01-01"
    features.loc[audit_mask, "forward_return_5d_open"] *= -1.0
    policy = ValidationSpec(
        train_start="2017-01-01",
        train_end="2017-12-31",
        validation_start="2023-01-01",
        validation_end="2024-12-31",
        audit_start="2025-01-01",
        min_cross_section=5,
        min_train_positive_year_ratio=0.0,
        bootstrap_samples=16,
        audit_min_observations=2,
    )
    factor = FactorSpec(name="alpha", family="test", expression="book_yield")
    result = evaluate_stage_a(features[features["eligible"]].copy(), factor, policy)

    falsified, reasons = runner_module._audit_falsification(
        result,
        {
            "observations": 5,
            "information_ratio": -0.5,
            "excess_return_mean_bootstrap": {"upper": -0.001},
        },
        policy,
    )

    assert result.stage_b_eligible is True
    assert result.selection_basis == "train_only"
    assert falsified is True
    assert "audit_active_return_bootstrap_upper_negative" in reasons


def test_final_factor_state_manifest_recovery_and_robustness_fingerprint(
    tmp_path: Path, monkeypatch
) -> None:
    features, execution = _frames()
    feature_path = tmp_path / "features.parquet"
    execution_path = tmp_path / "execution.parquet"
    features.to_parquet(feature_path, index=False)
    execution.to_parquet(execution_path, index=False)
    repository = Path(__file__).resolve().parents[2]
    calls: list[str] = []

    def fake_portfolio_result(factor, validation, *args, **kwargs):
        calls.append(factor.name)
        is_control = factor.name == "earnings_yield_over_pb"
        metrics = {
            "observations": 10,
            "net_return": 0.1,
            "gross_return": 0.1,
            "benchmark_return": 0.0,
            "net_annual_return": 0.12 if is_control else 0.15,
            "benchmark_annual_return": 0.0,
            "net_excess_annual_return": 0.10 if is_control else 0.12,
            "net_sharpe": 1.0 if is_control else 1.2,
            "information_ratio": 0.8,
            "max_drawdown": -0.10,
            "positive_half_year_ratio": 0.75,
            "average_holding_count": 50.0,
            "actual_turnover": 0.1,
            "capacity_violation_count": 0,
            "blocked_trade_count": 0,
            "total_cost": 0.0,
        }
        return {
            "factor_name": factor.name,
            "family": factor.family,
            "factor": factor.to_dict(),
            "frozen_direction": validation.frozen_direction,
            "stage_a": validation.to_dict(),
            "portfolio": {"status": "ok"},
            "windows": {"train": metrics, "validation": metrics, "audit": metrics},
            "gate_passed": True,
            "gate_blockers": [],
            "beats_control": False,
            "validated": False,
        }

    monkeypatch.setattr(runner_module, "_portfolio_result", fake_portfolio_result)
    monkeypatch.setattr(
        runner_module,
        "_control_comparison",
        lambda *args, **kwargs: {"passed": True, "blockers": [], "bootstrap": {}},
    )
    # This test exercises artifact finalization, not similarity clustering.
    monkeypatch.setattr(runner_module, "diagnose_train_similarity", lambda *args: [])
    arguments = {
        "project_root": tmp_path,
        "suite": "next",
        "mode": "full",
        "feature_path": feature_path,
        "execution_path": execution_path,
        "factors_path": repository / "configs" / "factors.json",
        "research_config_path": repository / "configs" / "research.json",
        "run_robustness": False,
    }
    first = run_research(**arguments)
    run_dir = tmp_path / "runtime" / "runs" / first["run_id"]
    challenger = next(row for row in first["stage_b"] if row["factor_name"] != first["control_factor"])
    factor_path = run_dir / "factors" / f"{challenger['factor_name']}.json"
    factor_payload = json.loads(factor_path.read_text(encoding="utf-8"))
    assert factor_payload["result"]["beats_control"] is True
    assert factor_payload["result"]["validated"] is True
    assert runner_module._completed_run_valid(
        run_dir / "summary.json", run_dir, first["run_fingerprint"]
    )

    manifest_path = run_dir / "manifest.json"
    summary_path = run_dir / "summary.json"
    original_manifest_text = manifest_path.read_text(encoding="utf-8")
    original_summary_text = summary_path.read_text(encoding="utf-8")

    def write_resigned_manifest(payload: dict[str, object]) -> None:
        payload["manifest_sha256"] = runner_module._manifest_payload_sha256(
            payload
        )
        manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    broken_digest = json.loads(original_manifest_text)
    broken_digest["manifest_sha256"] = "0" * 64
    manifest_path.write_text(json.dumps(broken_digest), encoding="utf-8")
    assert not runner_module._completed_run_valid(
        summary_path, run_dir, first["run_fingerprint"]
    )

    for field, value in (("algorithm", "sha512"), ("run_id", "0" * 16)):
        tampered = json.loads(original_manifest_text)
        tampered[field] = value
        write_resigned_manifest(tampered)
        assert not runner_module._completed_run_valid(
            summary_path, run_dir, first["run_fingerprint"]
        )

    tampered_audit = json.loads(original_manifest_text)
    tampered_audit["inputs"][0]["audit"] = {"status": "tampered"}
    write_resigned_manifest(tampered_audit)
    assert not runner_module._completed_run_valid(
        summary_path, run_dir, first["run_fingerprint"]
    )

    tampered_summary = json.loads(original_summary_text)
    tampered_summary["run_id"] = "0" * 16
    summary_path.write_text(json.dumps(tampered_summary), encoding="utf-8")
    resigned_for_summary = json.loads(original_manifest_text)
    summary_row = next(
        row for row in resigned_for_summary["files"] if row["path"] == "summary.json"
    )
    summary_row["size_bytes"] = summary_path.stat().st_size
    summary_row["sha256"] = runner_module._sha256_file(summary_path)
    write_resigned_manifest(resigned_for_summary)
    assert not runner_module._completed_run_valid(
        summary_path, run_dir, first["run_fingerprint"]
    )

    summary_path.write_text(original_summary_text, encoding="utf-8")
    manifest_path.write_text(original_manifest_text, encoding="utf-8")
    assert runner_module._completed_run_valid(
        summary_path, run_dir, first["run_fingerprint"]
    )

    # A completed checkpoint with a modified artifact must not be trusted or
    # re-blessed from the corrupted per-factor cache.
    factor_payload["result"]["validated"] = False
    factor_path.write_text(json.dumps(factor_payload), encoding="utf-8")
    calls.clear()
    repaired = run_research(**arguments)
    assert repaired["run_id"] == first["run_id"]
    assert len(calls) == len(first["stage_b_selected"])
    repaired_factor = json.loads(factor_path.read_text(encoding="utf-8"))
    assert repaired_factor["result"]["validated"] is True
    assert runner_module._completed_run_valid(
        run_dir / "summary.json", run_dir, repaired["run_fingerprint"]
    )

    with_robustness = run_research(**{**arguments, "run_robustness": True})
    assert with_robustness["run_id"] != first["run_id"]


def test_audit_veto_stops_route_without_triggering_robustness(
    tmp_path: Path, monkeypatch
) -> None:
    features, execution = _frames()
    feature_path = tmp_path / "features.parquet"
    execution_path = tmp_path / "execution.parquet"
    features.to_parquet(feature_path, index=False)
    execution.to_parquet(execution_path, index=False)
    repository = Path(__file__).resolve().parents[2]

    def fake_portfolio_result(factor, validation, *args, **kwargs):
        is_control = factor.name == "earnings_yield_over_pb"
        metrics = {
            "net_excess_annual_return": 0.10 if is_control else 0.12,
            "net_sharpe": 1.0 if is_control else 1.2,
            "information_ratio": 0.8,
            "max_drawdown": -0.10,
        }
        falsified = not is_control
        return {
            "factor_name": factor.name,
            "family": factor.family,
            "windows": {"train": metrics, "validation": metrics, "audit": metrics},
            "gate_passed": True,
            "gate_blockers": [],
            "audit_falsified": falsified,
            "audit_status": "falsified" if falsified else "not_falsified",
            "audit_falsification_reasons": ["audit_rank_ic_non_positive"]
            if falsified
            else [],
            "beats_control": False,
            "validated": False,
        }

    monkeypatch.setattr(runner_module, "_portfolio_result", fake_portfolio_result)
    monkeypatch.setattr(runner_module, "diagnose_train_similarity", lambda *args: [])
    monkeypatch.setattr(
        runner_module,
        "_control_comparison",
        lambda *args, **kwargs: {"passed": True, "blockers": [], "bootstrap": {}},
    )
    monkeypatch.setattr(
        runner_module,
        "_run_robustness",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("audit must not trigger robustness")
        ),
    )

    result = run_research(
        project_root=tmp_path,
        suite="next",
        mode="full",
        feature_path=feature_path,
        execution_path=execution_path,
        factors_path=repository / "configs" / "factors.json",
        research_config_path=repository / "configs" / "research.json",
        run_robustness=True,
    )

    assert result["validated_count"] == 0
    assert result["pre_audit_confirmed_factors"]
    assert result["robustness"] is None
    assert result["search_status"] == "audit_falsified_stop"
    assert result["search_stopped"] is True


def test_robustness_aggregates_every_fixed_anchor_without_best_selection(
    tmp_path: Path, monkeypatch
) -> None:
    factor = FactorSpec(name="alpha", family="test", expression="book_yield")
    second_factor = FactorSpec(name="beta", family="test", expression="earnings_yield")
    calls: list[tuple[str, int]] = []
    by_offset = {
        0: (0.02, 1.00, 0.70, -0.10, 0.70),
        5: (0.04, 0.90, 0.60, -0.15, 0.65),
        10: (0.01, 0.85, 0.55, -0.20, 0.65),
        15: (-0.02, 0.40, 0.20, -0.30, 0.40),
    }

    def fake_portfolio_result(factor, validation, signal, features, execution, config, research_config):
        calls.append((factor.name, config.rebalance_offset_days))
        excess, sharpe, information_ratio, drawdown, positive_half_year = by_offset[
            config.rebalance_offset_days
        ]
        metrics = {
            "observations": 20,
            "net_excess_annual_return": excess,
            "net_sharpe": sharpe,
            "information_ratio": information_ratio,
            "max_drawdown": drawdown,
            "positive_half_year_ratio": positive_half_year,
            "average_holding_count": 50.0,
            "capacity_violation_count": 0,
            "excess_return_mean_bootstrap_lower": 0.001,
            "benchmark_return_coverage": 1.0,
            "execution_input_policy_match_ratio": 1.0,
            "execution_input_future_violation_count": 0,
            "execution_input_coverage": 1.0,
            "execution_period_coverage": 1.0,
            "signal_evaluable_date_ratio": 1.0,
            "signal_median_cross_section_coverage": 1.0,
        }
        gate_passed, blockers = runner_module._gate(metrics, research_config)
        return {
            "gate_passed": gate_passed,
            "gate_blockers": blockers,
            "windows": {
                "train": {**metrics, "net_excess_annual_return": excess + 0.01},
                "validation": metrics,
                "audit": {**metrics, "net_excess_annual_return": excess - 0.01},
            },
            "portfolio": {"status": "ok"},
        }

    monkeypatch.setattr(runner_module, "_portfolio_result", fake_portfolio_result)
    research_config = {
        "promotion_gate": _promotion_gate(
            validation_net_sharpe_min=0.8,
            validation_information_ratio_min=0.5,
            validation_max_drawdown_min=-0.25,
            positive_half_year_ratio_min=0.6,
            average_holding_count_min=40,
            validation_observations_min=10,
        ),
        "robustness": {
            "position_counts": [50],
            "rebalance_every_days": [20],
            "anchor_offsets_by_rebalance_days": {"20": [0, 5, 10, 15]},
            "minimum_anchor_pass_ratio": 0.75,
        },
    }
    payload = runner_module._run_robustness(
        [factor, second_factor],
        {factor.name: object(), second_factor.name: object()},
        {
            factor.name: pd.Series(dtype=float),
            second_factor.name: pd.Series(dtype=float),
        },
        pd.DataFrame(),
        pd.DataFrame(),
        runner_module.LongOnlyPortfolioConfig(),
        research_config,
        tmp_path / "robustness.json",
    )

    assert calls == [
        ("alpha", 0),
        ("alpha", 5),
        ("alpha", 10),
        ("alpha", 15),
        ("beta", 0),
        ("beta", 5),
        ("beta", 10),
        ("beta", 15),
    ]
    assert payload["selection_basis"] == "train_shortlist_order"
    assert [row["factor_name"] for row in payload["results"]] == ["alpha", "beta"]
    row = payload["results"][0]
    assert row["anchor_offsets"] == [0, 5, 10, 15]
    assert row["anchor_count"] == 4
    assert row["anchor_pass_ratio"] == 0.75
    assert row["median_gate_passed"] is True
    assert row["robust"] is True
    assert row["promotion_eligible"] is False
    assert row["window_statistics"]["validation"]["net_excess_annual_return"] == {
        "min": -0.02,
        "median": 0.015,
        "max": 0.04,
    }
    assert row["median_windows"]["validation"]["net_sharpe"] == 0.875

    report = render_report(
        {
            "run_id": "test",
            "suite": "next",
            "mode": "full",
            "data": {},
            "stage_a": [],
            "stage_b": [],
            "robustness": payload,
            "validated_factors": [],
        }
    )
    assert "不选择最佳锚点" in report
    assert "-2.00% / 1.50% / 4.00%" in report
    assert "75.00%" in report


def test_robustness_requires_every_anchor_to_pass_data_integrity() -> None:
    anchors = [
        {"gate_blockers": []},
        {"gate_blockers": []},
        {"gate_blockers": []},
        {"gate_blockers": ["future_execution_input_detected"]},
    ]

    assert runner_module._robustness_integrity_blockers(anchors) == [
        "future_execution_input_detected"
    ]


def test_full_walk_forward_runs_every_offset_with_strictly_matured_history(
    tmp_path: Path,
) -> None:
    """Exercise the real runner without the production-sized ten-offset matrix."""

    features, execution = _frames()
    feature_path = tmp_path / "features.parquet"
    execution_path = tmp_path / "execution.parquet"
    factors_path = tmp_path / "factors.json"
    research_path = tmp_path / "research.json"
    features.to_parquet(feature_path, index=False)
    execution.to_parquet(execution_path, index=False)
    factors_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "control": {
                    "name": "earnings_yield_over_pb",
                    "family": "value",
                    "expression": "earnings_yield / pb",
                    "direction_policy": "train_ic",
                    "role": "control",
                },
                "suites": {
                    "walk-forward": [
                        {
                            "name": "value_defensive_rank",
                            "family": "value",
                            "expression": (
                                "rank(book_yield) + rank(earnings_yield) + "
                                "rank(-volatility_20)"
                            ),
                            "direction_policy": "fixed",
                            "params": {"fixed_direction": 1},
                        }
                    ]
                },
            }
        ),
        encoding="utf-8",
    )
    research_path.write_text(
        json.dumps(
            {
                    "schema_version": 4,
                    "engine": "factor-lab/research/v6",
                "validation": {
                    "train_start": "2017-01-01",
                    "train_end": "2022-12-31",
                    "validation_start": "2023-01-01",
                    "validation_end": "2024-12-31",
                    "audit_start": "2025-01-01",
                    "holding_days": 2,
                    "min_cross_section": 5,
                    "bootstrap_samples": 8,
                    "bootstrap_block_size": 2,
                    "audit_min_observations": 2,
                },
                "portfolio": {
                    "capital": 1_000_000,
                    "holding_days": 2,
                    "rebalance_every_days": 2,
                    "rebalance_offset_days": 0,
                    "position_count": 3,
                    "retention_buffer": 0,
                    "target_weight": 1 / 3,
                    "periods_per_year": 126.0,
                    "open_column": "open_adj",
                    "adv_column": "adv_20",
                    "volatility_column": "volatility_20",
                },
                "promotion_gate": _promotion_gate(validation_observations_min=2),
                "control_comparison": {
                    "max_drawdown_worsening_tolerance": 1.0
                },
                "results_first": {
                    "incomplete_period_policy": "exclude_from_ranking"
                },
                "walk_forward": {
                    "candidate_factors": ["value_defensive_rank"],
                    "candidate_weights": [0.7],
                    "rebalance_offsets": [0, 1],
                    "phase_quantile": 0.2,
                    "portfolio": {
                        "holding_days": 2,
                        "rebalance_every_days": 2,
                        "rebalance_offset_days": 0,
                        "position_count": 3,
                        "retention_buffer": 0,
                        "target_weight": 1 / 3,
                        "periods_per_year": 126.0,
                    },
                    "selector": {
                        "lookback_trading_days": 20,
                        "minimum_completed_periods": 2,
                        "update_every_trading_days": 1,
                        "score_method": "net_sharpe",
                        "control_score_guard": 0.0,
                        "history_policy": (
                            "end_date_strictly_before_signal_date"
                        ),
                        "missing_signal_policy": "fallback_control",
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    common = {
        "project_root": tmp_path,
        "suite": "walk-forward",
        "resume": False,
        "feature_path": feature_path,
        "execution_path": execution_path,
        "factors_path": factors_path,
        "research_config_path": research_path,
        "run_robustness": False,
    }
    canary = run_research(
        mode="canary",
        **common,
    )
    assert canary["evidence_class"] == "engineering_smoke"
    assert canary["walk_forward"]["evidence_class"] == "engineering_smoke"
    assert canary["walk_forward"]["selector_executed"] is False
    assert canary["walk_forward"]["fixed_comparator"]["factor_name"] == (
        "fixed_registry_equal_weight"
    )
    assert all(
        row["account_role"] == "engineering_smoke_account"
        and row["cross_strategy_comparison_eligible"] is False
        and row["authoritative_comparison_artifact"] is None
        for row in canary["stage_b"]
    )

    result = run_research(
        mode="full",
        **common,
    )

    walk_forward = result["walk_forward"]
    assert result["schema_version"] == 4
    assert result["evidence_class"] == "post_selection_causal_simulation"
    assert walk_forward["evidence_class"] == result["evidence_class"]
    assert walk_forward["protocol"] == "causal_walk_forward"
    assert walk_forward["selector_executed"] is True
    assert walk_forward["rebalance_offsets"] == [0, 1]
    assert [row["rebalance_offset_days"] for row in walk_forward["offsets"]] == [
        0,
        1,
    ]
    assert walk_forward["future_selection_violation_count"] == 0
    assert walk_forward["causal_history_valid"] is True
    assert walk_forward["full_dynamic_period_coverage"] is True
    assert walk_forward["scoring_account_protocol"] == (
        "fresh_cash_equal_aum_common_start"
    )
    assert walk_forward["scoring_initial_nav"] == 1_000_000
    assert walk_forward["scoring_account_count"] == (
        walk_forward["expected_scoring_account_count"]
    )
    assert walk_forward["equal_aum_scoring_valid"] is True
    assert walk_forward["equal_aum_scoring_violations"] == []
    assert walk_forward["common_evaluation_start"] is not None
    assert walk_forward["benchmark_return_coverage_minimum"] == 0.95
    assert len(result["stage_b"]) == len(walk_forward["candidate_registry"])
    assert walk_forward["dynamic_factor"] not in result["stage_b_selected"]
    assert walk_forward["fixed_comparator_factor"] not in result["stage_b_selected"]
    assert all(
        row["account_role"] == "selector_shadow_full_history"
        and row["cross_strategy_comparison_eligible"] is False
        and row["authoritative_comparison_artifact"]
        == "walk-forward/walk-forward-summary.json"
        for row in result["stage_b"]
    )
    fixed_name = walk_forward["fixed_comparator"]["factor_name"]
    assert fixed_name == "fixed_registry_equal_weight"
    assert fixed_name not in walk_forward["candidate_registry"]
    assert fixed_name in {
        row["strategy_name"] for row in walk_forward["phase_rankings"]
    }
    for offset in walk_forward["offsets"]:
        assert offset["signal_date_count"] > 0
        assert offset["update_count"] > 0
        assert offset["future_selection_violation_count"] == 0
        assert len(offset["scoring_account_audits"]) == (
            len(walk_forward["candidate_registry"]) + 2
        )
        assert all(
            audit["valid"] is True
            for audit in offset["scoring_account_audits"]
        )
        assert all(
            audit["common_window_execution_integrity"]
            and all(
                window["valid"] is True
                and window["execution_input_coverage"] == 1.0
                and window["execution_input_future_violation_count"] == 0
                and window["capacity_violation_count"] == 0
                for window in audit["common_window_execution_integrity"]
            )
            for audit in offset["scoring_account_audits"]
        )
        assert all(
            float(metrics["benchmark_return_coverage_min"])
            >= walk_forward["benchmark_return_coverage_minimum"]
            for metrics in offset["metrics"].values()
        )
        assert all(
            metrics["daily_nav_path_complete"] is True
            and metrics["max_drawdown_basis"] == "daily_account_nav"
            and metrics["equal_aum_account_audit_valid"] is True
            for metrics in offset["metrics"].values()
        )

    run_dir = tmp_path / "runtime" / "runs" / result["run_id"]
    walk_root = run_dir / "walk-forward"
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    manifest_paths = {str(row["path"]) for row in manifest["files"]}
    walk_artifacts = {
        path.relative_to(run_dir).as_posix()
        for path in walk_root.rglob("*.json")
    }
    assert walk_artifacts
    assert walk_artifacts <= manifest_paths
    assert "walk-forward/walk-forward-summary.json" in manifest_paths
    assert (
        f"factors/{walk_forward['dynamic_factor']}.json" not in manifest_paths
    )
    assert f"factors/{fixed_name}.json" not in manifest_paths
    factor_artifacts = list((run_dir / "factors").glob("*.json"))
    assert len(factor_artifacts) == len(walk_forward["candidate_registry"])
    for path in factor_artifacts:
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload["result"]["account_role"] == (
            "selector_shadow_full_history"
        )
        assert payload["result"]["cross_strategy_comparison_eligible"] is False
        assert payload["result"]["authoritative_comparison_artifact"] == (
            "walk-forward/walk-forward-summary.json"
        )
    assert runner_module._completed_run_valid(
        run_dir / "summary.json",
        run_dir,
        result["run_fingerprint"],
    )

    candidate_registry = list(walk_forward["candidate_registry"])
    for offset in (0, 1):
        offset_dir = walk_root / f"offset-{offset:02d}"
        dynamic = json.loads(
            (offset_dir / "dynamic.json").read_text(encoding="utf-8")
        )
        fixed_comparator = json.loads(
            (offset_dir / "fixed-comparator.json").read_text(encoding="utf-8")
        )
        decisions_payload = json.loads(
            (offset_dir / "decisions.json").read_text(encoding="utf-8")
        )
        assert decisions_payload["role"] == "causal_selector_audit"
        assert fixed_comparator["role"] == (
            "equal_aum_fixed_comparator_scoring_account"
        )
        assert fixed_comparator["evaluation_start_date"] == (
            walk_forward["common_evaluation_start"]
        )
        assert fixed_comparator["result"]["factor_name"] == fixed_name
        assert fixed_comparator["result"]["portfolio"]["status"] == "ok"
        assert fixed_comparator["result"]["portfolio"]["total_cost"] > 0
        assert dynamic["role"] == "equal_aum_dynamic_scoring_account"
        assert dynamic["evaluation_start_date"] == (
            walk_forward["common_evaluation_start"]
        )
        assert dynamic["decisions_path"] == "decisions.json"
        assert "decisions" not in dynamic
        assert dynamic["decisions_sha256"] == decisions_payload["decisions_sha256"]
        assert dynamic["result"]["factor_name"] == walk_forward["dynamic_factor"]
        assert dynamic["result"]["portfolio"]["status"] == "ok"
        assert dynamic["result"]["portfolio"]["trade_count"] > 0
        assert dynamic["result"]["portfolio"]["total_cost"] > 0
        for scoring_payload in (fixed_comparator, dynamic):
            scoring_result = scoring_payload["result"]
            portfolio = scoring_payload["result"]["portfolio"]
            assert pd.Timestamp(portfolio["evaluation_start_date"]) >= pd.Timestamp(
                walk_forward["common_evaluation_start"]
            )
            assert portfolio["initial_nav"] == walk_forward["scoring_initial_nav"]
            assert portfolio["first_pretrade_nav"] == (
                walk_forward["scoring_initial_nav"]
            )
            assert portfolio["end_nav"] > 0
            account_nav_path = scoring_result["account_nav_path"]
            assert account_nav_path[0]["date"] == (
                walk_forward["common_evaluation_start"]
            )
            assert account_nav_path[0]["phase"] == "accounting_boundary"
            assert [row["sequence"] for row in account_nav_path] == list(
                range(len(account_nav_path))
            )
            assert account_nav_path[-1]["nav"] == pytest.approx(
                portfolio["end_nav"]
            )
            assert all(
                period["account_nav_path_start_sequence"]
                <= period["account_nav_path_end_sequence"]
                < len(account_nav_path)
                for period in scoring_result["period_active_returns"]
            )
            assert all(
                window["max_drawdown_basis"] == "daily_account_nav"
                and window["daily_nav_path_complete"] is True
                for window in scoring_result["windows"].values()
            )
        decisions = decisions_payload["decisions"]
        assert decisions["history_policy"] == (
            "end_date_strictly_before_signal_date"
        )
        assert decisions["future_selection_violation_count"] == 0
        for update in decisions["updates"]:
            if update["latest_used_end_date"] is not None:
                assert pd.Timestamp(update["latest_used_end_date"]) < pd.Timestamp(
                    update["decision_date"]
                )
        static_files = list((offset_dir / "static").glob("*.json"))
        assert len(static_files) == len(candidate_registry)
        assert all(
            json.loads(path.read_text(encoding="utf-8"))["role"]
            == "causal_shadow_candidate"
            for path in static_files
        )
        scoring_static_files = list(
            (offset_dir / "scoring" / "static").glob("*.json")
        )
        assert len(scoring_static_files) == len(candidate_registry)
        for path in scoring_static_files:
            scoring_payload = json.loads(path.read_text(encoding="utf-8"))
            assert scoring_payload["role"] == "equal_aum_static_scoring_account"
            assert scoring_payload["evaluation_start_date"] == (
                walk_forward["common_evaluation_start"]
            )
            assert scoring_payload["shadow_history_path"].startswith(
                "../../static/"
            )
            portfolio = scoring_payload["result"]["portfolio"]
            assert pd.Timestamp(portfolio["evaluation_start_date"]) >= pd.Timestamp(
                walk_forward["common_evaluation_start"]
            )
            assert portfolio["initial_nav"] == walk_forward["scoring_initial_nav"]
            assert portfolio["first_pretrade_nav"] == (
                walk_forward["scoring_initial_nav"]
            )
            assert portfolio["end_nav"] > 0

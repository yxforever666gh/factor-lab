from __future__ import annotations

import pandas as pd

from factor_lab.earnings_event_failure_diagnosis import (
    bucket_profile,
    build_failure_diagnosis,
    signal_turnover_proxy,
    ticker_concentration_diagnostics,
)


def _sample_frame() -> pd.DataFrame:
    rows = []
    for date in ["20200101", "20200102", "20200103", "20200104", "20200105"]:
        for i in range(10):
            signal = float(i)
            rows.append({
                "date": date,
                "ticker": f"{i:06d}.SZ",
                "high_express_diluted_roe_yoy": signal,
                "forward_return_5d": 0.01 * signal,
            })
    return pd.DataFrame(rows)


def test_bucket_profile_computes_q3_q0_spread() -> None:
    report = bucket_profile(_sample_frame(), signal_col="high_express_diluted_roe_yoy")
    assert report["spreads"]["Q3-Q0"]["spread_mean"] > 0
    assert report["best_pair"]["spread_mean"] >= report["spreads"]["Q3-Q0"]["spread_mean"]


def test_ticker_concentration_leave_one_out_flags_benchmark() -> None:
    report = ticker_concentration_diagnostics(_sample_frame(), signal_col="high_express_diluted_roe_yoy", benchmark_spread=0.001)
    assert report["active_tickers"] == 10
    assert len(report["leave_one_ticker_out"]) == 10
    assert report["all_leave_one_out_above_benchmark"] is True


def test_signal_turnover_proxy_detects_changes() -> None:
    df = _sample_frame()
    report = signal_turnover_proxy(df, signal_col="high_express_diluted_roe_yoy")
    assert report["mean_abs_diff"] == 0.0
    df.loc[df["ticker"] == "000000.SZ", "high_express_diluted_roe_yoy"] = [0, 1, 0, 1, 0]
    changed = signal_turnover_proxy(df, signal_col="high_express_diluted_roe_yoy")
    assert changed["mean_abs_diff"] > 0


def test_build_failure_diagnosis_stops_when_signal_tickers_are_too_few_and_workflows_fail() -> None:
    df = _sample_frame()
    df.loc[~df["ticker"].isin(["000000.SZ", "000001.SZ"]), "high_express_diluted_roe_yoy"] = pd.NA
    report = build_failure_diagnosis(
        df,
        summary={"controlled_workflow": {"standard_result": {"pass_gate": False, "fail_reason": "sharpe_net<1.0", "sharpe_net": -2.0}}},
        rolling_results=[{"pass_gate": False}],
        split_results=[{"pass_gate": False}],
    )
    assert report["decision"]["decision"] == "stop_earnings_event_not_robust"
    assert "signal_coverage_depends_on_few_tickers" in report["decision"]["reasons"]
    assert "rolling_workflow_all_failed_standard_gate" in report["decision"]["reasons"]

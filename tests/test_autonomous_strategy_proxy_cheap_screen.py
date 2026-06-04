import pandas as pd

from factor_lab.autonomous_strategy_proxy_cheap_screen import build_proxy_cheap_screen, build_proxy_feature_frame


def frame():
    rows = []
    for ticker, pb, pe, roe, profit, debt, cash, fwd in [
        ("cheap_good", 0.8, 8, 0.2, 1.0, 0.2, 2.0, 0.03),
        ("cheap_bad", 0.7, 7, 0.01, -1.0, 0.8, -2.0, -0.02),
        ("exp_good", 2.0, 20, 0.2, 1.0, 0.2, 2.0, 0.01),
        ("exp_bad", 2.2, 22, 0.01, -1.0, 0.8, -2.0, -0.01),
    ]:
        for day in range(12):
            rows.append(
                {
                    "date": pd.Timestamp("2020-01-01") + pd.Timedelta(days=day),
                    "ticker": ticker,
                    "industry": "i",
                    "pb": pb + (1 if ticker.startswith("cheap") and day < 6 else 0),
                    "pe_ttm": pe + (10 if ticker.startswith("cheap") and day < 6 else 0),
                    "forward_return_5d": fwd,
                    "roe": roe,
                    "profit_yoy": profit,
                    "debt_to_asset": debt,
                    "operating_cashflow_to_profit": cash,
                }
            )
    return pd.DataFrame(rows)


def test_build_proxy_feature_frame_overlays_pit_fields():
    base = frame().assign(profit_yoy=pd.NA, debt_to_asset=pd.NA, operating_cashflow_to_profit=pd.NA)
    pit = frame().loc[:, ["ticker", "date", "profit_yoy", "debt_to_asset", "operating_cashflow_to_profit"]].copy()
    merged = build_proxy_feature_frame(base_frame=base, pit_frame=pit)
    assert bool(merged["profit_yoy"].notna().all())
    assert bool(merged["debt_to_asset"].notna().all())


def test_proxy_cheap_screen_runs_candidates():
    screen = build_proxy_cheap_screen(
        run_id="r",
        frame=frame(),
        plan={"decision": "prepare_proxy_cheap_screen_execution"},
        source_path="x",
        window=6,
        min_periods=6,
        min_usable_rows=1,
    )
    names = {candidate["candidate"] for candidate in screen["candidate_results"]}
    assert "cheap_baseline_pb_pe" in names
    assert "combined_quality_profit_proxy" in names
    assert screen["best_candidate"] is not None
    assert screen["controlled_execution_allowed"] is False
    assert screen["queue_write_allowed"] is False


def test_proxy_cheap_screen_blocks_bad_plan():
    screen = build_proxy_cheap_screen(
        run_id="r",
        frame=frame(),
        plan={"decision": "block_proxy_cheap_screen_plan"},
        source_path="x",
    )
    assert screen["overall_status"] == "blocked"
    assert screen["recommended_next_step"] == "respect_proxy_cheap_screen_plan"

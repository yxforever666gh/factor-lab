import json

import pandas as pd

from factor_lab.paper_portfolio import build_paper_portfolio


def test_build_paper_portfolio_returns_empty_positions_for_empty_candidate_pool(tmp_path):
    dataset_path = tmp_path / "dataset.csv"
    output_dir = tmp_path / "paper_portfolio"

    frame = pd.DataFrame(
        [
            {"date": "2026-03-20", "ticker": "AAA", "forward_return_5d": 0.01, "momentum_20": 0.2},
            {"date": "2026-03-20", "ticker": "BBB", "forward_return_5d": -0.01, "momentum_20": -0.1},
        ]
    )
    frame.to_csv(dataset_path, index=False)

    payload = build_paper_portfolio(
        dataset_path=dataset_path,
        factor_definitions=[],
        output_dir=output_dir,
        strategy_name="paper_candidates_only",
    )

    assert payload["positions"] == []
    assert payload["position_count"] == 0
    assert payload["reason"] == "candidate_pool_empty"

    written = json.loads((output_dir / "current_portfolio.json").read_text(encoding="utf-8"))
    assert written == payload

    contribution = json.loads((output_dir / "portfolio_contribution_report.json").read_text(encoding="utf-8"))
    assert contribution["status"] == "empty"
    assert contribution["reason"] == "candidate_pool_empty"


def test_build_paper_portfolio_writes_selected_factors_and_contribution_report(tmp_path):
    dataset_path = tmp_path / "dataset.csv"
    output_dir = tmp_path / "paper_portfolio"

    frame = pd.DataFrame(
        [
            {"date": "2026-03-19", "ticker": "AAA", "forward_return_5d": 0.05, "momentum_20": 2.0, "value_ep": 1.5},
            {"date": "2026-03-19", "ticker": "BBB", "forward_return_5d": -0.02, "momentum_20": -1.0, "value_ep": -0.8},
            {"date": "2026-03-19", "ticker": "CCC", "forward_return_5d": 0.01, "momentum_20": 0.5, "value_ep": 0.1},
            {"date": "2026-03-19", "ticker": "DDD", "forward_return_5d": -0.03, "momentum_20": -0.8, "value_ep": -0.2},
            {"date": "2026-03-19", "ticker": "EEE", "forward_return_5d": 0.04, "momentum_20": 1.6, "value_ep": 1.1},
            {"date": "2026-03-19", "ticker": "FFF", "forward_return_5d": -0.01, "momentum_20": -0.3, "value_ep": -0.5},
            {"date": "2026-03-19", "ticker": "GGG", "forward_return_5d": 0.03, "momentum_20": 1.2, "value_ep": 0.8},
            {"date": "2026-03-19", "ticker": "HHH", "forward_return_5d": -0.04, "momentum_20": -1.5, "value_ep": -1.2},
            {"date": "2026-03-19", "ticker": "III", "forward_return_5d": 0.02, "momentum_20": 0.9, "value_ep": 0.4},
            {"date": "2026-03-19", "ticker": "JJJ", "forward_return_5d": -0.05, "momentum_20": -1.8, "value_ep": -1.4},
            {"date": "2026-03-20", "ticker": "AAA", "forward_return_5d": 0.04, "momentum_20": 2.2, "value_ep": 1.7},
            {"date": "2026-03-20", "ticker": "BBB", "forward_return_5d": -0.01, "momentum_20": -0.9, "value_ep": -0.6},
            {"date": "2026-03-20", "ticker": "CCC", "forward_return_5d": 0.01, "momentum_20": 0.6, "value_ep": 0.2},
            {"date": "2026-03-20", "ticker": "DDD", "forward_return_5d": -0.02, "momentum_20": -0.7, "value_ep": -0.1},
            {"date": "2026-03-20", "ticker": "EEE", "forward_return_5d": 0.05, "momentum_20": 1.8, "value_ep": 1.3},
            {"date": "2026-03-20", "ticker": "FFF", "forward_return_5d": -0.02, "momentum_20": -0.4, "value_ep": -0.3},
            {"date": "2026-03-20", "ticker": "GGG", "forward_return_5d": 0.02, "momentum_20": 1.1, "value_ep": 0.9},
            {"date": "2026-03-20", "ticker": "HHH", "forward_return_5d": -0.03, "momentum_20": -1.4, "value_ep": -1.0},
            {"date": "2026-03-20", "ticker": "III", "forward_return_5d": 0.03, "momentum_20": 1.0, "value_ep": 0.5},
            {"date": "2026-03-20", "ticker": "JJJ", "forward_return_5d": -0.04, "momentum_20": -1.7, "value_ep": -1.3},
        ]
    )
    frame.to_csv(dataset_path, index=False)

    payload = build_paper_portfolio(
        dataset_path=dataset_path,
        factor_definitions=[
            {"name": "momentum_20", "expression": "momentum_20", "allocated_weight": 0.7, "max_weight": 0.8, "portfolio_bucket": "core_alpha", "approval_tier": "core", "lifecycle_state": "approved", "budget_reason": "core_budget"},
            {"name": "value_ep", "expression": "value_ep", "allocated_weight": 0.3, "max_weight": 0.4, "portfolio_bucket": "controlled_exposure", "approval_tier": "bridge", "lifecycle_state": "watchlist", "budget_reason": "watchlist_budget"},
        ],
        output_dir=output_dir,
        strategy_name="paper_candidates_only",
    )

    assert [row["name"] for row in payload["selected_factors"]] == ["momentum_20", "value_ep"]
    assert [row["expression"] for row in payload["selected_factors"]] == ["momentum_20", "value_ep"]
    assert payload["selected_factors"][0]["allocated_weight"] == 0.7
    assert payload["selected_factors"][1]["lifecycle_state"] == "watchlist"

    contribution = json.loads((output_dir / "portfolio_contribution_report.json").read_text(encoding="utf-8"))
    assert contribution["status"] == "ok"
    assert contribution["forward_return_column"] == "forward_return_5d"
    assert len(contribution["rows"]) == 2
    assert {row["factor_name"] for row in contribution["rows"]} == {"momentum_20", "value_ep"}

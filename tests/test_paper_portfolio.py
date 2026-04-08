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

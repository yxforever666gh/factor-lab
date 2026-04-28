import json

import pandas as pd

from scripts.run_scheduled_cycle import load_candidate_factor_definitions, update_paper_portfolio


def test_load_candidate_factor_definitions_ignores_missing_file(tmp_path):
    missing = tmp_path / "candidate_pool.json"
    assert load_candidate_factor_definitions(missing) == []


def test_update_paper_portfolio_handles_empty_candidate_pool(tmp_path):
    candidate_pool_path = tmp_path / "candidate_pool.json"
    dataset_path = tmp_path / "dataset.csv"
    output_dir = tmp_path / "paper_portfolio"

    candidate_pool_path.write_text("[]", encoding="utf-8")
    pd.DataFrame(
        [
            {"date": "2026-03-20", "ticker": "AAA", "forward_return_5d": 0.01, "momentum_20": 0.2},
            {"date": "2026-03-20", "ticker": "BBB", "forward_return_5d": -0.01, "momentum_20": -0.1},
        ]
    ).to_csv(dataset_path, index=False)

    payload = update_paper_portfolio(
        candidate_pool_path=candidate_pool_path,
        dataset_path=dataset_path,
        output_dir=output_dir,
    )

    assert payload["positions"] == []
    assert payload["reason"] == "candidate_pool_empty"

    history = json.loads((output_dir / "portfolio_history.json").read_text(encoding="utf-8"))
    assert history[-1]["reason"] == "candidate_pool_empty"
    assert "当前持仓数：0" in (output_dir / "portfolio_change_log.md").read_text(encoding="utf-8")

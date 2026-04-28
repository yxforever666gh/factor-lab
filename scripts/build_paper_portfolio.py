from pathlib import Path
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.approved_universe import resolve_paper_portfolio_inputs
from factor_lab.paper_portfolio import (
    build_paper_portfolio,
    append_portfolio_history,
    build_portfolio_change_log,
)


if __name__ == "__main__":
    inputs = resolve_paper_portfolio_inputs(
        db_path="artifacts/factor_lab.db",
        approved_universe_path="artifacts/approved_candidate_universe.json",
        fallback_candidate_pool_path="artifacts/tushare_workflow/candidate_pool.json",
        fallback_dataset_path="artifacts/tushare_workflow/dataset.csv",
    )
    dataset_path = Path(inputs["dataset_path"]) if inputs.get("dataset_path") else Path("artifacts/tushare_workflow/dataset.csv")
    current = build_paper_portfolio(
        dataset_path=dataset_path,
        factor_definitions=inputs.get("factor_definitions") or [],
        output_dir="artifacts/paper_portfolio",
        strategy_name="paper_candidates_only",
        source_metadata={"source": inputs.get("source"), **(inputs.get("metadata") or {})},
    )
    append_portfolio_history(
        current_path="artifacts/paper_portfolio/current_portfolio.json",
        history_path="artifacts/paper_portfolio/portfolio_history.json",
    )
    build_portfolio_change_log(
        current_path="artifacts/paper_portfolio/current_portfolio.json",
        history_path="artifacts/paper_portfolio/portfolio_history.json",
        output_path="artifacts/paper_portfolio/portfolio_change_log.md",
    )
    print(f"paper portfolio built from source={inputs.get('source')} dataset={dataset_path}")

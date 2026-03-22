from pathlib import Path
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.paper_portfolio import build_paper_portfolio, append_portfolio_history, build_portfolio_change_log


if __name__ == "__main__":
    candidates = json.loads(Path("artifacts/tushare_workflow/candidate_pool.json").read_text(encoding="utf-8"))
    current = build_paper_portfolio(
        dataset_path="artifacts/tushare_workflow/dataset.csv",
        factor_definitions=[{"name": row["factor_name"], "expression": row["expression"]} for row in candidates],
        output_dir="artifacts/paper_portfolio",
        strategy_name="paper_candidates_only",
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
    print("paper portfolio built")

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.batch import run_batch
from factor_lab.change_detection import build_change_report
from factor_lab.reporting import write_sqlite_report
from factor_lab.html_report import build_html_report
from factor_lab.index_page import build_index_page
from factor_lab.summary import build_run_summary
from factor_lab.paper_portfolio import build_paper_portfolio, append_portfolio_history, build_portfolio_change_log
import json


if __name__ == "__main__":
    run_batch(
        config_path="configs/tushare_batch.json",
        output_dir="artifacts/tushare_batch",
    )
    write_sqlite_report(
        db_path="artifacts/factor_lab.db",
        output_path="artifacts/sqlite_report.md",
    )
    build_html_report(
        db_path="artifacts/factor_lab.db",
        output_path="artifacts/report.html",
    )
    build_index_page(
        db_path="artifacts/factor_lab.db",
        output_path="artifacts/index.html",
    )
    build_run_summary(
        db_path="artifacts/factor_lab.db",
        output_path="artifacts/latest_summary.txt",
    )
    build_change_report(
        db_path="artifacts/factor_lab.db",
        output_path="artifacts/change_report.md",
    )

    candidates = json.loads(Path("artifacts/tushare_workflow/candidate_pool.json").read_text(encoding="utf-8"))
    build_paper_portfolio(
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

    from factor_lab.paper_portfolio_retrospective import build_portfolio_retrospective, build_portfolio_stability_score
    build_portfolio_retrospective(
        history_path="artifacts/paper_portfolio/portfolio_history.json",
        output_path="artifacts/paper_portfolio/portfolio_retrospective.json",
    )
    build_portfolio_stability_score(
        retro_path="artifacts/paper_portfolio/portfolio_retrospective.json",
        output_path="artifacts/paper_portfolio/portfolio_stability_score.json",
    )

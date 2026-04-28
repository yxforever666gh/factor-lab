from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.batch import run_batch
from factor_lab.change_detection import build_change_report
from factor_lab.html_report import build_html_report
from factor_lab.index_page import build_index_page
from factor_lab.reporting import write_sqlite_report
from factor_lab.robustness_batch import build_robustness_batch
from factor_lab.summary import build_run_summary
from factor_lab.promotion_scorecard import write_promotion_scorecard


if __name__ == "__main__":
    build_robustness_batch(
        db_path="artifacts/factor_lab.db",
        base_config_path="configs/tushare_workflow.json",
        batch_config_path="artifacts/generated_robustness_batches/top_promotion_candidates.json",
        batch_output_dir="artifacts/robustness_validation_batch",
        plan_output_path="artifacts/robustness_batch_plan.json",
        top_n=5,
    )
    run_batch(
        config_path="artifacts/generated_robustness_batches/top_promotion_candidates.json",
        output_dir="artifacts/robustness_validation_batch",
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
    write_promotion_scorecard(
        db_path="artifacts/factor_lab.db",
        output_path="artifacts/promotion_scorecard.json",
    )
    build_change_report(
        db_path="artifacts/factor_lab.db",
        output_path="artifacts/change_report.md",
    )

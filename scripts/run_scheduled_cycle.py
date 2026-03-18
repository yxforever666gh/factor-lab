from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.batch import run_batch
from factor_lab.reporting import write_sqlite_report
from factor_lab.html_report import build_html_report
from factor_lab.index_page import build_index_page
from factor_lab.summary import build_run_summary


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

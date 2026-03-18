from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.batch import run_batch
from factor_lab.reporting import write_sqlite_report
from factor_lab.html_report import build_html_report


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

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.html_report import build_html_report


if __name__ == "__main__":
    build_html_report(
        db_path="artifacts/factor_lab.db",
        output_path="artifacts/report.html",
    )

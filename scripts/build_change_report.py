from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.change_detection import build_change_report


if __name__ == "__main__":
    build_change_report(
        db_path="artifacts/factor_lab.db",
        output_path="artifacts/change_report.md",
    )

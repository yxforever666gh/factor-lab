from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.summary import build_run_summary


if __name__ == "__main__":
    build_run_summary(
        db_path="artifacts/factor_lab.db",
        output_path="artifacts/latest_summary.txt",
    )

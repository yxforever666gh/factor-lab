from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.promotion_scorecard import write_promotion_scorecard
from factor_lab.summary import build_run_summary


if __name__ == "__main__":
    db_path = "artifacts/factor_lab.db"
    build_run_summary(
        db_path=db_path,
        output_path="artifacts/latest_summary.txt",
    )
    write_promotion_scorecard(
        db_path=db_path,
        output_path="artifacts/promotion_scorecard.json",
    )

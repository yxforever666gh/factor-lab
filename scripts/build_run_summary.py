from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.promotion_scorecard import write_promotion_scorecard
from factor_lab.summary import build_run_summary
from factor_lab.au_zero_diagnosis import write_au_zero_diagnosis
from factor_lab.artifact_consistency import write_artifact_consistency_report
from factor_lab.factor_quality_effect_report import write_factor_quality_effect_report
from factor_lab.quality_not_proven_root_cause import write_quality_not_proven_root_cause_report


if __name__ == "__main__":
    db_path = "artifacts/factor_lab.db"
    write_promotion_scorecard(
        db_path=db_path,
        output_path="artifacts/promotion_scorecard.json",
    )
    build_run_summary(
        db_path=db_path,
        output_path="artifacts/latest_summary.txt",
    )
    write_au_zero_diagnosis(
        db_path=db_path,
        artifacts_dir="artifacts",
    )
    write_artifact_consistency_report(
        db_path=db_path,
        artifacts_dir="artifacts",
    )
    write_factor_quality_effect_report(
        db_path=db_path,
        artifacts_dir="artifacts",
    )
    write_quality_not_proven_root_cause_report(
        artifacts_dir="artifacts",
    )
    build_run_summary(
        db_path=db_path,
        output_path="artifacts/latest_summary.txt",
    )

from pathlib import Path
import sys
import json

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.robustness_batch import build_robustness_batch


if __name__ == "__main__":
    payload = build_robustness_batch(
        db_path="artifacts/factor_lab.db",
        base_config_path="configs/tushare_workflow.json",
        batch_config_path="artifacts/generated_robustness_batches/top_promotion_candidates.json",
        batch_output_dir="artifacts/robustness_validation_batch",
        plan_output_path="artifacts/robustness_batch_plan.json",
        top_n=5,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))

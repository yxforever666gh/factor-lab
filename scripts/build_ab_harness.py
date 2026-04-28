from pathlib import Path
import argparse
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.ab_harness import build_ab_harness_plan


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default="artifacts/factor_lab.db")
    parser.add_argument("--base-config", default="configs/tushare_workflow.json")
    parser.add_argument("--output-root", default="artifacts/ab_harness")
    parser.add_argument("--top-n", type=int, default=3)
    args = parser.parse_args()

    payload = build_ab_harness_plan(
        db_path=args.db_path,
        base_config_path=args.base_config,
        output_root=args.output_root,
        top_n=args.top_n,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))

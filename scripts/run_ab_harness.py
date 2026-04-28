from pathlib import Path
import argparse
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.ab_harness import build_ab_harness_plan, summarize_ab_harness, write_ab_harness_markdown
from factor_lab.batch import run_batch


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default="artifacts/factor_lab.db")
    parser.add_argument("--base-config", default="configs/tushare_workflow.json")
    parser.add_argument("--output-root", default="artifacts/ab_harness")
    parser.add_argument("--top-n", type=int, default=3)
    parser.add_argument("--build-only", action="store_true")
    parser.add_argument("--refresh-input-snapshot", action="store_true")
    args = parser.parse_args()

    plan = build_ab_harness_plan(
        db_path=args.db_path,
        base_config_path=args.base_config,
        output_root=args.output_root,
        top_n=args.top_n,
        refresh_input_snapshot=args.refresh_input_snapshot,
    )
    output_root = Path(args.output_root)
    summary_json_path = output_root / "summary.json"
    summary_md_path = output_root / "summary.md"

    if not args.build_only:
        # Clear previous summaries first so a failed rerun cannot masquerade as fresh output.
        for path in (summary_json_path, summary_md_path):
            if path.exists():
                path.unlink()
        for mode, payload in (plan.get("modes") or {}).items():
            run_batch(
                config_path=payload["batch_config_path"],
                output_dir=payload["batch_output_dir"],
            )

    summary = summarize_ab_harness(
        plan_path=output_root / "plan.json",
        db_path=args.db_path,
        output_path=summary_json_path,
    )
    write_ab_harness_markdown(summary, summary_md_path)
    print(json.dumps(summary, ensure_ascii=False, indent=2))

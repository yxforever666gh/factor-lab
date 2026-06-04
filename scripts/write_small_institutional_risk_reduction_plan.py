#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from factor_lab.small_institutional_risk_reduction_plan import (
    DEFAULT_DATASET_PATH,
    DEFAULT_JSON_PATH,
    DEFAULT_MARKDOWN_PATH,
    DEFAULT_MATRIX_PATH,
    DEFAULT_POLICY_PATH,
    write_risk_reduction_plan,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Write a manual-review-only small institutional risk reduction plan.")
    parser.add_argument("--policy-path", default=str(DEFAULT_POLICY_PATH))
    parser.add_argument("--matrix-path", default=str(DEFAULT_MATRIX_PATH))
    parser.add_argument("--dataset-path", default=None)
    parser.add_argument("--json-path", default=str(DEFAULT_JSON_PATH))
    parser.add_argument("--markdown-path", default=str(DEFAULT_MARKDOWN_PATH))
    parser.add_argument("--max-next-backtests", type=int, default=120)
    args = parser.parse_args()

    payload = write_risk_reduction_plan(
        policy_path=args.policy_path,
        matrix_path=args.matrix_path,
        dataset_path=args.dataset_path,
        json_path=args.json_path,
        markdown_path=args.markdown_path,
        max_next_backtests=args.max_next_backtests,
    )
    print(
        json.dumps(
            {
                "plan_status": payload.get("plan_status"),
                "candidate_count": payload.get("candidate_count"),
                "automation_allowed": payload.get("automation_allowed"),
                "manual_review_required": payload.get("manual_review_required"),
                "queue_write_allowed": payload.get("queue_write_allowed"),
                "json_path": args.json_path,
                "markdown_path": args.markdown_path,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

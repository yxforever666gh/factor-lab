#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from factor_lab.small_institutional_risk_reduction_executor import (
    DEFAULT_JSON_PATH,
    DEFAULT_MARKDOWN_PATH,
    DEFAULT_PLAN_PATH,
    DEFAULT_REPAIR_JSON_PATH,
    DEFAULT_REPAIR_MARKDOWN_PATH,
    DEFAULT_POLICY_PATH,
    write_risk_reduction_executor_results,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run capped manual-review-only small institutional risk-reduction executor.")
    parser.add_argument("--plan-path", default=str(DEFAULT_PLAN_PATH))
    parser.add_argument("--dataset-path", default=None)
    parser.add_argument("--max-candidates", type=int, default=20)
    parser.add_argument("--json-path", default=str(DEFAULT_JSON_PATH))
    parser.add_argument("--markdown-path", default=str(DEFAULT_MARKDOWN_PATH))
    parser.add_argument("--repair-json-path", default=str(DEFAULT_REPAIR_JSON_PATH))
    parser.add_argument("--repair-markdown-path", default=str(DEFAULT_REPAIR_MARKDOWN_PATH))
    parser.add_argument("--policy-path", default=str(DEFAULT_POLICY_PATH))
    args = parser.parse_args()

    payload = write_risk_reduction_executor_results(
        plan_path=args.plan_path,
        dataset_path=args.dataset_path,
        max_candidates=args.max_candidates,
        json_path=args.json_path,
        markdown_path=args.markdown_path,
        repair_json_path=args.repair_json_path,
        repair_markdown_path=args.repair_markdown_path,
        policy_path=args.policy_path,
    )
    print(
        json.dumps(
            {
                "matrix_status": payload.get("matrix_status"),
                "summary": payload.get("summary"),
                "execution": payload.get("execution"),
                "automation_allowed": payload.get("automation_allowed"),
                "manual_review_required": payload.get("manual_review_required"),
                "queue_write_allowed": payload.get("queue_write_allowed"),
                "live_trading_enabled": payload.get("live_trading_enabled"),
                "json_path": args.json_path,
                "markdown_path": args.markdown_path,
                "repair_json_path": args.repair_json_path,
                "repair_markdown_path": args.repair_markdown_path,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

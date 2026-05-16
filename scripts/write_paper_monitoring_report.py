#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.paper_monitoring_report import (
    DEFAULT_CURRENT_PORTFOLIO_PATH,
    DEFAULT_DIAGNOSTICS_PATH,
    DEFAULT_DRY_RUN_PATH,
    DEFAULT_JSON_PATH,
    DEFAULT_MARKDOWN_PATH,
    DEFAULT_RUNTIME_AUDIT_PATH,
    DEFAULT_STATUS_PATH,
    write_paper_monitoring_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write Factor Lab weekly paper monitoring report.")
    parser.add_argument("--current-portfolio-path", default=str(DEFAULT_CURRENT_PORTFOLIO_PATH))
    parser.add_argument("--diagnostics-path", default=str(DEFAULT_DIAGNOSTICS_PATH))
    parser.add_argument("--status-path", default=str(DEFAULT_STATUS_PATH))
    parser.add_argument("--dry-run-path", default=str(DEFAULT_DRY_RUN_PATH))
    parser.add_argument("--runtime-audit-path", default=str(DEFAULT_RUNTIME_AUDIT_PATH))
    parser.add_argument("--json-path", default=str(DEFAULT_JSON_PATH))
    parser.add_argument("--markdown-path", default=str(DEFAULT_MARKDOWN_PATH))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    payload = write_paper_monitoring_report(
        current_portfolio_path=args.current_portfolio_path,
        diagnostics_path=args.diagnostics_path,
        status_path=args.status_path,
        dry_run_path=args.dry_run_path,
        runtime_audit_path=args.runtime_audit_path,
        json_path=args.json_path,
        markdown_path=args.markdown_path,
    )
    print(
        json.dumps(
            {
                "strategy_name": (payload.get("portfolio") or {}).get("strategy_name"),
                "benchmark_id": (payload.get("benchmark") or {}).get("benchmark_id"),
                "runtime_safe": (payload.get("runtime") or {}).get("safe"),
                "would_run_count": (payload.get("runtime") or {}).get("would_run_count"),
                "missing_artifacts": payload.get("missing_artifacts"),
            },
            ensure_ascii=False,
        )
    )

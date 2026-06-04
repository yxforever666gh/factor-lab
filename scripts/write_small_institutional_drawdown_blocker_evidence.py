#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.small_institutional_drawdown_blocker_evidence import (
    DEFAULT_GROUP_PATH,
    DEFAULT_DIAGNOSTICS_PATH,
    DEFAULT_JSON_PATH,
    DEFAULT_MARKDOWN_PATH,
    DEFAULT_REPAIR_PATH,
    DEFAULT_STATUS_PATH,
    write_drawdown_blocker_evidence,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write small institutional drawdown blocker evidence pack.")
    parser.add_argument("--status-path", default=str(DEFAULT_STATUS_PATH))
    parser.add_argument("--repair-path", default=str(DEFAULT_REPAIR_PATH))
    parser.add_argument("--group-path", default=str(DEFAULT_GROUP_PATH))
    parser.add_argument("--diagnostics-path", default=str(DEFAULT_DIAGNOSTICS_PATH))
    parser.add_argument("--json-path", default=str(DEFAULT_JSON_PATH))
    parser.add_argument("--markdown-path", default=str(DEFAULT_MARKDOWN_PATH))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    payload = write_drawdown_blocker_evidence(
        status_path=args.status_path,
        repair_path=args.repair_path,
        group_path=args.group_path,
        diagnostics_path=args.diagnostics_path,
        json_path=args.json_path,
        markdown_path=args.markdown_path,
    )
    print(
        json.dumps(
            {
                "primary_issue": (payload.get("blocker") or {}).get("primary_issue"),
                "repair_status": (payload.get("repair") or {}).get("repair_status"),
                "candidate_count": (payload.get("repair") or {}).get("candidate_count"),
                "queue_write_allowed": (payload.get("safety") or {}).get("queue_write_allowed"),
                "broad_daemon_allowed": (payload.get("safety") or {}).get("broad_daemon_allowed"),
            },
            ensure_ascii=False,
        )
    )

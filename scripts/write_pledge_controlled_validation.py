#!/usr/bin/env python3
from __future__ import annotations

import json

from factor_lab.pledge_controlled_validation import write_pledge_controlled_validation


def main() -> None:
    report = write_pledge_controlled_validation()
    print(
        json.dumps(
            {
                "decision": report.get("decision"),
                "spread": report.get("recomputed_bucket_aware_result", {}).get("spread_mean"),
                "coverage": report.get("coverage"),
                "artifacts": report.get("artifact_paths"),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

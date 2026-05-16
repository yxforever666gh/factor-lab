#!/usr/bin/env python3
from __future__ import annotations

import json

from factor_lab.small_institutional_dataset_preflight import write_small_institutional_dataset_preflight


if __name__ == "__main__":
    payload = write_small_institutional_dataset_preflight()
    print(
        json.dumps(
            {
                "preflight_status": payload.get("preflight_status"),
                "next_action": payload.get("next_action"),
                "dataset": payload.get("dataset"),
                "estimated_combinations": payload.get("estimated_combinations"),
            },
            ensure_ascii=False,
            indent=2,
        )
    )

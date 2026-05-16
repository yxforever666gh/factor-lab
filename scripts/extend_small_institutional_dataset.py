#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from factor_lab.small_institutional_dataset_extender import extend_small_institutional_dataset


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Safely extend the small-institutional simulation dataset from feature store/Tushare cache.")
    parser.add_argument("--write", action="store_true", help="Write the rebuilt dataset.csv if validation passes.")
    parser.add_argument("--allow-fetch", action="store_true", help="Allow external Tushare/cache fetch if existing feature store does not cover the required window.")
    parser.add_argument("--policy", default=None, help="Optional simulation policy path.")
    args = parser.parse_args()
    payload = extend_small_institutional_dataset(policy_path=args.policy, write=args.write, allow_fetch=args.allow_fetch)
    print(
        json.dumps(
            {
                "extension_status": payload.get("extension_status"),
                "next_action": payload.get("next_action"),
                "write_performed": payload.get("write_performed"),
                "api_fetch_required": payload.get("api_fetch_required"),
                "required_window": payload.get("required_window"),
                "current_dataset": payload.get("current_dataset"),
                "validation": payload.get("validation"),
            },
            ensure_ascii=False,
            indent=2,
        )
    )

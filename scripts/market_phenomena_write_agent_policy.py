#!/usr/bin/env python
from __future__ import annotations

import argparse
from datetime import datetime, timezone

from factor_lab.market_phenomena_agent_policy import build_agent_policy, validate_agent_policy, write_agent_policy


def main() -> None:
    parser = argparse.ArgumentParser(description="Write market phenomena agent boundary/research policy artifacts.")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--output-dir", default="artifacts/market_phenomena")
    args = parser.parse_args()

    run_id = args.run_id or "agent_policy_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    policy = build_agent_policy(run_id=run_id)
    validation = validate_agent_policy(policy)
    if validation["decision"] != "keep":
        raise SystemExit(f"invalid policy: {validation}")
    paths = write_agent_policy(policy, args.output_dir)
    print(f"wrote {paths['json']}")
    print(f"wrote {paths['markdown']}")


if __name__ == "__main__":
    main()

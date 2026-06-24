#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from factor_lab.market_phenomena_research_handoff import build_research_handoff, write_research_handoff


def main() -> None:
    parser = argparse.ArgumentParser(description="Write controlled research/backtest handoff for supported market phenomena.")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--verdict", default="artifacts/market_phenomena/phenomenon_verdict.json")
    parser.add_argument("--output-dir", default="artifacts/market_phenomena")
    args = parser.parse_args()

    verdict = json.loads(Path(args.verdict).read_text(encoding="utf-8"))
    run_id = args.run_id or "research_handoff_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report = build_research_handoff(run_id=run_id, verdict_report=verdict)
    paths = write_research_handoff(report, args.output_dir)
    print(f"wrote {paths['json']}")
    print(f"wrote {paths['markdown']}")


if __name__ == "__main__":
    main()

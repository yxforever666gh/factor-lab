#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from factor_lab.market_phenomena_memory import load_phenomena_memory, upsert_phenomenon_verdict, write_phenomena_memory
from factor_lab.market_phenomena_verdict import build_phenomenon_verdict_report, write_phenomenon_verdict_report


def main() -> None:
    parser = argparse.ArgumentParser(description="Write market phenomenon verdict artifacts.")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--minimal-result", default="artifacts/market_phenomena/minimal_verification_result.json")
    parser.add_argument("--output-dir", default="artifacts/market_phenomena")
    parser.add_argument("--memory", default="knowledge/market_phenomena_memory.json")
    parser.add_argument("--lessons", default="knowledge/market_phenomena_lessons.md")
    args = parser.parse_args()

    minimal_result = json.loads(Path(args.minimal_result).read_text(encoding="utf-8"))
    run_id = args.run_id or "phenomenon_verdict_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report = build_phenomenon_verdict_report(run_id=run_id, minimal_result_report=minimal_result)
    paths = write_phenomenon_verdict_report(report, args.output_dir)

    memory = load_phenomena_memory(args.memory)
    for verdict in report.get("verdicts") or []:
        memory = upsert_phenomenon_verdict(memory, verdict)
    memory_paths = write_phenomena_memory(memory, args.memory, args.lessons)

    print(f"wrote {paths['json']}")
    print(f"wrote {paths['markdown']}")
    print(f"wrote {memory_paths['json']}")
    print(f"wrote {memory_paths['lessons']}")


if __name__ == "__main__":
    main()

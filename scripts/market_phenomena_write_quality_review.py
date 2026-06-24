#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from factor_lab.market_phenomena_quality import build_quality_review, write_quality_review


def main() -> None:
    parser = argparse.ArgumentParser(description="Write market phenomenon quality review artifacts.")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--candidates", default="artifacts/market_phenomena/phenomenon_candidates.json")
    parser.add_argument("--output-dir", default="artifacts/market_phenomena")
    args = parser.parse_args()

    candidates = json.loads(Path(args.candidates).read_text(encoding="utf-8"))
    run_id = args.run_id or "quality_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    review = build_quality_review(run_id=run_id, candidates_report=candidates)
    paths = write_quality_review(review, args.output_dir)
    print(f"wrote {paths['json']}")
    print(f"wrote {paths['markdown']}")


if __name__ == "__main__":
    main()

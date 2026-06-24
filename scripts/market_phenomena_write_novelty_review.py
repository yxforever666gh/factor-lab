#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from factor_lab.market_phenomena_memory import load_phenomena_memory
from factor_lab.market_phenomena_novelty import build_novelty_review, write_novelty_review


def main() -> None:
    parser = argparse.ArgumentParser(description="Write market phenomenon novelty review artifacts.")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--candidates", default="artifacts/market_phenomena/phenomenon_candidates.json")
    parser.add_argument("--quality-review", default="artifacts/market_phenomena/phenomenon_quality_review.json")
    parser.add_argument("--memory", default="knowledge/market_phenomena_memory.json")
    parser.add_argument("--output-dir", default="artifacts/market_phenomena")
    args = parser.parse_args()

    candidates = json.loads(Path(args.candidates).read_text(encoding="utf-8"))
    quality_review = json.loads(Path(args.quality_review).read_text(encoding="utf-8"))
    memory = load_phenomena_memory(args.memory)
    run_id = args.run_id or "novelty_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    review = build_novelty_review(run_id=run_id, quality_review=quality_review, candidates_report=candidates, memory=memory)
    paths = write_novelty_review(review, args.output_dir)
    print(f"wrote {paths['json']}")
    print(f"wrote {paths['markdown']}")


if __name__ == "__main__":
    main()

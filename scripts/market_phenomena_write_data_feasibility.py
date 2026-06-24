#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from factor_lab.market_phenomena_data import (
    build_data_feasibility_review,
    update_data_requests,
    write_data_feasibility_review,
)


def _load_json_or_default(path: str, default: dict) -> dict:
    p = Path(path)
    if not p.exists():
        return default
    return json.loads(p.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Write market phenomenon data feasibility artifacts.")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--candidates", default="artifacts/market_phenomena/phenomenon_candidates.json")
    parser.add_argument("--novelty-review", default="artifacts/market_phenomena/phenomenon_novelty_review.json")
    parser.add_argument("--output-dir", default="artifacts/market_phenomena")
    parser.add_argument("--data-requests", default="knowledge/market_phenomena_data_requests.json")
    args = parser.parse_args()

    candidates = json.loads(Path(args.candidates).read_text(encoding="utf-8"))
    novelty = json.loads(Path(args.novelty_review).read_text(encoding="utf-8"))
    run_id = args.run_id or "data_feasibility_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    review = build_data_feasibility_review(run_id=run_id, candidates_report=candidates, novelty_review=novelty)
    paths = write_data_feasibility_review(review, args.output_dir)

    requests_path = Path(args.data_requests)
    existing_requests = _load_json_or_default(args.data_requests, {"schema_version": 1, "requests": []})
    requests = update_data_requests(existing_requests, review)
    requests_path.parent.mkdir(parents=True, exist_ok=True)
    requests_path.write_text(json.dumps(requests, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    print(f"wrote {paths['json']}")
    print(f"wrote {paths['markdown']}")
    print(f"wrote {requests_path}")


if __name__ == "__main__":
    main()

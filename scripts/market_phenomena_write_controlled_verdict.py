#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.market_phenomena_controlled_verdict import (
    build_controlled_research_verdict,
    validate_controlled_research_verdict,
    write_controlled_research_verdict_artifacts,
)


def read_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Write controlled research verdict and next mutation request artifacts.")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--execution-result", default="artifacts/market_phenomena/controlled_research_execution_result.json")
    parser.add_argument("--output-dir", default="artifacts/market_phenomena")
    args = parser.parse_args()

    run_id = args.run_id or "controlled_verdict_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report = build_controlled_research_verdict(run_id=run_id, execution_result=read_json(args.execution_result))
    validation = validate_controlled_research_verdict(report)
    if validation["decision"] != "keep":
        raise SystemExit(f"invalid controlled research verdict: {validation}")
    paths = write_controlled_research_verdict_artifacts(report, args.output_dir)
    print(f"wrote {paths['verdict_json']}")
    print(f"wrote {paths['verdict_markdown']}")
    print(f"wrote {paths['mutation_request_json']}")
    print(f"decision {report['verdict']['decision']}")
    print(f"reason_codes {','.join(report['verdict']['reason_codes'])}")
    print(f"next_action {report['next_mutation_request']['action']}")
    print(f"queue_write_allowed {report['verdict']['queue_write_allowed']}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.market_phenomena_approval_gate import (
    build_strategy_design_approval_stub,
    validate_strategy_design_approval_stub,
    write_strategy_design_approval_stub,
)


def read_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def parse_horizons(text: str) -> list[int]:
    if not text:
        return []
    return [int(part.strip().removesuffix("d")) for part in text.split(",") if part.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Write strategy design approval stub and prototype gate artifacts.")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--strategy-design-spec", default="artifacts/market_phenomena/strategy_design_spec.json")
    parser.add_argument("--approval-status", default="pending_human_review", choices=["pending_human_review", "approved", "rejected"])
    parser.add_argument("--approved-horizons", default="")
    parser.add_argument("--reviewer", default=None)
    parser.add_argument("--output-dir", default="artifacts/market_phenomena")
    args = parser.parse_args()

    run_id = args.run_id or "strategy_design_approval_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report = build_strategy_design_approval_stub(
        run_id=run_id,
        strategy_design_spec=read_json(args.strategy_design_spec),
        approval_status=args.approval_status,
        approved_horizons=parse_horizons(args.approved_horizons),
        reviewer=args.reviewer,
    )
    validation = validate_strategy_design_approval_stub(report)
    if validation["decision"] != "keep":
        raise SystemExit(f"invalid strategy design approval gate: {validation}")
    paths = write_strategy_design_approval_stub(report, args.output_dir)
    print(f"wrote {paths['approval_report_json']}")
    print(f"wrote {paths['approval_report_markdown']}")
    print(f"wrote {paths['approval_stub_json']}")
    print(f"wrote {paths['prototype_gate_json']}")
    stub = report["strategy_design_approval_stub"]
    gate = report["strategy_design_prototype_gate"]
    print(f"approval_status {stub['approval_status']}")
    print(f"requested_horizons {','.join(str(h) for h in stub['requested_horizons'])}")
    print(f"approved_horizons {','.join(str(h) for h in stub['approved_horizons'])}")
    print(f"prototype_generation_allowed {gate['prototype_generation_allowed']}")
    print(f"queue_write_allowed {gate['queue_write_allowed']}")


if __name__ == "__main__":
    main()

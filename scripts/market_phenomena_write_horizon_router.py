#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.market_phenomena_horizon_router import (
    build_supported_horizon_router,
    validate_supported_horizon_router,
    write_supported_horizon_router,
)


def read_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Route supported horizons and write strategy design review gate.")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--deeper-oos-report", default="artifacts/market_phenomena/deeper_oos_horizon_report.json")
    parser.add_argument("--output-dir", default="artifacts/market_phenomena")
    args = parser.parse_args()

    run_id = args.run_id or "supported_horizon_router_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report = build_supported_horizon_router(run_id=run_id, deeper_oos_report=read_json(args.deeper_oos_report))
    validation = validate_supported_horizon_router(report)
    if validation["decision"] != "keep":
        raise SystemExit(f"invalid supported horizon router: {validation}")
    paths = write_supported_horizon_router(report, args.output_dir)
    print(f"wrote {paths['router_json']}")
    print(f"wrote {paths['router_markdown']}")
    print(f"wrote {paths['gate_json']}")
    print(f"supported_horizons {','.join(str(h) for h in report['supported_horizons'])}")
    print(f"rejected_horizons {len(report['rejected_horizons'])}")
    print(f"strategy_design_review_allowed {report['strategy_design_review_gate']['strategy_design_review_allowed']}")
    print(f"auto_promotion_allowed {report['strategy_design_review_gate']['auto_promotion_allowed']}")
    print(f"queue_write_allowed {report['strategy_design_review_gate']['queue_write_allowed']}")


if __name__ == "__main__":
    main()

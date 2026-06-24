#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.market_phenomena_review_pack import (
    build_human_review_pack,
    validate_human_review_pack,
    write_human_review_pack,
)


def read_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Write human review pack and strategy design spec artifacts.")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--iteration-plan", default="artifacts/market_phenomena/agent_iteration_plan_v2.json")
    parser.add_argument("--horizon-router", default="artifacts/market_phenomena/supported_horizon_router.json")
    parser.add_argument("--deeper-oos-report", default="artifacts/market_phenomena/deeper_oos_horizon_report.json")
    parser.add_argument("--output-dir", default="artifacts/market_phenomena")
    args = parser.parse_args()

    run_id = args.run_id or "human_review_pack_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    pack = build_human_review_pack(
        run_id=run_id,
        iteration_plan=read_json(args.iteration_plan),
        horizon_router=read_json(args.horizon_router),
        deeper_oos_report=read_json(args.deeper_oos_report),
    )
    validation = validate_human_review_pack(pack)
    if validation["decision"] != "keep":
        raise SystemExit(f"invalid human review pack: {validation}")
    paths = write_human_review_pack(pack, args.output_dir)
    print(f"wrote {paths['review_json']}")
    print(f"wrote {paths['review_markdown']}")
    print(f"wrote {paths['spec_json']}")
    print(f"wrote {paths['checklist_json']}")
    print(f"wrote {paths['checklist_markdown']}")
    print(f"supported_horizons {','.join(str(h) for h in pack['supported_horizons'])}")
    print(f"strategy_design_review_allowed {pack['strategy_design_spec']['strategy_design_review_allowed']}")
    print(f"strategy_generation_allowed {pack['strategy_design_spec']['strategy_generation_allowed']}")
    print(f"queue_write_allowed {pack['strategy_design_spec']['queue_write_allowed']}")


if __name__ == "__main__":
    main()

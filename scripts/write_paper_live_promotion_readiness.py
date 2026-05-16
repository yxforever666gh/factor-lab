#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.paper_live_promotion_readiness import write_paper_live_promotion_readiness


if __name__ == "__main__":
    payload = write_paper_live_promotion_readiness()
    print(
        json.dumps(
            {
                "readiness_status": payload.get("readiness_status"),
                "blockers": payload.get("blockers") or [],
                "warnings": payload.get("warnings") or [],
                "manual_approval_required": payload.get("manual_approval_required"),
                "live_trading_enabled": payload.get("live_trading_enabled"),
            },
            ensure_ascii=False,
        )
    )

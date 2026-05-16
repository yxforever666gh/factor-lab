#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.portfolio_constraint_hardening import write_portfolio_constraint_hardening


if __name__ == "__main__":
    payload = write_portfolio_constraint_hardening()
    print(
        json.dumps(
            {
                "constraint_status": payload.get("constraint_status"),
                "violations": payload.get("violations") or [],
                "warnings": payload.get("warnings") or [],
                "position_count": (payload.get("portfolio") or {}).get("position_count"),
                "max_position_weight": (payload.get("portfolio") or {}).get("max_position_weight"),
            },
            ensure_ascii=False,
        )
    )

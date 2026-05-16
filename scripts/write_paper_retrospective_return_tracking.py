#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.paper_retrospective_return_tracking import write_paper_retrospective_return_tracking


if __name__ == "__main__":
    payload = write_paper_retrospective_return_tracking()
    print(
        json.dumps(
            {
                "tracking_status": payload.get("tracking_status"),
                "portfolio_forward_return": (payload.get("portfolio_return") or {}).get("portfolio_forward_return"),
                "matched_position_count": (payload.get("portfolio_return") or {}).get("matched_position_count"),
                "missing_position_count": (payload.get("portfolio_return") or {}).get("missing_position_count"),
            },
            ensure_ascii=False,
        )
    )

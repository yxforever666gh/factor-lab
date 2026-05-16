#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.small_institutionalization_policy import write_small_institutionalization_status


if __name__ == "__main__":
    status = write_small_institutionalization_status()
    print(
        json.dumps(
            {
                "decision": status.get("decision"),
                "phase": status.get("phase"),
                "blockers": status.get("blockers") or [],
                "next_action": status.get("next_action"),
            },
            ensure_ascii=False,
        )
    )

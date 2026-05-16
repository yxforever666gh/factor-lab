#!/usr/bin/env python3
from __future__ import annotations

import json

from factor_lab.small_institutional_self_diagnosis import write_small_institutional_self_diagnosis


if __name__ == "__main__":
    payload = write_small_institutional_self_diagnosis()
    print(
        json.dumps(
            {
                "diagnosis_status": payload.get("diagnosis_status"),
                "primary_issue": payload.get("primary_issue"),
                "severity": payload.get("severity"),
                "next_action": payload.get("next_action"),
                "automation_allowed": payload.get("automation_allowed"),
                "recommended_run_mode": payload.get("recommended_run_mode"),
            },
            ensure_ascii=False,
            indent=2,
        )
    )

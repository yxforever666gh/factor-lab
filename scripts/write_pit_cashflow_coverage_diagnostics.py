from __future__ import annotations

import json
from pathlib import Path

from factor_lab.pit_cashflow_coverage_diagnostics import write_cashflow_coverage_diagnostics


def main() -> int:
    payload = write_cashflow_coverage_diagnostics()
    print(json.dumps({
        "output_dir": "artifacts/pit_value_trap_field_fix",
        "coverage": payload["coverage"],
        "diagnosis": payload["diagnosis"],
        "hard_stop_triggered": payload["hard_stop_triggered"],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

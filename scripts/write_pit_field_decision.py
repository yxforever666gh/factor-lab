from __future__ import annotations

import json

from factor_lab.pit_field_decision import write_field_decision


def main() -> int:
    payload = write_field_decision()
    print(json.dumps({
        "output_dir": "artifacts/pit_value_trap_field_fix",
        "decision": payload["decision"],
        "eligible_fields": payload["eligible_fields"],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

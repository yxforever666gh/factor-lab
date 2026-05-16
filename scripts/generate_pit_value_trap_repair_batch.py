from __future__ import annotations

import json

from factor_lab.pit_value_trap_repair_batch import write_repair_batch


def main() -> int:
    payload = write_repair_batch()
    print(json.dumps({
        "output_dir": "artifacts/pit_value_trap_field_fix",
        "decision": payload["decision"],
        "config_count": len(payload.get("configs", [])),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

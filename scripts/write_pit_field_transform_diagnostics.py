from __future__ import annotations

import json

from factor_lab.pit_field_transform_diagnostics import write_field_transform_diagnostics


def main() -> int:
    payload = write_field_transform_diagnostics()
    summary = {
        row["field"]: row.get("best_variant_by_ic")
        for row in payload.get("fields", [])
    }
    print(json.dumps({"output_dir": "artifacts/pit_value_trap_field_fix", "best_variants": summary}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

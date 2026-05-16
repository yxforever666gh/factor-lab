from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.feature_schema import TUSHARE_AVAILABLE_FEATURE_COLUMNS, TUSHARE_FEATURE_COLUMNS
from factor_lab.mechanism_templates import load_mechanism_templates


def build_mechanism_data_gap_report(
    *,
    templates: dict[str, dict[str, Any]] | None = None,
    available_fields: set[str] | frozenset[str] | None = None,
) -> dict[str, Any]:
    templates = templates if templates is not None else load_mechanism_templates()
    available = set(available_fields if available_fields is not None else TUSHARE_AVAILABLE_FEATURE_COLUMNS)
    rows: list[dict[str, Any]] = []
    for mechanism_id, template in sorted(templates.items()):
        required = sorted({str(field) for field in (template.get("required_data_fields") or []) if field})
        missing = sorted(set(required) - available)
        rows.append(
            {
                "mechanism_id": str(template.get("mechanism_id") or mechanism_id),
                "target_family": template.get("target_family"),
                "required_fields": required,
                "available_fields": sorted(set(required) & available),
                "missing_fields": missing,
                "coverage_status": "ready" if not missing else "blocked_missing_fields",
            }
        )
    ready_count = len([row for row in rows if row["coverage_status"] == "ready"])
    blocked_count = len(rows) - ready_count
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "template_count": len(rows),
            "total_templates": len(rows),
            "ready_template_count": ready_count,
            "ready_templates": ready_count,
            "blocked_template_count": blocked_count,
            "blocked_templates": blocked_count,
            "available_field_count": len(available),
            "missing_fields": sorted({field for row in rows for field in row["missing_fields"]}),
        },
        "templates": rows,
    }


def write_mechanism_data_gap_report(output_path: str | Path, *, available_fields: set[str] | None = None) -> dict[str, Any]:
    report = build_mechanism_data_gap_report(available_fields=available_fields)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report

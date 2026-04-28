from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def summarize_generated_batch_run(batch_output_dir: str | Path, output_path: str | Path) -> dict[str, Any]:
    root = Path(batch_output_dir)
    summary_path = root / "batch_summary.json"
    comparison_path = root / "batch_comparison.json"

    summary = json.loads(summary_path.read_text(encoding="utf-8")) if summary_path.exists() else []
    comparison = json.loads(comparison_path.read_text(encoding="utf-8")) if comparison_path.exists() else {}

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "batch_output_dir": str(root),
        "batch_summary": summary,
        "batch_comparison": comparison,
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List


def build_batch_comparison(root_dir: str | Path) -> dict:
    root = Path(root_dir)
    summary_path = root / "batch_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8")) if summary_path.exists() else []

    candidate_presence: Dict[str, List[str]] = {}
    graveyard_presence: Dict[str, List[str]] = {}
    representative_presence: Dict[str, List[str]] = {}

    for row in summary:
        job_name = row["job_name"]
        job_dir = root / job_name

        candidates = json.loads((job_dir / "candidate_pool.json").read_text(encoding="utf-8")) if (job_dir / "candidate_pool.json").exists() else []
        graveyard = json.loads((job_dir / "factor_graveyard.json").read_text(encoding="utf-8")) if (job_dir / "factor_graveyard.json").exists() else []
        representatives = json.loads((job_dir / "cluster_representatives.json").read_text(encoding="utf-8")) if (job_dir / "cluster_representatives.json").exists() else []

        for item in candidates:
            candidate_presence.setdefault(item["factor_name"], []).append(job_name)
        for item in graveyard:
            graveyard_presence.setdefault(item["factor_name"], []).append(job_name)
        for item in representatives:
            representative_presence.setdefault(item["factor_name"], []).append(job_name)

    payload = {
        "jobs": summary,
        "candidate_presence": candidate_presence,
        "graveyard_presence": graveyard_presence,
        "representative_presence": representative_presence,
    }
    (root / "batch_comparison.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload

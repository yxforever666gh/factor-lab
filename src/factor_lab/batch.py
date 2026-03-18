from __future__ import annotations

import json
from pathlib import Path
from typing import List

from factor_lab.batch_report import build_batch_comparison
from factor_lab.workflow import run_workflow


def run_batch(config_path: str, output_dir: str) -> None:
    with open(config_path, "r", encoding="utf-8") as handle:
        config = json.load(handle)

    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)

    summary: List[dict] = []
    for job in config["jobs"]:
        job_name = job["name"]
        job_output = root / job_name
        run_workflow(
            config_path=job["config_path"],
            output_dir=str(job_output),
        )
        results_path = job_output / "results.json"
        candidates_path = job_output / "candidate_pool.json"
        graveyard_path = job_output / "factor_graveyard.json"

        results = json.loads(results_path.read_text(encoding="utf-8")) if results_path.exists() else []
        candidates = json.loads(candidates_path.read_text(encoding="utf-8")) if candidates_path.exists() else []
        graveyard = json.loads(graveyard_path.read_text(encoding="utf-8")) if graveyard_path.exists() else []

        summary.append(
            {
                "job_name": job_name,
                "total_factors": len(results),
                "candidate_count": len(candidates),
                "graveyard_count": len(graveyard),
            }
        )

    (root / "batch_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    build_batch_comparison(root)

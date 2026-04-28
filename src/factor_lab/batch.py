from __future__ import annotations

import json
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List

from factor_lab.batch_report import build_batch_comparison
from factor_lab.workflow import run_workflow


DEFAULT_BATCH_MAX_WORKERS = 1


def batch_max_workers() -> int:
    raw = os.getenv("FACTOR_LAB_BATCH_MAX_WORKERS", str(DEFAULT_BATCH_MAX_WORKERS)).strip()
    try:
        value = int(raw)
    except Exception:
        value = DEFAULT_BATCH_MAX_WORKERS
    return max(1, min(4, value))


def _run_batch_job(root: Path, job: dict) -> dict:
    job_name = job["name"]
    job_output = root / job_name
    if job_output.exists():
        # Each batch job must start clean; otherwise failed reruns can silently read stale artifacts.
        shutil.rmtree(job_output)
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

    return {
        "job_name": job_name,
        "total_factors": len(results),
        "candidate_count": len(candidates),
        "graveyard_count": len(graveyard),
    }


def run_batch(config_path: str, output_dir: str) -> None:
    with open(config_path, "r", encoding="utf-8") as handle:
        config = json.load(handle)

    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)

    jobs = list(config.get("jobs") or [])
    summary: List[dict] = []
    workers = batch_max_workers()
    if len(jobs) > 1 and workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            summary = list(executor.map(lambda job: _run_batch_job(root, job), jobs))
    else:
        for job in jobs:
            summary.append(_run_batch_job(root, job))

    (root / "batch_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    build_batch_comparison(root)

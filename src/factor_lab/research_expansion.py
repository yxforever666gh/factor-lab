from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from factor_lab.dedup import config_fingerprint
from factor_lab.storage import ExperimentStore


ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "artifacts" / "factor_lab.db"


def _parse_date(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%d")


def _fmt_date(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def _make_window_config(base_config: dict[str, Any], start_date: str, end_date: str, output_dir: str) -> dict[str, Any]:
    config = deepcopy(base_config)
    config["start_date"] = start_date
    config["end_date"] = end_date
    config["output_dir"] = output_dir
    return config


def _write_generated_config(config: dict[str, Any], name: str) -> str:
    out_dir = ROOT / "artifacts" / "generated_configs"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{name}.json"
    path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path.relative_to(ROOT))


def expansion_candidates(store: ExperimentStore) -> list[dict[str, Any]]:
    recent_tasks = store.list_research_tasks(limit=100)
    existing_fingerprints = {t.get("fingerprint") for t in recent_tasks if t.get("status") in {"pending", "running", "finished"}}
    candidates: list[dict[str, Any]] = []

    base_recent = json.loads((ROOT / "configs" / "tushare_workflow.json").read_text(encoding="utf-8"))
    end_date = base_recent["end_date"]
    recent_start = _parse_date(base_recent["start_date"])
    end_dt = _parse_date(end_date)

    windows = [
        {
            "name": "rolling_30d_back",
            "start_date": _fmt_date(recent_start - timedelta(days=30)),
            "end_date": end_date,
            "output_dir": "artifacts/generated_rolling_30d_back",
            "priority": 18,
            "worker_note": "baseline｜历史扩窗 30 天",
        },
        {
            "name": "rolling_recent_45d",
            "start_date": _fmt_date(end_dt - timedelta(days=45)),
            "end_date": end_date,
            "output_dir": "artifacts/generated_recent_45d",
            "priority": 22,
            "worker_note": "validation｜近期 45 天窗口验证",
        },
        {
            "name": "rolling_recent_90d",
            "start_date": _fmt_date(end_dt - timedelta(days=90)),
            "end_date": end_date,
            "output_dir": "artifacts/generated_recent_90d",
            "priority": 24,
            "worker_note": "validation｜近期 90 天窗口验证",
        },
    ]

    for window in windows:
        config = _make_window_config(base_recent, window["start_date"], window["end_date"], window["output_dir"])
        fingerprint = f"workflow::{config_fingerprint(config)}::{window['output_dir']}"
        if fingerprint in existing_fingerprints:
            continue
        config_path = _write_generated_config(config, window["name"])
        candidates.append(
            {
                "task_type": "workflow",
                "priority": window["priority"],
                "payload": {"config_path": config_path, "output_dir": window["output_dir"]},
                "fingerprint": fingerprint,
                "worker_note": window["worker_note"],
            }
        )

    return candidates


def maybe_expand_research_space(store: ExperimentStore, max_new_tasks: int = 3) -> list[str]:
    recent_tasks = store.list_research_tasks(limit=50)
    pending_or_running = [t for t in recent_tasks if t["status"] in {"pending", "running"}]
    if pending_or_running:
        return []

    task_specs = expansion_candidates(store)[:max_new_tasks]
    new_task_ids = []
    for spec in task_specs:
        task_id = store.enqueue_research_task(
            task_type=spec["task_type"],
            payload=spec["payload"],
            priority=spec["priority"],
            fingerprint=spec["fingerprint"],
            worker_note=spec["worker_note"],
        )
        new_task_ids.append(task_id)
    return new_task_ids
{config_fingerprint(config)}::{window['output_dir']}"
        if fingerprint in existing_fingerprints:
            continue
        config_path = _write_generated_config(config, window["name"])
        candidates.append(
            {
                "task_type": "workflow",
                "priority": window["priority"],
                "payload": {"config_path": config_path, "output_dir": window["output_dir"]},
                "fingerprint": fingerprint,
                "worker_note": window["worker_note"],
            }
        )

    return candidates


def maybe_expand_research_space(store: ExperimentStore, max_new_tasks: int = 2) -> list[str]:
    recent_tasks = store.list_research_tasks(limit=50)
    pending_or_running = [t for t in recent_tasks if t["status"] in {"pending", "running"}]
    if pending_or_running:
        return []

    task_specs = expansion_candidates(store)[:max_new_tasks]
    new_task_ids = []
    for spec in task_specs:
        task_id = store.enqueue_research_task(
            task_type=spec["task_type"],
            payload=spec["payload"],
            priority=spec["priority"],
            fingerprint=spec["fingerprint"],
            worker_note=spec["worker_note"],
        )
        new_task_ids.append(task_id)
    return new_task_ids

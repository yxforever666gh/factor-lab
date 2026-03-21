from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timedelta
import time
from pathlib import Path
from typing import Any

from factor_lab.dedup import config_fingerprint
from factor_lab.storage import ExperimentStore
from factor_lab.feature_schema import TUSHARE_FEATURE_COLUMNS
from factor_lab.expression_validation import validate_expression


ROOT = Path(__file__).resolve().parents[2]


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


def _candidate_validation_specs(store: ExperimentStore, base_recent: dict[str, Any], end_date: str) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    end_dt = _parse_date(end_date)
    promising = store.top_promising_candidates(limit=4)
    for idx, candidate in enumerate(promising):
        definition = candidate.get("definition") or {}
        if not definition.get("name"):
            continue
        expression = definition.get("expression") or ""
        if expression:
            validation = validate_expression(expression, available_fields=TUSHARE_FEATURE_COLUMNS)
            if not validation.ok:
                # Skip deterministic invalid expressions (prevents circuit breaker stalls).
                continue
        for days, priority in [(45, 12), (90, 14)]:
            name = f"candidate_{definition['name']}_recent_{days}d"
            output_dir = f"artifacts/generated_candidate_{definition['name']}_recent_{days}d"
            cfg = deepcopy(base_recent)
            cfg["factors"] = [definition]
            cfg["start_date"] = _fmt_date(end_dt - timedelta(days=days))
            cfg["end_date"] = end_date
            cfg["output_dir"] = output_dir
            config_path = _write_generated_config(cfg, name)
            fingerprint = f"workflow::{config_fingerprint(cfg)}::{output_dir}"
            specs.append(
                {
                    "task_type": "workflow",
                    "priority": priority + idx,
                    "payload": {"config_path": config_path, "output_dir": output_dir},
                    "fingerprint": fingerprint,
                    "worker_note": f"validation｜candidate_validation {definition['name']} recent_{days}d",
                }
            )
    return specs


def expansion_candidates(store: ExperimentStore, *, allow_repeat: bool = False) -> list[dict[str, Any]]:
    recent_tasks = store.list_research_tasks(limit=300)
    existing_fingerprints = {
        t.get("fingerprint") for t in recent_tasks if t.get("status") in {"pending", "running", "finished"}
    }

    # When breaking stagnation, we may intentionally repeat expansions.
    # In that mode we still avoid duplicating *pending/running* work, but we allow new unique runs.
    pending_or_running_fingerprints = {
        t.get("fingerprint") for t in recent_tasks if t.get("status") in {"pending", "running"}
    }
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
            "name": "rolling_60d_back",
            "start_date": _fmt_date(recent_start - timedelta(days=60)),
            "end_date": end_date,
            "output_dir": "artifacts/generated_rolling_60d_back",
            "priority": 19,
            "worker_note": "baseline｜历史扩窗 60 天",
        },
        {
            "name": "rolling_120d_back",
            "start_date": _fmt_date(recent_start - timedelta(days=120)),
            "end_date": end_date,
            "output_dir": "artifacts/generated_rolling_120d_back",
            "priority": 20,
            "worker_note": "baseline｜历史扩窗 120 天",
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
        {
            "name": "rolling_recent_120d",
            "start_date": _fmt_date(end_dt - timedelta(days=120)),
            "end_date": end_date,
            "output_dir": "artifacts/generated_recent_120d",
            "priority": 25,
            "worker_note": "validation｜近期 120 天窗口验证",
        },
        {
            "name": "expanding_from_2025_10_01",
            "start_date": "2025-10-01",
            "end_date": end_date,
            "output_dir": "artifacts/generated_expanding_2025_10_01",
            "priority": 16,
            "worker_note": "baseline｜expanding 窗口 2025-10-01 起",
        },
    ]

    for spec in _candidate_validation_specs(store, base_recent, end_date) + windows:
        if "start_date" in spec:
            config = _make_window_config(base_recent, spec["start_date"], spec["end_date"], spec["output_dir"])
            fingerprint = f"workflow::{config_fingerprint(config)}::{spec['output_dir']}"
            if (not allow_repeat and fingerprint in existing_fingerprints) or (fingerprint in pending_or_running_fingerprints):
                continue
            config_path = _write_generated_config(config, spec["name"])
            candidates.append(
                {
                    "task_type": "workflow",
                    "priority": spec["priority"],
                    "payload": {"config_path": config_path, "output_dir": spec["output_dir"]},
                    "fingerprint": fingerprint,
                    "worker_note": spec["worker_note"],
                }
            )
            continue
        if (not allow_repeat and spec["fingerprint"] in existing_fingerprints) or (spec["fingerprint"] in pending_or_running_fingerprints):
            continue
        candidates.append(spec)

    return candidates


def maybe_expand_research_space(store: ExperimentStore, max_new_tasks: int = 3, *, allow_repeat: bool = False) -> list[str]:
    recent_tasks = store.list_research_tasks(limit=50)
    pending_or_running = [t for t in recent_tasks if t["status"] in {"pending", "running"}]
    if pending_or_running:
        return []

    task_specs = expansion_candidates(store, allow_repeat=allow_repeat)[:max_new_tasks]

    # If we're breaking stagnation and have nothing new, force a unique repeat baseline run.
    if not task_specs and allow_repeat:
        base_recent = json.loads((ROOT / "configs" / "tushare_workflow.json").read_text(encoding="utf-8"))
        nonce = int(time.time())
        output_dir = f"artifacts/forced_expand_{nonce}"
        cfg = deepcopy(base_recent)
        cfg["output_dir"] = output_dir
        config_path = _write_generated_config(cfg, f"forced_expand_{nonce}")
        fingerprint = f"workflow::{config_fingerprint(cfg)}::{output_dir}"
        task_specs = [{
            "task_type": "workflow",
            "priority": 26,
            "payload": {"config_path": config_path, "output_dir": output_dir},
            "fingerprint": fingerprint,
            "worker_note": "exploration｜forced expansion to break stagnation",
        }]
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

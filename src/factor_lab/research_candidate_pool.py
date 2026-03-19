from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

from factor_lab.dedup import config_fingerprint


ROOT = Path(__file__).resolve().parents[2]


def _read_json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _write_generated_config(config: dict[str, Any], name: str) -> str:
    out_dir = ROOT / "artifacts" / "generated_configs"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{name}.json"
    path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path.relative_to(ROOT))


def _make_task(task_type: str, category: str, priority_hint: int, reason: str, expected_knowledge_gain: list[str], payload: dict[str, Any], worker_note: str) -> dict[str, Any]:
    if task_type == "workflow":
        config = _read_json(ROOT / payload["config_path"])
        fingerprint = f"workflow::{config_fingerprint(config)}::{payload['output_dir']}"
    elif task_type == "generated_batch":
        batch = _read_json(ROOT / payload["batch_path"])
        fingerprint = f"generated_batch::{config_fingerprint(batch)}::{payload['output_dir']}"
    elif task_type == "diagnostic":
        fingerprint = f"diagnostic::{payload['diagnostic_type']}::{json.dumps(payload, ensure_ascii=False, sort_keys=True)}"
    else:
        fingerprint = f"{task_type}::{json.dumps(payload, ensure_ascii=False, sort_keys=True)}"
    return {
        "task_type": task_type,
        "category": category,
        "priority_hint": priority_hint,
        "reason": reason,
        "expected_knowledge_gain": expected_knowledge_gain,
        "payload": payload,
        "fingerprint": fingerprint,
        "worker_note": worker_note,
    }


def build_research_candidate_pool(snapshot_path: str | Path, output_path: str | Path) -> dict[str, Any]:
    snapshot = _read_json(snapshot_path)
    existing_fingerprints = {task.get("fingerprint") for task in snapshot.get("recent_research_tasks", [])}
    latest_run = snapshot.get("latest_run") or {}
    generated_configs = set(snapshot.get("generated_configs", []))
    stable_candidates = [row["factor_name"] for row in snapshot.get("stable_candidates", [])[:5]]
    latest_graveyard = snapshot.get("latest_graveyard", [])[:5]
    queue_budget = snapshot.get("queue_budget", {})
    exploration_state = snapshot.get("exploration_state", {})
    failure_state = snapshot.get("failure_state", {})

    base_config = _read_json(ROOT / "configs" / "tushare_workflow.json")
    end_date = latest_run.get("end_date") or base_config["end_date"]

    candidates: list[dict[str, Any]] = []

    window_specs = [
        ("rolling_180d_back", "2025-07-01", "artifacts/generated_rolling_180d_back", 18, "baseline｜历史扩窗 180 天"),
        ("rolling_240d_back", "2025-05-01", "artifacts/generated_rolling_240d_back", 19, "baseline｜历史扩窗 240 天"),
        ("rolling_recent_150d", "2025-10-20", "artifacts/generated_recent_150d", 23, "validation｜近期 150 天窗口验证"),
        ("expanding_from_2025_07_01", "2025-07-01", "artifacts/generated_expanding_2025_07_01", 16, "baseline｜expanding 窗口 2025-07-01 起"),
    ]

    for name, start_date, output_dir, priority, worker_note in window_specs:
        if f"{name}.json" in generated_configs:
            continue
        config = deepcopy(base_config)
        config["start_date"] = start_date
        config["end_date"] = end_date
        config["output_dir"] = output_dir
        config_path = _write_generated_config(config, name)
        task = _make_task(
            task_type="workflow",
            category="baseline" if "baseline" in worker_note else "validation",
            priority_hint=priority,
            reason=f"当前已覆盖到 {latest_run.get('config_path', 'base workflow')}，建议继续拓宽历史窗口 {start_date} → {end_date}。",
            expected_knowledge_gain=["window_stability_check"],
            payload={"config_path": config_path, "output_dir": output_dir},
            worker_note=worker_note,
        )
        if task["fingerprint"] not in existing_fingerprints:
            candidates.append(task)

    if stable_candidates:
        payload = {
            "diagnostic_type": "stable_candidate_validation_review",
            "focus_factors": stable_candidates,
            "reasons": ["stable_candidates_need_deeper_validation"],
            "knowledge_gain": ["stable_candidate_validation_requested"],
            "source_output_dir": "artifacts/tushare_batch",
        }
        task = _make_task(
            task_type="diagnostic",
            category="validation",
            priority_hint=28,
            reason="稳定候选已形成，下一步应做更严格验证而不是只继续扩时间窗口。",
            expected_knowledge_gain=["stable_candidate_validation_requested"],
            payload=payload,
            worker_note="validation｜稳定候选深化验证",
        )
        if task["fingerprint"] not in existing_fingerprints:
            candidates.append(task)

    if latest_graveyard:
        payload = {
            "diagnostic_type": "graveyard_window_sensitivity_review",
            "focus_factors": latest_graveyard,
            "reasons": ["recent_graveyard_needs_window_sensitivity_review"],
            "knowledge_gain": ["graveyard_window_sensitivity_requested"],
            "source_output_dir": "artifacts/tushare_batch",
        }
        task = _make_task(
            task_type="diagnostic",
            category="validation",
            priority_hint=30,
            reason="当前 graveyard 因子需要进一步区分是窗口敏感、还是结构性失效。",
            expected_knowledge_gain=["graveyard_window_sensitivity_requested"],
            payload=payload,
            worker_note="validation｜graveyard 窗口敏感性诊断",
        )
        if task["fingerprint"] not in existing_fingerprints:
            candidates.append(task)

    if not exploration_state.get("should_throttle") and queue_budget.get("exploration", 0) < 1:
        generated_batch_path = ROOT / "artifacts" / "generated_batch_from_llm.json"
        if generated_batch_path.exists():
            task = _make_task(
                task_type="generated_batch",
                category="exploration",
                priority_hint=55,
                reason="当前 exploration 未被 throttle，可允许一个受控生成 batch 进入候选池。",
                expected_knowledge_gain=["exploration_candidate_survived", "exploration_graveyard_identified"],
                payload={"batch_path": str(generated_batch_path.relative_to(ROOT)), "output_dir": "artifacts/llm_generated_batch_run"},
                worker_note="exploration｜执行 LLM 生成的 batch",
            )
            if task["fingerprint"] not in existing_fingerprints:
                candidates.append(task)

    payload = {
        "generated_from_snapshot": str(Path(snapshot_path)),
        "summary": {
            "latest_run_config": latest_run.get("config_path"),
            "queue_budget": queue_budget,
            "failure_state": failure_state,
            "exploration_state": exploration_state,
            "stable_candidate_count": len(stable_candidates),
            "graveyard_count": len(latest_graveyard),
            "candidate_count": len(candidates),
        },
        "tasks": sorted(candidates, key=lambda item: (item["priority_hint"], item["category"])),
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload

from __future__ import annotations

import json
from typing import Any


FAMILY_KEYS = {
    "stable_candidate_validation",
    "graveyard_diagnosis",
    "recent_window_validation",
    "window_expansion",
    "exploration",
}


def infer_family_from_task(task: dict[str, Any]) -> str:
    worker_note = (task.get("worker_note") or "")
    if "稳定候选" in worker_note:
        return "stable_candidate_validation"
    if "graveyard" in worker_note:
        return "graveyard_diagnosis"
    if "近期" in worker_note:
        return "recent_window_validation"
    if "扩窗" in worker_note or "expanding" in worker_note:
        return "window_expansion"
    if "exploration" in worker_note:
        return "exploration"
    category = task.get("category") or ((task.get("payload") or {}).get("category"))
    return category or "other"


def extract_knowledge_gains(task: dict[str, Any]) -> list[str]:
    payload = task.get("payload") or {}
    gains = [g for g in (payload.get("knowledge_gain") or payload.get("expected_knowledge_gain") or []) if g]
    note = task.get("worker_note") or ""
    if "knowledge_gain=" in note:
        tail = note.split("knowledge_gain=", 1)[-1]
        gains.extend([x.strip() for x in tail.split(",") if x.strip()])
    seen = set()
    out = []
    for gain in gains:
        if gain not in seen:
            out.append(gain)
            seen.add(gain)
    return out


def build_trial_log_entry(task: dict[str, Any]) -> dict[str, Any]:
    payload = task.get("payload") or {}
    status = task.get("status") or "pending"
    gains = extract_knowledge_gains(task)
    focus = payload.get("focus_factors") or []
    family = infer_family_from_task(task)
    if status == "failed":
        outcome = "failed"
    elif status == "finished" and gains:
        outcome = "informative"
    elif status == "finished":
        outcome = "no_gain"
    elif status == "running":
        outcome = "running"
    else:
        outcome = "pending"
    pressure_weight = 1.0
    if outcome == "no_gain":
        pressure_weight = 1.35
    elif outcome == "failed":
        pressure_weight = 1.6
    elif outcome == "informative":
        pressure_weight = 0.75
    return {
        "source_task_id": task.get("task_id"),
        "fingerprint": task.get("fingerprint"),
        "family": family,
        "category": task.get("category") or payload.get("category") or "other",
        "candidate_name": focus[0] if focus else None,
        "status": status,
        "outcome_label": outcome,
        "knowledge_gain_count": len(gains),
        "pressure_weight": pressure_weight,
        "created_at_utc": task.get("finished_at_utc") or task.get("started_at_utc") or task.get("created_at_utc"),
        "details": {
            "worker_note": task.get("worker_note"),
            "knowledge_gain": gains,
            "focus_factors": focus,
            "payload": payload,
        },
    }


def build_family_trial_summary(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        family = row.get("family") or "other"
        bucket = out.setdefault(family, {
            "family": family,
            "trial_count": 0,
            "informative_trial_count": 0,
            "no_gain_trial_count": 0,
            "failed_trial_count": 0,
            "running_trial_count": 0,
            "pending_trial_count": 0,
            "knowledge_gain_total": 0,
            "pressure_weight_total": 0.0,
            "latest_trial_at": None,
        })
        bucket["trial_count"] += 1
        outcome = row.get("outcome_label") or "pending"
        if outcome == "informative":
            bucket["informative_trial_count"] += 1
        elif outcome == "no_gain":
            bucket["no_gain_trial_count"] += 1
        elif outcome == "failed":
            bucket["failed_trial_count"] += 1
        elif outcome == "running":
            bucket["running_trial_count"] += 1
        else:
            bucket["pending_trial_count"] += 1
        bucket["knowledge_gain_total"] += int(row.get("knowledge_gain_count") or 0)
        bucket["pressure_weight_total"] += float(row.get("pressure_weight") or 0.0)
        ts = row.get("created_at_utc")
        if ts and (bucket["latest_trial_at"] is None or ts > bucket["latest_trial_at"]):
            bucket["latest_trial_at"] = ts
    for bucket in out.values():
        trial_count = max(int(bucket["trial_count"]), 1)
        informative = int(bucket["informative_trial_count"])
        no_gain = int(bucket["no_gain_trial_count"])
        failed = int(bucket["failed_trial_count"])
        bucket["knowledge_gain_per_trial"] = round(float(bucket["knowledge_gain_total"]) / trial_count, 6)
        bucket["trial_pressure"] = round(min(100.0, (float(bucket["pressure_weight_total"]) / trial_count) * 35.0 + no_gain * 7.5 + failed * 9.0), 6)
        bucket["false_positive_pressure"] = round(min(100.0, ((no_gain + failed) / trial_count) * 70.0 + max(0, trial_count - informative) * 2.5), 6)
    return out


def family_trial_recommended_action(trial_summary: dict[str, Any]) -> str:
    pressure = float(trial_summary.get("trial_pressure") or 0.0)
    false_positive_pressure = float(trial_summary.get("false_positive_pressure") or 0.0)
    informative = int(trial_summary.get("informative_trial_count") or 0)
    if false_positive_pressure >= 75 or pressure >= 82:
        return "pause"
    if false_positive_pressure >= 45 or pressure >= 55:
        return "refine"
    if informative == 0 and int(trial_summary.get("trial_count") or 0) <= 1:
        return "explore_new_branch"
    return "continue"

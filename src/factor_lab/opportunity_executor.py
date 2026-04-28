from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from factor_lab.opportunity_to_tasks import map_opportunity_to_task
from factor_lab.opportunity_store import sync_opportunities, update_opportunity_state
from factor_lab.opportunity_policy import should_bypass_recent_fingerprint
from factor_lab.opportunity_diagnostics import build_opportunity_review
from factor_lab.storage import ExperimentStore
from factor_lab.research_runtime_state import recently_finished_same_fingerprint


def _queue_counts(store: ExperimentStore, limit: int = 200) -> dict[str, int]:
    tasks = store.list_research_tasks(limit=limit)
    counts = {"validation": 0, "exploration": 0}

    def channel_from_task(task_type: str | None) -> str | None:
        if task_type == "diagnostic":
            return "validation"
        if task_type == "generated_batch":
            return "exploration"
        return None

    for task in tasks:
        if task.get("status") not in {"pending", "running"}:
            continue
        channel = channel_from_task(task.get("task_type"))
        if channel in counts:
            counts[channel] += 1
    return counts


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = int(raw)
    except Exception:
        value = default
    return max(minimum, value)


def _queue_capacity() -> dict[str, int]:
    # These caps prevent the opportunity system from flooding the queue.
    # Make exploration larger than validation by default to encourage autonomy.
    return {
        "validation": _env_int("RESEARCH_QUEUE_MAX_PENDING_VALIDATION", 3, minimum=1),
        "exploration": _env_int("RESEARCH_QUEUE_MAX_PENDING_EXPLORATION", 2, minimum=1),
    }


def _queue_backlog_targets() -> dict[str, int]:
    caps = _queue_capacity()
    return {
        "validation": min(caps["validation"], _env_int("RESEARCH_QUEUE_TARGET_VALIDATION_BACKLOG", 2, minimum=1)),
        "exploration": min(caps["exploration"], _env_int("RESEARCH_QUEUE_TARGET_EXPLORATION_BACKLOG", 2, minimum=1)),
    }

ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "artifacts" / "factor_lab.db"
OPPORTUNITY_LEARNING_PATH = ROOT / "artifacts" / "opportunity_learning.json"
OPPORTUNITY_RUNTIME_HEALTH_PATH = ROOT / "artifacts" / "opportunity_runtime_health.json"


def _opportunity_learning() -> dict[str, Any]:
    if not OPPORTUNITY_LEARNING_PATH.exists():
        return {}
    try:
        payload = json.loads(OPPORTUNITY_LEARNING_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _learning_channel_bias(limit: int) -> dict[str, int]:
    learning = _opportunity_learning()
    types = learning.get("types") or {}
    exploration_rows = [types.get(key) or {} for key in ["expand", "recombine", "probe"]]
    no_gain = sum(int(row.get("recent_no_gain_count") or 0) for row in exploration_rows)
    gain = sum(int(row.get("recent_gain_count") or 0) for row in exploration_rows)
    resource = sum(int(row.get("recent_resource_exhaustion_count") or 0) for row in exploration_rows)
    cooldowns = sum(1 for row in exploration_rows if row.get("cooldown_active"))
    if limit < 2:
        return {"validation_bonus": 0, "exploration_penalty": 0}
    should_shift = resource >= 2 or cooldowns >= 2 or (no_gain >= 4 and no_gain >= gain + 2)
    if not should_shift:
        return {"validation_bonus": 0, "exploration_penalty": 0}
    return {"validation_bonus": 1, "exploration_penalty": 1}


def _opportunity_runtime_health() -> dict[str, Any]:
    if not OPPORTUNITY_RUNTIME_HEALTH_PATH.exists():
        return {}
    try:
        payload = json.loads(OPPORTUNITY_RUNTIME_HEALTH_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _opportunity_channel(task: dict[str, Any] | None) -> str | None:
    if not task:
        return None
    task_type = task.get("task_type")
    if task_type == "diagnostic":
        return "validation"
    if task_type == "generated_batch":
        return "exploration"
    return None


def enqueue_opportunities(
    opportunities_path: str | Path,
    output_path: str | Path,
    db_path: str | Path = DB_PATH,
    limit: int = 2,
    *,
    queue_aware: bool = True,
) -> dict[str, Any]:
    opportunities_doc = json.loads(Path(opportunities_path).read_text(encoding="utf-8")) if Path(opportunities_path).exists() else {}
    opportunities = list(opportunities_doc.get("opportunities") or [])
    sync_opportunities(opportunities)
    review = build_opportunity_review()
    blocks = review.get("blocks") or {}
    downweights = review.get("downweights") or {}
    runtime_health = (_opportunity_runtime_health().get("opportunities") or {})
    critique_path = ROOT / "artifacts" / "meta_research_critique.json"
    portfolio_path = ROOT / "artifacts" / "research_portfolio_plan.json"
    critique = json.loads(critique_path.read_text(encoding="utf-8")) if critique_path.exists() else {}
    portfolio = json.loads(portfolio_path.read_text(encoding="utf-8")) if portfolio_path.exists() else {}
    corrective_actions = critique.get("corrective_actions") or []
    family_allocations = portfolio.get("family_allocations") or []
    diversified_family = next((row.get("suggested_family") for row in corrective_actions if row.get("action") == "diversify_family_allocation"), None)
    force_positive_frontier_probe = any(row.get("action") == "force_positive_frontier_probe" for row in corrective_actions)
    store = ExperimentStore(db_path)

    prepared: list[dict[str, Any]] = []
    skipped = []
    considered = 0

    for opportunity in opportunities:
        oid = opportunity.get("opportunity_id")
        if oid in blocks:
            skipped.append({"opportunity_id": oid, "reason": f"blocked:{blocks[oid].get('reason')}"})
            update_opportunity_state(oid, "rejected", reason=f"blocked:{blocks[oid].get('reason')}")
            continue

        runtime_meta = runtime_health.get(oid) or {}
        if runtime_meta.get("cooldown_active"):
            reason = runtime_meta.get("cooldown_reason") or "runtime_cooldown"
            skipped.append({"opportunity_id": oid, "reason": f"runtime_cooldown:{reason}"})
            update_opportunity_state(oid, "archived", reason=f"runtime_cooldown:{reason}")
            continue

        if oid in downweights:
            opportunity = {
                **opportunity,
                "priority": max(0.05, float(opportunity.get("priority") or 0.5) - float(downweights[oid].get("delta") or 0.0)),
            }

        task = map_opportunity_to_task(opportunity)
        if not task:
            skipped.append({"opportunity_id": oid, "reason": "unmappable"})
            update_opportunity_state(oid, "rejected", reason="unmappable")
            continue

        bypass = should_bypass_recent_fingerprint(opportunity)
        fingerprint = task.get("fingerprint")
        if fingerprint and recently_finished_same_fingerprint(
            store,
            fingerprint,
            task_type=task.get("task_type"),
            payload=task.get("payload") or {},
            worker_note=task.get("worker_note"),
        ) and not bypass.get("allow_bypass"):
            skipped.append({"opportunity_id": oid, "reason": "recently_finished_same_fingerprint"})
            update_opportunity_state(oid, "archived", reason="recently_finished_same_fingerprint")
            continue

        if bypass.get("allow_bypass"):
            task["payload"]["dedupe_bypass"] = True
            task["payload"]["dedupe_bypass_reason"] = bypass.get("reason")

        prepared.append({
            "opportunity": opportunity,
            "task": task,
            "channel": _opportunity_channel(task) or "other",
            "bypass": bypass,
        })
        considered += 1

    # Scale scheduling with `limit` (previously hard-capped at 1 validation + 1 exploration).
    # Default policy: always keep some validation, spend the rest on exploration.
    learning_bias = _learning_channel_bias(int(limit))
    if limit <= 0:
        channel_limits = {"validation": 0, "exploration": 0}
    elif limit == 1:
        channel_limits = {"validation": 1, "exploration": 0}
    else:
        validation_quota = min(int(limit), 1 + int(learning_bias.get("validation_bonus") or 0))
        exploration_quota = max(0, int(limit) - validation_quota - int(learning_bias.get("exploration_penalty") or 0))
        if validation_quota + exploration_quota <= 0:
            exploration_quota = max(0, int(limit) - validation_quota)
        channel_limits = {"validation": validation_quota, "exploration": exploration_quota}

    if force_positive_frontier_probe:
        channel_limits["exploration"] = max(1, channel_limits.get("exploration", 0))
        channel_limits["validation"] = max(0, limit - channel_limits["exploration"])

    if queue_aware:
        pending = _queue_counts(store)
        caps = _queue_capacity()
        targets = _queue_backlog_targets()
        channel_limits = {
            ch: max(
                0,
                min(
                    max(int(channel_limits.get(ch, 0)), max(0, int(targets.get(ch, 0)) - int(pending.get(ch, 0)))),
                    max(0, int(caps.get(ch, 0)) - int(pending.get(ch, 0))),
                ),
            )
            for ch in ("validation", "exploration")
        }

    selected: list[dict[str, Any]] = []
    selected_ids: set[str] = set()
    selected_families: set[str] = set()

    for channel in ("validation", "exploration"):
        quota = channel_limits.get(channel, 0)
        if quota <= 0:
            continue
        channel_rows = [row for row in prepared if row["channel"] == channel]
        diversified_rows = []
        fallback_rows = []
        for row in channel_rows:
            family = row["opportunity"].get("target_family") or "none"
            if diversified_family and family == diversified_family and family in selected_families:
                fallback_rows.append(row)
            else:
                diversified_rows.append(row)
        for row in (diversified_rows + fallback_rows)[:quota]:
            selected.append(row)
            selected_ids.add(row["opportunity"].get("opportunity_id"))
            selected_families.add(row["opportunity"].get("target_family") or "none")

    effective_total_limit = sum(int(channel_limits.get(ch, 0)) for ch in ("validation", "exploration")) if queue_aware else int(limit)
    remaining_slots = max(0, effective_total_limit - len(selected))
    if remaining_slots > 0:
        selected_channel_counts = {
            ch: sum(1 for row in selected if row["channel"] == ch)
            for ch in ("validation", "exploration")
        }
        prioritized_remainder = sorted(
            [row for row in prepared if row["opportunity"].get("opportunity_id") not in selected_ids],
            key=lambda row: (
                0 if row["channel"] == "exploration" else 1,
                -float((row["opportunity"] or {}).get("priority") or 0.0),
                -float((row["opportunity"] or {}).get("novelty_score") or 0.0),
            ),
        )
        for row in prioritized_remainder:
            channel = row["channel"]
            if queue_aware and selected_channel_counts.get(channel, 0) >= int(channel_limits.get(channel, 0)):
                continue
            oid = row["opportunity"].get("opportunity_id")
            selected.append(row)
            selected_ids.add(oid)
            selected_channel_counts[channel] = selected_channel_counts.get(channel, 0) + 1
            remaining_slots -= 1
            if remaining_slots <= 0:
                break

    injected = []
    for row in selected:
        opportunity = row["opportunity"]
        task = row["task"]
        bypass = row["bypass"]
        oid = opportunity.get("opportunity_id")
        fingerprint = task.get("fingerprint")
        task_id = store.enqueue_research_task(
            task_type=task["task_type"],
            payload=task["payload"],
            priority=int(task.get("priority") or 50),
            fingerprint=fingerprint,
            worker_note=task.get("worker_note"),
        )
        injected.append({
            "opportunity_id": oid,
            "task_id": task_id,
            "task_type": task.get("task_type"),
            "priority": task.get("priority"),
            "channel": row["channel"],
            "dedupe_bypass": bool(bypass.get("allow_bypass")),
        })
        update_opportunity_state(oid, "scheduled", reason="task_enqueued", extra={"task_id": task_id, "task_type": task.get("task_type")})

    unscheduled = [
        {
            "opportunity_id": row["opportunity"].get("opportunity_id"),
            "reason": f"channel_deferred:{row['channel']}",
        }
        for row in prepared
        if row["opportunity"].get("opportunity_id") not in selected_ids
    ]
    skipped.extend(unscheduled)

    payload = {
        "source": str(opportunities_path),
        "considered": considered,
        "channel_limits": channel_limits,
        "injected_count": len(injected),
        "injected": injected,
        "skipped": skipped,
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload

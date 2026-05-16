from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.feature_schema import TUSHARE_FEATURE_COLUMNS
from factor_lab.research_budget_allocator import ResearchBudgetAllocator
from factor_lab.research_experiment_gate import evaluate_research_experiment_gate
from factor_lab.research_experiment_proposal import proposal_from_research_task, proposal_from_workflow_task
from factor_lab.storage import ExperimentStore


DEFAULT_GATE_DECISION_LOG = Path("artifacts") / "research_gate_decisions.jsonl"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_gate_decision(
    *,
    path: str | Path = DEFAULT_GATE_DECISION_LOG,
    source: str,
    proposal_id: str,
    gate: dict[str, Any],
    budget: dict[str, Any] | None = None,
) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    reasons = list(gate.get("reasons") or []) + list((budget or {}).get("reasons") or [])
    decision = "block" if (gate.get("decision") == "block" or (budget or {}).get("decision") == "block") else gate.get("decision")
    payload = {
        "recorded_at_utc": _utc_now(),
        "source": source,
        "proposal_id": proposal_id,
        "decision": decision,
        "reasons": reasons,
        "budget_bucket": gate.get("budget_bucket") or (budget or {}).get("budget_bucket"),
        "expected_information_gain_score": gate.get("expected_information_gain_score"),
        "gate": gate,
        "budget": budget or {},
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


def _experiment_flags(task_spec: dict[str, Any]) -> dict[str, bool]:
    payload = task_spec.get("payload") or {}
    expressions = " ".join(str(expr) for expr in payload.get("expressions") or [])
    note = str(task_spec.get("worker_note") or "").lower()
    return {
        "mechanical_combination": any(marker in expressions for marker in ("+", "-", "*", "/")) or "combine" in note,
        "low_coverage_retest": "coverage_too_low" in note or "low_coverage" in note,
    }


def _attach_governance(task_spec: dict[str, Any], *, decision: str, source: str, gate: dict[str, Any], budget: dict[str, Any]) -> dict[str, Any]:
    governed = {**task_spec, "payload": dict(task_spec.get("payload") or {})}
    governed["payload"]["governance"] = {
        "decision": decision,
        "source": source,
        "proposal_id": gate.get("proposal_id"),
        "budget_bucket": gate.get("budget_bucket") or budget.get("budget_bucket"),
        "reasons": list(gate.get("reasons") or []) + list(budget.get("reasons") or []),
    }
    return governed


def govern_research_task_spec(
    task_spec: dict[str, Any],
    *,
    store: ExperimentStore,
    available_fields: set[str] | None = None,
    used_counts: dict[str, int] | None = None,
    allocator: ResearchBudgetAllocator | None = None,
    audit_path: str | Path | None = None,
    source: str = "research_task_generation",
) -> dict[str, Any]:
    proposal = proposal_from_research_task(task_spec)
    fingerprint = task_spec.get("fingerprint")
    gate = evaluate_research_experiment_gate(
        proposal,
        available_fields=available_fields or set(TUSHARE_FEATURE_COLUMNS),
        store=store,
        fingerprint=fingerprint,
        budget_available=True,
    )
    allocator = allocator or ResearchBudgetAllocator.from_file()
    budget = allocator.check(
        gate.get("budget_bucket") or proposal.budget_bucket,
        used_counts=used_counts or {},
        experiment_flags=_experiment_flags(task_spec),
    )
    decision = "block" if gate["decision"] == "block" or budget["decision"] == "block" else gate["decision"]
    if audit_path is not None or decision != "allow":
        write_gate_decision(
            path=audit_path or DEFAULT_GATE_DECISION_LOG,
            source=source,
            proposal_id=proposal.proposal_id,
            gate={**gate, "decision": decision},
            budget=budget,
        )
    governed = _attach_governance(task_spec, decision=decision, source=source, gate=gate, budget=budget)
    return {
        "decision": decision,
        "proposal": proposal,
        "gate": gate,
        "budget": budget,
        "task_spec": governed if decision == "allow" else None,
    }


def govern_workflow_task_spec(
    task_spec: dict[str, Any],
    *,
    config: dict[str, Any],
    store: ExperimentStore,
    available_fields: set[str] | None = None,
    used_counts: dict[str, int] | None = None,
    allocator: ResearchBudgetAllocator | None = None,
    audit_path: str | Path | None = None,
    source: str = "workflow_task_generation",
) -> dict[str, Any]:
    task = {
        "task_type": task_spec.get("task_type") or "workflow",
        "payload": dict(task_spec.get("payload") or {}),
        "worker_note": task_spec.get("worker_note"),
        "task_id": task_spec.get("task_id"),
    }
    proposal = proposal_from_workflow_task(task, config=config)
    fingerprint = task_spec.get("fingerprint")
    gate = evaluate_research_experiment_gate(
        proposal,
        available_fields=available_fields or set(TUSHARE_FEATURE_COLUMNS),
        store=store,
        fingerprint=fingerprint,
        budget_available=True,
    )
    allocator = allocator or ResearchBudgetAllocator.from_file()
    budget = allocator.check(
        gate.get("budget_bucket") or proposal.budget_bucket,
        used_counts=used_counts or {},
        experiment_flags=_experiment_flags(task_spec),
    )
    decision = "block" if gate["decision"] == "block" or budget["decision"] == "block" else gate["decision"]
    if audit_path is not None or decision != "allow":
        write_gate_decision(
            path=audit_path or DEFAULT_GATE_DECISION_LOG,
            source=source,
            proposal_id=proposal.proposal_id,
            gate={**gate, "decision": decision},
            budget=budget,
        )
    return {
        "decision": decision,
        "proposal": proposal,
        "gate": gate,
        "budget": budget,
        "task_spec": task_spec if decision == "allow" else None,
    }

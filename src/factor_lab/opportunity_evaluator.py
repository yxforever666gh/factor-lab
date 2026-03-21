from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from factor_lab.parent_child_delta import compute_parent_child_delta


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _epistemic_result(label: str, next_state: str, knowledge_gain: list[str], evidence: dict[str, Any], epistemic_gain: list[str]) -> tuple[str, str, list[str], dict[str, Any]]:
    merged_evidence = dict(evidence)
    merged_evidence["epistemic_gain"] = epistemic_gain
    return label, next_state, knowledge_gain, merged_evidence


def _evaluate_expand(payload: dict[str, Any], jobs: list[dict[str, Any]], batch_comparison: dict[str, Any]) -> tuple[str, str, list[str], dict[str, Any]]:
    total_candidates = sum(int(row.get("candidate_count") or 0) for row in jobs)
    total_graveyard = sum(int(row.get("graveyard_count") or 0) for row in jobs)
    candidate_presence = batch_comparison.get("candidate_presence") or {}
    graveyard_presence = batch_comparison.get("graveyard_presence") or {}
    job_count = max(len(jobs), 1)
    stable_candidates = [name for name, present in candidate_presence.items() if len(present) == job_count]
    repeated_failures = [name for name, present in graveyard_presence.items() if len(present) == job_count]
    evidence = {
        "total_candidates": total_candidates,
        "total_graveyard": total_graveyard,
        "candidate_presence": candidate_presence,
        "graveyard_presence": graveyard_presence,
        "stable_candidates": stable_candidates,
        "repeated_failures": repeated_failures,
        "expected_information_gain": list(payload.get("expected_information_gain") or []),
    }
    if stable_candidates:
        return _epistemic_result(
            "boundary_confirmed",
            "promoted",
            ["window_stability_check", "exploration_candidate_survived"],
            evidence,
            ["boundary_confirmed", "uncertainty_reduced", "validation_scope_expanded"],
        )
    if repeated_failures:
        return _epistemic_result(
            "boundary_broken",
            "evaluated",
            ["exploration_graveyard_identified"],
            evidence,
            ["boundary_broken", "search_space_reduced", "negative_result_recorded"],
        )
    return _epistemic_result(
        "inconclusive_expand",
        "evaluated",
        [],
        evidence,
        ["inconclusive", "uncertainty_preserved"],
    )


def _evaluate_recombine(payload: dict[str, Any], jobs: list[dict[str, Any]], batch_comparison: dict[str, Any]) -> tuple[str, str, list[str], dict[str, Any]]:
    total_candidates = sum(int(row.get("candidate_count") or 0) for row in jobs)
    total_graveyard = sum(int(row.get("graveyard_count") or 0) for row in jobs)
    candidate_presence = batch_comparison.get("candidate_presence") or {}
    graveyard_presence = batch_comparison.get("graveyard_presence") or {}
    target_candidates = set(payload.get("target_candidates") or [])
    novel_candidates = [name for name in candidate_presence.keys() if name not in target_candidates]
    novel_graveyard = [name for name in graveyard_presence.keys() if name not in target_candidates]
    evidence = {
        "total_candidates": total_candidates,
        "total_graveyard": total_graveyard,
        "candidate_presence": candidate_presence,
        "graveyard_presence": graveyard_presence,
        "novel_candidates": novel_candidates,
        "novel_graveyard": novel_graveyard,
        "target_candidates": list(target_candidates),
    }
    if novel_candidates:
        return _epistemic_result(
            "new_branch_opened",
            "promoted",
            ["exploration_candidate_survived"],
            evidence,
            ["new_branch_opened", "recombination_productive", "search_space_expanded"],
        )
    if total_graveyard > 0 and novel_graveyard:
        return _epistemic_result(
            "hybrid_invalidated",
            "evaluated",
            ["exploration_graveyard_identified"],
            evidence,
            ["hybrid_invalidated", "search_space_reduced", "negative_result_recorded"],
        )
    if total_graveyard > 0:
        return _epistemic_result(
            "repeat_without_new_information",
            "evaluated",
            ["exploration_graveyard_identified"],
            evidence,
            ["repeat_without_new_information", "low_novelty_realized"],
        )
    return _epistemic_result(
        "inconclusive_recombine",
        "evaluated",
        [],
        evidence,
        ["inconclusive", "uncertainty_preserved"],
    )


def _evaluate_probe(payload: dict[str, Any], jobs: list[dict[str, Any]], batch_comparison: dict[str, Any]) -> tuple[str, str, list[str], dict[str, Any]]:
    total_candidates = sum(int(row.get("candidate_count") or 0) for row in jobs)
    total_graveyard = sum(int(row.get("graveyard_count") or 0) for row in jobs)
    candidate_presence = batch_comparison.get("candidate_presence") or {}
    graveyard_presence = batch_comparison.get("graveyard_presence") or {}
    evidence = {
        "total_candidates": total_candidates,
        "total_graveyard": total_graveyard,
        "candidate_presence": candidate_presence,
        "graveyard_presence": graveyard_presence,
    }
    if total_candidates > 0:
        return _epistemic_result(
            "probe_promising",
            "promoted",
            ["exploration_candidate_survived"],
            evidence,
            ["probe_promising", "new_branch_opened", "uncertainty_reduced"],
        )
    if total_graveyard > 0:
        return _epistemic_result(
            "probe_negative_but_informative",
            "evaluated",
            ["exploration_graveyard_identified"],
            evidence,
            ["probe_negative_but_informative", "search_space_reduced", "negative_result_recorded"],
        )
    return _epistemic_result(
        "probe_inconclusive",
        "evaluated",
        [],
        evidence,
        ["inconclusive", "uncertainty_preserved"],
    )


def _evaluation_from_generated_batch(task: dict[str, Any], payload: dict[str, Any]) -> tuple[str, str, list[str], dict[str, Any]]:
    output_dir = Path(payload.get("output_dir") or "")
    batch_summary = _read_json(output_dir / "batch_summary.json")
    batch_comparison = _read_json(output_dir / "batch_comparison.json")
    jobs = batch_summary if isinstance(batch_summary, list) else []
    otype = payload.get("opportunity_type") or "probe"

    if otype == "expand":
        return _evaluate_expand(payload, jobs, batch_comparison)
    if otype == "recombine":
        return _evaluate_recombine(payload, jobs, batch_comparison)
    return _evaluate_probe(payload, jobs, batch_comparison)


def evaluate_opportunity_from_task(task: dict[str, Any], *, status: str, summary: str | None = None, error_text: str | None = None) -> dict[str, Any] | None:
    payload = task.get("payload") or {}
    opportunity_id = payload.get("opportunity_id")
    if not opportunity_id:
        return None
    store_path = Path(__file__).resolve().parents[2] / "artifacts" / "research_opportunity_store.json"
    store_payload = _read_json(store_path)

    evidence: dict[str, Any] = {}
    summary_text = (summary or "") + " " + (error_text or "")

    if status != "finished":
        label = "failed"
        next_state = "rejected"
        knowledge_gain = []
        epistemic_gain = ["execution_failed"]
        gain_count = 0
        has_gain = False
    elif task.get("task_type") == "generated_batch":
        label, next_state, knowledge_gain, evidence = _evaluation_from_generated_batch(task, payload)
        epistemic_gain = list(evidence.get("epistemic_gain") or [])
        gain_count = len([g for g in knowledge_gain if g and g != "no_significant_information_gain"])
        has_gain = any(tag not in {"inconclusive", "uncertainty_preserved", "low_novelty_realized"} for tag in epistemic_gain)
    else:
        knowledge_gain = [g for g in (payload.get("knowledge_gain") or payload.get("expected_information_gain") or []) if g]
        epistemic_gain = ["validated_expected_signal"] if knowledge_gain else []
        gain_count = len([g for g in knowledge_gain if g and g != "no_significant_information_gain"])
        has_gain = gain_count > 0 or ("knowledge_gain=" in summary_text and "no_significant_information_gain" not in summary_text)
        if gain_count >= 2:
            label = "high_gain"
            next_state = "promoted"
            epistemic_gain = ["hypothesis_supported", "uncertainty_reduced"]
        elif gain_count == 1:
            label = "moderate_gain"
            next_state = "evaluated"
            epistemic_gain = ["partial_support", "uncertainty_reduced"]
        elif "inconclusive" in summary_text.lower():
            label = "inconclusive"
            next_state = "evaluated"
            epistemic_gain = ["inconclusive", "uncertainty_preserved"]
        else:
            label = "low_gain"
            next_state = "evaluated"
            epistemic_gain = ["repeat_without_new_information"]

    evidence.setdefault("hypothesis", payload.get("hypothesis"))
    evidence.setdefault("question", payload.get("question"))
    evidence.setdefault("expected_information_gain", list(payload.get("expected_information_gain") or []))
    evidence["epistemic_gain"] = epistemic_gain

    full_run_recommended = False
    if task.get("task_type") == "generated_batch" and (payload.get("execution_mode") == "cheap_screen") and has_gain:
        full_run_recommended = True

    evaluation = {
        "opportunity_id": opportunity_id,
        "status": status,
        "evaluation_label": label,
        "has_gain": has_gain,
        "gain_count": gain_count,
        "summary": summary,
        "error_text": error_text,
        "task_id": task.get("task_id"),
        "task_type": task.get("task_type"),
        "knowledge_gain": knowledge_gain,
        "epistemic_gain": epistemic_gain,
        "next_state": next_state,
        "full_run_recommended": full_run_recommended,
        "evidence": evidence,
    }
    delta = compute_parent_child_delta(store_payload, opportunity_id)
    if delta:
        evaluation["parent_child_delta"] = delta
        evaluation["evidence"]["parent_child_delta"] = delta
    return evaluation

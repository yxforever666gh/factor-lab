from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class ResearchExperimentProposal:
    proposal_id: str
    experiment_type: str
    factor_names: list[str] = field(default_factory=list)
    expressions: list[str] = field(default_factory=list)
    mechanism_id: str | None = None
    hypothesis: str = ""
    expected_information_gain: list[str] = field(default_factory=list)
    required_data_fields: list[str] = field(default_factory=list)
    start_date: str | None = None
    end_date: str | None = None
    universe_limit: int | None = None
    horizon: str | None = None
    parent_candidates: list[str] = field(default_factory=list)
    novelty_claim: str | None = None
    falsification_criteria: list[str] = field(default_factory=list)
    promote_if: list[str] = field(default_factory=list)
    stop_if: list[str] = field(default_factory=list)
    source_agent: str = "unknown"
    budget_bucket: str = "pure_exploration"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ResearchExperimentProposal":
        data = dict(payload)
        list_fields = {
            "factor_names",
            "expressions",
            "expected_information_gain",
            "required_data_fields",
            "parent_candidates",
            "falsification_criteria",
            "promote_if",
            "stop_if",
        }
        for key in list_fields:
            value = data.get(key)
            if value is None:
                data[key] = []
            elif not isinstance(value, list):
                data[key] = [str(value)]
        return cls(**data)

    def validation_errors(self) -> list[str]:
        errors: list[str] = []
        if not self.proposal_id:
            errors.append("proposal_id is required")
        if not self.experiment_type:
            errors.append("experiment_type is required")
        if self.experiment_type == "generated_candidate" and not self.hypothesis.strip():
            errors.append("generated_candidate proposals require hypothesis")
        if self.experiment_type == "generated_candidate" and not (self.mechanism_id or self.novelty_claim):
            errors.append("generated_candidate proposals require mechanism_id or novelty_claim")
        return errors


def _factor_fields(config: dict[str, Any]) -> tuple[list[str], list[str]]:
    names: list[str] = []
    expressions: list[str] = []
    for row in config.get("factors") or []:
        if not isinstance(row, dict):
            continue
        if row.get("name"):
            names.append(str(row["name"]))
        if row.get("expression"):
            expressions.append(str(row["expression"]))
    return names, expressions


def _bucket_from_task(task: dict[str, Any]) -> str:
    note = str(task.get("worker_note") or "").lower()
    task_type = str(task.get("task_type") or "")
    if "baseline" in note:
        return "data_quality_coverage"
    if "validation" in note or task_type == "workflow":
        return "robustness_validation"
    if task_type == "diagnostic":
        return "mechanism_validation"
    return "pure_exploration"


def proposal_from_workflow_task(task: dict[str, Any], *, config: dict[str, Any]) -> ResearchExperimentProposal:
    payload = dict(task.get("payload") or {})
    factor_names, expressions = _factor_fields(config)
    return ResearchExperimentProposal(
        proposal_id=str(payload.get("branch_id") or task.get("task_id") or payload.get("config_path") or "legacy_workflow"),
        experiment_type=str(task.get("task_type") or "workflow"),
        factor_names=factor_names,
        expressions=expressions,
        mechanism_id=payload.get("mechanism_id"),
        hypothesis=str(payload.get("hypothesis") or payload.get("goal") or task.get("worker_note") or "legacy workflow task"),
        expected_information_gain=list(payload.get("expected_information_gain") or []),
        required_data_fields=list(payload.get("required_data_fields") or []),
        start_date=config.get("start_date"),
        end_date=config.get("end_date"),
        universe_limit=config.get("universe_limit"),
        horizon=payload.get("horizon"),
        parent_candidates=list(payload.get("parent_candidates") or []),
        novelty_claim=payload.get("novelty_claim"),
        falsification_criteria=list(payload.get("falsification_criteria") or payload.get("disconfirm_if") or []),
        promote_if=list(payload.get("promote_if") or []),
        stop_if=list(payload.get("stop_if") or []),
        source_agent=str(payload.get("source_agent") or payload.get("source") or "legacy_queue"),
        budget_bucket=str(payload.get("budget_bucket") or _bucket_from_task(task)),
    )


def proposal_from_research_task(task: dict[str, Any]) -> ResearchExperimentProposal:
    payload = dict(task.get("payload") or {})
    focus = list(payload.get("focus_factors") or payload.get("target_candidates") or payload.get("parent_candidates") or [])
    expressions = list(payload.get("expressions") or [])
    return ResearchExperimentProposal(
        proposal_id=str(payload.get("branch_id") or payload.get("opportunity_id") or task.get("task_id") or task.get("fingerprint") or "research_task"),
        experiment_type=str(task.get("task_type") or payload.get("task_type") or "research_task"),
        factor_names=[str(item) for item in focus],
        expressions=[str(item) for item in expressions],
        mechanism_id=payload.get("mechanism_id") or payload.get("diagnostic_type"),
        hypothesis=str(payload.get("hypothesis") or payload.get("goal") or payload.get("question") or task.get("worker_note") or "research task should add information"),
        expected_information_gain=list(payload.get("expected_information_gain") or payload.get("expected_knowledge_gain") or []),
        required_data_fields=list(payload.get("required_data_fields") or []),
        horizon=payload.get("horizon") or payload.get("validation_stage"),
        parent_candidates=list(payload.get("parent_candidates") or payload.get("target_candidates") or []),
        novelty_claim=payload.get("novelty_claim") or payload.get("opportunity_type"),
        falsification_criteria=list(payload.get("falsification_criteria") or payload.get("disconfirm_if") or []),
        promote_if=list(payload.get("promote_if") or []),
        stop_if=list(payload.get("stop_if") or []),
        source_agent=str(payload.get("source_agent") or payload.get("source") or "research_task"),
        budget_bucket=str(payload.get("budget_bucket") or _bucket_from_task(task)),
    )

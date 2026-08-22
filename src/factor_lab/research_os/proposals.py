"""Narrow LLM boundary: hypotheses and DSL only, never research decisions.

The proposal bridge in this module deliberately stops *before* experiment
registration and trial reservation.  It turns an untrusted model response into
an auditable, content-addressed review while keeping every result-affecting
research input under deterministic caller control.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Protocol, Sequence

from pydantic import ValidationError

from .catalog import CatalogConflict, ResearchCatalog, RunRecord
from .contracts import (
    DataQualityStatus,
    ExperimentSpec,
    FactorSpec,
    Preregistration,
    SignalFieldSpec,
    SnapshotTier,
)
from .dsl import (
    CompiledFactorGraph,
    DecisionPoint,
    FieldSpec,
    compile_factor_graph,
)
from .fingerprint import canonical_json, content_fingerprint


FORBIDDEN_DECISION_KEYS = frozenset(
    {
        "promote",
        "promotion",
        "pass_gate",
        "p_value",
        "sharpe",
        "metrics",
        "lifecycle_state",
        "target_weight",
        "target_weights",
        "weight",
        "weights",
        "allocation",
        "allocations",
        "order",
        "orders",
        "trade",
        "trades",
        "fill",
        "fills",
        "position",
        "positions",
        "portfolio_return",
        "annual_return",
        "net_return",
        "information_ratio",
        "max_drawdown",
        "turnover",
        "verdict",
        "decision",
        "run",
        "execute",
    }
)

_PROPOSAL_TOP_LEVEL_FIELDS = frozenset({"preregistration", "factor"})
_TEMPLATE_BOUND_FIELDS = frozenset(
    {
        "snapshot",
        "universe",
        "label",
        "portfolio",
        "validation",
        "evaluator_version",
        "environment",
        "evaluation_inputs",
    }
)
_BLOCKING_TRUST_LABELS = frozenset(
    {
        "st_history_unverified",
        "legacy_untrusted_data",
        "legacy_execution_regression_only",
        "disputed",
        "quarantined",
    }
)
_PROPOSAL_INPUT_DOMAIN = "factor-lab/research-os/v1/llm-proposal-input"
_PROPOSAL_DECISION_DOMAIN = "factor-lab/research-os/v1/llm-proposal-decision"
_TEMPLATE_DOMAIN = "factor-lab/research-os/v1/experiment-template"
_FIELD_REGISTRY_DOMAIN = "factor-lab/research-os/v1/proposal-field-registry"
_FORBIDDEN_DECISION_TOKENS = frozenset(
    {
        "allocation",
        "allocations",
        "drawdown",
        "execute",
        "execution",
        "fill",
        "fills",
        "metric",
        "metrics",
        "order",
        "orders",
        "position",
        "positions",
        "promote",
        "promoted",
        "promotion",
        "return",
        "returns",
        "sharpe",
        "trade",
        "trades",
        "turnover",
        "verdict",
        "weight",
        "weights",
    }
)


class HypothesisProposalPort(Protocol):
    def propose(self, context: Mapping[str, Any]) -> Mapping[str, Any]: ...


@dataclass(frozen=True)
class ValidatedProposal:
    preregistration: Preregistration
    factor: FactorSpec
    compiled_graph: CompiledFactorGraph


@dataclass(frozen=True)
class ProposalReview:
    accepted: bool
    proposal: ValidatedProposal | None
    violations: tuple[str, ...]


@dataclass(frozen=True)
class ProposalDecision:
    """Content-addressed deterministic review of one untrusted proposal."""

    decision_id: str
    input_fingerprint: str
    raw_proposal_hash: str
    template_hash: str
    field_registry_hash: str
    accepted: bool
    violations: tuple[str, ...]
    experiment_fingerprint: str | None = None
    experiment_spec: ExperimentSpec | None = None
    schema_version: str = "research-os/llm-proposal-review/v1"

    def __post_init__(self) -> None:
        digests = {
            "input_fingerprint": self.input_fingerprint,
            "raw_proposal_hash": self.raw_proposal_hash,
            "template_hash": self.template_hash,
            "field_registry_hash": self.field_registry_hash,
        }
        if self.experiment_fingerprint is not None:
            digests["experiment_fingerprint"] = self.experiment_fingerprint
        for name, value in digests.items():
            if len(value) != 64 or any(ch not in "0123456789abcdef" for ch in value):
                raise ValueError(f"{name} must be a lowercase SHA-256 digest")
        if not self.decision_id.startswith("proposal_review_"):
            raise ValueError("decision_id must use the proposal_review_ namespace")
        if self.accepted != (self.experiment_spec is not None):
            raise ValueError("accepted decisions must carry exactly one experiment spec")
        if self.accepted != (self.experiment_fingerprint is not None):
            raise ValueError("accepted decisions must carry an experiment fingerprint")
        if self.accepted and self.violations:
            raise ValueError("accepted decisions cannot contain violations")
        if not self.accepted and not self.violations:
            raise ValueError("rejected decisions must explain at least one violation")
        if self.experiment_spec is not None:
            if self.experiment_spec.fingerprint() != self.experiment_fingerprint:
                raise ValueError("experiment fingerprint does not match experiment spec")
        expected_id = _proposal_decision_id(self._content_payload())
        if self.decision_id != expected_id:
            raise ValueError("decision_id does not match proposal decision content")

    def _content_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "input_fingerprint": self.input_fingerprint,
            "raw_proposal_hash": self.raw_proposal_hash,
            "template_hash": self.template_hash,
            "field_registry_hash": self.field_registry_hash,
            "accepted": self.accepted,
            "violations": self.violations,
            "experiment_fingerprint": self.experiment_fingerprint,
            "experiment_spec": self.experiment_spec,
        }

    def to_dict(self) -> dict[str, Any]:
        return json.loads(
            canonical_json(
                {
                    "decision_id": self.decision_id,
                    **self._content_payload(),
                }
            )
        )


def _find_forbidden_keys(value: Any, *, prefix: str = "") -> list[str]:
    found: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            name = str(key)
            path = f"{prefix}.{name}" if prefix else name
            normalized = name.strip().lower()
            tokens = frozenset(filter(None, re.split(r"[^a-z0-9]+", normalized)))
            if (
                normalized in FORBIDDEN_DECISION_KEYS
                or tokens.intersection(_FORBIDDEN_DECISION_TOKENS)
            ):
                found.append(path)
            found.extend(_find_forbidden_keys(item, prefix=path))
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        for index, item in enumerate(value):
            found.extend(_find_forbidden_keys(item, prefix=f"{prefix}[{index}]"))
    return found


def validate_llm_proposal(
    payload: Mapping[str, Any],
    *,
    field_specs: Sequence[FieldSpec],
) -> ProposalReview:
    """Validate an untrusted proposal without registering or running it."""

    violations = [
        f"decision_authority_forbidden:{path}"
        for path in _find_forbidden_keys(payload)
    ]
    preregistration: Preregistration | None = None
    factor: FactorSpec | None = None
    compiled: CompiledFactorGraph | None = None
    try:
        preregistration = Preregistration.model_validate(payload.get("preregistration"))
    except (ValidationError, TypeError) as exc:
        violations.append(f"invalid_preregistration:{exc}")
    try:
        factor = FactorSpec.model_validate(payload.get("factor"))
    except (ValidationError, TypeError) as exc:
        violations.append(f"invalid_factor:{exc}")
    if factor is not None:
        if not isinstance(factor.expression, Mapping):
            violations.append("typed_dsl_required")
        else:
            try:
                compiled = compile_factor_graph(
                    factor.expression,
                    field_specs,
                    decision_point=DecisionPoint.AFTER_CLOSE,
                )
            except (TypeError, ValueError) as exc:
                violations.append(f"invalid_dsl:{exc}")
    if preregistration is not None and factor is not None:
        if preregistration.hypothesis_id == factor.factor_id:
            violations.append("hypothesis_and_factor_ids_must_be_distinct")
        allowed = set(preregistration.allowed_variants)
        if factor.allowed_variants and not set(factor.allowed_variants).issubset(allowed):
            violations.append("factor_variants_exceed_preregistered_budget")
    if violations or preregistration is None or factor is None or compiled is None:
        return ProposalReview(False, None, tuple(dict.fromkeys(violations)))
    return ProposalReview(
        True,
        ValidatedProposal(preregistration, factor, compiled),
        (),
    )


def _field_spec_payload(spec: FieldSpec) -> dict[str, Any]:
    return {
        "name": spec.name,
        "value_type": spec.value_type.value,
        "role": spec.role.value,
        "availability": spec.availability.value,
        "minimum_lag_sessions": spec.minimum_lag_sessions,
        "available_at_column": spec.available_at_column,
    }


def _canonical_field_specs(
    field_specs: Sequence[FieldSpec],
) -> tuple[tuple[FieldSpec, ...], tuple[str, ...]]:
    violations: list[str] = []
    checked: list[FieldSpec] = []
    for index, item in enumerate(field_specs):
        if not isinstance(item, FieldSpec):
            violations.append(f"invalid_field_registry_entry:{index}")
            continue
        checked.append(item)
    checked.sort(key=lambda item: item.name)
    names = [item.name for item in checked]
    duplicates = sorted({name for name in names if names.count(name) > 1})
    violations.extend(f"duplicate_field_registry:{name}" for name in duplicates)
    if not checked:
        violations.append("field_registry_empty")
    return tuple(checked), tuple(violations)


def _template_review(
    template: ExperimentSpec | Mapping[str, Any],
) -> tuple[ExperimentSpec | None, tuple[str, ...]]:
    violations: list[str] = []
    if isinstance(template, ExperimentSpec):
        supplied = set(template.model_fields_set)
        checked = template
    elif isinstance(template, Mapping):
        supplied = {str(key) for key in template}
        try:
            checked = ExperimentSpec.model_validate(template)
        except (ValidationError, TypeError, ValueError) as exc:
            checked = None
            violations.append(f"invalid_experiment_template:{exc}")
    else:
        supplied = set()
        checked = None
        violations.append("invalid_experiment_template:type")

    for field in sorted(_TEMPLATE_BOUND_FIELDS.difference(supplied)):
        violations.append(f"template_binding_missing:{field}")

    if checked is not None:
        if checked.factor is None or checked.sleeve is not None:
            violations.append("template_candidate_must_be_factor")
        snapshot = checked.snapshot
        if (
            snapshot.tier is not SnapshotTier.GOLD
            or snapshot.quality_status is not DataQualityStatus.ACCEPTED
        ):
            violations.append("template_snapshot_not_accepted_gold")
        if snapshot.snapshot_id != snapshot.content_hash:
            violations.append("template_snapshot_not_content_addressed")
        blocked = sorted(_BLOCKING_TRUST_LABELS.intersection(snapshot.trust_labels))
        violations.extend(f"template_snapshot_trust_blocker:{item}" for item in blocked)
        if checked.environment.evaluator_build != checked.evaluator_version:
            violations.append("template_evaluator_environment_mismatch")
    if violations:
        return None, tuple(sorted(set(violations)))
    return checked, ()


def _normalize_preregistration(value: Preregistration) -> Preregistration:
    payload = value.model_dump(mode="python", exclude_none=False)
    for key in (
        "expected_regimes",
        "falsification_criteria",
        "allowed_variants",
        "stop_rules",
    ):
        payload[key] = tuple(sorted(set(payload[key])))
    return Preregistration.model_validate(payload)


def _normalize_factor(
    proposal: ValidatedProposal,
    field_specs: Sequence[FieldSpec],
) -> FactorSpec:
    graph = proposal.compiled_graph.graph.to_dict()
    graph["nodes"] = sorted(graph["nodes"], key=lambda row: str(row["id"]))
    field_by_name = {item.name: item for item in field_specs}
    trusted_registry = tuple(
        SignalFieldSpec(
            name=field_by_name[name].name,
            value_type=field_by_name[name].value_type.value,
            role=field_by_name[name].role.value,
            availability=field_by_name[name].availability.value,
            minimum_lag_sessions=field_by_name[name].minimum_lag_sessions,
            available_at_column=field_by_name[name].available_at_column,
        )
        for name in sorted(proposal.compiled_graph.field_lags)
    )
    payload = proposal.factor.model_dump(mode="python", exclude_none=False)
    payload.update(
        {
            "expression": graph,
            "signal_field_registry": trusted_registry,
        }
    )
    for key in (
        "expected_regimes",
        "falsification_criteria",
        "allowed_variants",
        "data_requirements",
    ):
        payload[key] = tuple(sorted(set(payload[key])))
    return FactorSpec.model_validate(payload)


def _proposal_decision_id(payload: Mapping[str, Any]) -> str:
    digest = content_fingerprint(payload, domain=_PROPOSAL_DECISION_DOMAIN)
    return f"proposal_review_{digest}"


def review_llm_proposal(
    payload: Mapping[str, Any],
    *,
    experiment_template: ExperimentSpec | Mapping[str, Any],
    field_specs: Sequence[FieldSpec],
) -> ProposalDecision:
    """Bind a model proposal to a caller-owned, frozen experiment template.

    The returned object is an audit decision only.  This function intentionally
    does not register an experiment, reserve a trial, run an evaluator, or
    perform a lifecycle transition.
    """

    if not isinstance(payload, Mapping):
        raise TypeError("LLM proposal payload must be a mapping")
    raw_proposal_hash = content_fingerprint(payload, domain=_PROPOSAL_INPUT_DOMAIN)
    template_hash = content_fingerprint(
        experiment_template,
        domain=_TEMPLATE_DOMAIN,
    )
    canonical_fields, field_violations = _canonical_field_specs(field_specs)
    field_registry_hash = content_fingerprint(
        [_field_spec_payload(item) for item in canonical_fields],
        domain=_FIELD_REGISTRY_DOMAIN,
    )
    input_fingerprint = content_fingerprint(
        {
            "raw_proposal_hash": raw_proposal_hash,
            "template_hash": template_hash,
            "field_registry_hash": field_registry_hash,
        },
        domain=f"{_PROPOSAL_INPUT_DOMAIN}/review",
    )

    review = validate_llm_proposal(payload, field_specs=canonical_fields)
    template, template_violations = _template_review(experiment_template)
    violations = list(review.violations)
    violations.extend(field_violations)
    violations.extend(template_violations)
    violations.extend(
        f"proposal_scope_forbidden:{key}"
        for key in sorted(set(map(str, payload)).difference(_PROPOSAL_TOP_LEVEL_FIELDS))
    )
    factor_payload = payload.get("factor")
    if isinstance(factor_payload, Mapping) and "signal_field_registry" in factor_payload:
        violations.append("proposal_scope_forbidden:factor.signal_field_registry")

    spec: ExperimentSpec | None = None
    fingerprint: str | None = None
    if not violations and review.proposal is not None and template is not None:
        factor = _normalize_factor(review.proposal, canonical_fields)
        preregistration = _normalize_preregistration(review.proposal.preregistration)
        candidate = template.model_dump(mode="python", exclude_none=False)
        candidate.update(
            {
                "factor": factor,
                "sleeve": None,
                "preregistration": preregistration,
            }
        )
        try:
            spec = ExperimentSpec.model_validate(candidate)
            fingerprint = spec.fingerprint()
        except (ValidationError, TypeError, ValueError) as exc:
            violations.append(f"proposal_template_binding_failed:{exc}")
            spec = None
            fingerprint = None

    normalized_violations = tuple(sorted(set(violations)))
    content = {
        "schema_version": "research-os/llm-proposal-review/v1",
        "input_fingerprint": input_fingerprint,
        "raw_proposal_hash": raw_proposal_hash,
        "template_hash": template_hash,
        "field_registry_hash": field_registry_hash,
        "accepted": not normalized_violations,
        "violations": normalized_violations,
        "experiment_fingerprint": fingerprint,
        "experiment_spec": spec,
    }
    return ProposalDecision(
        decision_id=_proposal_decision_id(content),
        input_fingerprint=input_fingerprint,
        raw_proposal_hash=raw_proposal_hash,
        template_hash=template_hash,
        field_registry_hash=field_registry_hash,
        accepted=not normalized_violations,
        violations=normalized_violations,
        experiment_fingerprint=fingerprint,
        experiment_spec=spec,
    )


def _assert_same_persisted_decision(
    existing: RunRecord,
    expected: RunRecord,
) -> None:
    if (
        existing.run_type != expected.run_type
        or existing.status != expected.status
        or existing.input_fingerprint != expected.input_fingerprint
        or canonical_json(existing.metadata) != canonical_json(expected.metadata)
        or existing.error != expected.error
    ):
        raise CatalogConflict(
            f"proposal decision run identity collision for {expected.run_id!r}"
        )


def persist_proposal_decision(
    catalog: ResearchCatalog,
    decision: ProposalDecision,
    *,
    reviewed_at: datetime | None = None,
) -> RunRecord:
    """Idempotently append an accepted *or rejected* review to ``ros_runs``.

    Persistence remains intentionally separate from experiment registration and
    trial reservation.  A later deterministic monthly gate may consume an
    accepted review; this function grants it no research authority.
    """

    timestamp = reviewed_at or catalog.database_now()
    metadata = {
        "authority": "review_only_no_execution_no_budget_no_promotion",
        "proposal_decision": decision.to_dict(),
    }
    expected = RunRecord(
        run_id=decision.decision_id,
        run_type="llm_proposal_review",
        status="completed",
        input_fingerprint=decision.input_fingerprint,
        started_at=timestamp,
        completed_at=timestamp,
        metadata=metadata,
    )
    existing = catalog.get_run(expected.run_id)
    if existing is not None:
        _assert_same_persisted_decision(existing, expected)
        return existing
    claimed, won = catalog.claim_run(expected)
    if not won:
        _assert_same_persisted_decision(claimed, expected)
    return claimed


__all__ = [
    "FORBIDDEN_DECISION_KEYS",
    "HypothesisProposalPort",
    "ProposalReview",
    "ProposalDecision",
    "ValidatedProposal",
    "persist_proposal_decision",
    "review_llm_proposal",
    "validate_llm_proposal",
]

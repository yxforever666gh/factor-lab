from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from factor_lab.pit_financial_schema import field_names

LOW_INFORMATION_FIELDS = {"pe", "pb", "pe_ttm", "book_yield", "earnings_yield", "momentum_20", "momentum_60", "momentum_120", "turnover", "size_inv"}
REQUIRED_TOP_LEVEL = {"hypothesis_id", "economic_logic", "required_fields", "expected_mechanism", "failure_modes", "stop_rules", "max_variants", "min_coverage", "pit_requirements"}


@dataclass(frozen=True)
class ResearchGateDecision:
    decision: str
    reasons: tuple[str, ...]
    hypothesis_id: str | None = None
    max_variants: int | None = None
    required_pit_features: tuple[str, ...] = ()

    @property
    def allowed(self) -> bool:
        return self.decision == "allow_preflight"

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision": self.decision,
            "reasons": list(self.reasons),
            "hypothesis_id": self.hypothesis_id,
            "max_variants": self.max_variants,
            "required_pit_features": list(self.required_pit_features),
        }


def load_hypothesis(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def evaluate_research_gate(hypothesis: dict[str, Any]) -> ResearchGateDecision:
    reasons: list[str] = []
    missing = sorted(REQUIRED_TOP_LEVEL - set(hypothesis))
    if missing:
        reasons.append("missing_required_sections:" + ",".join(missing))
    hypothesis_id = hypothesis.get("hypothesis_id")
    economic_logic = str(hypothesis.get("economic_logic", "")).strip()
    mechanism = str(hypothesis.get("expected_mechanism", "")).strip()
    required_fields = list(hypothesis.get("required_fields") or [])
    required_pit_features = list(hypothesis.get("required_pit_features") or [])
    stop_rules = hypothesis.get("stop_rules") or {}
    pit_requirements = hypothesis.get("pit_requirements") or {}

    if len(economic_logic) < 30:
        reasons.append("economic_logic_too_thin")
    if len(mechanism) < 20:
        reasons.append("expected_mechanism_too_thin")
    if not required_fields:
        reasons.append("missing_required_fields")
    if required_fields and set(required_fields).issubset(LOW_INFORMATION_FIELDS):
        reasons.append("low_information_legacy_field_recombination")
    if not required_pit_features:
        reasons.append("missing_required_pit_features")
    else:
        known = field_names()
        unknown_pit = [field for field in required_pit_features if field not in known]
        if unknown_pit:
            reasons.append("unknown_pit_features:" + ",".join(sorted(unknown_pit)))
    try:
        max_variants = int(hypothesis.get("max_variants", 999))
    except Exception:
        max_variants = 999
    if max_variants > 3:
        reasons.append("max_variants_above_3")
    try:
        min_coverage = float(hypothesis.get("min_coverage", 0))
    except Exception:
        min_coverage = 0
    if min_coverage < 0.70:
        reasons.append("min_coverage_below_70pct")
    if not stop_rules:
        reasons.append("missing_stop_rules")
    else:
        if "max_pairwise_corr" not in stop_rules:
            reasons.append("missing_max_pairwise_corr_stop_rule")
        if "require_cost_adjusted_pass" not in stop_rules:
            reasons.append("missing_cost_adjusted_stop_rule")
    if not pit_requirements:
        reasons.append("missing_pit_requirements")
    else:
        if not pit_requirements.get("require_ann_date_asof"):
            reasons.append("pit_asof_not_required")
        if pit_requirements.get("forbid_end_date_only") is not True:
            reasons.append("end_date_only_not_forbidden")
    decision = "block" if reasons else "allow_preflight"
    return ResearchGateDecision(
        decision=decision,
        reasons=tuple(reasons),
        hypothesis_id=hypothesis_id,
        max_variants=max_variants,
        required_pit_features=tuple(required_pit_features),
    )


def evaluate_research_gate_file(path: str | Path) -> ResearchGateDecision:
    return evaluate_research_gate(load_hypothesis(path))

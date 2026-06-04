from __future__ import annotations

import json
from pathlib import Path

from factor_lab.value_route_scorecard import build_scorecard_rows


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_route_evidence(root: Path, route: str, *, base: float, cost: float, tail: float) -> None:
    _write_json(
        root / "value_route_bucket_aware" / "runs" / f"{route}_bucket_aware" / "bucket_aware_portfolio_results.json",
        [{"spread_mean": base, "pass_gate": True, "observations": 385}],
    )
    _write_json(
        root / "value_route_followups" / "runs" / f"{route}__cost_sensitivity_20bps" / "bucket_aware_portfolio_results.json",
        [{"spread_mean": cost, "pass_gate": True, "observations": 385}],
    )
    _write_json(
        root / "value_route_followups" / "runs" / f"{route}__bucket_pair_stricter_tail" / "bucket_aware_portfolio_results.json",
        [{"spread_mean": tail, "pass_gate": True, "observations": 385}],
    )


def test_scorecard_roles_and_tail_degradation(tmp_path: Path):
    _write_json(
        tmp_path / "controlled_route_policy.json",
        {
            "routes": {
                "value_quality_no_distress": {"decision": "promote"},
                "value_momentum_confirmation": {"decision": "promote"},
                "industry_relative_value": {"decision": "promote"},
            }
        },
    )
    _write_route_evidence(tmp_path, "value_quality_no_distress", base=0.006225, cost=0.0049, tail=0.0030876)
    _write_route_evidence(tmp_path, "value_momentum_confirmation", base=0.005275, cost=0.0041, tail=0.0029)
    _write_route_evidence(tmp_path, "industry_relative_value", base=0.003576, cost=0.0030, tail=0.0021)

    rows = build_scorecard_rows(artifact_dir=tmp_path)
    by = {r["route_id"]: r for r in rows}
    q = by["value_quality_no_distress"]

    assert round(q["tail_degradation_ratio"], 3) == 0.504
    assert q["recommended_role"] == "primary_candidate"
    assert q["preliminary_weight"] == 0.5
    assert by["industry_relative_value"]["recommended_role"] == "low_weight_core_value_candidate"

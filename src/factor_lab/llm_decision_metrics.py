from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "artifacts"



def _read_json(path: str | Path, default: Any) -> Any:
    path = Path(path)
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default



def build_llm_decision_metrics(
    *,
    hermes_decision_artifacts_path: str | Path = ARTIFACTS / "hermes_decision_artifacts.json",
    candidate_generation_plan_path: str | Path = ARTIFACTS / "candidate_generation_plan.json",
    output_path: str | Path = ARTIFACTS / "llm_decision_metrics.json",
) -> dict[str, Any]:
    hermes_decision_artifacts = _read_json(hermes_decision_artifacts_path, {})
    candidate_generation_plan = _read_json(candidate_generation_plan_path, {})
    proposals = list(candidate_generation_plan.get("proposals") or [])

    novelty_sources: dict[str, int] = {}
    for row in proposals:
        source = row.get("novelty_judgment_source") or row.get("decision_source") or "unknown"
        novelty_sources[source] = int(novelty_sources.get(source) or 0) + 1

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "decision_sources": {
            "planner": (((hermes_decision_artifacts.get("planner") or {}).get("decision_metadata") or {}).get("source") or ((hermes_decision_artifacts.get("planner") or {}).get("decision_source"))),
            "diagnostician": (((hermes_decision_artifacts.get("diagnostician") or {}).get("decision_metadata") or {}).get("source") or ((hermes_decision_artifacts.get("diagnostician") or {}).get("decision_source"))),
        },
        "decision_validation": {
            "planner_schema_valid": (((hermes_decision_artifacts.get("planner") or {}).get("decision_metadata") or {}).get("schema_valid")),
            "diagnostician_schema_valid": (((hermes_decision_artifacts.get("diagnostician") or {}).get("decision_metadata") or {}).get("schema_valid")),
        },
        "proposal_novelty_sources": novelty_sources,
        "proposal_count": len(proposals),
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload

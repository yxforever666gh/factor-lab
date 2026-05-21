from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.decision_context_builder import build_failure_decision_context, build_planner_decision_context
from factor_lab.decision_impact_report import build_decision_impact_report
from factor_lab.llm_decision_metrics import build_llm_decision_metrics
from factor_lab.hermes_decision_router import HermesDecisionRouter

ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = ROOT / "artifacts"
OUTPUT_DIR = ARTIFACTS / "llm_decision_ab"



def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))



def _run_arm(name: str, provider: str, planner_brief: dict, failure_brief: dict) -> dict:
    router = HermesDecisionRouter(provider=provider)
    planner_context = build_planner_decision_context(planner_brief) if planner_brief else {}
    failure_context = build_failure_decision_context(failure_brief) if failure_brief else {}
    planner = router.generate("planner", planner_context) if planner_context else {}
    failure = router.generate("diagnostician", failure_context) if failure_context else {}
    payload = {
        "arm": name,
        "provider": provider,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "planner": planner,
        "diagnostician": failure,
    }
    (OUTPUT_DIR / f"{name}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload



def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    planner_brief = _read_json(ARTIFACTS / "researcher_profile_brief.json")
    failure_brief = _read_json(ARTIFACTS / "diagnostician_brief.json")
    preferred_provider = (os.environ.get("FACTOR_LAB_DECISION_PROVIDER") or os.environ.get("FACTOR_LAB_LLM_PROVIDER") or "auto").strip().lower()

    assisted_provider = preferred_provider if preferred_provider != "mock" else "auto"
    primary_provider = assisted_provider if assisted_provider in {"hermes_native_agent", "hermes_native_gateway", "direct_model"} else "direct_model"
    primary_arm_name = "hermes_native_session_primary" if primary_provider in {"hermes_native_agent", "hermes_native_gateway"} else "llm_primary"
    assisted_arm_name = "hermes_native_session_assisted" if assisted_provider in {"hermes_native_agent", "hermes_native_gateway"} else "llm_assisted"

    arms = {
        "heuristic_only": _run_arm("heuristic_only", "heuristic", planner_brief, failure_brief),
        assisted_arm_name: _run_arm(assisted_arm_name, assisted_provider, planner_brief, failure_brief),
        primary_arm_name: _run_arm(primary_arm_name, primary_provider, planner_brief, failure_brief),
    }
    metrics = build_llm_decision_metrics(output_path=OUTPUT_DIR / "decision_metrics.json")
    impact = build_decision_impact_report(output_path=OUTPUT_DIR / "decision_impact_report.json")
    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "arms": {
            arm: {
                "planner_source": (((payload.get("planner") or {}).get("decision_metadata") or {}).get("source") or ((payload.get("planner") or {}).get("decision_source"))),
                "diagnostician_source": (((payload.get("diagnostician") or {}).get("decision_metadata") or {}).get("source") or ((payload.get("diagnostician") or {}).get("decision_source"))),
                "planner_schema_valid": (((payload.get("planner") or {}).get("decision_metadata") or {}).get("schema_valid")),
                "diagnostician_schema_valid": (((payload.get("diagnostician") or {}).get("decision_metadata") or {}).get("schema_valid")),
            }
            for arm, payload in arms.items()
        },
        "current_metrics": metrics,
        "decision_impact": impact,
    }
    (OUTPUT_DIR / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

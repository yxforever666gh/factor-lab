from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = ROOT / "artifacts"


def read_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


if __name__ == "__main__":
    provider = read_json(ARTIFACTS / "llm_provider_health.json", {})
    attribution = read_json(ARTIFACTS / "research_attribution.json", {})
    ab = read_json(ARTIFACTS / "llm_decision_ab" / "summary.json", {})
    impact = read_json(ARTIFACTS / "decision_impact_report.json", {})

    lines = [
        "# Factor Lab Decision Observation Report",
        "",
        f"Generated at: {provider.get('generated_at_utc') or attribution.get('generated_at_utc') or ab.get('generated_at_utc')}",
        "",
        "## Provider Health",
        f"- recommended_effective_source: {provider.get('recommended_effective_source')}",
        f"- real_provider_configured: {provider.get('real_provider_configured')}",
        f"- probe_attempted: {(provider.get('probe') or {}).get('attempted')}",
        f"- probe_ok: {(provider.get('probe') or {}).get('ok')}",
        f"- probe_error: {(provider.get('probe') or {}).get('error')}",
        "",
        "## Current Decision Sources",
        f"- planner: {(((attribution.get('current_snapshot') or {}).get('decision_layer') or {}).get('planner_source'))}",
        f"- failure_analyst: {(((attribution.get('current_snapshot') or {}).get('decision_layer') or {}).get('failure_analyst_source'))}",
        "",
        "## Decision Impact vs Heuristic Baseline",
        f"- planner_changed: {((impact.get('planner') or {}).get('changed'))}",
        f"- failure_analyst_changed: {((impact.get('failure_analyst') or {}).get('changed'))}",
        "",
        "## Current Attribution Snapshot",
        f"- proposal_count: {(((attribution.get('current_snapshot') or {}).get('generation') or {}).get('proposal_count'))}",
        f"- stable_alpha_candidate_count: {(((attribution.get('current_snapshot') or {}).get('final_conversion') or {}).get('stable_alpha_candidate_count'))}",
        f"- duplicate_suppress_count: {(((attribution.get('current_snapshot') or {}).get('final_conversion') or {}).get('duplicate_suppress_count'))}",
        "",
        "## 48h Window",
        f"- proposal_to_keep_validating: {(((attribution.get('observation_windows') or {}).get('48h') or {}).get('proposal_to_keep_validating'))}",
        f"- proposal_to_stable_alpha_candidate: {(((attribution.get('observation_windows') or {}).get('48h') or {}).get('proposal_to_stable_alpha_candidate'))}",
        "",
        "## 7d Window",
        f"- proposal_to_keep_validating: {(((attribution.get('observation_windows') or {}).get('7d') or {}).get('proposal_to_keep_validating'))}",
        f"- proposal_to_stable_alpha_candidate: {(((attribution.get('observation_windows') or {}).get('7d') or {}).get('proposal_to_stable_alpha_candidate'))}",
        f"- old_space stable conversion: {((((attribution.get('observation_windows') or {}).get('7d') or {}).get('by_pool') or {}).get('old_space_optimization') or {}).get('proposal_to_stable_alpha_candidate')}",
        f"- new_mechanism stable conversion: {((((attribution.get('observation_windows') or {}).get('7d') or {}).get('by_pool') or {}).get('new_mechanism_exploration') or {}).get('proposal_to_stable_alpha_candidate')}",
        "",
        "## A/B Arms",
    ]
    for arm, row in ((ab.get('arms') or {}).items()):
        lines.append(f"- {arm}: planner={row.get('planner_source')}, failure={row.get('failure_analyst_source')}, planner_schema_valid={row.get('planner_schema_valid')}, failure_schema_valid={row.get('failure_analyst_schema_valid')}")

    out = ARTIFACTS / "factor_lab_decision_observation_report.md"
    out.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    print(str(out))

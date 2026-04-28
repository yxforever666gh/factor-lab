import json

from factor_lab.decision_context_builder import build_failure_decision_context, build_planner_decision_context
from factor_lab.decision_impact_report import build_decision_impact_report
from factor_lab.llm_provider_router import DecisionProviderRouter



def test_decision_impact_report_marks_no_change_for_heuristic_baseline(tmp_path):
    planner_brief = {
        "agent_role": "planner_agent",
        "inputs": {
            "research_flow_state": {"state": "ready"},
            "failure_state": {},
            "queue_budget": {"validation": 2, "exploration": 1},
            "research_learning": {},
            "stable_candidates": [{"factor_name": "mom_20"}],
            "latest_graveyard": [],
            "branch_selected_families": [],
            "knowledge_gain_counter": {},
            "open_questions": [],
            "candidate_pool_tasks": [],
            "candidate_pool_suppressed": [],
            "candidate_hypothesis_cards": [],
        },
    }
    failure_brief = {
        "agent_role": "failure_analyst",
        "inputs": {
            "recent_failed_or_risky_tasks": [],
            "llm_diagnostics": {},
            "research_flow_state": {"state": "ready"},
            "latest_graveyard": [],
            "knowledge_gain_counter": {},
        },
    }
    router = DecisionProviderRouter(provider="heuristic")
    current = {
        "planner": router.generate("planner", build_planner_decision_context(planner_brief)),
        "failure_analyst": router.generate("failure_analyst", build_failure_decision_context(failure_brief)),
    }

    planner_brief_path = tmp_path / "planner.json"
    failure_brief_path = tmp_path / "failure.json"
    current_path = tmp_path / "agent_responses.json"
    output_path = tmp_path / "impact.json"
    planner_brief_path.write_text(json.dumps(planner_brief, ensure_ascii=False), encoding="utf-8")
    failure_brief_path.write_text(json.dumps(failure_brief, ensure_ascii=False), encoding="utf-8")
    current_path.write_text(json.dumps(current, ensure_ascii=False), encoding="utf-8")

    payload = build_decision_impact_report(
        planner_brief_path=planner_brief_path,
        failure_brief_path=failure_brief_path,
        agent_responses_path=current_path,
        output_path=output_path,
    )

    assert payload["planner"]["current_source"] == "heuristic"
    assert payload["planner"]["changed"] is False
    assert payload["failure_analyst"]["changed"] is False

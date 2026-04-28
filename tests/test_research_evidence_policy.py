import json
from pathlib import Path

from factor_lab.research_planner import ResearchPlannerAgent


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_research_evidence_policy_exists():
    path = REPO_ROOT / "configs" / "research_evidence_policy.json"
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["name"] == "openclaw_research_evidence_policy"
    assert payload["rules"]["require_acceptance_gate_for_frontier"] is True


def test_planner_keeps_generated_candidate_while_marking_evidence_missing():
    planner = ResearchPlannerAgent()
    snapshot = {
        "exploration_state": {},
        "failure_state": {},
        "knowledge_gain_counter": {},
        "analyst_signals": {},
    }
    candidate_pool = {
        "tasks": [
            {
                "task_type": "workflow",
                "category": "exploration",
                "priority_hint": 35,
                "worker_note": "exploration｜generated_candidate:test",
                "payload": {"source": "candidate_generation"},
                "focus_candidates": [
                    {"candidate_name": "gen_test", "evidence_gate": {"action": "evidence_missing"}}
                ],
                "expected_knowledge_gain": ["candidate_survival_check"],
                "reason": "generated candidate",
            }
        ]
    }

    result = planner.rank_tasks(snapshot, candidate_pool, None)

    assert len(result["selected_tasks"]) == 1
    assert result["selected_tasks"][0]["planner_score"] > 0

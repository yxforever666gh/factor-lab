import json
from pathlib import Path

from factor_lab.research_learning import build_research_learning
from factor_lab.research_strategy import build_strategy_plan


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_coding_policy_file_exists_and_has_core_keys():
    path = REPO_ROOT / "configs" / "research_coding_policy.json"
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["name"] == "openclaw_autonomous_research_coding_policy"
    assert payload["principles"]["shared_intermediates_first"] is True
    assert "performance_rules" in payload


def test_strategy_plan_carries_coding_policy(tmp_path):
    state_snapshot = {
        "autonomy_policy": {},
        "coding_policy": {"name": "coding-policy-test", "principles": {"shared_intermediates_first": True}},
        "memory": {},
        "convergence_policy": {"archive_after_no_gain_runs": 2},
        "exploration_state": {},
        "knowledge_gain_counter": {},
        "candidates": {"graveyard": [], "stable": []},
        "repeated_failure_patterns": [],
        "planner": {"analyst_signals": {}},
        "frontier_focus": {},
    }
    proposal = {"selected_tasks": []}
    state_path = tmp_path / "state.json"
    proposal_path = tmp_path / "proposal.json"
    output_path = tmp_path / "strategy_plan.json"
    state_path.write_text(json.dumps(state_snapshot, ensure_ascii=False), encoding="utf-8")
    proposal_path.write_text(json.dumps(proposal, ensure_ascii=False), encoding="utf-8")

    payload = build_strategy_plan(state_path, proposal_path, output_path)

    assert payload["coding_policy"]["name"] == "coding-policy-test"


def test_research_learning_carries_coding_profile(tmp_path):
    memory_path = tmp_path / "research_memory.json"
    memory_path.write_text(json.dumps({
        "updated_at_utc": "2026-04-01T00:00:00+00:00",
        "execution_feedback": [],
        "coding_profile": {"policy_name": "coding-policy-test"}
    }, ensure_ascii=False), encoding="utf-8")

    learning = build_research_learning(memory_path)
    assert learning["coding_profile"]["policy_name"] == "coding-policy-test"

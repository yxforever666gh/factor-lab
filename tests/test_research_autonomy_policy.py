import json
from pathlib import Path

from factor_lab.research_strategy import build_strategy_plan


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_autonomy_policy_file_exists_and_has_core_keys():
    path = REPO_ROOT / "configs" / "research_autonomy_policy.json"
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["name"] == "openclaw_autonomous_factor_research_constitution"
    assert payload["principles"]["unit_of_research"] == "hypothesis_not_factor_name"
    assert "budget_policy" in payload


def test_build_strategy_plan_carries_autonomy_policy(tmp_path):
    state_snapshot = {
        "autonomy_policy": {"name": "policy-test", "principles": {"unit_of_research": "hypothesis_not_factor_name"}},
        "memory": {},
        "convergence_policy": {"archive_after_no_gain_runs": 2},
        "exploration_state": {},
        "knowledge_gain_counter": {},
        "frontier_focus": {},
        "candidates": {"graveyard": [], "stable": []},
        "repeated_failure_patterns": [],
        "planner": {"analyst_signals": {}},
    }
    proposal = {"selected_tasks": []}
    state_path = tmp_path / "state.json"
    proposal_path = tmp_path / "proposal.json"
    output_path = tmp_path / "strategy_plan.json"
    state_path.write_text(json.dumps(state_snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    proposal_path.write_text(json.dumps(proposal, ensure_ascii=False, indent=2), encoding="utf-8")

    payload = build_strategy_plan(state_path, proposal_path, output_path)

    assert payload["autonomy_policy"]["name"] == "policy-test"

import json
from pathlib import Path

from factor_lab.research_learning import build_research_learning
from factor_lab.research_strategy import apply_strategy_plan


class DummyStore:
    pass


def test_apply_strategy_plan_updates_autonomy_profile(monkeypatch, tmp_path):
    validated_path = tmp_path / "validated.json"
    strategy_plan_path = tmp_path / "strategy_plan.json"
    output_path = tmp_path / "injected.json"
    memory_path = tmp_path / "research_memory.json"

    validated_path.write_text(json.dumps({"accepted_tasks": []}, ensure_ascii=False), encoding="utf-8")
    strategy_plan_path.write_text(
        json.dumps(
            {
                "approved_tasks": [],
                "memory_updates": {},
                "branch_actions": [],
                "convergence_policy": {"archive_after_no_gain_runs": 2},
                "autonomy_policy": {
                    "name": "policy-test",
                    "principles": {
                        "unit_of_research": "hypothesis_not_factor_name",
                        "objective": ["epistemic_gain"],
                        "failure_policy": {
                            "reward_high_value_failure": True,
                            "discourage_low_information_repetition": True,
                            "treat_boundary_discovery_as_progress": True,
                        },
                    },
                    "budget_policy": {"exploitation": 0.45, "adjacent_exploration": 0.35, "novelty_search": 0.2},
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr("factor_lab.research_strategy.ExperimentStore", lambda _path: DummyStore())
    monkeypatch.setattr("factor_lab.research_strategy.recently_finished_same_fingerprint", lambda *args, **kwargs: False)

    apply_strategy_plan(validated_path, strategy_plan_path, output_path, memory_path, db_path=tmp_path / "db.sqlite")
    memory = json.loads(memory_path.read_text(encoding="utf-8"))

    assert memory["autonomy_profile"]["policy_name"] == "policy-test"
    assert memory["autonomy_profile"]["unit_of_research"] == "hypothesis_not_factor_name"
    assert memory["autonomy_profile"]["learning_bias"]["reward_high_value_failure"] is True


def test_research_learning_carries_autonomy_profile(tmp_path):
    memory_path = tmp_path / "research_memory.json"
    memory_path.write_text(
        json.dumps(
            {
                "updated_at_utc": "2026-04-01T00:00:00+00:00",
                "execution_feedback": [],
                "autonomy_profile": {"policy_name": "policy-test", "unit_of_research": "hypothesis_not_factor_name"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    learning = build_research_learning(memory_path)
    assert learning["autonomy_profile"]["policy_name"] == "policy-test"

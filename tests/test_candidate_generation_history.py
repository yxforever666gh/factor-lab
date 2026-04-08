import json
from pathlib import Path

from factor_lab.research_learning import build_research_learning
from factor_lab.research_strategy import apply_strategy_plan


class DummyStore:
    def enqueue_research_task(self, **kwargs):
        return "task-123"


def test_apply_strategy_plan_writes_candidate_generation_history(monkeypatch, tmp_path):
    validated_path = tmp_path / "validated.json"
    strategy_plan_path = tmp_path / "strategy_plan.json"
    output_path = tmp_path / "injected.json"
    memory_path = tmp_path / "research_memory.json"

    validated_path.write_text(json.dumps({"accepted_tasks": []}, ensure_ascii=False), encoding="utf-8")
    strategy_plan_path.write_text(
        json.dumps(
            {
                "approved_tasks": [
                    {
                        "task_type": "workflow",
                        "category": "exploration",
                        "priority_hint": 55,
                        "fingerprint": "fp1",
                        "payload": {
                            "source": "candidate_generation",
                            "branch_id": "gen__combine_sub__mom_20__book_yield",
                            "candidate_generation_context": {
                                "candidate_id": "gen__combine_sub__mom_20__book_yield",
                                "operator": "combine_sub",
                                "base_factors": ["mom_20", "book_yield"],
                                "source": "stable_plus_graveyard",
                                "cheap_screen": {"pass": True, "score": 1.0},
                            },
                        },
                        "strategy_score": 50,
                    }
                ],
                "memory_updates": {},
                "branch_actions": [],
                "convergence_policy": {"archive_after_no_gain_runs": 2},
                "autonomy_policy": {},
                "coding_policy": {},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr("factor_lab.research_strategy.ExperimentStore", lambda _path: DummyStore())
    monkeypatch.setattr("factor_lab.research_strategy.recently_finished_same_fingerprint", lambda *args, **kwargs: False)

    apply_strategy_plan(validated_path, strategy_plan_path, output_path, memory_path, db_path=tmp_path / "db.sqlite")
    memory = json.loads(memory_path.read_text(encoding="utf-8"))

    row = memory["candidate_generation_history"][-1]
    assert row["operator"] == "combine_sub"
    assert row["cheap_screen"]["pass"] is True


def test_research_learning_exposes_candidate_generation_history(tmp_path):
    memory_path = tmp_path / "research_memory.json"
    memory_path.write_text(
        json.dumps(
            {
                "updated_at_utc": "2026-04-01T00:00:00+00:00",
                "execution_feedback": [],
                "candidate_generation_history": [
                    {"operator": "combine_sub", "cheap_screen": {"pass": True, "score": 1.0}}
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    learning = build_research_learning(memory_path)
    assert learning["operator_stats"]["combine_sub"]["proposal_count"] == 1
    assert learning["operator_stats"]["combine_sub"]["cheap_screen_pass_count"] == 1

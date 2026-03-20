from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.research_strategy import apply_strategy_plan


if __name__ == "__main__":
    apply_strategy_plan(
        validated_path="artifacts/research_planner_validated.json",
        strategy_plan_path="artifacts/strategy_plan.json",
        output_path="artifacts/research_planner_injected.json",
        memory_path="artifacts/research_memory.json",
        db_path="artifacts/factor_lab.db",
    )
    print("strategy plan applied")

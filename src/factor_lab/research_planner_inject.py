from __future__ import annotations

from pathlib import Path

from factor_lab.research_strategy import apply_strategy_plan


DB_PATH = Path("artifacts") / "factor_lab.db"


def inject_research_planner_tasks(
    validated_path: str | Path,
    output_path: str | Path,
    strategy_plan_path: str | Path | None = None,
    memory_path: str | Path | None = None,
) -> dict:
    return apply_strategy_plan(
        validated_path=validated_path,
        strategy_plan_path=strategy_plan_path or Path("artifacts") / "strategy_plan.json",
        output_path=output_path,
        memory_path=memory_path or Path("artifacts") / "research_memory.json",
        db_path=DB_PATH,
    )

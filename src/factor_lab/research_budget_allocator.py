from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_POLICY_PATH = Path("configs") / "research_budget_policy.json"
DEFAULT_POLICY = {
    "daily_budget": {
        "max_total_tasks": 100,
        "mechanism_validation": 40,
        "data_quality_coverage": 20,
        "robustness_validation": 20,
        "reverse_direction_tests": 10,
        "pure_exploration": 10,
    },
    "hard_limits": {
        "max_mechanical_combinations_per_day": 10,
        "max_same_family_experiments_per_day": 20,
        "max_low_coverage_retests_per_day": 5,
    },
}


@dataclass(frozen=True)
class ResearchBudgetAllocator:
    daily_budget: dict[str, int]
    hard_limits: dict[str, int]

    @classmethod
    def from_policy(cls, policy: dict[str, Any]) -> "ResearchBudgetAllocator":
        daily_budget = {
            str(key): max(0, int(value))
            for key, value in dict(policy.get("daily_budget") or {}).items()
        }
        hard_limits = {
            str(key): max(0, int(value))
            for key, value in dict(policy.get("hard_limits") or {}).items()
        }
        return cls(daily_budget=daily_budget, hard_limits=hard_limits)

    @classmethod
    def from_file(cls, path: str | Path = DEFAULT_POLICY_PATH) -> "ResearchBudgetAllocator":
        path = Path(path)
        if not path.exists():
            return cls.from_policy(DEFAULT_POLICY)
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            payload = DEFAULT_POLICY
        return cls.from_policy(payload)

    def check(
        self,
        budget_bucket: str,
        *,
        used_counts: dict[str, int] | None = None,
        experiment_flags: dict[str, bool] | None = None,
    ) -> dict[str, Any]:
        used_counts = dict(used_counts or {})
        experiment_flags = dict(experiment_flags or {})
        reasons: list[str] = []

        total_limit = int(self.daily_budget.get("max_total_tasks") or 0)
        total_used = int(used_counts.get("total") or 0)
        if total_limit > 0 and total_used >= total_limit:
            reasons.append("daily total budget exhausted")

        bucket_limit = int(self.daily_budget.get(budget_bucket) or 0)
        bucket_used = int(used_counts.get(budget_bucket) or 0)
        if bucket_limit > 0 and bucket_used >= bucket_limit:
            reasons.append(f"budget bucket exhausted: {budget_bucket}")

        if experiment_flags.get("mechanical_combination"):
            mechanical_limit = int(self.hard_limits.get("max_mechanical_combinations_per_day") or 0)
            mechanical_used = int(used_counts.get("mechanical_combinations") or 0)
            if mechanical_limit > 0 and mechanical_used >= mechanical_limit:
                reasons.append("mechanical combination daily limit exhausted")

        if experiment_flags.get("low_coverage_retest"):
            low_coverage_limit = int(self.hard_limits.get("max_low_coverage_retests_per_day") or 0)
            low_coverage_used = int(used_counts.get("low_coverage_retests") or 0)
            if low_coverage_limit > 0 and low_coverage_used >= low_coverage_limit:
                reasons.append("low coverage retest daily limit exhausted")

        return {
            "decision": "block" if reasons else "allow",
            "reasons": reasons,
            "budget_bucket": budget_bucket,
            "bucket_limit": bucket_limit,
            "bucket_used": bucket_used,
            "remaining_bucket": max(0, bucket_limit - bucket_used) if bucket_limit else None,
            "total_limit": total_limit,
            "total_used": total_used,
            "remaining_total": max(0, total_limit - total_used) if total_limit else None,
        }

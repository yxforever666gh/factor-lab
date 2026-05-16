from factor_lab.research_budget_allocator import ResearchBudgetAllocator


def test_research_budget_allocator_allows_within_bucket_limit():
    allocator = ResearchBudgetAllocator.from_policy({
        "daily_budget": {"max_total_tasks": 10, "mechanism_validation": 2, "pure_exploration": 1},
        "hard_limits": {"max_mechanical_combinations_per_day": 1},
    })

    result = allocator.check("mechanism_validation", used_counts={"mechanism_validation": 1, "total": 3})

    assert result["decision"] == "allow"
    assert result["remaining_bucket"] == 1


def test_research_budget_allocator_blocks_exhausted_bucket():
    allocator = ResearchBudgetAllocator.from_policy({
        "daily_budget": {"max_total_tasks": 10, "mechanism_validation": 2},
        "hard_limits": {},
    })

    result = allocator.check("mechanism_validation", used_counts={"mechanism_validation": 2, "total": 3})

    assert result["decision"] == "block"
    assert "budget bucket exhausted: mechanism_validation" in result["reasons"]


def test_research_budget_allocator_blocks_total_limit():
    allocator = ResearchBudgetAllocator.from_policy({
        "daily_budget": {"max_total_tasks": 2, "mechanism_validation": 10},
        "hard_limits": {},
    })

    result = allocator.check("mechanism_validation", used_counts={"mechanism_validation": 1, "total": 2})

    assert result["decision"] == "block"
    assert "daily total budget exhausted" in result["reasons"]


def test_research_budget_allocator_blocks_mechanical_limit():
    allocator = ResearchBudgetAllocator.from_policy({
        "daily_budget": {"max_total_tasks": 10, "pure_exploration": 10},
        "hard_limits": {"max_mechanical_combinations_per_day": 1},
    })

    result = allocator.check(
        "pure_exploration",
        used_counts={"pure_exploration": 0, "total": 0, "mechanical_combinations": 1},
        experiment_flags={"mechanical_combination": True},
    )

    assert result["decision"] == "block"
    assert "mechanical combination daily limit exhausted" in result["reasons"]

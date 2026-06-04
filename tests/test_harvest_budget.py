from factor_lab.harvest_budget import allocate_harvest_budget, budget_after_admission


def test_cycle_budget_capped_at_policy_max():
    budget = allocate_harvest_budget({"max_experiments_per_cycle": 2, "max_cycles_per_day": 4}, requested_experiments=10)
    assert budget["admitted_experiments"] == 2
    assert budget["remaining_cycle_experiments"] == 0


def test_daily_budget_accounts_for_cycles_and_completed_experiments():
    budget = allocate_harvest_budget(
        {"max_experiments_per_cycle": 2, "max_cycles_per_day": 4},
        requested_experiments=2,
        completed_today=7,
    )
    assert budget["admitted_experiments"] == 1
    assert budget["remaining_daily_experiments"] == 0


def test_budget_after_admission_reports_remaining():
    after = budget_after_admission(max_cycle=2, max_daily=8, admitted=1, completed_today=3)
    assert after == {"remaining_cycle_experiments": 1, "remaining_daily_experiments": 4}


def test_zero_budget_blocks_admission():
    budget = allocate_harvest_budget({"max_experiments_per_cycle": 2, "max_cycles_per_day": 4}, requested_experiments=1, completed_today=8)
    assert budget["admitted_experiments"] == 0
    assert budget["budget_exhausted"] is True
